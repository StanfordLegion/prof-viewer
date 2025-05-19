use std::collections::BTreeMap;
use std::sync::Arc;

use duckdb::arrow::{
    array::{
        ArrayBuilder, ArrayRef, BooleanBuilder, Int64Builder, ListBuilder, StringBuilder,
        StructBuilder, UInt64Builder,
    },
    datatypes::{DataType, Field, Fields, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};

use crate::data::{
    self, DataSourceDescription, DataSourceInfo, FieldID, ItemField, ItemLink, SlotMetaTile,
};
use crate::timestamp::Interval;

pub struct ArrowSchema {
    interval_fields: Fields,
    interval_data_type: DataType,
    item_link_fields: Fields,
    item_link_data_type: DataType,
    source_locator_item_field: Arc<Field>,
    info_schema: Arc<Schema>,
}

impl ArrowSchema {
    // Generate chunks of RecordBatch to work around: https://github.com/duckdb/duckdb-rs/issues/503
    const VECTOR_SIZE: usize = 2048;

    pub fn new() -> Self {
        // Interval data type
        let interval_field_start = Field::new("start", DataType::Int64, false);
        let interval_field_stop = Field::new("stop", DataType::Int64, false);
        let interval_fields = Fields::from(vec![
            interval_field_start.clone(),
            interval_field_stop.clone(),
        ]);
        let interval_data_type = DataType::Struct(interval_fields.clone());

        // ItemLink data type
        let item_link_field_item_uid = Field::new("item_uid", DataType::UInt64, true);
        let item_link_field_title = Field::new("title", DataType::Utf8, false);
        let item_link_field_interval = Field::new("interval", interval_data_type.clone(), true);
        let item_link_field_entry_slug = Field::new("entry_slug", DataType::Utf8, true);
        let item_link_fields = Fields::from(vec![
            item_link_field_item_uid.clone(),
            item_link_field_title.clone(),
            item_link_field_interval.clone(),
            item_link_field_entry_slug.clone(),
        ]);
        let item_link_data_type = DataType::Struct(item_link_fields.clone());

        let source_locator_item_field = Arc::new(Field::new_list_field(DataType::Utf8, false));

        let info_schema = Arc::new(Schema::new(vec![
            Field::new(
                "source_locator",
                DataType::List(source_locator_item_field.clone()),
                false,
            ),
            Field::new("interval", interval_data_type.clone(), false),
            Field::new("warning_message", DataType::Utf8, true),
        ]));

        Self {
            interval_fields,
            interval_data_type,
            item_link_fields,
            item_link_data_type,
            source_locator_item_field,
            info_schema,
        }
    }

    pub fn append_info(
        &self,
        app: &mut duckdb::Appender<'_>,
        desc: &DataSourceDescription,
        info: &DataSourceInfo,
    ) -> duckdb::Result<()> {
        let num_rows = 1;

        let mut source_locator_builder = ListBuilder::new(StringBuilder::new())
            .with_field(self.source_locator_item_field.clone());
        let mut interval_builder =
            StructBuilder::from_fields(self.interval_fields.clone(), num_rows);
        let mut warning_message_builder = StringBuilder::new();

        source_locator_builder.append_value(desc.source_locator.iter().map(|x| Some(x)));

        FieldType::append_interval(&mut interval_builder, info.interval).unwrap();

        warning_message_builder.append_option(info.warning_message.as_ref());

        let source_locator_array: ArrayRef = Arc::new(source_locator_builder.finish());
        let interval_array: ArrayRef = Arc::new(interval_builder.finish());
        let warning_message_array: ArrayRef = Arc::new(warning_message_builder.finish());

        let batch = RecordBatch::try_new(
            self.info_schema.clone(),
            vec![source_locator_array, interval_array, warning_message_array],
        )
        .unwrap();
        app.append_record_batch(batch)?;

        Ok(())
    }

    pub fn append_slot_meta_tile(
        &self,
        app: &mut duckdb::Appender<'_>,
        tile: &SlotMetaTile,
        field_slots: &BTreeMap<FieldID, usize>,
        slot_fields: &[(String, FieldType)],
    ) -> duckdb::Result<()> {
        // The schema varies dynamically depending on what fields we get,
        // so don't bother trying to pre-generate this.
        let mut schema_fields = vec![
            Field::new("item_uid", DataType::UInt64, false),
            Field::new("interval", self.interval_data_type.clone(), false),
            Field::new("title", DataType::Utf8, false),
        ];
        for (field_name, field_type) in slot_fields {
            schema_fields.push(Field::new(field_name, field_type.data_type(self), true));
        }
        let slot_meta_tile_schema = Arc::new(Schema::new(schema_fields));

        let mut item_uid_builder = UInt64Builder::new();
        let mut interval_builder =
            StructBuilder::from_fields(self.interval_fields.clone(), Self::VECTOR_SIZE);
        let mut title_builder = StringBuilder::new();

        let mut slot_builders: Vec<_> = slot_fields
            .iter()
            .map(|(_, field_type)| field_type.make_builder(self))
            .collect();

        let mut slot_present = Vec::new();
        slot_present.resize(slot_fields.len(), false);
        for row in &tile.data.items {
            for item in row {
                item_uid_builder.append_value(item.item_uid.0);
                FieldType::append_interval(&mut interval_builder, item.original_interval).unwrap();
                title_builder.append_value(&item.title);

                slot_present.clear();
                slot_present.resize(slot_fields.len(), false);
                for ItemField(field_id, field, _) in &item.fields {
                    let slot = *field_slots.get(field_id).unwrap();
                    let (_, field_type) = &slot_fields[slot];
                    let builder = &mut slot_builders[slot];
                    field_type.append_value(builder, field).unwrap();
                    slot_present[slot] = true;
                }
                for (slot, present) in slot_present.iter().enumerate() {
                    if !present {
                        let (_, field_type) = &slot_fields[slot];
                        let builder = &mut slot_builders[slot];
                        field_type.append_null(builder).unwrap();
                    }
                }

                if item_uid_builder.len() >= Self::VECTOR_SIZE {
                    let mut arrays = Vec::<ArrayRef>::new();
                    arrays.push(Arc::new(item_uid_builder.finish()));
                    arrays.push(Arc::new(interval_builder.finish()));
                    arrays.push(Arc::new(title_builder.finish()));
                    for builder in &mut slot_builders {
                        arrays.push(Arc::new(builder.finish()));
                    }
                    let batch =
                        RecordBatch::try_new(slot_meta_tile_schema.clone(), arrays).unwrap();
                    app.append_record_batch(batch)?;
                }
            }
        }

        let mut arrays = Vec::<ArrayRef>::new();
        arrays.push(Arc::new(item_uid_builder.finish()));
        arrays.push(Arc::new(interval_builder.finish()));
        arrays.push(Arc::new(title_builder.finish()));
        for builder in &mut slot_builders {
            arrays.push(Arc::new(builder.finish()));
        }
        let batch = RecordBatch::try_new(slot_meta_tile_schema, arrays).unwrap();
        app.append_record_batch(batch)?;

        Ok(())
    }
}

#[derive(Debug, Copy, Clone)]
pub enum FieldType {
    I64,
    U64,
    String,
    Interval,
    ItemLink,
    Vec,
    Empty,
}

impl FieldType {
    pub fn infer_type(value: &data::Field) -> FieldType {
        match value {
            data::Field::I64(..) => FieldType::I64,
            data::Field::U64(..) => FieldType::U64,
            data::Field::String(..) => FieldType::String,
            data::Field::Interval(..) => FieldType::Interval,
            data::Field::ItemLink(..) => FieldType::ItemLink,
            data::Field::Vec(..) => FieldType::Vec,
            data::Field::Empty => FieldType::Empty,
        }
    }

    pub fn meet(self, b: FieldType) -> FieldType {
        match (self, b) {
            // Anything can meet with itself
            (FieldType::I64, FieldType::I64) => FieldType::I64,
            (FieldType::U64, FieldType::U64) => FieldType::U64,
            (FieldType::String, FieldType::String) => FieldType::String,
            (FieldType::Interval, FieldType::Interval) => FieldType::Interval,
            (FieldType::ItemLink, FieldType::ItemLink) => FieldType::ItemLink,
            (FieldType::Vec, FieldType::Vec) => FieldType::Vec,
            (FieldType::Empty, FieldType::Empty) => FieldType::Empty,

            // Allow certain types to upgrade that we know are used together
            (FieldType::U64, FieldType::String) => FieldType::String,
            (FieldType::String, FieldType::U64) => FieldType::String,
            (FieldType::U64, FieldType::ItemLink) => FieldType::ItemLink,
            (FieldType::String, FieldType::ItemLink) => FieldType::ItemLink,
            (FieldType::ItemLink, FieldType::U64) => FieldType::ItemLink,
            (FieldType::ItemLink, FieldType::String) => FieldType::ItemLink,

            _ => panic!("Unknown combination of types: {self:?} and {b:?}"),
        }
    }

    pub fn data_type(self, schema: &ArrowSchema) -> DataType {
        match self {
            FieldType::I64 => DataType::Int64,
            FieldType::U64 => DataType::UInt64,
            FieldType::String => DataType::Utf8,
            FieldType::Interval => schema.interval_data_type.clone(),
            FieldType::ItemLink => schema.item_link_data_type.clone(),
            FieldType::Vec => DataType::Utf8,
            FieldType::Empty => DataType::Boolean,
        }
    }

    pub fn make_builder(self, schema: &ArrowSchema) -> Box<dyn ArrayBuilder> {
        match self {
            FieldType::I64 => Box::new(Int64Builder::new()),
            FieldType::U64 => Box::new(UInt64Builder::new()),
            FieldType::String => Box::new(StringBuilder::new()),
            FieldType::Interval => Box::new(StructBuilder::from_fields(
                schema.interval_fields.clone(),
                ArrowSchema::VECTOR_SIZE,
            )),
            FieldType::ItemLink => Box::new(StructBuilder::from_fields(
                schema.item_link_fields.clone(),
                ArrowSchema::VECTOR_SIZE,
            )),
            FieldType::Vec => Box::new(StringBuilder::new()),
            FieldType::Empty => Box::new(BooleanBuilder::new()),
        }
    }

    fn cast<T: ArrayBuilder>(builder: &mut Box<dyn ArrayBuilder>) -> Result<&mut T, ArrowError> {
        builder.as_any_mut().downcast_mut::<T>().ok_or_else(|| {
            ArrowError::SchemaError("Failed to downcast builder to expected type.".to_string())
        })
    }

    pub fn append_value(
        self,
        builder: &mut Box<dyn ArrayBuilder>,
        value: &data::Field,
    ) -> Result<(), ArrowError> {
        match (self, value) {
            (FieldType::I64, data::Field::I64(x)) => {
                let builder = Self::cast::<Int64Builder>(builder)?;
                builder.append_value(*x);
            }
            (FieldType::U64, data::Field::U64(x)) => {
                let builder = Self::cast::<UInt64Builder>(builder)?;
                builder.append_value(*x);
            }
            (FieldType::String, data::Field::U64(x)) => {
                let builder = Self::cast::<StringBuilder>(builder)?;
                builder.append_value(format!("{}", x));
            }
            (FieldType::String, data::Field::String(x)) => {
                let builder = Self::cast::<StringBuilder>(builder)?;
                builder.append_value(x);
            }
            (FieldType::Interval, data::Field::Interval(x)) => {
                let builder = Self::cast::<StructBuilder>(builder)?;
                Self::append_interval(builder, *x)?;
            }
            (FieldType::ItemLink, data::Field::U64(x)) => {
                let builder = Self::cast::<StructBuilder>(builder)?;
                Self::append_item_link_title(builder, &format!("{}", x))?;
            }
            (FieldType::ItemLink, data::Field::String(x)) => {
                let builder = Self::cast::<StructBuilder>(builder)?;
                Self::append_item_link_title(builder, x)?;
            }
            (FieldType::ItemLink, data::Field::ItemLink(x)) => {
                let builder = Self::cast::<StructBuilder>(builder)?;
                Self::append_item_link(builder, x)?;
            }
            (FieldType::Vec, data::Field::Vec(x)) => {
                let builder = Self::cast::<StringBuilder>(builder)?;
                builder.append_value(format!("{:?}", x));
            }
            (FieldType::Empty, data::Field::Empty) => {
                let builder = Self::cast::<BooleanBuilder>(builder)?;
                builder.append_value(true);
            }
            _ => panic!("Unknown combination of type/value: {self:?} and {value:?}"),
        }
        Ok(())
    }

    pub fn append_null(self, builder: &mut Box<dyn ArrayBuilder>) -> Result<(), ArrowError> {
        match self {
            FieldType::I64 => {
                let builder = Self::cast::<Int64Builder>(builder)?;
                builder.append_null();
            }
            FieldType::U64 => {
                let builder = Self::cast::<UInt64Builder>(builder)?;
                builder.append_null();
            }
            FieldType::String => {
                let builder = Self::cast::<StringBuilder>(builder)?;
                builder.append_null();
            }
            FieldType::Interval => {
                let builder = Self::cast::<StructBuilder>(builder)?;
                builder.append_null();
            }
            FieldType::ItemLink => {
                let builder = Self::cast::<StructBuilder>(builder)?;
                builder.append_null();
            }
            FieldType::Vec => {
                let builder = Self::cast::<StringBuilder>(builder)?;
                builder.append_null();
            }
            FieldType::Empty => {
                let builder = Self::cast::<BooleanBuilder>(builder)?;
                builder.append_null();
            }
        }
        Ok(())
    }

    pub fn append_interval(
        builder: &mut StructBuilder,
        interval: Interval,
    ) -> Result<(), ArrowError> {
        let interval_start_builder = builder
            .field_builder::<Int64Builder>(0).ok_or_else(|| ArrowError::SchemaError("Could not get 'start' field builder for Interval struct. Check field order and type.".to_string()))?;
        interval_start_builder.append_value(interval.start.0);

        let interval_stop_builder = builder
            .field_builder::<Int64Builder>(1).ok_or_else(|| ArrowError::SchemaError("Could not get 'stop' field builder for Interval struct. Check field order and type.".to_string()))?;
        interval_stop_builder.append_value(interval.start.0);

        builder.append(true);

        Ok(())
    }

    pub fn append_item_link(
        builder: &mut StructBuilder,
        link: &ItemLink,
    ) -> Result<(), ArrowError> {
        let item_link_item_uid_builder = builder
            .field_builder::<UInt64Builder>(0).ok_or_else(|| ArrowError::SchemaError("Could not get 'item_uid' field builder for ItemLink struct. Check field order and type.".to_string()))?;
        item_link_item_uid_builder.append_value(link.item_uid.0);

        let item_link_title_builder = builder
            .field_builder::<StringBuilder>(1).ok_or_else(|| ArrowError::SchemaError("Could not get 'title' field builder for ItemLink struct. Check field order and type.".to_string()))?;
        item_link_title_builder.append_value(&link.title);

        let item_link_interval_builder = builder
            .field_builder::<StructBuilder>(2).ok_or_else(|| ArrowError::SchemaError("Could not get 'interval' field builder for ItemLink struct. Check field order and type.".to_string()))?;
        Self::append_interval(item_link_interval_builder, link.interval)?;

        let item_link_entry_slug_builder = builder
            .field_builder::<StringBuilder>(3).ok_or_else(|| ArrowError::SchemaError("Could not get 'entry_slug' field builder for ItemLink struct. Check field order and type.".to_string()))?;
        item_link_entry_slug_builder.append_null(); // TODO

        builder.append(true);

        Ok(())
    }

    pub fn append_item_link_title(
        builder: &mut StructBuilder,
        title: &str,
    ) -> Result<(), ArrowError> {
        let item_link_item_uid_builder = builder
            .field_builder::<UInt64Builder>(0).ok_or_else(|| ArrowError::SchemaError("Could not get 'item_uid' field builder for ItemLink struct. Check field order and type.".to_string()))?;
        item_link_item_uid_builder.append_null();

        let item_link_title_builder = builder
            .field_builder::<StringBuilder>(1).ok_or_else(|| ArrowError::SchemaError("Could not get 'title' field builder for ItemLink struct. Check field order and type.".to_string()))?;
        item_link_title_builder.append_value(title);

        let item_link_interval_builder = builder
            .field_builder::<StructBuilder>(2).ok_or_else(|| ArrowError::SchemaError("Could not get 'interval' field builder for ItemLink struct. Check field order and type.".to_string()))?;
        item_link_interval_builder.append_null();

        let item_link_entry_slug_builder = builder
            .field_builder::<StringBuilder>(3).ok_or_else(|| ArrowError::SchemaError("Could not get 'entry_slug' field builder for ItemLink struct. Check field order and type.".to_string()))?;
        item_link_entry_slug_builder.append_null();

        builder.append(true);

        Ok(())
    }
}

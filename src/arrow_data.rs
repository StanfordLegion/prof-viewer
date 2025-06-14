use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, LazyLock, Mutex};

use duckdb::arrow::{
    array::{
        ArrayBuilder, ArrayRef, BooleanBuilder, Int64Builder, ListBuilder, StringBuilder,
        StructBuilder, UInt64Builder,
    },
    datatypes::{DataType, Field, Fields, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use log::warn;

use crate::data::{
    self, DataSourceDescription, DataSourceInfo, EntryID, FieldID, ItemField, ItemLink,
    SlotMetaTile,
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
        // Some versions of Legion generate negative intervals, so we have to represent this as signed
        let interval_field_duration = Field::new("duration", DataType::Int64, false);
        let interval_fields = Fields::from(vec![
            interval_field_start.clone(),
            interval_field_stop.clone(),
            interval_field_duration.clone(),
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

        source_locator_builder.append_value(desc.source_locator.iter().map(Some));

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
        entry_id_slug: &str,
        entry_id_slugs: &BTreeMap<EntryID, String>,
        tile: &SlotMetaTile,
        field_slots: &BTreeMap<FieldID, usize>,
        slot_fields: &[(String, FieldType)],
    ) -> duckdb::Result<()> {
        // The schema varies dynamically depending on what fields we get,
        // so don't bother trying to pre-generate this.
        let mut schema_fields = vec![
            Field::new("entry_id_slug", DataType::Utf8, false),
            Field::new("item_uid", DataType::UInt64, false),
            Field::new("title", DataType::Utf8, false),
        ];
        for (field_name, field_type) in slot_fields {
            schema_fields.push(Field::new(field_name, field_type.data_type(self), true));
        }
        let slot_meta_tile_schema = Arc::new(Schema::new(schema_fields));

        let mut entry_id_slug_builder = StringBuilder::new();
        let mut item_uid_builder = UInt64Builder::new();
        let mut title_builder = StringBuilder::new();

        let mut slot_builders: Vec<_> = slot_fields
            .iter()
            .map(|(_, field_type)| field_type.make_builder(self))
            .collect();

        let mut slot_present = Vec::new();
        let mut slot_duplicate = Vec::new();
        slot_present.resize(slot_fields.len(), false);
        slot_duplicate.resize(slot_fields.len(), false);
        for row in &tile.data.items {
            for item in row {
                entry_id_slug_builder.append_value(entry_id_slug);
                item_uid_builder.append_value(item.item_uid.0);
                title_builder.append_value(&item.title);

                slot_present.clear();
                slot_present.resize(slot_fields.len(), false);
                for ItemField(field_id, field, _) in &item.fields {
                    let slot = *field_slots.get(field_id).unwrap();
                    if slot_present[slot] {
                        slot_duplicate[slot] = true;
                        continue;
                    }
                    let (_, field_type) = &slot_fields[slot];
                    let builder = &mut slot_builders[slot];
                    field_type
                        .append_value(builder, field, entry_id_slugs)
                        .unwrap();
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
                    let mut arrays: Vec<ArrayRef> = vec![
                        Arc::new(entry_id_slug_builder.finish()),
                        Arc::new(item_uid_builder.finish()),
                        Arc::new(title_builder.finish()),
                    ];
                    for builder in &mut slot_builders {
                        arrays.push(Arc::new(builder.finish()));
                    }
                    let batch =
                        RecordBatch::try_new(slot_meta_tile_schema.clone(), arrays).unwrap();
                    app.append_record_batch(batch)?;
                }
            }
        }

        let mut arrays: Vec<ArrayRef> = vec![
            Arc::new(entry_id_slug_builder.finish()),
            Arc::new(item_uid_builder.finish()),
            Arc::new(title_builder.finish()),
        ];
        for builder in &mut slot_builders {
            arrays.push(Arc::new(builder.finish()));
        }
        let batch = RecordBatch::try_new(slot_meta_tile_schema, arrays).unwrap();
        app.append_record_batch(batch)?;

        static DUPLICATE_WARNINGS: LazyLock<Mutex<BTreeSet<String>>> =
            LazyLock::new(|| Mutex::new(BTreeSet::new()));
        for (slot, duplicate) in slot_duplicate.iter().enumerate() {
            if *duplicate {
                let (field_name, _) = &slot_fields[slot];
                let mut warnings = DUPLICATE_WARNINGS.lock().unwrap();
                if !warnings.contains(field_name) {
                    warn!("Skipping one or more duplicate entries for field {field_name}");
                    warnings.insert(field_name.to_string());
                }
            }
        }

        Ok(())
    }
}

impl Default for ArrowSchema {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum FieldType {
    I64,
    U64,
    String,
    Interval,
    ItemLink,
    Vec(Box<FieldType>),
    Empty,
    Unknown,
}

impl FieldType {
    pub fn infer_type(value: &data::Field) -> FieldType {
        match value {
            data::Field::I64(..) => FieldType::I64,
            data::Field::U64(..) => FieldType::U64,
            data::Field::String(..) => FieldType::String,
            data::Field::Interval(..) => FieldType::Interval,
            data::Field::ItemLink(..) => FieldType::ItemLink,
            data::Field::Vec(v) => FieldType::Vec(Box::new(
                v.iter()
                    .map(Self::infer_type)
                    .reduce(|x, y| x.meet(&y))
                    .unwrap_or(FieldType::Unknown),
            )),
            data::Field::Empty => FieldType::Empty,
        }
    }

    pub fn meet(&self, b: &FieldType) -> FieldType {
        match (self, b) {
            // Anything can meet with itself
            (FieldType::I64, FieldType::I64) => FieldType::I64,
            (FieldType::U64, FieldType::U64) => FieldType::U64,
            (FieldType::String, FieldType::String) => FieldType::String,
            (FieldType::Interval, FieldType::Interval) => FieldType::Interval,
            (FieldType::ItemLink, FieldType::ItemLink) => FieldType::ItemLink,
            (FieldType::Vec(va), FieldType::Vec(vb)) => FieldType::Vec(Box::new(va.meet(vb))),
            (FieldType::Empty, FieldType::Empty) => FieldType::Empty,
            (FieldType::Unknown, x) => x.clone(),
            (x, FieldType::Unknown) => x.clone(),

            // Allow certain types to upgrade that we know are used together

            // Strings, integers upgrade to ItemLink
            (FieldType::U64, FieldType::String) => FieldType::String,
            (FieldType::String, FieldType::U64) => FieldType::String,
            (FieldType::U64, FieldType::ItemLink) => FieldType::ItemLink,
            (FieldType::String, FieldType::ItemLink) => FieldType::ItemLink,
            (FieldType::ItemLink, FieldType::U64) => FieldType::ItemLink,
            (FieldType::ItemLink, FieldType::String) => FieldType::ItemLink,

            _ => panic!("Unknown combination of types: {self:?} and {b:?}"),
        }
    }

    pub fn data_type(&self, schema: &ArrowSchema) -> DataType {
        match self {
            FieldType::I64 => DataType::Int64,
            FieldType::U64 => DataType::UInt64,
            FieldType::String => DataType::Utf8,
            FieldType::Interval => schema.interval_data_type.clone(),
            FieldType::ItemLink => schema.item_link_data_type.clone(),
            FieldType::Vec(v) => {
                let field = match &**v {
                    FieldType::Unknown => DataType::Utf8,
                    v => v.data_type(schema),
                };
                DataType::List(Arc::new(Field::new_list_field(field, true)))
            }
            FieldType::Empty => DataType::Boolean,
            FieldType::Unknown => panic!("cannot write unknown type"),
        }
    }

    pub fn make_builder(&self, schema: &ArrowSchema) -> Box<dyn ArrayBuilder> {
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
            FieldType::Vec(v) => {
                let field: Box<dyn ArrayBuilder> = match &**v {
                    FieldType::Unknown => Box::new(StringBuilder::new()),
                    v => Box::new(v.make_builder(schema)),
                };
                Box::new(ListBuilder::new(field))
            }
            FieldType::Empty => Box::new(BooleanBuilder::new()),
            FieldType::Unknown => panic!("cannot write unknown type"),
        }
    }

    fn cast<T: ArrayBuilder>(builder: &mut Box<dyn ArrayBuilder>) -> Result<&mut T, ArrowError> {
        builder.as_any_mut().downcast_mut::<T>().ok_or_else(|| {
            ArrowError::SchemaError("Failed to downcast builder to expected type.".to_string())
        })
    }

    fn cast_field<'a, T: ArrayBuilder>(
        builder: &'a mut StructBuilder,
        field_idx: usize,
        field_name: &str,
        struct_name: &str,
    ) -> Result<&'a mut T, ArrowError> {
        builder.field_builder::<T>(field_idx).ok_or_else(|| {
            ArrowError::SchemaError(format!("Could not get '{field_name}' field builder for {struct_name} struct. Check field order and type."))
        })
    }

    pub fn append_value(
        &self,
        builder: &mut Box<dyn ArrayBuilder>,
        value: &data::Field,
        entry_id_slugs: &BTreeMap<EntryID, String>,
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
                Self::append_item_link(builder, x, entry_id_slugs)?;
            }
            (FieldType::Vec(v), data::Field::Vec(xs)) => {
                let builder = Self::cast::<ListBuilder<Box<dyn ArrayBuilder>>>(builder)?;
                for x in xs {
                    v.append_value(builder.values(), x, entry_id_slugs)?;
                }
                builder.append(true);
            }
            (FieldType::Empty, data::Field::Empty) => {
                let builder = Self::cast::<BooleanBuilder>(builder)?;
                builder.append_value(true);
            }
            (FieldType::Unknown, _) => panic!("cannot write unknown type"),
            _ => panic!("Unknown combination of type/value: {self:?} and {value:?}"),
        }
        Ok(())
    }

    pub fn append_null(&self, builder: &mut Box<dyn ArrayBuilder>) -> Result<(), ArrowError> {
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
                Self::append_interval_null(builder)?;
            }
            FieldType::ItemLink => {
                let builder = Self::cast::<StructBuilder>(builder)?;
                Self::append_item_link_null(builder)?;
            }
            FieldType::Vec(_) => {
                let builder = Self::cast::<ListBuilder<Box<dyn ArrayBuilder>>>(builder)?;
                builder.append_null();
            }
            FieldType::Empty => {
                let builder = Self::cast::<BooleanBuilder>(builder)?;
                builder.append_null();
            }
            FieldType::Unknown => panic!("cannot write unknown type"),
        }
        Ok(())
    }

    pub fn append_interval(
        builder: &mut StructBuilder,
        interval: Interval,
    ) -> Result<(), ArrowError> {
        Self::cast_field::<Int64Builder>(builder, 0, "start", "Interval")?
            .append_value(interval.start.0);

        Self::cast_field::<Int64Builder>(builder, 1, "stop", "Interval")?
            .append_value(interval.stop.0);

        let duration = interval.stop.0 - interval.start.0;
        Self::cast_field::<Int64Builder>(builder, 2, "duration", "Interval")?
            .append_value(duration);

        builder.append(true);

        Ok(())
    }

    pub fn append_interval_null(builder: &mut StructBuilder) -> Result<(), ArrowError> {
        Self::cast_field::<Int64Builder>(builder, 0, "start", "Interval")?.append_null();

        Self::cast_field::<Int64Builder>(builder, 1, "stop", "Interval")?.append_null();

        Self::cast_field::<Int64Builder>(builder, 2, "duration", "Interval")?.append_null();

        builder.append(false);

        Ok(())
    }

    pub fn append_item_link(
        builder: &mut StructBuilder,
        link: &ItemLink,
        entry_id_slugs: &BTreeMap<EntryID, String>,
    ) -> Result<(), ArrowError> {
        Self::cast_field::<UInt64Builder>(builder, 0, "item_uid", "ItemLink")?
            .append_value(link.item_uid.0);

        Self::cast_field::<StringBuilder>(builder, 1, "title", "ItemLink")?
            .append_value(&link.title);

        let item_link_interval_builder =
            Self::cast_field::<StructBuilder>(builder, 2, "interval", "Interval")?;
        Self::append_interval(item_link_interval_builder, link.interval)?;

        Self::cast_field::<StringBuilder>(builder, 3, "entry_slug", "ItemLink")?
            .append_value(entry_id_slugs.get(&link.entry_id).unwrap());

        builder.append(true);

        Ok(())
    }

    pub fn append_item_link_title(
        builder: &mut StructBuilder,
        title: &str,
    ) -> Result<(), ArrowError> {
        Self::cast_field::<UInt64Builder>(builder, 0, "item_uid", "ItemLink")?.append_null();

        Self::cast_field::<StringBuilder>(builder, 1, "title", "ItemLink")?.append_value(title);

        let item_link_interval_builder =
            Self::cast_field::<StructBuilder>(builder, 2, "interval", "Interval")?;
        Self::append_interval_null(item_link_interval_builder)?;

        Self::cast_field::<StringBuilder>(builder, 3, "entry_slug", "ItemLink")?.append_null();

        builder.append(true);

        Ok(())
    }

    pub fn append_item_link_null(builder: &mut StructBuilder) -> Result<(), ArrowError> {
        Self::cast_field::<UInt64Builder>(builder, 0, "item_uid", "ItemLink")?.append_null();

        Self::cast_field::<StringBuilder>(builder, 1, "title", "ItemLink")?.append_null();

        let item_link_interval_builder =
            Self::cast_field::<StructBuilder>(builder, 2, "interval", "Interval")?;
        Self::append_interval_null(item_link_interval_builder)?;

        Self::cast_field::<StringBuilder>(builder, 3, "entry_slug", "ItemLink")?.append_null();

        builder.append(false);

        Ok(())
    }
}

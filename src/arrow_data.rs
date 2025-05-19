use std::collections::BTreeMap;
use std::sync::Arc;

use duckdb::arrow::{
    array::{
        ArrayBuilder, ArrayRef, Int64Builder, ListBuilder, StringBuilder, StructBuilder,
        UInt64Builder,
    },
    datatypes::{DataType, Field, Fields, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};

use crate::data::{self, DataSourceDescription, DataSourceInfo, FieldID, SlotMetaTile};
use crate::timestamp::Interval;

pub struct ArrowSchema {
    interval_fields: Fields,
    interval_data_type: DataType,
    source_locator_item_field: Arc<Field>,
    info_schema: Arc<Schema>,
}

impl ArrowSchema {
    pub fn new() -> Self {
        let interval_field_start = Field::new("start", DataType::Int64, false);
        let interval_field_stop = Field::new("stop", DataType::Int64, false);
        let interval_fields = Fields::from(vec![
            interval_field_start.clone(),
            interval_field_stop.clone(),
        ]);
        let interval_data_type = DataType::Struct(interval_fields.clone());

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
            source_locator_item_field,
            info_schema,
        }
    }

    fn append_interval(builder: &mut StructBuilder, interval: Interval) -> Result<(), ArrowError> {
        let interval_start_builder = builder
            .field_builder::<Int64Builder>(0).ok_or_else(|| ArrowError::SchemaError("Could not get 'start' field builder for Interval struct. Check field order and type.".to_string()))?;
        interval_start_builder.append_value(interval.start.0);

        let interval_stop_builder = builder
            .field_builder::<Int64Builder>(1).ok_or_else(|| ArrowError::SchemaError("Could not get 'stop' field builder for Interval struct. Check field order and type.".to_string()))?;
        interval_stop_builder.append_value(interval.start.0);

        builder.append(true);

        Ok(())
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

        Self::append_interval(&mut interval_builder, info.interval).unwrap();

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
        field_map: &BTreeMap<FieldID, (usize, FieldType)>,
    ) -> duckdb::Result<()> {
        // Chunk this to work around: https://github.com/duckdb/duckdb-rs/issues/503
        const VECTOR_SIZE: usize = 2048;

        // The schema varies dynamically depending on what fields we get,
        // so don't bother trying to pre-generate this.
        let slot_meta_tile_schema = Arc::new(Schema::new(vec![
            Field::new("item_uid", DataType::UInt64, false),
            Field::new("interval", self.interval_data_type.clone(), false),
            Field::new("title", DataType::Utf8, false),
        ]));

        let mut item_uid_builder = UInt64Builder::new();
        let mut interval_builder =
            StructBuilder::from_fields(self.interval_fields.clone(), VECTOR_SIZE);
        let mut title_builder = StringBuilder::new();

        for row in &tile.data.items {
            for item in row {
                item_uid_builder.append_value(item.item_uid.0);
                Self::append_interval(&mut interval_builder, item.original_interval).unwrap();
                title_builder.append_value(&item.title);

                if item_uid_builder.len() >= VECTOR_SIZE {
                    let item_uid_array: ArrayRef = Arc::new(item_uid_builder.finish());
                    let interval_array: ArrayRef = Arc::new(interval_builder.finish());
                    let title_array: ArrayRef = Arc::new(title_builder.finish());

                    let batch = RecordBatch::try_new(
                        slot_meta_tile_schema.clone(),
                        vec![item_uid_array, interval_array, title_array],
                    )
                    .unwrap();
                    app.append_record_batch(batch)?;
                }
            }
        }

        let item_uid_array: ArrayRef = Arc::new(item_uid_builder.finish());
        let interval_array: ArrayRef = Arc::new(interval_builder.finish());
        let title_array: ArrayRef = Arc::new(title_builder.finish());

        let batch = RecordBatch::try_new(
            slot_meta_tile_schema,
            vec![item_uid_array, interval_array, title_array],
        )
        .unwrap();
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
    Vec,
    Empty,
}

impl FieldType {
    pub fn infer_type(field: &data::Field) -> Self {
        match field {
            data::Field::I64(..) => FieldType::I64,
            data::Field::U64(..) => FieldType::U64,
            data::Field::String(..) => FieldType::String,
            data::Field::Interval(..) => FieldType::Interval,
            data::Field::ItemLink(..) => FieldType::String, // for now map to string
            data::Field::Vec(..) => FieldType::Vec,
            data::Field::Empty => FieldType::Empty,
        }
    }
}

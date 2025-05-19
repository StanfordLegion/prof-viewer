use std::sync::Arc;

use duckdb::arrow::{
    array::{ArrayRef, Int64Builder, ListBuilder, StringBuilder, StructBuilder},
    datatypes::{DataType, Field, Fields, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};

use crate::data::{DataSourceDescription, DataSourceInfo};

pub struct ArrowSchema {
    source_locator_item_field: Arc<Field>,
    interval_fields: Fields,
    interval_data_type: DataType,
    info_schema: Arc<Schema>,
}

impl ArrowSchema {
    pub fn new() -> Self {
        let source_locator_item_field = Arc::new(Field::new_list_field(DataType::Utf8, false));

        let interval_field_start = Field::new("start", DataType::Int64, false);
        let interval_field_stop = Field::new("stop", DataType::Int64, false);
        let interval_fields = Fields::from(vec![
            interval_field_start.clone(),
            interval_field_stop.clone(),
        ]);
        let interval_data_type = DataType::Struct(interval_fields.clone());

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
            source_locator_item_field,
            interval_fields,
            interval_data_type,
            info_schema,
        }
    }
}

pub fn info_to_record_batch(
    desc: &DataSourceDescription,
    info: &DataSourceInfo,
    schema: &ArrowSchema,
) -> Result<RecordBatch, ArrowError> {
    let num_rows = 1;

    let mut source_locator_builder =
        ListBuilder::new(StringBuilder::new()).with_field(schema.source_locator_item_field.clone());
    let mut interval_builder = StructBuilder::from_fields(schema.interval_fields.clone(), num_rows);
    let mut warning_message_builder = StringBuilder::new();

    source_locator_builder.append_value(desc.source_locator.iter().map(|x| Some(x)));

    {
        let interval_start_builder = interval_builder
            .field_builder::<Int64Builder>(0).ok_or_else(|| ArrowError::SchemaError("Could not get 'start' field builder for Interval struct. Check field order and type.".to_string()))?;
        interval_start_builder.append_value(info.interval.start.0);

        let interval_stop_builder = interval_builder
            .field_builder::<Int64Builder>(1).ok_or_else(|| ArrowError::SchemaError("Could not get 'stop' field builder for Interval struct. Check field order and type.".to_string()))?;
        interval_stop_builder.append_value(info.interval.start.0);

        interval_builder.append(true);
    }

    warning_message_builder.append_option(info.warning_message.as_ref());

    let source_locator_array: ArrayRef = Arc::new(source_locator_builder.finish());
    let interval_array: ArrayRef = Arc::new(interval_builder.finish());
    let warning_message_array: ArrayRef = Arc::new(warning_message_builder.finish());

    RecordBatch::try_new(
        schema.info_schema.clone(),
        vec![source_locator_array, interval_array, warning_message_array],
    )
}

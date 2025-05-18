use std::sync::Arc;

use duckdb::arrow::{
    array::{ArrayRef, Int64Builder, StringBuilder, StructBuilder},
    datatypes::{DataType, Field, Fields, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};

use crate::data::DataSourceInfo;

pub struct ArrowSchema {
    interval_fields: Fields,
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

        let info_schema = Arc::new(Schema::new(vec![
            Field::new("interval", interval_data_type, false),
            Field::new("warning_message", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, false),
        ]));

        Self {
            interval_fields,
            info_schema,
        }
    }
}

pub fn info_to_record_batch(
    info: &DataSourceInfo,
    description: &str,
    schema: &ArrowSchema,
) -> Result<RecordBatch, ArrowError> {
    let num_rows = 1;

    let mut interval_builder = StructBuilder::from_fields(schema.interval_fields.clone(), num_rows);
    let mut warning_message_builder = StringBuilder::new();
    let mut description_builder = StringBuilder::new();

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
    description_builder.append_value(description);

    let warning_message_array: ArrayRef = Arc::new(warning_message_builder.finish());
    let description_array: ArrayRef = Arc::new(description_builder.finish());
    let interval_array: ArrayRef = Arc::new(interval_builder.finish());

    RecordBatch::try_new(
        schema.info_schema.clone(),
        vec![interval_array, warning_message_array, description_array],
    )
}

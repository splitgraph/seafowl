use arrow_schema::{DataType, TimeUnit, DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE};
use sqlparser::ast::{
    ColumnDef as SQLColumnDef, ColumnOption, DataType as SQLDataType, ExactNumberInfo,
    Ident, TimezoneInfo,
};

use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::config::ConfigOptions;
pub use datafusion::error::{DataFusionError as Error, Result};

// Normalize an identifier to a lowercase string unless the identifier is quoted.
pub(crate) fn normalize_ident(id: &Ident) -> String {
    match id.quote_style {
        Some(_) => id.value.clone(),
        None => id.value.to_ascii_lowercase(),
    }
}

// Copied from SqlRel (private there)
pub(crate) fn build_schema(columns: Vec<SQLColumnDef>) -> Result<Schema> {
    let mut fields = Vec::with_capacity(columns.len());

    for column in columns {
        let data_type = convert_simple_data_type(&column.data_type)?;

        // Modified from DataFusion to default to nullable
        let allow_null = !column
            .options
            .iter()
            .any(|x| x.option == ColumnOption::NotNull);
        fields.push(Field::new(
            &normalize_ident(&column.name),
            data_type,
            allow_null,
        ));
    }

    Ok(Schema::new(fields))
}

// Copied from SqlRel (private there since 15.0.0)
// NB: We don't handle SQLDataType::Timestamp(None, tz_info) with timezone as in DataFusion since we
// simply use the default `time_zone` config value.
pub(crate) fn convert_simple_data_type(sql_type: &SQLDataType) -> Result<DataType> {
    match sql_type {
        SQLDataType::Boolean => Ok(DataType::Boolean),
        SQLDataType::TinyInt(_) => Ok(DataType::Int8),
        SQLDataType::SmallInt(_) => Ok(DataType::Int16),
        SQLDataType::Int(_) | SQLDataType::Integer(_) => Ok(DataType::Int32),
        SQLDataType::BigInt(_) => Ok(DataType::Int64),
        SQLDataType::UnsignedTinyInt(_) => Ok(DataType::UInt8),
        SQLDataType::UnsignedSmallInt(_) => Ok(DataType::UInt16),
        SQLDataType::UnsignedInt(_) | SQLDataType::UnsignedInteger(_) => {
            Ok(DataType::UInt32)
        }
        SQLDataType::UnsignedBigInt(_) => Ok(DataType::UInt64),
        SQLDataType::Float(_) => Ok(DataType::Float32),
        SQLDataType::Real => Ok(DataType::Float32),
        SQLDataType::Double | SQLDataType::DoublePrecision => Ok(DataType::Float64),
        SQLDataType::Char(_)
        | SQLDataType::Varchar(_)
        | SQLDataType::Text
        | SQLDataType::String => Ok(DataType::Utf8),
        SQLDataType::Timestamp(None, tz_info) => {
            let tz = if matches!(tz_info, TimezoneInfo::Tz)
                || matches!(tz_info, TimezoneInfo::WithTimeZone)
            {
                // Timestamp With Time Zone
                // INPUT : [SQLDataType]   TimestampTz + [RuntimeConfig] Time Zone
                // OUTPUT: [ArrowDataType] Timestamp<TimeUnit, Some(Time Zone)>
                ConfigOptions::default().execution.time_zone
            } else {
                // Timestamp Without Time zone
                None
            };
            Ok(DataType::Timestamp(TimeUnit::Microsecond, tz.map(Into::into)))
        }
        SQLDataType::Date => Ok(DataType::Date32),
        SQLDataType::Time(None, tz_info) => {
            if matches!(tz_info, TimezoneInfo::None)
                || matches!(tz_info, TimezoneInfo::WithoutTimeZone)
            {
                Ok(DataType::Time64(TimeUnit::Nanosecond))
            } else {
                // We don't support TIMETZ and TIME WITH TIME ZONE for now.
                Err(Error::NotImplemented(format!(
                    "Unsupported SQL type {sql_type:?}"
                )))
            }
        }
        SQLDataType::Numeric(exact_number_info)
        |SQLDataType::Decimal(exact_number_info) => {
            let (precision, scale) = match *exact_number_info {
                ExactNumberInfo::None => (None, None),
                ExactNumberInfo::Precision(precision) => (Some(precision), None),
                ExactNumberInfo::PrecisionAndScale(precision, scale) => {
                    (Some(precision), Some(scale))
                }
            };
            make_decimal_type(precision, scale)
        }
        SQLDataType::Bytea => Ok(DataType::Binary),
        // Explicitly list all other types so that if sqlparser
        // adds/changes the `SQLDataType` the compiler will tell us on upgrade
        // and avoid bugs like https://github.com/apache/arrow-datafusion/issues/3059
        SQLDataType::Nvarchar(_)
        | SQLDataType::Uuid
        | SQLDataType::Binary(_)
        | SQLDataType::BigNumeric(_)
        | SQLDataType::BigDecimal(_)
        | SQLDataType::Varbinary(_)
        | SQLDataType::Blob(_)
        | SQLDataType::Datetime(_)
        | SQLDataType::Interval
        | SQLDataType::JSON
        | SQLDataType::Regclass
        | SQLDataType::Custom(_, _)
        | SQLDataType::Array(_)
        | SQLDataType::Enum(_)
        | SQLDataType::Set(_)
        | SQLDataType::MediumInt(_)
        | SQLDataType::UnsignedMediumInt(_)
        | SQLDataType::Character(_)
        | SQLDataType::CharacterVarying(_)
        | SQLDataType::CharVarying(_)
        | SQLDataType::CharacterLargeObject(_)
        | SQLDataType::CharLargeObject(_)
        // precision is not supported
        | SQLDataType::Timestamp(Some(_), _)
        // precision is not supported
        | SQLDataType::Time(Some(_), _)
        | SQLDataType::Dec(_)
        | SQLDataType::Clob(_) => Err(Error::NotImplemented(format!(
            "Unsupported SQL type {sql_type:?}"
        ))),
    }
}

/// Returns a validated `DataType` for the specified precision and
/// scale
pub(crate) fn make_decimal_type(
    precision: Option<u64>,
    scale: Option<u64>,
) -> Result<DataType> {
    // postgres like behavior
    let (precision, scale) = match (precision, scale) {
        (Some(p), Some(s)) => (p as u8, s as i8),
        (Some(p), None) => (p as u8, 0),
        (None, Some(_)) => {
            return Err(Error::Internal(
                "Cannot specify only scale for decimal data type".to_string(),
            ))
        }
        (None, None) => (DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE),
    };

    // Arrow decimal is i128 meaning 38 maximum decimal digits
    if precision == 0
        || precision > DECIMAL128_MAX_PRECISION
        || scale.unsigned_abs() > precision
    {
        Err(Error::Internal(format!(
            "Decimal(precision = {precision}, scale = {scale}) should satisfy `0 < precision <= 38`, and `scale <= precision`."
        )))
    } else {
        Ok(DataType::Decimal128(precision, scale))
    }
}

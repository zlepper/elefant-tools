use crate::pg_interval::Interval;
use crate::postgres_client_wrapper::FromRow;
use crate::schema_reader::define_working_query;
use tokio_postgres::Row;

pub struct TimescaleHypertableDimensionResult {
    pub table_schema: String,
    pub table_name: String,
    pub dimension_number: i64,
    pub column_name: String,
    pub time_interval: Option<Interval>,
    pub integer_interval: Option<i64>,
    pub num_partitions: Option<i16>,
}

impl FromRow for TimescaleHypertableDimensionResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(TimescaleHypertableDimensionResult {
            table_schema: row.try_get(0)?,
            table_name: row.try_get(1)?,
            dimension_number: row.try_get(2)?,
            column_name: row.try_get(3)?,
            time_interval: row.try_get(4)?,
            integer_interval: row.try_get(5)?,
            num_partitions: row.try_get(6)?,
        })
    }
}

//language=postgresql
define_working_query!(
    get_hypertable_dimensions,
    TimescaleHypertableDimensionResult,
    r#"
select h.hypertable_schema,
       h.hypertable_name,
       h.dimension_number,
       h.column_name,
       h.time_interval,
       h.integer_interval,
       h.num_partitions
from timescaledb_information.dimensions h
order by h.hypertable_schema, h.hypertable_name, h.dimension_number
"#
);

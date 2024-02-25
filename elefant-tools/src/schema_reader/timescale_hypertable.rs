use tokio_postgres::Row;
use crate::postgres_client_wrapper::FromRow;
use crate::schema_reader::define_working_query;

pub struct HypertableResult {
    pub table_schema: String,
    pub table_name: String,
    pub number_of_dimensions: i16,
    pub compression_enabled: bool,
}

impl FromRow for HypertableResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(HypertableResult {
            table_schema: row.try_get(0)?,
            table_name: row.try_get(1)?,
            number_of_dimensions: row.try_get(2)?,
            compression_enabled: row.try_get(3)?,
        })
    }
}

//language=postgresql
define_working_query!(get_hypertables, HypertableResult, r#"
select h.hypertable_schema,
         h.hypertable_name,
         h.num_dimensions,
         h.compression_enabled
from timescaledb_information.hypertables h
"#);

use tokio_postgres::Row;
use crate::postgres_client_wrapper::FromRow;
use crate::schema_reader::define_working_query;

pub struct SequenceResult {
    pub schema_name: String,
    pub sequence_name: String,
    pub data_type: String,
    pub start_value: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub increment_by: i64,
    pub cycle: bool,
    pub cache_size: i64,
    pub last_value: Option<i64>,
}

impl FromRow for SequenceResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(Self {
            schema_name: row.try_get(0)?,
            sequence_name: row.try_get(1)?,
            data_type: row.try_get(2)?,
            start_value: row.try_get(3)?,
            min_value: row.try_get(4)?,
            max_value: row.try_get(5)?,
            increment_by: row.try_get(6)?,
            cycle: row.try_get(7)?,
            cache_size: row.try_get(8)?,
            last_value: row.try_get(9)?,
        })
    }
}

//language=postgresql
define_working_query!(get_sequences, SequenceResult, r#"
select s.schemaname,
       s.sequencename,
       s.data_type::text,
       s.start_value,
       s.min_value,
       s.max_value,
       s.increment_by,
       s.cycle,
       s.cache_size,
       s.last_value
from pg_sequences s
where s.schemaname not in ('pg_catalog', 'pg_toast', 'information_schema')
order by s.schemaname, s.sequencename;
"#);
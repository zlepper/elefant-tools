use tokio_postgres::Row;
use crate::postgres_client_wrapper::FromRow;
use super::{define_working_query};

#[derive(Debug, Eq, PartialEq)]
pub struct TablesResult {
    pub schema_name: String,
    pub table_name: String,
    pub comment: Option<String>,
}

impl FromRow for TablesResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(TablesResult {
            schema_name: row.try_get(0)?,
            table_name: row.try_get(1)?,
            comment: row.try_get(2)?,
        })
    }
}


//language=postgresql
define_working_query!(get_tables, TablesResult, r#"
select ns.nspname, cl.relname, des.description
from pg_class cl
         join pg_catalog.pg_namespace ns on ns.oid = cl.relnamespace
         left join pg_description des on des.objoid = cl.oid and des.objsubid = 0
where cl.relkind = 'r'
  and cl.oid > 16384
order by ns.nspname, cl.relname;
"#);
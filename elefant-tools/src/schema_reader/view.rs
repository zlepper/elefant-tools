use tokio_postgres::Row;
use crate::postgres_client_wrapper::FromRow;
use crate::schema_reader::define_working_query;

pub struct ViewResult {
    pub view_name: String,
    pub schema_name: String,
    pub definition: String,
}

impl FromRow for ViewResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(Self {
            view_name: row.try_get(0)?,
            schema_name: row.try_get(1)?,
            definition: row.try_get(2)?,
        })
    }
}


//language=postgresql
define_working_query!(get_views, ViewResult, r#"
select tab.relname                   as view_name,
       ns.nspname                    as schema_name,
       pg_get_viewdef(tab.oid, true) as def
from pg_class tab
         join pg_namespace ns on tab.relnamespace = ns.oid
where tab.oid > 16384
  and tab.relkind = 'v';

"#);

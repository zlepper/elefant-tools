use tokio_postgres::Row;
use crate::postgres_client_wrapper::FromRow;
use crate::schema_reader::define_working_query;

pub struct ViewColumnResult {
    pub view_name: String,
    pub schema_name: String,
    pub column_name: String,
    pub ordinal_position: i32,
    pub comment: Option<String>,
}

impl FromRow for ViewColumnResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(Self {
            view_name: row.try_get(0)?,
            schema_name: row.try_get(1)?,
            column_name: row.try_get(2)?,
            ordinal_position: row.try_get(3)?,
            comment: row.try_get(4)?,
        })
    }
}


//language=postgresql
define_working_query!(get_view_columns, ViewColumnResult, r#"
select tab.relname  as view_name,
       ns.nspname   as schema_name,
       attr.attname as column_name,
       attr.attnum::int4  as ordinal_position,
         d.description as comment
from pg_class tab
         join pg_namespace ns on tab.relnamespace = ns.oid
         join pg_attribute attr on attr.attrelid = tab.oid
         left join pg_description d on d.objoid = attr.attrelid and d.objsubid = attr.attnum
where tab.oid > 16384
  and tab.relkind = 'v';
"#);

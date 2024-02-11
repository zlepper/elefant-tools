use tokio_postgres::Row;
use crate::postgres_client_wrapper::FromRow;
use crate::schema_reader::define_working_query;

pub struct IndexColumnResult {
    pub table_schema: String,
    pub table_name: String,
    pub index_name: String,
    pub is_key: bool,
    pub column_expression: String,
    pub is_desc: Option<bool>,
    pub nulls_first: Option<bool>,
    pub ordinal_position: i32,
}

impl FromRow for IndexColumnResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(IndexColumnResult {
            table_schema: row.try_get(0)?,
            table_name: row.try_get(1)?,
            index_name: row.try_get(2)?,
            is_key: row.try_get(3)?,
            column_expression: row.try_get(4)?,
            is_desc: row.try_get(5)?,
            nulls_first: row.try_get(6)?,
            ordinal_position: row.try_get(7)?,
        })
    }
}

//language=postgresql
define_working_query!(get_index_columns, IndexColumnResult, r#"
select n.nspname                                              as table_schema,
      table_class.relname                                    as table_name,
      index_class.relname                                    as index_name,
      a.attnum <= i.indnkeyatts                              as is_key,
      pg_catalog.pg_get_indexdef(a.attrelid, a.attnum, true) as indexdef,
      i.indoption[a.attnum - 1] & 1 <> 0                     as is_desc,
      i.indoption[a.attnum - 1] & 2 <> 0                     as nulls_first,
      a.attnum::int                                               as ordinal_position
from pg_index i
        join pg_class table_class on table_class.oid = i.indrelid
        join pg_class index_class on index_class.oid = i.indexrelid
        left join pg_namespace n on n.oid = table_class.relnamespace
        left join pg_tablespace ts on ts.oid = index_class.reltablespace
        join pg_catalog.pg_attribute a on a.attrelid = index_class.oid
where a.attnum > 0
 and not a.attisdropped
 and table_class.oid > 16384
and table_class.relkind = 'r'
order by table_schema, table_name, index_name, ordinal_position
"#);

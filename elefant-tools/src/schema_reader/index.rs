use tokio_postgres::Row;
use crate::postgres_client_wrapper::FromRow;
use crate::schema_reader::define_working_query;

pub struct IndexResult {
    pub table_schema: String,
    pub table_name: String,
    pub index_name: String,
    pub index_type: String,
    pub can_sort: bool,
    pub index_predicate: Option<String>,
}

impl FromRow for IndexResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(IndexResult {
            table_schema: row.try_get(0)?,
            table_name: row.try_get(1)?,
            index_name: row.try_get(2)?,
            index_type: row.try_get(3)?,
            can_sort: row.try_get(4)?,
            index_predicate: row.try_get(5)?,
        })
    }
}

//language=postgresql
define_working_query!(get_indices, IndexResult, r#"
select n.nspname           as table_schema,
       table_class.relname as table_name,
       index_class.relname as index_name,
       pa.amname           as index_type,
       pg_indexam_has_property(pa.oid, 'can_order') as can_sort,
       pg_catalog.pg_get_expr(i.indpred, i.indrelid, true) as index_predicate
from pg_index i
         join pg_class table_class on table_class.oid = i.indrelid
         join pg_class index_class on index_class.oid = i.indexrelid
         left join pg_namespace n on n.oid = table_class.relnamespace
         left join pg_tablespace ts on ts.oid = index_class.reltablespace
         join pg_catalog.pg_am pa on index_class.relam = pa.oid
where table_class.oid > 16384
  and not i.indisprimary
  and not i.indisunique
"#);
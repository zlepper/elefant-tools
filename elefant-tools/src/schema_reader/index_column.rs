use crate::postgres_client_wrapper::{FromRow, QueryResult};

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
    fn from_row(row: &elefant_client::PostgresDataRow<'_, '_>) -> crate::Result<Self> {
        Ok(IndexColumnResult {
            table_schema: row.get(0)?,
            table_name: row.get(1)?,
            index_name: row.get(2)?,
            is_key: row.get(3)?,
            column_expression: row.get(4)?,
            is_desc: row.get(5)?,
            nulls_first: row.get(6)?,
            ordinal_position: row.get(7)?,
        })
    }
}

//language=postgresql
pub(in crate::schema_reader) const QUERY: &str = r#"
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
         left join pg_depend dep on dep.objid = n.oid
where a.attnum > 0
 and not a.attisdropped
 and table_class.oid > 16384
and table_class.relkind = 'r'
  and (dep.objid is null or dep.deptype <> 'e' )
order by table_schema, table_name, index_name, ordinal_position;
"#;

impl QueryResult for IndexColumnResult {
    fn query(_version: i32) -> &'static str {
        QUERY
    }
}


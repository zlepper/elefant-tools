use crate::postgres_client_wrapper::QueryResult;

pub struct IndexResult {
    pub table_schema: String,
    pub table_name: String,
    pub index_name: String,
    pub index_type: String,
    pub can_sort: bool,
    pub index_predicate: Option<String>,
    pub is_unique: bool,
    pub is_primary_key: bool,
    pub nulls_not_distinct: bool,
    pub comment: Option<String>,
    pub storage_parameters: Option<Vec<String>>,
    pub constraint_definition: Option<String>,
}

impl<'a> elefant_client::FromSqlRow<'a> for IndexResult {
    fn from_sql_row(
        row: &'a elefant_client::PostgresDataRow<'_, '_>,
    ) -> Result<Self, elefant_client::ElefantClientError> {
        Ok(IndexResult {
            table_schema: row.get(0)?,
            table_name: row.get(1)?,
            index_name: row.get(2)?,
            index_type: row.get(3)?,
            can_sort: row.get(4)?,
            index_predicate: row.get(5)?,
            is_unique: row.get(6)?,
            is_primary_key: row.get(7)?,
            nulls_not_distinct: row.get(8)?,
            comment: row.get(9)?,
            storage_parameters: row.get(10)?,
            constraint_definition: row.get(11)?,
        })
    }
}

//language=postgresql
pub(in crate::schema_reader) const QUERY_V18: &str = r#"
select n.nspname           as table_schema,
       table_class.relname as table_name,
       index_class.relname as index_name,
       pa.amname           as index_type,
       pg_indexam_has_property(pa.oid, 'can_order') as can_sort,
       pg_catalog.pg_get_expr(i.indpred, i.indrelid, true) as index_predicate,
       i.indisunique       as is_unique,
       i.indisprimary      as is_primary_key,
       i.indnullsnotdistinct as nulls_not_distinct,
       d.description       as comment,
       index_class.reloptions as table_storage_parameters,
       case when con.conperiod then pg_get_constraintdef(con.oid) else null end as constraint_definition
from pg_index i
         join pg_class table_class on table_class.oid = i.indrelid
         join pg_class index_class on index_class.oid = i.indexrelid
         left join pg_namespace n on n.oid = table_class.relnamespace
         left join pg_tablespace ts on ts.oid = index_class.reltablespace
         join pg_catalog.pg_am pa on index_class.relam = pa.oid
         left join pg_description d on d.objoid = i.indexrelid
         left join pg_depend dep on dep.objid = n.oid
         left join pg_constraint con on con.conindid = i.indexrelid and con.contype = 'p'
where table_class.oid > 16384
and table_class.relkind in ('r', 'p')
and (dep.objid is null or dep.deptype <> 'e' )
order by table_schema, table_name, index_name;
"#;

//language=postgresql
pub(in crate::schema_reader) const QUERY_V15: &str = r#"
select n.nspname           as table_schema,
       table_class.relname as table_name,
       index_class.relname as index_name,
       pa.amname           as index_type,
       pg_indexam_has_property(pa.oid, 'can_order') as can_sort,
       pg_catalog.pg_get_expr(i.indpred, i.indrelid, true) as index_predicate,
       i.indisunique       as is_unique,
       i.indisprimary      as is_primary_key,
       i.indnullsnotdistinct as nulls_not_distinct,
       d.description       as comment,
       index_class.reloptions as table_storage_parameters,
       null::text as constraint_definition
from pg_index i
         join pg_class table_class on table_class.oid = i.indrelid
         join pg_class index_class on index_class.oid = i.indexrelid
         left join pg_namespace n on n.oid = table_class.relnamespace
         left join pg_tablespace ts on ts.oid = index_class.reltablespace
         join pg_catalog.pg_am pa on index_class.relam = pa.oid
         left join pg_description d on d.objoid = i.indexrelid
         left join pg_depend dep on dep.objid = n.oid
where table_class.oid > 16384
and table_class.relkind in ('r', 'p')
and (dep.objid is null or dep.deptype <> 'e' )
order by table_schema, table_name, index_name;
"#;

//language=postgresql
pub(in crate::schema_reader) const QUERY_LEGACY: &str = r#"
select n.nspname           as table_schema,
       table_class.relname as table_name,
       index_class.relname as index_name,
       pa.amname           as index_type,
       pg_indexam_has_property(pa.oid, 'can_order') as can_sort,
       pg_catalog.pg_get_expr(i.indpred, i.indrelid, true) as index_predicate,
       i.indisunique       as is_unique,
       i.indisprimary      as is_primary_key,
       false as nulls_not_distinct,
       d.description       as comment,
       index_class.reloptions as table_storage_parameters,
       null::text as constraint_definition
from pg_index i
         join pg_class table_class on table_class.oid = i.indrelid
         join pg_class index_class on index_class.oid = i.indexrelid
         left join pg_namespace n on n.oid = table_class.relnamespace
         left join pg_tablespace ts on ts.oid = index_class.reltablespace
         join pg_catalog.pg_am pa on index_class.relam = pa.oid
         left join pg_description d on d.objoid = i.indexrelid
         left join pg_depend dep on dep.objid = n.oid
where table_class.oid > 16384
and table_class.relkind in ('r', 'p')
and (dep.objid is null or dep.deptype <> 'e' )
order by table_schema, table_name, index_name;
"#;

impl QueryResult for IndexResult {
    fn query(version: i32) -> &'static str {
        if version >= 180 {
            QUERY_V18
        } else if version >= 150 {
            QUERY_V15
        } else {
            QUERY_LEGACY
        }
    }
}


use crate::postgres_client_wrapper::{FromRow, QueryResult};
use crate::schema_reader::SchemaReader;
use tracing::instrument;

pub struct ForeignKeyColumnResult {
    pub constraint_name: String,
    // pub constraint_schema_name: String,
    pub source_table_name: String,
    pub source_schema_name: String,
    pub source_table_column_name: String,
    pub target_table_column_name: String,
    pub affected_by_delete_action: bool,
}

impl FromRow for ForeignKeyColumnResult {
    fn from_row(row: &elefant_client::PostgresDataRow<'_, '_>) -> crate::Result<Self> {
        Ok(Self {
            constraint_name: row.get(0)?,
            // constraint_schema_name: row.get(1)?,
            source_table_name: row.get(2)?,
            source_schema_name: row.get(3)?,
            source_table_column_name: row.get(4)?,
            target_table_column_name: row.get(5)?,
            affected_by_delete_action: row.get(6)?,
        })
    }
}

//language=postgresql
pub(in crate::schema_reader) const QUERY_V15: &str = r#"
select con.conname       as constraint_name,
       con_ns.nspname    as constraint_schema_name,
       tab.relname       as source_table_name,
       tab_ns.nspname    as source_schema_name,
       source_table_attr.attname as source_table_column_name,
       target_table_attr.attname as target_table_column_name,
       (con.confdelsetcols is null or source_table_attr.attnum=any(con.confdelsetcols)) as affected_by_delete_action
from pg_constraint con
         left join pg_catalog.pg_namespace con_ns on con_ns.oid = con.connamespace
         join pg_catalog.pg_class tab on con.conrelid = tab.oid
         left join pg_namespace tab_ns on tab_ns.oid = tab.relnamespace
         join unnest(con.conkey, con.confkey) as cols (conkey, confkey) on true
         left join pg_attribute source_table_attr
                   on source_table_attr.attrelid = con.conrelid and source_table_attr.attnum = cols.conkey
         left join pg_attribute target_table_attr
                   on target_table_attr.attrelid = con.confrelid and target_table_attr.attnum = cols.confkey
         left join pg_depend dep on dep.objid = con_ns.oid
where con.contype = 'f'
and (dep.objid is null or dep.deptype <> 'e' )
order by constraint_schema_name, source_table_name, constraint_name, source_table_attr.attnum;
"#;

//language=postgresql
pub(in crate::schema_reader) const QUERY_LEGACY: &str = r#"
select con.conname       as constraint_name,
       con_ns.nspname    as constraint_schema_name,
       tab.relname       as source_table_name,
       tab_ns.nspname    as source_schema_name,
       source_table_attr.attname as source_table_column_name,
       target_table_attr.attname as target_table_column_name,
       true as affected_by_delete_action
from pg_constraint con
         left join pg_catalog.pg_namespace con_ns on con_ns.oid = con.connamespace
         join pg_catalog.pg_class tab on con.conrelid = tab.oid
         left join pg_namespace tab_ns on tab_ns.oid = tab.relnamespace
         join unnest(con.conkey, con.confkey) as cols (conkey, confkey) on true
         left join pg_attribute source_table_attr
                   on source_table_attr.attrelid = con.conrelid and source_table_attr.attnum = cols.conkey
         left join pg_attribute target_table_attr
                   on target_table_attr.attrelid = con.confrelid and target_table_attr.attnum = cols.confkey
where con.contype = 'f'
order by constraint_schema_name, source_table_name, constraint_name, source_table_attr.attnum;
"#;

impl QueryResult for ForeignKeyColumnResult {
    fn query(version: i32) -> &'static str {
        if version >= 150 {
            QUERY_V15
        } else {
            QUERY_LEGACY
        }
    }
}

impl SchemaReader<'_> {
    #[instrument(skip_all)]
    pub(in crate::schema_reader) async fn get_foreign_key_columns(
        &self,
    ) -> crate::Result<Vec<ForeignKeyColumnResult>> {
        let query = if self.connection.version() >= 150 {
            QUERY_V15
        } else {
            QUERY_LEGACY
        };
        self.connection.get_results(query).await
    }
}

use tokio_postgres::Row;
use crate::postgres_client_wrapper::{FromRow, RowEnumExt};
use crate::ReferenceAction;
use crate::schema_reader::define_working_query;

pub struct ForeignKeyResult {
    pub constraint_name: String,
    pub constraint_schema_name: String,
    pub source_table_name: String,
    pub source_table_schema_name: String,
    pub target_table_name: String,
    pub target_table_schema_name: String,
    pub update_action: ReferenceAction,
    pub delete_action: ReferenceAction,
}

impl FromRow for ForeignKeyResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(Self {
            constraint_name: row.try_get(0)?,
            constraint_schema_name: row.try_get(1)?,
            source_table_name: row.try_get(2)?,
            source_table_schema_name: row.try_get(3)?,
            target_table_name: row.try_get(4)?,
            target_table_schema_name: row.try_get(5)?,
            update_action: row.try_get_enum_value(6)?,
            delete_action: row.try_get_enum_value(7)?,
        })
    }
}

//language=postgresql
define_working_query!(get_foreign_keys, ForeignKeyResult, r#"
select con.conname              as constraint_name,
       con_ns.nspname           as constraint_schema_name,
       tab.relname              as source_table_name,
       tab_ns.nspname           as source_schema_name,
       target.relname           as target_table_name,
       target_ns.nspname        as target_schema_name,
       con.confupdtype    as update_action,
       con.confdeltype    as delete_action
from pg_catalog.pg_constraint con
         left join pg_catalog.pg_namespace con_ns on con_ns.oid = con.connamespace
         join pg_catalog.pg_class tab on con.conrelid = tab.oid
         left join pg_namespace tab_ns on tab_ns.oid = tab.relnamespace
         join pg_catalog.pg_class target on con.confrelid = target.oid
         left join pg_namespace target_ns on target_ns.oid = target.relnamespace
where con.contype = 'f';"#);

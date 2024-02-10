use tokio_postgres::Row;
use crate::postgres_client_wrapper::FromRow;
use crate::schema_reader::define_working_query;

pub struct ExtensionResult {
    pub extension_name: String,
    pub extension_schema_name: String,
    pub extension_version: String,
    pub extension_relocatable: bool,

}

impl FromRow for ExtensionResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(Self {
            extension_name: row.try_get(0)?,
            extension_schema_name: row.try_get(1)?,
            extension_version: row.try_get(2)?,
            extension_relocatable: row.try_get(3)?,
        })
    }
}

//language=postgresql
define_working_query!(get_extensions, ExtensionResult, r#"
select ext.extname        as extension_name,
       ns.nspname   as extension_schema_name,
       ext.extversion     as extension_version,
       ext.extrelocatable as extension_relocatable
from pg_catalog.pg_extension ext
         join pg_namespace ns on ext.extnamespace = ns.oid
        where ext.oid > 16384;
"#);

use crate::postgres_client_wrapper::QueryResult;

pub struct ExtensionResult {
    pub extension_name: String,
    pub extension_schema_name: String,
    pub extension_version: String,
    pub extension_relocatable: bool,
}

impl<'a> elefant_client::FromSqlRow<'a> for ExtensionResult {
    fn from_sql_row(
        row: &'a elefant_client::PostgresDataRow<'_, '_>,
    ) -> Result<Self, elefant_client::ElefantClientError> {
        Ok(Self {
            extension_name: row.get(0)?,
            extension_schema_name: row.get(1)?,
            extension_version: row.get(2)?,
            extension_relocatable: row.get(3)?,
        })
    }
}

//language=postgresql
pub(in crate::schema_reader) const QUERY: &str = r#"
select ext.extname        as extension_name,
       ns.nspname   as extension_schema_name,
       ext.extversion     as extension_version,
       ext.extrelocatable as extension_relocatable
from pg_catalog.pg_extension ext
         join pg_namespace ns on ext.extnamespace = ns.oid
        where ext.oid > 16384;
"#;

impl QueryResult for ExtensionResult {
    fn query(_version: i32) -> &'static str {
        QUERY
    }
}


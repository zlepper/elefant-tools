use crate::postgres_client_wrapper::QueryResult;

pub struct SchemaResult {
    pub name: String,
    pub comment: Option<String>,
}

impl<'a> elefant_client::FromSqlRow<'a> for SchemaResult {
    fn from_sql_row(
        row: &'a elefant_client::PostgresDataRow<'_, '_>,
    ) -> Result<Self, elefant_client::ElefantClientError> {
        Ok(Self {
            name: row.get(0)?,
            comment: row.get(1)?,
        })
    }
}

//language=postgresql
pub(in crate::schema_reader) const QUERY: &str = r#"
SELECT n.nspname AS name,
       d.description AS comment
FROM pg_namespace n
         LEFT JOIN pg_description d ON d.objoid = n.oid and (n.nspname <> 'public' or d.description <> 'standard public schema')
         left join pg_depend dep on dep.objid = n.oid
WHERE (n.oid > 16384 or n.nspname = 'public')
    and (dep.objid is null or dep.deptype <> 'e' )
    and has_schema_privilege(n.oid, 'CREATE')
ORDER BY n.nspname;
"#;

impl QueryResult for SchemaResult {
    fn query(_version: i32) -> &'static str {
        QUERY
    }
}


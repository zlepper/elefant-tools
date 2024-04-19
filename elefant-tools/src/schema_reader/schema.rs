use tokio_postgres::Row;
use crate::postgres_client_wrapper::FromRow;
use crate::schema_reader::define_working_query;

pub struct SchemaResult {
    pub name: String,
    pub comment: Option<String>,
}

impl FromRow for SchemaResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(Self {
            name: row.try_get(0)?,
            comment: row.try_get(1)?,
        })
    }
}

//language=postgresql
define_working_query!(get_schemas, SchemaResult, r#"
SELECT n.nspname AS name,
       d.description AS comment
FROM pg_namespace n
         LEFT JOIN pg_description d ON d.objoid = n.oid and (n.nspname <> 'public' or d.description <> 'standard public schema')
         left join pg_depend dep on dep.objid = n.oid
WHERE (n.oid > 16384 or n.nspname = 'public')
    and (dep.objid is null or dep.deptype <> 'e' )
    and has_schema_privilege(n.oid, 'CREATE')
ORDER BY n.nspname;
"#);

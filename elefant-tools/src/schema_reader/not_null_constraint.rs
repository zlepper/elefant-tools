use crate::postgres_client_wrapper::QueryResult;

#[derive(Debug, Eq, PartialEq)]
pub struct NotNullConstraintResult {
    pub table_schema: String,
    pub table_name: String,
    pub constraint_name: String,
    pub column_name: String,
    pub is_validated: bool,
}

impl<'a> elefant_client::FromSqlRow<'a> for NotNullConstraintResult {
    fn from_sql_row(
        row: &'a elefant_client::PostgresDataRow<'_, '_>,
    ) -> Result<Self, elefant_client::ElefantClientError> {
        Ok(NotNullConstraintResult {
            table_schema: row.get(0)?,
            table_name: row.get(1)?,
            constraint_name: row.get(2)?,
            column_name: row.get(3)?,
            is_validated: row.get(4)?,
        })
    }
}

//language=postgresql
pub(in crate::schema_reader) const QUERY_V18: &str = r#"
select ns.nspname      as table_schema,
       cl.relname      as table_name,
       ct.conname       as constraint_name,
       attr.attname     as column_name,
       ct.convalidated  as is_validated
from pg_constraint ct
         join pg_class cl on cl.oid = ct.conrelid
         join pg_namespace ns on ns.oid = cl.relnamespace
         join pg_attribute attr on attr.attrelid = ct.conrelid
              and attr.attnum = any(ct.conkey)
         left join pg_depend dep on dep.objid = ns.oid
where ct.oid > 16384
  and ct.contype = 'n'
  and (dep.objid is null or dep.deptype <> 'e')
  and (
    ct.convalidated = false
    or (
      ct.conislocal = true
      and right(ct.conname, length('_' || attr.attname || '_not_null')) <> '_' || attr.attname || '_not_null'
    )
  )
order by ns.nspname, cl.relname, ct.conname;
"#;

// For PG < 18, NOT NULL constraints are not in pg_constraint
// Return an empty result by adding an impossible WHERE clause
//language=postgresql
pub(in crate::schema_reader) const QUERY_LEGACY: &str = r#"
select ''::text as table_schema,
       ''::text as table_name,
       ''::text as constraint_name,
       ''::text as column_name,
       true     as is_validated
where false;
"#;

impl QueryResult for NotNullConstraintResult {
    fn query(version: i32) -> &'static str {
        if version >= 180 {
            QUERY_V18
        } else {
            QUERY_LEGACY
        }
    }
}

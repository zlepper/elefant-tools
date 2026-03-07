use crate::postgres_client_wrapper::QueryResult;

#[derive(Debug, Eq, PartialEq)]
pub struct CheckConstraintResult {
    pub table_schema: String,
    pub table_name: String,
    pub constraint_name: String,
    pub check_clause: String,
    pub comment: Option<String>,
    pub is_enforced: bool,
}

impl<'a> elefant_client::FromSqlRow<'a> for CheckConstraintResult {
    fn from_sql_row(
        row: &'a elefant_client::PostgresDataRow<'_, '_>,
    ) -> Result<Self, elefant_client::ElefantClientError> {
        Ok(CheckConstraintResult {
            table_schema: row.get(0)?,
            table_name: row.get(1)?,
            constraint_name: row.get(2)?,
            check_clause: row.get(3)?,
            comment: row.get(4)?,
            is_enforced: row.get(5)?,
        })
    }
}

//language=postgresql
pub(in crate::schema_reader) const QUERY_V18: &str = r#"
select ns.nspname                                     as table_schema,
       cl.relname                                     as table_name,
       ct.conname                                     as constraint_name,
       regexp_replace(substring(pg_get_constraintdef(ct.oid) from 7), ' NOT ENFORCED$', '') as constraint_def,
       des.description,
       ct.conenforced                                 as is_enforced
from pg_constraint ct
         join pg_class cl on cl.oid = ct.conrelid
         join pg_namespace ns on ns.oid = cl.relnamespace
         left join pg_description des on des.objoid = ct.oid
         left join pg_depend dep on dep.objid = ns.oid
where ct.oid > 16384
  and ct.contype = 'c'
  and (dep.objid is null or dep.deptype <> 'e' )
order by ns.nspname, cl.relname, ct.conname;
"#;

//language=postgresql
pub(in crate::schema_reader) const QUERY_LEGACY: &str = r#"
select ns.nspname                                     as table_schema,
       cl.relname                                     as table_name,
       ct.conname                                     as constraint_name,
       substring(pg_get_constraintdef(ct.oid) from 7) as constraint_def,
       des.description,
       true                                           as is_enforced
from pg_constraint ct
         join pg_class cl on cl.oid = ct.conrelid
         join pg_namespace ns on ns.oid = cl.relnamespace
         left join pg_description des on des.objoid = ct.oid
         left join pg_depend dep on dep.objid = ns.oid
where ct.oid > 16384
  and ct.contype = 'c'
  and (dep.objid is null or dep.deptype <> 'e' )
order by ns.nspname, cl.relname, ct.conname;
"#;

impl QueryResult for CheckConstraintResult {
    fn query(version: i32) -> &'static str {
        if version >= 180 {
            QUERY_V18
        } else {
            QUERY_LEGACY
        }
    }
}


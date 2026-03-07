use crate::postgres_client_wrapper::QueryResult;

#[derive(Debug, Eq, PartialEq)]
pub struct UniqueConstraintResult {
    pub table_schema: String,
    pub table_name: String,
    pub constraint_name: String,
    pub index_name: String,
    pub comment: Option<String>,
    pub constraint_definition: Option<String>,
}

impl<'a> elefant_client::FromSqlRow<'a> for UniqueConstraintResult {
    fn from_sql_row(
        row: &'a elefant_client::PostgresDataRow<'_, '_>,
    ) -> Result<Self, elefant_client::ElefantClientError> {
        Ok(UniqueConstraintResult {
            table_schema: row.get(0)?,
            table_name: row.get(1)?,
            constraint_name: row.get(2)?,
            index_name: row.get(3)?,
            comment: row.get(4)?,
            constraint_definition: row.get(5)?,
        })
    }
}

//language=postgresql
pub(in crate::schema_reader) const QUERY_V18: &str = r#"
select ns.nspname                                     as table_schema,
       cl.relname                                     as table_name,
       con.conname                                     as constraint_name,
       index_class.relname                            as index_name,
       d.description                                  as comment,
       case when con.conperiod then pg_get_constraintdef(con.oid) else null end as constraint_definition
from pg_constraint con
         join pg_class cl on cl.oid = con.conrelid
         join pg_namespace ns on ns.oid = cl.relnamespace
         join pg_index i on i.indexrelid = con.conindid
         join pg_class index_class on i.indexrelid = index_class.oid
         left join pg_description d on d.objoid = con.oid
         left join pg_depend dep on dep.objid = ns.oid
where con.oid > 16384
  and con.contype = 'u'
  and (dep.objid is null or dep.deptype <> 'e' )
order by ns.nspname, cl.relname, con.conname;
"#;

//language=postgresql
pub(in crate::schema_reader) const QUERY_LEGACY: &str = r#"
select ns.nspname                                     as table_schema,
       cl.relname                                     as table_name,
       con.conname                                     as constraint_name,
       index_class.relname                            as index_name,
       d.description                                  as comment,
       null::text                                     as constraint_definition
from pg_constraint con
         join pg_class cl on cl.oid = con.conrelid
         join pg_namespace ns on ns.oid = cl.relnamespace
         join pg_index i on i.indexrelid = con.conindid
         join pg_class index_class on i.indexrelid = index_class.oid
         left join pg_description d on d.objoid = con.oid
         left join pg_depend dep on dep.objid = ns.oid
where con.oid > 16384
  and con.contype = 'u'
  and (dep.objid is null or dep.deptype <> 'e' )
order by ns.nspname, cl.relname, con.conname;
"#;

impl QueryResult for UniqueConstraintResult {
    fn query(version: i32) -> &'static str {
        if version >= 180 {
            QUERY_V18
        } else {
            QUERY_LEGACY
        }
    }
}


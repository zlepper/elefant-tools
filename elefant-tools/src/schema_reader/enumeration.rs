use crate::postgres_client_wrapper::QueryResult;

pub struct EnumResult {
    pub schema_name: String,
    pub name: String,
    pub comment: Option<String>,
    pub values: Vec<String>,
}

impl<'a> elefant_client::FromSqlRow<'a> for EnumResult {
    fn from_sql_row(
        row: &'a elefant_client::PostgresDataRow<'_, '_>,
    ) -> Result<Self, elefant_client::ElefantClientError> {
        Ok(Self {
            schema_name: row.get(0)?,
            name: row.get(1)?,
            comment: row.get(2)?,
            values: row.get(3)?,
        })
    }
}

//language=postgresql
pub(in crate::schema_reader) const QUERY: &str = r#"
select enums.nspname, enums.typname, max(enums.description) as description, array_agg(enums.enumlabel)  from (
select ns.nspname, t.typname, e.enumlabel, d.description
from pg_enum e
join pg_type t on e.enumtypid = t.oid
join pg_namespace ns on t.typnamespace = ns.oid
left join pg_description d on d.objoid = t.oid
         left join pg_depend dep on dep.objid = ns.oid
where (dep.objid is null or dep.deptype <> 'e' )
  and has_type_privilege(t.oid, 'USAGE')
order by ns.nspname, t.typname, e.enumsortorder
) as enums
group by enums.nspname, enums.typname;
"#;

impl QueryResult for EnumResult {
    fn query(_version: i32) -> &'static str {
        QUERY
    }
}

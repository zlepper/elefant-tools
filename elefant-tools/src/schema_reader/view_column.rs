use crate::postgres_client_wrapper::QueryResult;

pub struct ViewColumnResult {
    pub view_name: String,
    pub schema_name: String,
    pub column_name: String,
    pub ordinal_position: i32,
    // pub comment: Option<String>,
}

impl<'a> elefant_client::FromSqlRow<'a> for ViewColumnResult {
    fn from_sql_row(
        row: &'a elefant_client::PostgresDataRow<'_, '_>,
    ) -> Result<Self, elefant_client::ElefantClientError> {
        Ok(Self {
            view_name: row.get(0)?,
            schema_name: row.get(1)?,
            column_name: row.get(2)?,
            ordinal_position: row.get(3)?,
            // comment: row.get(4)?,
        })
    }
}

//language=postgresql
pub(in crate::schema_reader) const QUERY: &str = r#"
select tab.relname  as view_name,
       ns.nspname   as schema_name,
       attr.attname as column_name,
       attr.attnum::int4  as ordinal_position,
         d.description as comment
from pg_class tab
         join pg_namespace ns on tab.relnamespace = ns.oid
         join pg_attribute attr on attr.attrelid = tab.oid
         left join pg_description d on d.objoid = attr.attrelid and d.objsubid = attr.attnum
         left join pg_depend dep on dep.objid = ns.oid
where tab.oid > 16384
  and tab.relkind in('v', 'm')
  and attr.attnum > 0
  and (dep.objid is null or dep.deptype <> 'e' )
order by ns.nspname, tab.relname, attr.attnum;
"#;

impl QueryResult for ViewColumnResult {
    fn query(_version: i32) -> &'static str {
        QUERY
    }
}

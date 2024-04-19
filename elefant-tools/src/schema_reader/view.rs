use tokio_postgres::Row;
use crate::postgres_client_wrapper::FromRow;
use crate::schema_reader::define_working_query;

pub struct ViewResult {
    pub oid: i64,
    pub view_name: String,
    pub schema_name: String,
    pub definition: String,
    pub comment: Option<String>,
    pub is_materialized: bool,
    pub depends_on: Option<Vec<i64>>,
    pub type_oid: i64,
}

impl FromRow for ViewResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(Self {
            oid: row.try_get(0)?,
            view_name: row.try_get(1)?,
            schema_name: row.try_get(2)?,
            definition: row.try_get(3)?,
            comment: row.try_get(4)?,
            is_materialized: row.try_get(5)?,
            depends_on: row.try_get(6)?,
            type_oid: row.try_get(7)?,
        })
    }
}


//language=postgresql
define_working_query!(get_views, ViewResult, r#"
select tab.oid::int8,
    tab.relname                   as view_name,
       ns.nspname                    as schema_name,
       pg_get_viewdef(tab.oid, true) as def,
       des.description,
         tab.relkind = 'm'             as is_materialized,
         (select array_agg(source_view.oid::int8)
        from pg_rewrite rew
                 join pg_depend dep on rew.oid = dep.objid
                 join pg_class source_view on dep.refobjid = source_view.oid and source_view.oid <> tab.oid
        where rew.ev_class = tab.oid) as depends_on,
    tab.reltype::int8
from pg_class tab
         join pg_namespace ns on tab.relnamespace = ns.oid
         left join pg_description des on des.objoid = tab.oid
         left join pg_depend dep on dep.objid = ns.oid
where tab.oid > 16384
  and tab.relkind in('v', 'm')
  and (dep.objid is null or dep.deptype <> 'e' )
  and has_table_privilege(tab.oid, 'SELECT')
order by schema_name, view_name;
"#);

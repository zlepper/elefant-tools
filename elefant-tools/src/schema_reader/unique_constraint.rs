use tokio_postgres::Row;
use crate::postgres_client_wrapper::FromRow;
use crate::schema_reader::define_working_query;

#[derive(Debug, Eq, PartialEq)]
pub struct UniqueConstraintResult {
    pub table_schema: String,
    pub table_name: String,
    pub constraint_name: String,
    pub index_name: String,
    pub comment: Option<String>,
}

impl FromRow for UniqueConstraintResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(UniqueConstraintResult {
            table_schema: row.try_get(0)?,
            table_name: row.try_get(1)?,
            constraint_name: row.try_get(2)?,
            index_name: row.try_get(3)?,
            comment: row.try_get(4)?,
        })
    }
}

//language=postgresql
define_working_query!(get_unique_constraints, UniqueConstraintResult, r#"
select ns.nspname                                     as table_schema,
       cl.relname                                     as table_name,
       con.conname                                     as constraint_name,
       index_class.relname                            as index_name,
         d.description                                  as comment
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
"#);

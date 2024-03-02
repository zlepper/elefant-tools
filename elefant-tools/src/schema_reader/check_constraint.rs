use tokio_postgres::Row;
use crate::postgres_client_wrapper::FromRow;
use crate::schema_reader::define_working_query;

#[derive(Debug, Eq, PartialEq)]
pub struct CheckConstraintResult {
    pub table_schema: String,
    pub table_name: String,
    pub constraint_name: String,
    pub check_clause: String,
    pub comment: Option<String>,
}

impl FromRow for CheckConstraintResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(CheckConstraintResult {
            table_schema: row.try_get(0)?,
            table_name: row.try_get(1)?,
            constraint_name: row.try_get(2)?,
            check_clause: row.try_get(3)?,
            comment: row.try_get(4)?,
        })
    }
}

//language=postgresql
define_working_query!(get_check_constraints, CheckConstraintResult, r#"
select ns.nspname                                     as table_schema,
       cl.relname                                     as table_name,
       ct.conname                                     as constraint_name,
       substring(pg_get_constraintdef(ct.oid) from 7) as constraint_def,
       des.description
from pg_constraint ct
         join pg_class cl on cl.oid = ct.conrelid
         join pg_namespace ns on ns.oid = cl.relnamespace
         left join pg_description des on des.objoid = ct.oid
         left join pg_depend dep on dep.objid = ns.oid
where ct.oid > 16384
  and ct.contype = 'c'
  and (dep.objid is null or dep.deptype <> 'e' )
order by ns.nspname, cl.relname, ct.conname;
"#);

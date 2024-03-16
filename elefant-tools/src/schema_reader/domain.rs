use tokio_postgres::Row;
use crate::postgres_client_wrapper::FromRow;
use crate::schema_reader::define_working_query;

pub struct DomainResult {
    pub schema_name: String,
    pub domain_name: String,
    pub constraint_name: Option<String>,
    pub constraint_definition: Option<String>,
    pub description: Option<String>,
    pub default_value: Option<String>,
    pub not_null: bool,
    pub base_type_name: String,
    pub domain_oid: i64,
    pub depends_on: Option<Vec<i64>>,
    pub data_type_length: Option<i32>
}

impl FromRow for DomainResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(DomainResult {
            schema_name: row.try_get(0)?,
            domain_name: row.try_get(1)?,
            constraint_name: row.try_get(2)?,
            constraint_definition: row.try_get(3)?,
            description: row.try_get(4)?,
            default_value: row.try_get(5)?,
            not_null: row.try_get(6)?,
            base_type_name: row.try_get(7)?,
            domain_oid: row.try_get(8)?,
            depends_on: row.try_get(9)?,
            data_type_length: row.try_get(10)?
        })
    }
}

//language=postgresql
define_working_query!(get_domains, DomainResult, r#"
select nsp.nspname                                     as schema_name,
       typ.typname                                     as domain_name,
       con.conname                                     as constraint_name,
       substring(pg_get_constraintdef(con.oid) from 7) as constraint_def,
       des.description                                 as description,
       typ.typdefault                                  as default_value,
       typ.typnotnull                                  as not_null,
       base_type.typname                               as base_type_name,
       typ.oid::int8                                   as domain_oid,
       (select array_agg(refobjid::int8)
        from pg_depend dep
        where typ.oid = dep.objid
          and dep.deptype <> 'e'
          and dep.refobjid > 16384
          and dep.objid <> dep.refobjid)               as depends_on,
       information_schema._pg_char_max_length(typ.typbasetype, typ.typtypmod) as data_type_length
from pg_type typ
         left join pg_constraint con on con.contypid = typ.oid
         join pg_type base_type on base_type.oid = typ.typbasetype
         join pg_namespace nsp on nsp.oid = typ.typnamespace
         left join pg_depend dep on dep.objid = nsp.oid
         left join pg_description des on des.objoid = typ.oid
where typ.oid > 16384
  and (dep.objid is null or dep.deptype <> 'e')
  and typ.typtype = 'd'
order by nsp.nspname, typ.typname, con.conname;
"#);

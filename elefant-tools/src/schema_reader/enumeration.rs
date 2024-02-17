use tokio_postgres::Row;
use crate::postgres_client_wrapper::FromRow;
use crate::schema_reader::define_working_query;

pub struct EnumResult {
    pub schema_name: String,
    pub name: String,
    pub comment: Option<String>,
    pub values: Vec<String>,
}

impl FromRow for EnumResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(Self {
            schema_name: row.try_get(0)?,
            name: row.try_get(1)?,
            comment: row.try_get(2)?,
            values: row.try_get(3)?,
        })
    }
}

//language=postgresql
define_working_query!(get_enums, EnumResult, r#"
select enums.nspname, enums.typname, max(enums.description) as description, array_agg(enums.enumlabel)  from (
select ns.nspname, t.typname, e.enumlabel, d.description
from pg_enum e
join pg_type t on e.enumtypid = t.oid
join pg_namespace ns on t.typnamespace = ns.oid
left join pg_description d on d.objoid = t.oid
order by ns.nspname, t.typname, e.enumsortorder
) as enums
group by enums.nspname, enums.typname;
"#);

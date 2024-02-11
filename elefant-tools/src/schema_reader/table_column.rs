use tokio_postgres::Row;
use crate::postgres_client_wrapper::FromRow;
use crate::PostgresColumn;
use crate::schema_reader::define_working_query;

#[derive(Debug, Eq, PartialEq)]
pub struct TableColumnsResult {
    pub schema_name: String,
    pub table_name: String,
    pub column_name: String,
    pub ordinal_position: i16,
    pub is_nullable: bool,
    pub data_type: String,
    pub column_default: Option<String>,
    pub generated: Option<String>,
}

impl FromRow for TableColumnsResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(TableColumnsResult {
            schema_name: row.try_get(0)?,
            table_name: row.try_get(1)?,
            column_name: row.try_get(2)?,
            ordinal_position: row.try_get(3)?,
            is_nullable: row.try_get(4)?,
            data_type: row.try_get(5)?,
            column_default: row.try_get(6)?,
            generated: row.try_get(7)?,
        })
    }
}


impl TableColumnsResult {
    pub fn to_postgres_column(&self) -> PostgresColumn {
        PostgresColumn {
            name: self.column_name.clone(),
            is_nullable: self.is_nullable,
            ordinal_position: self.ordinal_position as i32,
            data_type: self.data_type.clone(),
            default_value: self.column_default.clone(),
            generated: self.generated.clone(),
        }
    }
}


//language=postgresql
define_working_query!(get_columns, TableColumnsResult, r#"
select ns.nspname,
       cl.relname,
       attr.attname,
       attr.attnum,
       (attr.attnotnull OR t.typtype = 'd'::"char" AND t.typnotnull) = false as is_nullable,
       t.typname,
       CASE
           WHEN attr.attgenerated = ''::"char" THEN pg_get_expr(ad.adbin, ad.adrelid)
           ELSE NULL::text
           END::text                           AS column_default,
       CASE
           WHEN attr.attgenerated <> ''::"char" THEN pg_get_expr(ad.adbin, ad.adrelid)
           ELSE NULL::text
           END::text                           AS generation_expressio
from pg_attribute attr
         join pg_class cl on attr.attrelid = cl.oid
         join pg_type t on attr.atttypid = t.oid
         join pg_namespace ns on ns.oid = cl.relnamespace
         left join pg_attrdef ad on attr.attrelid = ad.adrelid and attr.attnum = ad.adnum
where cl.relkind = 'r'
  and cl.oid > 16384
  and attr.attnum > 0
order by ns.nspname, cl.relname, attr.attnum;
"#);

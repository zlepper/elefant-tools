use crate::postgres_client_wrapper::{QueryResult, RowEnumExt};
use crate::{ColumnIdentity, GeneratedColumn, GeneratedColumnType, PostgresColumn};

#[derive(Debug, Eq, PartialEq)]
pub struct TableColumnsResult {
    pub schema_name: String,
    pub table_name: String,
    pub column_name: String,
    pub ordinal_position: i16,
    pub is_nullable: bool,
    pub data_type: String,
    pub column_default: Option<String>,
    pub generation_expression: Option<String>,
    pub generated_type: String,
    pub comment: Option<String>,
    pub array_dimensions: i32,
    pub data_type_length: Option<i32>,
    pub identity: Option<ColumnIdentity>,
}

impl<'a> elefant_client::FromSqlRow<'a> for TableColumnsResult {
    fn from_sql_row(
        row: &'a elefant_client::PostgresDataRow<'_, '_>,
    ) -> Result<Self, elefant_client::ElefantClientError> {
        Ok(TableColumnsResult {
            schema_name: row.get(0)?,
            table_name: row.get(1)?,
            column_name: row.get(2)?,
            ordinal_position: row.get(3)?,
            is_nullable: row.get(4)?,
            data_type: row.get(5)?,
            column_default: row.get(6)?,
            generation_expression: row.get(7)?,
            generated_type: row.get(8)?,
            comment: row.get(9)?,
            array_dimensions: row.get(10)?,
            data_type_length: row.get(11)?,
            identity: row.try_get_opt_enum_value(12)?,
        })
    }
}

impl TableColumnsResult {
    pub fn to_postgres_column(&self) -> PostgresColumn {
        let generated = self.generation_expression.as_ref().map(|expression| {
            let generation_type = match self.generated_type.as_str() {
                "v" => GeneratedColumnType::Virtual,
                _ => GeneratedColumnType::Stored,
            };
            GeneratedColumn {
                expression: expression.clone(),
                generation_type,
            }
        });

        PostgresColumn {
            name: self.column_name.clone(),
            is_nullable: self.is_nullable,
            ordinal_position: self.ordinal_position as i32,
            data_type: self.data_type.clone(),
            default_value: self.column_default.clone(),
            generated,
            comment: self.comment.clone(),
            array_dimensions: self.array_dimensions,
            data_type_length: self.data_type_length,
            identity: self.identity,
        }
    }
}

//language=postgresql
pub(in crate::schema_reader) const QUERY: &str = r#"
select ns.nspname,
       cl.relname,
       attr.attname,
       attr.attnum,
       (attr.attnotnull OR t.typtype = 'd'::"char" AND t.typnotnull) = false                       as is_nullable,
       coalesce(non_array_type.typname, t.typname),
       CASE
           WHEN attr.attgenerated = ''::"char" THEN pg_get_expr(ad.adbin, ad.adrelid)
           ELSE NULL::text
           END::text                                                                               AS column_default,
       CASE
           WHEN attr.attgenerated <> ''::"char" THEN pg_get_expr(ad.adbin, ad.adrelid)
           ELSE NULL::text
           END::text                                                                               AS generation_expression,
       attr.attgenerated::text                                                                     AS generated_type,
       des.description,
       attr.attndims                                                                               as array_dimensions,
       information_schema._pg_char_max_length(coalesce(non_array_type.oid, t.oid), attr.atttypmod) as data_type_length,
       attidentity
from pg_attribute attr
         join pg_class cl on attr.attrelid = cl.oid
         join pg_type t on attr.atttypid = t.oid
         join pg_namespace ns on ns.oid = cl.relnamespace
         left join pg_attrdef ad on attr.attrelid = ad.adrelid and attr.attnum = ad.adnum
         left join pg_description des on des.objoid = cl.oid and des.objsubid = attr.attnum
         left join pg_type non_array_type on non_array_type.oid = t.typelem and non_array_type.typarray = t.oid
         left join pg_depend dep on dep.objid = ns.oid
where cl.relkind in ('r', 'p')
  and cl.oid > 16384
  and attr.attnum > 0
  and (dep.objid is null or dep.deptype <> 'e')
order by ns.nspname, cl.relname, attr.attnum;
"#;

impl QueryResult for TableColumnsResult {
    fn query(_version: i32) -> &'static str {
        QUERY
    }
}


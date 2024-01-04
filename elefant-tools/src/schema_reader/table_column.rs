use tokio_postgres::Row;
use crate::postgres_client_wrapper::FromRow;
use crate::PostgresColumn;
use crate::schema_reader::define_working_query;

#[derive(Debug, Eq, PartialEq)]
pub struct TableColumnsResult {
    pub schema_name: String,
    pub table_name: String,
    pub column_name: String,
    pub ordinal_position: i32,
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
            is_nullable: row.try_get::<usize, String>(4)? != "NO",
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
            ordinal_position: self.ordinal_position,
            data_type: self.data_type.clone(),
            default_value: self.column_default.clone(),
            generated: self.generated.clone(),
        }
    }
}


//language=postgresql
define_working_query!(get_columns, TableColumnsResult, r#"
select c.table_schema, c.table_name, c.column_name, c.ordinal_position, c.is_nullable, c.data_type, c.column_default, c.generation_expression from information_schema.tables t
join information_schema.columns c on t.table_schema = c.table_schema and t.table_name = c.table_name
where t.table_schema not in ('pg_catalog', 'pg_toast', 'information_schema') and t.table_type = 'BASE TABLE'
order by c.table_schema, c.table_name, c.ordinal_position;
"#);

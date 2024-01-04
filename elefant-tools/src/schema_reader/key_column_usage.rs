use std::str::FromStr;
use tokio_postgres::Row;
use crate::postgres_client_wrapper::FromRow;
use crate::schema_reader::{define_working_query};

#[derive(Debug, Eq, PartialEq)]
pub struct KeyColumnUsageResult {
    pub table_schema: String,
    pub table_name: String,
    pub constraint_name: String,
    pub column_name: String,
    pub ordinal_position: i32,
    pub position_in_unique_constraint: Option<i32>,
    pub key_type: ConstraintType,
    pub nulls_distinct: Option<bool>,
}

impl FromRow for KeyColumnUsageResult {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(KeyColumnUsageResult {
            table_schema: row.try_get(0)?,
            table_name: row.try_get(1)?,
            constraint_name: row.try_get(2)?,
            column_name: row.try_get(3)?,
            ordinal_position: row.try_get(4)?,
            position_in_unique_constraint: row.try_get(5)?,
            key_type: ConstraintType::from_str(row.try_get(6)?)?,
            nulls_distinct: match row.try_get::<usize, Option<&str>>(7)? {
                Some("YES") => Some(true),
                Some("NO") => Some(false),
                _ => None,
            },
        })
    }
}


#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum ConstraintType {
    PrimaryKey,
    ForeignKey,
    Check,
    Unique,
}

impl FromStr for ConstraintType {
    type Err = crate::ElefantToolsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "PRIMARY KEY" => Ok(ConstraintType::PrimaryKey),
            "FOREIGN KEY" => Ok(ConstraintType::ForeignKey),
            "CHECK" => Ok(ConstraintType::Check),
            "UNIQUE" => Ok(ConstraintType::Unique),
            _ => Err(crate::ElefantToolsError::UnknownConstraintType(
                s.to_string(),
            )),
        }
    }
}


//language=postgresql
define_working_query!(get_key_columns,KeyColumnUsageResult, r#"
select kcu.table_schema, kcu.table_name, kcu.constraint_name, kcu.column_name, kcu.ordinal_position, kcu.position_in_unique_constraint, tc.constraint_type, tc.nulls_distinct from information_schema.key_column_usage kcu
join information_schema.table_constraints tc on kcu.table_schema = tc.table_schema and kcu.table_name = tc.table_name and kcu.constraint_name = tc.constraint_name
where tc.constraint_type = 'PRIMARY KEY' or tc.constraint_type = 'UNIQUE'
order by kcu.table_schema, kcu.table_name, kcu.constraint_name, kcu.ordinal_position;
"#);

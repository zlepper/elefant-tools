use crate::{PostgresSchema, PostgresTable};

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresColumn {
    pub name: String,
    pub ordinal_position: i32,
    pub is_nullable: bool,
    pub data_type: String,
    pub default_value: Option<String>,
    pub generated: Option<String>,
}

impl PostgresColumn {
    pub fn get_alter_table_set_default_statement(&self, table: &PostgresTable, schema: &PostgresSchema) -> Option<String> {
        self.default_value.as_ref().map(|default_value| format!("alter table {}.{} alter column {} set default {};", schema.name, table.name, self.name, default_value))
    }
}

impl PostgresColumn {
    pub fn get_simplified_data_type(&self) -> SimplifiedDataType {
        match self.data_type.as_str() {
            "bigint"|"integer"|"smallint"|"real"|"double precision" => SimplifiedDataType::Number,
            "boolean" => SimplifiedDataType::Bool,
            _ => SimplifiedDataType::Text,
        }
    }
}

impl Default for PostgresColumn {
    fn default() -> Self {
        Self {
            name: "".to_string(),
            ordinal_position: 0,
            is_nullable: true,
            data_type: "".to_string(),
            default_value: None,
            generated: None,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum SimplifiedDataType {
    Number,
    Text,
    Bool,
}

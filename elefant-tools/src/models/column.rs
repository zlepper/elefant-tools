use AttemptedKeywordUsage::{Other};
use crate::{PostgresSchema, PostgresTable};
use crate::object_id::ObjectId;
use crate::quoting::{AttemptedKeywordUsage, IdentifierQuoter, Quotable};

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct PostgresColumn {
    pub name: String,
    pub ordinal_position: i32,
    pub is_nullable: bool,
    pub data_type: String,
    pub default_value: Option<String>,
    pub generated: Option<String>,
    pub comment: Option<String>,
    pub array_dimensions: i32,
    pub object_id: ObjectId,
}

impl PostgresColumn {
    pub fn get_alter_table_set_default_statement(&self, table: &PostgresTable, schema: &PostgresSchema, identifier_quoter: &IdentifierQuoter) -> Option<String> {
        self.default_value.as_ref().map(|default_value| format!("alter table {}.{} alter column {} set default {};", schema.name.quote(identifier_quoter, Other), table.name.quote(identifier_quoter, Other), self.name.quote(identifier_quoter, Other), default_value))
    }
}

impl PostgresColumn {
    pub fn get_simplified_data_type(&self) -> SimplifiedDataType {
        if self.array_dimensions > 0 {
            return SimplifiedDataType::Text;
        }
        match self.data_type.as_str() {
            "int2"|"int4"|"int8"|"float4"|"float8" => SimplifiedDataType::Number,
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
            comment: None,
            array_dimensions: 0,
            object_id: ObjectId::default(),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum SimplifiedDataType {
    Number,
    Text,
    Bool,
}

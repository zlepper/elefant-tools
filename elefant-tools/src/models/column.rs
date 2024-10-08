use crate::quoting::{AttemptedKeywordUsage, IdentifierQuoter, Quotable};
use crate::{ElefantToolsError, PostgresSchema, PostgresTable};
use serde::{Deserialize, Serialize};
use AttemptedKeywordUsage::Other;
use crate::postgres_client_wrapper::FromPgChar;

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct PostgresColumn {
    pub name: String,
    pub ordinal_position: i32,
    pub is_nullable: bool,
    pub data_type: String,
    pub default_value: Option<String>,
    pub generated: Option<String>,
    pub comment: Option<String>,
    pub array_dimensions: i32,
    pub data_type_length: Option<i32>,
    pub identity: Option<ColumnIdentity>,
}

impl PostgresColumn {
    pub fn get_alter_table_set_default_statement(
        &self,
        table: &PostgresTable,
        schema: &PostgresSchema,
        identifier_quoter: &IdentifierQuoter,
    ) -> Option<String> {
        self.default_value.as_ref().map(|default_value| {
            format!(
                "alter table {}.{} alter column {} set default {};",
                schema.name.quote(identifier_quoter, Other),
                table.name.quote(identifier_quoter, Other),
                self.name.quote(identifier_quoter, Other),
                default_value
            )
        })
    }
}

impl PostgresColumn {
    pub fn get_simplified_data_type(&self) -> SimplifiedDataType {
        if self.array_dimensions > 0 {
            return SimplifiedDataType::Text;
        }
        match self.data_type.as_str() {
            "int2" | "int4" | "int8" | "float4" | "float8" => SimplifiedDataType::Number,
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
            data_type_length: None,
            identity: None,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub enum SimplifiedDataType {
    Number,
    Text,
    Bool,
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub enum ColumnIdentity {
    GeneratedAlways,
    GeneratedByDefault
}

impl FromPgChar for ColumnIdentity {
    fn from_pg_char(c: char) -> Result<Self, ElefantToolsError> {
        match c {
            'a' => Ok(ColumnIdentity::GeneratedAlways),
            'd' => Ok(ColumnIdentity::GeneratedByDefault),
            _ => Err(ElefantToolsError::UnknownColumnIdentity(c.to_string())),
        }
    }
}
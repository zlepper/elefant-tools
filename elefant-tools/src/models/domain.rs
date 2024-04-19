use serde::{Deserialize, Serialize};
use crate::{IdentifierQuoter, ObjectId, PostgresSchema};
use crate::quoting::{AttemptedKeywordUsage, Quotable, quote_value_string};

#[derive(Debug, Eq, PartialEq, Clone, Default, Serialize, Deserialize)]
pub struct PostgresDomain {
    pub name: String,
    pub object_id: ObjectId,
    pub base_type_name: String,
    pub default_value: Option<String>,
    pub constraint: Option<PostgresDomainConstraint>,
    pub not_null: bool,
    pub description: Option<String>,
    pub depends_on: Vec<ObjectId>,
    pub data_type_length: Option<i32>
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct PostgresDomainConstraint {
    pub name: String,
    pub definition: String,
}

impl PostgresDomain {
    pub fn get_create_sql(&self, schema: &PostgresSchema, identifier_quoter: &IdentifierQuoter) -> String {
        let mut sql = format!("create domain {}.{} as {}", schema.name.quote(identifier_quoter, AttemptedKeywordUsage::TypeOrFunctionName), self.name.quote(identifier_quoter, AttemptedKeywordUsage::TypeOrFunctionName), self.base_type_name);
        
        if let Some(length) = self.data_type_length {
            sql.push_str(&format!("({})", length));
        }
        if let Some(default_value) = &self.default_value {
            sql.push_str(&format!(" default {}", default_value));
        }
        if self.not_null {
            sql.push_str(" not null");
        }
        if let Some(constraint) = &self.constraint {
            sql.push_str(&format!(" constraint {} check {}", constraint.name.quote(identifier_quoter, AttemptedKeywordUsage::TypeOrFunctionName), constraint.definition));
        }
        sql.push(';');
        
        if let Some(description) = &self.description {
            sql.push_str(&format!("\ncomment on domain {}.{} is {};", schema.name.quote(identifier_quoter, AttemptedKeywordUsage::TypeOrFunctionName), self.name.quote(identifier_quoter, AttemptedKeywordUsage::TypeOrFunctionName), quote_value_string(description)));
        }
        
        sql
    }
}
use crate::object_id::ObjectId;
use crate::quoting::{IdentifierQuoter, Quotable};
use crate::quoting::AttemptedKeywordUsage::ColumnName;

#[derive(Debug, Eq, PartialEq, Default, Clone)]
pub struct PostgresExtension {
    pub name: String,
    pub schema_name: String,
    pub version: String,
    pub relocatable: bool,
    pub object_id: ObjectId,
}


impl PostgresExtension {
    pub fn get_create_statement(&self, identifier_quoter: &IdentifierQuoter) -> String {
        format!("create extension if not exists {};", self.name.quote(identifier_quoter, ColumnName))
    }
}
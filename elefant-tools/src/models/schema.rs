use crate::models::sequence::PostgresSequence;
use crate::models::table::PostgresTable;
use crate::models::view::PostgresView;
use crate::PostgresFunction;
use crate::quoting::{IdentifierQuoter, Quotable};

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresSchema {
    pub tables: Vec<PostgresTable>,
    pub sequences: Vec<PostgresSequence>,
    pub views: Vec<PostgresView>,
    pub functions: Vec<PostgresFunction>,
    pub name: String,
}

impl PostgresSchema {
    pub fn get_create_statement(&self, identifier_quoter: &IdentifierQuoter) -> String {
        format!("create schema if not exists {};", self.name.quote(identifier_quoter))
    }
}

impl Default for PostgresSchema {
    fn default() -> Self {
        Self {
            views: vec![],
            name: "".to_string(),
            tables: vec![],
            sequences: vec![],
            functions: vec![],
        }
    }
}
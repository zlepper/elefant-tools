use crate::models::sequence::PostgresSequence;
use crate::models::table::PostgresTable;
use crate::models::view::PostgresView;
use crate::{PostgresFunction, PostgresTrigger};
use crate::quoting::{IdentifierQuoter, Quotable, quote_value_string};

#[derive(Debug, Eq, PartialEq, Default)]
pub struct PostgresSchema {
    pub tables: Vec<PostgresTable>,
    pub sequences: Vec<PostgresSequence>,
    pub views: Vec<PostgresView>,
    pub functions: Vec<PostgresFunction>,
    pub triggers: Vec<PostgresTrigger>,
    pub name: String,
    pub comment: Option<String>,
}

impl PostgresSchema {
    pub fn get_create_statement(&self, identifier_quoter: &IdentifierQuoter) -> String {
        let mut sql = format!("create schema if not exists {};", self.name.quote(identifier_quoter));
        
        if let Some(comment) = &self.comment {
            sql.push_str("\ncomment on schema ");
            sql.push_str(&self.name.quote(identifier_quoter));
            sql.push_str(" is ");
            sql.push_str(&quote_value_string(comment));
            sql.push(';');
        }
        
        sql
    }
}

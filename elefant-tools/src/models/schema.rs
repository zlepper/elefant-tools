use crate::models::sequence::PostgresSequence;
use crate::models::table::PostgresTable;

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresSchema {
    pub tables: Vec<PostgresTable>,
    pub sequences: Vec<PostgresSequence>,
    pub name: String,
}

impl PostgresSchema {
    pub fn get_create_statement(&self) -> String {
        format!("create schema if not exists {};", self.name)
    }
}
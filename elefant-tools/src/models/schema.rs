use crate::models::table::PostgresTable;

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresSchema {
    pub tables: Vec<PostgresTable>,
    pub name: String,
}

impl PostgresSchema {
    pub fn get_create_statement(&self) -> String {
        format!("create schema if not exists {};", self.name)
    }
}

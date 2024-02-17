use itertools::Itertools;
use crate::quoting::{IdentifierQuoter, Quotable, quote_value_string};

#[derive(Debug, Eq, PartialEq, Default)]
pub struct PostgresEnum {
    pub name: String,
    pub values: Vec<String>,
    pub comment: Option<String>,
}

impl PostgresEnum {
    pub fn get_create_statement(&self, identifier_quoter: &IdentifierQuoter) -> String {
        let mut sql = format!("create type {} as enum (", self.name.quote(identifier_quoter));
        sql.push_str(&self.values.iter().map(|v| quote_value_string(v)).join(", "));
        sql.push_str(");");

        if let Some(comment) = &self.comment {
            sql.push_str("\ncomment on type ");
            sql.push_str(&self.name.quote(identifier_quoter));
            sql.push_str(" is ");
            sql.push_str(&quote_value_string(comment));
            sql.push(';');
        }

        sql
    }
}
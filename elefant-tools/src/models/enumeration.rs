use itertools::Itertools;
use crate::object_id::ObjectId;
use crate::quoting::{IdentifierQuoter, Quotable, quote_value_string};
use crate::quoting::AttemptedKeywordUsage::{TypeOrFunctionName};

#[derive(Debug, Eq, PartialEq, Default, Clone)]
pub struct PostgresEnum {
    pub name: String,
    pub values: Vec<String>,
    pub comment: Option<String>,
    pub object_id: ObjectId,
}

impl PostgresEnum {
    pub fn get_create_statement(&self, identifier_quoter: &IdentifierQuoter) -> String {
        let mut sql = format!("create type {} as enum (", self.name.quote(identifier_quoter, TypeOrFunctionName));
        sql.push_str(&self.values.iter().map(|v| quote_value_string(v)).join(", "));
        sql.push_str(");");

        if let Some(comment) = &self.comment {
            sql.push_str("\ncomment on type ");
            sql.push_str(&self.name.quote(identifier_quoter, TypeOrFunctionName));
            sql.push_str(" is ");
            sql.push_str(&quote_value_string(comment));
            sql.push(';');
        }

        sql
    }
}
use crate::object_id::ObjectId;
use crate::quoting::AttemptedKeywordUsage::ColumnName;
use crate::quoting::{quote_value_string, IdentifierQuoter, Quotable};
use crate::PostgresSchema;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct PostgresSequence {
    pub name: String,
    pub data_type: String,
    pub start_value: i64,
    pub increment: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub cache_size: i64,
    pub cycle: bool,
    pub last_value: Option<i64>,
    pub comment: Option<String>,
    pub object_id: ObjectId,
}

impl Default for PostgresSequence {
    fn default() -> Self {
        Self {
            name: String::new(),
            data_type: String::new(),
            start_value: 1,
            increment: 1,
            min_value: 1,
            max_value: 2147483647,
            cache_size: 1,
            cycle: false,
            last_value: None,
            comment: None,
            object_id: ObjectId::default(),
        }
    }
}

impl PostgresSequence {
    pub fn get_create_statement(
        &self,
        schema: &PostgresSchema,
        identifier_quoter: &IdentifierQuoter,
    ) -> String {
        let mut sql = format!(
            "create sequence {}.{} as {} increment by {} minvalue {} maxvalue {} start {} cache {}",
            schema.name.quote(identifier_quoter, ColumnName),
            self.name.quote(identifier_quoter, ColumnName),
            self.data_type,
            self.increment,
            self.min_value,
            self.max_value,
            self.start_value,
            self.cache_size
        );

        if self.cycle {
            sql.push_str(" cycle");
        }

        sql.push(';');

        if let Some(comment) = &self.comment {
            sql.push_str("\ncomment on sequence ");
            sql.push_str(&schema.name.quote(identifier_quoter, ColumnName));
            sql.push('.');
            sql.push_str(&self.name.quote(identifier_quoter, ColumnName));
            sql.push_str(" is ");
            sql.push_str(&quote_value_string(comment));
            sql.push(';');
        }

        sql
    }

    pub fn get_set_value_statement(
        &self,
        schema: &PostgresSchema,
        identifier_quoter: &IdentifierQuoter,
    ) -> Option<String> {
        self.last_value.map(|last_value| {
            format!(
                "select pg_catalog.setval('{}.{}', {}, true);",
                schema.name.quote(identifier_quoter, ColumnName),
                self.name.quote(identifier_quoter, ColumnName),
                last_value
            )
        })
    }
}

impl Ord for PostgresSequence {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}

impl PartialOrd for PostgresSequence {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

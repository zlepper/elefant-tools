use crate::models::enumeration::PostgresEnum;
use crate::models::sequence::PostgresSequence;
use crate::models::table::PostgresTable;
use crate::models::view::PostgresView;
use crate::object_id::ObjectId;
use crate::quoting::AttemptedKeywordUsage::ColumnName;
use crate::quoting::{quote_value_string, IdentifierQuoter, Quotable};
use crate::{PostgresAggregateFunction, PostgresDomain, PostgresFunction, PostgresTrigger};
use serde::{Deserialize, Serialize};

#[derive(Debug, Eq, PartialEq, Default, Clone, Serialize, Deserialize)]
pub struct PostgresSchema {
    pub tables: Vec<PostgresTable>,
    pub sequences: Vec<PostgresSequence>,
    pub views: Vec<PostgresView>,
    pub functions: Vec<PostgresFunction>,
    pub aggregate_functions: Vec<PostgresAggregateFunction>,
    pub triggers: Vec<PostgresTrigger>,
    pub enums: Vec<PostgresEnum>,
    pub name: String,
    pub comment: Option<String>,
    pub domains: Vec<PostgresDomain>,
    pub object_id: ObjectId,
}

impl PostgresSchema {
    pub fn get_create_statement(&self, identifier_quoter: &IdentifierQuoter) -> String {
        let mut sql = format!(
            "create schema if not exists {};",
            self.name.quote(identifier_quoter, ColumnName)
        );

        if let Some(comment) = &self.comment {
            sql.push_str("\ncomment on schema ");
            sql.push_str(&self.name.quote(identifier_quoter, ColumnName));
            sql.push_str(" is ");
            sql.push_str(&quote_value_string(comment));
            sql.push(';');
        }

        sql
    }

    pub(crate) fn try_get_table(&self, table_name: &str) -> Option<&PostgresTable> {
        self.tables.iter().find(|t| t.name == table_name)
    }
}

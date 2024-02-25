use std::cmp::Ordering;
use crate::{PostgresSchema, PostgresTable};
use crate::quoting::{IdentifierQuoter, Quotable, quote_value_string};
use crate::quoting::AttemptedKeywordUsage::ColumnName;

#[derive(Debug, Eq, PartialEq, Default)]
pub struct PostgresUniqueConstraint {
    pub name: String,
    pub unique_index_name: String,
    pub comment: Option<String>,
}

impl PartialOrd for PostgresUniqueConstraint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PostgresUniqueConstraint {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}

impl PostgresUniqueConstraint {
    pub fn get_create_statement(&self, table: &PostgresTable, schema: &PostgresSchema, quoter: &IdentifierQuoter) -> String {

        let mut sql = format!("alter table {}.{} add constraint {} unique using index {};", schema.name.quote(quoter, ColumnName), table.name.quote(quoter, ColumnName), self.name.quote(quoter, ColumnName), self.unique_index_name.quote(quoter, ColumnName));

        if let Some(comment) = &self.comment {
            sql.push_str("\ncomment on constraint ");
            sql.push_str(&self.name.quote(quoter, ColumnName));
            sql.push_str(" on ");
            sql.push_str(&schema.name.quote(quoter, ColumnName));
            sql.push_str(".");
            sql.push_str(&table.name.quote(quoter, ColumnName));
            sql.push_str(" is ");
            sql.push_str(&quote_value_string(comment));
            sql.push(';');
        }

        sql
    }
}
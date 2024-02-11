use std::cmp::Ordering;
use crate::{PostgresSchema, PostgresTable};
use crate::quoting::{IdentifierQuoter, Quotable};

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresUniqueConstraint {
    pub name: String,
    pub unique_index_name: String,
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

        format!("alter table {}.{} add constraint {} unique using index {};", schema.name.quote(quoter), table.name.quote(quoter), self.name.quote(quoter), self.unique_index_name.quote(quoter))
    }
}
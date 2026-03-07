use crate::object_id::ObjectId;
use crate::quoting::AttemptedKeywordUsage::ColumnName;
use crate::quoting::{IdentifierQuoter, Quotable};
use crate::{PostgresSchema, PostgresTable};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct PostgresNotNullConstraint {
    pub name: String,
    pub column_name: String,
    pub is_validated: bool,
    pub comment: Option<String>,
    pub object_id: ObjectId,
}

impl Default for PostgresNotNullConstraint {
    fn default() -> Self {
        Self {
            name: String::new(),
            column_name: String::new(),
            is_validated: true,
            comment: None,
            object_id: ObjectId::default(),
        }
    }
}

impl PostgresNotNullConstraint {
    pub fn get_create_statement(
        &self,
        table: &PostgresTable,
        schema: &PostgresSchema,
        identifier_quoter: &IdentifierQuoter,
    ) -> String {
        let mut sql = format!(
            "alter table {}.{} add constraint {} not null {}",
            schema.name.quote(identifier_quoter, ColumnName),
            table.name.quote(identifier_quoter, ColumnName),
            self.name.quote(identifier_quoter, ColumnName),
            self.column_name.quote(identifier_quoter, ColumnName),
        );

        if !self.is_validated {
            sql.push_str(" not valid");
        }

        sql.push(';');

        sql
    }
}

impl PartialOrd for PostgresNotNullConstraint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PostgresNotNullConstraint {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}

use std::cmp::Ordering;
use serde::{Deserialize, Serialize};
use crate::object_id::ObjectId;
use crate::whitespace_ignorant_string::WhitespaceIgnorantString;

#[derive(Debug, Eq, PartialEq, Default, Clone, Serialize, Deserialize)]
pub struct PostgresCheckConstraint {
    pub name: String,
    pub check_clause: WhitespaceIgnorantString,
    pub comment: Option<String>,
    pub object_id: ObjectId,
}

impl PartialOrd for PostgresCheckConstraint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PostgresCheckConstraint {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}

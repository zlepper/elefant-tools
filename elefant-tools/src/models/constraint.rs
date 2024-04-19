use serde::{Deserialize, Serialize};
use crate::models::check_constraint::PostgresCheckConstraint;
use crate::models::foreign_key::PostgresForeignKey;
use crate::models::unique_constraint::PostgresUniqueConstraint;

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PostgresConstraint {
    Check(PostgresCheckConstraint),
    ForeignKey(PostgresForeignKey),
    Unique(PostgresUniqueConstraint)
}

impl From<PostgresCheckConstraint> for PostgresConstraint {
    fn from(value: PostgresCheckConstraint) -> Self {
        PostgresConstraint::Check(value)
    }
}

impl From<PostgresForeignKey> for PostgresConstraint {
    fn from(value: PostgresForeignKey) -> Self {
        PostgresConstraint::ForeignKey(value)
    }
}

impl From<PostgresUniqueConstraint> for PostgresConstraint {
    fn from(value: PostgresUniqueConstraint) -> Self {
        PostgresConstraint::Unique(value)
    }
}
use crate::models::primary_key::PostgresPrimaryKey;
use crate::models::check_constraint::PostgresCheckConstraint;
use crate::models::unique_constraint::PostgresUniqueConstraint;

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum PostgresConstraint {
    PrimaryKey(PostgresPrimaryKey),
    Unique(PostgresUniqueConstraint),
    Check(PostgresCheckConstraint),
}

impl From<PostgresPrimaryKey> for PostgresConstraint {
    fn from(value: PostgresPrimaryKey) -> Self {
        PostgresConstraint::PrimaryKey(value)
    }
}

impl From<PostgresCheckConstraint> for PostgresConstraint {
    fn from(value: PostgresCheckConstraint) -> Self {
        PostgresConstraint::Check(value)
    }
}

impl From<PostgresUniqueConstraint> for PostgresConstraint {
    fn from(value: PostgresUniqueConstraint) -> Self {
        PostgresConstraint::Unique(value)
    }
}

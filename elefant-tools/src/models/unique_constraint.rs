use std::cmp::Ordering;

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresUniqueConstraint {
    pub name: String,
    pub columns: Vec<PostgresUniqueConstraintColumn>,
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

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresUniqueConstraintColumn {
    pub column_name: String,
    pub ordinal_position: i32,
}

impl PartialOrd for PostgresUniqueConstraintColumn {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PostgresUniqueConstraintColumn {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ordinal_position.cmp(&other.ordinal_position)
    }
}

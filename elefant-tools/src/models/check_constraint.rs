use std::cmp::Ordering;

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresCheckConstraint {
    pub name: String,
    pub check_clause: String,
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

use std::cmp::Ordering;

#[derive(Debug, Eq, PartialEq, Default)]
pub struct PostgresCheckConstraint {
    pub name: String,
    pub check_clause: String,
    pub comment: Option<String>
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

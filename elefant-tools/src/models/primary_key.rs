use std::cmp::Ordering;

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresPrimaryKey {
    pub name: String,
    pub columns: Vec<PostgresPrimaryKeyColumn>,
}

impl PartialOrd for PostgresPrimaryKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PostgresPrimaryKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresPrimaryKeyColumn {
    pub column_name: String,
    pub ordinal_position: i32,
}

impl PartialOrd for PostgresPrimaryKeyColumn {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PostgresPrimaryKeyColumn {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ordinal_position.cmp(&other.ordinal_position)
    }
}

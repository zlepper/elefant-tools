use std::cmp::Ordering;
use crate::{PostgresSchema, PostgresTable};

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresUniqueConstraint {
    pub name: String,
    pub columns: Vec<PostgresUniqueConstraintColumn>,
    pub distinct_nulls: bool,
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
    pub fn get_create_statement(&self, schema: &PostgresSchema, table: &PostgresTable) -> String {
        let mut s = format!("alter table {}.{} add constraint {} unique ", schema.name, table.name, self.name);


        if !self.distinct_nulls {
            s.push_str("nulls not distinct ")
        }

        s.push('(');

        for (index, column) in self.columns.iter().enumerate() {
            if index != 0 {
                s.push_str(", ");
            }
            s.push_str(&column.column_name);
        }

        s.push_str(");");

        s
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

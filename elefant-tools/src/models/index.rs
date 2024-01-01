use std::cmp::Ordering;
use crate::{PostgresSchema, PostgresTable};

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresIndex {
    pub name: String,
    pub columns: Vec<PostgresIndexColumn>,
    pub index_type: String,
}

impl Ord for PostgresIndex {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}

impl PartialOrd for PostgresIndex {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PostgresIndex {
    pub fn get_create_index_command(&self, schema: &PostgresSchema, table: &PostgresTable) -> String {
        let mut command = format!("create index {} on {}.{} using {} (", self.name, schema.name, table.name, self.index_type);

        for (i, column) in self.columns.iter().enumerate() {
            if i > 0 {
                command.push_str(", ");
            }

            command.push_str(&column.name);

            match column.direction {
                Some(PostgresIndexColumnDirection::Ascending) => {
                    command.push_str(" asc");
                },
                Some(PostgresIndexColumnDirection::Descending) => {
                    command.push_str(" desc");
                },
                _ => {},
            }

            match column.nulls_order {
                Some(PostgresIndexNullsOrder::First) => {
                    command.push_str(" nulls first");
                },
                Some(PostgresIndexNullsOrder::Last) => {
                    command.push_str(" nulls last");
                },
                _ => {}
            }
        }

        command.push_str(");");

        command
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresIndexColumn {
    pub name: String,
    pub ordinal_position: i32,
    pub direction: Option<PostgresIndexColumnDirection>,
    pub nulls_order: Option<PostgresIndexNullsOrder>,
}

impl Ord for PostgresIndexColumn {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ordinal_position.cmp(&other.ordinal_position)
    }
}

impl PartialOrd for PostgresIndexColumn {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum PostgresIndexColumnDirection {
    Ascending,
    Descending,
}

#[derive(Debug, Eq, PartialEq)]
pub enum PostgresIndexNullsOrder {
    First,
    Last,
}

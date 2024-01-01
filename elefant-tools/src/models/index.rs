use std::cmp::Ordering;
use crate::{PostgresSchema, PostgresTable};

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresIndex {
    pub name: String,
    pub columns: Vec<PostgresIndexColumn>,
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
        let mut command = format!("create index {} on {}.{} (", self.name, schema.name, table.name);

        for (i, column) in self.columns.iter().enumerate() {
            if i > 0 {
                command.push_str(", ");
            }

            command.push_str(&column.name);

            match column.direction {
                PostgresIndexColumnDirection::Ascending => {
                    command.push_str(" asc");
                },
                PostgresIndexColumnDirection::Descending => {
                    command.push_str(" desc");
                },
            }

            match column.nulls_order {
                PostgresIndexNullsOrder::First => {
                    command.push_str(" nulls first");
                },
                PostgresIndexNullsOrder::Last => {
                    command.push_str(" nulls last");
                },
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
    pub direction: PostgresIndexColumnDirection,
    pub nulls_order: PostgresIndexNullsOrder,
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

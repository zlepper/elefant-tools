use std::cmp::Ordering;
use crate::{PostgresColumn, PostgresSchema, PostgresTable};

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresSequence {
    pub name: String,
    pub data_type: String,
    pub start_value: i64,
    pub increment: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub cache_size: i64,
    pub cycle: bool,
    pub last_value: Option<i64>,
}

impl PostgresSequence {
    pub fn get_create_statement(&self, schema: &PostgresSchema) -> String {
        let mut sql = format!("create sequence {}.{} as {} increment by {} minvalue {} maxvalue {} start {} cache {}",
                              schema.name, self.name, self.data_type, self.increment, self.min_value, self.max_value, self.start_value, self.cache_size);

        if self.cycle {
            sql.push_str(" cycle");
        }

        sql.push(';');

        sql
    }

    pub fn get_set_value_statement(&self, schema: &PostgresSchema) -> Option<String> {
        self.last_value.map(|last_value| format!("select pg_catalog.setval('{}.{}', {}, true);", schema.name, self.name, last_value))
    }
}

impl Ord for PostgresSequence {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}

impl PartialOrd for PostgresSequence {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

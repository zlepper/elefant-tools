use std::cmp::Ordering;
use itertools::Itertools;
use crate::{PostgresSchema, PostgresTable};

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresForeignKey {
    pub name: String,
    pub columns: Vec<PostgresForeignKeyColumn>,
    pub referenced_schema: Option<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<PostgresForeignKeyColumn>,
}

impl PostgresForeignKey {
    pub fn get_create_statement(&self, table: &PostgresTable, schema: &PostgresSchema) -> String {
        let mut sql = format!("alter table {}.{} add constraint {} foreign key (",
                              schema.name, table.name, self.name);

        let columns = self.columns.iter()
            .sorted_by_key(|c| c.ordinal_position)
            .map(|c| c.name.as_str())
            .join(", ");

        sql.push_str(&columns);
        sql.push_str(") references ");
        let referenced_schema = self.referenced_schema.as_ref().unwrap_or(&schema.name);
        sql.push_str(referenced_schema);
        sql.push('.');
        sql.push_str(&self.referenced_table);
        sql.push_str(" (");

        let referenced_columns = self.referenced_columns.iter()
            .sorted_by_key(|c| c.ordinal_position)
            .map(|c| c.name.as_str())
            .join(", ");

        sql.push_str(&referenced_columns);
        sql.push_str(");");

        sql
    }
}

impl Ord for PostgresForeignKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}

impl PartialOrd for PostgresForeignKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresForeignKeyColumn {
    pub name: String,
    pub ordinal_position: i32,
}

impl Ord for PostgresForeignKeyColumn {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ordinal_position.cmp(&other.ordinal_position)
    }
}

impl PartialOrd for PostgresForeignKeyColumn {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

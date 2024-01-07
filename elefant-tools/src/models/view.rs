use crate::PostgresSchema;
use crate::quoting::{IdentifierQuoter, Quotable};

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresView {
    pub name: String,
    pub definition: String,
    pub columns: Vec<PostgresViewColumn>,
}

impl PostgresView {
    pub fn get_create_view_sql(&self, schema: &PostgresSchema, identifier_quoter: &IdentifierQuoter) -> String {
        let mut sql = format!("create view {}.{} (", schema.name.quote(identifier_quoter), self.name.quote(identifier_quoter));

        for (i, column) in self.columns.iter().enumerate() {
            if i != 0 {
                sql.push_str(", ");
            }

            sql.push_str(&column.name.quote(identifier_quoter));
        }

        sql.push_str(") as ");

        sql.push_str(&self.definition);

        sql
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresViewColumn {
    pub name: String,
    pub ordinal_position: i32,
}
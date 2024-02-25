use crate::PostgresSchema;
use crate::quoting::{quote_value_string, IdentifierQuoter, Quotable};
use crate::quoting::AttemptedKeywordUsage::ColumnName;

#[derive(Debug, Eq, PartialEq, Default)]
pub struct PostgresView {
    pub name: String,
    pub definition: String,
    pub columns: Vec<PostgresViewColumn>,
    pub comment: Option<String>,
    pub is_materialized: bool,
}

impl PostgresView {
    pub fn get_create_view_sql(&self, schema: &PostgresSchema, identifier_quoter: &IdentifierQuoter) -> String {

        let mut sql = "create".to_string();

        if self.is_materialized {
            sql.push_str(" materialized");
        }

        sql.push_str(" view ");
        sql.push_str(&schema.name.quote(identifier_quoter, ColumnName));
        sql.push('.');
        sql.push_str(&self.name.quote(identifier_quoter, ColumnName));
        sql.push_str(" (");

        for (i, column) in self.columns.iter().enumerate() {
            if i != 0 {
                sql.push_str(", ");
            }

            sql.push_str(&column.name.quote(identifier_quoter, ColumnName));
        }

        sql.push_str(") as ");

        sql.push_str(&self.definition);

        if let Some(comment) = &self.comment {
            sql.push_str("\ncomment on ");
            if self.is_materialized {
                sql.push_str("materialized ");
            }
            sql.push_str("view ");
            sql.push_str(&schema.name.quote(identifier_quoter, ColumnName));
            sql.push('.');
            sql.push_str(&self.name.quote(identifier_quoter, ColumnName));
            sql.push_str(" is ");
            sql.push_str(&quote_value_string(comment));
            sql.push(';');

        }

        sql
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresViewColumn {
    pub name: String,
    pub ordinal_position: i32,
}
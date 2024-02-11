use itertools::Itertools;
use crate::models::column::PostgresColumn;
use crate::models::constraint::PostgresConstraint;
use crate::{DataFormat, DdlQueryBuilder, PostgresIndexType};
use crate::models::index::PostgresIndex;
use crate::models::schema::PostgresSchema;
use crate::quoting::{IdentifierQuoter, Quotable, QuotableIter};

#[derive(Debug, Eq, PartialEq)]
pub struct PostgresTable {
    pub name: String,
    pub columns: Vec<PostgresColumn>,
    pub constraints: Vec<PostgresConstraint>,
    pub indices: Vec<PostgresIndex>,
}

impl Default for PostgresTable {
    fn default() -> Self {
        Self::new("")
    }
}

impl PostgresTable {
    pub fn new(name: &str) -> Self {
        PostgresTable {
            name: name.to_string(),
            columns: vec![],
            constraints: vec![],
            indices: vec![],
        }
    }

    pub fn get_create_statement(&self, schema: &PostgresSchema, identifier_quoter: &IdentifierQuoter) -> String {
        let mut query_builder = DdlQueryBuilder::new(identifier_quoter);
        let mut table_builder = query_builder.create_table(&schema.name, &self.name);


        for column in &self.columns {
            let mut column_builder = table_builder.column(&column.name, &column.data_type);

            if !column.is_nullable {
                column_builder.not_null();
            }

            if let Some(generated) = &column.generated {
                column_builder.generated(generated);
            }
        }

        for index in &self.indices {
            if index.index_constraint_type == PostgresIndexType::PrimaryKey {
                let columns = index.key_columns.iter().sorted_by_key(|c| c.ordinal_position).map(|c| c.name.as_str());
                table_builder.primary_key(&index.name, columns);
            }
        }

        for constraint in &self.constraints {
            match constraint {
                PostgresConstraint::Check(check) => {
                    table_builder.check_constraint(&check.name, &check.check_clause);
                }
                PostgresConstraint::ForeignKey(_) => {
                    // Deferred until last part of the transaction
                },
                PostgresConstraint::Unique(_) => {
                    // Deferred until last part of the transaction
                }
            }
        }


        query_builder.build()
    }

    pub fn get_copy_in_command(&self, schema: &PostgresSchema, data_format: &DataFormat, identifier_quoter: &IdentifierQuoter) -> String {
        let mut s = "copy ".to_string();
        s.push_str(&schema.name.quote(identifier_quoter));
        s.push('.');
        s.push_str(&self.name.quote(identifier_quoter));

        s.push_str(" (");

        let cols = self.get_copy_columns_expression(identifier_quoter);

        s.push_str(&cols);

        s.push_str(") from stdin with (format ");
        match data_format {
            DataFormat::Text => {
                s.push_str("text");
            }
            DataFormat::PostgresBinary { .. } => {
                s.push_str("binary");
            }
        }
        s.push_str(", header false);");

        s
    }

    pub fn get_copy_out_command(&self, schema: &PostgresSchema, data_format: &DataFormat, identifier_quoter: &IdentifierQuoter) -> String {
        let mut s = "copy ".to_string();
        s.push_str(&schema.name.quote(identifier_quoter));
        s.push('.');
        s.push_str(&self.name.quote(identifier_quoter));

        s.push_str(" (");

        let cols = self.get_copy_columns_expression(identifier_quoter);

        s.push_str(&cols);
        s.push_str(") ");

        s.push_str(" to stdout with (format ");
        match data_format {
            DataFormat::Text => {
                s.push_str("text");
            }
            DataFormat::PostgresBinary { .. } => {
                s.push_str("binary");
            }
        }
        s.push_str(", header false, encoding 'utf-8');");

        s
    }

    fn get_copy_columns_expression(&self, identifier_quoter: &IdentifierQuoter) -> String {
        self.columns.iter()
            .filter(|c| c.generated.is_none())
            .sorted_by_key(|c| c.ordinal_position)
            .map(|c| c.name.as_str())
            .quote(identifier_quoter)
            .join(", ")
    }
}
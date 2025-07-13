use crate::helpers::StringExt;
use crate::models::column::PostgresColumn;
use crate::models::constraint::PostgresConstraint;
use crate::models::hypertable_retention::HypertableRetention;
use crate::models::index::PostgresIndex;
use crate::models::schema::PostgresSchema;
use crate::object_id::ObjectId;
use crate::pg_interval::Interval;
use crate::postgres_client_wrapper::FromPgChar;
use crate::quoting::AttemptedKeywordUsage::{ColumnName, TypeOrFunctionName};
use crate::quoting::{
    quote_value_string, AttemptedKeywordUsage, IdentifierQuoter, Quotable, QuotableIter,
};
use crate::storage::DataFormat;
use crate::{default, ColumnIdentity, ElefantToolsError, HypertableCompression, PostgresIndexType};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

#[derive(Debug, Eq, PartialEq, Default, Clone, Serialize, Deserialize)]
pub struct PostgresTable {
    pub name: String,
    pub columns: Vec<PostgresColumn>,
    pub constraints: Vec<PostgresConstraint>,
    pub indices: Vec<PostgresIndex>,
    pub comment: Option<String>,
    pub storage_parameters: Vec<String>,
    pub table_type: TableTypeDetails,
    pub object_id: ObjectId,
    pub depends_on: Vec<ObjectId>,
}

impl PostgresTable {
    pub fn new(name: &str) -> Self {
        PostgresTable {
            name: name.to_string(),
            ..default()
        }
    }

    pub fn get_create_statement(
        &self,
        schema: &PostgresSchema,
        identifier_quoter: &IdentifierQuoter,
    ) -> String {
        let escaped_relation_name = format!(
            "{}.{}",
            schema.name.quote(identifier_quoter, ColumnName),
            self.name.quote(identifier_quoter, ColumnName)
        );
        let mut sql = "create table ".to_string();
        sql.push_str(&escaped_relation_name);

        if let TableTypeDetails::PartitionedChildTable {
            partition_expression,
            parent_table,
        } = &self.table_type
        {
            sql.push_str(" partition of ");
            sql.push_str(&parent_table.quote(identifier_quoter, ColumnName));
            sql.push(' ');
            sql.push_str(partition_expression);
        } else {
            sql.push_str(" (");

            let mut text_row_count = 0;

            for (column_index, column) in self.columns.iter().enumerate() {
                let column_position = (column_index + 1) as i32;

                if text_row_count > 0 {
                    sql.push(',');
                }
                sql.push_str("\n    ");
                sql.push_str(&column.name.quote(identifier_quoter, ColumnName));
                sql.push(' ');
                sql.push_str(&column.data_type.quote(identifier_quoter, ColumnName));

                if let Some(length) = column.data_type_length {
                    sql.push_str(&format!("({length})"));
                }

                for _ in 0..column.array_dimensions {
                    sql.push_str("[]");
                }

                if !column.is_nullable {
                    sql.push_str(" not null");
                }

                if let Some(generated) = &column.generated {
                    sql.push_str(" generated always as (");
                    sql.push_str(generated);
                    sql.push_str(") stored");
                }

                if let Some(identity) = &column.identity {
                    sql.push_str(" generated ");
                    match identity {
                        ColumnIdentity::GeneratedAlways => sql.push_str("always"),
                        ColumnIdentity::GeneratedByDefault => sql.push_str("by default"),
                    }
                    sql.push_str(" as identity");

                    if let Some(seq) = &schema.sequences.iter().find(|s| {
                        s.author_table.as_ref().is_some_and(|t| *t == self.name)
                            && s.author_table_column_position == Some(column_position)
                    }) {
                        sql.push_str(" ( sequence name ");
                        sql.push_str(&seq.name.quote(identifier_quoter, TypeOrFunctionName));
                        sql.push_str(" )");
                    }
                }

                text_row_count += 1;
            }

            for index in &self.indices {
                if index.index_constraint_type == PostgresIndexType::PrimaryKey {
                    if text_row_count > 0 {
                        sql.push(',');
                    }

                    sql.push_str("\n    constraint ");
                    sql.push_str(&index.name.quote(identifier_quoter, ColumnName));
                    sql.push_str(" primary key (");

                    // We don't need to escape the column names here as they are already escaped in the index definition.
                    sql.push_join(", ", index.key_columns.iter().map(|c| &c.name));
                    sql.push(')');
                    text_row_count += 1;
                }
            }

            for constraint in &self.constraints {
                if let PostgresConstraint::Check(check) = constraint {
                    if text_row_count > 0 {
                        sql.push(',');
                    }
                    sql.push_str("\n    constraint ");
                    sql.push_str(&check.name.quote(identifier_quoter, ColumnName));
                    sql.push_str(" check ");
                    sql.push_str(&check.check_clause);
                    text_row_count += 1;
                }
            }

            if let TableTypeDetails::PartitionedParentTable {
                partition_strategy,
                partition_columns,
                ..
            } = &self.table_type
            {
                sql.push_str("\n) partition by ");
                sql.push_str(match partition_strategy {
                    TablePartitionStrategy::Hash => "hash",
                    TablePartitionStrategy::List => "list",
                    TablePartitionStrategy::Range => "range",
                });
                sql.push_str(" (");

                match partition_columns {
                    PartitionedTableColumns::Columns(columns) => {
                        sql.push_join(
                            ", ",
                            columns
                                .iter()
                                .map(|c| c.quote(identifier_quoter, ColumnName)),
                        );
                    }
                    PartitionedTableColumns::Expression(expr) => {
                        sql.push_str(expr);
                    }
                }

                sql.push(')');
            } else if let TableTypeDetails::InheritedTable { parent_tables } = &self.table_type {
                sql.push_str("\n) inherits (");
                sql.push_join(
                    ", ",
                    parent_tables.iter().map(|c| {
                        c.quote(identifier_quoter, AttemptedKeywordUsage::TypeOrFunctionName)
                    }),
                );
                sql.push(')');
            } else {
                sql.push_str("\n)");
            }
        }

        if !self.storage_parameters.is_empty() {
            sql.push_str("\nwith (");
            sql.push_join(", ", self.storage_parameters.iter());
            sql.push(')');
        }

        sql.push(';');

        if let Some(c) = &self.comment {
            sql.push_str(&format!(
                "\ncomment on table {} is {};",
                escaped_relation_name,
                quote_value_string(c)
            ));
        }

        for col in &self.columns {
            if let Some(c) = &col.comment {
                sql.push_str(&format!(
                    "\ncomment on column {}.{} is {};",
                    escaped_relation_name,
                    col.name.quote(identifier_quoter, ColumnName),
                    quote_value_string(c)
                ));
            }
        }

        for constraint in &self.constraints {
            if let PostgresConstraint::Check(constraint) = constraint {
                if let Some(c) = &constraint.comment {
                    sql.push_str(&format!(
                        "\ncomment on constraint {} on {} is {};",
                        constraint.name.quote(identifier_quoter, ColumnName),
                        escaped_relation_name,
                        quote_value_string(c)
                    ));
                }
            }
        }

        if let TableTypeDetails::TimescaleHypertable {
            dimensions,
            compression: _,
            retention: _,
        } = &self.table_type
        {
            for index in &self.indices {
                if index.index_constraint_type == PostgresIndexType::PrimaryKey {
                    continue;
                }

                let create_index_sql =
                    index.get_create_index_command(schema, self, identifier_quoter);
                sql.push_str(&create_index_sql);
            }

            for constraint in &self.constraints {
                if let PostgresConstraint::Unique(uk) = constraint {
                    let create_constraint_sql =
                        uk.get_create_statement(self, schema, identifier_quoter);
                    sql.push_str(&create_constraint_sql);
                }
            }

            // We don't need timescale to create the indices as we do it later on again based on what was exported.
            for (idx, dim) in dimensions.iter().enumerate() {
                match dim {
                    HypertableDimension::Time {
                        column_name,
                        time_interval,
                    } => {
                        if idx == 0 {
                            sql.push_str(&format!("\nselect public.create_hypertable('{}', by_range('{}', INTERVAL '{}'), create_default_indexes => false);", escaped_relation_name, column_name.quote(identifier_quoter, ColumnName), time_interval.to_postgres()));
                        } else {
                            sql.push_str(&format!("\nselect public.add_dimension('{}', by_range('{}', INTERVAL '{}'));", escaped_relation_name, column_name.quote(identifier_quoter, ColumnName), time_interval.to_postgres()));
                        }
                    }
                    HypertableDimension::SpaceInterval {
                        column_name,
                        integer_interval,
                    } => {
                        if idx == 0 {
                            sql.push_str(&format!("\nselect public.create_hypertable('{}', by_range('{}', {}), create_default_indexes => false);", escaped_relation_name, column_name.quote(identifier_quoter, ColumnName), integer_interval));
                        } else {
                            sql.push_str(&format!(
                                "\nselect public.add_dimension('{}', by_range('{}', {}));",
                                escaped_relation_name,
                                column_name.quote(identifier_quoter, ColumnName),
                                integer_interval
                            ));
                        }
                    }
                    HypertableDimension::SpacePartitions {
                        column_name,
                        num_partitions,
                    } => {
                        if idx == 0 {
                            sql.push_str(&format!("\nselect public.create_hypertable('{}', by_hash('{}', {}), create_default_indexes => false);", escaped_relation_name, column_name.quote(identifier_quoter, ColumnName), num_partitions));
                        } else {
                            sql.push_str(&format!(
                                "\nselect public.add_dimension('{}', by_hash('{}', {}));",
                                escaped_relation_name,
                                column_name.quote(identifier_quoter, ColumnName),
                                num_partitions
                            ));
                        }
                    }
                }
            }
        }

        sql
    }

    pub fn get_copy_in_command(
        &self,
        schema: &PostgresSchema,
        data_format: &DataFormat,
        identifier_quoter: &IdentifierQuoter,
    ) -> String {
        let mut s = "copy ".to_string();

        s.push_str(&schema.name.quote(identifier_quoter, ColumnName));
        s.push('.');
        s.push_str(&self.name.quote(identifier_quoter, ColumnName));

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

    pub fn get_copy_out_command(
        &self,
        schema: &PostgresSchema,
        data_format: &DataFormat,
        identifier_quoter: &IdentifierQuoter,
    ) -> String {
        let mut s = "copy ".to_string();

        if let TableTypeDetails::TimescaleHypertable { .. } = self.table_type {
            s.push_str("(select ");
            let cols = self.get_copy_columns_expression(identifier_quoter);

            s.push_str(&cols);
            s.push_str(" from ");

            s.push_str(&schema.name.quote(identifier_quoter, ColumnName));
            s.push('.');
            s.push_str(&self.name.quote(identifier_quoter, ColumnName));
            s.push_str(") ");
        } else {
            s.push_str(&schema.name.quote(identifier_quoter, ColumnName));
            s.push('.');
            s.push_str(&self.name.quote(identifier_quoter, ColumnName));

            s.push_str(" (");

            let cols = self.get_copy_columns_expression(identifier_quoter);

            s.push_str(&cols);
            s.push_str(") ");
        }

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
        self.get_writable_columns()
            .map(|c| c.name.as_str())
            .quote(identifier_quoter, ColumnName)
            .join(", ")
    }

    pub fn get_writable_columns(&self) -> impl Iterator<Item = &PostgresColumn> {
        self.columns
            .iter()
            .filter(|c| c.generated.is_none())
            .sorted_by_key(|c| c.ordinal_position)
    }

    pub fn get_timescale_post_settings(
        &self,
        schema: &PostgresSchema,
        identifier_quoter: &IdentifierQuoter,
    ) -> Option<String> {
        if let TableTypeDetails::TimescaleHypertable {
            compression,
            retention,
            ..
        } = &self.table_type
        {
            let escaped_relation_name = format!(
                "{}.{}",
                schema.name.quote(identifier_quoter, ColumnName),
                self.name.quote(identifier_quoter, ColumnName)
            );
            let mut sql = String::new();
            if let Some(compression) = compression {
                sql.push_str("alter table ");
                compression.add_compression_settings(
                    &mut sql,
                    &escaped_relation_name,
                    identifier_quoter,
                );
            }

            if let Some(retention) = retention {
                if !sql.is_empty() {
                    sql.push('\n');
                }

                retention.add_retention(&mut sql, &escaped_relation_name);
            }

            if !sql.is_empty() {
                return Some(sql);
            }
        }

        None
    }

    pub fn is_timescale_table(&self) -> bool {
        matches!(
            self.table_type,
            TableTypeDetails::TimescaleHypertable { .. }
        )
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TableTypeDetails {
    #[default]
    Table,
    PartitionedParentTable {
        partition_strategy: TablePartitionStrategy,
        default_partition_name: Option<String>,
        partition_columns: PartitionedTableColumns,
    },
    PartitionedChildTable {
        parent_table: String,
        partition_expression: String,
    },
    InheritedTable {
        parent_tables: Vec<String>,
    },
    TimescaleHypertable {
        dimensions: Vec<HypertableDimension>,
        compression: Option<HypertableCompression>,
        retention: Option<HypertableRetention>,
    },
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PartitionedTableColumns {
    Columns(Vec<String>),
    Expression(String),
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub enum TablePartitionStrategy {
    Hash,
    List,
    Range,
}

impl FromPgChar for TablePartitionStrategy {
    fn from_pg_char(c: char) -> Result<Self, ElefantToolsError> {
        match c {
            'h' => Ok(TablePartitionStrategy::Hash),
            'l' => Ok(TablePartitionStrategy::List),
            'r' => Ok(TablePartitionStrategy::Range),
            _ => Err(ElefantToolsError::InvalidTablePartitioningStrategy(
                c.to_string(),
            )),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum HypertableDimension {
    Time {
        column_name: String,
        time_interval: Interval,
    },
    SpaceInterval {
        column_name: String,
        integer_interval: i64,
    },
    SpacePartitions {
        column_name: String,
        num_partitions: i16,
    },
}

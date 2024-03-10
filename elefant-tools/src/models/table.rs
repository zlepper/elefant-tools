use itertools::Itertools;
use pg_interval::Interval;
use crate::models::column::PostgresColumn;
use crate::models::constraint::PostgresConstraint;
use crate::{default, ElefantToolsError, HypertableCompression, PostgresIndexType};
use crate::helpers::StringExt;
use crate::models::hypertable_retention::HypertableRetention;
use crate::models::index::PostgresIndex;
use crate::models::schema::PostgresSchema;
use crate::object_id::ObjectId;
use crate::postgres_client_wrapper::FromPgChar;
use crate::quoting::{IdentifierQuoter, Quotable, QuotableIter, quote_value_string};
use crate::quoting::AttemptedKeywordUsage::ColumnName;
use crate::storage::DataFormat;

#[derive(Debug, Eq, PartialEq, Default, Clone)]
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

    pub fn get_create_statement(&self, schema: &PostgresSchema, identifier_quoter: &IdentifierQuoter) -> String {

        let escaped_relation_name = format!("{}.{}", schema.name.quote(identifier_quoter, ColumnName), self.name.quote(identifier_quoter, ColumnName));
        let mut sql = "create table ".to_string();
        sql.push_str(&escaped_relation_name);

        if let TableTypeDetails::PartitionedChildTable {partition_expression, parent_table} = &self.table_type {
            sql.push_str(" partition of ");
            sql.push_str(&parent_table.quote(identifier_quoter, ColumnName));
            sql.push(' ');
            sql.push_str(partition_expression);
        } else {
            sql.push_str(" (");

            let mut text_row_count = 0;

            for column in &self.columns {
                if text_row_count > 0 {
                    sql.push(',');
                }
                sql.push_str("\n    ");
                sql.push_str(&column.name.quote(identifier_quoter, ColumnName));
                sql.push(' ');
                sql.push_str(&column.data_type.quote(identifier_quoter, ColumnName));

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

                    sql.push_join(", ", index.key_columns.iter().map(|c| c.name.quote(identifier_quoter, ColumnName)));
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

            if let TableTypeDetails::PartitionedParentTable {partition_strategy, partition_columns, ..} = &self.table_type {
                sql.push_str("\n) partition by ");
                sql.push_str(match partition_strategy {
                    TablePartitionStrategy::Hash => "hash",
                    TablePartitionStrategy::List => "list",
                    TablePartitionStrategy::Range => "range",
                });
                sql.push_str(" (");

                match partition_columns {
                    PartitionedTableColumns::Columns(columns) => {
                        sql.push_join(", ", columns.iter().map(|c| c.quote(identifier_quoter, ColumnName)));
                    }
                    PartitionedTableColumns::Expression(expr) => {
                        sql.push_str(expr);
                    }
                }

                sql.push(')');
            }
            else if let TableTypeDetails::InheritedTable {parent_tables} = &self.table_type {
                sql.push_str("\n) inherits (");
                sql.push_join(", ", parent_tables.iter().map(|c| c.quote(identifier_quoter, ColumnName)));
                sql.push(')');
            }
            else {
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
            sql.push_str(&format!("\ncomment on table {} is {};", escaped_relation_name, quote_value_string(c)));
        }

        for col in &self.columns {
            if let Some(c) = &col.comment {
                sql.push_str(&format!("\ncomment on column {}.{} is {};", escaped_relation_name, col.name.quote(identifier_quoter, ColumnName), quote_value_string(c)));
            }
        }

        for constraint in &self.constraints {
            if let PostgresConstraint::Check(constraint) = constraint {
                if let Some(c) = &constraint.comment {
                    sql.push_str(&format!("\ncomment on constraint {} on {} is {};", constraint.name.quote(identifier_quoter, ColumnName), escaped_relation_name, quote_value_string(c)));
                }
            }
        }

        if let TableTypeDetails::TimescaleHypertable {dimensions, compression, retention} = &self.table_type {
            // We don't need timescale to create the indices as we do it later on again based on what was exported.
            for (idx, dim) in dimensions.iter().enumerate() {
                match dim {
                    HypertableDimension::Time {column_name, time_interval} => {
                        if idx == 0 {
                            sql.push_str(&format!("\nselect public.create_hypertable('{}', by_range('{}', INTERVAL '{}'), create_default_indexes => false);", escaped_relation_name, column_name.quote(identifier_quoter, ColumnName), time_interval.to_postgres()));
                        } else {
                            sql.push_str(&format!("\nselect public.add_dimension('{}', by_range('{}', INTERVAL '{}'));", escaped_relation_name, column_name.quote(identifier_quoter, ColumnName), time_interval.to_postgres()));
                        }
                    }
                    HypertableDimension::SpaceInterval { column_name, integer_interval } => {
                        if idx == 0 {
                            sql.push_str(&format!("\nselect public.create_hypertable('{}', by_range('{}', {}), create_default_indexes => false);", escaped_relation_name, column_name.quote(identifier_quoter, ColumnName), integer_interval));
                        } else {
                            sql.push_str(&format!("\nselect public.add_dimension('{}', by_range('{}', {}));", escaped_relation_name, column_name.quote(identifier_quoter, ColumnName), integer_interval));
                        }
                    }
                    HypertableDimension::SpacePartitions { column_name, num_partitions } => {
                        if idx == 0 {
                            sql.push_str(&format!("\nselect public.create_hypertable('{}', by_hash('{}', {}), create_default_indexes => false);", escaped_relation_name, column_name.quote(identifier_quoter, ColumnName), num_partitions));
                        } else {
                            sql.push_str(&format!("\nselect public.add_dimension('{}', by_hash('{}', {}));", escaped_relation_name, column_name.quote(identifier_quoter, ColumnName), num_partitions));
                        }
                    }
                }
            }

            if let Some(compression) = compression {
                sql.push_str("\nalter table ");
                compression.add_compression_settings(&mut sql, &escaped_relation_name, identifier_quoter);
            }

            if let Some(retention) = retention {
                sql.push('\n');
                retention.add_retention(&mut sql, &escaped_relation_name);
            }
        }

        sql

    }

    pub fn get_copy_in_command(&self, schema: &PostgresSchema, data_format: &DataFormat, identifier_quoter: &IdentifierQuoter) -> String {
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

    pub fn get_copy_out_command(&self, schema: &PostgresSchema, data_format: &DataFormat, identifier_quoter: &IdentifierQuoter) -> String {
        let mut s = "copy ".to_string();
        
        if let TableTypeDetails::TimescaleHypertable {..} = self.table_type {
            s.push_str("(select ");
            let cols = self.get_copy_columns_expression(identifier_quoter);

            s.push_str(&cols);
            s.push_str(" from ");

            s.push_str(&schema.name.quote(identifier_quoter, ColumnName));
            s.push('.');
            s.push_str(&self.name.quote(identifier_quoter, ColumnName));
            s.push_str(") ");
        }  else {
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
        self.columns.iter()
            .filter(|c| c.generated.is_none())
            .sorted_by_key(|c| c.ordinal_position)
            .map(|c| c.name.as_str())
            .quote(identifier_quoter, ColumnName)
            .join(", ")
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Default)]
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
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum PartitionedTableColumns {
    Columns(Vec<String>),
    Expression(String),
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
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
            _ => Err(ElefantToolsError::InvalidTablePartitioningStrategy(c.to_string())),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
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


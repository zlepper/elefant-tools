use crate::models::PostgresSequence;
use crate::models::*;
use crate::postgres_client_wrapper::{PostgresClientWrapper};
use crate::Result;
use itertools::Itertools;
use ordered_float::NotNan;
use crate::schema_reader::table::TablesResult;
use crate::schema_reader::check_contraint::CheckConstraintResult;
use crate::schema_reader::foreign_key::ForeignKeyResult;
use crate::schema_reader::foreign_key_column::ForeignKeyColumnResult;
use crate::schema_reader::index::IndexResult;
use crate::schema_reader::index_column::IndexColumnResult;
use crate::schema_reader::key_column_usage::{ConstraintType, KeyColumnUsageResult};
use crate::schema_reader::table_column::TableColumnsResult;



#[cfg(test)]
pub mod tests;
mod table;
mod table_column;
mod key_column_usage;
mod check_contraint;
mod index_column;
mod index;
mod sequence;
mod foreign_key;
mod foreign_key_column;
mod view;
mod view_column;
mod function;


pub struct SchemaReader<'a> {
    connection: &'a PostgresClientWrapper,
}

impl SchemaReader<'_> {
    pub fn new(connection: &PostgresClientWrapper) -> SchemaReader {
        SchemaReader { connection }
    }

    pub async fn introspect_database(&self) -> Result<PostgresDatabase> {
        let tables = self.get_tables().await?;
        let columns = self.get_columns().await?;
        let key_columns = self.get_key_columns().await?;
        let check_constraints = self.get_check_constraints().await?;
        let indices = self.get_indices().await?;
        let index_columns = self.get_index_columns().await?;
        let sequences = self.get_sequences().await?;
        let foreign_keys = self.get_foreign_keys().await?;
        let foreign_key_columns = self.get_foreign_key_columns().await?;
        let views = self.get_views().await?;
        let view_columns = self.get_view_columns().await?;
        let functions = self.get_functions().await?;

        let mut db = PostgresDatabase { schemas: vec![] };

        for row in tables {
            let current_schema = db.get_or_create_schema_mut(&row.schema_name);

            let table = PostgresTable {
                name: row.table_name.clone(),
                columns: Self::add_columns(&columns, &row),
                constraints: Self::add_constraints(
                    &key_columns,
                    &check_constraints,
                    &foreign_keys,
                    &foreign_key_columns,
                    &row,
                ),
                indices: Self::add_indices(&indices, &index_columns, &row),
            };

            current_schema.tables.push(table);
        }

        for sequence in sequences {
            let current_schema = db.get_or_create_schema_mut(&sequence.schema_name);

            let sequence = PostgresSequence {
                name: sequence.sequence_name.clone(),
                data_type: sequence.data_type.clone(),
                start_value: sequence.start_value,
                increment: sequence.increment_by,
                min_value: sequence.min_value,
                max_value: sequence.max_value,
                cache_size: sequence.cache_size,
                cycle: sequence.cycle,
                last_value: sequence.last_value,
            };

            current_schema.sequences.push(sequence);
        }

        for view in &views {
            let current_schema = db.get_or_create_schema_mut(&view.schema_name);

            let view = PostgresView {
                name: view.view_name.clone(),
                definition: view.definition.clone(),
                columns: view_columns.iter().filter(|c| c.view_name == view.view_name && c.schema_name == view.schema_name).map(|c| PostgresViewColumn {
                    name: c.column_name.clone(),
                    ordinal_position: c.ordinal_position,
                }).collect(),
            };

            current_schema.views.push(view);
        }

        for function in &functions {
            let current_schema = db.get_or_create_schema_mut(&function.schema_name);

            let function = PostgresFunction {
                function_name: function.function_name.clone(),
                language: function.language_name.clone(),
                estimated_cost: NotNan::new(function.estimated_cost).unwrap_or(NotNan::new(100.0).unwrap()),
                estimated_rows: NotNan::new(function.estimated_rows).unwrap_or(NotNan::new(1000.0).unwrap()),
                support_function: function.support_function_name.clone(),
                kind: function.function_kind,
                security_definer: function.security_definer,
                leak_proof: function.leak_proof,
                strict: function.strict,
                returns_set: function.returns_set,
                volatility: function.volatility,
                parallel: function.parallel,
                sql_body: function.sql_body.clone(),
                configuration: function.configuration.clone(),
                arguments: function.arguments.clone(),
                result: function.result.clone(),
            };

            current_schema.functions.push(function);
        }

        Ok(db)
    }

    fn add_columns(columns: &[TableColumnsResult], row: &TablesResult) -> Vec<PostgresColumn> {
        columns
            .iter()
            .filter(|c| c.schema_name == row.schema_name && c.table_name == row.table_name)
            .map(|column| column.to_postgres_column())
            .collect()
    }

    fn add_constraints(
        key_columns: &[KeyColumnUsageResult],
        check_constraints: &[CheckConstraintResult],
        foreign_keys: &[ForeignKeyResult],
        foreign_key_columns: &[ForeignKeyColumnResult],
        row: &TablesResult,
    ) -> Vec<PostgresConstraint> {
        let key_columns = key_columns
            .iter()
            .filter(|c| c.table_schema == row.schema_name && c.table_name == row.table_name)
            .group_by(|c| (c.constraint_name.clone(), c.key_type));
        let mut constraints: Vec<PostgresConstraint> = key_columns
            .into_iter()
            .map(|g| (g.0.0, g.0.1, g.1.collect_vec()))
            .map(
                |(constraint_name, constraint_type, key_columns)| match constraint_type {
                    ConstraintType::PrimaryKey => PostgresPrimaryKey {
                        name: constraint_name.clone(),
                        columns: key_columns
                            .iter()
                            .map(|c| PostgresPrimaryKeyColumn {
                                column_name: c.column_name.clone(),
                                ordinal_position: c.ordinal_position,
                            })
                            .collect(),
                    }
                        .into(),
                    ConstraintType::ForeignKey => {
                        // These are handled separately, and thus this panic should never execute
                        unreachable!("Unexpected foreign key when handling key columns");
                    }
                    ConstraintType::Check => {
                        // These are handled separately, and thus this panic should never execute
                        unreachable!("Unexpected check constraint when handling key columns");
                    }
                    ConstraintType::Unique => PostgresUniqueConstraint {
                        name: constraint_name.clone(),
                        distinct_nulls: key_columns
                            .iter()
                            .any(|c| c.nulls_distinct.is_some_and(|v| v)),
                        columns: key_columns
                            .iter()
                            .map(|c| PostgresUniqueConstraintColumn {
                                column_name: c.column_name.clone(),
                                ordinal_position: c.ordinal_position,
                            })
                            .collect(),
                    }
                        .into(),
                },
            )
            .collect();

        let mut check_constraints = check_constraints
            .iter()
            .filter(|c| c.table_schema == row.schema_name && c.table_name == row.table_name)
            .map(|check_constraint| {
                PostgresCheckConstraint {
                    name: check_constraint.constraint_name.clone(),
                    check_clause: check_constraint.check_clause.clone(),
                }
                    .into()
            })
            .collect();

        constraints.append(&mut check_constraints);

        let mut foreign_key_constraints = foreign_keys
            .iter()
            .filter(|fk| {
                fk.source_table_schema_name == row.schema_name
                    && fk.source_table_name == row.table_name
            })
            .map(|fk| {
                PostgresForeignKey {
                    name: fk.constraint_name.clone(),
                    referenced_table: fk.target_table_name.clone(),
                    referenced_schema: if fk.source_table_schema_name == fk.target_table_schema_name
                    {
                        None
                    } else {
                        Some(fk.target_table_schema_name.clone())
                    },
                    delete_action: fk.delete_action,
                    update_action: fk.update_action,
                    columns: foreign_key_columns
                        .iter()
                        .filter(|c| {
                            c.source_table_name == row.table_name
                                && c.source_schema_name == row.schema_name
                                && c.constraint_name == fk.constraint_name
                        })
                        .enumerate()
                        .map(|(index, c)| PostgresForeignKeyColumn {
                            name: c.source_table_column_name.clone(),
                            ordinal_position: index as i32 + 1,
                            affected_by_delete_action: c.affected_by_delete_action,
                        })
                        .collect(),
                    referenced_columns: foreign_key_columns
                        .iter()
                        .filter(|c| {
                            c.source_table_name == row.table_name
                                && c.source_schema_name == row.schema_name
                                && c.constraint_name == fk.constraint_name
                        })
                        .enumerate()
                        .map(|(index, c)| PostgresForeignKeyReferencedColumn {
                            name: c.target_table_column_name.clone(),
                            ordinal_position: index as i32 + 1,
                        })
                        .collect(),
                }
                    .into()
            })
            .collect();

        constraints.append(&mut foreign_key_constraints);

        constraints.sort();

        constraints
    }

    fn add_indices(
        indices: &[IndexResult],
        index_columns: &[IndexColumnResult],
        row: &TablesResult,
    ) -> Vec<PostgresIndex> {
        let mut result = vec![];

        let indices = indices
            .iter()
            .filter(|c| c.table_schema == row.schema_name && c.table_name == row.table_name);
        for index in indices {
            let index_columns = index_columns
                .iter()
                .filter(|c| {
                    c.table_schema == row.schema_name
                        && c.table_name == row.table_name
                        && c.index_name == index.index_name
                })
                .collect_vec();
            let mut key_columns = index_columns
                .iter()
                .filter(|c| c.is_key)
                .map(|c| PostgresIndexKeyColumn {
                    name: c.column_expression.clone(),
                    ordinal_position: c.ordinal_position,
                    direction: if index.can_sort {
                        Some(match c.is_desc {
                            Some(true) => PostgresIndexColumnDirection::Descending,
                            _ => PostgresIndexColumnDirection::Ascending,
                        })
                    } else {
                        None
                    },
                    nulls_order: if index.can_sort {
                        Some(match c.nulls_first {
                            Some(true) => PostgresIndexNullsOrder::First,
                            _ => PostgresIndexNullsOrder::Last,
                        })
                    } else {
                        None
                    },
                })
                .collect_vec();

            key_columns.sort();

            let mut included_columns = index_columns
                .iter()
                .filter(|c| !c.is_key)
                .map(|c| PostgresIndexIncludedColumn {
                    name: c.column_expression.clone(),
                    ordinal_position: c.ordinal_position,
                })
                .collect_vec();

            included_columns.sort();

            result.push(PostgresIndex {
                name: index.index_name.clone(),
                key_columns,
                index_type: index.index_type.clone(),
                predicate: index.index_predicate.clone(),
                included_columns,
            });
        }

        result.sort();

        result
    }
}

macro_rules! define_working_query {
    ($fn_name:ident, $result:ident, $query:literal) => {
        impl $crate::schema_reader::SchemaReader<'_> {
            pub(in crate::schema_reader) async fn $fn_name(&self) -> $crate::Result<Vec<$result>> {
                self.connection.get_results($query).await
            }
        }
    };
}

pub(crate) use define_working_query;
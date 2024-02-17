use crate::models::PostgresSequence;
use crate::models::*;
use crate::postgres_client_wrapper::PostgresClientWrapper;
use crate::schema_reader::check_contraint::CheckConstraintResult;
use crate::schema_reader::foreign_key::ForeignKeyResult;
use crate::schema_reader::foreign_key_column::ForeignKeyColumnResult;
use crate::schema_reader::index::IndexResult;
use crate::schema_reader::index_column::IndexColumnResult;
use crate::schema_reader::table::TablesResult;
use crate::schema_reader::table_column::TableColumnsResult;
use crate::{ElefantToolsError, Result};
use itertools::Itertools;
use ordered_float::NotNan;

mod check_contraint;
mod enumeration;
mod extension;
mod foreign_key;
mod foreign_key_column;
mod function;
mod index;
mod index_column;
mod schema;
mod sequence;
mod table;
mod table_column;
#[cfg(test)]
pub mod tests;
mod trigger;
mod unique_constraint;
mod view;
mod view_column;

pub struct SchemaReader<'a> {
    connection: &'a PostgresClientWrapper,
}

impl SchemaReader<'_> {
    pub fn new(connection: &PostgresClientWrapper) -> SchemaReader {
        SchemaReader { connection }
    }

    pub async fn introspect_database(&self) -> Result<PostgresDatabase> {
        let schemas = self.get_schemas().await?;
        let tables = self.get_tables().await?;
        let columns = self.get_columns().await?;
        let check_constraints = self.get_check_constraints().await?;
        let unique_constraints = self.get_unique_constraints().await?;
        let indices = self.get_indices().await?;
        let index_columns = self.get_index_columns().await?;
        let sequences = self.get_sequences().await?;
        let foreign_keys = self.get_foreign_keys().await?;
        let foreign_key_columns = self.get_foreign_key_columns().await?;
        let views = self.get_views().await?;
        let view_columns = self.get_view_columns().await?;
        let functions = self.get_functions().await?;
        let extensions = self.get_extensions().await?;
        let triggers = self.get_triggers().await?;
        let enums = self.get_enums().await?;

        let mut db = PostgresDatabase::default();

        for row in schemas {
            let schema = PostgresSchema {
                name: row.name.clone(),
                comment: row.comment.clone(),
                ..Default::default()
            };

            db.schemas.push(schema);
        }

        for row in tables {
            let current_schema = db.get_or_create_schema_mut(&row.schema_name);

            let table = PostgresTable {
                name: row.table_name.clone(),
                columns: Self::add_columns(&columns, &row),
                constraints: Self::add_constraints(
                    &check_constraints,
                    &foreign_keys,
                    &foreign_key_columns,
                    &unique_constraints,
                    &row,
                ),
                indices: Self::add_indices(&indices, &index_columns, &row),
                comment: row.comment,
                table_type: if row.is_partition {
                    let parent_tables = row.parent_tables.ok_or_else(|| {
                        ElefantToolsError::PartitionedTableWithoutParent(row.table_name.clone())
                    })?;

                    if parent_tables.len() != 1 {
                        return Err(ElefantToolsError::PartitionedTableHasMultipleParent {
                            table: row.table_name.clone(),
                            parents: parent_tables.clone(),
                        })
                    }

                    TableTypeDetails::PartitionedChildTable {
                        parent_table: parent_tables[0].clone(),
                        partition_expression: row.partition_expression.ok_or_else(|| {
                            ElefantToolsError::PartitionedTableWithoutExpression(
                                row.table_name.clone(),
                            )
                        })?,
                    }
                } else if let Some(partition_stat) = &row.partition_strategy {
                    TableTypeDetails::PartitionedParentTable {
                        partition_strategy: *partition_stat,
                        default_partition_name: row.default_partition_name.clone(),
                        partition_columns: match (row.partition_column_indices, row.partition_expression_columns) {
                            (None, None) => return Err(ElefantToolsError::PartitionedTableWithoutPartitionColumns(row.table_name.clone())),
                            (None, Some(expr)) => PartitionedTableColumns::Expression(expr.clone()),
                            (Some(column_indices), None) => {
                                let column_names = column_indices.iter()
                                    .map(|idx| columns.iter()
                                        .find(|c| c.schema_name == row.schema_name && c.table_name == row.table_name && c.ordinal_position == *idx)
                                        .unwrap()
                                        .column_name
                                        .clone())
                                    .collect();
                                PartitionedTableColumns::Columns(column_names)
                            },
                            (Some(_), Some(_)) => return Err(ElefantToolsError::PartitionedTableWithBothPartitionColumnsAndExpression(row.table_name.clone())),
                        }
                    }
                } else if let Some(parent_table) = &row.parent_tables {
                    TableTypeDetails::InheritedTable {
                        parent_tables: parent_table.clone(),
                    }
                } else {
                    TableTypeDetails::Table
                },
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
                comment: sequence.comment,
            };

            current_schema.sequences.push(sequence);
        }

        for view in &views {
            let current_schema = db.get_or_create_schema_mut(&view.schema_name);

            let view = PostgresView {
                name: view.view_name.clone(),
                definition: view.definition.clone(),
                columns: view_columns
                    .iter()
                    .filter(|c| c.view_name == view.view_name && c.schema_name == view.schema_name)
                    .map(|c| PostgresViewColumn {
                        name: c.column_name.clone(),
                        ordinal_position: c.ordinal_position,
                    })
                    .collect(),
                comment: view.comment.clone(),
                is_materialized: view.is_materialized,
            };

            current_schema.views.push(view);
        }

        for function in &functions {
            let current_schema = db.get_or_create_schema_mut(&function.schema_name);

            let function = PostgresFunction {
                function_name: function.function_name.clone(),
                language: function.language_name.clone(),
                estimated_cost: NotNan::new(function.estimated_cost)
                    .unwrap_or(NotNan::new(100.0).unwrap()),
                estimated_rows: NotNan::new(function.estimated_rows)
                    .unwrap_or(NotNan::new(1000.0).unwrap()),
                support_function: function.support_function_name.clone(),
                kind: function.function_kind,
                security_definer: function.security_definer,
                leak_proof: function.leak_proof,
                strict: function.strict,
                returns_set: function.returns_set,
                volatility: function.volatility,
                parallel: function.parallel,
                sql_body: function.sql_body.trim().to_string(),
                configuration: function.configuration.clone(),
                arguments: function.arguments.clone(),
                result: function.result.clone(),
                comment: function.comment.clone(),
            };

            current_schema.functions.push(function);
        }

        for extension in &extensions {
            let extension = PostgresExtension {
                name: extension.extension_name.clone(),
                schema_name: extension.extension_schema_name.clone(),
                version: extension.extension_version.clone(),
                relocatable: extension.extension_relocatable,
            };

            db.enabled_extensions.push(extension);
        }

        for trigger in triggers {
            let current_schema = db.get_or_create_schema_mut(&trigger.schema_name);

            let trigger = PostgresTrigger {
                name: trigger.name.clone(),
                table_name: trigger.table_name.clone(),
                event: trigger.event,
                timing: trigger.timing,
                level: trigger.level,
                function_name: trigger.function_name.clone(),
                condition: trigger.condition.clone(),
                comment: trigger.comment.clone(),
                old_table_name: trigger.old_table_name.clone(),
                new_table_name: trigger.new_table_name.clone(),
            };

            current_schema.triggers.push(trigger);
        }

        for enumeration in enums {
            let current_schema = db.get_or_create_schema_mut(&enumeration.schema_name);

            let enumeration = PostgresEnum {
                name: enumeration.name.clone(),
                values: enumeration.values.clone(),
                comment: enumeration.comment.clone(),
            };

            current_schema.enums.push(enumeration);
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
        check_constraints: &[CheckConstraintResult],
        foreign_keys: &[ForeignKeyResult],
        foreign_key_columns: &[ForeignKeyColumnResult],
        unique_constraints: &[UniqueConstraintResult],
        row: &TablesResult,
    ) -> Vec<PostgresConstraint> {
        let mut constraints: Vec<PostgresConstraint> = check_constraints
            .iter()
            .filter(|c| c.table_schema == row.schema_name && c.table_name == row.table_name)
            .map(|check_constraint| {
                PostgresCheckConstraint {
                    name: check_constraint.constraint_name.clone(),
                    check_clause: check_constraint.check_clause.clone(),
                    comment: check_constraint.comment.clone(),
                }
                .into()
            })
            .collect();

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
                    comment: fk.comment.clone(),
                }
                .into()
            })
            .collect();

        constraints.append(&mut foreign_key_constraints);

        let mut unique_constraints = unique_constraints
            .iter()
            .filter(|c| c.table_schema == row.schema_name && c.table_name == row.table_name)
            .map(|c| PostgresUniqueConstraint {
                name: c.constraint_name.clone(),
                unique_index_name: c.index_name.clone(),
                comment: c.comment.clone(),
            })
            .map(|c| c.into())
            .collect_vec();

        constraints.append(&mut unique_constraints);

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
                    direction: match (index.can_sort, c.is_desc) {
                        (true, Some(true)) => Some(PostgresIndexColumnDirection::Descending),
                        (true, Some(false)) => Some(PostgresIndexColumnDirection::Ascending),
                        _ => None,
                    },
                    nulls_order: match (index.can_sort, c.nulls_first) {
                        (true, Some(true)) => Some(PostgresIndexNullsOrder::First),
                        (true, Some(false)) => Some(PostgresIndexNullsOrder::Last),
                        _ => None,
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
                index_constraint_type: match (index.is_primary_key, index.is_unique) {
                    (true, _) => PostgresIndexType::PrimaryKey,
                    (_, true) => PostgresIndexType::Unique {
                        nulls_distinct: !index.nulls_not_distinct,
                    },
                    _ => PostgresIndexType::Index,
                },
                comment: index.comment.clone(),
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

use crate::schema_reader::unique_constraint::UniqueConstraintResult;
pub(crate) use define_working_query;

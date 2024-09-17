use crate::models::PostgresSequence;
use crate::models::*;
use crate::object_id::ObjectIdGenerator;
use crate::postgres_client_wrapper::PostgresClientWrapper;
use crate::schema_reader::check_constraint::CheckConstraintResult;
use crate::schema_reader::foreign_key::ForeignKeyResult;
use crate::schema_reader::foreign_key_column::ForeignKeyColumnResult;
use crate::schema_reader::index::IndexResult;
use crate::schema_reader::index_column::IndexColumnResult;
use crate::schema_reader::table::TablesResult;
use crate::schema_reader::table_column::TableColumnsResult;
use crate::schema_reader::timescale_continuous_aggregate::ContinuousAggregateResult;
use crate::schema_reader::timescale_hypertable::HypertableResult;
use crate::schema_reader::timescale_hypertable_dimension::TimescaleHypertableDimensionResult;
use crate::schema_reader::unique_constraint::UniqueConstraintResult;
use crate::schema_reader::view::ViewResult;
use crate::schema_reader::view_column::ViewColumnResult;
use crate::TableTypeDetails::TimescaleHypertable;
use crate::{ElefantToolsError, ObjectId, Result};
use futures::try_join;
use std::collections::HashMap;

use itertools::Itertools;
use ordered_float::NotNan;
use tracing::instrument;

mod check_constraint;
mod domain;
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
mod timescale_continuous_aggregate;
mod timescale_hypertable;
mod timescale_hypertable_dimension;
mod timescale_job;
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

    #[instrument(skip_all)]
    pub async fn introspect_database(&self) -> Result<PostgresDatabase> {
        let mut object_id_generator = ObjectIdGenerator::new();
        let mut object_id_mapping = PgOidToObjectIdMapping::default();

        let (
            extensions,
            schemas,
            tables,
            columns,
            check_constraints,
            unique_constraints,
            indices,
            index_columns,
            sequences,
            foreign_keys,
            foreign_key_columns,
            views,
            view_columns,
            functions,
            triggers,
            enums,
            domains,
        ) = try_join!(
            self.get_extensions(),
            self.get_schemas(),
            self.get_tables(),
            self.get_columns(),
            self.get_check_constraints(),
            self.get_unique_constraints(),
            self.get_indices(),
            self.get_index_columns(),
            self.get_sequences(),
            self.get_foreign_keys(),
            self.get_foreign_key_columns(),
            self.get_views(),
            self.get_view_columns(),
            self.get_functions(),
            self.get_triggers(),
            self.get_enums(),
            self.get_domains()
        )?;

        let mut extensions = extensions;

        let mut db = PostgresDatabase::default();

        if extensions.iter().any(|e| e.extension_name == "timescaledb") {
            db.timescale_support.is_enabled = true;
            extensions.retain(|e| e.extension_name != "timescaledb");
        }

        if extensions
            .iter()
            .any(|e| e.extension_name == "timescaledb_toolkit")
        {
            db.timescale_support.timescale_toolkit_is_enabled = true;
            extensions.retain(|e| e.extension_name != "timescaledb_toolkit");
        }

        let (hypertables, hypertable_dimensions, continuous_aggregates, timescale_jobs) =
            if db.timescale_support.is_enabled {
                try_join!(
                    self.get_hypertables(),
                    self.get_hypertable_dimensions(),
                    self.get_continuous_aggregates(),
                    self.get_timescale_jobs()
                )?
            } else {
                (vec![], vec![], vec![], vec![])
            };

        for row in schemas {
            let schema = PostgresSchema {
                name: row.name.clone(),
                comment: row.comment.clone(),
                object_id: object_id_generator.next(),
                ..Default::default()
            };

            db.schemas.push(schema);
        }

        for table in &tables {
            let current_schema = db.get_or_create_schema_mut(&table.schema_name);

            let oid = table.oid;
            let type_oid = table.type_oid;

            let table = Self::add_table(
                table,
                &columns,
                &check_constraints,
                &unique_constraints,
                &indices,
                &index_columns,
                &foreign_keys,
                &foreign_key_columns,
                &hypertables,
                &hypertable_dimensions,
                &mut object_id_generator,
            )?;

            object_id_mapping.insert(oid, table.object_id);
            object_id_mapping.insert(type_oid, table.object_id);

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
                is_internally_created: sequence.is_internally_created,
                object_id: object_id_generator.next(),
            };

            current_schema.sequences.push(sequence);
        }

        let pg_stat_statements_enabled = extensions
            .iter()
            .any(|e| e.extension_name == "pg_stat_statements");

        for view in &views {
            if pg_stat_statements_enabled
                && view.schema_name == "public"
                && (view.view_name == "pg_stat_statements"
                    || view.view_name == "pg_stat_statements_info")
            {
                continue;
            }

            let current_schema = db.get_or_create_schema_mut(&view.schema_name);

            let oid = view.oid;
            let type_oid = view.type_oid;

            let view = Self::add_view(
                view,
                &view_columns,
                &continuous_aggregates,
                &mut object_id_generator,
            );

            object_id_mapping.insert(oid, view.object_id);
            object_id_mapping.insert(type_oid, view.object_id);

            current_schema.views.push(view);
        }

        for function in &functions {
            let current_schema = db.get_or_create_schema_mut(&function.schema_name);

            let oid = function.oid;

            if function.function_kind == FunctionKind::Aggregate {
                let function = PostgresAggregateFunction {
                    function_name: function.function_name.clone(),
                    arguments: function.arguments.clone(),
                    state_transition_function: function
                        .aggregate_state_transition_function
                        .clone()
                        .ok_or_else(|| {
                            ElefantToolsError::AggregateFunctionMissingTransitionFunction(
                                function.function_name.clone(),
                            )
                        })?,
                    final_function: function
                        .aggregate_final_function
                        .clone()
                        .and_then(none_if_irrelevant),
                    combine_function: function
                        .aggregate_combine_function
                        .clone()
                        .and_then(none_if_irrelevant),
                    serial_function: function
                        .aggregate_serial_function
                        .clone()
                        .and_then(none_if_irrelevant),
                    deserial_function: function
                        .aggregate_deserial_function
                        .clone()
                        .and_then(none_if_irrelevant),
                    moving_state_transition_function: function
                        .aggregate_moving_state_transition_function
                        .clone()
                        .and_then(none_if_irrelevant),
                    inverse_moving_state_transition_function: function
                        .aggregate_inverse_moving_state_transition_function
                        .clone()
                        .and_then(none_if_irrelevant),
                    moving_final_function: function
                        .aggregate_moving_final_function
                        .clone()
                        .and_then(none_if_irrelevant),
                    final_extra_data: function.aggregate_final_extra_data.unwrap_or_default(),
                    moving_final_extra_data: function
                        .aggregate_moving_final_extra_data
                        .unwrap_or_default(),
                    final_modify: function.aggregate_final_modify.unwrap_or_default(),
                    moving_final_modify: function.aggregate_moving_final_modify.unwrap_or_default(),
                    sort_operator: function
                        .aggregate_sort_operator
                        .clone()
                        .and_then(none_if_irrelevant),
                    transition_type: function.aggregate_transition_type.clone().ok_or_else(
                        || {
                            ElefantToolsError::AggregateFunctionMissingTransitionType(
                                function.function_name.clone(),
                            )
                        },
                    )?,
                    transition_space: function.aggregate_transition_space.and_then(none_if_zero),
                    moving_transition_type: function
                        .aggregate_moving_transition_type
                        .clone()
                        .and_then(none_if_irrelevant),
                    moving_transition_space: function
                        .aggregate_moving_transition_space
                        .and_then(none_if_zero),
                    initial_value: function.aggregate_initial_value.clone(),
                    moving_initial_value: function.aggregate_moving_initial_value.clone(),
                    parallel: function.parallel,
                    object_id: object_id_generator.next(),
                    depends_on: vec![],
                };

                object_id_mapping.insert(oid, function.object_id);

                current_schema.aggregate_functions.push(function);
            } else {
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
                    sql_body: function.sql_body.trim().into(),
                    configuration: function.configuration.clone(),
                    arguments: function.arguments.clone(),
                    result: function.result.clone(),
                    comment: function.comment.clone(),
                    object_id: object_id_generator.next(),
                    depends_on: vec![],
                };

                object_id_mapping.insert(oid, function.object_id);

                current_schema.functions.push(function);
            }
        }

        for extension in &extensions {
            let extension = PostgresExtension {
                name: extension.extension_name.clone(),
                schema_name: extension.extension_schema_name.clone(),
                version: extension.extension_version.clone(),
                relocatable: extension.extension_relocatable,
                object_id: object_id_generator.next(),
            };

            db.enabled_extensions.push(extension);
        }

        for trigger in triggers {
            if db.timescale_support.is_enabled
                && hypertables.iter().any(|h| {
                    h.table_name == trigger.table_name && h.table_schema == trigger.schema_name
                })
            {
                // Skip the trigger if it's a TimescaleDB internal trigger
                if trigger.name == "ts_insert_blocker"
                    || trigger.name == "ts_cagg_invalidation_trigger"
                {
                    continue;
                }
            }

            let current_schema = db.get_or_create_schema_mut(&trigger.schema_name);

            let trigger = PostgresTrigger {
                name: trigger.name.clone(),
                table_name: trigger.table_name.clone(),
                events: trigger.events.clone(),
                timing: trigger.timing,
                level: trigger.level,
                function_name: trigger.function_name.clone(),
                condition: trigger.condition.clone(),
                comment: trigger.comment.clone(),
                old_table_name: trigger.old_table_name.clone(),
                new_table_name: trigger.new_table_name.clone(),
                object_id: object_id_generator.next(),
                arguments: trigger.arguments.clone(),
            };

            current_schema.triggers.push(trigger);
        }

        for enumeration in enums {
            let current_schema = db.get_or_create_schema_mut(&enumeration.schema_name);

            let enumeration = PostgresEnum {
                name: enumeration.name.clone(),
                values: enumeration.values.clone(),
                comment: enumeration.comment.clone(),
                object_id: object_id_generator.next(),
            };

            current_schema.enums.push(enumeration);
        }

        for timescale_job in timescale_jobs {
            db.timescale_support
                .user_defined_jobs
                .push(TimescaleDbUserDefinedJob {
                    function_name: timescale_job.function_name.clone(),
                    function_schema: timescale_job.function_schema.clone(),
                    check_config_name: timescale_job.check_config_name.clone(),
                    check_config_schema: timescale_job.check_config_schema.clone(),
                    schedule_interval: timescale_job.schedule_interval,
                    fixed_schedule: timescale_job.fixed_schedule,
                    config: timescale_job.config.clone().map(|c| c.into()),
                    scheduled: timescale_job.scheduled,
                    object_id: object_id_generator.next(),
                })
        }

        for domain in &domains {
            let current_schema = db.get_or_create_schema_mut(&domain.schema_name);

            let oid = domain.domain_oid;

            let domain = PostgresDomain {
                name: domain.domain_name.clone(),
                base_type_name: domain.base_type_name.clone(),
                default_value: domain.default_value.clone(),
                not_null: domain.not_null,
                constraint: if let (Some(name), Some(definition)) =
                    (&domain.constraint_name, &domain.constraint_definition)
                {
                    Some(PostgresDomainConstraint {
                        name: name.clone(),
                        definition: definition.clone(),
                    })
                } else {
                    None
                },
                description: domain.description.clone(),
                object_id: object_id_generator.next(),
                depends_on: vec![],
                data_type_length: domain.data_type_length,
            };

            object_id_mapping.insert(oid, domain.object_id);

            current_schema.domains.push(domain);
        }

        for view in &views {
            if let Some(depends_on) = &view.depends_on {
                let current_schema = db.get_or_create_schema_mut(&view.schema_name);

                let own_object_id = object_id_mapping.get(view.oid).unwrap(); // SAFE: We have just inserted the oid above

                let this = current_schema
                    .views
                    .iter_mut()
                    .find(|v| v.object_id == own_object_id)
                    .unwrap(); // SAFE: We have just inserted it above

                for oid in depends_on {
                    if let Some(depends_on) = object_id_mapping.get(*oid) {
                        this.depends_on.push(depends_on);
                    }
                }
            }
        }

        for table in &tables {
            if let Some(depends_on) = &table.depends_on {
                let current_schema = db.get_or_create_schema_mut(&table.schema_name);

                let own_object_id = object_id_mapping.get(table.oid).unwrap(); // SAFE: We have just inserted the oid above

                let this = current_schema
                    .tables
                    .iter_mut()
                    .find(|v| v.object_id == own_object_id)
                    .unwrap(); // SAFE: We have just inserted it above

                for oid in depends_on {
                    if let Some(depends_on) = object_id_mapping.get(*oid) {
                        this.depends_on.push(depends_on);
                    }
                }
            }
        }

        for function in &functions {
            if let Some(depends_on) = &function.depends_on {
                let current_schema = db.get_or_create_schema_mut(&function.schema_name);

                let own_object_id = object_id_mapping.get(function.oid).unwrap(); // SAFE: We have just inserted the oid above

                if function.function_kind == FunctionKind::Aggregate {
                    let this = current_schema
                        .aggregate_functions
                        .iter_mut()
                        .find(|v| v.object_id == own_object_id)
                        .unwrap(); // SAFE: We have just inserted it above

                    for oid in depends_on {
                        if let Some(depends_on) = object_id_mapping.get(*oid) {
                            this.depends_on.push(depends_on);
                        }
                    }
                } else {
                    let this = current_schema
                        .functions
                        .iter_mut()
                        .find(|v| v.object_id == own_object_id)
                        .unwrap(); // SAFE: We have just inserted it above

                    for oid in depends_on {
                        if let Some(depends_on) = object_id_mapping.get(*oid) {
                            this.depends_on.push(depends_on);
                        }
                    }
                }
            }
        }

        for domain in &domains {
            if let Some(depends_on) = &domain.depends_on {
                let current_schema = db.get_or_create_schema_mut(&domain.schema_name);

                let own_object_id = object_id_mapping.get(domain.domain_oid).unwrap(); // SAFE: We have just inserted the oid above

                let this = current_schema
                    .domains
                    .iter_mut()
                    .find(|v| v.object_id == own_object_id)
                    .unwrap(); // SAFE: We have just inserted it above

                for oid in depends_on {
                    if let Some(depends_on) = object_id_mapping.get(*oid) {
                        this.depends_on.push(depends_on);
                    }
                }
            }
        }

        Ok(db)
    }

    #[instrument(skip_all)]
    fn add_view(
        view: &ViewResult,
        view_columns: &[ViewColumnResult],
        continuous_aggregates: &[ContinuousAggregateResult],
        object_id_generator: &mut ObjectIdGenerator,
    ) -> PostgresView {
        let continuous_aggregate = continuous_aggregates
            .iter()
            .find(|c| c.view_name == view.view_name && c.view_schema == view.schema_name);
        PostgresView {
            name: view.view_name.clone(),
            definition: if let Some(ca) = &continuous_aggregate {
                &ca.view_definition
            } else {
                &view.definition
            }
            .clone()
            .into(),
            columns: view_columns
                .iter()
                .filter(|c| c.view_name == view.view_name && c.schema_name == view.schema_name)
                .map(|c| PostgresViewColumn {
                    name: c.column_name.clone(),
                    ordinal_position: c.ordinal_position,
                })
                .collect(),
            comment: view.comment.clone(),
            is_materialized: view.is_materialized || continuous_aggregate.is_some(),
            view_options: if let Some(ca) = continuous_aggregate {
                let refresh = if let (Some(refresh), Some(start), Some(end)) = (
                    ca.refresh_interval,
                    ca.refresh_start_offset,
                    ca.refresh_end_offset,
                ) {
                    Some(TimescaleContinuousAggregateRefreshOptions {
                        interval: refresh,
                        start_offset: start,
                        end_offset: end,
                    })
                } else {
                    None
                };

                let compression = if let (false, None, None, None, None, None) = (
                    ca.compression_enabled,
                    ca.compress_after,
                    ca.compress_job_interval,
                    &ca.compress_segment_by,
                    &ca.compress_order_by,
                    &ca.compress_chunk_time_interval,
                ) {
                    None
                } else {
                    Some(HypertableCompression {
                        enabled: ca.compression_enabled,
                        compression_schedule_interval: ca.compress_job_interval,
                        chunk_time_interval: ca.compress_chunk_time_interval,
                        compress_after: ca.compress_after,
                        order_by_columns: Self::get_hypertable_compression_order_by_columns(
                            &ca.compress_order_by,
                            &ca.compress_order_by_desc,
                            &ca.compress_order_by_nulls_first,
                        ),
                        segment_by_columns: ca.compress_segment_by.clone(),
                    })
                };

                let retention = if let (Some(schedule_interval), Some(drop_after)) =
                    (ca.retention_schedule_interval, ca.retention_drop_after)
                {
                    Some(HypertableRetention {
                        schedule_interval,
                        drop_after,
                    })
                } else {
                    None
                };

                ViewOptions::TimescaleContinuousAggregate {
                    refresh,
                    compression,
                    retention,
                }
            } else {
                ViewOptions::None
            },
            object_id: object_id_generator.next(),
            depends_on: vec![],
        }
    }

    fn get_hypertable_compression_order_by_columns(
        compress_order_by: &Option<Vec<String>>,
        compress_order_by_desc: &Option<Vec<bool>>,
        compress_order_by_nulls_first: &Option<Vec<bool>>,
    ) -> Option<Vec<HypertableCompressionOrderedColumn>> {
        if let (Some(order_by), Some(desc), Some(nulls_first)) = (
            &compress_order_by,
            &compress_order_by_desc,
            &compress_order_by_nulls_first,
        ) {
            let cols = itertools::izip!(order_by, desc, nulls_first)
                .map(
                    |(column, desc, nulls_first)| HypertableCompressionOrderedColumn {
                        column_name: column.clone(),
                        descending: *desc,
                        nulls_first: *nulls_first,
                    },
                )
                .collect();

            Some(cols)
        } else {
            None
        }
    }

    #[instrument(skip_all)]
    #[allow(clippy::too_many_arguments)]
    fn add_table(
        row: &TablesResult,
        columns: &[TableColumnsResult],
        check_constraints: &[CheckConstraintResult],
        unique_constraints: &[UniqueConstraintResult],
        indices: &[IndexResult],
        index_columns: &[IndexColumnResult],
        foreign_keys: &[ForeignKeyResult],
        foreign_key_columns: &[ForeignKeyColumnResult],
        hypertables: &[HypertableResult],
        hypertable_dimensions: &[TimescaleHypertableDimensionResult],
        object_id_generator: &mut ObjectIdGenerator,
    ) -> Result<PostgresTable> {
        let table_columns = Self::add_columns(columns, row);

        let constraints = Self::add_constraints(
            check_constraints,
            foreign_keys,
            foreign_key_columns,
            unique_constraints,
            row,
            object_id_generator,
        );
        let indices = Self::add_indices(indices, index_columns, row, object_id_generator);

        let hypertable = hypertables
            .iter()
            .find(|h| h.table_name == row.table_name && h.table_schema == row.schema_name);

        let table_details = if let Some(hypertable) = hypertable {
            let mut dimensions = vec![];

            for dim in hypertable_dimensions.iter() {
                if dim.table_name == row.table_name && dim.table_schema == row.schema_name {
                    let dim = if let Some(interval) = dim.time_interval {
                        HypertableDimension::Time {
                            column_name: dim.column_name.clone(),
                            time_interval: interval,
                        }
                    } else if let Some(interval) = dim.integer_interval {
                        HypertableDimension::SpaceInterval {
                            column_name: dim.column_name.clone(),
                            integer_interval: interval,
                        }
                    } else if let Some(num_partitions) = dim.num_partitions {
                        HypertableDimension::SpacePartitions {
                            column_name: dim.column_name.clone(),
                            num_partitions,
                        }
                    } else {
                        return Err(ElefantToolsError::HypertableDimensionWithoutInterval {
                            table_name: row.table_name.clone(),
                            dimension_number: dim.dimension_number,
                        });
                    };

                    dimensions.push(dim);
                }
            }

            let compression = if let (false, None, None, None, None, None) = (
                hypertable.compression_enabled,
                hypertable.compress_after,
                hypertable.compression_chunk_interval,
                hypertable.compression_schedule_interval,
                &hypertable.compress_segment_by,
                &hypertable.compress_order_by,
            ) {
                None
            } else {
                Some(HypertableCompression {
                    enabled: hypertable.compression_enabled,
                    compression_schedule_interval: hypertable.compression_schedule_interval,
                    chunk_time_interval: hypertable.compression_chunk_interval,
                    compress_after: hypertable.compress_after,
                    order_by_columns: Self::get_hypertable_compression_order_by_columns(
                        &hypertable.compress_order_by,
                        &hypertable.compress_order_by_desc,
                        &hypertable.compress_order_by_nulls_first,
                    ),
                    segment_by_columns: hypertable.compress_segment_by.clone(),
                })
            };

            let retention = hypertable
                .retention_schedule_interval
                .zip(hypertable.retention_drop_after)
                .map(|(schedule_interval, drop_after)| HypertableRetention {
                    schedule_interval,
                    drop_after,
                });

            TimescaleHypertable {
                dimensions,
                compression,
                retention,
            }
        } else if row.is_partition {
            let parent_tables = row.parent_tables.clone().ok_or_else(|| {
                ElefantToolsError::PartitionedTableWithoutParent(row.table_name.clone())
            })?;

            if parent_tables.len() != 1 {
                return Err(ElefantToolsError::PartitionedTableHasMultipleParent {
                    table: row.table_name.clone(),
                    parents: parent_tables.clone(),
                });
            }

            TableTypeDetails::PartitionedChildTable {
                parent_table: parent_tables[0].clone(),
                partition_expression: row.partition_expression.clone().ok_or_else(|| {
                    ElefantToolsError::PartitionedTableWithoutExpression(row.table_name.clone())
                })?,
            }
        } else if let Some(partition_stat) = &row.partition_strategy {
            TableTypeDetails::PartitionedParentTable {
                partition_strategy: *partition_stat,
                default_partition_name: row.default_partition_name.clone(),
                partition_columns: match (
                    &row.partition_column_indices,
                    &row.partition_expression_columns,
                ) {
                    (None, None) => {
                        return Err(ElefantToolsError::PartitionedTableWithoutPartitionColumns(
                            row.table_name.clone(),
                        ));
                    }
                    (None, Some(expr)) => PartitionedTableColumns::Expression(expr.clone()),
                    (Some(column_indices), None) => {
                        let column_names = column_indices
                            .iter()
                            .map(|idx| {
                                columns
                                    .iter()
                                    .find(|c| {
                                        c.schema_name == row.schema_name
                                            && c.table_name == row.table_name
                                            && c.ordinal_position == *idx
                                    })
                                    .unwrap()
                                    .column_name
                                    .clone()
                            })
                            .collect();
                        PartitionedTableColumns::Columns(column_names)
                    }
                    (Some(_), Some(_)) => return Err(
                        ElefantToolsError::PartitionedTableWithBothPartitionColumnsAndExpression(
                            row.table_name.clone(),
                        ),
                    ),
                },
            }
        } else if let Some(parent_table) = &row.parent_tables {
            TableTypeDetails::InheritedTable {
                parent_tables: parent_table.clone(),
            }
        } else {
            TableTypeDetails::Table
        };

        let table = PostgresTable {
            name: row.table_name.clone(),
            columns: table_columns,
            constraints,
            indices,
            comment: row.comment.clone(),
            storage_parameters: row.storage_parameters.clone().unwrap_or_default(),
            table_type: table_details,
            object_id: object_id_generator.next(),
            depends_on: vec![],
        };

        Ok(table)
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
        object_id_generator: &mut ObjectIdGenerator,
    ) -> Vec<PostgresConstraint> {
        let mut constraints: Vec<PostgresConstraint> = check_constraints
            .iter()
            .filter(|c| c.table_schema == row.schema_name && c.table_name == row.table_name)
            .map(|check_constraint| {
                PostgresCheckConstraint {
                    name: check_constraint.constraint_name.clone(),
                    check_clause: check_constraint.check_clause.clone().into(),
                    comment: check_constraint.comment.clone(),
                    object_id: object_id_generator.next(),
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
                    object_id: object_id_generator.next(),
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
                object_id: object_id_generator.next(),
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
        object_id_generator: &mut ObjectIdGenerator,
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
                storage_parameters: index.storage_parameters.clone().unwrap_or_else(Vec::new),
                object_id: object_id_generator.next(),
            });
        }

        result.sort();

        result
    }
}

macro_rules! define_working_query {
    ($fn_name:ident, $result:ident, $query:literal) => {
        impl $crate::schema_reader::SchemaReader<'_> {
            #[tracing::instrument(skip_all)]
            pub(in crate::schema_reader) async fn $fn_name(&self) -> $crate::Result<Vec<$result>> {
                self.connection.get_results($query).await
            }
        }
    };
}

pub(crate) use define_working_query;

fn none_if_irrelevant(s: String) -> Option<String> {
    if s == "-" || s == "0" {
        None
    } else {
        Some(s)
    }
}

fn none_if_zero(i: i32) -> Option<i32> {
    if i == 0 {
        None
    } else {
        Some(i)
    }
}

#[derive(Debug, Default)]
struct PgOidToObjectIdMapping {
    mapping: HashMap<i64, ObjectId>,
}

impl PgOidToObjectIdMapping {
    fn insert(&mut self, oid: i64, object_id: ObjectId) {
        self.mapping.insert(oid, object_id);
    }

    fn get(&self, oid: i64) -> Option<ObjectId> {
        self.mapping.get(&oid).copied()
    }
}

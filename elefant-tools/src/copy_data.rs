use crate::object_id::DependencySortable;
use crate::parallel_runner::ParallelRunner;
use crate::quoting::IdentifierQuoter;
use crate::storage::DataFormat;
use crate::storage::{CopyDestination, CopySource};
use crate::*;
use itertools::Itertools;
use std::num::NonZeroUsize;
use tracing::{debug, info, instrument};

#[derive(Debug, Default)]
pub struct CopyDataOptions {
    /// Force this data format to be used
    pub data_format: Option<DataFormat>,
    /// How many tables to copy in parallel at most
    pub max_parallel: Option<NonZeroUsize>,

    /// The schema to inspect
    pub target_schema: Option<String>,

    /// If `target_schema` is specified it will be renamed to this
    /// when applied to the destination.
    pub rename_schema_to: Option<String>,

    /// Only the schema will be copied, but not any data
    pub schema_only: bool,

    /// Only the structures missing in the destination will be copied.
    /// Data copy is only checked against "empty table" vs "non-empty table".
    /// This only works with data sources that supports structural inspections, aka
    /// not sql-files.
    pub differential: bool,
}

const NON_ZERO_USIZE1: NonZeroUsize = unsafe {
    // SAFETY: 1 is not zero
    NonZeroUsize::new_unchecked(1)
};

impl CopyDataOptions {
    fn get_max_parallel_or_1(&self) -> NonZeroUsize {
        self.max_parallel.unwrap_or(NON_ZERO_USIZE1)
    }
}

/// Copies data and structures from the provided source to the destination.
///
/// This is probably the main function you want to deal with when using Elefant Tools as a library.
#[instrument(skip_all)]
pub async fn copy_data<'d, S: CopySourceFactory, D: CopyDestinationFactory<'d>>(
    source: &S,
    destination: &'d mut D,
    options: CopyDataOptions,
) -> Result<()> {
    let data_format = get_data_type(source, destination, &options).await?;

    let expected_parallelism = if options.get_max_parallel_or_1() == NON_ZERO_USIZE1 {
        SupportedParallelism::Sequential
    } else {
        source
            .supported_parallelism()
            .negotiate_parallelism(destination.supported_parallelism())
    };

    let (source, mut destination) = match expected_parallelism {
        SupportedParallelism::Sequential => (
            SequentialOrParallel::Sequential(source.create_sequential_source().await?),
            SequentialOrParallel::Sequential(destination.create_sequential_destination().await?),
        ),
        SupportedParallelism::Parallel => (
            source.create_source().await?,
            destination.create_destination().await?,
        ),
    };

    let definition = source.get_introspection().await?;
    let destination_definition = if options.differential {
        destination
            .try_get_introspeciton()
            .await?
            .unwrap_or_default()
    } else {
        default()
    };

    let source_definition = if let Some(target_schema) = &options.target_schema {
        definition.filtered_to_schema(target_schema)
    } else {
        definition
    };

    let target_definition = if let (Some(target_schema), Some(rename_to)) =
        (&options.target_schema, &options.rename_schema_to)
    {
        source_definition.with_renamed_schema(target_schema, rename_to)
    } else {
        source_definition.clone()
    };

    if let Some(target_schema) = &options.target_schema {
        destination_definition.filtered_to_schema(target_schema);
    }

    destination.begin_transaction().await?;

    match &mut destination {
        SequentialOrParallel::Sequential(ref mut d) => {
            apply_pre_copy_structure(d, &target_definition, &destination_definition).await?;
        }
        SequentialOrParallel::Parallel(ref mut d) => {
            apply_pre_copy_structure(d, &target_definition, &destination_definition).await?;
        }
    }

    destination.commit_transaction().await?;

    if !options.schema_only {
        let mut parallel_runner = ParallelRunner::new(options.get_max_parallel_or_1());

        for target_schema in &target_definition.schemas {
            let source_schema = source_definition
                .schemas
                .iter()
                .find(|s| s.object_id == target_schema.object_id);
            let source_schema = match source_schema {
                Some(s) => s,
                None => {
                    continue;
                }
            };

            for target_table in &target_schema.tables {
                if let TableTypeDetails::PartitionedParentTable { .. } = &target_table.table_type {
                    continue;
                }

                let source_table = source_schema
                    .tables
                    .iter()
                    .find(|t| t.object_id == target_table.object_id);
                let source_table = match source_table {
                    Some(s) => s,
                    None => {
                        continue;
                    }
                };

                match source {
                    SequentialOrParallel::Sequential(ref source) => match &mut destination {
                        SequentialOrParallel::Sequential(ref mut destination) => {
                            do_copy(
                                source,
                                destination,
                                target_schema,
                                target_table,
                                source_schema,
                                source_table,
                                &data_format,
                                &options,
                            )
                            .await?
                        }
                        SequentialOrParallel::Parallel(ref mut destination) => {
                            do_copy(
                                source,
                                destination,
                                target_schema,
                                target_table,
                                source_schema,
                                source_table,
                                &data_format,
                                &options,
                            )
                            .await?
                        }
                    },
                    SequentialOrParallel::Parallel(ref source) => match &mut destination {
                        SequentialOrParallel::Sequential(ref mut destination) => {
                            do_copy(
                                source,
                                destination,
                                target_schema,
                                target_table,
                                source_schema,
                                source_table,
                                &data_format,
                                &options,
                            )
                            .await?
                        }
                        SequentialOrParallel::Parallel(ref mut destination) => {
                            let source = source.clone();
                            let destination = destination.clone();
                            let df = data_format.clone();
                            let opt = &options;
                            parallel_runner
                                .enqueue(async move {
                                    let source = source;
                                    let mut destination = destination;
                                    do_copy(
                                        &source,
                                        &mut destination,
                                        target_schema,
                                        target_table,
                                        source_schema,
                                        source_table,
                                        &df,
                                        opt,
                                    )
                                    .await
                                })
                                .await?;
                        }
                    },
                }
            }
        }

        parallel_runner.run_remaining().await?;
    }

    match &mut destination {
        SequentialOrParallel::Sequential(ref mut destination) => {
            apply_post_copy_structure_sequential(
                destination,
                &target_definition,
                &destination_definition,
            )
            .await?;
        }
        SequentialOrParallel::Parallel(ref mut destination) => {
            apply_post_copy_structure_parallel(
                destination,
                &target_definition,
                &options,
                &destination_definition,
            )
            .await?;
        }
    }

    destination.finish().await?;

    Ok(())
}

/// Applies all structures needed to be able to actually insert data. This includes:
/// * Creating schemas
/// * Creating tables
/// * Creating functions
/// * Creating views
/// * Creating custom types
#[instrument(skip_all)]
async fn apply_pre_copy_structure<D: CopyDestination>(
    destination: &mut D,
    definition: &PostgresDatabase,
    target_definition: &PostgresDatabase,
) -> Result<()> {
    let identifier_quoter = destination.get_identifier_quoter();

    for schema in &definition.schemas {

        let target_schema = target_definition.try_get_schema(&schema.name);
        if target_schema.is_none() {
            destination
                .apply_transactional_statement(&schema.get_create_statement(&identifier_quoter))
                .await?;
        }

        if let Some(comment_statement) = schema.get_set_comment_statement(&identifier_quoter) {
            destination.apply_transactional_statement(&comment_statement).await?;
        }
    }

    for ext in &definition.enabled_extensions {
        if target_definition
            .enabled_extensions
            .iter()
            .any(|e| e.name == ext.name)
        {
            debug!("Extension {} already exists in destination", ext.name);
            continue;
        }

        destination
            .apply_transactional_statement(&ext.get_create_statement(&identifier_quoter))
            .await?;
    }

    for schema in &definition.schemas {
        let target_schema = target_definition.try_get_schema(&schema.name);

        for enumeration in &schema.enums {
            if target_schema.is_some_and(|s| s.enums.iter().any(|e| e.name == enumeration.name)) {
                debug!("Enum {} already exists in destination", enumeration.name);
                continue;
            }

            destination
                .apply_transactional_statement(
                    &enumeration.get_create_statement(&identifier_quoter),
                )
                .await?;
        }
    }

    let mut tables_and_functions: Vec<PostgresThingWithDependencies> = Vec::new();

    for schema in &definition.schemas {
        let target_schema = target_definition.try_get_schema(&schema.name);

        for function in &schema.functions {
            if target_schema.is_some_and(|s| {
                s.functions
                    .iter()
                    .any(|f| f.function_name == function.function_name)
            }) {
                debug!(
                    "Function {} already exists in destination",
                    function.function_name
                );
                continue;
            }

            tables_and_functions.push(PostgresThingWithDependencies::Function(function, schema));
        }

        for aggregate_function in &schema.aggregate_functions {
            if target_schema.is_some_and(|s| {
                s.aggregate_functions
                    .iter()
                    .any(|f| f.function_name == aggregate_function.function_name)
            }) {
                debug!(
                    "Aggregate function {} already exists in destination",
                    aggregate_function.function_name
                );
                continue;
            }

            tables_and_functions.push(PostgresThingWithDependencies::AggregateFunction(
                aggregate_function,
                schema,
            ));
        }

        for table in &schema.tables {
            if target_schema
                .and_then(|s| s.try_get_table(&table.name))
                .is_some()
            {
                debug!("Table {} already exists in destination", table.name);
                continue;
            }

            tables_and_functions.push(PostgresThingWithDependencies::Table(table, schema));
        }

        for view in &schema.views {
            if target_schema.is_some_and(|s| s.views.iter().any(|v| v.name == view.name)) {
                debug!("View {} already exists in destination", view.name);
                continue;
            }

            tables_and_functions.push(PostgresThingWithDependencies::View(view, schema));
        }

        for domain in &schema.domains {
            if target_schema.is_some_and(|s| s.domains.iter().any(|d| d.name == domain.name)) {
                debug!("Domain {} already exists in destination", domain.name);
                continue;
            }

            tables_and_functions.push(PostgresThingWithDependencies::Domain(domain, schema));
        }
    }

    let sorted = tables_and_functions.iter().sort_by_dependencies();

    for thing in sorted {
        let sql = thing.get_create_sql(&identifier_quoter);
        destination.apply_transactional_statement(&sql).await?;
    }

    Ok(())
}

/// Actually copies data between two tables.
#[instrument(skip_all)]
#[allow(clippy::too_many_arguments)]
async fn do_copy<S: CopySource, D: CopyDestination>(
    source: &S,
    destination: &mut D,
    target_schema: &PostgresSchema,
    target_table: &PostgresTable,
    source_schema: &PostgresSchema,
    source_table: &PostgresTable,
    data_format: &DataFormat,
    options: &CopyDataOptions,
) -> Result<()> {
    let has_data = options.differential
        && destination
            .has_data_in_table(target_schema, target_table)
            .await?;

    if !has_data {
        info!(
            "Skipping table {} as it already has data in the destination",
            target_table.name
        );
        let data = source
            .get_data(source_schema, source_table, data_format)
            .await?;

        destination
            .apply_data(target_schema, target_table, data)
            .await?;
    }

    Ok(())
}

/// Get instructions to apply after the data has been copied. This includes:
/// * Creating indexes
/// * Creating constraints
/// * Creating triggers
/// * Refreshing materialized views
#[instrument(skip_all)]
fn get_post_apply_statement_groups(
    definition: &PostgresDatabase,
    identifier_quoter: &IdentifierQuoter,
    target_definition: &PostgresDatabase,
) -> Vec<Vec<String>> {
    let mut statements = Vec::new();

    for schema in &definition.schemas {
        let existing_schema = target_definition.try_get_schema(&schema.name);

        let mut group_1 = Vec::new();
        let mut group_2 = Vec::new();
        for table in &schema.tables {
            let existing_table = existing_schema.and_then(|s| s.try_get_table(&table.name));

            for index in &table.indices {
                if index.index_constraint_type == PostgresIndexType::PrimaryKey {
                    continue;
                }

                if existing_table.is_some_and(|t| t.indices.iter().any(|i| i.name == index.name)) {
                    debug!(
                        "Index {} on table {} already exists in destination",
                        index.name, table.name
                    );
                    continue;
                }

                let sql = index.get_create_index_command(schema, table, identifier_quoter);
                if table.is_timescale_table() {
                    statements.push(vec![sql]);
                } else {
                    group_1.push(sql);
                }
            }
        }

        for sequence in &schema.sequences {
            let existing_sequence = existing_schema
                .and_then(|s| s.sequences.iter().find(|seq| seq.name == sequence.name));

            if existing_sequence.is_none() {
                group_1.push(sequence.get_create_statement(schema, identifier_quoter));
            } else {
                debug!("Sequence {} already exists in destination", sequence.name);
            }
            if existing_sequence.is_none()
                || existing_sequence.is_some_and(|s| s.last_value != sequence.last_value)
            {
                if let Some(sql) = sequence.get_set_value_statement(schema, identifier_quoter) {
                    group_2.push(sql);
                }
            }
        }

        for table in &schema.tables {
            let existing_table = existing_schema.and_then(|s| s.try_get_table(&table.name));

            for column in &table.columns {
                let target_column =
                    existing_table.and_then(|t| t.columns.iter().find(|c| c.name == column.name));

                if target_column.is_some_and(|c| c.default_value == column.default_value) {
                    debug!(
                        "Default value for column {} on table {} already matches destination",
                        column.name, table.name
                    );
                    continue;
                }

                if let Some(sql) =
                    column.get_alter_table_set_default_statement(table, schema, identifier_quoter)
                {
                    group_2.push(sql);
                }
            }
        }

        statements.push(group_1);
        statements.push(group_2);
    }

    for schema in &definition.schemas {
        let existing_schema = target_definition.try_get_schema(&schema.name);

        let mut group_3 = Vec::new();
        for table in &schema.tables {
            let existing_table = existing_schema.and_then(|s| s.try_get_table(&table.name));
            for constraint in &table.constraints {
                if let PostgresConstraint::Unique(uk) = constraint {
                    if existing_table.is_some_and(|t| {
                        t.constraints.iter().any(|c| c.name() == constraint.name())
                    }) {
                        debug!(
                            "Unique constraint {} on table {} already exists in destination",
                            constraint.name(),
                            table.name
                        );
                        continue;
                    }
                    let sql = uk.get_create_statement(table, schema, identifier_quoter);
                    if table.is_timescale_table() {
                        statements.push(vec![sql]);
                    } else {
                        group_3.push(sql);
                    }
                }
            }
        }
        statements.push(group_3);
    }

    for schema in &definition.schemas {
        let existing_schema = target_definition.try_get_schema(&schema.name);
        for table in &schema.tables {
            let existing_table = existing_schema.and_then(|s| s.try_get_table(&table.name));
            for constraint in &table.constraints {
                if existing_table
                    .is_some_and(|t| t.constraints.iter().any(|c| c.name() == constraint.name()))
                {
                    debug!(
                        "Foreign key constraint {} on table {} already exists in destination",
                        constraint.name(),
                        table.name
                    );
                    continue;
                }

                if let PostgresConstraint::ForeignKey(fk) = constraint {
                    let sql = fk.get_create_statement(table, schema, identifier_quoter);
                    statements.push(vec![sql]);
                }
            }
        }
    }

    let mut group_4 = Vec::new();
    for schema in &definition.schemas {
        let existing_schema = target_definition.try_get_schema(&schema.name);

        for trigger in &schema.triggers {
            if existing_schema.is_some_and(|s| s.triggers.iter().any(|t| t.name == trigger.name)) {
                debug!(
                    "Trigger {} on table {} already exists in destination",
                    trigger.name, trigger.table_name
                );
                continue;
            }

            let sql = trigger.get_create_statement(schema, identifier_quoter);
            group_4.push(sql);
        }
    }
    statements.push(group_4);

    for schema in &definition.schemas {
        for view in schema.views.iter().sort_by_dependencies() {
            if let Some(sql) = view.get_refresh_sql(schema, identifier_quoter) {
                statements.push(vec![sql]);
            }
        }
    }

    let mut group_5 = Vec::new();
    for job in &definition.timescale_support.user_defined_jobs {
        if target_definition
            .timescale_support
            .user_defined_jobs
            .iter()
            .any(|j| {
                j.function_schema == job.function_schema
                    && j.function_name == job.function_name
                    && j.config == job.config
            })
        {
            debug!(
                "Timescale job {} already exists in destination",
                job.function_name
            );
            continue;
        }

        group_5.push(job.get_create_sql(identifier_quoter));
    }

    for schema in &definition.schemas {
        let existing_schema = target_definition.try_get_schema(&schema.name);

        for table in &schema.tables {
            if let TableTypeDetails::TimescaleHypertable {
                compression: existing_compression,
                retention: existing_retention,
                ..
            } = &table.table_type
            {
                let existing_table = existing_schema.and_then(|s| s.try_get_table(&table.name));

                if existing_table.is_some_and(|t| {
                    if let TableTypeDetails::TimescaleHypertable {
                        compression,
                        retention,
                        ..
                    } = &t.table_type
                    {
                        compression == existing_compression && retention == existing_retention
                    } else {
                        false
                    }
                }) {
                    debug!(
                        "Timescale hypertable {} already exists in destination",
                        table.name
                    );
                    continue;
                }
            }

            if let Some(timescale_post) =
                table.get_timescale_post_settings(schema, identifier_quoter)
            {
                group_5.push(timescale_post);
            }
        }
    }

    statements.push(group_5);

    statements
}

/// Applies the structures generated in [get_post_apply_statement_groups] to the destination sequentially.
#[instrument(skip_all)]
async fn apply_post_copy_structure_sequential<D: CopyDestination>(
    destination: &mut D,
    definition: &PostgresDatabase,
    target_definition: &PostgresDatabase,
) -> Result<()> {
    let identifier_quoter = destination.get_identifier_quoter();

    let statement_groups =
        get_post_apply_statement_groups(definition, &identifier_quoter, target_definition);

    for group in statement_groups {
        for statement in group {
            destination
                .apply_non_transactional_statement(&statement)
                .await?;
        }
    }

    Ok(())
}

/// Applies the structures generated in [get_post_apply_statement_groups] to the destination in parallel.
#[instrument(skip_all)]
async fn apply_post_copy_structure_parallel<D: CopyDestination + Sync + Clone>(
    destination: &mut D,
    definition: &PostgresDatabase,
    options: &CopyDataOptions,
    target_definition: &PostgresDatabase,
) -> Result<()> {
    let identifier_quoter = destination.get_identifier_quoter();

    let statement_groups =
        get_post_apply_statement_groups(definition, &identifier_quoter, target_definition);

    for group in statement_groups {
        if group.is_empty() {
            continue;
        }

        if group.len() == 1 {
            destination
                .apply_non_transactional_statement(&group[0])
                .await?;
        } else {
            let mut join_handles = ParallelRunner::new(options.get_max_parallel_or_1());

            for statement in group {
                let mut destination = destination.clone();
                join_handles
                    .enqueue(async move {
                        destination
                            .apply_non_transactional_statement(&statement)
                            .await
                    })
                    .await?;
            }

            join_handles.run_remaining().await?;
        }
    }

    Ok(())
}

/// Get the data format to use when copying data from the source to the destination, that both
/// source and destination supports.
#[instrument(skip_all)]
async fn get_data_type(
    source: &impl CopySourceFactory,
    destination: &impl CopyDestinationFactory<'_>,
    options: &CopyDataOptions,
) -> Result<DataFormat> {
    let source_formats = source.supported_data_format().await?;
    let destination_formats = destination.supported_data_format().await?;

    let overlap = source_formats
        .iter()
        .filter(|f| destination_formats.contains(f))
        .collect_vec();

    if overlap.is_empty()
        || options
            .data_format
            .as_ref()
            .is_some_and(|d| !overlap.contains(&d))
    {
        Err(ElefantToolsError::DataFormatsNotCompatible {
            supported_by_source: source_formats,
            supported_by_target: destination_formats,
            required_format: options.data_format.clone(),
        })
    } else {
        for format in &overlap {
            if let DataFormat::PostgresBinary { .. } = format {
                return Ok((*format).clone());
            }
        }

        Ok(overlap[0].clone())
    }
}

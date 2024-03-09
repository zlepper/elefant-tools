use std::num::NonZeroUsize;
use itertools::Itertools;
use tracing::instrument;
use crate::*;
use crate::parallel_runner::ParallelRunner;
use crate::quoting::IdentifierQuoter;
use crate::storage::{CopyDestination, CopySource};
use crate::storage::DataFormat;


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
}

impl CopyDataOptions {
    fn get_max_parallel_or_1(&self) -> NonZeroUsize {
        self.max_parallel.unwrap_or(NonZeroUsize::new(1).unwrap())
    }
}

#[instrument(skip_all)]
pub async fn copy_data<'d, S: CopySourceFactory, D: CopyDestinationFactory<'d>>(source: &S, destination: &'d mut D, options: CopyDataOptions) -> Result<()> {
    let data_format = get_data_type(source, destination, &options).await?;

    let source = source.create_source().await?;
    let mut destination = destination.create_destination().await?;

    let definition = source.get_introspection().await?;
    
    let source_definition = if let Some(target_schema) = &options.target_schema {
        definition.filtered_to_schema(target_schema)
    } else {
        definition
    };
    
    let target_definition = if let (Some(target_schema), Some(rename_to)) = (&options.target_schema, &options.rename_schema_to) {
        source_definition.with_renamed_schema(target_schema, rename_to)
    } else {
        source_definition.clone()
    };
    
    destination.begin_transaction().await?;

    match &mut destination {
        SequentialOrParallel::Sequential(ref mut d) => {
            apply_pre_copy_structure(d, &target_definition).await?;
        }
        SequentialOrParallel::Parallel(ref mut d) => {
            apply_pre_copy_structure(d, &target_definition).await?;
        }
    }

    destination.commit_transaction().await?;

    let mut parallel_runner = ParallelRunner::new(options.get_max_parallel_or_1());


    for target_schema in &target_definition.schemas {
        let source_schema = source_definition.schemas.iter().find(|s| s.object_id.actual_eq(&target_schema.object_id));
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
            
            let source_table = source_schema.tables.iter().find(|t| t.object_id.actual_eq(&target_table.object_id));
            let source_table = match source_table {
                Some(s) => s,
                None => {
                    continue;
                }
            };

            match source {
                SequentialOrParallel::Sequential(ref source) => {
                    match &mut destination {
                        SequentialOrParallel::Sequential(ref mut destination) => {
                            do_copy(source, destination, target_schema, target_table, source_schema, source_table, &data_format).await?
                        }
                        SequentialOrParallel::Parallel(ref mut destination) => {
                            do_copy(source, destination, target_schema, target_table,  source_schema, source_table, &data_format).await?
                        }
                    }
                }
                SequentialOrParallel::Parallel(ref source) => {
                    match &mut destination {
                        SequentialOrParallel::Sequential(ref mut destination) => {
                            do_copy(source, destination, target_schema, target_table,  source_schema, source_table, &data_format).await?
                        }
                        SequentialOrParallel::Parallel(ref mut destination) => {
                            let source = source.clone();
                            let destination = destination.clone();
                            let df = data_format.clone();
                            parallel_runner.enqueue(async move {
                                let source = source;
                                let mut destination = destination;
                                do_copy(&source, &mut destination, target_schema, target_table, source_schema, source_table, &df).await
                            }).await?;
                        }
                    }
                }
            }
        }
    }

    parallel_runner.run_remaining().await?;

    match &mut destination {
        SequentialOrParallel::Sequential(ref mut destination) => {
            apply_post_copy_structure_sequential(destination, &target_definition).await?;
        }
        SequentialOrParallel::Parallel(ref mut destination) => {
            apply_post_copy_structure_parallel(destination, &target_definition, &options).await?;
        }
    }

    Ok(())
}

#[instrument(skip_all)]
async fn apply_pre_copy_structure<D: CopyDestination>(destination: &mut D, definition: &PostgresDatabase) -> Result<()> {
    let identifier_quoter = destination.get_identifier_quoter();

    for schema in &definition.schemas {
        destination.apply_transactional_statement(&schema.get_create_statement(&identifier_quoter)).await?;
    }

    for schema in &definition.schemas {
        for enumeration in &schema.enums {
            destination.apply_transactional_statement(&enumeration.get_create_statement(&identifier_quoter)).await?;
        }
    }

    for schema in &definition.schemas {
        for function in &schema.functions {
            destination.apply_transactional_statement(&function.get_create_statement(&identifier_quoter)).await?;
        }
    }

    for ext in &definition.enabled_extensions {
        destination.apply_transactional_statement(&ext.get_create_statement(&identifier_quoter)).await?;
    }

    for schema in &definition.schemas {
        let tables = schema.tables.iter().sorted_by_key(|t|
            match t.table_type {
                TableTypeDetails::Table => 0,
                TableTypeDetails::TimescaleHypertable { .. } => 1,
                TableTypeDetails::PartitionedParentTable { .. } => 2,
                TableTypeDetails::PartitionedChildTable { .. } => 3,
                TableTypeDetails::InheritedTable { .. } => 4,
            }
        );

        for table in tables {
            destination.apply_transactional_statement(&table.get_create_statement(schema, &identifier_quoter)).await?;
        }
    }

    Ok(())
}

#[instrument(skip_all)]
async fn do_copy<S: CopySource, D: CopyDestination>(source: &S, destination: &mut D, target_schema: &PostgresSchema, target_table: &PostgresTable, source_schema: &PostgresSchema, source_table: &PostgresTable, data_format: &DataFormat) -> Result<()> {
    let data = source.get_data(source_schema, source_table, data_format).await?;

    destination.apply_data(target_schema, target_table, data).await
}


#[instrument(skip_all)]
fn get_post_apply_statement_groups(definition: &PostgresDatabase, identifier_quoter: &IdentifierQuoter) -> Vec<Vec<String>> {
    let mut statements = Vec::new();


    for schema in &definition.schemas {
        let mut group_1 = Vec::new();
        let mut group_2 = Vec::new();
        for table in &schema.tables {
            for index in &table.indices {
                if index.index_constraint_type == PostgresIndexType::PrimaryKey {
                    continue;
                }
                group_1.push(index.get_create_index_command(schema, table, identifier_quoter));
            }
        }

        for sequence in &schema.sequences {
            group_1.push(sequence.get_create_statement(schema, identifier_quoter));
            if let Some(sql) = sequence.get_set_value_statement(schema, identifier_quoter) {
                group_2.push(sql);
            }
        }


        for table in &schema.tables {
            for column in &table.columns {
                if let Some(sql) = column.get_alter_table_set_default_statement(table, schema, identifier_quoter) {
                    group_2.push(sql);
                }
            }
        }

        statements.push(group_1);
        statements.push(group_2);


        for view in &schema.views {
            statements.push(vec![view.get_create_view_sql(schema, identifier_quoter)]);
        }
    }

    for schema in &definition.schemas {
        let mut group_3 = Vec::new();
        for table in &schema.tables {
            for constraint in &table.constraints {
                if let PostgresConstraint::ForeignKey(fk) = constraint {
                    let sql = fk.get_create_statement(table, schema, identifier_quoter);
                    group_3.push(sql);
                }
                if let PostgresConstraint::Unique(uk) = constraint {
                    let sql = uk.get_create_statement(table, schema, identifier_quoter);
                    group_3.push(sql);
                }
            }
        }
        statements.push(group_3);
    }

    let mut group_4 = Vec::new();
    for schema in &definition.schemas {
        for trigger in &schema.triggers {
            let sql = trigger.get_create_statement(schema, identifier_quoter);
            group_4.push(sql);
        }
    }

    for schema in &definition.schemas {
        for view in &schema.views {
            if let Some(sql) = view.get_refresh_sql(schema, identifier_quoter) {
                group_4.push(sql);
            }
        }
    }

    for job in &definition.timescale_support.user_defined_jobs {
        group_4.push(job.get_create_sql(identifier_quoter));
    }
    statements.push(group_4);


    statements
}


#[instrument(skip_all)]
async fn apply_post_copy_structure_sequential<D: CopyDestination>(destination: &mut D, definition: &PostgresDatabase) -> Result<()> {
    let identifier_quoter = destination.get_identifier_quoter();

    let statement_groups = get_post_apply_statement_groups(definition, &identifier_quoter);

    for group in statement_groups {
        for statement in group {
            destination.apply_non_transactional_statement(&statement).await?;
        }
    }

    Ok(())
}

#[instrument(skip_all)]
async fn apply_post_copy_structure_parallel<D: CopyDestination + Sync + Clone>(destination: &mut D, definition: &PostgresDatabase, options: &CopyDataOptions) -> Result<()> {
    let identifier_quoter = destination.get_identifier_quoter();

    let statement_groups = get_post_apply_statement_groups(definition, &identifier_quoter);

    for group in statement_groups {
        if group.is_empty() {
            continue;
        }

        if group.len() == 1 {
            destination.apply_non_transactional_statement(&group[0]).await?;
        } else {
            let mut join_handles = ParallelRunner::new(options.get_max_parallel_or_1());


            for statement in group {
                let mut destination = destination.clone();
                join_handles.enqueue(async move {
                    destination.apply_non_transactional_statement(&statement).await
                }).await?;
            }

            join_handles.run_remaining().await?;
        }
    }

    Ok(())
}

#[instrument(skip_all)]
async fn get_data_type(source: &impl CopySourceFactory, destination: &impl CopyDestinationFactory<'_>, options: &CopyDataOptions) -> Result<DataFormat> {
    let source_formats = source.supported_data_format().await?;
    let destination_formats = destination.supported_data_format().await?;

    let overlap = source_formats.iter().filter(|f| destination_formats.contains(f)).collect_vec();

    if overlap.is_empty() || options.data_format.as_ref().is_some_and(|d| !overlap.contains(&d)) {
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


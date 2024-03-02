use itertools::Itertools;
use crate::*;
use crate::storage::{CopyDestination, CopySource, DataFormat};



#[derive(Debug, Default)]
pub struct CopyDataOptions {
    /// Force this data format to be used
    pub data_format: Option<DataFormat>,
}

pub async fn copy_data(source: &impl CopySource, destination: &mut impl CopyDestination, options: CopyDataOptions) -> Result<()> {

    let data_format = get_data_type(source, destination, &options).await?;


    let definition = source.get_introspection().await?;

    apply_pre_copy_structure(destination, &definition).await?;

    for schema in &definition.schemas {
        for table in &schema.tables {

            if let TableTypeDetails::PartitionedParentTable {..} = &table.table_type {
                continue;
            }

            let data = source.get_data(schema, table, &data_format).await?;

            assert_eq!(data_format, data.get_data_format());

            destination.apply_data(schema, table, data).await?;
        }
    }

    apply_post_copy_structure(destination, &definition).await?;

    Ok(())
}

async fn apply_pre_copy_structure(destination: &mut impl CopyDestination, definition: &PostgresDatabase) -> Result<()> {
    let identifier_quoter = destination.get_identifier_quoter();

    for schema in &definition.schemas {
        destination.apply_ddl_statement(&schema.get_create_statement(&identifier_quoter)).await?;
    }

    for schema in &definition.schemas {
        for enumeration in &schema.enums {
            destination.apply_ddl_statement(&enumeration.get_create_statement(&identifier_quoter)).await?;
        }
    }

    for schema in &definition.schemas {
        for function in &schema.functions {
            destination.apply_ddl_statement(&function.get_create_statement(&identifier_quoter)).await?;
        }
    }

    for ext in &definition.enabled_extensions {
        destination.apply_ddl_statement(&ext.get_create_statement(&identifier_quoter)).await?;
    }

    for schema in &definition.schemas {
        let tables = schema.tables.iter().sorted_by_key(|t|
            match t.table_type {
                TableTypeDetails::Table => 0,
                TableTypeDetails::TimescaleHypertable {..} => 1,
                TableTypeDetails::PartitionedParentTable {..} => 2,
                TableTypeDetails::PartitionedChildTable {..} => 3,
                TableTypeDetails::InheritedTable {..} => 4,
            }
        );

        for table in tables {
            destination.apply_ddl_statement(&table.get_create_statement(schema, &identifier_quoter)).await?;
        }
    }

    Ok(())
}

async fn apply_post_copy_structure(destination: &mut impl CopyDestination, definition: &PostgresDatabase) -> Result<()> {
    let identifier_quoter = destination.get_identifier_quoter();

    for schema in &definition.schemas {
        for table in &schema.tables {
            for index in &table.indices {
                if index.index_constraint_type == PostgresIndexType::PrimaryKey {
                    continue;
                }
                destination.apply_ddl_statement(&index.get_create_index_command(schema, table, &identifier_quoter)).await?;
            }
        }

        for sequence in &schema.sequences {
            destination.apply_ddl_statement(&sequence.get_create_statement(schema, &identifier_quoter)).await?;
            if let Some(sql) = sequence.get_set_value_statement(schema, &identifier_quoter) {
                destination.apply_ddl_statement(&sql).await?;
            }
        }


        for table in &schema.tables {
            for column in &table.columns {
                if let Some(sql) = column.get_alter_table_set_default_statement(table, schema, &identifier_quoter) {
                    destination.apply_ddl_statement(&sql).await?;
                }
            }
        }


        for view in &schema.views {
            destination.apply_ddl_statement(&view.get_create_view_sql(schema, &identifier_quoter)).await?;
        }
    }

    for schema in &definition.schemas {
        for table in &schema.tables {
            for constraint in &table.constraints {
                if let PostgresConstraint::ForeignKey(fk) = constraint {
                    let sql = fk.get_create_statement(table, schema, &identifier_quoter);
                    destination.apply_ddl_statement(&sql).await?;
                }
                if let PostgresConstraint::Unique(uk) = constraint {
                    let sql = uk.get_create_statement(table, schema, &identifier_quoter);
                    destination.apply_ddl_statement(&sql).await?;
                }
            }
        }
    }

    for schema in &definition.schemas {
        for trigger in &schema.triggers {
            let sql = trigger.get_create_statement(schema, &identifier_quoter);
            destination.apply_ddl_statement(&sql).await?;
        }
    }

    for schema in &definition.schemas {
        for view in &schema.views {
            if let Some(sql) = view.get_refresh_sql(schema, &identifier_quoter) {
                destination.apply_ddl_statement(&sql).await?;
            }
        }
    }

    Ok(())
}

async fn get_data_type(source: &impl CopySource, destination: &impl CopyDestination, options: &CopyDataOptions) -> Result<DataFormat> {
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
            if let DataFormat::PostgresBinary {..} = format {
                return Ok((*format).clone());
            }
        }



        Ok(overlap[0].clone())
    }
}


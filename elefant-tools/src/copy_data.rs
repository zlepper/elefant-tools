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

    destination.apply_structure(&definition).await?;


    for schema in &definition.schemas {
        for table in &schema.tables {

            let data = source.get_data(schema, table, &data_format).await?;

            assert_eq!(data_format, data.get_data_format());

            destination.apply_data(schema, table, data).await?;
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


#[cfg(test)]
mod tests {
    use crate::test_helpers::*;
    use tokio::test;
    use crate::schema_reader::tests::introspect_schema;
    use crate::schema_importer::tests::import_database_schema;
    use futures::{pin_mut, SinkExt};
    use crate::storage::DataFormat;

    async fn test_copy(data_format: DataFormat) {
        let source = get_test_helper().await;

        //language=postgresql
        source.execute_not_query(r#"
        create table people(
            id serial primary key,
            name text not null,
            age int not null
        );

        insert into people(name, age)
        values
            ('foo', 42),
            ('bar', 89),
            ('nice', 69),
            (E'str\nange', 420)
            ;
        "#).await;

        let db = introspect_schema(&source).await;

        let destination = get_test_helper().await;

        import_database_schema(&destination, &db).await;

        let out_stream = source.copy_out(&db.schemas[0].tables[0].get_copy_out_command(&db.schemas[0], &data_format)).await;
        pin_mut!(out_stream);

        let in_sink = destination.copy_in(&db.schemas[0].tables[0].get_copy_in_command(&db.schemas[0], &data_format)).await;
        pin_mut!(in_sink);

        in_sink.send_all(&mut out_stream).await.expect("Sink copy failed");

        let inserted_count = in_sink.finish().await.expect("Failed to finish copy");

        assert_eq!(inserted_count, 4);

        let items = destination.get_results::<(i32, String, i32)>("select id, name, age from people;").await;

        assert_eq!(items, vec![
            (1, "foo".to_string(), 42),
            (2, "bar".to_string(), 89),
            (3, "nice".to_string(), 69),
            (4, "str\nange".to_string(), 420),
        ]);
    }


    #[test]
    async fn copies_between_databases_binary_format() {
        test_copy(DataFormat::PostgresBinary {
            postgres_version: Some("15".to_string()),
        }).await;
    }

    #[test]
    async fn copies_between_databases_text_format() {
        test_copy(DataFormat::Text).await;
    }
}





use async_trait::async_trait;
use bytes::Bytes;
use futures::{pin_mut, SinkExt, Stream, StreamExt, TryStreamExt};
use futures::stream::MapErr;
use tokio_postgres::CopyOutStream;
use crate::models::PostgresDatabase;
use crate::postgres_client_wrapper::PostgresClientWrapper;
use crate::schema_reader::SchemaReader;
use crate::storage::{BaseCopyTarget, CopyDestination, CopySource, DataFormat, TableData};
use crate::*;
use crate::models::PostgresSchema;
use crate::models::PostgresTable;

pub struct PostgresInstanceStorage<'a> {
    connection: &'a PostgresClientWrapper,
    postgres_version: String,
}

impl<'a> PostgresInstanceStorage<'a> {
    pub async fn new(connection: &'a PostgresClientWrapper) -> Result<Self> {
        let postgres_version = connection.get_single_result("select version()").await?;

        Ok(PostgresInstanceStorage {
            connection,
            postgres_version,
        })
    }
}

#[async_trait]
impl BaseCopyTarget for PostgresInstanceStorage<'_> {

    async fn supported_data_format(&self) -> Result<Vec<DataFormat>> {
        Ok(vec![
            DataFormat::Text,
            DataFormat::PostgresBinary {
                postgres_version: Some(self.postgres_version.clone()),
            },
        ])
    }
}

fn tokio_postgres_error_to_crate_error(e: tokio_postgres::Error) -> ElefantToolsError {
    e.into()
}

#[async_trait]
impl<'a> CopySource for PostgresInstanceStorage<'a> {
    type DataStream = MapErr<CopyOutStream, fn(tokio_postgres::Error) -> ElefantToolsError>;

    async fn get_introspection(&self) -> Result<PostgresDatabase> {
        let reader = SchemaReader::new(self.connection);
        reader.introspect_database().await
    }

    async fn get_data(&self, schema: &PostgresSchema, table: &PostgresTable, data_format: &DataFormat) -> Result<TableData<Self::DataStream>> {
        let copy_command = table.get_copy_out_command(schema, data_format);
        let copy_out_stream = self.connection.copy_out(&copy_command).await?;

        let stream = copy_out_stream.map_err(tokio_postgres_error_to_crate_error as fn(tokio_postgres::Error) -> ElefantToolsError);

        match data_format {
            DataFormat::Text => {
                Ok(TableData::Text {
                    data: stream
                })
            },
            DataFormat::PostgresBinary { .. } => {
                Ok(TableData::PostgresBinary {
                    postgres_version: self.postgres_version.clone(),
                    data: stream,
                })
            },
        }
    }
}


#[async_trait]
impl<'a> CopyDestination for PostgresInstanceStorage<'a> {
    async fn apply_structure(&mut self, db: &PostgresDatabase) -> Result<()> {
        for schema in &db.schemas {

            self.connection.execute_non_query(&schema.get_create_statement()).await?;

            for table in &schema.tables {
                self.connection.execute_non_query(&table.get_create_statement(schema)).await?;
            }
        }

        Ok(())
    }

    async fn apply_data<S: Stream<Item=Result<Bytes>> + Send>(&mut self, schema: &PostgresSchema, table: &PostgresTable, data: TableData<S>) -> Result<()> {
        let data_format = data.get_data_format();

        let copy_statement = table.get_copy_in_command(schema, &data_format);

        let sink = self.connection.copy_in::<Bytes>(&copy_statement).await?;
        pin_mut!(sink);

        let stream = data.into_stream();

        pin_mut!(stream);

        while let Some(item) = stream.next().await {
            let item = item?;
            sink.send(item).await?;
        }

        sink.close().await?;

        Ok(())
    }

    async fn apply_post_structure(&mut self, db: &PostgresDatabase) -> Result<()> {
        for schema in &db.schemas {
            for table in &schema.tables {
                for index in &table.indices {
                    self.connection.execute_non_query(&index.get_create_index_command(schema, table)).await?;
                }

                for constraint in &table.constraints {
                    if let PostgresConstraint::Unique(unique) = constraint {
                        self.connection.execute_non_query(&unique.get_create_statement(schema, table)).await?;
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tokio::test;
    use tokio_postgres::error::SqlState;
    use crate::copy_data::{copy_data, CopyDataOptions};
    use crate::schema_reader::tests::introspect_schema;
    use super::*;
    use crate::test_helpers::*;


    async fn test_copy(data_format: DataFormat) {
        let source = get_test_helper().await;

        //language=postgresql
        source.execute_not_query(storage::tests::SOURCE_DATABASE_CREATE_SCRIPT).await;


        let source_schema = introspect_schema(&source).await;
        let source = PostgresInstanceStorage::new(source.get_conn()).await.unwrap();

        let destination = get_test_helper().await;
        let mut destination_worker = PostgresInstanceStorage::new(destination.get_conn()).await.unwrap();

        copy_data(&source, &mut destination_worker, CopyDataOptions {
            data_format: Some(data_format)
        }).await.expect("Failed to copy data");



        let items = destination.get_results::<(i32, String, i32)>("select id, name, age from people;").await;

        assert_eq!(items, storage::tests::get_expected_data());

        let destination_schema = introspect_schema(&destination).await;

        assert_eq!(source_schema, destination_schema);

        // TODO: Make sure primary key auto increments
        let result = destination.get_conn().execute_non_query("insert into people (id, name, age) values (5, 'new-value', 10000)").await;
        assert_pg_error(result, 23514);

        let result = destination.get_conn().execute_non_query("insert into people (id, name, age) values (5, 'foo', 100)").await;
        assert_pg_error(result, 23505);
    }

    fn assert_pg_error(result: Result, code: u16) {
        match result {
            Err(ElefantToolsError::PostgresErrorWithQuery {
                    source,
                    ..
                }) => {


                assert_eq!(*source.as_db_error().unwrap().code(), SqlState::from_code(&code.to_string()));

            },
            _ => {
                panic!("Expected PostgresErrorWithQuery, got {:?}", result);
            }
        }
    }


    #[test]
    async fn copies_between_databases_binary_format() {
        test_copy(DataFormat::PostgresBinary {
            postgres_version: None
        }).await;
    }

    #[test]
    async fn copies_between_databases_text_format() {
        test_copy(DataFormat::Text).await;
    }

}
use crate::schema_reader::SchemaReader;
use crate::{
    CopySource, DataFormat, ElefantToolsError, IdentifierQuoter, PostgresClientWrapper,
    PostgresDatabase, PostgresInstanceStorage, PostgresSchema, PostgresTable, TableData,
};
use futures::stream::MapErr;
use futures::TryStreamExt;
use std::sync::Arc;
use tokio_postgres::CopyOutStream;
use tracing::instrument;

/// A copy source for Postgres that works well single-threaded workloads.
#[derive(Clone)]
pub struct SequentialSafePostgresInstanceCopySourceStorage<'a> {
    connection: &'a PostgresClientWrapper,
    identifier_quoter: Arc<IdentifierQuoter>,
}

impl<'a> SequentialSafePostgresInstanceCopySourceStorage<'a> {
    #[instrument(skip_all)]
    pub async fn new(storage: &PostgresInstanceStorage<'a>) -> crate::Result<Self> {
        let main_connection = storage.connection;

        main_connection
            .execute_non_query("begin transaction isolation level repeatable read read only;")
            .await?;

        Ok(SequentialSafePostgresInstanceCopySourceStorage {
            connection: main_connection,
            identifier_quoter: storage.identifier_quoter.clone(),
        })
    }
}

impl CopySource for SequentialSafePostgresInstanceCopySourceStorage<'_> {
    type DataStream = MapErr<CopyOutStream, fn(tokio_postgres::Error) -> ElefantToolsError>;
    type Cleanup = ();

    async fn get_introspection(&self) -> crate::Result<PostgresDatabase> {
        let reader = SchemaReader::new(self.connection);
        reader.introspect_database().await
    }

    #[instrument(skip_all)]
    async fn get_data(
        &self,
        schema: &PostgresSchema,
        table: &PostgresTable,
        data_format: &DataFormat,
    ) -> crate::Result<TableData<Self::DataStream, Self::Cleanup>> {
        let copy_command = table.get_copy_out_command(schema, data_format, &self.identifier_quoter);

        let copy_out_stream = self.connection.copy_out(&copy_command).await?;

        let stream = copy_out_stream.map_err(
            tokio_postgres_error_to_crate_error as fn(tokio_postgres::Error) -> ElefantToolsError,
        );

        Ok(TableData {
            data_format: data_format.clone(),
            data: stream,
            cleanup: (),
        })
    }
}

fn tokio_postgres_error_to_crate_error(e: tokio_postgres::Error) -> ElefantToolsError {
    e.into()
}

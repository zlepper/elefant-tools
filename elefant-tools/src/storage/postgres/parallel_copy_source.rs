use crate::schema_reader::SchemaReader;
use crate::storage::postgres::connection_pool::{ConnectionPool, ReleaseConnection};
use crate::storage::postgres::postgres_instance_storage::PostgresInstanceStorage;
use crate::{
    CopySource, DataFormat, ElefantToolsError, IdentifierQuoter, PostgresClientWrapper,
    PostgresDatabase, PostgresSchema, PostgresTable, TableData,
};
use futures::stream::MapErr;
use futures::TryStreamExt;
use std::sync::Arc;
use tokio_postgres::CopyOutStream;
use tracing::instrument;

/// A copy source for Postgres that works well with parallelism.
///
/// This uses repeatable read isolation level and a snapshot to ensure that the data is consistent
/// across the entire dump.
#[derive(Clone)]
pub struct ParallelSafePostgresInstanceCopySourceStorage<'a> {
    connection_pool: ConnectionPool,
    main_connection: &'a PostgresClientWrapper,
    transaction_id: String,
    identifier_quoter: Arc<IdentifierQuoter>,
}

impl<'a> ParallelSafePostgresInstanceCopySourceStorage<'a> {
    #[instrument(skip_all)]
    pub async fn new(storage: &PostgresInstanceStorage<'a>) -> crate::Result<Self> {
        let main_connection = storage.connection;

        main_connection
            .execute_non_query("begin transaction isolation level repeatable read read only;")
            .await?;
        let transaction_id = main_connection
            .get_single_result("select pg_export_snapshot();")
            .await?;

        Ok(ParallelSafePostgresInstanceCopySourceStorage {
            connection_pool: ConnectionPool::new(),
            transaction_id,
            main_connection,
            identifier_quoter: storage.identifier_quoter.clone(),
        })
    }

    async fn get_connection(&self) -> crate::Result<PostgresClientWrapper> {
        if let Some(existing) = self.connection_pool.get_connection().await {
            Ok(existing)
        } else {
            let new_conn = self.main_connection.create_another_connection().await?;

            new_conn.execute_non_query(&format!("begin transaction isolation level repeatable read read only; set transaction snapshot '{}';", self.transaction_id)).await?;

            Ok(new_conn)
        }
    }
}

impl<'a> CopySource for ParallelSafePostgresInstanceCopySourceStorage<'a> {
    type DataStream = MapErr<CopyOutStream, fn(tokio_postgres::Error) -> ElefantToolsError>;
    type Cleanup = ReleaseConnection;

    async fn get_introspection(&self) -> crate::Result<PostgresDatabase> {
        let reader = SchemaReader::new(self.main_connection);
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

        let connection = self.get_connection().await?;

        let copy_out_stream = connection.copy_out(&copy_command).await?;

        let stream = copy_out_stream.map_err(
            tokio_postgres_error_to_crate_error as fn(tokio_postgres::Error) -> ElefantToolsError,
        );

        Ok(TableData {
            data_format: data_format.clone(),
            data: stream,
            cleanup: ReleaseConnection::new(self.connection_pool.clone(), connection),
        })
    }
}

fn tokio_postgres_error_to_crate_error(e: tokio_postgres::Error) -> ElefantToolsError {
    e.into()
}

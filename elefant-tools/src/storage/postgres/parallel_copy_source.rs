use crate::storage::postgres::postgres_instance_storage::PostgresInstanceStorage;
use crate::{
    CopySource, DataFormat, IdentifierQuoter, PostgresClientWrapper, PostgresSchema, PostgresTable,
    TableData, TableDataReader,
};
use elefant_client::tokio_connection::{TokioConnectionFactory, TokioPoolableClient};
use elefant_client::OwnedCopyReader;
use std::sync::Arc;
use tracing::instrument;

/// A copy source for Postgres that works well with parallelism.
///
/// This uses repeatable read isolation level and a snapshot to ensure that the data is consistent
/// across the entire dump.
pub struct ParallelSafePostgresInstanceCopySourceStorage<'a> {
    wrapper: &'a PostgresClientWrapper,
    transaction_id: String,
    identifier_quoter: Arc<IdentifierQuoter>,
    /// Holds the client with the snapshot-exporting transaction open.
    /// Must stay alive for the entire duration of the copy operation.
    _snapshot_client: Option<TokioPoolableClient>,
}

impl Clone for ParallelSafePostgresInstanceCopySourceStorage<'_> {
    fn clone(&self) -> Self {
        Self {
            wrapper: self.wrapper,
            transaction_id: self.transaction_id.clone(),
            identifier_quoter: self.identifier_quoter.clone(),
            _snapshot_client: None,
        }
    }
}

impl<'a> ParallelSafePostgresInstanceCopySourceStorage<'a> {
    #[instrument(skip_all)]
    pub async fn new(storage: &PostgresInstanceStorage<'a>) -> crate::Result<Self> {
        let wrapper = storage.connection;

        // Start a repeatable read transaction on a dedicated client and export snapshot
        let mut client = wrapper.pool().get_client().await?;
        client
            .execute_non_query_simple(
                "begin transaction isolation level repeatable read read only;",
            )
            .await?;

        let transaction_id: String = client
            .query_simple("select pg_export_snapshot();")
            .await?
            .collect_single_column_to_vec::<String>()
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| {
                crate::ElefantToolsError::PostgresError(
                    elefant_client::ElefantClientError::PostgresError(
                        "pg_export_snapshot returned no rows".to_string(),
                    ),
                )
            })?;

        Ok(ParallelSafePostgresInstanceCopySourceStorage {
            wrapper,
            transaction_id,
            identifier_quoter: storage.identifier_quoter.clone(),
            _snapshot_client: Some(client),
        })
    }
}

pub struct PostgresCopyOutReader {
    reader: OwnedCopyReader<TokioConnectionFactory>,
}

impl TableDataReader for PostgresCopyOutReader {
    async fn read_chunk(&mut self) -> crate::Result<Option<&[u8]>> {
        let data = self.reader.read().await?;
        Ok(data.map(|cd| cd.data))
    }
}

impl PostgresCopyOutReader {
    pub async fn end(self) -> crate::Result<()> {
        self.reader.end().await?;
        Ok(())
    }
}

impl CopySource for ParallelSafePostgresInstanceCopySourceStorage<'_> {
    type DataReader<'a> = PostgresCopyOutReader where Self: 'a;
    type Cleanup = ();

    #[instrument(skip_all)]
    async fn get_data<'a>(
        &'a mut self,
        schema: &'a PostgresSchema,
        table: &'a PostgresTable,
        data_format: &'a DataFormat,
    ) -> crate::Result<TableData<Self::DataReader<'a>, Self::Cleanup>> {
        let copy_command = table.get_copy_out_command(schema, data_format, &self.identifier_quoter);

        // Get a new client from the pool and set the snapshot
        let mut client = self.wrapper.pool().get_client().await?;
        client
            .execute_non_query_simple(&format!(
                "begin transaction isolation level repeatable read read only; set transaction snapshot '{}';",
                self.transaction_id
            ))
            .await?;

        let reader = OwnedCopyReader::new(client, &*copy_command, &[]).await?;

        Ok(TableData {
            data_format: data_format.clone(),
            data: PostgresCopyOutReader { reader },
            cleanup: (),
        })
    }
}

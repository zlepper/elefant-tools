use crate::storage::postgres::postgres_instance_storage::PostgresInstanceStorage;
use crate::{
    CopySource, DataFormat, IdentifierQuoter, PostgresSchema, PostgresTable, TableData,
    TableDataReader,
};
use elefant_client::tokio_connection::{TokioConnectionFactory, TokioPoolableClient};
use elefant_client::CopyReader;
use std::sync::Arc;
use tracing::instrument;

/// A copy source for Postgres that works well single-threaded workloads.
pub struct SequentialSafePostgresInstanceCopySourceStorage<'a> {
    client: TokioPoolableClient,
    identifier_quoter: Arc<IdentifierQuoter>,
    _lifetime: std::marker::PhantomData<&'a ()>,
}

impl Clone for SequentialSafePostgresInstanceCopySourceStorage<'_> {
    fn clone(&self) -> Self {
        panic!("SequentialSafePostgresInstanceCopySourceStorage should not be cloned")
    }
}

impl<'a> SequentialSafePostgresInstanceCopySourceStorage<'a> {
    #[instrument(skip_all)]
    pub async fn new(storage: &PostgresInstanceStorage<'a>) -> crate::Result<Self> {
        let wrapper = storage.connection;

        let mut client = wrapper.pool().get_client().await?;
        client
            .execute_non_query_simple(
                "begin transaction isolation level repeatable read read only;",
            )
            .await?;

        Ok(SequentialSafePostgresInstanceCopySourceStorage {
            client,
            identifier_quoter: storage.identifier_quoter.clone(),
            _lifetime: std::marker::PhantomData,
        })
    }
}

pub struct BorrowedPostgresCopyOutReader<'a> {
    reader: CopyReader<'a, TokioConnectionFactory>,
}

impl TableDataReader for BorrowedPostgresCopyOutReader<'_> {
    async fn read_chunk(&mut self) -> crate::Result<Option<&[u8]>> {
        let data = self.reader.read().await?;
        Ok(data.map(|cd| cd.data))
    }
}

impl CopySource for SequentialSafePostgresInstanceCopySourceStorage<'_> {
    type DataReader<'a> = BorrowedPostgresCopyOutReader<'a> where Self: 'a;
    type Cleanup = ();

    #[instrument(skip_all)]
    async fn get_data<'a>(
        &'a mut self,
        schema: &'a PostgresSchema,
        table: &'a PostgresTable,
        data_format: &'a DataFormat,
    ) -> crate::Result<TableData<Self::DataReader<'a>, Self::Cleanup>> {
        let copy_command = table.get_copy_out_command(schema, data_format, &self.identifier_quoter);

        let copy_reader = self.client.copy_out(&*copy_command, &[]).await?;

        Ok(TableData {
            data_format: data_format.clone(),
            data: BorrowedPostgresCopyOutReader {
                reader: copy_reader,
            },
            cleanup: (),
        })
    }
}

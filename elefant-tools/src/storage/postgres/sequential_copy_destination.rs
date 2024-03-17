use std::sync::Arc;
use bytes::Bytes;
use futures::{pin_mut, SinkExt, Stream, StreamExt};
use crate::{AsyncCleanup, CopyDestination, IdentifierQuoter, PostgresClientWrapper, PostgresSchema, PostgresTable, TableData};
use crate::helpers::IMPORT_PREFIX;
use crate::storage::postgres::postgres_instance_storage::PostgresInstanceStorage;

/// A copy destination for Postgres that works well single-threaded workloads.
#[derive(Clone)]
pub struct SequentialSafePostgresInstanceCopyDestinationStorage<'a> {
    connection: &'a PostgresClientWrapper,
    identifier_quoter: Arc<IdentifierQuoter>,
}

impl<'a> SequentialSafePostgresInstanceCopyDestinationStorage<'a> {
    pub async fn new(storage: &PostgresInstanceStorage<'a>) -> crate::Result<Self> {
        let main_connection = storage.connection;

        main_connection.execute_non_query(IMPORT_PREFIX).await?;
        
        Ok(SequentialSafePostgresInstanceCopyDestinationStorage {
            connection: main_connection,
            identifier_quoter: storage.identifier_quoter.clone(),
        })
    }
}

impl<'a> CopyDestination for SequentialSafePostgresInstanceCopyDestinationStorage<'a> {
    async fn apply_data<S: Stream<Item =crate::Result<Bytes>> + Send, C: AsyncCleanup>(
        &mut self,
        schema: &PostgresSchema,
        table: &PostgresTable,
        data: TableData<S, C>,
    ) -> crate::Result<()> {
        let data_format = data.data_format;

        let copy_statement =
            table.get_copy_in_command(schema, &data_format, &self.identifier_quoter);

        let sink = self.connection.copy_in::<Bytes>(&copy_statement).await?;
        pin_mut!(sink);

        let stream = data.data;

        pin_mut!(stream);

        while let Some(item) = stream.next().await {
            let item = item?;
            sink.feed(item).await?;
        }

        sink.close().await?;

        data.cleanup.cleanup().await?;

        Ok(())
    }

    async fn apply_transactional_statement(&mut self, statement: &str) -> crate::Result<()> {
        self.connection.execute_non_query(statement).await?;
        Ok(())
    }

    async fn apply_non_transactional_statement(&mut self, statement: &str) -> crate::Result<()> {
        self.connection.execute_non_query(statement).await?;
        Ok(())
    }

    async fn begin_transaction(&mut self) -> crate::Result<()> {
        self.connection
            .execute_non_query("begin transaction isolation level serializable read write;")
            .await?;
        Ok(())
    }

    async fn commit_transaction(&mut self) -> crate::Result<()> {
        self.connection.execute_non_query("commit;").await?;
        Ok(())
    }

    fn get_identifier_quoter(&self) -> Arc<IdentifierQuoter> {
        self.identifier_quoter.clone()
    }
}


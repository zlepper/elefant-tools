use std::collections::HashSet;
use std::sync::Arc;
use bytes::Bytes;
use futures::{pin_mut, SinkExt, Stream, StreamExt};
use itertools::Itertools;
use tracing::{error, info, instrument};
use crate::{AsyncCleanup, CopyDestination, IdentifierQuoter, PostgresClientWrapper, PostgresSchema, PostgresTable, TableData};
use crate::helpers::IMPORT_PREFIX;
use crate::storage::postgres::connection_pool::ConnectionPool;
use crate::storage::postgres::postgres_instance_storage::PostgresInstanceStorage;

/// A copy destination for Postgres that works well with parallelism.
#[derive(Clone)]
pub struct ParallelSafePostgresInstanceCopyDestinationStorage<'a> {
    connection_pool: ConnectionPool,
    main_connection: &'a PostgresClientWrapper,
    identifier_quoter: Arc<IdentifierQuoter>,
    in_flight_statements: Arc<tokio::sync::Mutex<HashSet<String>>>,
}

impl<'a> ParallelSafePostgresInstanceCopyDestinationStorage<'a> {
    pub async fn new(storage: &PostgresInstanceStorage<'a>) -> crate::Result<Self> {
        let main_connection = storage.connection;

        main_connection.execute_non_query(IMPORT_PREFIX).await?;
        
        Ok(ParallelSafePostgresInstanceCopyDestinationStorage {
            connection_pool: ConnectionPool::new(),
            main_connection,
            identifier_quoter: storage.identifier_quoter.clone(),
            in_flight_statements: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
        })
    }

    async fn get_connection(&self) -> crate::Result<PostgresClientWrapper> {
        if let Some(existing) = self.connection_pool.get_connection().await {
            Ok(existing)
        } else {
            let new_conn = self.main_connection.create_another_connection().await?;
            
            new_conn.execute_non_query(IMPORT_PREFIX).await?;
            
            Ok(new_conn)
        }
    }

    async fn release_connection(&self, connection: PostgresClientWrapper) {
        self.connection_pool.release_connection(connection).await;
    }
}

impl<'a> CopyDestination for ParallelSafePostgresInstanceCopyDestinationStorage<'a> {
    async fn apply_data<S: Stream<Item =crate::Result<Bytes>> + Send, C: AsyncCleanup>(
        &mut self,
        schema: &PostgresSchema,
        table: &PostgresTable,
        data: TableData<S, C>,
    ) -> crate::Result<()> {
        let data_format = data.data_format;

        let copy_statement =
            table.get_copy_in_command(schema, &data_format, &self.identifier_quoter);

        let connection = self.get_connection().await?;

        let sink = connection.copy_in::<Bytes>(&copy_statement).await?;
        pin_mut!(sink);

        let stream = data.data;

        pin_mut!(stream);

        while let Some(item) = stream.next().await {
            let item = item?;
            sink.feed(item).await?;
        }

        sink.close().await?;

        data.cleanup.cleanup().await?;
        self.release_connection(connection).await;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn apply_transactional_statement(&mut self, statement: &str) -> crate::Result<()> {
        info!("Executing transactional statement");
        self.main_connection.execute_non_query(statement).await?;
        info!("Executed transactional statement");
        Ok(())
    }

    #[instrument(skip(self))]
    async fn apply_non_transactional_statement(&mut self, statement: &str) -> crate::Result<()> {
        let in_flight_when_started= {
            let mut in_flight_statements = self.in_flight_statements.lock().await;
            in_flight_statements.insert(statement.to_string());
            in_flight_statements.iter().cloned().collect_vec()
        };
        
        info!("Executing non-transactional statement");
        let connection = self.get_connection().await?;
        let result = connection.execute_non_query(statement).await;
        {
            let mut in_flight_statements = self.in_flight_statements.lock().await;
            if let Err(e) = result {
                error!("Error occurred. In flight statements: {:?}. In flight when started: {:?}", in_flight_statements, in_flight_when_started);
                return Err(e);
            }
            in_flight_statements.remove(statement);
        }
        
        self.release_connection(connection).await;
        info!("Executed non-transactional statement");
        Ok(())
    }

    #[instrument(skip(self))]
    async fn begin_transaction(&mut self) -> crate::Result<()> {
        self.main_connection
            .execute_non_query("begin transaction isolation level serializable read write;")
            .await?;
        Ok(())
    }

    #[instrument(skip(self))]
    async fn commit_transaction(&mut self) -> crate::Result<()> {
        self.main_connection.execute_non_query("commit;").await?;
        Ok(())
    }

    fn get_identifier_quoter(&self) -> Arc<IdentifierQuoter> {
        self.identifier_quoter.clone()
    }
}


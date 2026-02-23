use crate::helpers::IMPORT_PREFIX;
use crate::quoting::{AttemptedKeywordUsage, Quotable};
use crate::schema_reader::SchemaReader;
use crate::storage::postgres::postgres_instance_storage::PostgresInstanceStorage;
use crate::{
    AsyncCleanup, CopyDestination, CopyTransaction, IdentifierQuoter, PostgresClientWrapper,
    PostgresDatabase, PostgresSchema, PostgresTable, TableData, TableDataReader,
};
use elefant_client::tokio_connection::TokioPoolableClient;
use itertools::Itertools;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{error, info, instrument};

/// A copy destination for Postgres that works well with parallelism.
pub struct ParallelSafePostgresInstanceCopyDestinationStorage<'a> {
    wrapper: &'a PostgresClientWrapper,
    identifier_quoter: Arc<IdentifierQuoter>,
    in_flight_statements: Arc<tokio::sync::Mutex<HashSet<String>>>,
}

impl Clone for ParallelSafePostgresInstanceCopyDestinationStorage<'_> {
    fn clone(&self) -> Self {
        Self {
            wrapper: self.wrapper,
            identifier_quoter: self.identifier_quoter.clone(),
            in_flight_statements: self.in_flight_statements.clone(),
        }
    }
}

impl<'a> ParallelSafePostgresInstanceCopyDestinationStorage<'a> {
    pub fn new(storage: &PostgresInstanceStorage<'a>) -> Self {
        ParallelSafePostgresInstanceCopyDestinationStorage {
            wrapper: storage.connection,
            identifier_quoter: storage.identifier_quoter.clone(),
            in_flight_statements: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
        }
    }
}

/// A transaction on a parallel Postgres copy destination.
pub struct PostgresTransaction {
    client: TokioPoolableClient,
}

impl CopyTransaction for PostgresTransaction {
    #[instrument(skip(self))]
    async fn apply_statement(&mut self, statement: &str) -> crate::Result<()> {
        info!("Executing transactional statement");
        self.client
            .execute_non_query_simple(statement)
            .await
            .map_err(|e| crate::ElefantToolsError::PostgresErrorWithQuery {
                source: e,
                query: statement.to_string(),
            })?;
        info!("Executed transactional statement");
        Ok(())
    }

    #[instrument(skip(self))]
    async fn commit(mut self) -> crate::Result<()> {
        self.client.execute_non_query_simple("commit;").await?;
        Ok(())
    }
}

impl CopyDestination for ParallelSafePostgresInstanceCopyDestinationStorage<'_> {
    type Transaction<'a>
        = PostgresTransaction
    where
        Self: 'a;

    async fn apply_data<R: TableDataReader, C: AsyncCleanup>(
        &mut self,
        schema: &PostgresSchema,
        table: &PostgresTable,
        mut data: TableData<R, C>,
    ) -> crate::Result<()> {
        let data_format = data.data_format.clone();

        let copy_statement =
            table.get_copy_in_command(schema, &data_format, &self.identifier_quoter);

        let mut client = self.wrapper.pool().get_client().await?;
        client.execute_non_query_simple(IMPORT_PREFIX).await?;

        let mut writer = client.copy_in(&*copy_statement, &[]).await?;

        while let Some(chunk) = data.data.read_chunk().await? {
            writer.write(chunk).await?;
        }

        writer.end().await?;

        data.cleanup.cleanup().await?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn apply_non_transactional_statement(&mut self, statement: &str) -> crate::Result<()> {
        let in_flight_when_started = {
            let mut in_flight_statements = self.in_flight_statements.lock().await;
            in_flight_statements.insert(statement.to_string());
            in_flight_statements.iter().cloned().collect_vec()
        };

        info!("Executing non-transactional statement");
        let mut client = self.wrapper.pool().get_client().await?;
        let result = client.execute_non_query_simple(statement).await;
        {
            let mut in_flight_statements = self.in_flight_statements.lock().await;
            if let Err(e) = result {
                error!(
                    "Error occurred. In flight statements: {:?}. In flight when started: {:?}",
                    in_flight_statements, in_flight_when_started
                );
                return Err(e.into());
            }
            in_flight_statements.remove(statement);
        }

        info!("Executed non-transactional statement");
        Ok(())
    }

    #[instrument(skip(self))]
    async fn begin_transaction(&mut self) -> crate::Result<PostgresTransaction> {
        let mut client = self.wrapper.pool().get_client().await?;
        client.execute_non_query_simple(IMPORT_PREFIX).await?;
        client
            .execute_non_query_simple("begin transaction isolation level serializable read write;")
            .await?;
        Ok(PostgresTransaction { client })
    }

    fn get_identifier_quoter(&self) -> Arc<IdentifierQuoter> {
        self.identifier_quoter.clone()
    }

    async fn try_introspect(&self) -> crate::Result<Option<PostgresDatabase>> {
        let reader = SchemaReader::new(self.wrapper);
        reader.introspect_database().await.map(Some)
    }

    async fn has_data_in_table(
        &self,
        schema: &PostgresSchema,
        table: &PostgresTable,
    ) -> crate::Result<bool> {
        let schema_name = schema.name.quote(
            &self.identifier_quoter,
            AttemptedKeywordUsage::TypeOrFunctionName,
        );
        let table_name = table.name.quote(
            &self.identifier_quoter,
            AttemptedKeywordUsage::TypeOrFunctionName,
        );
        let query = format!("select exists(select 1 from {schema_name}.{table_name} limit 1);");
        let result = self.wrapper.get_single_result::<bool>(&query).await?;
        Ok(result)
    }
}

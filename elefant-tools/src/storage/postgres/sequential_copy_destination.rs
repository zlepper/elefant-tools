use crate::helpers::IMPORT_PREFIX;
use crate::quoting::{AttemptedKeywordUsage, Quotable};
use crate::schema_reader::SchemaReader;
use crate::storage::postgres::postgres_instance_storage::PostgresInstanceStorage;
use crate::{
    AsyncCleanup, CopyDestination, CopyTransaction, IdentifierQuoter, PostgresClientWrapper,
    PostgresDatabase, PostgresSchema, PostgresTable, TableData, TableDataReader,
};
use elefant_client::tokio_connection::TokioPoolableClient;
use std::sync::Arc;

/// A copy destination for Postgres that works well single-threaded workloads.
pub struct SequentialSafePostgresInstanceCopyDestinationStorage<'a> {
    wrapper: &'a PostgresClientWrapper,
    client: TokioPoolableClient,
    identifier_quoter: Arc<IdentifierQuoter>,
}

impl<'a> SequentialSafePostgresInstanceCopyDestinationStorage<'a> {
    pub async fn new(storage: &PostgresInstanceStorage<'a>) -> crate::Result<Self> {
        let wrapper = storage.connection;

        let mut client = wrapper.pool().get_client().await?;
        client.execute_non_query_simple(IMPORT_PREFIX).await?;

        Ok(SequentialSafePostgresInstanceCopyDestinationStorage {
            wrapper,
            client,
            identifier_quoter: storage.identifier_quoter.clone(),
        })
    }
}

/// A transaction on a sequential Postgres copy destination, borrowing the existing client.
pub struct SequentialPostgresTransaction<'a> {
    client: &'a mut TokioPoolableClient,
}

impl CopyTransaction for SequentialPostgresTransaction<'_> {
    async fn apply_statement(&mut self, statement: &str) -> crate::Result<()> {
        self.client
            .execute_non_query_simple(statement)
            .await
            .map_err(|e| crate::ElefantToolsError::PostgresErrorWithQuery {
                source: e,
                query: statement.to_string(),
            })?;
        Ok(())
    }

    async fn commit(self) -> crate::Result<()> {
        self.client.execute_non_query_simple("commit;").await?;
        Ok(())
    }
}

impl CopyDestination for SequentialSafePostgresInstanceCopyDestinationStorage<'_> {
    type Transaction<'a>
        = SequentialPostgresTransaction<'a>
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

        let mut writer = self.client.copy_in(&*copy_statement, &[]).await?;

        while let Some(chunk) = data.data.read_chunk().await? {
            writer.write(chunk).await?;
        }

        writer.end().await?;

        data.cleanup.cleanup().await?;

        Ok(())
    }

    async fn apply_non_transactional_statement(&mut self, statement: &str) -> crate::Result<()> {
        self.client
            .execute_non_query_simple(statement)
            .await
            .map_err(|e| crate::ElefantToolsError::PostgresErrorWithQuery {
                source: e,
                query: statement.to_string(),
            })?;
        Ok(())
    }

    async fn begin_transaction(&mut self) -> crate::Result<SequentialPostgresTransaction<'_>> {
        self.client
            .execute_non_query_simple("begin transaction isolation level serializable read write;")
            .await?;
        Ok(SequentialPostgresTransaction {
            client: &mut self.client,
        })
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

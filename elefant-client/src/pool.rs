use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use crate::protocol::async_io::ElefantAsyncReadWrite;
use crate::protocol::PostgresConnection;
use crate::types::EnumTypeRegistry;
use crate::{ElefantClientError, PostgresConnectionSettings};
use tracing::debug;

pub trait ConnectionFactory {
    type Connection: ElefantAsyncReadWrite;

    fn create_connection(
        &self,
        settings: &PostgresConnectionSettings,
    ) -> impl std::future::Future<
        Output = Result<PostgresConnection<Self::Connection>, ElefantClientError>,
    >;
}

pub struct PostgresPool<F: ConnectionFactory>(Arc<PostgresPoolInner<F>>);

struct PostgresPoolInner<F: ConnectionFactory> {
    factory: F,
    settings: PostgresConnectionSettings,
    idle_connections: Mutex<Vec<crate::postgres_client::PostgresClient<F>>>,
    enum_registry: Arc<EnumTypeRegistry>,
}

impl<F: ConnectionFactory> Clone for PostgresPool<F> {
    fn clone(&self) -> Self {
        PostgresPool(Arc::clone(&self.0))
    }
}

impl<F: ConnectionFactory> PostgresPool<F> {
    pub async fn new(
        factory: F,
        settings: PostgresConnectionSettings,
    ) -> Result<Self, ElefantClientError> {
        let empty_registry = Arc::new(EnumTypeRegistry::new());

        if settings.enum_type_names().is_empty() {
            return Ok(PostgresPool(Arc::new(PostgresPoolInner {
                factory,
                settings,
                idle_connections: Mutex::new(Vec::new()),
                enum_registry: empty_registry,
            })));
        }

        // Create a temporary connection to query enum OIDs
        let connection = factory.create_connection(&settings).await?;
        let mut client =
            crate::postgres_client::PostgresClient::new(connection, &settings, empty_registry)
                .await?;

        // Build the SQL query filtering by the registered enum names
        let names_sql = settings
            .enum_type_names()
            .iter()
            .map(|n| format!("'{}'", n.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");

        let query = format!(
            "SELECT t.typname, n.nspname, t.oid::int4, t.typarray::int4 \
             FROM pg_catalog.pg_type t \
             JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid \
             WHERE t.typtype = 'e' AND (t.typname IN ({names_sql}) \
                   OR (n.nspname || '.' || t.typname) IN ({names_sql}))"
        );

        let mut registry = EnumTypeRegistry::new();

        let mut query_result = client.query_simple(&query).await?;
        loop {
            let result_set = query_result.next_result_set().await?;
            match result_set {
                crate::postgres_client::QueryResultSet::QueryProcessingComplete => break,
                crate::postgres_client::QueryResultSet::RowDescriptionReceived(mut row_reader) => {
                    while let Some(row) = row_reader.next_row().await? {
                        let typname: &str = row.get_text(0)?;
                        let nspname: &str = row.get_text(1)?;
                        let oid: i32 = row.get_text(2)?;
                        let typarray: i32 = row.get_text(3)?;

                        let qualified_name = format!("{nspname}.{typname}");

                        // Insert under the unqualified name
                        registry.insert(
                            typname.to_string(),
                            crate::types::EnumOidEntry {
                                oid,
                                array_oid: typarray,
                                schema: nspname.to_string(),
                            },
                        );

                        // Also insert under the schema-qualified name
                        registry.insert(
                            qualified_name,
                            crate::types::EnumOidEntry {
                                oid,
                                array_oid: typarray,
                                schema: nspname.to_string(),
                            },
                        );
                    }
                }
            }
        }

        let registry = Arc::new(registry);
        client.enum_registry = registry.clone();

        // Return the client to the idle pool for reuse
        Ok(PostgresPool(Arc::new(PostgresPoolInner {
            factory,
            settings,
            idle_connections: Mutex::new(vec![client]),
            enum_registry: registry,
        })))
    }

    pub fn settings(&self) -> &PostgresConnectionSettings {
        &self.0.settings
    }

    pub fn enum_registry(&self) -> &Arc<EnumTypeRegistry> {
        &self.0.enum_registry
    }

    pub(crate) fn return_connection(&self, mut client: crate::postgres_client::PostgresClient<F>) {
        client.pool = None;
        self.0.idle_connections.lock().unwrap().push(client);
    }

    pub async fn get_client(&self) -> Result<PoolableClient<F>, ElefantClientError> {
        // Try to reuse an idle connection
        loop {
            let idle_client = { self.0.idle_connections.lock().unwrap().pop() };
            match idle_client {
                Some(mut client) => match client.reset().await {
                    Ok(()) => {
                        client.pool = Some(self.clone());
                        client.enum_registry = self.0.enum_registry.clone();
                        return Ok(PoolableClient {
                            client: Some(client),
                            pool: self.clone(),
                        });
                    }
                    Err(e) => {
                        debug!("Idle connection reset failed, discarding: {:?}", e);
                        continue;
                    }
                },
                None => break,
            }
        }

        // No idle connection available, create a new one
        let connection = self.0.factory.create_connection(&self.0.settings).await?;
        let mut client = crate::postgres_client::PostgresClient::new(
            connection,
            &self.0.settings,
            self.0.enum_registry.clone(),
        )
        .await?;
        client.pool = Some(self.clone());
        Ok(PoolableClient {
            client: Some(client),
            pool: self.clone(),
        })
    }
}

pub struct PoolableClient<F: ConnectionFactory> {
    client: Option<crate::postgres_client::PostgresClient<F>>,
    pool: PostgresPool<F>,
}

impl<F: ConnectionFactory> Deref for PoolableClient<F> {
    type Target = crate::postgres_client::PostgresClient<F>;

    fn deref(&self) -> &Self::Target {
        self.client
            .as_ref()
            .expect("client already returned to pool")
    }
}

impl<F: ConnectionFactory> DerefMut for PoolableClient<F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client
            .as_mut()
            .expect("client already returned to pool")
    }
}

impl<F: ConnectionFactory> Drop for PoolableClient<F> {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            self.pool.return_connection(client);
        }
    }
}

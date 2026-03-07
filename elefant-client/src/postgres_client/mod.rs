mod copy;
mod easy_client;
mod establish;
mod query;
pub mod replication;
mod statements;

use crate::pool::{ConnectionFactory, PostgresPool};
use crate::protocol::{
    BackendMessage, CurrentTransactionStatus, FrontendMessage, PostgresConnection,
};
use crate::types::EnumTypeRegistry;
use crate::{reborrow_until_polonius, ElefantClientError, PostgresConnectionSettings};
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tracing::{debug, trace};

pub use copy::{CopyReader, CopyWriter, OwnedCopyReader};
pub use query::{PostgresDataRow, QueryResult, QueryResultSet, RowResultReader, SimpleQueryResult};
pub use statements::*;

pub struct PostgresClient<F: ConnectionFactory> {
    pub(crate) connection: PostgresConnection<F::Connection>,
    pub(crate) pool: Option<PostgresPool<F>>,
    pub(crate) ready_for_query: bool,
    write_buffer: Vec<u8>,
    pub(crate) client_id: u64,
    pub(crate) prepared_query_counter: u64,
    sync_required: bool,
    current_transaction_status: CurrentTransactionStatus,
    pub(crate) enum_registry: Arc<EnumTypeRegistry>,
    parameter_statuses: HashMap<String, String>,
}

impl<F: ConnectionFactory> PostgresClient<F> {
    pub(crate) async fn start_new_query(&mut self) -> Result<(), ElefantClientError> {
        if !self.ready_for_query {
            if self.sync_required {
                self.connection
                    .write_frontend_message(&FrontendMessage::Sync)
                    .await?;
                self.connection.flush().await?;
                self.sync_required = false;
            }

            loop {
                match self.read_next_backend_message().await {
                    Err(ElefantClientError::IoError(e)) => {
                        return Err(ElefantClientError::IoError(e));
                    }
                    Err(e) => {
                        debug!("Ignoring error while starting new query: {:?}", e);
                    }
                    Ok(msg) => match msg {
                        BackendMessage::ReadyForQuery(_) => {
                            break;
                        }
                        _ => {
                            trace!("Ignoring message while starting new query: {:?}", msg);
                        }
                    },
                }
            }
        }

        self.ready_for_query = false;
        Ok(())
    }

    pub async fn reset(&mut self) -> Result<(), ElefantClientError> {
        if !self.ready_for_query {
            if self.sync_required {
                self.connection
                    .write_frontend_message(&FrontendMessage::Sync)
                    .await?;
                self.connection.flush().await?;
                self.sync_required = false;
            }

            loop {
                match self.read_next_backend_message().await {
                    Err(ElefantClientError::IoError(io_err)) => {
                        return Err(ElefantClientError::IoError(io_err));
                    }
                    Err(e) => {
                        debug!("Ignoring error while resetting elefant client: {:?}", e);
                    }
                    Ok(msg) => match msg {
                        BackendMessage::ReadyForQuery(rfq) => {
                            self.current_transaction_status = rfq.current_transaction_status;
                            self.ready_for_query = true;
                            break;
                        }
                        _ => {
                            debug!("Ignoring message while resetting elefant client: {:?}", msg);
                        }
                    },
                }
            }
        }

        // If the connection was left in a transaction, roll it back to ensure a clean state.
        if self.current_transaction_status == CurrentTransactionStatus::InTransaction
            || self.current_transaction_status == CurrentTransactionStatus::InFailedTransaction
        {
            debug!("Rolling back lingering transaction during pool reset");
            self.connection
                .write_frontend_message(&FrontendMessage::Query(crate::protocol::Query {
                    query: std::borrow::Cow::Borrowed("ROLLBACK;"),
                }))
                .await?;
            self.connection.flush().await?;
            self.ready_for_query = false;
            loop {
                if let BackendMessage::ReadyForQuery(rfq) = self.read_next_backend_message().await?
                {
                    self.current_transaction_status = rfq.current_transaction_status;
                    self.ready_for_query = true;
                    break;
                }
            }
        }

        Ok(())
    }

    /// Get a server parameter received via ParameterStatus messages.
    ///
    /// PostgreSQL sends these during connection startup and whenever a session
    /// parameter changes. Common parameters include `server_version`,
    /// `server_encoding`, `TimeZone`, etc.
    pub fn get_parameter(&self, name: &str) -> Option<&str> {
        self.parameter_statuses.get(name).map(|s| s.as_str())
    }

    /// Gracefully close this connection by sending a Terminate message to the backend
    /// and a TLS close_notify (if applicable). Consumes the client so it cannot be used afterward.
    pub async fn close(mut self) -> Result<(), ElefantClientError> {
        self.connection
            .write_frontend_message(&FrontendMessage::Terminate)
            .await?;
        self.connection.flush().await?;
        // Best-effort TLS shutdown — the connection is already logically closed.
        let _ = self.connection.shutdown().await;
        Ok(())
    }

    pub(crate) async fn new(
        connection: PostgresConnection<F::Connection>,
        settings: &PostgresConnectionSettings,
        enum_registry: Arc<EnumTypeRegistry>,
        channel_binding_data: Option<Vec<u8>>,
    ) -> Result<Self, ElefantClientError> {
        let mut client = Self {
            connection,
            pool: None,
            ready_for_query: false,
            write_buffer: Vec::new(),
            client_id: CLIENT_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            prepared_query_counter: 1,
            sync_required: false,
            current_transaction_status: CurrentTransactionStatus::Idle,
            enum_registry,
            parameter_statuses: HashMap::new(),
        };

        client.establish(settings, channel_binding_data).await?;
        Ok(client)
    }

    /// Helper method for reading backend messages while ignoring and handling "async" messages.
    pub(crate) async fn read_next_backend_message(
        &mut self,
    ) -> Result<BackendMessage<'_>, ElefantClientError> {
        loop {
            let connection: &mut PostgresConnection<F::Connection> =
                reborrow_until_polonius!(&mut self.connection);
            let msg = connection.read_backend_message().await?;
            match msg {
                BackendMessage::NoticeResponse(nr) => {
                    debug!("Received notice response from postgres: {:?}", nr);
                }
                BackendMessage::ParameterStatus(ps) => {
                    debug!("Received parameter status from postgres: {:?}", ps);
                    self.parameter_statuses
                        .insert(ps.name.into_owned(), ps.value.into_owned());
                }
                _ => {
                    return Ok(msg);
                }
            }
        }
    }
}

static CLIENT_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

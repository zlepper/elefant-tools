mod copy;
mod easy_client;
mod establish;
mod query;
mod statements;

use crate::pool::{ConnectionFactory, PostgresPool};
use crate::protocol::{
    BackendMessage, CurrentTransactionStatus, FrontendMessage, PostgresConnection,
};
use crate::{reborrow_until_polonius, ElefantClientError, PostgresConnectionSettings};
use std::sync::atomic::AtomicU64;
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

    pub(crate) async fn new(
        connection: PostgresConnection<F::Connection>,
        settings: &PostgresConnectionSettings,
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
        };

        client.establish(settings).await?;
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
                }
                _ => {
                    return Ok(msg);
                }
            }
        }
    }
}

static CLIENT_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

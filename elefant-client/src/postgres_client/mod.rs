mod establish;
mod query;
mod easy_client;
mod statements;
mod copy;

use std::sync::atomic::AtomicU64;
use futures::{AsyncBufRead, AsyncRead, AsyncWrite};
use tracing::{debug, trace};
use crate::{protocol, reborrow_until_polonius, ElefantClientError, PostgresConnectionSettings};
use crate::protocol::{BackendMessage, CurrentTransactionStatus, FrontendMessage, PostgresConnection};

pub use query::{PostgresDataRow, QueryResult, QueryResultSet, RowResultReader};
pub use statements::*;

pub struct PostgresClient<C> {
    pub(crate) connection: PostgresConnection<C>,
    pub(crate) settings: PostgresConnectionSettings,
    pub(crate) ready_for_query: bool,
    write_buffer: Vec<u8>,
    pub(crate) client_id: u64,
    pub(crate) prepared_query_counter: u64,
    sync_required: bool,
    current_transaction_status: CurrentTransactionStatus
}

impl<C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin> PostgresClient<C> {
    pub(crate) async fn start_new_query(&mut self) -> Result<(), ElefantClientError> {

        if !self.ready_for_query {
            if self.sync_required {
                self.connection.write_frontend_message(&FrontendMessage::Sync).await?;
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
            loop {
                match self.read_next_backend_message().await {
                    Err(ElefantClientError::IoError(io_err)) => {
                        return Err(ElefantClientError::IoError(io_err));
                    }
                    Err(e) => {
                        debug!("Ignoring error while resetting elefant client: {:?}", e);
                    }
                    Ok(msg) => match msg {
                        BackendMessage::ReadyForQuery(_) => {
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

        // TODO: Handle being in a transaction.
        Ok(())
    }

    pub(crate) async fn new(connection: PostgresConnection<C>, settings: PostgresConnectionSettings) -> Result<Self, ElefantClientError> {
        let mut client = Self {
            connection,
            settings,
            ready_for_query: false,
            write_buffer: Vec::new(),
            client_id: CLIENT_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            prepared_query_counter: 1,
            sync_required: false,
            current_transaction_status: CurrentTransactionStatus::Idle,
        };

        client.establish().await?;

        Ok(client)
    }

    /// Helper method for reading backend messages while ignoring and handling "async" messages.
    pub(crate) async fn read_next_backend_message(&mut self) -> Result<BackendMessage, ElefantClientError> {

        loop {
            let connection: &mut PostgresConnection<C> = reborrow_until_polonius!(&mut self.connection);
            let msg = connection.read_backend_message().await?;
            match msg {
                BackendMessage::NoticeResponse(nr) => {
                    debug!("Received notice response from postgres: {:?}", nr);
                },
                BackendMessage::ParameterStatus(ps) => {
                    debug!("Received parameter status from postgres: {:?}", ps);
                },
                _ => {
                    return Ok(msg);
                }
            }
        }
    }
}

static CLIENT_ID_COUNTER: AtomicU64 = AtomicU64::new(1);


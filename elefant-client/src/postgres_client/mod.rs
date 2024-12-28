mod establish;
mod query;
mod data_types;
mod easy_client;

use std::borrow::Cow;
use std::sync::atomic::AtomicU64;
use futures::{AsyncRead, AsyncWrite, AsyncBufRead};
use tracing::{debug, trace};
use crate::{protocol, ElefantClientError, PostgresConnectionSettings};
use crate::protocol::{BackendMessage, FrontendMessage, FrontendPMessage, PostgresConnection, sasl, SASLInitialResponse, SASLResponse, StartupMessage, StartupMessageParameter};
use crate::protocol::sasl::ChannelBinding;

pub use query::{QueryResultSet, PostgresDataRow, QueryResult,  RowResultReader, Statement};
pub use data_types::{FromSql, ToSql, FromSqlOwned};

pub struct PostgresClient<C> {
    pub(crate) connection: PostgresConnection<C>,
    pub(crate) settings: PostgresConnectionSettings,
    pub(crate) ready_for_query: bool,
    write_buffer: Vec<u8>,
    pub(crate) client_id: u64,
    pub(crate) prepared_query_counter: u64,
    sync_required: bool
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
                match self.connection.read_backend_message().await {
                    Err(protocol::PostgresMessageParseError::IoError(io_err)) => {
                        return Err(ElefantClientError::IoError(io_err));
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
                match self.connection.read_backend_message().await {
                    Err(protocol::PostgresMessageParseError::IoError(io_err)) => {
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
            sync_required: false
        };

        client.establish().await?;

        Ok(client)
    }
}

static CLIENT_ID_COUNTER: AtomicU64 = AtomicU64::new(1);


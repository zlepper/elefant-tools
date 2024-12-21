mod establish;
mod query;
mod data_types;

use std::borrow::Cow;
use futures::{AsyncRead, AsyncWrite, AsyncBufRead};
use tracing::{debug, trace};
use crate::{protocol, ElefantClientError, PostgresConnectionSettings};
use crate::protocol::{BackendMessage, FrontendMessage, FrontendPMessage, PostgresConnection, sasl, SASLInitialResponse, SASLResponse, StartupMessage, StartupMessageParameter};
use crate::protocol::sasl::ChannelBinding;

pub use query::{QueryResultSet, PostgresDataRow, QueryResult,  RowResultReader};
pub use data_types::{FromSql, ToSql, FromSqlOwned};

pub struct PostgresClient<C> {
    pub(crate) connection: PostgresConnection<C>,
    pub(crate) settings: PostgresConnectionSettings,
    pub(crate) ready_for_query: bool,
}

impl<C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin> PostgresClient<C> {
    pub(crate) async fn start_new_query(&mut self) -> Result<(), ElefantClientError> {
        if self.ready_for_query {
            self.ready_for_query = false;
            Ok(())
        } else {

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
                            self.ready_for_query = true;
                            return Ok(())
                        }
                        _ => {
                            trace!("Ignoring message while starting new query: {:?}", msg);
                        }
                    },
                }
            }
        }
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
        };

        client.establish().await?;

        Ok(client)
    }
}


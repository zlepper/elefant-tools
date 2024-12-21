mod establish;
mod query;

use std::borrow::Cow;
use futures::{AsyncRead, AsyncWrite, AsyncBufRead};
use crate::{ElefantClientError, PostgresConnectionSettings};
use crate::protocol::{BackendMessage, FrontendMessage, FrontendPMessage, PostgresConnection, sasl, SASLInitialResponse, SASLResponse, StartupMessage, StartupMessageParameter};
use crate::protocol::sasl::ChannelBinding;

pub use query::{QueryResultSet, PostgresDataRow, QueryResult, FromSql, RowResultReader, ToSql};

pub struct PostgresClient<C> {
    pub(crate) connection: PostgresConnection<C>,
    pub(crate) settings: PostgresConnectionSettings,
    pub(crate) ready_for_query: bool,
}

impl<C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin> PostgresClient<C> {
    pub(crate) fn start_new_query(&mut self) -> Result<(), ElefantClientError> {
        if self.ready_for_query {
            self.ready_for_query = false;
            Ok(())
        } else {
            Err(ElefantClientError::ClientIsNotReadyForQuery)
        }
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


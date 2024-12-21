use crate::postgres_client::PostgresClient;
use crate::protocol::{BackendMessage, FieldDescription, FrontendMessage, Query, RowDescription};
use crate::{protocol, ElefantClientError};
use futures::{AsyncBufRead, AsyncRead, AsyncWrite};
use std::borrow::Cow;
use std::error::Error;
use std::marker::PhantomData;
use tracing::{debug, info};

macro_rules! reborrow_until_polonius {
    ($e:expr) => {
        unsafe {
            // This gets around the borrow checker not supporting releasing the borrow because
            // it is only kept alive in the return statement. This should all be solved when polonius is a thing
            // properly, but for now this is the best way to go.
            &mut *(($e) as *mut _)
        }
    };
}

impl<C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin> PostgresClient<C> {
    pub async fn query(
        &mut self,
        query: &str,
        parameters: &[&(dyn ToSql + Sync)],
    ) -> Result<QueryResult<C>, ElefantClientError> {
        self.start_new_query().await?;

        if parameters.is_empty() {
            self.connection
                .write_frontend_message(&FrontendMessage::Query(Query {
                    query: Cow::Borrowed(query),
                }))
                .await?;
            self.connection.flush().await?;
        } else {
            todo!("Implement parameterized queries");
        }

        Ok(QueryResult { client: self })
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
}

pub struct QueryResult<'a, C> {
    client: &'a mut PostgresClient<C>,
}

impl<'postgres_client, C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin>
    QueryResult<'postgres_client, C>
{
    pub async fn next_result_set<'query_result>(
        &'query_result mut self,
    ) -> Result<QueryResultSet<'postgres_client, 'query_result, C>, ElefantClientError>
    {
        loop {
            let client: &mut PostgresClient<C> = reborrow_until_polonius!(self.client);
            let msg = client.connection.read_backend_message().await?;

            match msg {
                BackendMessage::CommandComplete(cc) => {
                    debug!("Command complete: {:?}", cc);
                }
                BackendMessage::RowDescription(rd) => {
                    return Ok(QueryResultSet::RowDescriptionReceived(RowResultReader {
                        client,
                        row_description: rd,
                        query_result_res: PhantomData,
                    }));
                }
                BackendMessage::DataRow(dr) => {
                    return Err(ElefantClientError::UnexpectedBackendMessage(format!(
                        "Received DataRow without receiving a RowDescription: {:?}",
                        dr
                    )));
                }
                BackendMessage::EmptyQueryResponse => {
                    debug!("Empty query response");
                }
                BackendMessage::ErrorResponse(er) => {
                    return Err(ElefantClientError::PostgresError(format!("{:?}", er)));
                }
                BackendMessage::ReadyForQuery(rfq) => {
                    println!("Ready for query: {:?}", rfq);
                    self.client.ready_for_query = true;
                    return Ok(QueryResultSet::QueryProcessingComplete);
                }
                BackendMessage::NoticeResponse(nr) => {
                    info!("Notice from postgres: {:?}", nr);
                }
                _ => {
                    return Err(ElefantClientError::UnexpectedBackendMessage(format!(
                        "{:?}",
                        msg
                    )));
                }
            }
        }
    }
}

pub enum QueryResultSet<'postgres_client, 'query_result_set, C> {
    QueryProcessingComplete,
    RowDescriptionReceived(RowResultReader<'postgres_client, 'query_result_set, C>),
}

pub struct RowResultReader<'postgres_client, 'query_result_set, C> {
    client: &'postgres_client mut PostgresClient<C>,
    row_description: RowDescription,
    // Ensures that the QueryResult cannot be used while we are processing rows.
    query_result_res: PhantomData<&'query_result_set QueryResult<'postgres_client, C>>,
}

impl<'postgres_client, 'query_result_set, C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin>
    RowResultReader<'postgres_client, 'query_result_set, C>
{
    pub async fn next_row<'row_result_reader>(
        &'row_result_reader mut self,
    ) -> Result<Option<PostgresDataRow<'postgres_client, 'row_result_reader>>, ElefantClientError>
    {
        loop {
            let client: &mut PostgresClient<C> = reborrow_until_polonius!(self.client);
            let msg = client.connection.read_backend_message().await?;

            match msg {
                BackendMessage::DataRow(dr) => {
                    return Ok(Some(PostgresDataRow {
                        row_description: &self.row_description,
                        data_row: dr,
                    }));
                },
                BackendMessage::CommandComplete(cc) => {
                    debug!("Command complete: {:?}", cc);
                    return Ok(None);
                }
                BackendMessage::ReadyForQuery(rfq) => {
                    println!("Ready for query: {:?}", rfq);
                    self.client.ready_for_query = true;
                    return Ok(None);
                }
                BackendMessage::ErrorResponse(er) => {
                    return Err(ElefantClientError::PostgresError(format!("{:?}", er)))
                }
                BackendMessage::NoticeResponse(nr) => {
                    info!("Notice from postgres: {:?}", nr);
                }
                _ => {
                    return Err(ElefantClientError::UnexpectedBackendMessage(format!(
                        "{:?}",
                        msg
                    )))
                }
            }
        }
    }
}


pub trait FromSql<'a>: Sized {
    fn from_sql_binary(
        raw: &'a [u8],
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>>;

    fn from_sql_text(
        raw: &'a str,
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>>;

    fn accepts(field: &FieldDescription) -> bool;
}

pub trait ToSql {
    fn to_sql_binary(&self, target_buffer: &mut Vec<u8>) -> Vec<u8>;
}

pub struct PostgresDataRow<'postgres_client, 'row_result_reader> {
    row_description: &'row_result_reader RowDescription,
    data_row: protocol::DataRow<'postgres_client>,
}

impl PostgresDataRow<'_, '_> {
    pub fn get_some_bytes(&self) -> &[Option<&[u8]>] {
        &self.data_row.values
    }
}
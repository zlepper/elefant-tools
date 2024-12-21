use crate::postgres_client::{FromSql, PostgresClient};
use crate::protocol::{BackendMessage, FieldDescription, FrontendMessage, Query, RowDescription, ValueFormat};
use crate::{protocol, ElefantClientError};
use futures::{AsyncBufRead, AsyncRead, AsyncWrite};
use std::borrow::Cow;
use std::error::Error;
use std::marker::PhantomData;
use tracing::{debug, info};
use crate::postgres_client::data_types::ToSql;

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
            
            self.connection.write_frontend_message(&FrontendMessage::Parse(protocol::Parse {
                destination: Cow::Borrowed(""), 
                query: Cow::Borrowed(query),
                parameter_types: vec![],
            })).await?;
            self.connection.flush().await?;
            
            loop {
                let msg = self.connection.read_backend_message().await?;
                
                match msg {
                    BackendMessage::ParseComplete => {
                        break;
                    },
                    BackendMessage::ErrorResponse(er) => {
                        return Err(ElefantClientError::PostgresError(format!("{:?}", er)));
                    },
                    BackendMessage::NoticeResponse(nr) => {
                        info!("Notice from postgres: {:?}", nr);
                    },
                    _ => {
                        return Err(ElefantClientError::UnexpectedBackendMessage(format!("{:?}", msg)));
                    }
                }
            }
            
            let mut parameter_values: Vec<Option<&[u8]>> = Vec::with_capacity(parameters.len());
            self.write_buffer.clear();

            let mut parameter_positions = Vec::with_capacity(parameters.len());
            
            for param in parameters.iter() {
                let start_index = self.write_buffer.len();
                param.to_sql_binary(&mut self.write_buffer);
                let end_index = self.write_buffer.len();
                parameter_positions.push((start_index, end_index));
            }
            
            for (start_index, end_index) in parameter_positions {
                // TODO: Figure out how to handle `None`/NULL values.
                parameter_values.push(Some(&self.write_buffer[start_index..end_index]));
            }
            
            
            self.connection.write_frontend_message(&FrontendMessage::Bind(protocol::Bind {
                source_statement_name: Cow::Borrowed(""),
                destination_portal_name: Cow::Borrowed(""),
                parameter_values,
                result_column_formats: vec![ValueFormat::Binary],
                parameter_formats: vec![ValueFormat::Binary],
            })).await?;
            self.connection.flush().await?;
            
            loop {
                let msg = self.connection.read_backend_message().await?;
                
                match msg {
                    BackendMessage::BindComplete => {
                        break;
                    },
                    BackendMessage::ErrorResponse(er) => {
                        return Err(ElefantClientError::PostgresError(format!("{:?}", er)));
                    },
                    BackendMessage::NoticeResponse(nr) => {
                        info!("Notice from postgres: {:?}", nr);
                    },
                    _ => {
                        return Err(ElefantClientError::UnexpectedBackendMessage(format!("{:?}", msg)));
                    }
                }
            }
            
            
        }

        Ok(QueryResult { client: self })
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


pub struct PostgresDataRow<'postgres_client, 'row_result_reader> {
    row_description: &'row_result_reader RowDescription,
    data_row: protocol::DataRow<'postgres_client>,
}

impl<'postgres_client> PostgresDataRow<'postgres_client, '_> {
    pub fn get_some_bytes(&self) -> &[Option<&[u8]>] {
        &self.data_row.values
    }
    
    pub fn get<T>(&self, index: usize) -> Result<T, ElefantClientError> 
    where T: FromSql<'postgres_client>
    {
        let field = &self.row_description.fields[index];
        let raw = self.data_row.values[index].unwrap();
        if !T::accepts(field) {
            return Err(ElefantClientError::UnsupportedFieldType {
                postgres_field: field.clone(),
                desired_rust_type: std::any::type_name::<T>(),
            })
        }
        
        let value = match field.format {
            ValueFormat::Text => {
                let raw_str = std::str::from_utf8(raw).map_err(|e| ElefantClientError::IoError(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?;
                T::from_sql_text(raw_str, field).map_err(|e| ElefantClientError::DataTypeParseError {
                    original_error: e,
                    column_index: index,
                })?
            }
            ValueFormat::Binary => {
                T::from_sql_binary(raw, field).map_err(|e| ElefantClientError::DataTypeParseError {
                    original_error: e,
                    column_index: index,
                })?
            }
        };
        
        Ok(value)
    }
    
}
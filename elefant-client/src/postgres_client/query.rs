use crate::postgres_client::data_types::ToSql;
use crate::postgres_client::{FromSql, PostgresClient};
use crate::protocol::{
    BackendMessage, FieldDescription, FrontendMessage, Query, RowDescription, ValueFormat,
};
use crate::{protocol, ElefantClientError};
use futures::{AsyncBufRead, AsyncRead, AsyncWrite};
use std::borrow::Cow;
use std::error::Error;
use std::marker::PhantomData;
use std::rc::Rc;
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
        query: &(impl Statement + ?Sized),
        parameters: &[&(dyn ToSql)],
    ) -> Result<QueryResult<C>, ElefantClientError> {
        query.send(self, parameters).await
    }

    pub async fn prepare_query(
        &mut self,
        query: &str,
    ) -> Result<PreparedQuery, ElefantClientError> {
        self.prepared_query_counter += 1;

        let name = format!("elefant_prepared_query_{}", self.prepared_query_counter);

        self.prepare_with_name(query, Some(name)).await
    }

    async fn prepare_with_name(
        &mut self,
        query: &str,
        name: Option<String>,
    ) -> Result<PreparedQuery, ElefantClientError> {
        self.start_new_query().await?;

        let destination = name
            .as_ref()
            .map(|n| Cow::Borrowed(n.as_ref()))
            .unwrap_or(Cow::Borrowed(""));

        self.connection
            .write_frontend_message(&FrontendMessage::Parse(protocol::Parse {
                destination: destination.clone(),
                query: Cow::Borrowed(query),
                parameter_types: vec![],
            }))
            .await?;

        self.connection
            .write_frontend_message(&FrontendMessage::Describe(protocol::Describe {
                name: destination,
                target: protocol::DescribeTarget::Statement,
            }))
            .await?;
        self.connection
            .write_frontend_message(&FrontendMessage::Flush)
            .await?;
        self.connection.flush().await?;

        loop {
            let msg = self.connection.read_backend_message().await?;

            match msg {
                BackendMessage::ParseComplete => {
                    break;
                }
                BackendMessage::ErrorResponse(er) => {
                    return Err(ElefantClientError::PostgresError(format!("{:?}", er)));
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

        let parameter_description = loop {
            let msg = self.connection.read_backend_message().await?;

            match msg {
                BackendMessage::ParameterDescription(pd) => {
                    break pd;
                }
                BackendMessage::ErrorResponse(er) => {
                    return Err(ElefantClientError::PostgresError(format!("{:?}", er)));
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
            };
        };

        let row_description = loop {
            let msg = self.connection.read_backend_message().await?;

            match msg {
                BackendMessage::RowDescription(rd) => {
                    break PreparedQueryResult::RowDescription(RowDescription {
                        fields: rd
                            .fields
                            .iter()
                            .map(|f| FieldDescription {
                                name: f.name.clone(),
                                format: ValueFormat::Binary,
                                data_type_oid: f.data_type_oid,
                                data_type_size: f.data_type_size,
                                type_modifier: f.type_modifier,
                                table_oid: f.table_oid,
                                column_attribute_number: f.column_attribute_number,
                            })
                            .collect(),
                    });
                }
                BackendMessage::NoData => {
                    break PreparedQueryResult::NoData;
                }
                BackendMessage::ErrorResponse(er) => {
                    return Err(ElefantClientError::PostgresError(format!("{:?}", er)));
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
            };
        };

        self.ready_for_query = true;

        Ok(PreparedQuery {
            name,
            client_id: self.client_id,
            parameter_description,
            result: Rc::new(row_description),
        })
    }
}

enum PreparedQueryResult {
    RowDescription(protocol::RowDescription),
    NoData,
}

pub struct QueryResult<'postgres_client, C> {
    client: &'postgres_client mut PostgresClient<C>,
    prepared_query_result: Option<Rc<PreparedQueryResult>>,
}

impl<'postgres_client, C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin>
    QueryResult<'postgres_client, C>
{
    pub async fn next_result_set<'query_result>(
        &'query_result mut self,
    ) -> Result<QueryResultSet<'postgres_client, 'query_result, C>, ElefantClientError> {
        if let Some(prepared) = self.prepared_query_result.take() {
            self.prepared_query_result = Some(Rc::new(PreparedQueryResult::NoData));
            return match prepared.as_ref() {
                PreparedQueryResult::RowDescription(rd) => {
                    let client: &mut PostgresClient<C> = reborrow_until_polonius!(self.client);
                    Ok(QueryResultSet::RowDescriptionReceived(RowResultReader {
                        client,
                        row_description: rd.clone(),
                        query_result_res: PhantomData,
                    }))
                }
                PreparedQueryResult::NoData => Ok(QueryResultSet::QueryProcessingComplete),
            };
        }

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
                }
                BackendMessage::CommandComplete(cc) => {
                    debug!("Command complete: {:?}", cc);
                    return Ok(None);
                }
                BackendMessage::ReadyForQuery(rfq) => {
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
    where
        T: FromSql<'postgres_client>,
    {
        let field = &self.row_description.fields[index];

        if !T::accepts(field) {
            return Err(ElefantClientError::UnsupportedFieldType {
                postgres_field: field.clone(),
                desired_rust_type: std::any::type_name::<T>(),
            });
        }

        if let Some(raw) = self.data_row.values[index] {
            let value = match field.format {
                ValueFormat::Text => {
                    let raw_str = std::str::from_utf8(raw).map_err(|e| {
                        ElefantClientError::IoError(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            e,
                        ))
                    })?;
                    T::from_sql_text(raw_str, field).map_err(|e| {
                        ElefantClientError::DataTypeParseError {
                            original_error: e,
                            column_index: index,
                        }
                    })?
                }
                ValueFormat::Binary => T::from_sql_binary(raw, field).map_err(|e| {
                    ElefantClientError::DataTypeParseError {
                        original_error: e,
                        column_index: index,
                    }
                })?,
            };

            Ok(value)
        } else {
            T::from_null(field)
        }
    }
}

pub struct PreparedQuery {
    name: Option<String>,
    client_id: u64,
    parameter_description: protocol::ParameterDescription,
    result: Rc<PreparedQueryResult>,
}

trait Sealed {}

pub trait Statement: Sealed {
    async fn send<'postgres_client, C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin>(
        &self,
        client: &'postgres_client mut PostgresClient<C>,
        parameters: &[&(dyn ToSql)],
    ) -> Result<QueryResult<'postgres_client, C>, ElefantClientError>;
}

impl Sealed for PreparedQuery {}

impl Statement for PreparedQuery {
    async fn send<'postgres_client, C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin>(
        &self,
        client: &'postgres_client mut PostgresClient<C>,
        parameters: &[&(dyn ToSql)],
    ) -> Result<QueryResult<'postgres_client, C>, ElefantClientError> {
        client.start_new_query().await?;
        client.sync_required = true;

        let mut parameter_values: Vec<Option<&[u8]>> = Vec::with_capacity(parameters.len());
        client.write_buffer.clear();

        let mut parameter_positions = Vec::with_capacity(parameters.len());

        for param in parameters.iter() {
            if param.is_null() {
                parameter_positions.push(None);
                continue;
            }
            let start_index = client.write_buffer.len();
            param.to_sql_binary(&mut client.write_buffer).map_err(|e| {
                ElefantClientError::IoError(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            })?;
            let end_index = client.write_buffer.len();
            parameter_positions.push(Some((start_index, end_index)));
        }

        for position in parameter_positions {
            if let Some((start_index, end_index)) = position {
                parameter_values.push(Some(&client.write_buffer[start_index..end_index]));
            } else {
                parameter_values.push(None);
            }
        }

        let source_statement_name = self
            .name
            .as_ref()
            .map(|n| Cow::Borrowed(n.as_str()))
            .unwrap_or(Cow::Borrowed(""));

        client
            .connection
            .write_frontend_message(&FrontendMessage::Bind(protocol::Bind {
                source_statement_name,
                destination_portal_name: Cow::Borrowed(""),
                parameter_values,
                result_column_formats: vec![ValueFormat::Binary],
                parameter_formats: vec![ValueFormat::Binary],
            }))
            .await?;
        client
            .connection
            .write_frontend_message(&FrontendMessage::Execute(protocol::Execute {
                portal_name: Cow::Borrowed(""),
                max_rows: 0,
            }))
            .await?;
        client
            .connection
            .write_frontend_message(&FrontendMessage::Flush)
            .await?;
        client.connection.flush().await?;

        loop {
            let msg = client.connection.read_backend_message().await?;

            match msg {
                BackendMessage::BindComplete => {
                    break;
                }
                BackendMessage::ErrorResponse(er) => {
                    return Err(ElefantClientError::PostgresError(format!("{:?}", er)));
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

        Ok(QueryResult {
            client,
            prepared_query_result: Some(self.result.clone()),
        })
    }
}

impl Sealed for str {}

impl Statement for str {
    async fn send<'postgres_client, C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin>(
        &self,
        client: &'postgres_client mut PostgresClient<C>,
        parameters: &[&(dyn ToSql)],
    ) -> Result<QueryResult<'postgres_client, C>, ElefantClientError> {
        if parameters.is_empty() {
            client.start_new_query().await?;
            client
                .connection
                .write_frontend_message(&FrontendMessage::Query(Query {
                    query: Cow::Borrowed(self),
                }))
                .await?;
            client.connection.flush().await?;

            Ok(QueryResult {
                client,
                prepared_query_result: None,
            })
        } else {
            let prepared_query = client.prepare_with_name(self, None).await?;
            prepared_query.send(client, parameters).await
        }
    }
}

impl Sealed for String {}

impl Statement for String {
    async fn send<'postgres_client, C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin>(
        &self,
        client: &'postgres_client mut PostgresClient<C>,
        parameters: &[&(dyn ToSql)],
    ) -> Result<QueryResult<'postgres_client, C>, ElefantClientError> {
        self.as_str().send(client, parameters).await
    }
}

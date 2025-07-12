use crate::postgres_client::PostgresClient;
use crate::protocol::{
    BackendMessage, FieldDescription, FrontendMessage, RowDescription, ValueFormat,
};
use crate::{protocol, ElefantClientError, FromSql, FromSqlOwned, FromSqlRowOwned, ToSql};
use crate::protocol::async_io::ElefantAsyncReadWrite;
use std::borrow::Cow;
use std::marker::PhantomData;
use std::rc::Rc;
use tracing::{debug, trace};
use crate::postgres_client::statements::{PreparedQuery, Statement};

#[macro_export] macro_rules! reborrow_until_polonius {
    ($e:expr) => {
        unsafe {
            // This gets around the borrow checker not supporting releasing the borrow because
            // it is only kept alive in the return statement. This should all be solved when polonius is a thing
            // properly, but for now this is the best way to go.
            &mut *(($e) as *mut _)
        }
    };
}

impl<C: ElefantAsyncReadWrite> PostgresClient<C> {
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

    pub(crate) async fn prepare_with_name(
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

        let msg = self.read_next_backend_message().await?;

        match msg {
            BackendMessage::ParseComplete => {
                trace!("Parse complete");
            }
            BackendMessage::ErrorResponse(er) => {
                return Err(ElefantClientError::PostgresError(format!("{:?}", er)));
            }
            _ => {
                return Err(ElefantClientError::UnexpectedBackendMessage(format!(
                    "{:?}",
                    msg
                )));
            }
        }

        let parameter_description = {
            let msg = self.read_next_backend_message().await?;

            match msg {
                BackendMessage::ParameterDescription(pd) => {
                    pd
                }
                BackendMessage::ErrorResponse(er) => {
                    return Err(ElefantClientError::PostgresError(format!("{:?}", er)));
                }
                _ => {
                    return Err(ElefantClientError::UnexpectedBackendMessage(format!(
                        "{:?}",
                        msg
                    )));
                }
            }
        };

        let row_description = {
            let msg = self.read_next_backend_message().await?;

            match msg {
                BackendMessage::RowDescription(rd) => {
                    PreparedQueryResult::RowDescription(RowDescription {
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
                    })
                }
                BackendMessage::NoData => {
                    PreparedQueryResult::NoData
                }
                BackendMessage::ErrorResponse(er) => {
                    return Err(ElefantClientError::PostgresError(format!("{:?}", er)));
                },
                _ => {
                    return Err(ElefantClientError::UnexpectedBackendMessage(format!(
                        "{:?}",
                        msg
                    )));
                }
            }
        };

        self.ready_for_query = true;

        Ok(PreparedQuery::new(name, self.client_id, parameter_description, row_description))
    }
}

pub(crate) enum PreparedQueryResult {
    RowDescription(protocol::RowDescription),
    NoData,
}

pub struct QueryResult<'postgres_client, C> {
    client: &'postgres_client mut PostgresClient<C>,
    prepared_query_result: Option<Rc<PreparedQueryResult>>,
}

impl<'postgres_client, C: ElefantAsyncReadWrite>
    QueryResult<'postgres_client, C>
{
    pub(crate) fn new(
        client: &'postgres_client mut PostgresClient<C>,
        prepared_query_result: Option<Rc<PreparedQueryResult>>,
    ) -> Self {
        Self {
            client,
            prepared_query_result,
        }
    }

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
            let msg = client.read_next_backend_message().await?;

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
                    self.client.current_transaction_status = rfq.current_transaction_status;
                    return Ok(QueryResultSet::QueryProcessingComplete);
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

    pub async fn collect_to_vec<T>(mut self) -> Result<Vec<T>, ElefantClientError>
        where T: FromSqlRowOwned
    {
        let mut results = Vec::new();
        loop {
            let result_set = self.next_result_set().await?;
            match result_set {
                QueryResultSet::QueryProcessingComplete => {
                    return Ok(results);
                }
                QueryResultSet::RowDescriptionReceived(mut row_result_reader) => {
                    loop {
                        let row = row_result_reader.next_row().await?;
                        match row {
                            Some(row) => {
                                let value = T::from_sql_row(&row)?;
                                results.push(value);
                            }
                            None => {
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    pub async fn collect_single_column_to_vec<T>(mut self) -> Result<Vec<T>, ElefantClientError>
        where T: FromSqlOwned
    {
        let mut results = Vec::new();
        loop {
            let result_set = self.next_result_set().await?;
            match result_set {
                QueryResultSet::QueryProcessingComplete => {
                    return Ok(results);
                }
                QueryResultSet::RowDescriptionReceived(mut row_result_reader) => {
                    loop {
                        let row = row_result_reader.next_row().await?;
                        match row {
                            Some(row) => {
                                let value: T = row.get(0)?;
                                results.push(value);
                            }
                            None => {
                                break;
                            }
                        }
                    }
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

impl<'postgres_client, 'query_result_set, C: ElefantAsyncReadWrite>
    RowResultReader<'postgres_client, 'query_result_set, C>
{
    pub async fn next_row<'row_result_reader>(
        &'row_result_reader mut self,
    ) -> Result<Option<PostgresDataRow<'postgres_client, 'row_result_reader>>, ElefantClientError>
    {
        let client: &mut PostgresClient<C> = reborrow_until_polonius!(self.client);
        let msg = client.read_next_backend_message().await?;

        match msg {
            BackendMessage::DataRow(dr) => {
                Ok(Some(PostgresDataRow {
                    row_description: &self.row_description,
                    data_row: dr,
                }))
            }
            BackendMessage::CommandComplete(cc) => {
                debug!("Command complete: {:?}", cc);
                Ok(None)
            }
            BackendMessage::ReadyForQuery(rfq) => {
                self.client.ready_for_query = true;
                self.client.current_transaction_status = rfq.current_transaction_status;
                Ok(None)
            }
            BackendMessage::ErrorResponse(er) => {
                Err(ElefantClientError::PostgresError(format!("{:?}", er)))
            }
            _ => {
                Err(ElefantClientError::UnexpectedBackendMessage(format!(
                    "{:?}",
                    msg
                )))
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

    pub fn column_count(&self) -> usize {
        self.row_description.fields.len()
    }

    pub fn require_columns(&self, count: usize) -> Result<(), ElefantClientError> {
        if self.column_count() < count {
            return Err(ElefantClientError::NotEnoughColumns {
                desired: count,
                actual: self.column_count(),
            });
        }
        Ok(())
    }
}


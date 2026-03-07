use crate::pool::ConnectionFactory;
use crate::postgres_client::statements::{PreparedQuery, Statement};
use crate::postgres_client::PostgresClient;
use crate::protocol::{
    BackendMessage, FieldDescription, FrontendMessage, RowDescription, ValueFormat,
};
use crate::types::EnumTypeRegistry;
use crate::{
    protocol, ElefantClientError, FromSql, FromSqlBinary, FromSqlBinaryOwned, FromSqlRowOwned,
    FromSqlText, FromSqlTextOwned, ToSql,
};
use std::borrow::Cow;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;
use tracing::{debug, trace};

#[macro_export]
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

impl<F: ConnectionFactory> PostgresClient<F> {
    /// Execute a query in binary mode - always uses prepared statements
    pub async fn query(
        &mut self,
        query: &(impl Statement + ?Sized),
        parameters: &[&dyn ToSql],
    ) -> Result<QueryResult<'_, F>, ElefantClientError> {
        let prepared = query.prepare(self).await?;
        prepared.execute(self, parameters).await
    }

    /// Execute a simple query in text mode - only accepts &str, no parameters
    pub async fn query_simple(
        &mut self,
        query: &str,
    ) -> Result<SimpleQueryResult<'_, F>, ElefantClientError> {
        self.start_new_query().await?;
        self.connection
            .write_frontend_message(&FrontendMessage::Query(protocol::Query {
                query: Cow::Borrowed(query),
            }))
            .await?;
        self.connection.flush().await?;

        Ok(SimpleQueryResult::new(self, None))
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
                return Err(ElefantClientError::PostgresError(format!("{er:?}")));
            }
            _ => {
                return Err(ElefantClientError::UnexpectedBackendMessage(format!(
                    "{msg:?}"
                )));
            }
        }

        let parameter_description = {
            let msg = self.read_next_backend_message().await?;

            match msg {
                BackendMessage::ParameterDescription(pd) => pd,
                BackendMessage::ErrorResponse(er) => {
                    return Err(ElefantClientError::PostgresError(format!("{er:?}")));
                }
                _ => {
                    return Err(ElefantClientError::UnexpectedBackendMessage(format!(
                        "{msg:?}"
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
                BackendMessage::NoData => PreparedQueryResult::NoData,
                BackendMessage::ErrorResponse(er) => {
                    return Err(ElefantClientError::PostgresError(format!("{er:?}")));
                }
                _ => {
                    return Err(ElefantClientError::UnexpectedBackendMessage(format!(
                        "{msg:?}"
                    )));
                }
            }
        };

        self.ready_for_query = true;

        Ok(PreparedQuery::new(
            name,
            self.client_id,
            parameter_description,
            row_description,
        ))
    }
}

pub(crate) enum PreparedQueryResult {
    RowDescription(protocol::RowDescription),
    NoData,
}

// Shared base structure for common query result functionality
pub struct QueryResultBase<'postgres_client, F: ConnectionFactory> {
    client: &'postgres_client mut PostgresClient<F>,
    prepared_query_result: Option<Rc<PreparedQueryResult>>,
}

// Binary mode query result - enforces FromSqlBinary constraint
pub struct QueryResult<'postgres_client, F: ConnectionFactory> {
    base: QueryResultBase<'postgres_client, F>,
}

// Simple mode query result - enforces FromSqlText constraint
pub struct SimpleQueryResult<'postgres_client, F: ConnectionFactory> {
    base: QueryResultBase<'postgres_client, F>,
}

impl<'postgres_client, F: ConnectionFactory> QueryResultBase<'postgres_client, F> {
    pub(crate) fn new(
        client: &'postgres_client mut PostgresClient<F>,
        prepared_query_result: Option<Rc<PreparedQueryResult>>,
    ) -> Self {
        Self {
            client,
            prepared_query_result,
        }
    }

    pub async fn next_result_set<'query_result>(
        &'query_result mut self,
    ) -> Result<QueryResultSet<'postgres_client, 'query_result, F>, ElefantClientError> {
        if let Some(prepared) = self.prepared_query_result.take() {
            self.prepared_query_result = Some(Rc::new(PreparedQueryResult::NoData));
            return match prepared.as_ref() {
                PreparedQueryResult::RowDescription(rd) => {
                    let client: &mut PostgresClient<F> = reborrow_until_polonius!(self.client);
                    let registry = client.enum_registry.clone();
                    Ok(QueryResultSet::RowDescriptionReceived(RowResultReader {
                        client,
                        row_description: rd.clone(),
                        enum_registry: registry,
                        query_result_res: PhantomData,
                    }))
                }
                PreparedQueryResult::NoData => Ok(QueryResultSet::QueryProcessingComplete),
            };
        }

        loop {
            let client: &mut PostgresClient<F> = reborrow_until_polonius!(self.client);
            let msg = client.read_next_backend_message().await?;

            match msg {
                BackendMessage::CommandComplete(cc) => {
                    debug!("Command complete: {:?}", cc);
                }
                BackendMessage::RowDescription(rd) => {
                    let registry = client.enum_registry.clone();
                    return Ok(QueryResultSet::RowDescriptionReceived(RowResultReader {
                        client,
                        row_description: rd,
                        enum_registry: registry,
                        query_result_res: PhantomData,
                    }));
                }
                BackendMessage::DataRow(dr) => {
                    return Err(ElefantClientError::UnexpectedBackendMessage(format!(
                        "Received DataRow without receiving a RowDescription: {dr:?}"
                    )));
                }
                BackendMessage::EmptyQueryResponse => {
                    debug!("Empty query response");
                }
                BackendMessage::ErrorResponse(er) => {
                    return Err(ElefantClientError::PostgresError(format!("{er:?}")));
                }
                BackendMessage::ReadyForQuery(rfq) => {
                    self.client.ready_for_query = true;
                    self.client.current_transaction_status = rfq.current_transaction_status;
                    return Ok(QueryResultSet::QueryProcessingComplete);
                }
                _ => {
                    return Err(ElefantClientError::UnexpectedBackendMessage(format!(
                        "{msg:?}"
                    )));
                }
            }
        }
    }
}

// QueryResult implementations (binary mode)
impl<'postgres_client, F: ConnectionFactory> QueryResult<'postgres_client, F> {
    pub(crate) fn new(
        client: &'postgres_client mut PostgresClient<F>,
        prepared_query_result: Option<Rc<PreparedQueryResult>>,
    ) -> Self {
        Self {
            base: QueryResultBase::new(client, prepared_query_result),
        }
    }

    pub async fn next_result_set<'query_result>(
        &'query_result mut self,
    ) -> Result<QueryResultSet<'postgres_client, 'query_result, F>, ElefantClientError> {
        self.base.next_result_set().await
    }

    pub async fn collect_to_vec<T>(mut self) -> Result<Vec<T>, ElefantClientError>
    where
        T: FromSqlRowOwned,
    {
        let mut results = Vec::new();
        loop {
            match self.next_result_set().await? {
                QueryResultSet::QueryProcessingComplete => return Ok(results),
                QueryResultSet::RowDescriptionReceived(reader) => {
                    results.extend(reader.collect_to_vec::<T>().await?);
                }
            }
        }
    }

    pub async fn collect_single_column_to_vec<T>(mut self) -> Result<Vec<T>, ElefantClientError>
    where
        T: FromSqlBinaryOwned,
    {
        let mut results = Vec::new();
        loop {
            match self.next_result_set().await? {
                QueryResultSet::QueryProcessingComplete => return Ok(results),
                QueryResultSet::RowDescriptionReceived(mut row_result_reader) => {
                    while let Some(row) = row_result_reader.next_row().await? {
                        results.push(row.get_binary(0)?);
                    }
                }
            }
        }
    }
}

// SimpleQueryResult implementations (text mode)
impl<'postgres_client, F: ConnectionFactory> SimpleQueryResult<'postgres_client, F> {
    pub(crate) fn new(
        client: &'postgres_client mut PostgresClient<F>,
        prepared_query_result: Option<Rc<PreparedQueryResult>>,
    ) -> Self {
        Self {
            base: QueryResultBase::new(client, prepared_query_result),
        }
    }

    pub async fn next_result_set<'query_result>(
        &'query_result mut self,
    ) -> Result<QueryResultSet<'postgres_client, 'query_result, F>, ElefantClientError> {
        self.base.next_result_set().await
    }

    pub async fn collect_to_vec<T>(mut self) -> Result<Vec<T>, ElefantClientError>
    where
        T: FromSqlRowOwned,
    {
        let mut results = Vec::new();
        loop {
            match self.next_result_set().await? {
                QueryResultSet::QueryProcessingComplete => return Ok(results),
                QueryResultSet::RowDescriptionReceived(reader) => {
                    results.extend(reader.collect_to_vec::<T>().await?);
                }
            }
        }
    }

    /// Advance to the next result set and collect all rows into a `Vec<T>`.
    ///
    /// Returns `BatchQueryUnexpectedEnd` if the query has already completed
    /// (no more result sets available).
    pub async fn collect_next_to_vec<T: FromSqlRowOwned>(
        &mut self,
    ) -> Result<Vec<T>, ElefantClientError> {
        match self.next_result_set().await? {
            QueryResultSet::RowDescriptionReceived(reader) => Ok(reader.collect_to_vec().await?),
            QueryResultSet::QueryProcessingComplete => {
                Err(ElefantClientError::BatchQueryUnexpectedEnd)
            }
        }
    }

    pub async fn collect_single_column_to_vec<T>(mut self) -> Result<Vec<T>, ElefantClientError>
    where
        T: FromSqlTextOwned,
    {
        let mut results = Vec::new();
        loop {
            match self.next_result_set().await? {
                QueryResultSet::QueryProcessingComplete => return Ok(results),
                QueryResultSet::RowDescriptionReceived(mut row_result_reader) => {
                    while let Some(row) = row_result_reader.next_row().await? {
                        results.push(row.get_text(0)?);
                    }
                }
            }
        }
    }
}

pub enum QueryResultSet<'postgres_client, 'query_result_set, F: ConnectionFactory> {
    QueryProcessingComplete,
    RowDescriptionReceived(RowResultReader<'postgres_client, 'query_result_set, F>),
}

pub struct RowResultReader<'postgres_client, 'query_result_set, F: ConnectionFactory> {
    client: &'postgres_client mut PostgresClient<F>,
    row_description: RowDescription,
    enum_registry: Arc<EnumTypeRegistry>,
    // Ensures that the QueryResult cannot be used while we are processing rows.
    query_result_res: PhantomData<&'query_result_set QueryResult<'postgres_client, F>>,
}

impl<'postgres_client, 'query_result_set, F: ConnectionFactory>
    RowResultReader<'postgres_client, 'query_result_set, F>
{
    pub async fn next_row<'row_result_reader>(
        &'row_result_reader mut self,
    ) -> Result<Option<PostgresDataRow<'postgres_client, 'row_result_reader>>, ElefantClientError>
    {
        let client: &mut PostgresClient<F> = reborrow_until_polonius!(self.client);
        let msg = client.read_next_backend_message().await?;

        match msg {
            BackendMessage::DataRow(dr) => Ok(Some(PostgresDataRow {
                row_description: &self.row_description,
                data_row: dr,
                enum_registry: &self.enum_registry,
            })),
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
                Err(ElefantClientError::PostgresError(format!("{er:?}")))
            }
            _ => Err(ElefantClientError::UnexpectedBackendMessage(format!(
                "{msg:?}"
            ))),
        }
    }

    pub async fn collect_to_vec<T>(mut self) -> Result<Vec<T>, ElefantClientError>
    where
        T: FromSqlRowOwned,
    {
        let mut results = Vec::new();
        while let Some(row) = self.next_row().await? {
            results.push(T::from_sql_row(&row)?);
        }
        Ok(results)
    }
}

pub struct PostgresDataRow<'postgres_client, 'row_result_reader> {
    row_description: &'row_result_reader RowDescription,
    data_row: protocol::DataRow<'postgres_client>,
    enum_registry: &'row_result_reader EnumTypeRegistry,
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

        if !T::accepts_with_registry(field, self.enum_registry) {
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

    /// Get a value from binary format data - enforces compile-time constraint that T supports binary parsing
    pub fn get_binary<T>(&self, index: usize) -> Result<T, ElefantClientError>
    where
        T: FromSqlBinary<'postgres_client>,
    {
        let field = &self.row_description.fields[index];

        if !T::accepts_with_registry(field, self.enum_registry) {
            return Err(ElefantClientError::UnsupportedFieldType {
                postgres_field: field.clone(),
                desired_rust_type: std::any::type_name::<T>(),
            });
        }

        if let Some(raw) = self.data_row.values[index] {
            let value = T::from_sql_binary(raw, field).map_err(|e| {
                ElefantClientError::DataTypeParseError {
                    original_error: e,
                    column_index: index,
                }
            })?;
            Ok(value)
        } else {
            T::from_null(field)
        }
    }

    /// Get a value from text format data - enforces compile-time constraint that T supports text parsing
    pub fn get_text<T>(&self, index: usize) -> Result<T, ElefantClientError>
    where
        T: FromSqlText<'postgres_client>,
    {
        let field = &self.row_description.fields[index];

        if !T::accepts_with_registry(field, self.enum_registry) {
            return Err(ElefantClientError::UnsupportedFieldType {
                postgres_field: field.clone(),
                desired_rust_type: std::any::type_name::<T>(),
            });
        }

        if let Some(raw) = self.data_row.values[index] {
            let raw_str = std::str::from_utf8(raw).map_err(|e| {
                ElefantClientError::IoError(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            })?;
            let value = T::from_sql_text(raw_str, field).map_err(|e| {
                ElefantClientError::DataTypeParseError {
                    original_error: e,
                    column_index: index,
                }
            })?;
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

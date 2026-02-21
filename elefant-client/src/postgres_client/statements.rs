use crate::postgres_client::query::PreparedQueryResult;
use crate::protocol::async_io::ElefantAsyncReadWrite;
use crate::protocol::{BackendMessage, FrontendMessage, ValueFormat};
use crate::{protocol, ElefantClientError, PostgresClient, QueryResult, ToSql};
use std::borrow::Cow;
use std::future::Future;
use std::rc::Rc;
use tracing::trace;

pub struct PreparedQuery {
    name: Option<String>,
    client_id: u64,
    parameter_description: protocol::ParameterDescription,
    result: Rc<PreparedQueryResult>,
}

impl PreparedQuery {
    pub(crate) fn new(
        name: Option<String>,
        client_id: u64,
        parameter_description: protocol::ParameterDescription,
        result: PreparedQueryResult,
    ) -> Self {
        PreparedQuery {
            name,
            client_id,
            parameter_description,
            result: Rc::new(result),
        }
    }

    /// Execute this prepared statement with parameters, returning a binary mode result
    pub async fn execute<'postgres_client, C: ElefantAsyncReadWrite>(
        &self,
        client: &'postgres_client mut PostgresClient<C>,
        parameters: &[&dyn ToSql],
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

        let msg = client.read_next_backend_message().await?;

        match msg {
            BackendMessage::BindComplete => {
                trace!("Bind complete");
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

        Ok(QueryResult::new(client, Some(self.result.clone())))
    }
}

trait Sealed {}

#[allow(private_bounds)]
pub trait Statement: Sealed {
    fn prepare<C: ElefantAsyncReadWrite>(
        &self,
        client: &mut PostgresClient<C>,
    ) -> impl Future<Output = Result<PreparedQuery, ElefantClientError>>;
}

impl Sealed for PreparedQuery {}

impl Statement for PreparedQuery {
    async fn prepare<C: ElefantAsyncReadWrite>(
        &self,
        _client: &mut PostgresClient<C>,
    ) -> Result<PreparedQuery, ElefantClientError> {
        // PreparedQuery is already prepared, so just clone it
        Ok(PreparedQuery {
            name: self.name.clone(),
            client_id: self.client_id,
            parameter_description: self.parameter_description.clone(),
            result: self.result.clone(),
        })
    }
}

impl Sealed for str {}

impl Statement for str {
    async fn prepare<C: ElefantAsyncReadWrite>(
        &self,
        client: &mut PostgresClient<C>,
    ) -> Result<PreparedQuery, ElefantClientError> {
        client.prepare_with_name(self, None).await
    }
}

impl Sealed for String {}

impl Statement for String {
    async fn prepare<C: ElefantAsyncReadWrite>(
        &self,
        client: &mut PostgresClient<C>,
    ) -> Result<PreparedQuery, ElefantClientError> {
        self.as_str().prepare(client).await
    }
}

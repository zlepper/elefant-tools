use super::messages::Lsn;
use super::stream::ReplicationStream;
use crate::pool::ConnectionFactory;
use crate::postgres_client::PostgresClient;
use crate::protocol::FrontendMessage;
use crate::ElefantClientError;
use std::borrow::Cow;

/// Quotes an identifier for use in replication protocol commands using
/// PostgreSQL's standard double-quoting rules: wrap in double quotes and
/// escape any internal double quotes by doubling them.
fn quote_identifier(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

impl<F: ConnectionFactory> PostgresClient<F> {
    pub async fn create_replication_slot(
        &mut self,
        slot_name: &str,
        output_plugin: &str,
    ) -> Result<(String, Lsn), ElefantClientError> {
        let slot_quoted = quote_identifier(slot_name);
        let plugin_quoted = quote_identifier(output_plugin);
        let query = format!("CREATE_REPLICATION_SLOT {slot_quoted} LOGICAL {plugin_quoted}");
        let mut result = self.query_simple(&query).await?;
        let mut slot = String::new();
        let mut lsn = Lsn(0);

        loop {
            match result.next_result_set().await? {
                crate::postgres_client::QueryResultSet::QueryProcessingComplete => break,
                crate::postgres_client::QueryResultSet::RowDescriptionReceived(mut reader) => {
                    if let Some(row) = reader.next_row().await? {
                        slot = row.get_text::<String>(0)?;
                        let lsn_str: String = row.get_text(1)?;
                        lsn = Lsn::from_pg_string(&lsn_str)?;
                    }
                }
            }
        }

        Ok((slot, lsn))
    }

    pub async fn drop_replication_slot(
        &mut self,
        slot_name: &str,
    ) -> Result<(), ElefantClientError> {
        let slot_quoted = quote_identifier(slot_name);
        let query = format!("DROP_REPLICATION_SLOT {slot_quoted}");
        self.execute_non_query_simple(&query).await
    }

    pub async fn start_replication(
        &mut self,
        slot_name: &str,
        lsn: Lsn,
        options: &str,
    ) -> Result<ReplicationStream<'_, F>, ElefantClientError> {
        let slot_quoted = quote_identifier(slot_name);
        let query = format!(
            "START_REPLICATION SLOT {slot_quoted} LOGICAL {lsn} ({options})"
        );

        self.start_new_query().await?;
        self.connection
            .write_frontend_message(&FrontendMessage::Query(crate::protocol::Query {
                query: Cow::Borrowed(&query),
            }))
            .await?;
        self.connection.flush().await?;

        let msg = self.read_next_backend_message().await?;
        match msg {
            crate::protocol::BackendMessage::CopyBothResponse(_) => Ok(ReplicationStream::new(self)),
            _ => Err(ElefantClientError::UnexpectedBackendMessage(format!(
                "Expected CopyBothResponse, got {msg:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_identifier_simple_name() {
        assert_eq!(quote_identifier("my_slot"), "\"my_slot\"");
    }

    #[test]
    fn quote_identifier_escapes_double_quotes() {
        assert_eq!(quote_identifier(r#"my"slot"#), r#""my""slot""#);
    }

    #[test]
    fn quote_identifier_handles_spaces_and_special_chars() {
        assert_eq!(
            quote_identifier("my replication slot"),
            "\"my replication slot\""
        );
        assert_eq!(quote_identifier("UPPER_CASE"), "\"UPPER_CASE\"");
    }

    #[test]
    fn quote_identifier_handles_empty_string() {
        assert_eq!(quote_identifier(""), "\"\"");
    }
}

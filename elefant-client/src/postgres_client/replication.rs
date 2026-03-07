use crate::pool::ConnectionFactory;
use crate::postgres_client::PostgresClient;
use crate::protocol::frame_reader::{ByteSliceError, ByteSliceReader, ByteSliceWriter};
use crate::protocol::{BackendMessage, CopyData, FrontendMessage};
use crate::{reborrow_until_polonius, ElefantClientError};
use std::borrow::Cow;
use std::fmt;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum ReplicationError {
    TruncatedMessage,
    UnknownReplicationMessageType(u8),
    UnknownPgOutputMessageType(u8),
    UnknownTupleColumnType(u8),
    UnknownUpdateMarker(u8),
}

impl From<ByteSliceError> for ReplicationError {
    fn from(_: ByteSliceError) -> Self {
        ReplicationError::TruncatedMessage
    }
}

impl From<ReplicationError> for ElefantClientError {
    fn from(e: ReplicationError) -> Self {
        ElefantClientError::PostgresError(format!("{e:?}"))
    }
}

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

/// PostgreSQL Log Sequence Number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Lsn(pub u64);

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

impl Lsn {
    pub fn from_pg_string(s: &str) -> Result<Self, ElefantClientError> {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() != 2 {
            return Err(ElefantClientError::PostgresError(format!(
                "Invalid LSN format: {s}"
            )));
        }
        let high = u64::from_str_radix(parts[0], 16).map_err(|e| {
            ElefantClientError::PostgresError(format!("Invalid LSN high part: {e}"))
        })?;
        let low = u64::from_str_radix(parts[1], 16).map_err(|e| {
            ElefantClientError::PostgresError(format!("Invalid LSN low part: {e}"))
        })?;
        Ok(Lsn((high << 32) | low))
    }
}

// ---------------------------------------------------------------------------
// Replication streaming protocol messages (CopyData wrapper layer)
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum ReplicationMessage<'a> {
    XLogData(XLogData<'a>),
    PrimaryKeepalive(PrimaryKeepalive),
}

#[derive(Debug)]
pub struct XLogData<'a> {
    pub start_lsn: Lsn,
    pub end_lsn: Lsn,
    pub server_time: i64,
    pub data: &'a [u8],
}

#[derive(Debug)]
pub struct PrimaryKeepalive {
    pub end_lsn: Lsn,
    pub server_time: i64,
    pub reply_requested: bool,
}

// ---------------------------------------------------------------------------
// pgoutput logical replication messages
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum PgOutputMessage<'a> {
    Begin(BeginMessage),
    Commit(CommitMessage),
    Relation(RelationMessage<'a>),
    Insert(InsertMessage<'a>),
    Update(UpdateMessage<'a>),
    Delete(DeleteMessage<'a>),
    Truncate(TruncateMessage),
}

#[derive(Debug)]
pub struct BeginMessage {
    pub final_lsn: Lsn,
    pub commit_timestamp: i64,
    pub xid: u32,
}

#[derive(Debug)]
pub struct CommitMessage {
    pub flags: u8,
    pub commit_lsn: Lsn,
    pub end_lsn: Lsn,
    pub commit_timestamp: i64,
}

#[derive(Debug)]
pub struct RelationMessage<'a> {
    pub relation_id: u32,
    pub namespace: Cow<'a, str>,
    pub name: Cow<'a, str>,
    pub replica_identity: u8,
    pub columns: Vec<RelationColumn<'a>>,
}

#[derive(Debug)]
pub struct RelationColumn<'a> {
    pub flags: u8,
    pub name: Cow<'a, str>,
    pub type_oid: u32,
    pub type_modifier: i32,
}

#[derive(Debug)]
pub struct InsertMessage<'a> {
    pub relation_id: u32,
    pub tuple: TupleData<'a>,
}

#[derive(Debug)]
pub struct UpdateMessage<'a> {
    pub relation_id: u32,
    pub old_tuple: Option<TupleData<'a>>,
    pub new_tuple: TupleData<'a>,
}

#[derive(Debug)]
pub struct DeleteMessage<'a> {
    pub relation_id: u32,
    pub old_tuple: TupleData<'a>,
}

#[derive(Debug)]
pub struct TruncateMessage {
    pub option_bits: u8,
    pub relation_ids: Vec<u32>,
}

#[derive(Debug)]
pub struct TupleData<'a> {
    pub columns: Vec<TupleColumn<'a>>,
}

#[derive(Debug)]
pub enum TupleColumn<'a> {
    Null,
    Unchanged,
    Text(Cow<'a, str>),
    Binary(&'a [u8]),
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

pub fn parse_replication_message(data: &[u8]) -> Result<ReplicationMessage<'_>, ReplicationError> {
    let mut reader = ByteSliceReader::new(data);
    let msg_type = reader.read_u8()?;

    match msg_type {
        b'w' => {
            let start_lsn = Lsn(reader.read_u64()?);
            let end_lsn = Lsn(reader.read_u64()?);
            let server_time = reader.read_i64()?;
            let remaining = reader.read_bytes(data.len() - reader.get_read_bytes())?;
            Ok(ReplicationMessage::XLogData(XLogData {
                start_lsn,
                end_lsn,
                server_time,
                data: remaining,
            }))
        }
        b'k' => {
            let end_lsn = Lsn(reader.read_u64()?);
            let server_time = reader.read_i64()?;
            let reply = reader.read_u8()?;
            Ok(ReplicationMessage::PrimaryKeepalive(PrimaryKeepalive {
                end_lsn,
                server_time,
                reply_requested: reply != 0,
            }))
        }
        _ => Err(ReplicationError::UnknownReplicationMessageType(msg_type)),
    }
}

fn parse_tuple_data<'a>(reader: &mut ByteSliceReader<'a>) -> Result<TupleData<'a>, ReplicationError> {
    let col_count = reader.read_i16()? as usize;
    let mut columns = Vec::with_capacity(col_count);

    for _ in 0..col_count {
        let col_type = reader.read_u8()?;
        match col_type {
            b'n' => columns.push(TupleColumn::Null),
            b'u' => columns.push(TupleColumn::Unchanged),
            b't' => {
                let len = reader.read_i32()? as usize;
                let bytes = reader.read_bytes(len)?;
                let text = String::from_utf8_lossy(bytes);
                columns.push(TupleColumn::Text(text));
            }
            b'b' => {
                let len = reader.read_i32()? as usize;
                let bytes = reader.read_bytes(len)?;
                columns.push(TupleColumn::Binary(bytes));
            }
            _ => return Err(ReplicationError::UnknownTupleColumnType(col_type)),
        }
    }

    Ok(TupleData { columns })
}

pub fn parse_pgoutput_message(data: &[u8]) -> Result<PgOutputMessage<'_>, ReplicationError> {
    let mut reader = ByteSliceReader::new(data);
    let msg_type = reader.read_u8()?;

    match msg_type {
        b'B' => {
            let final_lsn = Lsn(reader.read_u64()?);
            let commit_timestamp = reader.read_i64()?;
            let xid = reader.read_i32()? as u32;
            Ok(PgOutputMessage::Begin(BeginMessage {
                final_lsn,
                commit_timestamp,
                xid,
            }))
        }
        b'C' => {
            let flags = reader.read_u8()?;
            let commit_lsn = Lsn(reader.read_u64()?);
            let end_lsn = Lsn(reader.read_u64()?);
            let commit_timestamp = reader.read_i64()?;
            Ok(PgOutputMessage::Commit(CommitMessage {
                flags,
                commit_lsn,
                end_lsn,
                commit_timestamp,
            }))
        }
        b'R' => {
            let relation_id = reader.read_i32()? as u32;
            let namespace = reader.read_null_terminated_string()?;
            let name = reader.read_null_terminated_string()?;
            let replica_identity = reader.read_u8()?;
            let col_count = reader.read_i16()? as usize;

            let mut columns = Vec::with_capacity(col_count);
            for _ in 0..col_count {
                let flags = reader.read_u8()?;
                let col_name = reader.read_null_terminated_string()?;
                let type_oid = reader.read_i32()? as u32;
                let type_modifier = reader.read_i32()?;
                columns.push(RelationColumn {
                    flags,
                    name: col_name,
                    type_oid,
                    type_modifier,
                });
            }

            Ok(PgOutputMessage::Relation(RelationMessage {
                relation_id,
                namespace,
                name,
                replica_identity,
                columns,
            }))
        }
        b'I' => {
            let relation_id = reader.read_i32()? as u32;
            let _new_marker = reader.read_u8()?; // 'N'
            let tuple = parse_tuple_data(&mut reader)?;
            Ok(PgOutputMessage::Insert(InsertMessage {
                relation_id,
                tuple,
            }))
        }
        b'U' => {
            let relation_id = reader.read_i32()? as u32;
            let marker = reader.read_u8()?;

            let (old_tuple, new_tuple) = match marker {
                b'K' | b'O' => {
                    let old = parse_tuple_data(&mut reader)?;
                    let _new_marker = reader.read_u8()?; // 'N'
                    let new = parse_tuple_data(&mut reader)?;
                    (Some(old), new)
                }
                b'N' => {
                    let new = parse_tuple_data(&mut reader)?;
                    (None, new)
                }
                _ => return Err(ReplicationError::UnknownUpdateMarker(marker)),
            };

            Ok(PgOutputMessage::Update(UpdateMessage {
                relation_id,
                old_tuple,
                new_tuple,
            }))
        }
        b'D' => {
            let relation_id = reader.read_i32()? as u32;
            let _marker = reader.read_u8()?; // 'K' or 'O'
            let old_tuple = parse_tuple_data(&mut reader)?;
            Ok(PgOutputMessage::Delete(DeleteMessage {
                relation_id,
                old_tuple,
            }))
        }
        b'T' => {
            let num_relations = reader.read_i32()? as usize;
            let option_bits = reader.read_u8()?;
            let mut relation_ids = Vec::with_capacity(num_relations);
            for _ in 0..num_relations {
                let id = reader.read_i32()? as u32;
                relation_ids.push(id);
            }
            Ok(PgOutputMessage::Truncate(TruncateMessage {
                option_bits,
                relation_ids,
            }))
        }
        _ => Err(ReplicationError::UnknownPgOutputMessageType(msg_type)),
    }
}

// ---------------------------------------------------------------------------
// ReplicationStream
// ---------------------------------------------------------------------------

pub struct ReplicationStream<'a, F: ConnectionFactory> {
    client: &'a mut PostgresClient<F>,
    status_buf: Vec<u8>,
}

impl<'a, F: ConnectionFactory> ReplicationStream<'a, F> {
    fn new(client: &'a mut PostgresClient<F>) -> Self {
        Self {
            client,
            status_buf: Vec::with_capacity(34),
        }
    }

    pub async fn next_message(&mut self) -> Result<ReplicationMessage<'_>, ElefantClientError> {
        let client: &mut PostgresClient<F> = reborrow_until_polonius!(&mut *self.client);
        let msg = client.read_next_backend_message().await?;
        match msg {
            BackendMessage::CopyData(cd) => Ok(parse_replication_message(cd.data)?),
            _ => Err(ElefantClientError::UnexpectedBackendMessage(format!(
                "Expected CopyData during replication, got {msg:?}"
            ))),
        }
    }

    pub async fn send_status_update(
        &mut self,
        write_lsn: Lsn,
        flush_lsn: Lsn,
        apply_lsn: Lsn,
    ) -> Result<(), ElefantClientError> {
        // Timestamp: microseconds since PostgreSQL epoch (2000-01-01)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        let pg_epoch_offset_us = 946_684_800i64 * 1_000_000;
        let pg_timestamp = (now.as_micros() as i64) - pg_epoch_offset_us;

        self.status_buf.clear();
        let mut writer = ByteSliceWriter::new(&mut self.status_buf);
        writer.write_u8(b'r');
        writer.write_u64(write_lsn.0);
        writer.write_u64(flush_lsn.0);
        writer.write_u64(apply_lsn.0);
        writer.write_i64(pg_timestamp);
        writer.write_u8(0); // no reply requested

        self.client
            .connection
            .write_frontend_message(&FrontendMessage::CopyData(CopyData { data: &self.status_buf }))
            .await?;
        self.client.connection.flush().await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Client methods for replication slot management
// ---------------------------------------------------------------------------

impl<F: ConnectionFactory> PostgresClient<F> {
    pub async fn create_replication_slot(
        &mut self,
        slot_name: &str,
        output_plugin: &str,
    ) -> Result<(String, Lsn), ElefantClientError> {
        let query = format!("CREATE_REPLICATION_SLOT {slot_name} LOGICAL {output_plugin}");
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
        let query = format!("DROP_REPLICATION_SLOT {slot_name}");
        self.execute_non_query_simple(&query).await
    }

    pub async fn start_replication(
        &mut self,
        slot_name: &str,
        lsn: Lsn,
        options: &str,
    ) -> Result<ReplicationStream<'_, F>, ElefantClientError> {
        let query = format!(
            "START_REPLICATION SLOT {slot_name} LOGICAL {lsn} ({options})"
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
            BackendMessage::CopyBothResponse(_) => Ok(ReplicationStream::new(self)),
            _ => Err(ElefantClientError::UnexpectedBackendMessage(format!(
                "Expected CopyBothResponse, got {msg:?}"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use super::*;
    use crate::tokio_connection::new_client;
    use crate::PostgresConnectionSettings;

    fn regular_settings() -> PostgresConnectionSettings {
        PostgresConnectionSettings::new("localhost")
            .port(5415)
            .password("passw0rd")
    }

    fn replication_settings() -> PostgresConnectionSettings {
        PostgresConnectionSettings::new("localhost")
            .port(5415)
            .password("passw0rd")
            .replication("database")
    }

    /// Create a logical replication slot on a regular (non-replication) connection
    /// using the SQL function. This avoids the slot being held by a walsender.
    async fn create_slot_on_regular<F: ConnectionFactory>(
        client: &mut PostgresClient<F>,
        slot_name: &str,
    ) -> Lsn {
        let lsn_str: String = client
            .try_read_single_value_simple(&format!(
                "SELECT lsn::text FROM pg_create_logical_replication_slot('{slot_name}', 'pgoutput')"
            ))
            .await
            .unwrap();
        Lsn::from_pg_string(&lsn_str).unwrap()
    }

    /// Drop a replication slot on a regular connection (best-effort).
    async fn drop_slot_on_regular<F: ConnectionFactory>(
        client: &mut PostgresClient<F>,
        slot_name: &str,
    ) {
        let _ = client
            .execute_non_query_simple(&format!(
                "SELECT pg_drop_replication_slot('{slot_name}')"
            ))
            .await;
    }

    #[tokio::test]
    async fn basic_logical_replication() {
        // -- Setup: regular connection for DDL/DML --
        let mut regular = new_client(regular_settings()).await.unwrap();
        regular
            .execute_non_query_simple(
                "DROP PUBLICATION IF EXISTS test_pub_basic; \
                 DROP TABLE IF EXISTS repl_test_basic; \
                 CREATE TABLE repl_test_basic(id int PRIMARY KEY, value text); \
                 CREATE PUBLICATION test_pub_basic FOR TABLE repl_test_basic;",
            )
            .await
            .unwrap();

        // Create slot on the regular connection so it's not held by a walsender
        let consistent_lsn = create_slot_on_regular(&mut regular, "test_slot_basic").await;

        // Insert data
        regular
            .execute_non_query_simple(
                "INSERT INTO repl_test_basic VALUES (1, 'hello'), (2, 'world');",
            )
            .await
            .unwrap();

        // -- Replication connection: start streaming --
        let mut repl = new_client(replication_settings()).await.unwrap();
        let mut stream = repl
            .start_replication(
                "test_slot_basic",
                consistent_lsn,
                "proto_version '1', publication_names 'test_pub_basic'",
            )
            .await
            .unwrap();

        // Collect messages with a timeout
        let mut saw_begin = false;
        let mut saw_commit = false;
        let mut relation_name = String::new();
        let mut relation_col_count = 0usize;
        let mut insert_values: Vec<(String, String)> = Vec::new();
        let mut last_lsn = consistent_lsn;

        let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let msg = stream.next_message().await.unwrap();
                match msg {
                    ReplicationMessage::XLogData(xlog) => {
                        last_lsn = xlog.end_lsn;
                        let pgmsg = parse_pgoutput_message(xlog.data).unwrap();
                        match pgmsg {
                            PgOutputMessage::Begin(_) => saw_begin = true,
                            PgOutputMessage::Relation(rel) => {
                                relation_name = rel.name.into_owned();
                                relation_col_count = rel.columns.len();
                            }
                            PgOutputMessage::Insert(ins) => {
                                let id = match &ins.tuple.columns[0] {
                                    TupleColumn::Text(t) => t.to_string(),
                                    _ => panic!("Expected text column for id"),
                                };
                                let val = match &ins.tuple.columns[1] {
                                    TupleColumn::Text(t) => t.to_string(),
                                    _ => panic!("Expected text column for value"),
                                };
                                insert_values.push((id, val));
                            }
                            PgOutputMessage::Commit(_) => {
                                saw_commit = true;
                                break;
                            }
                            _ => {}
                        }
                    }
                    ReplicationMessage::PrimaryKeepalive(ka) => {
                        if ka.reply_requested {
                            last_lsn = ka.end_lsn;
                        }
                    }
                }
            }
        })
        .await;
        assert!(result.is_ok(), "Timed out waiting for replication messages");

        // Send status update
        stream
            .send_status_update(last_lsn, last_lsn, last_lsn)
            .await
            .unwrap();

        // Validate
        assert!(saw_begin, "Should have seen BEGIN");
        assert!(saw_commit, "Should have seen COMMIT");
        assert_eq!(relation_name, "repl_test_basic");
        assert_eq!(relation_col_count, 2);
        assert_eq!(
            insert_values,
            vec![
                ("1".to_string(), "hello".to_string()),
                ("2".to_string(), "world".to_string()),
            ]
        );

        // Cleanup
        drop(stream);
        drop(repl);
        drop_slot_on_regular(&mut regular, "test_slot_basic").await;
        regular
            .execute_non_query_simple(
                "DROP PUBLICATION IF EXISTS test_pub_basic; \
                 DROP TABLE IF EXISTS repl_test_basic;",
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn replication_handles_column_addition() {
        let mut regular = new_client(regular_settings()).await.unwrap();
        regular
            .execute_non_query_simple(
                "DROP PUBLICATION IF EXISTS test_pub_addcol; \
                 DROP TABLE IF EXISTS repl_test_addcol; \
                 CREATE TABLE repl_test_addcol(id int PRIMARY KEY, value text); \
                 CREATE PUBLICATION test_pub_addcol FOR TABLE repl_test_addcol;",
            )
            .await
            .unwrap();

        let consistent_lsn = create_slot_on_regular(&mut regular, "test_slot_addcol").await;

        // Insert before schema change
        regular
            .execute_non_query_simple("INSERT INTO repl_test_addcol VALUES (1, 'before');")
            .await
            .unwrap();

        // Schema change: add column
        regular
            .execute_non_query_simple(
                "ALTER TABLE repl_test_addcol ADD COLUMN extra text DEFAULT 'def';",
            )
            .await
            .unwrap();

        // Insert after schema change
        regular
            .execute_non_query_simple("INSERT INTO repl_test_addcol VALUES (2, 'after', 'extra_val');")
            .await
            .unwrap();

        let mut repl = new_client(replication_settings()).await.unwrap();
        let mut stream = repl
            .start_replication(
                "test_slot_addcol",
                consistent_lsn,
                "proto_version '1', publication_names 'test_pub_addcol'",
            )
            .await
            .unwrap();

        let mut relation_versions: Vec<(String, usize)> = Vec::new(); // (name, col_count)
        let mut insert_col_counts: Vec<usize> = Vec::new();
        let mut commits_seen = 0u32;
        let mut last_lsn = consistent_lsn;

        let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let msg = stream.next_message().await.unwrap();
                match msg {
                    ReplicationMessage::XLogData(xlog) => {
                        last_lsn = xlog.end_lsn;
                        let pgmsg = parse_pgoutput_message(xlog.data).unwrap();
                        match pgmsg {
                            PgOutputMessage::Relation(rel) => {
                                relation_versions
                                    .push((rel.name.into_owned(), rel.columns.len()));
                            }
                            PgOutputMessage::Insert(ins) => {
                                insert_col_counts.push(ins.tuple.columns.len());
                            }
                            PgOutputMessage::Commit(_) => {
                                commits_seen += 1;
                                if commits_seen >= 2 {
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                    ReplicationMessage::PrimaryKeepalive(ka) => {
                        if ka.reply_requested {
                            last_lsn = ka.end_lsn;
                        }
                    }
                }
            }
        })
        .await;
        assert!(result.is_ok(), "Timed out waiting for replication messages");

        stream
            .send_status_update(last_lsn, last_lsn, last_lsn)
            .await
            .unwrap();

        // Validate: should see two Relation messages - first with 2 columns, then 3
        assert!(
            relation_versions.len() >= 2,
            "Expected at least 2 Relation messages, got {relation_versions:?}"
        );
        assert_eq!(relation_versions[0].1, 2, "First relation should have 2 columns");
        assert_eq!(
            relation_versions.last().unwrap().1,
            3,
            "Last relation should have 3 columns after ALTER TABLE ADD COLUMN"
        );

        // First insert has 2 columns, second has 3
        assert_eq!(insert_col_counts.len(), 2);
        assert_eq!(insert_col_counts[0], 2);
        assert_eq!(insert_col_counts[1], 3);

        // Cleanup
        drop(stream);
        drop(repl);
        drop_slot_on_regular(&mut regular, "test_slot_addcol").await;
        regular
            .execute_non_query_simple(
                "DROP PUBLICATION IF EXISTS test_pub_addcol; \
                 DROP TABLE IF EXISTS repl_test_addcol;",
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn replication_handles_column_removal() {
        let mut regular = new_client(regular_settings()).await.unwrap();
        regular
            .execute_non_query_simple(
                "DROP PUBLICATION IF EXISTS test_pub_dropcol; \
                 DROP TABLE IF EXISTS repl_test_dropcol; \
                 CREATE TABLE repl_test_dropcol(id int PRIMARY KEY, old_col text, value text); \
                 CREATE PUBLICATION test_pub_dropcol FOR TABLE repl_test_dropcol;",
            )
            .await
            .unwrap();

        let consistent_lsn = create_slot_on_regular(&mut regular, "test_slot_dropcol").await;

        // Insert before schema change
        regular
            .execute_non_query_simple(
                "INSERT INTO repl_test_dropcol VALUES (1, 'old_data', 'value1');",
            )
            .await
            .unwrap();

        // Schema change: drop column
        regular
            .execute_non_query_simple("ALTER TABLE repl_test_dropcol DROP COLUMN old_col;")
            .await
            .unwrap();

        // Insert after schema change
        regular
            .execute_non_query_simple("INSERT INTO repl_test_dropcol VALUES (2, 'value2');")
            .await
            .unwrap();

        let mut repl = new_client(replication_settings()).await.unwrap();
        let mut stream = repl
            .start_replication(
                "test_slot_dropcol",
                consistent_lsn,
                "proto_version '1', publication_names 'test_pub_dropcol'",
            )
            .await
            .unwrap();

        let mut relation_col_counts: Vec<usize> = Vec::new();
        let mut relation_col_names: Vec<Vec<String>> = Vec::new();
        let mut insert_col_counts: Vec<usize> = Vec::new();
        let mut commits_seen = 0u32;
        let mut last_lsn = consistent_lsn;

        let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let msg = stream.next_message().await.unwrap();
                match msg {
                    ReplicationMessage::XLogData(xlog) => {
                        last_lsn = xlog.end_lsn;
                        let pgmsg = parse_pgoutput_message(xlog.data).unwrap();
                        match pgmsg {
                            PgOutputMessage::Relation(rel) => {
                                relation_col_counts.push(rel.columns.len());
                                relation_col_names.push(
                                    rel.columns
                                        .iter()
                                        .map(|c| c.name.to_string())
                                        .collect(),
                                );
                            }
                            PgOutputMessage::Insert(ins) => {
                                insert_col_counts.push(ins.tuple.columns.len());
                            }
                            PgOutputMessage::Commit(_) => {
                                commits_seen += 1;
                                if commits_seen >= 2 {
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                    ReplicationMessage::PrimaryKeepalive(ka) => {
                        if ka.reply_requested {
                            last_lsn = ka.end_lsn;
                        }
                    }
                }
            }
        })
        .await;
        assert!(result.is_ok(), "Timed out waiting for replication messages");

        stream
            .send_status_update(last_lsn, last_lsn, last_lsn)
            .await
            .unwrap();

        // Validate: first relation has 3 columns (id, old_col, value), second has 2 (id, value)
        assert!(
            relation_col_counts.len() >= 2,
            "Expected at least 2 Relation messages, got {relation_col_counts:?}"
        );
        assert_eq!(relation_col_counts[0], 3, "First relation should have 3 columns");
        assert_eq!(
            relation_col_counts.last().copied().unwrap(),
            2,
            "Last relation should have 2 columns after DROP COLUMN"
        );

        // Verify column names changed
        assert_eq!(
            relation_col_names[0],
            vec!["id", "old_col", "value"],
            "First relation should have id, old_col, value"
        );
        assert_eq!(
            relation_col_names.last().unwrap().as_slice(),
            &["id", "value"],
            "Last relation should have only id, value"
        );

        // First insert has 3 columns, second has 2
        assert_eq!(insert_col_counts.len(), 2);
        assert_eq!(insert_col_counts[0], 3);
        assert_eq!(insert_col_counts[1], 2);

        // Cleanup
        drop(stream);
        drop(repl);
        drop_slot_on_regular(&mut regular, "test_slot_dropcol").await;
        regular
            .execute_non_query_simple(
                "DROP PUBLICATION IF EXISTS test_pub_dropcol; \
                 DROP TABLE IF EXISTS repl_test_dropcol;",
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn basic_logical_replication_binary() {
        let mut regular = new_client(regular_settings()).await.unwrap();
        regular
            .execute_non_query_simple(
                "DROP PUBLICATION IF EXISTS test_pub_bin; \
                 DROP TABLE IF EXISTS repl_test_bin; \
                 CREATE TABLE repl_test_bin(id int PRIMARY KEY, value text, flag bool); \
                 CREATE PUBLICATION test_pub_bin FOR TABLE repl_test_bin;",
            )
            .await
            .unwrap();

        let consistent_lsn = create_slot_on_regular(&mut regular, "test_slot_bin").await;

        regular
            .execute_non_query_simple(
                "INSERT INTO repl_test_bin VALUES (1, 'hello', true), (2, 'world', false);",
            )
            .await
            .unwrap();

        let mut repl = new_client(replication_settings()).await.unwrap();
        let mut stream = repl
            .start_replication(
                "test_slot_bin",
                consistent_lsn,
                "proto_version '2', binary 'true', publication_names 'test_pub_bin'",
            )
            .await
            .unwrap();

        let mut saw_begin = false;
        let mut saw_commit = false;
        let mut relation_name = String::new();
        let mut insert_values: Vec<(i32, String, bool)> = Vec::new();
        let mut last_lsn = consistent_lsn;

        let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let msg = stream.next_message().await.unwrap();
                match msg {
                    ReplicationMessage::XLogData(xlog) => {
                        last_lsn = xlog.end_lsn;
                        let pgmsg = parse_pgoutput_message(xlog.data).unwrap();
                        match pgmsg {
                            PgOutputMessage::Begin(_) => saw_begin = true,
                            PgOutputMessage::Relation(rel) => {
                                relation_name = rel.name.into_owned();
                            }
                            PgOutputMessage::Insert(ins) => {
                                let id = match &ins.tuple.columns[0] {
                                    TupleColumn::Binary(b) => {
                                        i32::from_be_bytes((*b).try_into().unwrap())
                                    }
                                    other => panic!("Expected Binary column for id, got {other:?}"),
                                };
                                let value = match &ins.tuple.columns[1] {
                                    TupleColumn::Binary(b) => {
                                        std::str::from_utf8(b).unwrap().to_string()
                                    }
                                    other => panic!("Expected Binary column for value, got {other:?}"),
                                };
                                let flag = match &ins.tuple.columns[2] {
                                    TupleColumn::Binary(b) => b[0] != 0,
                                    other => panic!("Expected Binary column for flag, got {other:?}"),
                                };
                                insert_values.push((id, value, flag));
                            }
                            PgOutputMessage::Commit(_) => {
                                saw_commit = true;
                                break;
                            }
                            _ => {}
                        }
                    }
                    ReplicationMessage::PrimaryKeepalive(ka) => {
                        if ka.reply_requested {
                            last_lsn = ka.end_lsn;
                        }
                    }
                }
            }
        })
        .await;
        assert!(result.is_ok(), "Timed out waiting for replication messages");

        stream
            .send_status_update(last_lsn, last_lsn, last_lsn)
            .await
            .unwrap();

        assert!(saw_begin);
        assert!(saw_commit);
        assert_eq!(relation_name, "repl_test_bin");
        assert_eq!(
            insert_values,
            vec![
                (1, "hello".to_string(), true),
                (2, "world".to_string(), false),
            ]
        );

        drop(stream);
        drop(repl);
        drop_slot_on_regular(&mut regular, "test_slot_bin").await;
        regular
            .execute_non_query_simple(
                "DROP PUBLICATION IF EXISTS test_pub_bin; \
                 DROP TABLE IF EXISTS repl_test_bin;",
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn replication_binary_handles_column_addition() {
        let mut regular = new_client(regular_settings()).await.unwrap();
        regular
            .execute_non_query_simple(
                "DROP PUBLICATION IF EXISTS test_pub_binadd; \
                 DROP TABLE IF EXISTS repl_test_binadd; \
                 CREATE TABLE repl_test_binadd(id int PRIMARY KEY, value text); \
                 CREATE PUBLICATION test_pub_binadd FOR TABLE repl_test_binadd;",
            )
            .await
            .unwrap();

        let consistent_lsn = create_slot_on_regular(&mut regular, "test_slot_binadd").await;

        regular
            .execute_non_query_simple("INSERT INTO repl_test_binadd VALUES (1, 'before');")
            .await
            .unwrap();

        regular
            .execute_non_query_simple(
                "ALTER TABLE repl_test_binadd ADD COLUMN extra int DEFAULT 42;",
            )
            .await
            .unwrap();

        regular
            .execute_non_query_simple("INSERT INTO repl_test_binadd VALUES (2, 'after', 99);")
            .await
            .unwrap();

        let mut repl = new_client(replication_settings()).await.unwrap();
        let mut stream = repl
            .start_replication(
                "test_slot_binadd",
                consistent_lsn,
                "proto_version '2', binary 'true', publication_names 'test_pub_binadd'",
            )
            .await
            .unwrap();

        let mut relation_col_counts: Vec<usize> = Vec::new();
        let mut insert_col_counts: Vec<usize> = Vec::new();
        let mut commits_seen = 0u32;
        let mut last_lsn = consistent_lsn;

        let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let msg = stream.next_message().await.unwrap();
                match msg {
                    ReplicationMessage::XLogData(xlog) => {
                        last_lsn = xlog.end_lsn;
                        let pgmsg = parse_pgoutput_message(xlog.data).unwrap();
                        match pgmsg {
                            PgOutputMessage::Relation(rel) => {
                                relation_col_counts.push(rel.columns.len());
                            }
                            PgOutputMessage::Insert(ins) => {
                                // Verify all columns are Binary variant
                                for col in &ins.tuple.columns {
                                    assert!(
                                        matches!(col, TupleColumn::Binary(_)),
                                        "Expected Binary column in binary mode, got {col:?}"
                                    );
                                }
                                insert_col_counts.push(ins.tuple.columns.len());
                            }
                            PgOutputMessage::Commit(_) => {
                                commits_seen += 1;
                                if commits_seen >= 2 {
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                    ReplicationMessage::PrimaryKeepalive(ka) => {
                        if ka.reply_requested {
                            last_lsn = ka.end_lsn;
                        }
                    }
                }
            }
        })
        .await;
        assert!(result.is_ok(), "Timed out waiting for replication messages");

        stream
            .send_status_update(last_lsn, last_lsn, last_lsn)
            .await
            .unwrap();

        assert!(relation_col_counts.len() >= 2);
        assert_eq!(relation_col_counts[0], 2);
        assert_eq!(relation_col_counts.last().copied().unwrap(), 3);

        assert_eq!(insert_col_counts.len(), 2);
        assert_eq!(insert_col_counts[0], 2);
        assert_eq!(insert_col_counts[1], 3);

        drop(stream);
        drop(repl);
        drop_slot_on_regular(&mut regular, "test_slot_binadd").await;
        regular
            .execute_non_query_simple(
                "DROP PUBLICATION IF EXISTS test_pub_binadd; \
                 DROP TABLE IF EXISTS repl_test_binadd;",
            )
            .await
            .unwrap();
    }
}

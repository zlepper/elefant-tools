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
    Origin(OriginMessage<'a>),
    Type(TypeMessage<'a>),
    LogicalDecodingMessage(LogicalDecodingMessage<'a>),
    StreamStart(StreamStartMessage),
    StreamStop,
    StreamCommit(StreamCommitMessage),
    StreamAbort(StreamAbortMessage),
    /// A pgoutput message type that is part of the protocol but not yet
    /// fully supported (e.g., two-phase commit messages from proto_version 3+).
    Unsupported { msg_type: u8, data: &'a [u8] },
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
pub struct OriginMessage<'a> {
    pub origin_lsn: Lsn,
    pub origin_name: Cow<'a, str>,
}

#[derive(Debug)]
pub struct TypeMessage<'a> {
    pub type_oid: u32,
    pub namespace: Cow<'a, str>,
    pub name: Cow<'a, str>,
}

#[derive(Debug)]
pub struct LogicalDecodingMessage<'a> {
    pub transactional: bool,
    pub lsn: Lsn,
    pub prefix: Cow<'a, str>,
    pub content: &'a [u8],
}

#[derive(Debug)]
pub struct StreamStartMessage {
    pub xid: u32,
    pub first_segment: bool,
}

#[derive(Debug)]
pub struct StreamCommitMessage {
    pub xid: u32,
    pub flags: u8,
    pub commit_lsn: Lsn,
    pub end_lsn: Lsn,
    pub commit_timestamp: i64,
}

#[derive(Debug)]
pub struct StreamAbortMessage {
    pub xid: u32,
    pub sub_xid: u32,
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
        b'O' => {
            let origin_lsn = Lsn(reader.read_u64()?);
            let origin_name = reader.read_null_terminated_string()?;
            Ok(PgOutputMessage::Origin(OriginMessage {
                origin_lsn,
                origin_name,
            }))
        }
        b'Y' => {
            let type_oid = reader.read_i32()? as u32;
            let namespace = reader.read_null_terminated_string()?;
            let name = reader.read_null_terminated_string()?;
            Ok(PgOutputMessage::Type(TypeMessage {
                type_oid,
                namespace,
                name,
            }))
        }
        b'M' => {
            let flags = reader.read_u8()?;
            let transactional = (flags & 1) != 0;
            let lsn = Lsn(reader.read_u64()?);
            let prefix = reader.read_null_terminated_string()?;
            let content_length = reader.read_i32()? as usize;
            let content = reader.read_bytes(content_length)?;
            Ok(PgOutputMessage::LogicalDecodingMessage(
                LogicalDecodingMessage {
                    transactional,
                    lsn,
                    prefix,
                    content,
                },
            ))
        }
        b'S' => {
            let xid = reader.read_i32()? as u32;
            let first_segment = reader.read_u8()? != 0;
            Ok(PgOutputMessage::StreamStart(StreamStartMessage {
                xid,
                first_segment,
            }))
        }
        b'E' => Ok(PgOutputMessage::StreamStop),
        b'c' => {
            let xid = reader.read_i32()? as u32;
            let flags = reader.read_u8()?;
            let commit_lsn = Lsn(reader.read_u64()?);
            let end_lsn = Lsn(reader.read_u64()?);
            let commit_timestamp = reader.read_i64()?;
            Ok(PgOutputMessage::StreamCommit(StreamCommitMessage {
                xid,
                flags,
                commit_lsn,
                end_lsn,
                commit_timestamp,
            }))
        }
        b'A' => {
            let xid = reader.read_i32()? as u32;
            let sub_xid = reader.read_i32()? as u32;
            Ok(PgOutputMessage::StreamAbort(StreamAbortMessage {
                xid,
                sub_xid,
            }))
        }
        _ => {
            let remaining = data.len() - reader.get_read_bytes();
            let payload = reader.read_bytes(remaining)?;
            Ok(PgOutputMessage::Unsupported {
                msg_type,
                data: payload,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// ReplicationStream
// ---------------------------------------------------------------------------

pub struct ReplicationStream<'a, F: ConnectionFactory> {
    client: &'a mut PostgresClient<F>,
    status_buf: Vec<u8>,
    last_received_lsn: Lsn,
}

/// Result of reading a single raw replication message, used internally
/// to separate parsing (which borrows the frame buffer) from actions
/// like sending keepalive replies (which need mutable access).
enum RawReadResult<'a> {
    Message(ReplicationMessage<'a>),
    KeepaliveReply(Lsn),
    EndOfStream,
}

impl<'a, F: ConnectionFactory> ReplicationStream<'a, F> {
    fn new(client: &'a mut PostgresClient<F>) -> Self {
        Self {
            client,
            status_buf: Vec::with_capacity(34),
            last_received_lsn: Lsn(0),
        }
    }

    /// Reads the next replication message from the stream.
    ///
    /// Returns `Ok(None)` when the server ends the replication stream
    /// (sends `CopyDone`). Automatically replies to keepalive messages
    /// that have `reply_requested` set, so callers never see those.
    pub async fn next_message(
        &mut self,
    ) -> Result<Option<ReplicationMessage<'_>>, ElefantClientError> {
        loop {
            let result = {
                let client: &mut PostgresClient<F> =
                    reborrow_until_polonius!(&mut *self.client);
                let msg = client.read_next_backend_message().await?;
                match msg {
                    BackendMessage::CopyData(cd) => {
                        let repl_msg = parse_replication_message(cd.data)?;
                        match &repl_msg {
                            ReplicationMessage::XLogData(xlog) => {
                                self.last_received_lsn = xlog.end_lsn;
                                RawReadResult::Message(repl_msg)
                            }
                            ReplicationMessage::PrimaryKeepalive(ka) => {
                                if ka.end_lsn > self.last_received_lsn {
                                    self.last_received_lsn = ka.end_lsn;
                                }
                                if ka.reply_requested {
                                    RawReadResult::KeepaliveReply(self.last_received_lsn)
                                } else {
                                    RawReadResult::Message(repl_msg)
                                }
                            }
                        }
                    }
                    BackendMessage::CopyDone => RawReadResult::EndOfStream,
                    _ => {
                        return Err(ElefantClientError::UnexpectedBackendMessage(format!(
                            "Expected CopyData or CopyDone during replication, got {msg:?}"
                        )));
                    }
                }
            };

            match result {
                RawReadResult::Message(msg) => return Ok(Some(msg)),
                RawReadResult::EndOfStream => return Ok(None),
                RawReadResult::KeepaliveReply(lsn) => {
                    self.send_status_update(lsn, lsn, Lsn(0)).await?;
                }
            }
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
            .unwrap_or(std::time::Duration::ZERO);
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
// Identifier quoting
// ---------------------------------------------------------------------------

/// Quotes an identifier for use in replication protocol commands using
/// PostgreSQL's standard double-quoting rules: wrap in double quotes and
/// escape any internal double quotes by doubling them.
fn quote_identifier(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
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

#[cfg(test)]
mod unit_tests {
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

    #[test]
    fn parse_origin_message() {
        let mut buf = vec![b'O'];
        buf.extend_from_slice(&42u64.to_be_bytes());
        buf.extend_from_slice(b"origin_name\0");
        let msg = parse_pgoutput_message(&buf).unwrap();
        match msg {
            PgOutputMessage::Origin(o) => {
                assert_eq!(o.origin_lsn, Lsn(42));
                assert_eq!(o.origin_name.as_ref(), "origin_name");
            }
            _ => panic!("Expected Origin, got {msg:?}"),
        }
    }

    #[test]
    fn parse_type_message() {
        let mut buf = vec![b'Y'];
        buf.extend_from_slice(&100i32.to_be_bytes());
        buf.extend_from_slice(b"public\0");
        buf.extend_from_slice(b"my_type\0");
        let msg = parse_pgoutput_message(&buf).unwrap();
        match msg {
            PgOutputMessage::Type(t) => {
                assert_eq!(t.type_oid, 100);
                assert_eq!(t.namespace.as_ref(), "public");
                assert_eq!(t.name.as_ref(), "my_type");
            }
            _ => panic!("Expected Type, got {msg:?}"),
        }
    }

    #[test]
    fn parse_logical_decoding_message() {
        let mut buf = vec![b'M'];
        buf.push(1); // transactional
        buf.extend_from_slice(&99u64.to_be_bytes());
        buf.extend_from_slice(b"my_prefix\0");
        let content = b"hello world";
        buf.extend_from_slice(&(content.len() as i32).to_be_bytes());
        buf.extend_from_slice(content);
        let msg = parse_pgoutput_message(&buf).unwrap();
        match msg {
            PgOutputMessage::LogicalDecodingMessage(m) => {
                assert!(m.transactional);
                assert_eq!(m.lsn, Lsn(99));
                assert_eq!(m.prefix.as_ref(), "my_prefix");
                assert_eq!(m.content, b"hello world");
            }
            _ => panic!("Expected LogicalDecodingMessage, got {msg:?}"),
        }
    }

    #[test]
    fn parse_stream_start_message() {
        let mut buf = vec![b'S'];
        buf.extend_from_slice(&42i32.to_be_bytes());
        buf.push(1); // first segment
        let msg = parse_pgoutput_message(&buf).unwrap();
        match msg {
            PgOutputMessage::StreamStart(s) => {
                assert_eq!(s.xid, 42);
                assert!(s.first_segment);
            }
            _ => panic!("Expected StreamStart, got {msg:?}"),
        }
    }

    #[test]
    fn parse_stream_stop_message() {
        let buf = vec![b'E'];
        let msg = parse_pgoutput_message(&buf).unwrap();
        assert!(matches!(msg, PgOutputMessage::StreamStop));
    }

    #[test]
    fn parse_stream_commit_message() {
        let mut buf = vec![b'c'];
        buf.extend_from_slice(&10i32.to_be_bytes()); // xid
        buf.push(0); // flags
        buf.extend_from_slice(&100u64.to_be_bytes()); // commit_lsn
        buf.extend_from_slice(&200u64.to_be_bytes()); // end_lsn
        buf.extend_from_slice(&999i64.to_be_bytes()); // timestamp
        let msg = parse_pgoutput_message(&buf).unwrap();
        match msg {
            PgOutputMessage::StreamCommit(c) => {
                assert_eq!(c.xid, 10);
                assert_eq!(c.flags, 0);
                assert_eq!(c.commit_lsn, Lsn(100));
                assert_eq!(c.end_lsn, Lsn(200));
                assert_eq!(c.commit_timestamp, 999);
            }
            _ => panic!("Expected StreamCommit, got {msg:?}"),
        }
    }

    #[test]
    fn parse_stream_abort_message() {
        let mut buf = vec![b'A'];
        buf.extend_from_slice(&5i32.to_be_bytes()); // xid
        buf.extend_from_slice(&6i32.to_be_bytes()); // sub_xid
        let msg = parse_pgoutput_message(&buf).unwrap();
        match msg {
            PgOutputMessage::StreamAbort(a) => {
                assert_eq!(a.xid, 5);
                assert_eq!(a.sub_xid, 6);
            }
            _ => panic!("Expected StreamAbort, got {msg:?}"),
        }
    }

    #[test]
    fn parse_unsupported_message_type_does_not_error() {
        // Simulate a two-phase commit 'b' (BeginPrepare) message
        let data = [b'b', 0, 0, 0, 1, 0, 0, 0, 2];
        let msg = parse_pgoutput_message(&data).unwrap();
        match msg {
            PgOutputMessage::Unsupported { msg_type, data } => {
                assert_eq!(msg_type, b'b');
                assert_eq!(data, &[0, 0, 0, 1, 0, 0, 0, 2]);
            }
            _ => panic!("Expected Unsupported, got {msg:?}"),
        }
    }

    #[test]
    fn lsn_display_and_parse_roundtrip() {
        let lsn = Lsn(0x0000_0001_0000_00A0);
        let s = lsn.to_string();
        let parsed = Lsn::from_pg_string(&s).unwrap();
        assert_eq!(lsn, parsed);
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use super::*;
    use crate::tokio_connection::new_client;
    use crate::PostgresConnectionSettings;

    fn unique_suffix() -> String {
        uuid::Uuid::new_v4().simple().to_string()[..12].to_string()
    }

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
        let sfx = unique_suffix();
        let table = format!("repl_test_basic_{sfx}");
        let slot = format!("test_slot_basic_{sfx}");
        let pub_name = format!("test_pub_basic_{sfx}");

        let mut regular = new_client(regular_settings()).await.unwrap();
        regular
            .execute_non_query_simple(&format!(
                "DROP PUBLICATION IF EXISTS {pub_name}; \
                 DROP TABLE IF EXISTS {table}; \
                 CREATE TABLE {table}(id int PRIMARY KEY, value text); \
                 CREATE PUBLICATION {pub_name} FOR TABLE {table};"
            ))
            .await
            .unwrap();

        let consistent_lsn = create_slot_on_regular(&mut regular, &slot).await;

        regular
            .execute_non_query_simple(&format!(
                "INSERT INTO {table} VALUES (1, 'hello'), (2, 'world');"
            ))
            .await
            .unwrap();

        let mut repl = new_client(replication_settings()).await.unwrap();
        let mut stream = repl
            .start_replication(
                &slot,
                consistent_lsn,
                &format!("proto_version '1', publication_names '{pub_name}'"),
            )
            .await
            .unwrap();

        let mut saw_begin = false;
        let mut saw_commit = false;
        let mut relation_name = String::new();
        let mut relation_col_count = 0usize;
        let mut insert_values: Vec<(String, String)> = Vec::new();
        let mut last_lsn = consistent_lsn;

        let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let msg = stream
                    .next_message()
                    .await
                    .unwrap()
                    .expect("unexpected end of stream");
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
                    ReplicationMessage::PrimaryKeepalive(_) => {}
                }
            }
        })
        .await;
        assert!(result.is_ok(), "Timed out waiting for replication messages");

        stream
            .send_status_update(last_lsn, last_lsn, last_lsn)
            .await
            .unwrap();

        assert!(saw_begin, "Should have seen BEGIN");
        assert!(saw_commit, "Should have seen COMMIT");
        assert_eq!(relation_name, table);
        assert_eq!(relation_col_count, 2);
        assert_eq!(
            insert_values,
            vec![
                ("1".to_string(), "hello".to_string()),
                ("2".to_string(), "world".to_string()),
            ]
        );

        drop(stream);
        drop(repl);
        drop_slot_on_regular(&mut regular, &slot).await;
        regular
            .execute_non_query_simple(&format!(
                "DROP PUBLICATION IF EXISTS {pub_name}; \
                 DROP TABLE IF EXISTS {table};"
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn replication_handles_column_addition() {
        let sfx = unique_suffix();
        let table = format!("repl_test_addcol_{sfx}");
        let slot = format!("test_slot_addcol_{sfx}");
        let pub_name = format!("test_pub_addcol_{sfx}");

        let mut regular = new_client(regular_settings()).await.unwrap();
        regular
            .execute_non_query_simple(&format!(
                "DROP PUBLICATION IF EXISTS {pub_name}; \
                 DROP TABLE IF EXISTS {table}; \
                 CREATE TABLE {table}(id int PRIMARY KEY, value text); \
                 CREATE PUBLICATION {pub_name} FOR TABLE {table};"
            ))
            .await
            .unwrap();

        let consistent_lsn = create_slot_on_regular(&mut regular, &slot).await;

        regular
            .execute_non_query_simple(&format!(
                "INSERT INTO {table} VALUES (1, 'before');"
            ))
            .await
            .unwrap();

        regular
            .execute_non_query_simple(&format!(
                "ALTER TABLE {table} ADD COLUMN extra text DEFAULT 'def';"
            ))
            .await
            .unwrap();

        regular
            .execute_non_query_simple(&format!(
                "INSERT INTO {table} VALUES (2, 'after', 'extra_val');"
            ))
            .await
            .unwrap();

        let mut repl = new_client(replication_settings()).await.unwrap();
        let mut stream = repl
            .start_replication(
                &slot,
                consistent_lsn,
                &format!("proto_version '1', publication_names '{pub_name}'"),
            )
            .await
            .unwrap();

        let mut relation_versions: Vec<(String, usize)> = Vec::new();
        let mut insert_col_counts: Vec<usize> = Vec::new();
        let mut commits_seen = 0u32;
        let mut last_lsn = consistent_lsn;

        let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let msg = stream
                    .next_message()
                    .await
                    .unwrap()
                    .expect("unexpected end of stream");
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
                    ReplicationMessage::PrimaryKeepalive(_) => {}
                }
            }
        })
        .await;
        assert!(result.is_ok(), "Timed out waiting for replication messages");

        stream
            .send_status_update(last_lsn, last_lsn, last_lsn)
            .await
            .unwrap();

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

        assert_eq!(insert_col_counts.len(), 2);
        assert_eq!(insert_col_counts[0], 2);
        assert_eq!(insert_col_counts[1], 3);

        drop(stream);
        drop(repl);
        drop_slot_on_regular(&mut regular, &slot).await;
        regular
            .execute_non_query_simple(&format!(
                "DROP PUBLICATION IF EXISTS {pub_name}; \
                 DROP TABLE IF EXISTS {table};"
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn replication_handles_column_removal() {
        let sfx = unique_suffix();
        let table = format!("repl_test_dropcol_{sfx}");
        let slot = format!("test_slot_dropcol_{sfx}");
        let pub_name = format!("test_pub_dropcol_{sfx}");

        let mut regular = new_client(regular_settings()).await.unwrap();
        regular
            .execute_non_query_simple(&format!(
                "DROP PUBLICATION IF EXISTS {pub_name}; \
                 DROP TABLE IF EXISTS {table}; \
                 CREATE TABLE {table}(id int PRIMARY KEY, old_col text, value text); \
                 CREATE PUBLICATION {pub_name} FOR TABLE {table};"
            ))
            .await
            .unwrap();

        let consistent_lsn = create_slot_on_regular(&mut regular, &slot).await;

        regular
            .execute_non_query_simple(&format!(
                "INSERT INTO {table} VALUES (1, 'old_data', 'value1');"
            ))
            .await
            .unwrap();

        regular
            .execute_non_query_simple(&format!(
                "ALTER TABLE {table} DROP COLUMN old_col;"
            ))
            .await
            .unwrap();

        regular
            .execute_non_query_simple(&format!(
                "INSERT INTO {table} VALUES (2, 'value2');"
            ))
            .await
            .unwrap();

        let mut repl = new_client(replication_settings()).await.unwrap();
        let mut stream = repl
            .start_replication(
                &slot,
                consistent_lsn,
                &format!("proto_version '1', publication_names '{pub_name}'"),
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
                let msg = stream
                    .next_message()
                    .await
                    .unwrap()
                    .expect("unexpected end of stream");
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
                    ReplicationMessage::PrimaryKeepalive(_) => {}
                }
            }
        })
        .await;
        assert!(result.is_ok(), "Timed out waiting for replication messages");

        stream
            .send_status_update(last_lsn, last_lsn, last_lsn)
            .await
            .unwrap();

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

        assert_eq!(insert_col_counts.len(), 2);
        assert_eq!(insert_col_counts[0], 3);
        assert_eq!(insert_col_counts[1], 2);

        drop(stream);
        drop(repl);
        drop_slot_on_regular(&mut regular, &slot).await;
        regular
            .execute_non_query_simple(&format!(
                "DROP PUBLICATION IF EXISTS {pub_name}; \
                 DROP TABLE IF EXISTS {table};"
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn basic_logical_replication_binary() {
        let sfx = unique_suffix();
        let table = format!("repl_test_bin_{sfx}");
        let slot = format!("test_slot_bin_{sfx}");
        let pub_name = format!("test_pub_bin_{sfx}");

        let mut regular = new_client(regular_settings()).await.unwrap();
        regular
            .execute_non_query_simple(&format!(
                "DROP PUBLICATION IF EXISTS {pub_name}; \
                 DROP TABLE IF EXISTS {table}; \
                 CREATE TABLE {table}(id int PRIMARY KEY, value text, flag bool); \
                 CREATE PUBLICATION {pub_name} FOR TABLE {table};"
            ))
            .await
            .unwrap();

        let consistent_lsn = create_slot_on_regular(&mut regular, &slot).await;

        regular
            .execute_non_query_simple(&format!(
                "INSERT INTO {table} VALUES (1, 'hello', true), (2, 'world', false);"
            ))
            .await
            .unwrap();

        let mut repl = new_client(replication_settings()).await.unwrap();
        let mut stream = repl
            .start_replication(
                &slot,
                consistent_lsn,
                &format!("proto_version '2', binary 'true', publication_names '{pub_name}'"),
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
                let msg = stream
                    .next_message()
                    .await
                    .unwrap()
                    .expect("unexpected end of stream");
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
                                    other => {
                                        panic!("Expected Binary column for id, got {other:?}")
                                    }
                                };
                                let value = match &ins.tuple.columns[1] {
                                    TupleColumn::Binary(b) => {
                                        std::str::from_utf8(b).unwrap().to_string()
                                    }
                                    other => {
                                        panic!("Expected Binary column for value, got {other:?}")
                                    }
                                };
                                let flag = match &ins.tuple.columns[2] {
                                    TupleColumn::Binary(b) => b[0] != 0,
                                    other => {
                                        panic!("Expected Binary column for flag, got {other:?}")
                                    }
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
                    ReplicationMessage::PrimaryKeepalive(_) => {}
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
        assert_eq!(relation_name, table);
        assert_eq!(
            insert_values,
            vec![
                (1, "hello".to_string(), true),
                (2, "world".to_string(), false),
            ]
        );

        drop(stream);
        drop(repl);
        drop_slot_on_regular(&mut regular, &slot).await;
        regular
            .execute_non_query_simple(&format!(
                "DROP PUBLICATION IF EXISTS {pub_name}; \
                 DROP TABLE IF EXISTS {table};"
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn replication_binary_handles_column_addition() {
        let sfx = unique_suffix();
        let table = format!("repl_test_binadd_{sfx}");
        let slot = format!("test_slot_binadd_{sfx}");
        let pub_name = format!("test_pub_binadd_{sfx}");

        let mut regular = new_client(regular_settings()).await.unwrap();
        regular
            .execute_non_query_simple(&format!(
                "DROP PUBLICATION IF EXISTS {pub_name}; \
                 DROP TABLE IF EXISTS {table}; \
                 CREATE TABLE {table}(id int PRIMARY KEY, value text); \
                 CREATE PUBLICATION {pub_name} FOR TABLE {table};"
            ))
            .await
            .unwrap();

        let consistent_lsn = create_slot_on_regular(&mut regular, &slot).await;

        regular
            .execute_non_query_simple(&format!(
                "INSERT INTO {table} VALUES (1, 'before');"
            ))
            .await
            .unwrap();

        regular
            .execute_non_query_simple(&format!(
                "ALTER TABLE {table} ADD COLUMN extra int DEFAULT 42;"
            ))
            .await
            .unwrap();

        regular
            .execute_non_query_simple(&format!(
                "INSERT INTO {table} VALUES (2, 'after', 99);"
            ))
            .await
            .unwrap();

        let mut repl = new_client(replication_settings()).await.unwrap();
        let mut stream = repl
            .start_replication(
                &slot,
                consistent_lsn,
                &format!("proto_version '2', binary 'true', publication_names '{pub_name}'"),
            )
            .await
            .unwrap();

        let mut relation_col_counts: Vec<usize> = Vec::new();
        let mut insert_col_counts: Vec<usize> = Vec::new();
        let mut commits_seen = 0u32;
        let mut last_lsn = consistent_lsn;

        let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let msg = stream
                    .next_message()
                    .await
                    .unwrap()
                    .expect("unexpected end of stream");
                match msg {
                    ReplicationMessage::XLogData(xlog) => {
                        last_lsn = xlog.end_lsn;
                        let pgmsg = parse_pgoutput_message(xlog.data).unwrap();
                        match pgmsg {
                            PgOutputMessage::Relation(rel) => {
                                relation_col_counts.push(rel.columns.len());
                            }
                            PgOutputMessage::Insert(ins) => {
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
                    ReplicationMessage::PrimaryKeepalive(_) => {}
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
        drop_slot_on_regular(&mut regular, &slot).await;
        regular
            .execute_non_query_simple(&format!(
                "DROP PUBLICATION IF EXISTS {pub_name}; \
                 DROP TABLE IF EXISTS {table};"
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn replication_captures_updates() {
        let sfx = unique_suffix();
        let table = format!("repl_test_upd_{sfx}");
        let slot = format!("test_slot_upd_{sfx}");
        let pub_name = format!("test_pub_upd_{sfx}");

        let mut regular = new_client(regular_settings()).await.unwrap();
        regular
            .execute_non_query_simple(&format!(
                "DROP PUBLICATION IF EXISTS {pub_name}; \
                 DROP TABLE IF EXISTS {table}; \
                 CREATE TABLE {table}(id int PRIMARY KEY, value text); \
                 ALTER TABLE {table} REPLICA IDENTITY FULL; \
                 CREATE PUBLICATION {pub_name} FOR TABLE {table};"
            ))
            .await
            .unwrap();

        let consistent_lsn = create_slot_on_regular(&mut regular, &slot).await;

        regular
            .execute_non_query_simple(&format!(
                "INSERT INTO {table} VALUES (1, 'original'); \
                 UPDATE {table} SET value = 'modified' WHERE id = 1;"
            ))
            .await
            .unwrap();

        let mut repl = new_client(replication_settings()).await.unwrap();
        let mut stream = repl
            .start_replication(
                &slot,
                consistent_lsn,
                &format!("proto_version '1', publication_names '{pub_name}'"),
            )
            .await
            .unwrap();

        let mut saw_update = false;
        let mut update_old_value: Option<String> = None;
        let mut update_new_value: Option<String> = None;
        let mut saw_commit = false;
        let mut last_lsn = consistent_lsn;

        let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let msg = stream
                    .next_message()
                    .await
                    .unwrap()
                    .expect("unexpected end of stream");
                match msg {
                    ReplicationMessage::XLogData(xlog) => {
                        last_lsn = xlog.end_lsn;
                        let pgmsg = parse_pgoutput_message(xlog.data).unwrap();
                        match pgmsg {
                            PgOutputMessage::Update(upd) => {
                                saw_update = true;
                                if let Some(ref old) = upd.old_tuple {
                                    if let TupleColumn::Text(t) = &old.columns[1] {
                                        update_old_value = Some(t.to_string());
                                    }
                                }
                                if let TupleColumn::Text(t) = &upd.new_tuple.columns[1] {
                                    update_new_value = Some(t.to_string());
                                }
                            }
                            PgOutputMessage::Commit(_) => {
                                saw_commit = true;
                                break;
                            }
                            _ => {}
                        }
                    }
                    ReplicationMessage::PrimaryKeepalive(_) => {}
                }
            }
        })
        .await;
        assert!(result.is_ok(), "Timed out waiting for replication messages");

        stream
            .send_status_update(last_lsn, last_lsn, last_lsn)
            .await
            .unwrap();

        assert!(saw_update, "Should have seen UPDATE");
        assert!(saw_commit, "Should have seen COMMIT");
        assert_eq!(update_old_value.as_deref(), Some("original"));
        assert_eq!(update_new_value.as_deref(), Some("modified"));

        drop(stream);
        drop(repl);
        drop_slot_on_regular(&mut regular, &slot).await;
        regular
            .execute_non_query_simple(&format!(
                "DROP PUBLICATION IF EXISTS {pub_name}; \
                 DROP TABLE IF EXISTS {table};"
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn replication_captures_deletes() {
        let sfx = unique_suffix();
        let table = format!("repl_test_del_{sfx}");
        let slot = format!("test_slot_del_{sfx}");
        let pub_name = format!("test_pub_del_{sfx}");

        let mut regular = new_client(regular_settings()).await.unwrap();
        regular
            .execute_non_query_simple(&format!(
                "DROP PUBLICATION IF EXISTS {pub_name}; \
                 DROP TABLE IF EXISTS {table}; \
                 CREATE TABLE {table}(id int PRIMARY KEY, value text); \
                 ALTER TABLE {table} REPLICA IDENTITY FULL; \
                 CREATE PUBLICATION {pub_name} FOR TABLE {table};"
            ))
            .await
            .unwrap();

        let consistent_lsn = create_slot_on_regular(&mut regular, &slot).await;

        regular
            .execute_non_query_simple(&format!(
                "INSERT INTO {table} VALUES (1, 'to_delete'); \
                 DELETE FROM {table} WHERE id = 1;"
            ))
            .await
            .unwrap();

        let mut repl = new_client(replication_settings()).await.unwrap();
        let mut stream = repl
            .start_replication(
                &slot,
                consistent_lsn,
                &format!("proto_version '1', publication_names '{pub_name}'"),
            )
            .await
            .unwrap();

        let mut saw_delete = false;
        let mut deleted_id: Option<String> = None;
        let mut deleted_value: Option<String> = None;
        let mut saw_commit = false;
        let mut last_lsn = consistent_lsn;

        let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let msg = stream
                    .next_message()
                    .await
                    .unwrap()
                    .expect("unexpected end of stream");
                match msg {
                    ReplicationMessage::XLogData(xlog) => {
                        last_lsn = xlog.end_lsn;
                        let pgmsg = parse_pgoutput_message(xlog.data).unwrap();
                        match pgmsg {
                            PgOutputMessage::Delete(del) => {
                                saw_delete = true;
                                if let TupleColumn::Text(t) = &del.old_tuple.columns[0] {
                                    deleted_id = Some(t.to_string());
                                }
                                if let TupleColumn::Text(t) = &del.old_tuple.columns[1] {
                                    deleted_value = Some(t.to_string());
                                }
                            }
                            PgOutputMessage::Commit(_) => {
                                saw_commit = true;
                                break;
                            }
                            _ => {}
                        }
                    }
                    ReplicationMessage::PrimaryKeepalive(_) => {}
                }
            }
        })
        .await;
        assert!(result.is_ok(), "Timed out waiting for replication messages");

        stream
            .send_status_update(last_lsn, last_lsn, last_lsn)
            .await
            .unwrap();

        assert!(saw_delete, "Should have seen DELETE");
        assert!(saw_commit, "Should have seen COMMIT");
        assert_eq!(deleted_id.as_deref(), Some("1"));
        assert_eq!(deleted_value.as_deref(), Some("to_delete"));

        drop(stream);
        drop(repl);
        drop_slot_on_regular(&mut regular, &slot).await;
        regular
            .execute_non_query_simple(&format!(
                "DROP PUBLICATION IF EXISTS {pub_name}; \
                 DROP TABLE IF EXISTS {table};"
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn replication_captures_truncate() {
        let sfx = unique_suffix();
        let table = format!("repl_test_trunc_{sfx}");
        let slot = format!("test_slot_trunc_{sfx}");
        let pub_name = format!("test_pub_trunc_{sfx}");

        let mut regular = new_client(regular_settings()).await.unwrap();
        regular
            .execute_non_query_simple(&format!(
                "DROP PUBLICATION IF EXISTS {pub_name}; \
                 DROP TABLE IF EXISTS {table}; \
                 CREATE TABLE {table}(id int PRIMARY KEY, value text); \
                 CREATE PUBLICATION {pub_name} FOR TABLE {table};"
            ))
            .await
            .unwrap();

        let consistent_lsn = create_slot_on_regular(&mut regular, &slot).await;

        regular
            .execute_non_query_simple(&format!(
                "INSERT INTO {table} VALUES (1, 'a'), (2, 'b');"
            ))
            .await
            .unwrap();

        regular
            .execute_non_query_simple(&format!("TRUNCATE {table};"))
            .await
            .unwrap();

        let mut repl = new_client(replication_settings()).await.unwrap();
        let mut stream = repl
            .start_replication(
                &slot,
                consistent_lsn,
                &format!("proto_version '1', publication_names '{pub_name}'"),
            )
            .await
            .unwrap();

        let mut saw_truncate = false;
        let mut truncate_relation_count = 0usize;
        let mut commits_seen = 0u32;
        let mut last_lsn = consistent_lsn;

        let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let msg = stream
                    .next_message()
                    .await
                    .unwrap()
                    .expect("unexpected end of stream");
                match msg {
                    ReplicationMessage::XLogData(xlog) => {
                        last_lsn = xlog.end_lsn;
                        let pgmsg = parse_pgoutput_message(xlog.data).unwrap();
                        match pgmsg {
                            PgOutputMessage::Truncate(trunc) => {
                                saw_truncate = true;
                                truncate_relation_count = trunc.relation_ids.len();
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
                    ReplicationMessage::PrimaryKeepalive(_) => {}
                }
            }
        })
        .await;
        assert!(result.is_ok(), "Timed out waiting for replication messages");

        stream
            .send_status_update(last_lsn, last_lsn, last_lsn)
            .await
            .unwrap();

        assert!(saw_truncate, "Should have seen TRUNCATE");
        assert_eq!(
            truncate_relation_count, 1,
            "TRUNCATE should reference 1 relation"
        );

        drop(stream);
        drop(repl);
        drop_slot_on_regular(&mut regular, &slot).await;
        regular
            .execute_non_query_simple(&format!(
                "DROP PUBLICATION IF EXISTS {pub_name}; \
                 DROP TABLE IF EXISTS {table};"
            ))
            .await
            .unwrap();
    }
}

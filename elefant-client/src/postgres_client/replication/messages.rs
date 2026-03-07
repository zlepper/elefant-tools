use crate::protocol::frame_reader::ByteSliceError;
use crate::protocol::FieldDescription;
use crate::types::{FromSqlBase, FromSqlText};
use crate::ElefantClientError;
use crate::PostgresType;
use std::borrow::Cow;
use std::error::Error;
use std::fmt;
use std::str::FromStr;

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

/// PostgreSQL Log Sequence Number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Lsn(pub u64);

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

impl FromStr for Lsn {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (high_str, low_str) = s
            .split_once('/')
            .ok_or_else(|| format!("Invalid LSN format: {s}"))?;
        let high =
            u64::from_str_radix(high_str, 16).map_err(|e| format!("Invalid LSN high part: {e}"))?;
        let low =
            u64::from_str_radix(low_str, 16).map_err(|e| format!("Invalid LSN low part: {e}"))?;
        Ok(Lsn((high << 32) | low))
    }
}

impl<'a> FromSqlBase<'a> for Lsn {
    fn accepts_postgres_type(oid: i32) -> bool {
        oid == PostgresType::PG_LSN.oid
    }
}

impl<'a> FromSqlText<'a> for Lsn {
    fn from_sql_text(
        raw: &'a str,
        _field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(raw.parse()?)
    }
}

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
    Unsupported {
        msg_type: u8,
        data: &'a [u8],
    },
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

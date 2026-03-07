use super::messages::*;
use crate::protocol::frame_reader::ByteSliceReader;

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

fn parse_tuple_data<'a>(
    reader: &mut ByteSliceReader<'a>,
) -> Result<TupleData<'a>, ReplicationError> {
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

#[cfg(test)]
mod tests {
    use super::*;

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

use super::message_reader::parse_replication_message;
use super::messages::*;
use crate::pool::ConnectionFactory;
use crate::postgres_client::PostgresClient;
use crate::protocol::frame_reader::ByteSliceWriter;
use crate::protocol::{BackendMessage, CopyData, FrontendMessage};
use crate::{reborrow_until_polonius, ElefantClientError};

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
    pub(super) fn new(client: &'a mut PostgresClient<F>) -> Self {
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
                let client: &mut PostgresClient<F> = reborrow_until_polonius!(&mut *self.client);
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
            .write_frontend_message(&FrontendMessage::CopyData(CopyData {
                data: &self.status_buf,
            }))
            .await?;
        self.client.connection.flush().await?;
        Ok(())
    }
}

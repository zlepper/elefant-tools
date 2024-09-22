use futures::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt};
use crate::error::PostgresMessageParseError;
use crate::io_extensions::AsyncReadExt2;
use crate::messages::{AuthenticationGSSContinue, AuthenticationMD5Password, AuthenticationSASL, AuthenticationSASLContinue, AuthenticationSASLFinal, BackendKeyData, BackendMessage};

pub struct MessageReader<R: AsyncRead + AsyncBufRead + Unpin> {
    reader: R,

    /// A buffer that can be reused when reading messages to avoid having to constantly resize.
    read_buffer: Vec<u8>,
}

impl<R: AsyncRead + AsyncBufRead + Unpin> MessageReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            read_buffer: Vec::new(),
        }
    }

    pub async fn parse_backend_message(
        &mut self,
    ) -> Result<BackendMessage, PostgresMessageParseError> {
        let reader = &mut self.reader;
        let message_type = reader.read_u8().await?;

        match message_type {
            b'R' => self.parse_authentication_message(message_type).await,
            b'K' => self.parse_backend_key_data().await,
            _ => Err(PostgresMessageParseError::UnknownMessage(message_type)),
        }
    }

    async fn parse_authentication_message(&mut self, message_type: u8) -> Result<BackendMessage, PostgresMessageParseError> {

        let length = self.reader.read_i32().await?;
        if length < 4 {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length,
            });
        }

        let sub_message_type = self.reader.read_i32().await?;

        match (length, sub_message_type) {
            (8, 0) => Ok(BackendMessage::AuthenticationOk),
            (8, 2) => Ok(BackendMessage::AuthenticationKerberosV5),
            (8, 3) => Ok(BackendMessage::AuthenticationCleartextPassword),
            (12, 5) => {
                let mut salt = [0; 4];
                self.reader.read_exact(&mut salt).await?;
                Ok(BackendMessage::AuthenticationMD5Password(
                    AuthenticationMD5Password { salt },
                ))
            }
            (8, 7) => Ok(BackendMessage::AuthenticationGSS),
            (_, 8) => {
                let remaining = (length - 8) as usize;
                self.extend_buffer(remaining);
                let data = &mut self.read_buffer[..remaining];
                self.reader.read_exact(data).await?;
                Ok(BackendMessage::AuthenticationGSSContinue(
                    AuthenticationGSSContinue { data },
                ))
            }
            (8, 9) => Ok(BackendMessage::AuthenticationSSPI),
            (_, 10) => {
                let remaining = (length - 8) as usize;
                self.extend_buffer(remaining);
                let data = &mut self.read_buffer[..remaining];
                self.reader.read_exact(data).await?;

                let mechanisms = data.split(|b| *b == b'\0')
                    .filter(|b| !b.is_empty())
                    .map(|slice| String::from_utf8_lossy(slice))
                    .collect();

                Ok(BackendMessage::AuthenticationSASL(AuthenticationSASL {
                    mechanisms,
                }))
            },
            (_, 11) => {
                let remaining = (length - 8) as usize;
                self.extend_buffer(remaining);
                let data = &mut self.read_buffer[..remaining];
                self.reader.read_exact(data).await?;
                Ok(BackendMessage::AuthenticationSASLContinue(
                    AuthenticationSASLContinue { data },
                ))
            },
            (_, 12) => {
                let remaining = (length - 8) as usize;
                self.extend_buffer(remaining);
                let data = &mut self.read_buffer[..remaining];
                self.reader.read_exact(data).await?;
                Ok(BackendMessage::AuthenticationSASLFinal(
                    AuthenticationSASLFinal { outcome: data },
                ))
            },
            _ => Err(PostgresMessageParseError::UnknownSubMessage {
                message_type,
                length,
                sub_message_type,
            }),
        }
    }

    async fn parse_backend_key_data(&mut self) -> Result<BackendMessage, PostgresMessageParseError> {
        let length = self.reader.read_i32().await?;

        if length != 12 {
            Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type: b'K',
                length,
            })
        } else {
            let process_id = self.reader.read_i32().await?;
            let secret_key = self.reader.read_i32().await?;
            Ok(BackendMessage::BackendKeyData(BackendKeyData {
                process_id,
                secret_key,
            }))
        }
    }

    async fn read_null_terminated_string(&mut self) -> Result<(String, usize), std::io::Error> {
        self.read_buffer.clear();
        let bytes_read = self.reader.read_until(b'\0', &mut self.read_buffer).await?;
        self.read_buffer.pop();

        String::from_utf8(self.read_buffer.clone())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            .map(|s| (s, bytes_read))
    }

    fn extend_buffer(&mut self, len: usize) {
        if self.read_buffer.len() < len {
            self.read_buffer.resize(len, 0);
        }
    }
}
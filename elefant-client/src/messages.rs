use crate::error::PostgresMessageParseError;
use crate::io_extensions::AsyncReadExt2;
use futures::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt};

#[derive(Debug, PartialEq, Eq)]
pub enum FrontendMessage {}

#[derive(Debug, PartialEq, Eq)]
pub enum BackendMessage {
    AuthenticationOk,
    AuthenticationKerberosV5,
    AuthenticationCleartextPassword,
    AuthenticationMD5Password(AuthenticationMD5Password),
    AuthenticationGSS,
    AuthenticationGSSContinue(AuthenticationGSSContinue),
    AuthenticationSSPI,
    AuthenticationSASL(AuthenticationSASL),
    AuthenticationSASLContinue(AuthenticationSASLContinue)
}

#[derive(Debug, PartialEq, Eq)]
pub struct AuthenticationMD5Password {
    pub salt: [u8; 4],
}

#[derive(Debug, PartialEq, Eq)]
pub struct AuthenticationGSSContinue {
    pub data: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct AuthenticationSASL {
    pub mechanisms: Vec<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct AuthenticationSASLContinue {
    pub data: Vec<u8>
}

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
            _ => Err(PostgresMessageParseError::UnknownMessage(message_type)),
        }
    }

    async fn parse_authentication_message(&mut self, message_type: u8) -> Result<BackendMessage, PostgresMessageParseError> {
        let reader = &mut self.reader;

        let length = reader.read_i32().await?;
        if length < 4 {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length,
            });
        }

        let sub_message_type = reader.read_i32().await?;

        match (length, sub_message_type) {
            (8, 0) => Ok(BackendMessage::AuthenticationOk),
            (8, 2) => Ok(BackendMessage::AuthenticationKerberosV5),
            (8, 3) => Ok(BackendMessage::AuthenticationCleartextPassword),
            (12, 5) => {
                let mut salt = [0; 4];
                reader.read_exact(&mut salt).await?;
                Ok(BackendMessage::AuthenticationMD5Password(
                    AuthenticationMD5Password { salt },
                ))
            }
            (8, 7) => Ok(BackendMessage::AuthenticationGSS),
            (_, 8) => {
                let remaining = (length - 8) as usize;
                let mut data = vec![0; remaining];
                reader.read_exact(&mut data).await?;
                Ok(BackendMessage::AuthenticationGSSContinue(
                    AuthenticationGSSContinue { data },
                ))
            }
            (8, 9) => Ok(BackendMessage::AuthenticationSSPI),
            (_, 10) => {
                let mut remaining = (length - 8) as usize;

                let mut mechanisms = Vec::new();
                while remaining > 0 {
                    let (mechanism, bytes_read) =
                        self.read_null_terminated_string().await?;
                    remaining -= bytes_read;
                    mechanisms.push(mechanism);
                }

                Ok(BackendMessage::AuthenticationSASL(AuthenticationSASL {
                    mechanisms,
                }))
            },
            (_, 11) => {
                let remaining = (length - 8) as usize;
                let mut data = vec![0; remaining];
                reader.read_exact(&mut data).await?;
                Ok(BackendMessage::AuthenticationSASLContinue(
                    AuthenticationSASLContinue { data },
                ))
            },
            _ => Err(PostgresMessageParseError::UnknownSubMessage {
                message_type,
                length,
                sub_message_type,
            }),
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io_extensions::ByteSliceExt;
    use futures::io::Cursor;
    use tokio::test;

    async fn assert_message_parses_as<By: AsRef<[u8]>>(bytes: By, expected: BackendMessage) {
        let mut cursor = Cursor::new(&bytes);
        let mut reader = MessageReader::new(&mut cursor);
        let result = reader.parse_backend_message().await.unwrap();
        assert_eq!(result, expected);
    }

    macro_rules! to_wire_bytes {
        ($($val:expr),*) => {{
            let mut bytes = Vec::new();
            $(
                bytes.extend_from_slice(&$val.to_be_bytes());
            )*
            bytes
        }};
    }

    #[test]
    async fn test_parse_backend_message() {
        assert_message_parses_as(
            to_wire_bytes!(b'R', 8i32, 0i32),
            BackendMessage::AuthenticationOk,
        )
        .await;
    }

    #[test]
    async fn test_parse_authentication_sasl_1_mechanism() {
        assert_message_parses_as(
            to_wire_bytes!(b'R', 12i32, 10i32, b"foo\0"),
            BackendMessage::AuthenticationSASL(AuthenticationSASL {
                mechanisms: vec!["foo".to_string()],
            }),
        )
        .await;
    }
    #[test]
    async fn test_parse_authentication_sasl_2_mechanisms() {
        assert_message_parses_as(
            to_wire_bytes!(b'R', 21i32, 10i32, b"foo\0", b"booooooo\0"),
            BackendMessage::AuthenticationSASL(AuthenticationSASL {
                mechanisms: vec!["foo".to_string(), "booooooo".to_string()],
            }),
        )
        .await;
    }
}

use crate::error::PostgresMessageParseError;
use crate::io_extensions::{AsyncReadExt2, ByteSliceReader};
use crate::messages::{AuthenticationGSSContinue, AuthenticationMD5Password, AuthenticationSASL, AuthenticationSASLContinue, AuthenticationSASLFinal, BackendKeyData, BackendMessage, Bind, CancelRequest, Close, CloseType, CommandComplete, CopyData, CopyFail, CopyResponse, DataRow, Describe, DescribeTarget, FrontendMessage, ValueFormat};
use futures::{AsyncBufRead, AsyncRead, AsyncReadExt};

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
        let message_type = self.reader.read_u8().await?;

        match message_type {
            b'R' => self.parse_authentication_message(message_type).await,
            b'K' => self.parse_backend_key_data().await,
            b'2' => self.parse_bind_completed(message_type).await,
            b'3' => self.parse_close_complete(message_type).await,
            b'C' => self.parse_command_complete(message_type).await,
            b'd' => Ok(BackendMessage::CopyData(self.parse_copy_data().await?)),
            b'c' => {
                self.assert_len_equals(4, message_type).await?;
                Ok(BackendMessage::CopyDone)
            },
            b'G' => Ok(BackendMessage::CopyInResponse(self.parse_copy_response(message_type).await?)),
            b'H' => Ok(BackendMessage::CopyOutResponse(self.parse_copy_response(message_type).await?)),
            b'W' => Ok(BackendMessage::CopyBothResponse(self.parse_copy_response(message_type).await?)),
            b'D' => self.parse_data_row(message_type).await,
            b'I' => {
                self.assert_len_equals(4, message_type).await?;
                Ok(BackendMessage::EmptyQueryResponse)
            },
            _ => Err(PostgresMessageParseError::UnknownMessage(message_type)),
        }
    }

    async fn parse_copy_response(&mut self, message_type: u8) -> Result<CopyResponse, PostgresMessageParseError> {
        let len = self.reader.read_i32().await?;

        if len < (4 + 1 + 2) {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            });
        }

        let format = match self.reader.read_i8().await? {
            0 => ValueFormat::Text,
            1 => ValueFormat::Binary,
            _ => return Err(PostgresMessageParseError::UnknownMessage(message_type)),
        };

        let column_count = self.reader.read_i16().await?;

        let mut column_formats = Vec::with_capacity(column_count as usize);

        for _ in 0..column_count {
            let format = match self.reader.read_i16().await? {
                0 => ValueFormat::Text,
                1 => ValueFormat::Binary,
                _ => return Err(PostgresMessageParseError::UnknownMessage(message_type)),
            };
            column_formats.push(format);
        }

        Ok(CopyResponse {
            format,
            column_formats,
        })
    }

    async fn assert_len_equals(
        &mut self,
        expected: i32,
        message_type: u8,
    ) -> Result<(), PostgresMessageParseError> {
        let len = self.reader.read_i32().await?;
        if len != expected {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            });
        }
        Ok(())
    }

    async fn parse_copy_data(&mut self) -> Result<CopyData<'_>, PostgresMessageParseError> {
        let len = self.reader.read_i32().await?;
        let len = len as usize - 4;
        self.extend_buffer(len);
        self.reader.read_exact(&mut self.read_buffer[..len]).await?;
        Ok(CopyData {
            data: &self.read_buffer[..len],
        })
    }

    async fn parse_command_complete(
        &mut self,
        message_type: u8,
    ) -> Result<BackendMessage, PostgresMessageParseError> {
        let len = self.reader.read_i32().await?;
        if len < 4 {
            Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            })
        } else {
            let length = len as usize - 4;
            self.extend_buffer(length);
            self.reader
                .read_exact(&mut self.read_buffer[..length])
                .await?;
            let tag = String::from_utf8_lossy(&self.read_buffer[..length - 1]);

            Ok(BackendMessage::CommandComplete(CommandComplete { tag }))
        }
    }
    async fn parse_data_row(&mut self, message_type: u8) -> Result<BackendMessage, PostgresMessageParseError> {
        let len = self.reader.read_i32().await?;
        if len < 4 {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            });
        }
        
        let length = len as usize - 4;
        self.extend_buffer(length);
        self.reader.read_exact(&mut self.read_buffer[..length]).await?;
        
        let mut reader = ByteSliceReader::new(&self.read_buffer);
        let column_count = reader.read_i16()?;
        
        let mut columns = Vec::with_capacity(column_count as usize);
        for _ in 0..column_count {
            let len = reader.read_i32()?;
            if len == -1 {
                columns.push(None);
            } else {
                let bytes = reader.read_bytes(len as usize)?;
                columns.push(Some(bytes));
            }
        }
        
        Ok(BackendMessage::DataRow(DataRow { values: columns }))
    }

    async fn parse_close_complete(
        &mut self,
        message_type: u8,
    ) -> Result<BackendMessage, PostgresMessageParseError> {
        self.assert_len_equals(4, message_type).await?;
        Ok(BackendMessage::CloseComplete)
    }

    async fn parse_bind_completed(
        &mut self,
        message_type: u8,
    ) -> Result<BackendMessage, PostgresMessageParseError> {
        self.assert_len_equals(4, message_type).await?;
        Ok(BackendMessage::BindComplete)
    }

    async fn parse_authentication_message(
        &mut self,
        message_type: u8,
    ) -> Result<BackendMessage, PostgresMessageParseError> {
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

                let mechanisms = data
                    .split(|b| *b == b'\0')
                    .filter(|b| !b.is_empty())
                    .map(|slice| String::from_utf8_lossy(slice))
                    .collect();

                Ok(BackendMessage::AuthenticationSASL(AuthenticationSASL {
                    mechanisms,
                }))
            }
            (_, 11) => {
                let remaining = (length - 8) as usize;
                self.extend_buffer(remaining);
                let data = &mut self.read_buffer[..remaining];
                self.reader.read_exact(data).await?;
                Ok(BackendMessage::AuthenticationSASLContinue(
                    AuthenticationSASLContinue { data },
                ))
            }
            (_, 12) => {
                let remaining = (length - 8) as usize;
                self.extend_buffer(remaining);
                let data = &mut self.read_buffer[..remaining];
                self.reader.read_exact(data).await?;
                Ok(BackendMessage::AuthenticationSASLFinal(
                    AuthenticationSASLFinal { outcome: data },
                ))
            }
            _ => Err(PostgresMessageParseError::UnknownSubMessage {
                message_type,
                length,
                sub_message_type,
            }),
        }
    }

    async fn parse_backend_key_data(
        &mut self,
    ) -> Result<BackendMessage, PostgresMessageParseError> {
        self.assert_len_equals(12, b'K').await?;
        let process_id = self.reader.read_i32().await?;
        let secret_key = self.reader.read_i32().await?;
        Ok(BackendMessage::BackendKeyData(BackendKeyData {
            process_id,
            secret_key,
        }))
    }

    pub async fn parse_frontend_message(
        &mut self,
    ) -> Result<FrontendMessage, PostgresMessageParseError> {
        let message_type = self.reader.read_u8().await?;

        match message_type {
            b'B' => self.parse_bind_message().await,
            b'C' => self.parse_close_message().await,
            b'd' => Ok(FrontendMessage::CopyData(self.parse_copy_data().await?)),
            b'c' => {
                self.assert_len_equals(4, message_type).await?;
                Ok(FrontendMessage::CopyDone)
            }
            b'f' => {
                let len = self.reader.read_i32().await?;
                if len <= 4 {
                    return Err(PostgresMessageParseError::UnexpectedMessageLength {
                        message_type,
                        length: len,
                    });
                }

                let length = len as usize - 4;
                self.extend_buffer(length);
                self.reader
                    .read_exact(&mut self.read_buffer[..length])
                    .await?;
                let message = String::from_utf8_lossy(&self.read_buffer[..length - 1]);

                Ok(FrontendMessage::CopyFail(CopyFail { message }))
            }
            b'D' => {
                let len = self.reader.read_i32().await?;
                if len <= 4 {
                    return Err(PostgresMessageParseError::UnexpectedMessageLength {
                        message_type,
                        length: len,
                    });
                }
                
                let length = len as usize - 4;
                self.extend_buffer(length);
                self.reader.read_exact(&mut self.read_buffer[..length]).await?;
                let mut reader = ByteSliceReader::new(&self.read_buffer);
                
                let typ = match reader.read_u8()? {
                    b'P' => DescribeTarget::Portal,
                    b'S' => DescribeTarget::Statement,
                    b => return Err(PostgresMessageParseError::UnknownDescribeTarget(b)),
                };
                
                let name = reader.read_null_terminated_string()?;
                
                Ok(FrontendMessage::Describe(Describe {
                    target: typ,
                    name,
                }))
            },
            _ => {
                let more = self.reader.read_bytes::<3>().await?;
                let length = i32::from_be_bytes([message_type, more[0], more[1], more[2]]);

                if length == 16 {
                    let code = self.reader.read_i32().await?;
                    if code == 80877102 {
                        let process_id = self.reader.read_i32().await?;
                        let secret_key = self.reader.read_i32().await?;

                        return Ok(FrontendMessage::CancelRequest(CancelRequest {
                            process_id,
                            secret_key,
                        }));
                    }
                }

                Err(PostgresMessageParseError::UnknownMessage(message_type))
            }
        }
    }

    async fn parse_bind_message(&mut self) -> Result<FrontendMessage, PostgresMessageParseError> {
        let length = (self.reader.read_i32().await? as usize) - 4;
        self.read_buffer.resize(length, 0);
        self.reader
            .read_exact(&mut self.read_buffer[..length])
            .await?;

        let mut reader = ByteSliceReader::new(&self.read_buffer);

        let portal_name = reader.read_null_terminated_string()?;
        let statement_name = reader.read_null_terminated_string()?;
        let parameter_format_count = reader.read_i16()?;

        let mut parameter_formats = Vec::with_capacity(parameter_format_count as usize);
        for _ in 0..parameter_format_count {
            let format = reader.read_i16()?;
            let format = match format {
                0 => ValueFormat::Text,
                1 => ValueFormat::Binary,
                _ => {
                    return Err(PostgresMessageParseError::UnknownValueFormat(
                        format,
                    ))
                }
            };
            parameter_formats.push(format);
        }

        let parameter_value_count = reader.read_i16()?;
        let mut parameter_values = Vec::with_capacity(parameter_value_count as usize);
        for _ in 0..parameter_value_count {
            let len = reader.read_i32()?;

            if len == -1 {
                parameter_values.push(None);
            } else {
                let bytes = reader.read_bytes(len as usize)?;
                parameter_values.push(Some(bytes));
            }
        }

        let result_format_count = reader.read_i16()?;
        let mut result_formats = Vec::with_capacity(result_format_count as usize);
        for _ in 0..result_format_count {
            let format = reader.read_i16()?;
            let format = match format {
                0 => ValueFormat::Text,
                1 => ValueFormat::Binary,
                _ => return Err(PostgresMessageParseError::UnknownValueFormat(format)),
            };
            result_formats.push(format);
        }

        Ok(FrontendMessage::Bind(Bind {
            destination_portal_name: portal_name,
            source_statement_name: statement_name,
            parameter_formats,
            parameter_values,
            result_column_formats: result_formats,
        }))
    }

    async fn parse_close_message(&mut self) -> Result<FrontendMessage, PostgresMessageParseError> {
        let length = (self.reader.read_i32().await? as usize) - 4;
        self.read_buffer.resize(length, 0);
        self.reader
            .read_exact(&mut self.read_buffer[..length])
            .await?;

        let mut reader = ByteSliceReader::new(&self.read_buffer);

        let target = match reader.read_u8()? {
            b'S' => CloseType::Statement,
            b'P' => CloseType::Portal,
            b => return Err(PostgresMessageParseError::UnknownCloseTarget(b)),
        };
        let name = reader.read_null_terminated_string()?;

        Ok(FrontendMessage::Close(Close { target, name }))
    }

    fn extend_buffer(&mut self, len: usize) {
        if self.read_buffer.len() < len {
            self.read_buffer.resize(len, 0);
        }
    }
}

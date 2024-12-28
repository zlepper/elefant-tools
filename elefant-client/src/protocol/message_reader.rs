use std::rc::Rc;
use futures::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncWrite};
use crate::protocol::{AuthenticationGSSContinue, AuthenticationMD5Password, AuthenticationSASL, AuthenticationSASLContinue, AuthenticationSASLFinal, BackendKeyData, BackendMessage, Bind, CancelRequest, Close, CloseType, CommandComplete, CopyData, CopyFail, CopyResponse, CurrentTransactionStatus, DataRow, Describe, DescribeTarget, ErrorField, ErrorResponse, Execute, FieldDescription, FrontendMessage, FrontendPMessage, FunctionCall, FunctionCallResponse, NegotiateProtocolVersion, NotificationResponse, ParameterDescription, ParameterStatus, Parse, PostgresMessageParseError, Query, ReadyForQuery, RowDescription, StartupMessage, StartupMessageParameter, UndecidedFrontendPMessage, ValueFormat};
use crate::protocol::io_extensions::{AsyncReadExt2, ByteSliceReader};
use crate::protocol::postgres_connection::PostgresConnection;

impl<C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin> PostgresConnection<C> {
    

    pub async fn read_backend_message(
        &mut self,
    ) -> Result<BackendMessage, PostgresMessageParseError> {
        let message_type = self.connection.read_u8().await?;

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
            b'E' => Ok(BackendMessage::ErrorResponse(self.parse_error_response(message_type).await?)),
            b'N' => Ok(BackendMessage::NoticeResponse(self.parse_error_response(message_type).await?)),
            b'V' => self.parse_function_call_response(message_type).await,
            b'v' => self.parse_negotiate_protocol_version(message_type).await,
            b'n' => {
                self.assert_len_equals(4, message_type).await?;
                Ok(BackendMessage::NoData)
            },
            b'A' => self.parse_notification_response(message_type).await,
            b't' => self.parse_parameter_description(message_type).await,
            b'S' => self.parse_parameter_status(message_type).await,
            b'1' => {
                self.assert_len_equals(4, message_type).await?;
                Ok(BackendMessage::ParseComplete)
            },
            b's' => {
                self.assert_len_equals(4, message_type).await?;
                Ok(BackendMessage::PortalSuspended)
            },
            b'Z' => {
                self.assert_len_equals(5, message_type).await?;
                let status = match self.connection.read_u8().await? {
                    b'I' => CurrentTransactionStatus::Idle,
                    b'T' => CurrentTransactionStatus::InTransaction,
                    b'E' => CurrentTransactionStatus::InFailedTransaction,
                    status => return Err(PostgresMessageParseError::UnknownTransactionStatus(status)),
                };
                Ok(BackendMessage::ReadyForQuery(ReadyForQuery { current_transaction_status: status }))
            },
            b'T' => self.parse_row_description(message_type).await,
            _ => Err(PostgresMessageParseError::UnknownMessage(message_type)),
        }
    }

    async fn parse_copy_response(&mut self, message_type: u8) -> Result<CopyResponse, PostgresMessageParseError> {
        let len = self.connection.read_i32().await?;

        if len < (4 + 1 + 2) {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            });
        }

        let format = match self.connection.read_i8().await? {
            0 => ValueFormat::Text,
            1 => ValueFormat::Binary,
            _ => return Err(PostgresMessageParseError::UnknownMessage(message_type)),
        };

        let column_count = self.connection.read_i16().await?;

        let mut column_formats = Vec::with_capacity(column_count as usize);

        for _ in 0..column_count {
            let format = match self.connection.read_i16().await? {
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
    
    async fn parse_row_description(&mut self, message_type: u8) -> Result<BackendMessage, PostgresMessageParseError> {
        let len = self.connection.read_i32().await?;
        if len < 6 {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            });
        }
        
        let length = len as usize - 4;
        self.extend_buffer(length);
        self.connection.read_exact(&mut self.read_buffer[..length]).await?;
        
        let mut reader = ByteSliceReader::new(&self.read_buffer);
        let column_count = reader.read_i16()?;
        
        let mut fields = Vec::with_capacity(column_count as usize);
        for _ in 0..column_count {
            let name = reader.read_null_terminated_string()?;
            let table_oid = reader.read_i32()?;
            let column_attribute_number = reader.read_i16()?;
            let data_type_oid = reader.read_i32()?;
            let data_type_size = reader.read_i16()?;
            let type_modifier = reader.read_i32()?;
            let format = match reader.read_i16()? {
                0 => ValueFormat::Text,
                1 => ValueFormat::Binary,
                format => return Err(PostgresMessageParseError::UnknownValueFormat(format)),
            };
            
            fields.push(FieldDescription {
                name: Rc::new(name.to_string()),
                table_oid,
                column_attribute_number,
                data_type_oid,
                data_type_size,
                type_modifier,
                format,
            });
        }
        
        Ok(BackendMessage::RowDescription(RowDescription { fields }))
    }
    
    async fn parse_notification_response(&mut self, message_type: u8) -> Result<BackendMessage, PostgresMessageParseError> {
        let len = self.connection.read_i32().await?;

        if len < 4 {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            });
        }

        let length = len as usize - 4;
        self.extend_buffer(length);
        self.connection.read_exact(&mut self.read_buffer[..length]).await?;

        let mut reader = ByteSliceReader::new(&self.read_buffer);
        let pid = reader.read_i32()?;
        let channel = reader.read_null_terminated_string()?;
        let payload = reader.read_null_terminated_string()?;

        Ok(BackendMessage::NotificationResponse(NotificationResponse {
            process_id: pid,
            channel,
            payload,
        }))
    }

    async fn parse_function_call_response(&mut self, message_type: u8) -> Result<BackendMessage, PostgresMessageParseError> {
        let len = self.connection.read_i32().await?;

        if len < 8 {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            });
        }

        let length = len as usize - 4;
        self.extend_buffer(length);
        self.connection.read_exact(&mut self.read_buffer[..length]).await?;

        let mut reader = ByteSliceReader::new(&self.read_buffer);
        let result_value_length = reader.read_i32()?;

        if result_value_length == -1 {
            Ok(BackendMessage::FunctionCallResponse(FunctionCallResponse { value: None }))
        } else {
            let bytes = reader.read_bytes(result_value_length as usize)?;
            Ok(BackendMessage::FunctionCallResponse(FunctionCallResponse { value: Some(bytes) }))
        }
    }

    async fn parse_negotiate_protocol_version(&mut self, message_type: u8) -> Result<BackendMessage, PostgresMessageParseError> {
        let len = self.connection.read_i32().await?;

        if len < 12 {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            });
        }

        let length = len as usize - 4;
        self.extend_buffer(length);
        self.connection.read_exact(&mut self.read_buffer[..length]).await?;

        let mut reader = ByteSliceReader::new(&self.read_buffer);
        let newest_protocol_version = reader.read_i32()?;
        let option_count = reader.read_i32()?;

        let mut options = Vec::with_capacity(option_count as usize);
        for _ in 0..option_count {
            options.push(reader.read_null_terminated_string()?);
        }

        Ok(BackendMessage::NegotiateProtocolVersion(NegotiateProtocolVersion {
            newest_protocol_version,
            protocol_options: options,
        }))
    }

    async fn parse_error_response(&mut self, message_type: u8) -> Result<ErrorResponse, PostgresMessageParseError> {
        let len = self.connection.read_i32().await?;
        if len < 4 {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            });
        }

        let length = len as usize - 4;
        self.extend_buffer(length);
        self.connection.read_exact(&mut self.read_buffer[..length]).await?;

        let mut reader = ByteSliceReader::new(&self.read_buffer);
        let mut fields = Vec::new();

        loop {
            let field_type = reader.read_u8()?;
            if field_type == 0 {
                break;
            }

            let value = reader.read_null_terminated_string()?;
            fields.push(ErrorField {
                field_type,
                value,
            });
        }

        Ok(ErrorResponse { fields })
    }

    async fn assert_len_equals(
        &mut self,
        expected: i32,
        message_type: u8,
    ) -> Result<(), PostgresMessageParseError> {
        let len = self.connection.read_i32().await?;
        if len != expected {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            });
        }
        Ok(())
    }

    async fn parse_copy_data(&mut self) -> Result<CopyData<'_>, PostgresMessageParseError> {
        let len = self.connection.read_i32().await?;
        let len = len as usize - 4;
        self.extend_buffer(len);
        self.connection.read_exact(&mut self.read_buffer[..len]).await?;
        Ok(CopyData {
            data: &self.read_buffer[..len],
        })
    }

    async fn parse_command_complete(
        &mut self,
        message_type: u8,
    ) -> Result<BackendMessage, PostgresMessageParseError> {
        let len = self.connection.read_i32().await?;
        if len < 4 {
            Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            })
        } else {
            let length = len as usize - 4;
            self.extend_buffer(length);
            self.connection
                .read_exact(&mut self.read_buffer[..length])
                .await?;
            let tag = String::from_utf8_lossy(&self.read_buffer[..length - 1]);

            Ok(BackendMessage::CommandComplete(CommandComplete { tag }))
        }
    }
    async fn parse_data_row(&mut self, message_type: u8) -> Result<BackendMessage, PostgresMessageParseError> {
        let len = self.connection.read_i32().await?;
        if len < 4 {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            });
        }

        let length = len as usize - 4;
        self.extend_buffer(length);
        self.connection.read_exact(&mut self.read_buffer[..length]).await?;

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
        let length = self.connection.read_i32().await?;
        if length < 4 {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length,
            });
        }

        let sub_message_type = self.connection.read_i32().await?;

        match (length, sub_message_type) {
            (8, 0) => Ok(BackendMessage::AuthenticationOk),
            (8, 2) => Ok(BackendMessage::AuthenticationKerberosV5),
            (8, 3) => Ok(BackendMessage::AuthenticationCleartextPassword),
            (12, 5) => {
                let mut salt = [0; 4];
                self.connection.read_exact(&mut salt).await?;
                Ok(BackendMessage::AuthenticationMD5Password(
                    AuthenticationMD5Password { salt },
                ))
            }
            (8, 7) => Ok(BackendMessage::AuthenticationGSS),
            (_, 8) => {
                let remaining = (length - 8) as usize;
                self.extend_buffer(remaining);
                let data = &mut self.read_buffer[..remaining];
                self.connection.read_exact(data).await?;
                Ok(BackendMessage::AuthenticationGSSContinue(
                    AuthenticationGSSContinue { data },
                ))
            }
            (8, 9) => Ok(BackendMessage::AuthenticationSSPI),
            (_, 10) => {
                let remaining = (length - 8) as usize;
                self.extend_buffer(remaining);
                let data = &mut self.read_buffer[..remaining];
                self.connection.read_exact(data).await?;

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
                self.connection.read_exact(data).await?;
                Ok(BackendMessage::AuthenticationSASLContinue(
                    AuthenticationSASLContinue { data },
                ))
            }
            (_, 12) => {
                let remaining = (length - 8) as usize;
                self.extend_buffer(remaining);
                let data = &mut self.read_buffer[..remaining];
                self.connection.read_exact(data).await?;
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
        let process_id = self.connection.read_i32().await?;
        let secret_key = self.connection.read_i32().await?;
        Ok(BackendMessage::BackendKeyData(BackendKeyData {
            process_id,
            secret_key,
        }))
    }


    async fn parse_parameter_description(&mut self, message_type: u8) -> Result<BackendMessage, PostgresMessageParseError> {

        let len = self.connection.read_i32().await?;
        if len < 6 {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            });
        }

        let parameter_count = self.connection.read_i16().await?;

        let mut parameters = Vec::with_capacity(parameter_count as usize);
        for _ in 0..parameter_count {
            let oid = self.connection.read_i32().await?;
            parameters.push(oid);
        }

        Ok(BackendMessage::ParameterDescription(ParameterDescription {
            types: parameters,
        }))
    }

    async fn parse_parameter_status(&mut self, message_type: u8) -> Result<BackendMessage, PostgresMessageParseError> {
        let len = self.connection.read_i32().await?;
        if len < 4 {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            });
        }

        let length = len as usize - 4;
        self.extend_buffer(length);
        self.connection.read_exact(&mut self.read_buffer[..length]).await?;

        let mut reader = ByteSliceReader::new(&self.read_buffer);
        let name = reader.read_null_terminated_string()?;
        let value = reader.read_null_terminated_string()?;

        Ok(BackendMessage::ParameterStatus(ParameterStatus {
            name,
            value,
        }))
    }

    pub async fn parse_frontend_message(
        &mut self,
    ) -> Result<FrontendMessage, PostgresMessageParseError> {
        let message_type = self.connection.read_u8().await?;

        match message_type {
            b'B' => self.parse_bind_message().await,
            b'C' => self.parse_close_message().await,
            b'd' => Ok(FrontendMessage::CopyData(self.parse_copy_data().await?)),
            b'c' => {
                self.assert_len_equals(4, message_type).await?;
                Ok(FrontendMessage::CopyDone)
            }
            b'f' => {
                let len = self.connection.read_i32().await?;
                if len <= 4 {
                    return Err(PostgresMessageParseError::UnexpectedMessageLength {
                        message_type,
                        length: len,
                    });
                }

                let length = len as usize - 4;
                self.extend_buffer(length);
                self.connection
                    .read_exact(&mut self.read_buffer[..length])
                    .await?;
                let message = String::from_utf8_lossy(&self.read_buffer[..length - 1]);

                Ok(FrontendMessage::CopyFail(CopyFail { message }))
            }
            b'D' => {
                let len = self.connection.read_i32().await?;
                if len <= 4 {
                    return Err(PostgresMessageParseError::UnexpectedMessageLength {
                        message_type,
                        length: len,
                    });
                }

                let length = len as usize - 4;
                self.extend_buffer(length);
                self.connection.read_exact(&mut self.read_buffer[..length]).await?;
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
            b'E' => {
                let len = self.connection.read_i32().await?;
                if len <= 4 {
                    return Err(PostgresMessageParseError::UnexpectedMessageLength {
                        message_type,
                        length: len,
                    });
                }

                let length = len as usize - 4;
                self.extend_buffer(length);
                self.connection.read_exact(&mut self.read_buffer[..length]).await?;
                let mut reader = ByteSliceReader::new(&self.read_buffer);

                let portal_name = reader.read_null_terminated_string()?;
                let max_rows = reader.read_i32()?;

                Ok(FrontendMessage::Execute(Execute {
                    portal_name,
                    max_rows,
                }))
            },
            b'H' => {
                let len = self.connection.read_i32().await?;
                if len != 4 {
                    return Err(PostgresMessageParseError::UnexpectedMessageLength {
                        message_type,
                        length: len,
                    });
                }

                Ok(FrontendMessage::Flush)
            },
            b'F' => self.parse_function_call(message_type).await,
            b'p' =>  {
                let len = self.connection.read_i32().await?;
                if len < 4 {
                    return Err(PostgresMessageParseError::UnexpectedMessageLength {
                        message_type,
                        length: len,
                    });
                }

                let length = len as usize - 4;
                self.extend_buffer(length);
                self.connection.read_exact(&mut self.read_buffer[..length]).await?;
                
                Ok(FrontendMessage::FrontendPMessage(FrontendPMessage::Undecided(UndecidedFrontendPMessage {
                    data: &self.read_buffer[..length],
                })))
            },
            b'P' => {
                let len = self.connection.read_i32().await?;
                if len < 8 {
                    return Err(PostgresMessageParseError::UnexpectedMessageLength {
                        message_type,
                        length: len,
                    });
                }

                let length = len as usize - 4;
                self.extend_buffer(length);
                self.connection.read_exact(&mut self.read_buffer[..length]).await?;

                let mut reader = ByteSliceReader::new(&self.read_buffer);
                let destination = reader.read_null_terminated_string()?;
                let query = reader.read_null_terminated_string()?;

                let parameter_count = reader.read_i16()?;
                let mut parameter_types = Vec::with_capacity(parameter_count as usize);

                for _ in 0..parameter_count {
                    parameter_types.push(reader.read_i32()?);
                }

                Ok(FrontendMessage::Parse(Parse {
                    destination,
                    query,
                    parameter_types,
                }))
            },
            b'Q' => {
                let len = self.connection.read_i32().await?;
                if len < 5 {
                    return Err(PostgresMessageParseError::UnexpectedMessageLength {
                        message_type,
                        length: len,
                    });
                }
                
                let length = len as usize - 4;
                self.extend_buffer(length);
                self.connection.read_exact(&mut self.read_buffer[..length]).await?;
                
                let mut reader = ByteSliceReader::new(&self.read_buffer);
                
                let query = reader.read_null_terminated_string()?;
                
                Ok(FrontendMessage::Query(Query { query }))
            },
            b'S' => {
                self.assert_len_equals(4, message_type).await?;
                Ok(FrontendMessage::Sync)
            },
            b'X' => {
                self.assert_len_equals(4, message_type).await?;
                Ok(FrontendMessage::Terminate)
            },
            _ => {
                let more = self.connection.read_bytes::<3>().await?;
                let length = i32::from_be_bytes([message_type, more[0], more[1], more[2]]);

                if length == 16 {
                    let code = self.connection.read_i32().await?;
                    if code == 80877102 {
                        let process_id = self.connection.read_i32().await?;
                        let secret_key = self.connection.read_i32().await?;

                        return Ok(FrontendMessage::CancelRequest(CancelRequest {
                            process_id,
                            secret_key,
                        }));
                    }
                } else if length == 8 {
                    let code = self.connection.read_i32().await?;
                    if code == 80877104 {
                        return Ok(FrontendMessage::GSSENCRequest);
                    } else if code == 80877103 {
                        return Ok(FrontendMessage::SSLRequest);
                    }
                }
                
                if length >= 8 {
                    let code = self.connection.read_i32().await?;
                    if code == 196608 {
                        let len = (length - 8) as usize;
                        self.extend_buffer(len);
                        self.connection.read_exact(&mut self.read_buffer[..len]).await?;
                        
                        let mut reader = ByteSliceReader::new(&self.read_buffer[..len]);
                        let mut options = Vec::new();
                        loop {
                            let option = reader.read_null_terminated_string()?;
                            if option.is_empty() {
                                break;
                            }
                            
                            let value = reader.read_null_terminated_string()?;
                            options.push(StartupMessageParameter {
                                name: option,
                                value,
                            });
                        }
                        
                        return Ok(FrontendMessage::StartupMessage(StartupMessage {
                            parameters: options,
                        }));
                    }
                }

                Err(PostgresMessageParseError::UnknownMessage(message_type))
            }
        }
    }

    async fn parse_bind_message(&mut self) -> Result<FrontendMessage, PostgresMessageParseError> {
        let length = (self.connection.read_i32().await? as usize) - 4;
        self.read_buffer.resize(length, 0);
        self.connection
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
        let length = (self.connection.read_i32().await? as usize) - 4;
        self.read_buffer.resize(length, 0);
        self.connection
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

    async fn parse_function_call(&mut self, message_type: u8) -> Result<FrontendMessage, PostgresMessageParseError> {
        let len = self.connection.read_i32().await?;
        if len <= 4 {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            });
        }

        let length = len as usize - 4;
        self.extend_buffer(length);
        self.connection.read_exact(&mut self.read_buffer[..length]).await?;

        let mut reader = ByteSliceReader::new(&self.read_buffer);
        let object_id = reader.read_i32()?;
        let argument_format_count = reader.read_i16()?;
        let mut argument_formats = Vec::with_capacity(argument_format_count as usize);

        for _ in 0..argument_format_count {
            let format = match reader.read_i16()? {
                0 => ValueFormat::Text,
                1 => ValueFormat::Binary,
                format => return Err(PostgresMessageParseError::UnknownValueFormat(format)),
            };
            argument_formats.push(format);
        }

        let argument_count = reader.read_i16()?;
        let mut arguments = Vec::with_capacity(argument_count as usize);

        for _ in 0..argument_count {
            let len = reader.read_i32()?;
            if len == -1 {
                arguments.push(None);
            } else {
                let bytes = reader.read_bytes(len as usize)?;
                arguments.push(Some(bytes));
            }
        }

        let result_format = match reader.read_i16()? {
            0 => ValueFormat::Text,
            1 => ValueFormat::Binary,
            format => return Err(PostgresMessageParseError::UnknownValueFormat(format)),
        };

        Ok(FrontendMessage::FunctionCall(FunctionCall {
            object_id,
            argument_formats,
            arguments,
            result_format,
        }))
    }

    fn extend_buffer(&mut self, len: usize) {
        if self.read_buffer.len() < len {
            self.read_buffer.resize(len, 0);
        }
    }

}
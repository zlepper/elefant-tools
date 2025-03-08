use futures::{AsyncRead, AsyncWrite};
use crate::protocol::{AuthenticationGSSContinue, AuthenticationMD5Password, AuthenticationSASL, AuthenticationSASLContinue, AuthenticationSASLFinal, BackendKeyData, BackendMessage, Bind, CancelRequest, Close, CloseType, CommandComplete, CopyData, CopyFail, CopyResponse, CurrentTransactionStatus, DataRow, Describe, DescribeTarget, ErrorField, ErrorResponse, Execute, FieldDescription, FrontendMessage, FrontendPMessage, FunctionCall, FunctionCallResponse, NegotiateProtocolVersion, NotificationResponse, ParameterDescription, ParameterStatus, Parse, PostgresMessageParseError, Query, ReadyForQuery, RowDescription, StartupMessage, StartupMessageParameter, UndecidedFrontendPMessage, ValueFormat};
use crate::protocol::frame_reader::{DecodeResult, Decoder};
use crate::protocol::postgres_connection::PostgresConnection;

struct PostgresMessageDecoder<'a, 'b> {
    buffer: &'b mut crate::protocol::frame_reader::ByteSliceReader<'a>,
}


impl<'a, 'b> PostgresMessageDecoder<'a, 'b> {

    fn parse_authentication_message(
        self,
        message_type: u8,
    ) -> DecodeResult<BackendMessage<'a>, PostgresMessageParseError> {
        let length = self.buffer.read_i32()?;
        if length < 4 {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length,
            }.into());
        }

        let sub_message_type = self.buffer.read_i32()?;

        match (length, sub_message_type) {
            (8, 0) => Ok(BackendMessage::AuthenticationOk),
            (8, 2) => Ok(BackendMessage::AuthenticationKerberosV5),
            (8, 3) => Ok(BackendMessage::AuthenticationCleartextPassword),
            (12, 5) => {
                let mut salt = [0; 4];
                self.buffer.read_exact(&mut salt)?;
                Ok(BackendMessage::AuthenticationMD5Password(
                    AuthenticationMD5Password { salt },
                ))
            }
            (8, 7) => Ok(BackendMessage::AuthenticationGSS),
            (_, 8) => {
                let remaining = (length - 8) as usize;
                let data = self.buffer.read_bytes(remaining)?;
                Ok(BackendMessage::AuthenticationGSSContinue(
                    AuthenticationGSSContinue { data },
                ))
            }
            (8, 9) => Ok(BackendMessage::AuthenticationSSPI),
            (_, 10) => {
                let remaining = (length - 8) as usize;
                let data = self.buffer.read_bytes(remaining)?;

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
                let data = self.buffer.read_bytes(remaining)?;
                Ok(BackendMessage::AuthenticationSASLContinue(
                    AuthenticationSASLContinue { data },
                ))
            }
            (_, 12) => {
                let remaining = (length - 8) as usize;
                let data = self.buffer.read_bytes(remaining)?;
                Ok(BackendMessage::AuthenticationSASLFinal(
                    AuthenticationSASLFinal { outcome: data },
                ))
            }
            _ => Err(PostgresMessageParseError::UnknownSubMessage {
                message_type,
                length,
                sub_message_type,
            }.into()),
        }
    }


    fn parse_backend_key_data(
        mut self
    ) -> DecodeResult<BackendMessage<'a>, PostgresMessageParseError> {
        self.assert_len_equals(12, b'K')?;
        let process_id = self.buffer.read_i32()?;
        let secret_key = self.buffer.read_i32()?;
        Ok(BackendMessage::BackendKeyData(BackendKeyData {
            process_id,
            secret_key,
        }))
    }


    fn parse_bind_completed(
        mut self,
        message_type: u8,
    ) -> DecodeResult<BackendMessage<'a>, PostgresMessageParseError> {
        self.assert_len_equals(4, message_type)?;
        Ok(BackendMessage::BindComplete)
    }

    fn parse_close_complete(
        mut self,
        message_type: u8,
    ) -> DecodeResult<BackendMessage<'a>, PostgresMessageParseError> {
        self.assert_len_equals(4, message_type)?;
        Ok(BackendMessage::CloseComplete)
    }

    fn assert_len_equals(&mut self, expected: i32, message_type: u8) -> DecodeResult<(), PostgresMessageParseError> {
        let length = self.buffer.read_i32()?;

        if length != expected {
            Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length,
            }.into())
        } else {
            Ok(())
        }
    }


    fn parse_command_complete(
        self,
        message_type: u8,
    ) -> DecodeResult<BackendMessage<'a>, PostgresMessageParseError> {
        let len = self.buffer.read_i32()?;
        if len < 4 {
            Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            }.into())
        } else {
            self.buffer.require_bytes(len as usize - 4)?;
            let tag = self.buffer.read_null_terminated_string()?;

            Ok(BackendMessage::CommandComplete(CommandComplete { tag }))
        }
    }


    fn parse_copy_data(self) -> DecodeResult<CopyData<'a>, PostgresMessageParseError> {
        let len = self.buffer.read_i32()?;
        let len = len as usize - 4;
        let data = self.buffer.read_bytes(len)?;
        Ok(CopyData {
            data,
        })
    }


    fn parse_error_response(self, message_type: u8) -> DecodeResult<ErrorResponse<'a>, PostgresMessageParseError> {
        let len = self.buffer.read_i32()?;
        if len < 4 {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            }.into());
        }

        self.buffer.require_bytes(len as usize - 4)?;

        let mut fields = Vec::new();

        loop {
            let field_type = self.buffer.read_u8()?;
            if field_type == 0 {
                break;
            }

            let value = self.buffer.read_null_terminated_string()?;
            fields.push(ErrorField {
                field_type,
                value,
            });
        }

        Ok(ErrorResponse { fields })
    }


    fn parse_negotiate_protocol_version(self, message_type: u8) -> DecodeResult<BackendMessage<'a>, PostgresMessageParseError> {
        let len = self.buffer.read_i32()?;

        if len < 12 {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            }.into());
        }

        self.buffer.require_bytes(len as usize - 4)?;
        let newest_protocol_version = self.buffer.read_i32()?;
        let option_count = self.buffer.read_i32()?;

        let mut options = Vec::with_capacity(option_count as usize);
        for _ in 0..option_count {
            options.push(self.buffer.read_null_terminated_string()?);
        }

        Ok(BackendMessage::NegotiateProtocolVersion(NegotiateProtocolVersion {
            newest_protocol_version,
            protocol_options: options,
        }))
    }


    fn parse_function_call_response(self, message_type: u8) -> DecodeResult<BackendMessage<'a>, PostgresMessageParseError> {
        let len = self.buffer.read_i32()?;

        if len < 8 {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            }.into());
        }

        let length = len as usize - 4;
        self.buffer.require_bytes(length)?;
        let result_value_length = self.buffer.read_i32()?;

        if result_value_length == -1 {
            Ok(BackendMessage::FunctionCallResponse(FunctionCallResponse { value: None }))
        } else {
            let bytes = self.buffer.read_bytes(result_value_length as usize)?;
            Ok(BackendMessage::FunctionCallResponse(FunctionCallResponse { value: Some(bytes) }))
        }
    }


    fn parse_notification_response(self, message_type: u8) -> DecodeResult<BackendMessage<'a>, PostgresMessageParseError> {
        let len = self.buffer.read_i32()?;

        if len < 4 {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            }.into());
        }

        self.buffer.require_bytes(len as usize - 4)?;

        let pid = self.buffer.read_i32()?;
        let channel = self.buffer.read_null_terminated_string()?;
        let payload = self.buffer.read_null_terminated_string()?;

        Ok(BackendMessage::NotificationResponse(NotificationResponse {
            process_id: pid,
            channel,
            payload,
        }))
    }


    fn parse_row_description(self, message_type: u8) -> DecodeResult<BackendMessage<'a>, PostgresMessageParseError> {
        let len = self.buffer.read_i32()?;
        if len < 6 {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            }.into());
        }

        self.buffer.require_bytes(len as usize - 4)?;

        let column_count = self.buffer.read_i16()?;

        let mut fields = Vec::with_capacity(column_count as usize);
        for _ in 0..column_count {
            let name = self.buffer.read_null_terminated_string()?;
            let table_oid = self.buffer.read_i32()?;
            let column_attribute_number = self.buffer.read_i16()?;
            let data_type_oid = self.buffer.read_i32()?;
            let data_type_size = self.buffer.read_i16()?;
            let type_modifier = self.buffer.read_i32()?;
            let format = match self.buffer.read_i16()? {
                0 => ValueFormat::Text,
                1 => ValueFormat::Binary,
                format => return Err(PostgresMessageParseError::UnknownValueFormat(format).into()),
            };

            fields.push(FieldDescription {
                name: name.to_string(),
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


    fn parse_copy_response(self, message_type: u8) -> DecodeResult<CopyResponse, PostgresMessageParseError> {
        let len = self.buffer.read_i32()?;

        if len < (4 + 1 + 2) {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            }.into());
        }

        let format = match self.buffer.read_i8()? {
            0 => ValueFormat::Text,
            1 => ValueFormat::Binary,
            _ => return Err(PostgresMessageParseError::UnknownMessage(message_type).into()),
        };

        let column_count = self.buffer.read_i16()?;

        let mut column_formats = Vec::with_capacity(column_count as usize);

        for _ in 0..column_count {
            let format = match self.buffer.read_i16()? {
                0 => ValueFormat::Text,
                1 => ValueFormat::Binary,
                _ => return Err(PostgresMessageParseError::UnknownMessage(message_type).into()),
            };
            column_formats.push(format);
        }

        Ok(CopyResponse {
            format,
            column_formats,
        })
    }

    fn parse_data_row(self, message_type: u8) -> DecodeResult<BackendMessage<'a>, PostgresMessageParseError> {
        let len = self.buffer.read_i32()?;
        if len < 4 {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            }.into());
        }

        self.buffer.require_bytes(len as usize - 4)?;
        let column_count = self.buffer.read_i16()?;

        let mut columns = Vec::with_capacity(column_count as usize);
        for _ in 0..column_count {
            let len = self.buffer.read_i32()?;
            if len == -1 {
                columns.push(None);
            } else {
                let bytes = self.buffer.read_bytes(len as usize)?;
                columns.push(Some(bytes));
            }
        }

        Ok(BackendMessage::DataRow(DataRow { values: columns }))
    }


    fn parse_parameter_description(self, message_type: u8) -> DecodeResult<BackendMessage<'a>, PostgresMessageParseError> {

        let len = self.buffer.read_i32()?;
        if len < 6 {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            }.into());
        }

        self.buffer.require_bytes(len as usize - 4)?;

        let parameter_count = self.buffer.read_i16()?;

        let mut parameters = Vec::with_capacity(parameter_count as usize);
        for _ in 0..parameter_count {
            let oid = self.buffer.read_i32()?;
            parameters.push(oid);
        }

        Ok(BackendMessage::ParameterDescription(ParameterDescription {
            types: parameters,
        }))
    }


    fn parse_parameter_status(self, message_type: u8) -> DecodeResult<BackendMessage<'a>, PostgresMessageParseError> {
        let len = self.buffer.read_i32()?;
        if len < 4 {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            }.into());
        }

        self.buffer.require_bytes(len as usize - 4)?;

        let name = self.buffer.read_null_terminated_string()?;
        let value = self.buffer.read_null_terminated_string()?;

        Ok(BackendMessage::ParameterStatus(ParameterStatus {
            name,
            value,
        }))
    }




    fn new(buffer: &'b mut crate::protocol::frame_reader::ByteSliceReader<'a>) -> Self {
        Self {
            buffer,
        }
    }

    fn decode_backend_message(mut self) -> DecodeResult<BackendMessage<'a>, PostgresMessageParseError> {

        let message_type = self.buffer.read_u8()?;

        match message_type {
            b'R' => self.parse_authentication_message(message_type),
            b'K' => self.parse_backend_key_data(),
            b'2' => self.parse_bind_completed(message_type),
            b'3' => self.parse_close_complete(message_type),
            b'C' => self.parse_command_complete(message_type),
            b'd' => Ok(BackendMessage::CopyData(self.parse_copy_data()?)),
            b'c' => {
                self.assert_len_equals(4, message_type)?;
                Ok(BackendMessage::CopyDone)
            },
            b'G' => Ok(BackendMessage::CopyInResponse(self.parse_copy_response(message_type)?)),
            b'H' => Ok(BackendMessage::CopyOutResponse(self.parse_copy_response(message_type)?)),
            b'W' => Ok(BackendMessage::CopyBothResponse(self.parse_copy_response(message_type)?)),
            b'D' => self.parse_data_row(message_type),
            b'I' => {
                self.assert_len_equals(4, message_type)?;
                Ok(BackendMessage::EmptyQueryResponse)
            },
            b'E' => Ok(BackendMessage::ErrorResponse(self.parse_error_response(message_type)?)),
            b'N' => Ok(BackendMessage::NoticeResponse(self.parse_error_response(message_type)?)),
            b'V' => self.parse_function_call_response(message_type),
            b'v' => self.parse_negotiate_protocol_version(message_type),
            b'n' => {
                self.assert_len_equals(4, message_type)?;
                Ok(BackendMessage::NoData)
            },
            b'A' => self.parse_notification_response(message_type),
            b't' => self.parse_parameter_description(message_type),
            b'S' => self.parse_parameter_status(message_type),
            b'1' => {
                self.assert_len_equals(4, message_type)?;
                Ok(BackendMessage::ParseComplete)
            },
            b's' => {
                self.assert_len_equals(4, message_type)?;
                Ok(BackendMessage::PortalSuspended)
            },
            b'Z' => {
                self.assert_len_equals(5, message_type)?;
                let status = match self.buffer.read_u8()? {
                    b'I' => CurrentTransactionStatus::Idle,
                    b'T' => CurrentTransactionStatus::InTransaction,
                    b'E' => CurrentTransactionStatus::InFailedTransaction,
                    status => return Err(PostgresMessageParseError::UnknownTransactionStatus(status).into()),
                };
                Ok(BackendMessage::ReadyForQuery(ReadyForQuery { current_transaction_status: status }))
            },
            b'T' => self.parse_row_description(message_type),
            _ => Err(PostgresMessageParseError::UnknownMessage(message_type).into()),
        }
    }


    fn parse_bind_message(self) -> DecodeResult<FrontendMessage<'a>, PostgresMessageParseError> {
        let length = (self.buffer.read_i32()? as usize) - 4;
        self.buffer.require_bytes(length)?;

        let portal_name = self.buffer.read_null_terminated_string()?;
        let statement_name = self.buffer.read_null_terminated_string()?;
        let parameter_format_count = self.buffer.read_i16()?;

        let mut parameter_formats = Vec::with_capacity(parameter_format_count as usize);
        for _ in 0..parameter_format_count {
            let format = self.buffer.read_i16()?;
            let format = match format {
                0 => ValueFormat::Text,
                1 => ValueFormat::Binary,
                _ => {
                    return Err(PostgresMessageParseError::UnknownValueFormat(
                        format,
                    ).into())
                }
            };
            parameter_formats.push(format);
        }

        let parameter_value_count = self.buffer.read_i16()?;
        let mut parameter_values = Vec::with_capacity(parameter_value_count as usize);
        for _ in 0..parameter_value_count {
            let len = self.buffer.read_i32()?;

            if len == -1 {
                parameter_values.push(None);
            } else {
                let bytes = self.buffer.read_bytes(len as usize)?;
                parameter_values.push(Some(bytes));
            }
        }

        let result_format_count = self.buffer.read_i16()?;
        let mut result_formats = Vec::with_capacity(result_format_count as usize);
        for _ in 0..result_format_count {
            let format = self.buffer.read_i16()?;
            let format = match format {
                0 => ValueFormat::Text,
                1 => ValueFormat::Binary,
                _ => return Err(PostgresMessageParseError::UnknownValueFormat(format).into()),
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

    fn parse_close_message(self) -> DecodeResult<FrontendMessage<'a>, PostgresMessageParseError> {
        let length = (self.buffer.read_i32()? as usize) - 4;
        self.buffer.require_bytes(length)?;

        let target = match self.buffer.read_u8()? {
            b'S' => CloseType::Statement,
            b'P' => CloseType::Portal,
            b => return Err(PostgresMessageParseError::UnknownCloseTarget(b).into()),
        };
        let name = self.buffer.read_null_terminated_string()?;

        Ok(FrontendMessage::Close(Close { target, name }))
    }

    fn parse_function_call(self, message_type: u8) -> DecodeResult<FrontendMessage<'a>, PostgresMessageParseError> {
        let len = self.buffer.read_i32()?;
        if len <= 4 {
            return Err(PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length: len,
            }.into());
        }

        self.buffer.require_bytes(len as usize - 4)?;
        
        let object_id = self.buffer.read_i32()?;
        let argument_format_count = self.buffer.read_i16()?;
        let mut argument_formats = Vec::with_capacity(argument_format_count as usize);

        for _ in 0..argument_format_count {
            let format = match self.buffer.read_i16()? {
                0 => ValueFormat::Text,
                1 => ValueFormat::Binary,
                format => return Err(PostgresMessageParseError::UnknownValueFormat(format).into()),
            };
            argument_formats.push(format);
        }

        let argument_count = self.buffer.read_i16()?;
        let mut arguments = Vec::with_capacity(argument_count as usize);

        for _ in 0..argument_count {
            let len = self.buffer.read_i32()?;
            if len == -1 {
                arguments.push(None);
            } else {
                let bytes = self.buffer.read_bytes(len as usize)?;
                arguments.push(Some(bytes));
            }
        }

        let result_format = match self.buffer.read_i16()? {
            0 => ValueFormat::Text,
            1 => ValueFormat::Binary,
            format => return Err(PostgresMessageParseError::UnknownValueFormat(format).into()),
        };

        Ok(FrontendMessage::FunctionCall(FunctionCall {
            object_id,
            argument_formats,
            arguments,
            result_format,
        }))
    }
    
    fn decode_frontend_message(mut self) -> DecodeResult<FrontendMessage<'a>, PostgresMessageParseError> {
        let message_type = self.buffer.read_u8()?;

        match message_type {
            b'B' => self.parse_bind_message(),
            b'C' => self.parse_close_message(),
            b'd' => Ok(FrontendMessage::CopyData(self.parse_copy_data()?)),
            b'c' => {
                self.assert_len_equals(4, message_type)?;
                Ok(FrontendMessage::CopyDone)
            }
            b'f' => {
                let len = self.buffer.read_i32()?;
                if len <= 4 {
                    return Err(PostgresMessageParseError::UnexpectedMessageLength {
                        message_type,
                        length: len,
                    }.into());
                }

                self.buffer.require_bytes(len as usize - 4)?;
                let message = self.buffer.read_null_terminated_string()?;

                Ok(FrontendMessage::CopyFail(CopyFail { message }))
            }
            b'D' => {
                let len = self.buffer.read_i32()?;
                if len <= 4 {
                    return Err(PostgresMessageParseError::UnexpectedMessageLength {
                        message_type,
                        length: len,
                    }.into());
                }

                self.buffer.require_bytes(len as usize - 4)?;

                let typ = match self.buffer.read_u8()? {
                    b'P' => DescribeTarget::Portal,
                    b'S' => DescribeTarget::Statement,
                    b => return Err(PostgresMessageParseError::UnknownDescribeTarget(b).into()),
                };

                let name = self.buffer.read_null_terminated_string()?;

                Ok(FrontendMessage::Describe(Describe {
                    target: typ,
                    name,
                }))
            },
            b'E' => {
                let len = self.buffer.read_i32()?;
                if len <= 4 {
                    return Err(PostgresMessageParseError::UnexpectedMessageLength {
                        message_type,
                        length: len,
                    }.into());
                }

                self.buffer.require_bytes(len as usize - 4)?;

                let portal_name = self.buffer.read_null_terminated_string()?;
                let max_rows = self.buffer.read_i32()?;

                Ok(FrontendMessage::Execute(Execute {
                    portal_name,
                    max_rows,
                }))
            },
            b'H' => {
                let len = self.buffer.read_i32()?;
                if len != 4 {
                    return Err(PostgresMessageParseError::UnexpectedMessageLength {
                        message_type,
                        length: len,
                    }.into());
                }

                Ok(FrontendMessage::Flush)
            },
            b'F' => self.parse_function_call(message_type),
            b'p' =>  {
                let len = self.buffer.read_i32()?;
                if len < 4 {
                    return Err(PostgresMessageParseError::UnexpectedMessageLength {
                        message_type,
                        length: len,
                    }.into());
                }

                let data = self.buffer.read_bytes(len as usize - 4)?;
                Ok(FrontendMessage::FrontendPMessage(FrontendPMessage::Undecided(UndecidedFrontendPMessage {
                    data,
                })))
            },
            b'P' => {
                let len = self.buffer.read_i32()?;
                if len < 8 {
                    return Err(PostgresMessageParseError::UnexpectedMessageLength {
                        message_type,
                        length: len,
                    }.into());
                }

                self.buffer.require_bytes(len as usize - 4)?;
                
                let destination = self.buffer.read_null_terminated_string()?;
                let query = self.buffer.read_null_terminated_string()?;

                let parameter_count = self.buffer.read_i16()?;
                let mut parameter_types = Vec::with_capacity(parameter_count as usize);

                for _ in 0..parameter_count {
                    parameter_types.push(self.buffer.read_i32()?);
                }

                Ok(FrontendMessage::Parse(Parse {
                    destination,
                    query,
                    parameter_types,
                }))
            },
            b'Q' => {
                let len = self.buffer.read_i32()?;
                if len < 5 {
                    return Err(PostgresMessageParseError::UnexpectedMessageLength {
                        message_type,
                        length: len,
                    }.into());
                }

                self.buffer.require_bytes(len as usize - 4)?;

                let query = self.buffer.read_null_terminated_string()?;

                Ok(FrontendMessage::Query(Query { query }))
            },
            b'S' => {
                self.assert_len_equals(4, message_type)?;
                Ok(FrontendMessage::Sync)
            },
            b'X' => {
                self.assert_len_equals(4, message_type)?;
                Ok(FrontendMessage::Terminate)
            },
            _ => {
                let mut more = [0; 3];
                self.buffer.read_exact(&mut more)?;
                let length = i32::from_be_bytes([message_type, more[0], more[1], more[2]]);

                if length == 16 {
                    let code = self.buffer.read_i32()?;
                    if code == 80877102 {
                        let process_id = self.buffer.read_i32()?;
                        let secret_key = self.buffer.read_i32()?;

                        return Ok(FrontendMessage::CancelRequest(CancelRequest {
                            process_id,
                            secret_key,
                        }));
                    }
                } else if length == 8 {
                    let code = self.buffer.read_i32()?;
                    if code == 80877104 {
                        return Ok(FrontendMessage::GSSENCRequest);
                    } else if code == 80877103 {
                        return Ok(FrontendMessage::SSLRequest);
                    }
                }

                if length >= 8 {
                    let code = self.buffer.read_i32()?;
                    if code == 196608 {
                        let len = (length - 8) as usize;
                        self.buffer.require_bytes(len)?;
                        
                        let mut options = Vec::new();
                        loop {
                            let option = self.buffer.read_null_terminated_string()?;
                            if option.is_empty() {
                                break;
                            }

                            let value = self.buffer.read_null_terminated_string()?;
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

                Err(PostgresMessageParseError::UnknownMessage(message_type).into())
            }
        }
    }

}

impl<'a, 'b> Decoder<'a, BackendMessage<'a>> for PostgresMessageDecoder<'a, 'b> {
    type Error = PostgresMessageParseError;

    fn decode(buffer: &mut crate::protocol::frame_reader::ByteSliceReader<'a>) -> DecodeResult<BackendMessage<'a>, Self::Error> {
        let decoder = PostgresMessageDecoder::new(buffer);

        decoder.decode_backend_message()
    }
}

impl<'a, 'b> Decoder<'a, FrontendMessage<'a>> for PostgresMessageDecoder<'a, 'b> {
    type Error = PostgresMessageParseError;

    fn decode(buffer: &mut crate::protocol::frame_reader::ByteSliceReader<'a>) -> DecodeResult<FrontendMessage<'a>, Self::Error> {
        let decoder = PostgresMessageDecoder::new(buffer);

        decoder.decode_frontend_message()
    }
}


impl<C: AsyncRead + AsyncWrite + Unpin> PostgresConnection<C> {
    pub async fn read_backend_message(
        &mut self,
    ) -> Result<BackendMessage, PostgresMessageParseError> {
        self.connection.read_frame::<PostgresMessageDecoder, _>().await
    }

    pub async fn parse_frontend_message(
        &mut self,
    ) -> Result<FrontendMessage, PostgresMessageParseError> {
        self.connection.read_frame::<PostgresMessageDecoder, _>().await
    }
}
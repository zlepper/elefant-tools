use crate::protocol::io_extensions::AsyncWriteExt2;
use crate::protocol::messages::{BackendMessage, Bind, CloseType, CopyResponse, DescribeTarget, ErrorResponse, FrontendMessage};
use futures::{AsyncBufRead, AsyncRead, AsyncWrite, AsyncWriteExt};
use std::io::Error;
use crate::protocol::postgres_connection::PostgresConnection;
use crate::protocol::{AuthenticationGSSContinue, AuthenticationMD5Password, AuthenticationSASL, AuthenticationSASLContinue, AuthenticationSASLFinal, BackendKeyData, CancelRequest, Close, CommandComplete, CopyData, CopyFail, CurrentTransactionStatus, DataRow, Describe, Execute, FrontendPMessage, FunctionCall, FunctionCallResponse, NegotiateProtocolVersion, NotificationResponse, ParameterDescription, ParameterStatus, Parse, Query, ReadyForQuery, RowDescription, StartupMessage, ValueFormat};

impl<C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin> PostgresConnection<C> {


    pub async fn write_backend_message(
        &mut self,
        message: &BackendMessage<'_>,
    ) -> Result<(), std::io::Error> {
        match message {
            BackendMessage::AuthenticationOk => {
                self.write_authentication_ok().await
            }
            BackendMessage::AuthenticationKerberosV5 => {
                self.write_authentication_kerberos_v5().await
            }
            BackendMessage::AuthenticationCleartextPassword => {
                self.write_authentication_cleartext_password().await
            }
            BackendMessage::AuthenticationMD5Password(md5) => {
                self.write_authentication_md5_password(md5).await
            }
            BackendMessage::AuthenticationGSS => {
                self.write_authentication_gss().await
            }
            BackendMessage::AuthenticationGSSContinue(gss) => {
                self.write_authentication_gss_continue(gss).await
            }
            BackendMessage::AuthenticationSSPI => {
                self.write_authentication_sspi().await
            }
            BackendMessage::AuthenticationSASL(sasl) => {
                self.write_authentication_sasl(sasl).await
            }
            BackendMessage::AuthenticationSASLContinue(sasl) => {
                self.write_authentication_sasl_continue(sasl).await
            }
            BackendMessage::AuthenticationSASLFinal(sasl) => {
                self.write_authentication_sasl_final(sasl).await
            }
            BackendMessage::BackendKeyData(bkd) => {
                self.write_backend_key_data(bkd).await
            }
            BackendMessage::BindComplete => {
                self.write_bind_complete().await
            }
            BackendMessage::CloseComplete => {
                self.write_close_complete().await
            }
            BackendMessage::CommandComplete(cc) => {
                self.write_command_complete(cc).await
            }
            BackendMessage::CopyData(cd) => {
                self.write_copy_data(cd).await
            }
            BackendMessage::CopyDone => {
                self.write_copy_done().await
            }
            BackendMessage::CopyInResponse(cr) => {
                self.write_copy_in_response(cr).await
            }
            BackendMessage::CopyOutResponse(cr) => {
                self.write_copy_out_response(cr).await
            }
            BackendMessage::CopyBothResponse(cr) => {
                self.write_copy_both_response(cr).await
            }
            BackendMessage::DataRow(dr) => {
                self.write_data_row(dr).await?;
                Ok(())
            },
            BackendMessage::EmptyQueryResponse => {
                self.write_empty_query_response().await
            },
            BackendMessage::ErrorResponse(er) => {
                self.write_error_response(er).await
            },
            BackendMessage::NoticeResponse(er) => {
                self.write_notice_response(er).await
            },
            BackendMessage::FunctionCallResponse(fr) => {
                self.write_function_call_response(fr).await
            },
            BackendMessage::NegotiateProtocolVersion(npv) => {
                self.write_negotiate_protocol_version(npv).await
            },
            BackendMessage::NoData => {
                self.write_no_data().await
            },
            BackendMessage::NotificationResponse(nr) => {
                self.write_notification_response(nr).await
            },
            BackendMessage::ParameterDescription(dp) => {
                self.write_parameter_description(dp).await
            },
            BackendMessage::ParameterStatus(ps) => {
                self.write_parameter_status(ps).await
            },
            BackendMessage::ParseComplete => {
                self.write_parse_complete().await
            },
            BackendMessage::PortalSuspended => {
                self.write_portal_suspended().await
            },
            BackendMessage::ReadyForQuery(q) => {
                self.write_ready_for_query(q).await
            },
            BackendMessage::RowDescription(rd) => {
                self.write_row_description(rd).await
            }
        }
    }

    async fn write_row_description(&mut self, rd: &RowDescription) -> Result<(), Error> {
        self.connection.write_u8(b'T').await?;
        let length = 4 + 2 + rd.fields.iter().map(|f| f.name.len() + 1 + 4 + 2 + 4 + 2 + 4 + 2).sum::<usize>() as i32;
        self.connection.write_i32(length).await?;
        self.connection.write_i16(rd.fields.len() as i16).await?;
        for field in &rd.fields {
            self.connection.write_null_terminated_string(&field.name).await?;
            self.connection.write_i32(field.table_oid).await?;
            self.connection.write_i16(field.column_attribute_number).await?;
            self.connection.write_i32(field.data_type_oid).await?;
            self.connection.write_i16(field.data_type_size).await?;
            self.connection.write_i32(field.type_modifier).await?;
            self.connection.write_i16(match field.format {
                ValueFormat::Text => 0,
                ValueFormat::Binary => 1
            }).await?;
        }
        Ok(())
    }

    async fn write_ready_for_query(&mut self, q: &ReadyForQuery) -> Result<(), Error> {
        self.connection.write_u8(b'Z').await?;
        self.connection.write_i32(5).await?;
        self.connection.write_u8(match q.current_transaction_status {
            CurrentTransactionStatus::Idle => b'I',
            CurrentTransactionStatus::InTransaction => b'T',
            CurrentTransactionStatus::InFailedTransaction => b'E',
        }).await?;
        Ok(())
    }

    async fn write_portal_suspended(&mut self) -> Result<(), Error> {
        self.connection.write_u8(b's').await?;
        self.connection.write_i32(4).await?;
        Ok(())
    }

    async fn write_parse_complete(&mut self) -> Result<(), Error> {
        self.connection.write_u8(b'1').await?;
        self.connection.write_i32(4).await?;
        Ok(())
    }

    async fn write_parameter_status(&mut self, ps: &ParameterStatus<'_>) -> Result<(), Error> {
        self.connection.write_u8(b'S').await?;
        let length = 4 + ps.name.len() + 1 + ps.value.len() + 1;
        self.connection.write_i32(length as i32).await?;
        self.connection.write_null_terminated_string(&ps.name).await?;
        self.connection.write_null_terminated_string(&ps.value).await?;
        Ok(())
    }

    async fn write_parameter_description(&mut self, dp: &ParameterDescription) -> Result<(), Error> {
        self.connection.write_u8(b't').await?;
        let length = 4 + 2 + dp.types.len() as i32 * 4;
        self.connection.write_i32(length).await?;
        self.connection.write_i16(dp.types.len() as i16).await?;
        for ty in &dp.types {
            self.connection.write_i32(*ty).await?;
        }
        Ok(())
    }

    async fn write_notification_response(&mut self, nr: &NotificationResponse<'_>) -> Result<(), Error> {
        self.connection.write_u8(b'A').await?;
        let length = 4 + 4 + nr.channel.len() + 1 + nr.payload.len() + 1;
        self.connection.write_i32(length as i32).await?;
        self.connection.write_i32(nr.process_id).await?;
        self.connection.write_null_terminated_string(&nr.channel).await?;
        self.connection.write_null_terminated_string(&nr.payload).await?;
        Ok(())
    }

    async fn write_no_data(&mut self) -> Result<(), Error> {
        self.connection.write_u8(b'n').await?;
        self.connection.write_i32(4).await?;
        Ok(())
    }

    async fn write_negotiate_protocol_version(&mut self, npv: &NegotiateProtocolVersion<'_>) -> Result<(), Error> {
        self.connection.write_u8(b'v').await?;
        let length = 4 + 4 + 4 + npv.protocol_options.iter().map(|s| s.len() + 1).sum::<usize>() as i32;
        self.connection.write_i32(length).await?;
        self.connection.write_i32(npv.newest_protocol_version).await?;
        self.connection.write_i32(npv.protocol_options.len() as i32).await?;
        for option in &npv.protocol_options {
            self.connection.write_all(option.as_bytes()).await?;
            self.connection.write_u8(0).await?;
        }
        Ok(())
    }

    async fn write_function_call_response(&mut self, fr: &FunctionCallResponse<'_>) -> Result<(), Error> {
        self.connection.write_u8(b'V').await?;
        let length = 4 + 4 + fr.value.map(|v| v.len()).unwrap_or(0) as i32;
        self.connection.write_i32(length).await?;
        if let Some(value) = &fr.value {
            self.connection.write_i32(value.len() as i32).await?;
            self.connection.write_all(value).await?;
        } else {
            self.connection.write_i32(-1).await?;
        }
        Ok(())
    }

    async fn write_notice_response(&mut self, er: &ErrorResponse<'_>) -> Result<(), Error> {
        self.connection.write_u8(b'N').await?;
        self.write_error_response_body(er).await?;
        Ok(())
    }

    async fn write_error_response(&mut self, er: &ErrorResponse<'_>) -> Result<(), Error> {
        self.connection.write_u8(b'E').await?;
        self.write_error_response_body(er).await?;
        Ok(())
    }

    async fn write_empty_query_response(&mut self) -> Result<(), Error> {
        self.connection.write_u8(b'I').await?;
        self.connection.write_i32(4).await?;
        Ok(())
    }

    async fn write_data_row(&mut self, dr: &DataRow<'_>) -> Result<(), Error> {
        self.connection.write_u8(b'D').await?;
        let length = 4
            + 2
            + dr.values
            .iter()
            .map(|c| {
                if let Some(c) = c {
                    4 + c.len() as i32
                } else {
                    4
                }
            })
            .sum::<i32>();
        self.connection.write_i32(length).await?;
        self.connection.write_i16(dr.values.len() as i16).await?;
        for column in &dr.values {
            if let Some(column) = column {
                self.connection.write_i32(column.len() as i32).await?;
                self.connection.write_all(column).await?;
            } else {
                self.connection.write_i32(-1).await?;
            }
        }
        Ok(())
    }

    async fn write_copy_both_response(&mut self, cr: &CopyResponse) -> Result<(), Error> {
        self.connection.write_u8(b'W').await?;
        self.write_copy_response(cr).await
    }

    async fn write_copy_out_response(&mut self, cr: &CopyResponse) -> Result<(), Error> {
        self.connection.write_u8(b'H').await?;
        self.write_copy_response(cr).await
    }

    async fn write_copy_in_response(&mut self, cr: &CopyResponse) -> Result<(), Error> {
        self.connection.write_u8(b'G').await?;
        self.write_copy_response(cr).await
    }

    async fn write_copy_done(&mut self) -> Result<(), Error> {
        self.connection.write_u8(b'c').await?;
        self.connection.write_i32(4).await?;
        Ok(())
    }

    async fn write_copy_data(&mut self, cd: &CopyData<'_>) -> Result<(), Error> {
        let length = 4 + cd.data.len() + 1;
        self.extend_buffer(length);
        self.read_buffer[0] = b'd';
        let bytes = (length as i32 - 1).to_be_bytes();
        self.read_buffer[1] = bytes[0];
        self.read_buffer[2] = bytes[1];
        self.read_buffer[3] = bytes[2];
        self.read_buffer[4] = bytes[3];
        self.read_buffer[5..5 + cd.data.len()].copy_from_slice(cd.data);
        // // self.connection.write_u8(b'd').await?;
        // self.connection.write_i32((length - 1) as i32).await?;
        self.connection.write_all(&self.read_buffer[..length]).await?;
        Ok(())
    }

    async fn write_command_complete(&mut self, cc: &CommandComplete<'_>) -> Result<(), Error> {
        self.connection.write_u8(b'C').await?;
        let length = 4 + cc.tag.len() + 1;
        self.connection.write_i32(length as i32).await?;
        self.connection.write_all(cc.tag.as_bytes()).await?;
        self.connection.write_u8(0).await?;
        Ok(())
    }

    async fn write_close_complete(&mut self) -> Result<(), Error> {
        self.connection.write_u8(b'3').await?;
        self.connection.write_i32(4).await?;
        Ok(())
    }

    async fn write_bind_complete(&mut self) -> Result<(), Error> {
        self.connection.write_u8(b'2').await?;
        self.connection.write_i32(4).await?;
        Ok(())
    }

    async fn write_backend_key_data(&mut self, bkd: &BackendKeyData) -> Result<(), Error> {
        self.connection.write_u8(b'K').await?;
        self.connection.write_i32(12).await?;
        self.connection.write_i32(bkd.process_id).await?;
        self.connection.write_i32(bkd.secret_key).await?;
        Ok(())
    }

    async fn write_authentication_sasl_final(&mut self, sasl: &AuthenticationSASLFinal<'_>) -> Result<(), Error> {
        self.connection.write_u8(b'R').await?;
        self.connection.write_i32(8 + sasl.outcome.len() as i32).await?;
        self.connection.write_i32(12).await?;
        self.connection.write_all(sasl.outcome).await?;
        Ok(())
    }

    async fn write_authentication_sasl_continue(&mut self, sasl: &AuthenticationSASLContinue<'_>) -> Result<(), Error> {
        self.connection.write_u8(b'R').await?;
        self.connection.write_i32(8 + sasl.data.len() as i32).await?;
        self.connection.write_i32(11).await?;
        self.connection.write_all(sasl.data).await?;
        Ok(())
    }

    async fn write_authentication_sasl(&mut self, sasl: &AuthenticationSASL<'_>) -> Result<(), Error> {
        self.connection.write_u8(b'R').await?;
        let length = 8 + sasl.mechanisms.iter().map(|s| s.len() + 1).sum::<usize>() as i32;
        self.connection.write_i32(length).await?;
        self.connection.write_i32(10).await?;
        for mechanism in &sasl.mechanisms {
            self.connection.write_all(mechanism.as_bytes()).await?;
            self.connection.write_u8(0).await?;
        }
        Ok(())
    }

    async fn write_authentication_sspi(&mut self) -> Result<(), Error> {
        self.connection.write_u8(b'R').await?;
        self.connection.write_i32(8).await?;
        self.connection.write_i32(9).await?;
        Ok(())
    }

    async fn write_authentication_gss_continue(&mut self, gss: &AuthenticationGSSContinue<'_>) -> Result<(), Error> {
        self.connection.write_u8(b'R').await?;
        self.connection.write_i32(8 + gss.data.len() as i32).await?;
        self.connection.write_i32(8).await?;
        self.connection.write_all(gss.data).await?;
        Ok(())
    }

    async fn write_authentication_gss(&mut self) -> Result<(), Error> {
        self.connection.write_u8(b'R').await?;
        self.connection.write_i32(8).await?;
        self.connection.write_i32(7).await?;
        Ok(())
    }

    async fn write_authentication_md5_password(&mut self, md5: &AuthenticationMD5Password) -> Result<(), Error> {
        self.connection.write_u8(b'R').await?;
        self.connection.write_i32(12).await?;
        self.connection.write_i32(5).await?;
        self.connection.write_all(&md5.salt).await?;
        Ok(())
    }

    async fn write_authentication_cleartext_password(&mut self) -> Result<(), Error> {
        self.connection.write_u8(b'R').await?;
        self.connection.write_i32(8).await?;
        self.connection.write_i32(3).await?;
        Ok(())
    }

    async fn write_authentication_kerberos_v5(&mut self) -> Result<(), Error> {
        self.connection.write_u8(b'R').await?;
        self.connection.write_i32(8).await?;
        self.connection.write_i32(2).await?;
        Ok(())
    }

    async fn write_authentication_ok(&mut self) -> Result<(), Error> {
        self.connection.write_u8(b'R').await?;
        self.connection.write_i32(8).await?;
        self.connection.write_i32(0).await?;
        Ok(())
    }

    async fn write_error_response_body(&mut self, er: &ErrorResponse<'_>) -> Result<(), Error> {
        let length = 4
            + er.fields
            .iter()
            .map(|f| 1 + f.value.len() as i32 + 1)
            .sum::<i32>()
            + 1;
        self.connection.write_i32(length).await?;
        for f in &er.fields {
            self.connection.write_u8(f.field_type).await?;
            self.connection.write_all(f.value.as_bytes()).await?;
            self.connection.write_u8(0).await?;
        }
        self.connection.write_u8(0).await?;
        Ok(())
    }

    async fn write_copy_response(&mut self, cir: &CopyResponse) -> Result<(), Error> {
        let len = 4 + 1 + 2 + (cir.column_formats.len() * 2) as i32;
        self.connection.write_i32(len).await?;

        self.connection
            .write_u8(match cir.format {
                ValueFormat::Text => 0,
                ValueFormat::Binary => 1,
            })
            .await?;

        self.connection
            .write_i16(cir.column_formats.len() as i16)
            .await?;

        for format in &cir.column_formats {
            self.connection
                .write_i16(match format {
                    ValueFormat::Text => 0,
                    ValueFormat::Binary => 1,
                })
                .await?;
        }

        Ok(())
    }

    pub async fn write_frontend_message(
        &mut self,
        message: &FrontendMessage<'_>,
    ) -> Result<(), std::io::Error> {
        match message {
            FrontendMessage::Bind(bind) => self.write_bind_message(bind).await,
            FrontendMessage::CancelRequest(cr) => {
                self.write_cancel_request(cr).await
            }
            FrontendMessage::Close(close) => {
                self.write_close(close).await
            }
            FrontendMessage::CopyData(cd) => {
                self.write_copy_data(cd).await
            }
            FrontendMessage::CopyDone => {
                self.write_copy_done().await
            }
            FrontendMessage::CopyFail(cf) => {
                self.write_copy_fail(cf).await
            },
            FrontendMessage::Describe(d) => {
                self.write_describe(d).await
            },
            FrontendMessage::Execute(e) => {
                self.write_execute(e).await
            },
            FrontendMessage::Flush => {
                self.write_flush().await
            },
            FrontendMessage::FunctionCall(fc) => {
                self.write_function_call(fc).await
            },
            FrontendMessage::GSSENCRequest => {
                self.write_gss_enc_request().await
            },
            FrontendMessage::FrontendPMessage(gr) => {
                self.write_frontend_p_message(gr).await
            },
            FrontendMessage::Parse(p) => {
                self.write_parse(&p).await
            },
            FrontendMessage::Query(q) => {
                self.write_query(q).await
            },
            FrontendMessage::SSLRequest => {
                self.write_ssl_request().await
            },
            FrontendMessage::StartupMessage(s) => {
                self.write_startup_message(s).await
            },
            FrontendMessage::Sync => {
                self.write_sync().await
            },
            FrontendMessage::Terminate => {
                self.write_terminate().await
            },
        }
    }

    async fn write_terminate(&mut self) -> Result<(), Error> {
        self.connection.write_u8(b'X').await?;
        self.connection.write_i32(4).await?;
        Ok(())
    }

    async fn write_sync(&mut self) -> Result<(), Error> {
        self.connection.write_u8(b'S').await?;
        self.connection.write_i32(4).await?;
        Ok(())
    }

    async fn write_startup_message(&mut self, s: &StartupMessage<'_>) -> Result<(), Error> {
        let length = 4 + 4 + s.parameters.iter().map(|p| p.name.len() + 1 + p.value.len() + 1).sum::<usize>() as i32 + 1;
        self.connection.write_i32(length).await?;
        self.connection.write_i32(196608).await?;
        for p in &s.parameters {
            self.connection.write_null_terminated_string(&p.name).await?;
            self.connection.write_null_terminated_string(&p.value).await?;
        }
        self.connection.write_u8(0).await?;
        Ok(())
    }

    async fn write_ssl_request(&mut self) -> Result<(), Error> {
        self.connection.write_i32(8).await?;
        self.connection.write_i32(80877103).await?;
        Ok(())
    }

    async fn write_query(&mut self, q: &Query<'_>) -> Result<(), Error> {
        self.connection.write_u8(b'Q').await?;
        let length = 4 + q.query.len() + 1;
        self.connection.write_i32(length as i32).await?;
        self.connection.write_null_terminated_string(&q.query).await?;
        Ok(())
    }

    async fn write_parse(&mut self, p: &Parse<'_>) -> Result<(), Error> {
        self.connection.write_u8(b'P').await?;
        let length = 4 + p.destination.len() + 1 + p.query.len() + 1 + 2 + p.parameter_types.len() * 4;
        self.connection.write_i32(length as i32).await?;
        self.connection.write_null_terminated_string(&p.destination).await?;
        self.connection.write_null_terminated_string(&p.query).await?;
        self.connection.write_i16(p.parameter_types.len() as i16).await?;
        for ty in &p.parameter_types {
            self.connection.write_i32(*ty).await?;
        }
        Ok(())
    }

    async fn write_frontend_p_message(&mut self, gr: &FrontendPMessage<'_>) -> Result<(), Error> {
        gr.write_to(&mut self.connection).await?;
        Ok(())
    }

    async fn write_gss_enc_request(&mut self) -> Result<(), Error> {
        self.connection.write_i32(8).await?;
        self.connection.write_i32(80877104).await?;
        Ok(())
    }

    async fn write_function_call(&mut self, fc: &FunctionCall<'_>) -> Result<(), Error> {
        self.connection.write_u8(b'F').await?;
        let length = 4 + 4 + 2 + (fc.argument_formats.len() as i32 * 2) + 2 + (fc.arguments.iter().map(|a| if let Some(a) = a {
            a.len() as i32
        } else {
            0
        } + 4).sum::<i32>()) + 2;
        self.connection.write_i32(length).await?;
        self.connection.write_i32(fc.object_id).await?;
        self.connection.write_i16(fc.argument_formats.len() as i16).await?;
        for format in &fc.argument_formats {
            self.connection.write_i16(match format {
                ValueFormat::Text => 0,
                ValueFormat::Binary => 1,
            }).await?;
        }
        self.connection.write_i16(fc.arguments.len() as i16).await?;
        for argument in &fc.arguments {
            match argument {
                Some(argument) => {
                    self.connection.write_i32(argument.len() as i32).await?;
                    self.connection.write_all(argument).await?;
                },
                None => {
                    self.connection.write_i32(-1).await?;
                },
            }
        }
        self.connection.write_i16(match fc.result_format {
            ValueFormat::Text => 0,
            ValueFormat::Binary => 1,
        }).await?;
        Ok(())
    }

    async fn write_flush(&mut self) -> Result<(), Error> {
        self.connection.write_u8(b'H').await?;
        self.connection.write_i32(4).await?;
        Ok(())
    }

    async fn write_execute(&mut self, e: &Execute<'_>) -> Result<(), Error> {
        self.connection.write_u8(b'E').await?;
        let length = 4 + e.portal_name.len() + 1 + 4;
        self.connection.write_i32(length as i32).await?;
        self.connection.write_all(e.portal_name.as_bytes()).await?;
        self.connection.write_u8(0).await?;
        self.connection.write_i32(e.max_rows).await?;
        Ok(())
    }

    async fn write_describe(&mut self, d: &Describe<'_>) -> Result<(), Error> {
        self.connection.write_u8(b'D').await?;
        self.connection.write_i32(4 + 1 + d.name.len() as i32 + 1).await?;
        self.connection.write_u8(match d.target {
            DescribeTarget::Statement => b'S',
            DescribeTarget::Portal => b'P',
        }).await?;
        self.connection.write_all(d.name.as_bytes()).await?;
        self.connection.write_u8(0).await?;
        Ok(())
    }

    async fn write_copy_fail(&mut self, cf: &CopyFail<'_>) -> Result<(), Error> {
        self.connection.write_u8(b'f').await?;
        self.connection
            .write_i32(4 + cf.message.len() as i32 + 1)
            .await?;
        self.connection.write_all(cf.message.as_bytes()).await?;
        self.connection.write_u8(0).await?;
        Ok(())
    }

    async fn write_close(&mut self, close: &Close<'_>) -> Result<(), Error> {
        self.connection.write_u8(b'C').await?;
        self.connection
            .write_i32(4 + 1 + close.name.len() as i32 + 1)
            .await?;
        self.connection
            .write_u8(match close.target {
                CloseType::Statement => b'S',
                CloseType::Portal => b'P',
            })
            .await?;
        self.connection.write_all(close.name.as_bytes()).await?;
        self.connection.write_u8(0).await?;
        Ok(())
    }

    async fn write_cancel_request(&mut self, cr: &CancelRequest) -> Result<(), Error> {
        self.connection.write_i32(16).await?;
        self.connection.write_i32(80877102).await?;
        self.connection.write_i32(cr.process_id).await?;
        self.connection.write_i32(cr.secret_key).await?;
        Ok(())
    }

    async fn write_bind_message(&mut self, bind: &Bind<'_>) -> Result<(), Error> {
        self.connection.write_u8(b'B').await?;
        let length = size_of::<i32>()
            + bind.destination_portal_name.len()
            + size_of::<u8>()
            + bind.source_statement_name.len()
            + size_of::<u8>()
            + size_of::<i16>()
            + bind.parameter_formats.len() * size_of::<i16>()
            + size_of::<i16>()
            + bind
            .parameter_values
            .iter()
            .map(|v| v.map(|v| v.len()).unwrap_or(0))
            .sum::<usize>()
            + bind.parameter_values.len() * size_of::<i32>()
            + size_of::<i16>()
            + bind.result_column_formats.len() * size_of::<i16>();
        self.connection.write_i32(length as i32).await?;
        self.connection
            .write_all(bind.destination_portal_name.as_bytes())
            .await?;
        self.connection.write_u8(0).await?;
        self.connection
            .write_all(bind.source_statement_name.as_bytes())
            .await?;
        self.connection.write_u8(0).await?;
        self.connection
            .write_i16(bind.parameter_formats.len() as i16)
            .await?;
        for format in &bind.parameter_formats {
            self.connection
                .write_i16(match format {
                    ValueFormat::Text => 0,
                    ValueFormat::Binary => 1,
                })
                .await?;
        }
        self.connection
            .write_i16(bind.parameter_values.len() as i16)
            .await?;
        for value in &bind.parameter_values {
            match value {
                Some(value) => {
                    self.connection.write_i32(value.len() as i32).await?;
                    self.connection.write_all(value).await?;
                }
                None => {
                    self.connection.write_i32(-1).await?;
                }
            }
        }
        self.connection
            .write_i16(bind.result_column_formats.len() as i16)
            .await?;
        for format in &bind.result_column_formats {
            self.connection
                .write_i16(match format {
                    ValueFormat::Text => 0,
                    ValueFormat::Binary => 1,
                })
                .await?;
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> std::io::Result<()> {
        self.connection.flush().await
    }
}

use crate::protocol::io_extensions::AsyncWriteExt2;
use crate::protocol::messages::{BackendMessage, Bind, CloseType, CopyResponse, DescribeTarget, ErrorResponse, FrontendMessage};
use futures::{AsyncBufRead, AsyncRead, AsyncWrite, AsyncWriteExt};
use std::io::Error;
use crate::protocol::postgres_connection::PostgresConnection;
use crate::protocol::{CurrentTransactionStatus, ValueFormat};

impl<C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin> PostgresConnection<C> {


    pub async fn write_backend_message(
        &mut self,
        message: &BackendMessage<'_>,
    ) -> Result<(), std::io::Error> {
        match message {
            BackendMessage::AuthenticationOk => {
                self.connection.write_u8(b'R').await?;
                self.connection.write_i32(8).await?;
                self.connection.write_i32(0).await?;
                Ok(())
            }
            BackendMessage::AuthenticationKerberosV5 => {
                self.connection.write_u8(b'R').await?;
                self.connection.write_i32(8).await?;
                self.connection.write_i32(2).await?;
                Ok(())
            }
            BackendMessage::AuthenticationCleartextPassword => {
                self.connection.write_u8(b'R').await?;
                self.connection.write_i32(8).await?;
                self.connection.write_i32(3).await?;
                Ok(())
            }
            BackendMessage::AuthenticationMD5Password(md5) => {
                self.connection.write_u8(b'R').await?;
                self.connection.write_i32(12).await?;
                self.connection.write_i32(5).await?;
                self.connection.write_all(&md5.salt).await?;
                Ok(())
            }
            BackendMessage::AuthenticationGSS => {
                self.connection.write_u8(b'R').await?;
                self.connection.write_i32(8).await?;
                self.connection.write_i32(7).await?;
                Ok(())
            }
            BackendMessage::AuthenticationGSSContinue(gss) => {
                self.connection.write_u8(b'R').await?;
                self.connection.write_i32(8 + gss.data.len() as i32).await?;
                self.connection.write_i32(8).await?;
                self.connection.write_all(gss.data).await?;
                Ok(())
            }
            BackendMessage::AuthenticationSSPI => {
                self.connection.write_u8(b'R').await?;
                self.connection.write_i32(8).await?;
                self.connection.write_i32(9).await?;
                Ok(())
            }
            BackendMessage::AuthenticationSASL(sasl) => {
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
            BackendMessage::AuthenticationSASLContinue(sasl) => {
                self.connection.write_u8(b'R').await?;
                self.connection.write_i32(8 + sasl.data.len() as i32).await?;
                self.connection.write_i32(11).await?;
                self.connection.write_all(sasl.data).await?;
                Ok(())
            }
            BackendMessage::AuthenticationSASLFinal(sasl) => {
                self.connection.write_u8(b'R').await?;
                self.connection.write_i32(8 + sasl.outcome.len() as i32).await?;
                self.connection.write_i32(12).await?;
                self.connection.write_all(sasl.outcome).await?;
                Ok(())
            }
            BackendMessage::BackendKeyData(bkd) => {
                self.connection.write_u8(b'K').await?;
                self.connection.write_i32(12).await?;
                self.connection.write_i32(bkd.process_id).await?;
                self.connection.write_i32(bkd.secret_key).await?;
                Ok(())
            }
            BackendMessage::BindComplete => {
                self.connection.write_u8(b'2').await?;
                self.connection.write_i32(4).await?;
                Ok(())
            }
            BackendMessage::CloseComplete => {
                self.connection.write_u8(b'3').await?;
                self.connection.write_i32(4).await?;
                Ok(())
            }
            BackendMessage::CommandComplete(cc) => {
                self.connection.write_u8(b'C').await?;
                let length = 4 + cc.tag.len() + 1;
                self.connection.write_i32(length as i32).await?;
                self.connection.write_all(cc.tag.as_bytes()).await?;
                self.connection.write_u8(0).await?;
                Ok(())
            }
            BackendMessage::CopyData(cd) => {
                self.connection.write_u8(b'd').await?;
                let length = 4 + cd.data.len() as i32;
                self.connection.write_i32(length).await?;
                self.connection.write_all(cd.data).await?;
                Ok(())
            }
            BackendMessage::CopyDone => {
                self.connection.write_u8(b'c').await?;
                self.connection.write_i32(4).await?;
                Ok(())
            }
            BackendMessage::CopyInResponse(cr) => {
                self.connection.write_u8(b'G').await?;
                self.write_copy_response(cr).await
            }
            BackendMessage::CopyOutResponse(cr) => {
                self.connection.write_u8(b'H').await?;
                self.write_copy_response(cr).await
            }
            BackendMessage::CopyBothResponse(cr) => {
                self.connection.write_u8(b'W').await?;
                self.write_copy_response(cr).await
            }
            BackendMessage::DataRow(dr) => {
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
            },
            BackendMessage::EmptyQueryResponse => {
                self.connection.write_u8(b'I').await?;
                self.connection.write_i32(4).await?;
                Ok(())
            },
            BackendMessage::ErrorResponse(er) => {
                self.connection.write_u8(b'E').await?;
                self.write_error_response_body(&er).await?;
                Ok(())
            },
            BackendMessage::NoticeResponse(er) => {
                self.connection.write_u8(b'N').await?;
                self.write_error_response_body(&er).await?;
                Ok(())
            },
            BackendMessage::FunctionCallResponse(fr) => {
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
            },
            BackendMessage::NegotiateProtocolVersion(npv) => {
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
            },
            BackendMessage::NoData => {
                self.connection.write_u8(b'n').await?;
                self.connection.write_i32(4).await?;
                Ok(())
            },
            BackendMessage::NotificationResponse(nr) => {
                self.connection.write_u8(b'A').await?;
                let length = 4 + 4 + nr.channel.len() + 1 + nr.payload.len() + 1;
                self.connection.write_i32(length as i32).await?;
                self.connection.write_i32(nr.process_id).await?;
                self.connection.write_null_terminated_string(&nr.channel).await?;
                self.connection.write_null_terminated_string(&nr.payload).await?;
                Ok(())
            },
            BackendMessage::ParameterDescription(dp) => {
                self.connection.write_u8(b't').await?;
                let length = 4 + 2 + dp.types.len() as i32 * 4;
                self.connection.write_i32(length).await?;
                self.connection.write_i16(dp.types.len() as i16).await?;
                for ty in &dp.types {
                    self.connection.write_i32(*ty).await?;
                }
                Ok(())
            },
            BackendMessage::ParameterStatus(ps) => {
                self.connection.write_u8(b'S').await?;
                let length = 4 + ps.name.len() + 1 + ps.value.len() + 1;
                self.connection.write_i32(length as i32).await?;
                self.connection.write_null_terminated_string(&ps.name).await?;
                self.connection.write_null_terminated_string(&ps.value).await?;
                Ok(())
            },
            BackendMessage::ParseComplete => {
                self.connection.write_u8(b'1').await?;
                self.connection.write_i32(4).await?;
                Ok(())
            },
            BackendMessage::PortalSuspended => {
                self.connection.write_u8(b's').await?;
                self.connection.write_i32(4).await?;
                Ok(())
            },
            BackendMessage::ReadyForQuery(q) => {
                self.connection.write_u8(b'Z').await?;
                self.connection.write_i32(5).await?;
                self.connection.write_u8(match q.current_transaction_status {
                    CurrentTransactionStatus::Idle => b'I',
                    CurrentTransactionStatus::InTransaction => b'T',
                    CurrentTransactionStatus::InFailedTransaction => b'E',
                }).await?;
                Ok(())
            },
            BackendMessage::RowDescription(rd) => {
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
        }
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
                self.connection.write_i32(16).await?;
                self.connection.write_i32(80877102).await?;
                self.connection.write_i32(cr.process_id).await?;
                self.connection.write_i32(cr.secret_key).await?;
                Ok(())
            }
            FrontendMessage::Close(close) => {
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
            FrontendMessage::CopyData(cd) => {
                self.connection.write_u8(b'd').await?;
                let length = 4 + cd.data.len() as i32;
                self.connection.write_i32(length).await?;
                self.connection.write_all(cd.data).await?;
                Ok(())
            }
            FrontendMessage::CopyDone => {
                self.connection.write_u8(b'c').await?;
                self.connection.write_i32(4).await?;
                Ok(())
            }
            FrontendMessage::CopyFail(cf) => {
                self.connection.write_u8(b'f').await?;
                self.connection
                    .write_i32(4 + cf.message.len() as i32 + 1)
                    .await?;
                self.connection.write_all(cf.message.as_bytes()).await?;
                self.connection.write_u8(0).await?;
                Ok(())
            },
            FrontendMessage::Describe(d) => {
                self.connection.write_u8(b'D').await?;
                self.connection.write_i32(4 + 1 + d.name.len() as i32 + 1).await?;
                self.connection.write_u8(match d.target {
                    DescribeTarget::Statement => b'S',
                    DescribeTarget::Portal => b'P',
                }).await?;
                self.connection.write_all(d.name.as_bytes()).await?;
                self.connection.write_u8(0).await?;
                Ok(())
            },
            FrontendMessage::Execute(e) => {
                self.connection.write_u8(b'E').await?;
                let length = 4 + e.portal_name.len() + 1 + 4;
                self.connection.write_i32(length as i32).await?;
                self.connection.write_all(e.portal_name.as_bytes()).await?;
                self.connection.write_u8(0).await?;
                self.connection.write_i32(e.max_rows).await?;
                Ok(())
            },
            FrontendMessage::Flush => {
                self.connection.write_u8(b'H').await?;
                self.connection.write_i32(4).await?;
                Ok(())
            },
            FrontendMessage::FunctionCall(fc) => {
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
            },
            FrontendMessage::GSSENCRequest => {
                self.connection.write_i32(8).await?;
                self.connection.write_i32(80877104).await?;
                Ok(())
            },
            FrontendMessage::FrontendPMessage(gr) => {
                gr.write_to(&mut self.connection).await?;
                Ok(())
            },
            FrontendMessage::Parse(p) => {
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
            },
            FrontendMessage::Query(q) => {
                self.connection.write_u8(b'Q').await?;
                let length = 4 + q.query.len() + 1;
                self.connection.write_i32(length as i32).await?;
                self.connection.write_null_terminated_string(&q.query).await?;
                Ok(())
            },
            FrontendMessage::SSLRequest => {
                self.connection.write_i32(8).await?;
                self.connection.write_i32(80877103).await?;
                Ok(())
            },
            FrontendMessage::StartupMessage(s) => {
                let length = 4 + 4 + s.parameters.iter().map(|p| p.name.len() + 1 + p.value.len() + 1).sum::<usize>() as i32 + 1;
                self.connection.write_i32(length).await?;
                self.connection.write_i32(196608).await?;
                for p in &s.parameters {
                    self.connection.write_null_terminated_string(&p.name).await?;
                    self.connection.write_null_terminated_string(&p.value).await?;
                }
                self.connection.write_u8(0).await?;
                Ok(())
            },
            FrontendMessage::Sync => {
                self.connection.write_u8(b'S').await?;
                self.connection.write_i32(4).await?;
                Ok(())
            },
            FrontendMessage::Terminate => {
                self.connection.write_u8(b'X').await?;
                self.connection.write_i32(4).await?;
                Ok(())
            },
        }
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

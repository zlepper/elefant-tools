use crate::protocol::messages::{BackendMessage, Bind, CloseType, CopyResponse, DescribeTarget, ErrorResponse, FrontendMessage};
use crate::protocol::async_io::ElefantAsyncReadWrite;
use crate::protocol::postgres_connection::PostgresConnection;
use crate::protocol::{AuthenticationGSSContinue, AuthenticationMD5Password, AuthenticationSASL, AuthenticationSASLContinue, AuthenticationSASLFinal, BackendKeyData, CancelRequest, Close, CommandComplete, CopyData, CopyFail, CurrentTransactionStatus, DataRow, Describe, Execute, FrontendPMessage, FunctionCall, FunctionCallResponse, NegotiateProtocolVersion, NotificationResponse, ParameterDescription, ParameterStatus, Parse, Query, ReadyForQuery, RowDescription, StartupMessage, ValueFormat};
use crate::protocol::frame_reader::{ByteSliceWriter, Encoder};

struct PostgresMessageEncoder<'a, 'b> {
    destination: &'b mut ByteSliceWriter<'a>,
}

impl<'a, 'b> PostgresMessageEncoder<'a, 'b> {
    fn encode_backend_message(mut self, message: &BackendMessage<'_>) -> Result<(), std::io::Error> {
        match message {
            BackendMessage::AuthenticationOk => {
                self.write_authentication_ok()
            }
            BackendMessage::AuthenticationKerberosV5 => {
                self.write_authentication_kerberos_v5()
            }
            BackendMessage::AuthenticationCleartextPassword => {
                self.write_authentication_cleartext_password()
            }
            BackendMessage::AuthenticationMD5Password(md5) => {
                self.write_authentication_md5_password(md5)
            }
            BackendMessage::AuthenticationGSS => {
                self.write_authentication_gss()
            }
            BackendMessage::AuthenticationGSSContinue(gss) => {
                self.write_authentication_gss_continue(gss)
            }
            BackendMessage::AuthenticationSSPI => {
                self.write_authentication_sspi()
            }
            BackendMessage::AuthenticationSASL(sasl) => {
                self.write_authentication_sasl(sasl)
            }
            BackendMessage::AuthenticationSASLContinue(sasl) => {
                self.write_authentication_sasl_continue(sasl)
            }
            BackendMessage::AuthenticationSASLFinal(sasl) => {
                self.write_authentication_sasl_final(sasl)
            }
            BackendMessage::BackendKeyData(bkd) => {
                self.write_backend_key_data(bkd)
            }
            BackendMessage::BindComplete => {
                self.write_bind_complete()
            }
            BackendMessage::CloseComplete => {
                self.write_close_complete()
            }
            BackendMessage::CommandComplete(cc) => {
                self.write_command_complete(cc)
            }
            BackendMessage::CopyData(cd) => {
                self.write_copy_data(cd)
            }
            BackendMessage::CopyDone => {
                self.write_copy_done()
            }
            BackendMessage::CopyInResponse(cr) => {
                self.write_copy_in_response(cr)
            }
            BackendMessage::CopyOutResponse(cr) => {
                self.write_copy_out_response(cr)
            }
            BackendMessage::CopyBothResponse(cr) => {
                self.write_copy_both_response(cr)
            }
            BackendMessage::DataRow(dr) => {
                self.write_data_row(dr)
            },
            BackendMessage::EmptyQueryResponse => {
                self.write_empty_query_response()
            },
            BackendMessage::ErrorResponse(er) => {
                self.write_error_response(er)
            },
            BackendMessage::NoticeResponse(er) => {
                self.write_notice_response(er)
            },
            BackendMessage::FunctionCallResponse(fr) => {
                self.write_function_call_response(fr)
            },
            BackendMessage::NegotiateProtocolVersion(npv) => {
                self.write_negotiate_protocol_version(npv)
            },
            BackendMessage::NoData => {
                self.write_no_data()
            },
            BackendMessage::NotificationResponse(nr) => {
                self.write_notification_response(nr)
            },
            BackendMessage::ParameterDescription(dp) => {
                self.write_parameter_description(dp)
            },
            BackendMessage::ParameterStatus(ps) => {
                self.write_parameter_status(ps)
            },
            BackendMessage::ParseComplete => {
                self.write_parse_complete()
            },
            BackendMessage::PortalSuspended => {
                self.write_portal_suspended()
            },
            BackendMessage::ReadyForQuery(q) => {
                self.write_ready_for_query(q)
            },
            BackendMessage::RowDescription(rd) => {
                self.write_row_description(rd)
            }
        }

        Ok(())
    }


    fn write_row_description(self, rd: &RowDescription) {
        self.destination.write_u8(b'T');
        let length = 4 + 2 + rd.fields.iter().map(|f| f.name.len() + 1 + 4 + 2 + 4 + 2 + 4 + 2).sum::<usize>() as i32;
        self.destination.write_i32(length);
        self.destination.write_i16(rd.fields.len() as i16);
        for field in &rd.fields {
            self.destination.write_null_terminated_string(&field.name);
            self.destination.write_i32(field.table_oid);
            self.destination.write_i16(field.column_attribute_number);
            self.destination.write_i32(field.data_type_oid);
            self.destination.write_i16(field.data_type_size);
            self.destination.write_i32(field.type_modifier);
            self.destination.write_i16(match field.format {
                ValueFormat::Text => 0,
                ValueFormat::Binary => 1
            });
        }
    }

    fn write_ready_for_query(self, q: &ReadyForQuery) {
        self.destination.write_u8(b'Z');
        self.destination.write_i32(5);
        self.destination.write_u8(match q.current_transaction_status {
            CurrentTransactionStatus::Idle => b'I',
            CurrentTransactionStatus::InTransaction => b'T',
            CurrentTransactionStatus::InFailedTransaction => b'E',
        });
    }

    fn write_portal_suspended(self) {
        self.destination.write_u8(b's');
        self.destination.write_i32(4);
    }

    fn write_parse_complete(&mut self) {
        self.destination.write_u8(b'1');
        self.destination.write_i32(4);
    }

    fn write_parameter_status(&mut self, ps: &ParameterStatus<'_>) {
        self.destination.write_u8(b'S');
        let length = 4 + ps.name.len() + 1 + ps.value.len() + 1;
        self.destination.write_i32(length as i32);
        self.destination.write_null_terminated_string(&ps.name);
        self.destination.write_null_terminated_string(&ps.value);
    }

    fn write_parameter_description(&mut self, dp: &ParameterDescription) {
        self.destination.write_u8(b't');
        let length = 4 + 2 + dp.types.len() as i32 * 4;
        self.destination.write_i32(length);
        self.destination.write_i16(dp.types.len() as i16);
        for ty in &dp.types {
            self.destination.write_i32(*ty);
        }
    }

    fn write_notification_response(&mut self, nr: &NotificationResponse<'_>)  {
        self.destination.write_u8(b'A');
        let length = 4 + 4 + nr.channel.len() + 1 + nr.payload.len() + 1;
        self.destination.write_i32(length as i32);
        self.destination.write_i32(nr.process_id);
        self.destination.write_null_terminated_string(&nr.channel);
        self.destination.write_null_terminated_string(&nr.payload);
    }

    fn write_no_data(&mut self) {
        self.destination.write_u8(b'n');
        self.destination.write_i32(4);
    }

    fn write_negotiate_protocol_version(&mut self, npv: &NegotiateProtocolVersion<'_>) {
        self.destination.write_u8(b'v');
        let length = 4 + 4 + 4 + npv.protocol_options.iter().map(|s| s.len() + 1).sum::<usize>() as i32;
        self.destination.write_i32(length);
        self.destination.write_i32(npv.newest_protocol_version);
        self.destination.write_i32(npv.protocol_options.len() as i32);
        for option in &npv.protocol_options {
            self.destination.write_bytes(option.as_bytes());
            self.destination.write_u8(0);
        }
    }

    fn write_function_call_response(&mut self, fr: &FunctionCallResponse<'_>) {
        self.destination.write_u8(b'V');
        let length = 4 + 4 + fr.value.map(|v| v.len()).unwrap_or(0) as i32;
        self.destination.write_i32(length);
        if let Some(value) = &fr.value {
            self.destination.write_i32(value.len() as i32);
            self.destination.write_bytes(value);
        } else {
            self.destination.write_i32(-1);
        }
    }

    fn write_notice_response(&mut self, er: &ErrorResponse<'_>) {
        self.destination.write_u8(b'N');
        self.write_error_response_body(er);
    }

    fn write_error_response(&mut self, er: &ErrorResponse<'_>) {
        self.destination.write_u8(b'E');
        self.write_error_response_body(er);
    }

    fn write_empty_query_response(&mut self) {
        self.destination.write_u8(b'I');
        self.destination.write_i32(4);
    }

    fn write_data_row(&mut self, dr: &DataRow<'_>) {
        self.destination.write_u8(b'D');
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
        self.destination.write_i32(length);
        self.destination.write_i16(dr.values.len() as i16);
        for column in &dr.values {
            if let Some(column) = column {
                self.destination.write_i32(column.len() as i32);
                self.destination.write_bytes(column);
            } else {
                self.destination.write_i32(-1);
            }
        }
    }

    fn write_copy_both_response(self, cr: &CopyResponse)  {
        self.destination.write_u8(b'W');
        self.write_copy_response(cr);
    }

    fn write_copy_out_response(self, cr: &CopyResponse) {
        self.destination.write_u8(b'H');
        self.write_copy_response(cr)
    }

    fn write_copy_in_response(self, cr: &CopyResponse) {
        self.destination.write_u8(b'G');
        self.write_copy_response(cr)
    }

    fn write_copy_done(self) {
        self.destination.write_u8(b'c');
        self.destination.write_i32(4);
    }

    fn write_copy_data(self, cd: &CopyData<'_>) {
        self.destination.write_u8(b'd');
        let length = 4 + cd.data.len();
        self.destination.write_i32(length as i32);
        self.destination.write_bytes(cd.data);
    }

    fn write_command_complete(&mut self, cc: &CommandComplete<'_>) {
        self.destination.write_u8(b'C');
        let length = 4 + cc.tag.len() + 1;
        self.destination.write_i32(length as i32);
        self.destination.write_bytes(cc.tag.as_bytes());
        self.destination.write_u8(0);
    }

    fn write_close_complete(&mut self) {
        self.destination.write_u8(b'3');
        self.destination.write_i32(4);
    }

    fn write_bind_complete(&mut self) {
        self.destination.write_u8(b'2');
        self.destination.write_i32(4);
    }

    fn write_backend_key_data(&mut self, bkd: &BackendKeyData) {
        self.destination.write_u8(b'K');
        self.destination.write_i32(12);
        self.destination.write_i32(bkd.process_id);
        self.destination.write_i32(bkd.secret_key);
    }

    fn write_authentication_sasl_final(&mut self, sasl: &AuthenticationSASLFinal<'_>)  {
        self.destination.write_u8(b'R');
        self.destination.write_i32(8 + sasl.outcome.len() as i32);
        self.destination.write_i32(12);
        self.destination.write_bytes(sasl.outcome);
    }

    fn write_authentication_sasl_continue(&mut self, sasl: &AuthenticationSASLContinue<'_>) {
        self.destination.write_u8(b'R');
        self.destination.write_i32(8 + sasl.data.len() as i32);
        self.destination.write_i32(11);
        self.destination.write_bytes(sasl.data);
    }

    fn write_authentication_sasl(&mut self, sasl: &AuthenticationSASL<'_>) {
        self.destination.write_u8(b'R');
        let length = 8 + sasl.mechanisms.iter().map(|s| s.len() + 1).sum::<usize>() as i32;
        self.destination.write_i32(length);
        self.destination.write_i32(10);
        for mechanism in &sasl.mechanisms {
            self.destination.write_bytes(mechanism.as_bytes());
            self.destination.write_u8(0);
        }
    }

    fn write_authentication_sspi(&mut self) {
        self.destination.write_u8(b'R');
        self.destination.write_i32(8);
        self.destination.write_i32(9);
    }

    fn write_authentication_gss_continue(&mut self, gss: &AuthenticationGSSContinue<'_>)  {
        self.destination.write_u8(b'R');
        self.destination.write_i32(8 + gss.data.len() as i32);
        self.destination.write_i32(8);
        self.destination.write_bytes(gss.data);
    }

    fn write_authentication_gss(&mut self) {
        self.destination.write_u8(b'R');
        self.destination.write_i32(8);
        self.destination.write_i32(7);
    }

    fn write_authentication_md5_password(&mut self, md5: &AuthenticationMD5Password) {
        self.destination.write_u8(b'R');
        self.destination.write_i32(12);
        self.destination.write_i32(5);
        self.destination.write_bytes(&md5.salt);
    }

    fn write_authentication_cleartext_password(&mut self) {
        self.destination.write_u8(b'R');
        self.destination.write_i32(8);
        self.destination.write_i32(3);
    }

    fn write_authentication_kerberos_v5(&mut self) {
        self.destination.write_u8(b'R');
        self.destination.write_i32(8);
        self.destination.write_i32(2);
    }

    fn write_authentication_ok(&mut self) {
        self.destination.write_u8(b'R');
        self.destination.write_i32(8);
        self.destination.write_i32(0);
    }

    fn write_error_response_body(&mut self, er: &ErrorResponse<'_>)  {
        let length = 4
            + er.fields
            .iter()
            .map(|f| 1 + f.value.len() as i32 + 1)
            .sum::<i32>()
            + 1;
        self.destination.write_i32(length);
        for f in &er.fields {
            self.destination.write_u8(f.field_type);
            self.destination.write_bytes(f.value.as_bytes());
            self.destination.write_u8(0);
        }
        self.destination.write_u8(0);
    }

    fn write_copy_response(self, cir: &CopyResponse)  {
        let len = 4 + 1 + 2 + (cir.column_formats.len() * 2) as i32;
        self.destination.write_i32(len);

        self.destination
            .write_u8(match cir.format {
                ValueFormat::Text => 0,
                ValueFormat::Binary => 1,
            });

        self.destination.write_i16(cir.column_formats.len() as i16);

        for format in &cir.column_formats {
            self.destination
                .write_i16(match format {
                    ValueFormat::Text => 0,
                    ValueFormat::Binary => 1,
                });
        }
    }

    fn encode_frontend_message(mut self, message: &FrontendMessage<'_>) {

        match message {
            FrontendMessage::Bind(bind) => self.write_bind_message(bind),
            FrontendMessage::CancelRequest(cr) => {
                self.write_cancel_request(cr)
            }
            FrontendMessage::Close(close) => {
                self.write_close(close)
            }
            FrontendMessage::CopyData(cd) => {
                self.write_copy_data(cd)
            }
            FrontendMessage::CopyDone => {
                self.write_copy_done()
            }
            FrontendMessage::CopyFail(cf) => {
                self.write_copy_fail(cf)
            },
            FrontendMessage::Describe(d) => {
                self.write_describe(d)
            },
            FrontendMessage::Execute(e) => {
                self.write_execute(e)
            },
            FrontendMessage::Flush => {
                self.write_flush()
            },
            FrontendMessage::FunctionCall(fc) => {
                self.write_function_call(fc)
            },
            FrontendMessage::GSSENCRequest => {
                self.write_gss_enc_request()
            },
            FrontendMessage::FrontendPMessage(gr) => {
                self.write_frontend_p_message(gr)
            },
            FrontendMessage::Parse(p) => {
                self.write_parse(p)
            },
            FrontendMessage::Query(q) => {
                self.write_query(q)
            },
            FrontendMessage::SSLRequest => {
                self.write_ssl_request()
            },
            FrontendMessage::StartupMessage(s) => {
                self.write_startup_message(s)
            },
            FrontendMessage::Sync => {
                self.write_sync()
            },
            FrontendMessage::Terminate => {
                self.write_terminate()
            },
        }

    }


    fn write_terminate(&mut self) {
        self.destination.write_u8(b'X');
        self.destination.write_i32(4);
    }

    fn write_sync(&mut self) {
        self.destination.write_u8(b'S');
        self.destination.write_i32(4);
    }

    fn write_startup_message(&mut self, s: &StartupMessage<'_>) {
        let length = 4 + 4 + s.parameters.iter().map(|p| p.name.len() + 1 + p.value.len() + 1).sum::<usize>() as i32 + 1;
        self.destination.write_i32(length);
        self.destination.write_i32(196608);
        for p in &s.parameters {
            self.destination.write_null_terminated_string(&p.name);
            self.destination.write_null_terminated_string(&p.value);
        }
        self.destination.write_u8(0);
    }

    fn write_ssl_request(&mut self) {
        self.destination.write_i32(8);
        self.destination.write_i32(80877103);
    }

    fn write_query(&mut self, q: &Query<'_>) {
        self.destination.write_u8(b'Q');
        let length = 4 + q.query.len() + 1;
        self.destination.write_i32(length as i32);
        self.destination.write_null_terminated_string(&q.query);
    }

    fn write_parse(&mut self, p: &Parse<'_>) {
        self.destination.write_u8(b'P');
        let length = 4 + p.destination.len() + 1 + p.query.len() + 1 + 2 + p.parameter_types.len() * 4;
        self.destination.write_i32(length as i32);
        self.destination.write_null_terminated_string(&p.destination);
        self.destination.write_null_terminated_string(&p.query);
        self.destination.write_i16(p.parameter_types.len() as i16);
        for ty in &p.parameter_types {
            self.destination.write_i32(*ty);
        }
    }

    fn write_frontend_p_message(&mut self, gr: &FrontendPMessage<'_>) {
        gr.write_to(self.destination);
    }

    fn write_gss_enc_request(&mut self) {
        self.destination.write_i32(8);
        self.destination.write_i32(80877104);
    }

    fn write_function_call(&mut self, fc: &FunctionCall<'_>) {
        self.destination.write_u8(b'F');
        let length = 4 + 4 + 2 + (fc.argument_formats.len() as i32 * 2) + 2 + (fc.arguments.iter().map(|a| if let Some(a) = a {
            a.len() as i32
        } else {
            0
        } + 4).sum::<i32>()) + 2;
        self.destination.write_i32(length);
        self.destination.write_i32(fc.object_id);
        self.destination.write_i16(fc.argument_formats.len() as i16);
        for format in &fc.argument_formats {
            self.destination.write_i16(match format {
                ValueFormat::Text => 0,
                ValueFormat::Binary => 1,
            });
        }
        self.destination.write_i16(fc.arguments.len() as i16);
        for argument in &fc.arguments {
            match argument {
                Some(argument) => {
                    self.destination.write_i32(argument.len() as i32);
                    self.destination.write_bytes(argument);
                },
                None => {
                    self.destination.write_i32(-1);
                },
            }
        }
        self.destination.write_i16(match fc.result_format {
            ValueFormat::Text => 0,
            ValueFormat::Binary => 1,
        });
    }

    fn write_flush(&mut self)  {
        self.destination.write_u8(b'H');
        self.destination.write_i32(4);
    }

    fn write_execute(&mut self, e: &Execute<'_>) {
        self.destination.write_u8(b'E');
        let length = 4 + e.portal_name.len() + 1 + 4;
        self.destination.write_i32(length as i32);
        self.destination.write_bytes(e.portal_name.as_bytes());
        self.destination.write_u8(0);
        self.destination.write_i32(e.max_rows);
    }

    fn write_describe(&mut self, d: &Describe<'_>) {
        self.destination.write_u8(b'D');
        self.destination.write_i32(4 + 1 + d.name.len() as i32 + 1);
        self.destination.write_u8(match d.target {
            DescribeTarget::Statement => b'S',
            DescribeTarget::Portal => b'P',
        });
        self.destination.write_bytes(d.name.as_bytes());
        self.destination.write_u8(0);
    }

    fn write_copy_fail(&mut self, cf: &CopyFail<'_>) {
        self.destination.write_u8(b'f');
        self.destination
            .write_i32(4 + cf.message.len() as i32 + 1);
        self.destination.write_bytes(cf.message.as_bytes());
        self.destination.write_u8(0);
    }

    fn write_close(&mut self, close: &Close<'_>){
        self.destination.write_u8(b'C');
        self.destination
            .write_i32(4 + 1 + close.name.len() as i32 + 1);
        self.destination
            .write_u8(match close.target {
                CloseType::Statement => b'S',
                CloseType::Portal => b'P',
            });
        self.destination.write_bytes(close.name.as_bytes());
        self.destination.write_u8(0);
    }

    fn write_cancel_request(&mut self, cr: &CancelRequest)  {
        self.destination.write_i32(16);
        self.destination.write_i32(80877102);
        self.destination.write_i32(cr.process_id);
        self.destination.write_i32(cr.secret_key);
    }

    fn write_bind_message(&mut self, bind: &Bind<'_>) {
        self.destination.write_u8(b'B');
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
        self.destination.write_i32(length as i32);
        self.destination.write_bytes(bind.destination_portal_name.as_bytes());
        self.destination.write_u8(0);
        self.destination.write_bytes(bind.source_statement_name.as_bytes());
        self.destination.write_u8(0);
        self.destination
            .write_i16(bind.parameter_formats.len() as i16);
        for format in &bind.parameter_formats {
            self.destination
                .write_i16(match format {
                    ValueFormat::Text => 0,
                    ValueFormat::Binary => 1,
                });
        }
        self.destination
            .write_i16(bind.parameter_values.len() as i16);
        for value in &bind.parameter_values {
            match value {
                Some(value) => {
                    self.destination.write_i32(value.len() as i32);
                    self.destination.write_bytes(value);
                }
                None => {
                    self.destination.write_i32(-1);
                }
            }
        }
        self.destination
            .write_i16(bind.result_column_formats.len() as i16);
        for format in &bind.result_column_formats {
            self.destination
                .write_i16(match format {
                    ValueFormat::Text => 0,
                    ValueFormat::Binary => 1,
                });
        }
    }

    fn new(destination: &'b mut ByteSliceWriter<'a>) -> PostgresMessageEncoder<'a, 'b> {
        PostgresMessageEncoder { destination }
    }
}

impl<'a, 'b> Encoder<'a, &'a BackendMessage<'a>> for PostgresMessageEncoder<'a, 'b> {
    type Error = std::io::Error;

    fn encode(destination: &mut ByteSliceWriter, input: &BackendMessage<'a>) -> Result<(), Self::Error> {
        let encoder = PostgresMessageEncoder::new(destination);

        encoder.encode_backend_message(input)



    }
}

impl<'a, 'b> Encoder<'a, &'a FrontendMessage<'a>> for PostgresMessageEncoder<'a, 'b> {
    type Error = std::io::Error;

    fn encode(destination: &mut ByteSliceWriter, input: &'a FrontendMessage<'a>) -> Result<(), Self::Error> {
        let encoder = PostgresMessageEncoder::new(destination);

        encoder.encode_frontend_message(input);

        Ok(())
    }
}


impl<C: ElefantAsyncReadWrite> PostgresConnection<C> {


    pub async fn write_backend_message(
        &mut self,
        message: &BackendMessage<'_>,
    ) -> Result<(), std::io::Error> {
        self.connection.write_frame::<PostgresMessageEncoder, _>(message).await
    }

    pub async fn write_frontend_message(
        &mut self,
        message: &FrontendMessage<'_>,
    ) -> Result<(), std::io::Error> {
        self.connection.write_frame::<PostgresMessageEncoder, _>(message).await
    }

    pub async fn flush(&mut self) -> std::io::Result<()> {
        self.connection.flush().await
    }
}

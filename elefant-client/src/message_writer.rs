use std::io::Error;
use crate::io_extensions::AsyncWriteExt2;
use crate::messages::{BackendMessage, Bind, FrontendMessage};
use futures::{AsyncWrite, AsyncWriteExt};

pub struct MessageWriter<W: AsyncWrite + Unpin> {
    writer: W,
    write_buffer: Vec<u8>,
}

impl<W: AsyncWrite + Unpin> MessageWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            write_buffer: Vec::new(),
        }
    }

    pub async fn write_backend_message(
        &mut self,
        message: &BackendMessage<'_>,
    ) -> Result<(), std::io::Error> {
        match message {
            BackendMessage::AuthenticationOk => {
                self.writer.write_u8(b'R').await?;
                self.writer.write_i32(8).await?;
                self.writer.write_i32(0).await?;
                Ok(())
            }
            BackendMessage::AuthenticationKerberosV5 => {
                self.writer.write_u8(b'R').await?;
                self.writer.write_i32(8).await?;
                self.writer.write_i32(2).await?;
                Ok(())
            }
            BackendMessage::AuthenticationCleartextPassword => {
                self.writer.write_u8(b'R').await?;
                self.writer.write_i32(8).await?;
                self.writer.write_i32(3).await?;
                Ok(())
            }
            BackendMessage::AuthenticationMD5Password(md5) => {
                self.writer.write_u8(b'R').await?;
                self.writer.write_i32(12).await?;
                self.writer.write_i32(5).await?;
                self.writer.write_all(&md5.salt).await?;
                Ok(())
            }
            BackendMessage::AuthenticationGSS => {
                self.writer.write_u8(b'R').await?;
                self.writer.write_i32(8).await?;
                self.writer.write_i32(7).await?;
                Ok(())
            }
            BackendMessage::AuthenticationGSSContinue(gss) => {
                self.writer.write_u8(b'R').await?;
                self.writer.write_i32(8 + gss.data.len() as i32).await?;
                self.writer.write_i32(8).await?;
                self.writer.write_all(gss.data).await?;
                Ok(())
            }
            BackendMessage::AuthenticationSSPI => {
                self.writer.write_u8(b'R').await?;
                self.writer.write_i32(8).await?;
                self.writer.write_i32(9).await?;
                Ok(())
            }
            BackendMessage::AuthenticationSASL(sasl) => {
                self.writer.write_u8(b'R').await?;
                let length = 8 + sasl.mechanisms.iter().map(|s| s.len() + 1).sum::<usize>() as i32;
                self.writer.write_i32(length).await?;
                self.writer.write_i32(10).await?;
                for mechanism in &sasl.mechanisms {
                    self.writer.write_all(mechanism.as_bytes()).await?;
                    self.writer.write_u8(0).await?;
                }
                Ok(())
            }
            BackendMessage::AuthenticationSASLContinue(sasl) => {
                self.writer.write_u8(b'R').await?;
                self.writer.write_i32(8 + sasl.data.len() as i32).await?;
                self.writer.write_i32(11).await?;
                self.writer.write_all(sasl.data).await?;
                Ok(())
            }
            BackendMessage::AuthenticationSASLFinal(sasl) => {
                self.writer.write_u8(b'R').await?;
                self.writer.write_i32(8 + sasl.outcome.len() as i32).await?;
                self.writer.write_i32(12).await?;
                self.writer.write_all(sasl.outcome).await?;
                Ok(())
            }
            BackendMessage::BackendKeyData(bkd) => {
                self.writer.write_u8(b'K').await?;
                self.writer.write_i32(12).await?;
                self.writer.write_i32(bkd.process_id).await?;
                self.writer.write_i32(bkd.secret_key).await?;
                Ok(())
            },
            BackendMessage::BindComplete => {
                self.writer.write_u8(b'2').await?;
                self.writer.write_i32(4).await?;
                Ok(())
            }
        }
    }

    pub async fn write_frontend_message(
        &mut self,
        message: &FrontendMessage<'_>,
    ) -> Result<(), std::io::Error> {
        match message {
            FrontendMessage::Bind(bind) => self.write_bind_message(&bind).await,
            FrontendMessage::CancelRequest(cr) => {
                self.writer.write_i32(16).await?;
                self.writer.write_i32(80877102).await?;
                self.writer.write_i32(cr.process_id).await?;
                self.writer.write_i32(cr.secret_key).await?;
                Ok(())
            }
        }
    }

    async fn write_bind_message(&mut self, bind: &Bind<'_>) -> Result<(), Error> {
        self.writer.write_u8(b'B').await?;
        let length = size_of::<i32>()
            + bind.destination_portal_name.len() + size_of::<u8>()
            + bind.source_statement_name.len() + size_of::<u8>()
            + size_of::<i16>() + bind.parameter_formats.len() * size_of::<i16>()
            + size_of::<i16>() + bind
            .parameter_values
            .iter()
            .map(|v| v.map(|v| v.len()).unwrap_or(0))
            .sum::<usize>()
            + bind.parameter_values.len() * size_of::<i32>()
            + size_of::<i16>() + bind.result_column_formats.len() * size_of::<i16>();
        self.writer.write_i32(length as i32).await?;
        self.writer
            .write_all(bind.destination_portal_name.as_bytes())
            .await?;
        self.writer.write_u8(0).await?;
        self.writer
            .write_all(bind.source_statement_name.as_bytes())
            .await?;
        self.writer.write_u8(0).await?;
        self.writer
            .write_i16(bind.parameter_formats.len() as i16)
            .await?;
        for format in &bind.parameter_formats {
            self.writer
                .write_i16(match format {
                    crate::messages::BindParameterFormat::Text => 0,
                    crate::messages::BindParameterFormat::Binary => 1,
                })
                .await?;
        }
        self.writer
            .write_i16(bind.parameter_values.len() as i16)
            .await?;
        for value in &bind.parameter_values {
            match value {
                Some(value) => {
                    self.writer.write_i32(value.len() as i32).await?;
                    self.writer.write_all(value).await?;
                }
                None => {
                    self.writer.write_i32(-1).await?;
                }
            }
        }
        self.writer
            .write_i16(bind.result_column_formats.len() as i16)
            .await?;
        for format in &bind.result_column_formats {
            self.writer
                .write_i16(match format {
                    crate::messages::ResultColumnFormat::Text => 0,
                    crate::messages::ResultColumnFormat::Binary => 1,
                })
                .await?;
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush().await
    }
}

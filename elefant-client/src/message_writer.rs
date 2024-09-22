use futures::{AsyncWrite, AsyncWriteExt};
use crate::io_extensions::AsyncWriteExt2;
use crate::messages::BackendMessage;

pub struct MessageWriter<W: AsyncWrite + Unpin> {
    writer: W,
    write_buffer: Vec<u8>,
}

impl <W: AsyncWrite + Unpin> MessageWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            write_buffer: Vec::new(),
        }
    }

    pub async fn write_backend_message(&mut self, message: &BackendMessage<'_>) -> Result<(), std::io::Error> {
        match message {
            BackendMessage::AuthenticationOk => {
                self.writer.write_u8(b'R').await?;
                self.writer.write_i32(8).await?;
                self.writer.write_i32(0).await?;
                Ok(())
            },
            BackendMessage::AuthenticationKerberosV5 => {
                self.writer.write_u8(b'R').await?;
                self.writer.write_i32(8).await?;
                self.writer.write_i32(2).await?;
                Ok(())
            },
            BackendMessage::AuthenticationCleartextPassword => {
                self.writer.write_u8(b'R').await?;
                self.writer.write_i32(8).await?;
                self.writer.write_i32(3).await?;
                Ok(())
            },
            BackendMessage::AuthenticationMD5Password(md5) => {
                self.writer.write_u8(b'R').await?;
                self.writer.write_i32(12).await?;
                self.writer.write_i32(5).await?;
                self.writer.write_all(&md5.salt).await?;
                Ok(())
            },
            BackendMessage::AuthenticationGSS => {
                self.writer.write_u8(b'R').await?;
                self.writer.write_i32(8).await?;
                self.writer.write_i32(7).await?;
                Ok(())
            },
            BackendMessage::AuthenticationGSSContinue(gss) => {
                self.writer.write_u8(b'R').await?;
                self.writer.write_i32(8 + gss.data.len() as i32).await?;
                self.writer.write_i32(8).await?;
                self.writer.write_all(gss.data).await?;
                Ok(())
            },
            BackendMessage::AuthenticationSSPI => {
                self.writer.write_u8(b'R').await?;
                self.writer.write_i32(8).await?;
                self.writer.write_i32(9).await?;
                Ok(())
            },
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
            },
            BackendMessage::AuthenticationSASLContinue(sasl) => {
                self.writer.write_u8(b'R').await?;
                self.writer.write_i32(8 + sasl.data.len() as i32).await?;
                self.writer.write_i32(11).await?;
                self.writer.write_all(sasl.data).await?;
                Ok(())
            },
            BackendMessage::AuthenticationSASLFinal(sasl) => {
                self.writer.write_u8(b'R').await?;
                self.writer.write_i32(8 + sasl.outcome.len() as i32).await?;
                self.writer.write_i32(12).await?;
                self.writer.write_all(sasl.outcome).await?;
                Ok(())
            },
            BackendMessage::BackendKeyData(bkd) => {
                self.writer.write_u8(b'K').await?;
                self.writer.write_i32(12).await?;
                self.writer.write_i32(bkd.process_id).await?;
                self.writer.write_i32(bkd.secret_key).await?;
                Ok(())
            },
        }
    }
    
    pub async fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush().await
    }
    
}
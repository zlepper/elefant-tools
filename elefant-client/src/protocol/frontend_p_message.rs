use std::borrow::Cow;
use futures::{AsyncWrite, AsyncWriteExt};
use crate::protocol::io_extensions::AsyncWriteExt2;
use crate::protocol::PostgresMessageParseError;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FrontendPMessage<'a> {
    SASLInitialResponse(SASLInitialResponse<'a>),
    SASLResponse(SASLResponse<'a>),
    GSSResponse(GSSResponse<'a>),
    PasswordMessage(PasswordMessage<'a>),
    Undecided(UndecidedFrontendPMessage<'a>)
}

#[derive(Debug, PartialEq, Eq)]
pub struct SASLInitialResponse<'a> {
    pub mechanism: Cow<'a, str>,
    pub data: Option<&'a [u8]>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct SASLResponse<'a> {
    pub data: &'a [u8]
}

#[derive(Debug, PartialEq, Eq)]
pub struct GSSResponse<'a> {
    pub data: &'a [u8]
}

#[derive(Debug, PartialEq, Eq)]
pub struct PasswordMessage<'a> {
    pub password: Cow<'a, str>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct UndecidedFrontendPMessage<'a> {
    pub data: &'a [u8],
}


impl FrontendPMessage<'_> {
    pub(crate) async fn write_to<C: AsyncWrite + Unpin>(&self, destination: &mut C) -> Result<(), std::io::Error> {
        match self {
            FrontendPMessage::SASLInitialResponse(sasl) => {
                destination.write_u8(b'p').await?;
                
                let length = 4 + sasl.mechanism.len() + 1 + 4 + sasl.data.map(|d| d.len()).unwrap_or(0);
                
                destination.write_i32(length as i32).await?;
                destination.write_null_terminated_string(&sasl.mechanism).await?;
                if let Some(data) = sasl.data {
                    destination.write_i32(data.len() as i32).await?;
                    destination.write_all(data).await?;
                } else {
                    destination.write_i32(-1).await?;
                }
            }
            FrontendPMessage::SASLResponse(sasl) => {
                destination.write_u8(b'p').await?;
                destination.write_i32(4 + sasl.data.len() as i32).await?;
                destination.write_all(sasl.data).await?;
            }
            FrontendPMessage::GSSResponse(gss) => {
                destination.write_u8(b'p').await?;
                destination.write_i32(4 + gss.data.len() as i32).await?;
                destination.write_all(gss.data).await?;
            }
            FrontendPMessage::PasswordMessage(pw) => {
                destination.write_u8(b'p').await?;
                destination.write_i32(4 + pw.password.len() as i32 + 1 ).await?;
                destination.write_null_terminated_string(&pw.password).await?;
            },
            FrontendPMessage::Undecided(undecided) => {
                destination.write_u8(b'p').await?;
                destination.write_i32(4 + undecided.data.len() as i32).await?;
                destination.write_all(undecided.data).await?;
            }
        }
        
        Ok(())
    }
}

use std::borrow::Cow;
use crate::protocol::frame_reader::ByteSliceWriter;

#[derive(Debug, PartialEq, Eq)]
pub enum FrontendPMessage<'a> {
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
    pub(crate) fn write_to(&self, destination: &mut ByteSliceWriter) {
        match self {
            FrontendPMessage::SASLInitialResponse(sasl) => {
                destination.write_u8(b'p');
                
                let length = 4 + sasl.mechanism.len() + 1 + 4 + sasl.data.map(|d| d.len()).unwrap_or(0);
                
                destination.write_i32(length as i32);
                destination.write_null_terminated_string(&sasl.mechanism);
                if let Some(data) = sasl.data {
                    destination.write_i32(data.len() as i32);
                    destination.write_bytes(data);
                } else {
                    destination.write_i32(-1);
                }
            }
            FrontendPMessage::SASLResponse(sasl) => {
                destination.write_u8(b'p');
                destination.write_i32(4 + sasl.data.len() as i32);
                destination.write_bytes(sasl.data);
            }
            FrontendPMessage::GSSResponse(gss) => {
                destination.write_u8(b'p');
                destination.write_i32(4 + gss.data.len() as i32);
                destination.write_bytes(gss.data);
            }
            FrontendPMessage::PasswordMessage(pw) => {
                destination.write_u8(b'p');
                destination.write_i32(4 + pw.password.len() as i32 + 1 );
                destination.write_null_terminated_string(&pw.password);
            },
            FrontendPMessage::Undecided(undecided) => {
                destination.write_u8(b'p');
                destination.write_i32(4 + undecided.data.len() as i32);
                destination.write_bytes(undecided.data);
            }
        }
        
    }
}

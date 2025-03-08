use std::error::Error;
use std::fmt::Display;
use crate::protocol::frame_reader::DecodeErrorError;

#[derive(Debug)]
pub enum PostgresMessageParseError {
    IoError(std::io::Error),
    UnknownMessage(u8),
    UnknownSubMessage {
        message_type: u8,
        length: i32,
        sub_message_type: i32,
    },
    UnexpectedMessageLength {
        message_type: u8,
        length: i32,
    },
    UnknownValueFormat(i16),
    UnknownCloseTarget(u8),
    UnknownDescribeTarget(u8),
    UnknownTransactionStatus(u8),
}

impl DecodeErrorError for PostgresMessageParseError {}

impl From<std::io::Error> for PostgresMessageParseError {
    fn from(e: std::io::Error) -> Self {
        PostgresMessageParseError::IoError(e)
    }
}

impl Display for PostgresMessageParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PostgresMessageParseError::IoError(e) => write!(f, "IO error: {}", e),
            PostgresMessageParseError::UnknownMessage(m) => write!(f, "Unexpected message: {}", m),
            PostgresMessageParseError::UnknownSubMessage {
                message_type,
                length,
                sub_message_type,
            } => write!(
                f,
                "Unknown sub-message: {} for message {} with length {}",
                sub_message_type, message_type, length
            ),
            PostgresMessageParseError::UnexpectedMessageLength {
                message_type,
                length,
            } => write!(
                f,
                "Unexpected message length: {} for {}",
                length, message_type
            ),
            PostgresMessageParseError::UnknownValueFormat(code) => write!(
                f,
                "Unknown value column format: {}.",
                code
            ),
            PostgresMessageParseError::UnknownCloseTarget(code) => {
                write!(f, "Unknown close target: {}. Expected 'S' or 'P'", code)
            },
            PostgresMessageParseError::UnknownDescribeTarget(code) => {
                write!(f, "Unknown describe target: {}. Expected 'S' or 'P'", code)
            },
            PostgresMessageParseError::UnknownTransactionStatus(code) => {
                write!(f, "Unknown transaction status: {}", code)
            },
        }
    }
}

impl Error for PostgresMessageParseError {}

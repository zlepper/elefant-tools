use std::error::Error;
use std::fmt::Display;

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
    UnknownBindParameterFormat(i16),
    UnknownResultColumnFormat(i16),
}

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
            PostgresMessageParseError::UnknownBindParameterFormat(code) => write!(
                f,
                "Unknown bind parameter format: {}. Expected '1' or '2'",
                code
            ),
            PostgresMessageParseError::UnknownResultColumnFormat(code) => write!(
                f,
                "Unknown result column format: {}. Expected '1' or '2'",
                code
            ),
        }
    }
}

impl Error for PostgresMessageParseError {}

use crate::io_extensions::ByteSliceExt;
use crate::message_writer::MessageWriter;
use crate::messages::*;
use futures::io::Cursor;
use tokio::test;
use crate::message_reader::MessageReader;

async fn assert_message_parses_as<By: AsRef<[u8]>>(bytes: By, expected: BackendMessage<'_>) {
    let mut cursor = Cursor::new(&bytes);
    let mut reader = MessageReader::new(&mut cursor);
    let result = reader.parse_backend_message().await.unwrap();
    assert_eq!(result, expected);

    let mut cursor = Cursor::new(Vec::new());
    let mut writer = MessageWriter::new(&mut cursor);
    writer.write_backend_message(&expected).await.unwrap();
    writer.flush().await.unwrap();
    let result = cursor.into_inner();
    assert_eq!(result, bytes.as_ref());
}

macro_rules! to_wire_bytes {
        ($($val:expr),*) => {{
            let mut bytes = Vec::new();
            $(
                bytes.extend_from_slice(&$val.to_be_bytes());
            )*
            bytes
        }};
    }

#[test]
async fn test_parse_backend_message() {
    assert_message_parses_as(
        to_wire_bytes!(b'R', 8i32, 0i32),
        BackendMessage::AuthenticationOk,
    )
    .await;
}

#[test]
async fn test_parse_authentication_sasl_1_mechanism() {
    assert_message_parses_as(
        to_wire_bytes!(b'R', 12i32, 10i32, b"foo\0"),
        BackendMessage::AuthenticationSASL(AuthenticationSASL {
            mechanisms: vec!["foo".into()],
        }),
    )
    .await;
}
#[test]
async fn test_parse_authentication_sasl_2_mechanisms() {
    assert_message_parses_as(
        to_wire_bytes!(b'R', 21i32, 10i32, b"foo\0", b"booooooo\0"),
        BackendMessage::AuthenticationSASL(AuthenticationSASL {
            mechanisms: vec!["foo".into(), "booooooo".into()],
        }),
    )
    .await;
}

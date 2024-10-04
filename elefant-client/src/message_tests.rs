use crate::io_extensions::ByteSliceExt;
use crate::message_writer::MessageWriter;
use crate::messages::*;
use futures::io::Cursor;
use tokio::test;
use crate::message_reader::MessageReader;

async fn assert_backend_message_parses_as<By: AsRef<[u8]>>(bytes: By, expected: BackendMessage<'_>) {
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

async fn assert_frontend_message_round_trip(input: FrontendMessage<'_>) {
    let mut cursor = Cursor::new(Vec::new());
    let mut writer = MessageWriter::new(&mut cursor);
    writer.write_frontend_message(&input).await.unwrap();
    writer.flush().await.unwrap();
    let bytes = cursor.into_inner();
    
    eprintln!("{:?}", bytes);

    let mut cursor = Cursor::new(&bytes);
    let mut reader = MessageReader::new(&mut cursor);
    let result = reader.parse_frontend_message().await.unwrap();
    assert_eq!(result, input);
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
    assert_backend_message_parses_as(
        to_wire_bytes!(b'R', 8i32, 0i32),
        BackendMessage::AuthenticationOk,
    )
    .await;
}

#[test]
async fn test_parse_authentication_sasl_1_mechanism() {
    assert_backend_message_parses_as(
        to_wire_bytes!(b'R', 12i32, 10i32, b"foo\0"),
        BackendMessage::AuthenticationSASL(AuthenticationSASL {
            mechanisms: vec!["foo".into()],
        }),
    )
    .await;
}
#[test]
async fn test_parse_authentication_sasl_2_mechanisms() {
    assert_backend_message_parses_as(
        to_wire_bytes!(b'R', 21i32, 10i32, b"foo\0", b"booooooo\0"),
        BackendMessage::AuthenticationSASL(AuthenticationSASL {
            mechanisms: vec!["foo".into(), "booooooo".into()],
        }),
    )
    .await;
}

#[test]
async fn round_trip_bind_message() {
    assert_frontend_message_round_trip(FrontendMessage::Bind(Bind {
        destination_portal_name: "foo".into(),
        source_statement_name: "bar".into(),
        parameter_formats: vec![BindParameterFormat::Text, BindParameterFormat::Binary],
        parameter_values: vec![Some(&[1, 2, 3]), None],
        result_column_formats: vec![ResultColumnFormat::Text],
    })).await;
    
    assert_frontend_message_round_trip(FrontendMessage::Bind(Bind {
        destination_portal_name: "".into(),
        source_statement_name: "".into(),
        parameter_formats: vec![],
        parameter_values: vec![],
        result_column_formats: vec![],
    })).await;
}

#[test]
async fn round_trip_close_message() {
    assert_frontend_message_round_trip(FrontendMessage::Close(Close {
        target: CloseType::Portal,
        name: "foo".into(),
    })).await;
}
use crate::io_extensions::ByteSliceExt;
use crate::message_reader::MessageReader;
use crate::message_writer::MessageWriter;
use crate::messages::*;
use futures::io::Cursor;
use tokio::test;

async fn assert_backend_message_parses_as<By: AsRef<[u8]>>(
    bytes: By,
    expected: BackendMessage<'_>,
) {
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

async fn assert_backend_message_round_trip(input: BackendMessage<'_>) {
    let mut cursor = Cursor::new(Vec::new());
    let mut writer = MessageWriter::new(&mut cursor);
    writer.write_backend_message(&input).await.unwrap();
    writer.flush().await.unwrap();
    let bytes = cursor.into_inner();

    let mut cursor = Cursor::new(&bytes);
    let mut reader = MessageReader::new(&mut cursor);
    let result = reader.parse_backend_message().await.unwrap();
    assert_eq!(result, input);
}

async fn assert_frontend_message_round_trip(input: FrontendMessage<'_>) {
    let mut cursor = Cursor::new(Vec::new());
    let mut writer = MessageWriter::new(&mut cursor);
    writer.write_frontend_message(&input).await.unwrap();
    writer.flush().await.unwrap();
    let bytes = cursor.into_inner();

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
        parameter_formats: vec![ValueFormat::Text, ValueFormat::Binary],
        parameter_values: vec![Some(&[1, 2, 3]), None],
        result_column_formats: vec![ValueFormat::Text],
    }))
    .await;

    assert_frontend_message_round_trip(FrontendMessage::Bind(Bind {
        destination_portal_name: "".into(),
        source_statement_name: "".into(),
        parameter_formats: vec![],
        parameter_values: vec![],
        result_column_formats: vec![],
    }))
    .await;
}

#[test]
async fn round_trip_close_message() {
    assert_frontend_message_round_trip(FrontendMessage::Close(Close {
        target: CloseType::Portal,
        name: "foo".into(),
    }))
    .await;
}

#[test]
async fn round_trip_close_complete() {
    assert_backend_message_round_trip(BackendMessage::CloseComplete).await;
}

#[test]
async fn round_trip_command_complete() {
    assert_backend_message_round_trip(BackendMessage::CommandComplete(CommandComplete {
        tag: "INSERT 42".into(),
    }))
    .await;
}

#[test]
async fn round_trip_copy_data() {
    assert_frontend_message_round_trip(FrontendMessage::CopyData(CopyData { data: &[1, 2, 3] }))
        .await;

    assert_frontend_message_round_trip(FrontendMessage::CopyData(CopyData { data: &[] })).await;

    assert_backend_message_round_trip(BackendMessage::CopyData(CopyData { data: &[1, 2, 3] }))
        .await;

    assert_backend_message_round_trip(BackendMessage::CopyData(CopyData { data: &[] })).await;
}

#[test]
async fn round_trip_copy_done() {
    assert_frontend_message_round_trip(FrontendMessage::CopyDone).await;
    assert_backend_message_round_trip(BackendMessage::CopyDone).await;
}

#[test]
async fn round_trip_copy_fail() {
    assert_frontend_message_round_trip(FrontendMessage::CopyFail(CopyFail {
        message: "foo".into(),
    }))
    .await;
    assert_frontend_message_round_trip(FrontendMessage::CopyFail(CopyFail { message: "".into() }))
        .await;
}

#[test]
async fn round_trip_copy_in_response() {
    assert_backend_message_round_trip(BackendMessage::CopyInResponse(CopyResponse {
        format: ValueFormat::Text,
        column_formats: vec![ValueFormat::Text, ValueFormat::Text],
    }))
    .await;

    assert_backend_message_round_trip(BackendMessage::CopyInResponse(CopyResponse {
        format: ValueFormat::Binary,
        column_formats: vec![ValueFormat::Binary],
    }))
    .await;

    assert_backend_message_round_trip(BackendMessage::CopyInResponse(CopyResponse {
        format: ValueFormat::Binary,
        column_formats: vec![ValueFormat::Text, ValueFormat::Binary],
    }))
    .await;
}

#[test]
async fn round_trip_copy_out_response() {
    assert_backend_message_round_trip(BackendMessage::CopyOutResponse(CopyResponse {
        format: ValueFormat::Text,
        column_formats: vec![ValueFormat::Text, ValueFormat::Text],
    }))
    .await;

    assert_backend_message_round_trip(BackendMessage::CopyOutResponse(CopyResponse {
        format: ValueFormat::Binary,
        column_formats: vec![ValueFormat::Binary],
    }))
    .await;

    assert_backend_message_round_trip(BackendMessage::CopyOutResponse(CopyResponse {
        format: ValueFormat::Binary,
        column_formats: vec![ValueFormat::Text, ValueFormat::Binary],
    }))
    .await;
}

#[test]
async fn round_trip_copy_both_response() {
    assert_backend_message_round_trip(BackendMessage::CopyBothResponse(CopyResponse {
        format: ValueFormat::Text,
        column_formats: vec![ValueFormat::Text, ValueFormat::Text],
    }))
    .await;

    assert_backend_message_round_trip(BackendMessage::CopyBothResponse(CopyResponse {
        format: ValueFormat::Binary,
        column_formats: vec![ValueFormat::Binary],
    }))
    .await;

    assert_backend_message_round_trip(BackendMessage::CopyBothResponse(CopyResponse {
        format: ValueFormat::Binary,
        column_formats: vec![ValueFormat::Text, ValueFormat::Binary],
    }))
    .await;
}

#[test]
async fn round_trip_data_row() {
    assert_backend_message_round_trip(BackendMessage::DataRow(DataRow {
        values: vec![Some(&[1, 2, 3]), None, Some(&[])],
    }))
    .await;

    assert_backend_message_round_trip(BackendMessage::DataRow(DataRow {
        values: vec![None, Some(&[]), Some(&[1, 2, 3])],
    }))
    .await;

    assert_backend_message_round_trip(BackendMessage::DataRow(DataRow { values: vec![] })).await;

    assert_backend_message_round_trip(BackendMessage::DataRow(DataRow {
        values: vec![None, None],
    }))
    .await;
}

#[test]
async fn round_trip_describe() {
    assert_frontend_message_round_trip(FrontendMessage::Describe(Describe {
        target: DescribeTarget::Statement,
        name: "foo".into(),
    })).await;
    assert_frontend_message_round_trip(FrontendMessage::Describe(Describe {
        target: DescribeTarget::Portal,
        name: "foo".into(),
    })).await;
    assert_frontend_message_round_trip(FrontendMessage::Describe(Describe {
        target: DescribeTarget::Statement,
        name: "".into(),
    })).await;
    assert_frontend_message_round_trip(FrontendMessage::Describe(Describe {
        target: DescribeTarget::Portal,
        name: "".into(),
    })).await;
}

#[test]
async fn round_trip_empty_query_response() {
    assert_backend_message_round_trip(BackendMessage::EmptyQueryResponse).await;
}

#[test]
async fn round_trip_error_response() {
    assert_backend_message_round_trip(BackendMessage::ErrorResponse(ErrorResponse {
        fields: vec![ErrorField {
            field_type: b'S',
            value: "PANIC".into(),
        }, ErrorField {
            field_type: b'n',
            value: "my_constraint".into(),
        }],
    }))
    .await;
}

#[test]
async fn round_trip_execute() {
    assert_frontend_message_round_trip(FrontendMessage::Execute(Execute {
        portal_name: "foo".into(),
        max_rows: 42,
    })).await;

    assert_frontend_message_round_trip(FrontendMessage::Execute(Execute {
        portal_name: "".into(),
        max_rows: 0,
    })).await;

    assert_frontend_message_round_trip(FrontendMessage::Execute(Execute {
        portal_name: "foo".into(),
        max_rows: 0,
    })).await;

    assert_frontend_message_round_trip(FrontendMessage::Execute(Execute {
        portal_name: "".into(),
        max_rows: 666,
    })).await;
}

#[test]
async fn round_trip_flush() {
    assert_frontend_message_round_trip(FrontendMessage::Flush).await;
}
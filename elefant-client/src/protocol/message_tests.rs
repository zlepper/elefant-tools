use crate::protocol::io_extensions::ByteSliceExt;
use crate::protocol::messages::*;
use futures::io::Cursor;
use tokio::test;
use crate::protocol::{FrontendPMessage, PostgresConnection, UndecidedFrontendPMessage};

async fn assert_backend_message_parses_as<By: AsRef<[u8]>>(
    bytes: By,
    expected: BackendMessage<'_>,
) {
    let mut cursor = Cursor::new(bytes.as_ref().to_vec());
    let mut reader = PostgresConnection::new(&mut cursor);
    let result = reader.read_backend_message().await.unwrap();
    assert_eq!(result, expected);

    let mut cursor = Cursor::new(Vec::new());
    let mut writer = PostgresConnection::new(&mut cursor);
    writer.write_backend_message(&expected).await.unwrap();
    writer.flush().await.unwrap();
    let result = cursor.into_inner();
    assert_eq!(result, bytes.as_ref());
}

async fn assert_backend_message_round_trip(input: BackendMessage<'_>) {
    let mut cursor = Cursor::new(Vec::new());
    let mut writer = PostgresConnection::new(&mut cursor);
    writer.write_backend_message(&input).await.unwrap();
    writer.flush().await.unwrap();
    let bytes = cursor.into_inner();

    let mut cursor = Cursor::new(bytes);
    let mut reader = PostgresConnection::new(&mut cursor);
    let result = reader.read_backend_message().await.unwrap();
    assert_eq!(result, input);
}

async fn assert_frontend_message_round_trip(input: FrontendMessage<'_>) {
    let mut cursor = Cursor::new(Vec::new());
    let mut writer = PostgresConnection::new(&mut cursor);
    writer.write_frontend_message(&input).await.unwrap();
    writer.flush().await.unwrap();
    let bytes = cursor.into_inner();

    let mut cursor = Cursor::new(bytes);
    let mut reader = PostgresConnection::new(&mut cursor);
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
    }))
    .await;
    assert_frontend_message_round_trip(FrontendMessage::Describe(Describe {
        target: DescribeTarget::Portal,
        name: "foo".into(),
    }))
    .await;
    assert_frontend_message_round_trip(FrontendMessage::Describe(Describe {
        target: DescribeTarget::Statement,
        name: "".into(),
    }))
    .await;
    assert_frontend_message_round_trip(FrontendMessage::Describe(Describe {
        target: DescribeTarget::Portal,
        name: "".into(),
    }))
    .await;
}

#[test]
async fn round_trip_empty_query_response() {
    assert_backend_message_round_trip(BackendMessage::EmptyQueryResponse).await;
}

#[test]
async fn round_trip_error_response() {
    assert_backend_message_round_trip(BackendMessage::ErrorResponse(ErrorResponse {
        fields: vec![
            ErrorField {
                field_type: b'S',
                value: "PANIC".into(),
            },
            ErrorField {
                field_type: b'n',
                value: "my_constraint".into(),
            },
        ],
    }))
    .await;
}

#[test]
async fn round_trip_notice_response() {
    assert_backend_message_round_trip(BackendMessage::NoticeResponse(ErrorResponse {
        fields: vec![
            ErrorField {
                field_type: b'S',
                value: "PANIC".into(),
            },
            ErrorField {
                field_type: b'n',
                value: "my_constraint".into(),
            },
        ],
    }))
    .await;
}

#[test]
async fn round_trip_execute() {
    assert_frontend_message_round_trip(FrontendMessage::Execute(Execute {
        portal_name: "foo".into(),
        max_rows: 42,
    }))
    .await;

    assert_frontend_message_round_trip(FrontendMessage::Execute(Execute {
        portal_name: "".into(),
        max_rows: 0,
    }))
    .await;

    assert_frontend_message_round_trip(FrontendMessage::Execute(Execute {
        portal_name: "foo".into(),
        max_rows: 0,
    }))
    .await;

    assert_frontend_message_round_trip(FrontendMessage::Execute(Execute {
        portal_name: "".into(),
        max_rows: 666,
    }))
    .await;
}

#[test]
async fn round_trip_flush() {
    assert_frontend_message_round_trip(FrontendMessage::Flush).await;
}

#[test]
async fn round_trip_function_call() {
    assert_frontend_message_round_trip(FrontendMessage::FunctionCall(FunctionCall {
        object_id: 42,
        argument_formats: vec![ValueFormat::Text, ValueFormat::Binary],
        arguments: vec![Some(&[1, 2, 3]), None],
        result_format: ValueFormat::Text,
    }))
    .await;
    assert_frontend_message_round_trip(FrontendMessage::FunctionCall(FunctionCall {
        object_id: 42,
        argument_formats: vec![ValueFormat::Text],
        arguments: vec![None],
        result_format: ValueFormat::Text,
    }))
    .await;
    assert_frontend_message_round_trip(FrontendMessage::FunctionCall(FunctionCall {
        object_id: 42,
        argument_formats: vec![],
        arguments: vec![],
        result_format: ValueFormat::Text,
    }))
    .await;
}

#[test]
async fn round_trip_function_call_response() {
    assert_backend_message_round_trip(BackendMessage::FunctionCallResponse(FunctionCallResponse {
        value: None,
    }))
    .await;
    assert_backend_message_round_trip(BackendMessage::FunctionCallResponse(FunctionCallResponse {
        value: Some(&[1, 2, 3]),
    }))
    .await;
}

#[test]
async fn round_trip_gssenc_request() {
    assert_frontend_message_round_trip(FrontendMessage::GSSENCRequest).await;
}

#[test]
async fn round_trip_undecided_frontend_p_message() {
    assert_frontend_message_round_trip(FrontendMessage::FrontendPMessage(
        FrontendPMessage::Undecided(UndecidedFrontendPMessage { data: &[1, 2, 3] }),
    ))
    .await;
}

#[test]
async fn round_trip_negotiate_protocol_version() {
    assert_backend_message_round_trip(BackendMessage::NegotiateProtocolVersion(
        NegotiateProtocolVersion {
            newest_protocol_version: 42,
            protocol_options: vec!["foo".into(), "bar".into()],
        },
    ))
    .await;
    assert_backend_message_round_trip(BackendMessage::NegotiateProtocolVersion(
        NegotiateProtocolVersion {
            newest_protocol_version: 42,
            protocol_options: vec!["bar".into()],
        },
    ))
    .await;
    assert_backend_message_round_trip(BackendMessage::NegotiateProtocolVersion(
        NegotiateProtocolVersion {
            newest_protocol_version: 42,
            protocol_options: vec![],
        },
    ))
    .await;
}

#[test]
async fn round_trip_no_data() {
    assert_backend_message_round_trip(BackendMessage::NoData).await;
}

#[test]
async fn round_trip_notification_response() {
    assert_backend_message_round_trip(BackendMessage::NotificationResponse(NotificationResponse {
        process_id: 42,
        channel: "foo".into(),
        payload: "bar".into(),
    }))
    .await;
    assert_backend_message_round_trip(BackendMessage::NotificationResponse(NotificationResponse {
        process_id: 42,
        channel: "".into(),
        payload: "".into(),
    }))
    .await;
}

#[test]
async fn round_trip_parameter_description() {
    assert_backend_message_round_trip(BackendMessage::ParameterDescription(ParameterDescription {
        types: vec![1, 2, 3],
    }))
    .await;
    assert_backend_message_round_trip(BackendMessage::ParameterDescription(ParameterDescription {
        types: vec![],
    }))
    .await;
}

#[test]
async fn round_trip_parameter_status() {
    assert_backend_message_round_trip(BackendMessage::ParameterStatus(ParameterStatus {
        name: "foo".into(),
        value: "bar".into(),
    }))
    .await;
}

#[test]
async fn round_trip_parse() {
    assert_frontend_message_round_trip(FrontendMessage::Parse(Parse {
        destination: "foo".into(),
        query: "SELECT 42".into(),
        parameter_types: vec![1, 0, 2, 3, 0],
    }))
    .await;
}

#[test]
async fn round_trip_parse_complete() {
    assert_backend_message_round_trip(BackendMessage::ParseComplete).await;
}

#[test]
async fn round_trip_portal_suspended() {
    assert_backend_message_round_trip(BackendMessage::PortalSuspended).await;
}

#[test]
async fn round_trip_query() {
    assert_frontend_message_round_trip(FrontendMessage::Query(Query {
        query: "SELECT 42".into(),
    }))
    .await;
}

#[test]
async fn round_trip_ready_for_query() {
    assert_backend_message_round_trip(BackendMessage::ReadyForQuery(ReadyForQuery {
        current_transaction_status: CurrentTransactionStatus::Idle,
    }))
    .await;
    assert_backend_message_round_trip(BackendMessage::ReadyForQuery(ReadyForQuery {
        current_transaction_status: CurrentTransactionStatus::InTransaction,
    }))
    .await;
    assert_backend_message_round_trip(BackendMessage::ReadyForQuery(ReadyForQuery {
        current_transaction_status: CurrentTransactionStatus::InFailedTransaction,
    }))
    .await;
}

#[test]
async fn round_trip_row_description() {
    assert_backend_message_round_trip(BackendMessage::RowDescription(RowDescription {
        fields: vec![
            FieldDescription {
                name: "foo".into(),
                table_oid: 42,
                column_attribute_number: 666,
                data_type_oid: 42,
                data_type_size: 666,
                type_modifier: 42,
                format: ValueFormat::Text,
            },
            FieldDescription {
                name: "bar".into(),
                table_oid: 666,
                column_attribute_number: 42,
                data_type_oid: 666,
                data_type_size: 42,
                type_modifier: 666,
                format: ValueFormat::Binary,
            },
            FieldDescription {
                name: "bar".into(),
                table_oid: 0,
                column_attribute_number: 0,
                data_type_oid: 666,
                data_type_size: 42,
                type_modifier: 666,
                format: ValueFormat::Text,
            },
        ],
    }))
    .await;
    assert_backend_message_round_trip(BackendMessage::RowDescription(RowDescription {
        fields: vec![],
    }))
    .await;
}

#[test]
async fn round_trip_ssl_request() {
    assert_frontend_message_round_trip(FrontendMessage::SSLRequest).await;
}

#[test]
async fn round_trip_startup_message() {
    assert_frontend_message_round_trip(FrontendMessage::StartupMessage(StartupMessage {
        parameters: vec![
            StartupMessageParameter {
                name: "foo".into(),
                value: "bar".into(),
            },
            StartupMessageParameter {
                name: "bar2".into(),
                value: "foo3".into(),
            },
        ],
    })).await;
}

#[test]
async fn round_trip_sync() {
    assert_frontend_message_round_trip(FrontendMessage::Sync).await;
}

#[test]
async fn round_trip_terminate() {
    assert_frontend_message_round_trip(FrontendMessage::Terminate).await;
}
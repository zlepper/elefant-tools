use std::borrow::Cow;

#[derive(Debug, PartialEq, Eq)]
pub enum BackendMessage<'a> {
    AuthenticationOk,
    AuthenticationKerberosV5,
    AuthenticationCleartextPassword,
    AuthenticationMD5Password(AuthenticationMD5Password),
    AuthenticationGSS,
    AuthenticationGSSContinue(AuthenticationGSSContinue<'a>),
    AuthenticationSSPI,
    AuthenticationSASL(AuthenticationSASL<'a>),
    AuthenticationSASLContinue(AuthenticationSASLContinue<'a>),
    AuthenticationSASLFinal(AuthenticationSASLFinal<'a>),
    BackendKeyData(BackendKeyData),
    BindComplete,
    CloseComplete,
    CommandComplete(CommandComplete<'a>),
    CopyData(CopyData<'a>),
    CopyDone,
    CopyInResponse(CopyResponse),
    CopyOutResponse(CopyResponse),
    CopyBothResponse(CopyResponse),
    DataRow(DataRow<'a>),
    EmptyQueryResponse,
    ErrorResponse(ErrorResponse<'a>),
}

#[derive(Debug, PartialEq, Eq)]
pub struct AuthenticationMD5Password {
    pub salt: [u8; 4],
}

#[derive(Debug, PartialEq, Eq)]
pub struct AuthenticationGSSContinue<'a> {
    pub data: &'a [u8],
}

#[derive(Debug, PartialEq, Eq)]
pub struct AuthenticationSASL<'a> {
    pub mechanisms: Vec<Cow<'a, str>>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct AuthenticationSASLContinue<'a> {
    pub data: &'a [u8],
}

#[derive(Debug, PartialEq, Eq)]
pub struct AuthenticationSASLFinal<'a> {
    pub outcome: &'a [u8],
}

#[derive(Debug, PartialEq, Eq)]
pub struct BackendKeyData {
    pub process_id: i32,
    pub secret_key: i32,
}

#[derive(Debug, PartialEq, Eq)]
pub struct CommandComplete<'a> {
    pub tag: Cow<'a, str>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct CopyResponse {
    pub format: ValueFormat,
    pub column_formats: Vec<ValueFormat>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct DataRow<'a> {
    pub values: Vec<Option<&'a [u8]>>
}

#[derive(Debug, PartialEq, Eq)]
pub struct ErrorResponse<'a> {
    pub fields: Vec<ErrorField<'a>>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ErrorField<'a> {
    pub field_type: u8,
    pub value: Cow<'a, str>,
}


#[derive(Debug, PartialEq, Eq)]
pub enum FrontendMessage<'a> {
    Bind(Bind<'a>),
    CancelRequest(CancelRequest),
    Close(Close<'a>),
    CopyData(CopyData<'a>),
    CopyDone,
    CopyFail(CopyFail<'a>),
    Describe(Describe<'a>),
    Execute(Execute<'a>),
}

#[derive(Debug, PartialEq, Eq)]
pub struct Bind<'a> {
    pub destination_portal_name: Cow<'a, str>,
    pub source_statement_name: Cow<'a, str>,
    pub parameter_formats: Vec<ValueFormat>,
    pub parameter_values: Vec<Option<&'a [u8]>>,
    pub result_column_formats: Vec<ValueFormat>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ValueFormat {
    Text,
    Binary,
}

#[derive(Debug, PartialEq, Eq)]
pub struct CancelRequest {
    pub process_id: i32,
    pub secret_key: i32,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Close<'a> {
    pub target: CloseType,
    pub name: Cow<'a, str>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum CloseType {
    Statement,
    Portal,
}

#[derive(Debug, PartialEq, Eq)]
pub struct CopyData<'a> {
    pub data: &'a [u8],
}

#[derive(Debug, PartialEq, Eq)]
pub struct CopyFail<'a> {
    pub message: Cow<'a, str>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Describe<'a> {
    pub target: DescribeTarget,
    pub name: Cow<'a, str>,
}


#[derive(Debug, PartialEq, Eq)]
pub enum DescribeTarget {
    Statement,
    Portal,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Execute<'a> {
    pub portal_name: Cow<'a, str>,
    pub max_rows: i32,
}
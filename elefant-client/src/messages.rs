use std::borrow::Cow;
#[derive(Debug, PartialEq, Eq)]
pub enum FrontendMessage {}

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


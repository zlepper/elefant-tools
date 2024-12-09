mod establish;

use std::borrow::Cow;
use futures::{AsyncRead, AsyncWrite, AsyncBufRead};
use crate::{ElefantClientError, PostgresConnectionSettings};
use crate::protocol::{BackendMessage, FrontendMessage, FrontendPMessage, PostgresConnection, sasl, SASLInitialResponse, SASLResponse, StartupMessage, StartupMessageParameter};
use crate::protocol::sasl::ChannelBinding;

pub struct PostgresClient<C> {
    pub(crate) connection: PostgresConnection<C>,
    pub(crate) settings: PostgresConnectionSettings,
}

impl<C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin> PostgresClient<C> {
}


use crate::protocol::async_io::{ElefantAsyncRead, ElefantAsyncReadWrite, ElefantAsyncWrite};
use crate::protocol::frame_reader::Framed;
use std::io;

/// Internal stream wrapper handling optional TLS.
///
/// Without the `rustls` feature this is a single-variant enum (zero overhead).
pub(super) enum MaybeTlsStream<S> {
    Plain(S),
    #[cfg(feature = "rustls")]
    Tls(Box<crate::tls::TlsStream<S>>),
}

impl<S: ElefantAsyncReadWrite> ElefantAsyncRead for MaybeTlsStream<S> {
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            MaybeTlsStream::Plain(s) => s.read(buf).await,
            #[cfg(feature = "rustls")]
            MaybeTlsStream::Tls(s) => s.read(buf).await,
        }
    }
}

impl<S: ElefantAsyncReadWrite> ElefantAsyncWrite for MaybeTlsStream<S> {
    async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        match self {
            MaybeTlsStream::Plain(s) => s.write_all(buf).await,
            #[cfg(feature = "rustls")]
            MaybeTlsStream::Tls(s) => s.write_all(buf).await,
        }
    }

    async fn flush(&mut self) -> io::Result<()> {
        match self {
            MaybeTlsStream::Plain(s) => s.flush().await,
            #[cfg(feature = "rustls")]
            MaybeTlsStream::Tls(s) => s.flush().await,
        }
    }
}

impl<S: ElefantAsyncReadWrite> MaybeTlsStream<S> {
    /// Send TLS close_notify if this is a TLS connection; no-op for plain.
    pub(super) async fn shutdown(&mut self) -> io::Result<()> {
        match self {
            MaybeTlsStream::Plain(_) => Ok(()),
            #[cfg(feature = "rustls")]
            MaybeTlsStream::Tls(s) => s.shutdown().await,
        }
    }
}

pub struct PostgresConnection<C> {
    pub(super) connection: Framed<MaybeTlsStream<C>>,
}

impl<C: ElefantAsyncReadWrite> PostgresConnection<C> {
    pub fn new(connection: C) -> (Self, Option<Vec<u8>>) {
        (
            Self {
                connection: Framed::new(MaybeTlsStream::Plain(connection)),
            },
            None,
        )
    }

    /// Send a TLS close_notify if the connection uses TLS; no-op for plain.
    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.connection.get_stream_mut().shutdown().await
    }

    /// Create a new connection, negotiating TLS if configured.
    ///
    /// When the `rustls` feature is enabled and TLS settings are provided,
    /// this sends the PostgreSQL SSLRequest, performs the TLS handshake,
    /// and returns channel binding data alongside the connection. In "prefer"
    /// mode, falls back to plaintext when the server declines SSL.
    /// Otherwise the stream is used as-is.
    pub async fn new_maybe_tls(
        stream: C,
        settings: &crate::PostgresConnectionSettings,
    ) -> Result<(Self, Option<Vec<u8>>), crate::ElefantClientError> {
        #[cfg(feature = "rustls")]
        {
            if let Some(config) = settings.tls.config() {
                let result = crate::tls::negotiate_tls(stream, &settings.host, config).await?;
                return match result {
                    crate::tls::TlsNegotiationResult::Tls(tls_stream, channel_binding) => Ok((
                        Self {
                            connection: Framed::new(MaybeTlsStream::Tls(tls_stream)),
                        },
                        channel_binding,
                    )),
                    crate::tls::TlsNegotiationResult::Declined(stream) => {
                        if settings.tls.is_required() {
                            Err(crate::ElefantClientError::TlsError(
                                "server does not support SSL".into(),
                            ))
                        } else {
                            Ok(Self::new(stream))
                        }
                    }
                };
            }
        }

        let _ = settings; // avoid unused warning without rustls
        Ok(Self::new(stream))
    }
}

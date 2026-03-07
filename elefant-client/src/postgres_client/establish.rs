use crate::pool::ConnectionFactory;
use crate::postgres_client::PostgresClient;
use crate::protocol::sasl::ChannelBinding;
use crate::protocol::{
    sasl, BackendMessage, FrontendMessage, FrontendPMessage, PasswordMessage, SASLInitialResponse,
    SASLResponse, StartupMessage, StartupMessageParameter,
};
use crate::{ElefantClientError, PostgresConnectionSettings};
use md5::Digest;
use std::borrow::Cow;

impl<F: ConnectionFactory> PostgresClient<F> {
    pub(crate) async fn establish(
        &mut self,
        settings: &PostgresConnectionSettings,
        channel_binding_data: Option<Vec<u8>>,
    ) -> Result<(), ElefantClientError> {
        self.connection
            .write_frontend_message(&FrontendMessage::StartupMessage(StartupMessage {
                parameters: vec![
                    StartupMessageParameter::new("user", &settings.user),
                    StartupMessageParameter::new("database", &settings.database),
                    StartupMessageParameter::new("client_encoding", "UTF8"),
                ]
                .into_iter()
                .chain(
                    settings
                        .options
                        .as_deref()
                        .map(|opts| StartupMessageParameter::new("options", opts)),
                )
                .chain(
                    settings
                        .replication
                        .as_deref()
                        .map(|r| StartupMessageParameter::new("replication", r)),
                )
                .collect(),
            }))
            .await?;
        self.connection.flush().await?;

        let msg = self.read_next_backend_message().await?;

        match msg {
            BackendMessage::AuthenticationOk => {
                // Trust auth (or peer/ident/cert) — server requires no credentials.
            }
            BackendMessage::AuthenticationSASL(ref sasl) => {
                let supported_mechanism =
                    select_sasl_mechanism(&sasl.mechanisms, channel_binding_data.is_some());

                let (channel_binding, mechanism_name) =
                    match (&supported_mechanism, channel_binding_data) {
                        (Some(SaslMechanism::ScramSha256Plus), Some(data)) => (
                            ChannelBinding::tls_server_end_point(data),
                            sasl::SCRAM_SHA_256_PLUS,
                        ),
                        (Some(SaslMechanism::ScramSha256), Some(_)) => {
                            // TLS is active but PLUS not selected — "y,," signals to the
                            // server that we support channel binding but chose not to use it,
                            // allowing the server to detect a MITM downgrade (RFC 5802 §6).
                            (ChannelBinding::unrequested(), sasl::SCRAM_SHA_256)
                        }
                        (Some(SaslMechanism::ScramSha256), None) => {
                            // No TLS — "n,," signals no channel binding support.
                            (ChannelBinding::unsupported(), sasl::SCRAM_SHA_256)
                        }
                        _ => {
                            return Err(ElefantClientError::UnsupportedAuthenticationMethod(
                                format!(
                                    "Unsupported SASL mechanism. Server offered: {:?}",
                                    sasl.mechanisms
                                ),
                            ));
                        }
                    };

                {
                    let mut sas =
                        sasl::ScramSha256::new(settings.password.as_bytes(), channel_binding);

                    let data = sas.message();

                    self.connection
                        .write_frontend_message(&FrontendMessage::FrontendPMessage(
                            FrontendPMessage::SASLInitialResponse(SASLInitialResponse {
                                mechanism: Cow::Borrowed(mechanism_name),
                                data: Some(data),
                            }),
                        ))
                        .await?;
                    self.connection.flush().await?;

                    let msg = self.read_next_backend_message().await?;

                    match msg {
                        BackendMessage::AuthenticationSASLContinue(ref sasl_continue) => {
                            sas.update(sasl_continue.data)?;
                            let data = sas.message();

                            self.connection
                                .write_frontend_message(&FrontendMessage::FrontendPMessage(
                                    FrontendPMessage::SASLResponse(SASLResponse { data }),
                                ))
                                .await?;
                            self.connection.flush().await?;

                            let msg = self.read_next_backend_message().await?;

                            match msg {
                                BackendMessage::AuthenticationSASLFinal(fin) => {
                                    sas.finish(fin.outcome)?;

                                    let msg = self.read_next_backend_message().await?;

                                    match msg {
                                        BackendMessage::AuthenticationOk => {
                                            // Authentication successful, whoop whoop!
                                        }
                                        BackendMessage::ErrorResponse(er) => {
                                            return Err(ElefantClientError::PostgresError(
                                                format!("{er:?}"),
                                            ));
                                        }
                                        _ => {
                                            return Err(
                                                ElefantClientError::UnexpectedBackendMessage(
                                                    format!("{msg:?}"),
                                                ),
                                            );
                                        }
                                    }
                                }
                                BackendMessage::ErrorResponse(er) => {
                                    return Err(ElefantClientError::PostgresError(format!(
                                        "{er:?}"
                                    )));
                                }
                                _ => {
                                    return Err(ElefantClientError::UnexpectedBackendMessage(
                                        format!("{msg:?}"),
                                    ));
                                }
                            }
                        }
                        BackendMessage::ErrorResponse(er) => {
                            return Err(ElefantClientError::PostgresError(format!("{er:?}")));
                        }
                        _ => {
                            return Err(ElefantClientError::UnexpectedBackendMessage(format!(
                                "{msg:?}"
                            )));
                        }
                    }
                }
            }
            BackendMessage::AuthenticationMD5Password(md5_pw) => {
                let pw = calculate_md5_password_message(settings, md5_pw.salt);
                self.connection
                    .write_frontend_message(&FrontendMessage::FrontendPMessage(
                        FrontendPMessage::PasswordMessage(PasswordMessage {
                            password: pw.into(),
                        }),
                    ))
                    .await?;
                self.connection.flush().await?;

                let msg = self.read_next_backend_message().await?;
                match msg {
                    BackendMessage::AuthenticationOk => {
                        // Authentication successful, whoop whoop!
                    }
                    BackendMessage::ErrorResponse(er) => {
                        return Err(ElefantClientError::PostgresError(format!("{er:?}")));
                    }
                    _ => {
                        return Err(ElefantClientError::UnexpectedBackendMessage(format!(
                            "{msg:?}"
                        )));
                    }
                }
            }
            BackendMessage::AuthenticationCleartextPassword => {
                self.connection
                    .write_frontend_message(&FrontendMessage::FrontendPMessage(
                        FrontendPMessage::PasswordMessage(PasswordMessage {
                            password: Cow::Borrowed(&settings.password),
                        }),
                    ))
                    .await?;
                self.connection.flush().await?;

                let msg = self.read_next_backend_message().await?;
                match msg {
                    BackendMessage::AuthenticationOk => {
                        // Authentication successful, whoop whoop!
                    }
                    BackendMessage::ErrorResponse(er) => {
                        return Err(ElefantClientError::PostgresError(format!("{er:?}")));
                    }
                    _ => {
                        return Err(ElefantClientError::UnexpectedBackendMessage(format!(
                            "{msg:?}"
                        )));
                    }
                }
            }
            BackendMessage::AuthenticationGSS
            | BackendMessage::AuthenticationSSPI
            | BackendMessage::AuthenticationKerberosV5 => {
                return Err(ElefantClientError::UnsupportedAuthenticationMethod(
                    format!("{msg:?}"),
                ));
            }
            BackendMessage::ErrorResponse(er) => {
                return Err(ElefantClientError::PostgresError(format!("{er:?}")));
            }
            _ => {
                return Err(ElefantClientError::UnexpectedBackendMessage(format!(
                    "{msg:?}"
                )));
            }
        }

        loop {
            let msg = self.read_next_backend_message().await?;

            match msg {
                BackendMessage::BackendKeyData(_) => {}
                BackendMessage::ReadyForQuery(_) => {
                    self.ready_for_query = true;
                    break;
                }
                _ => {
                    return Err(ElefantClientError::UnexpectedBackendMessage(format!(
                        "{msg:?}"
                    )));
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq)]
enum SaslMechanism {
    ScramSha256,
    ScramSha256Plus,
}

/// Select the best SASL mechanism from the server's offered list.
///
/// When `has_channel_binding` is true (TLS active with channel binding data),
/// prefers SCRAM-SHA-256-PLUS regardless of server ordering.
fn select_sasl_mechanism(
    mechanisms: &[impl AsRef<str>],
    has_channel_binding: bool,
) -> Option<SaslMechanism> {
    if has_channel_binding {
        let has_plus = mechanisms
            .iter()
            .any(|m| m.as_ref() == sasl::SCRAM_SHA_256_PLUS);
        let has_plain = mechanisms.iter().any(|m| m.as_ref() == sasl::SCRAM_SHA_256);
        if has_plus {
            Some(SaslMechanism::ScramSha256Plus)
        } else if has_plain {
            Some(SaslMechanism::ScramSha256)
        } else {
            None
        }
    } else if mechanisms.iter().any(|m| m.as_ref() == sasl::SCRAM_SHA_256) {
        Some(SaslMechanism::ScramSha256)
    } else {
        None
    }
}

fn calculate_md5_password_message(settings: &PostgresConnectionSettings, salt: [u8; 4]) -> String {
    let mut hasher = md5::Md5::new();
    hasher.update(&settings.password);
    hasher.update(&settings.user);
    let username_password_md5 = hasher.finalize_reset();

    hasher.update(format!("{username_password_md5:x}"));
    hasher.update(salt);
    let password_md5 = hasher.finalize_reset();

    format!("md5{password_md5:x}")
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use super::*;
    use crate::tokio_connection::new_client;

    fn settings_for_port(port: u16) -> PostgresConnectionSettings {
        PostgresConnectionSettings::new("localhost")
            .port(port)
            .password("passw0rd")
    }

    #[tokio::test]
    async fn auth_scram_sha_256() {
        let mut client = new_client(settings_for_port(5415)).await.unwrap();
        let value: i32 = client.read_single_value_simple("select 1").await;
        assert_eq!(value, 1);
    }

    #[tokio::test]
    async fn auth_md5() {
        // PG 12 defaults to MD5 authentication
        let mut client = new_client(settings_for_port(5412)).await.unwrap();
        let value: i32 = client.read_single_value_simple("select 1").await;
        assert_eq!(value, 1);
    }

    #[tokio::test]
    async fn auth_cleartext_password() {
        // PG 15 with POSTGRES_HOST_AUTH_METHOD=password
        let mut client = new_client(settings_for_port(5601)).await.unwrap();
        let value: i32 = client.read_single_value_simple("select 1").await;
        assert_eq!(value, 1);
    }

    #[tokio::test]
    async fn auth_trust() {
        // PG 15 with POSTGRES_HOST_AUTH_METHOD=trust
        let mut client = new_client(settings_for_port(5602)).await.unwrap();
        let value: i32 = client.read_single_value_simple("select 1").await;
        assert_eq!(value, 1);
    }

    #[tokio::test]
    async fn auth_wrong_password_returns_error() {
        let settings = PostgresConnectionSettings::new("localhost")
            .port(5415)
            .password("wrong_password");
        let result = new_client(settings).await;
        assert!(
            result.is_err(),
            "Connection with wrong password should fail"
        );
    }

    #[test]
    fn md5_password_hash_correctness() {
        // Verify against the PostgreSQL MD5 auth spec:
        // md5(md5(password + username) + salt)
        let settings = PostgresConnectionSettings::new("localhost").password("passw0rd");
        let salt = [0x01, 0x02, 0x03, 0x04];

        let result = calculate_md5_password_message(&settings, salt);

        // Manually compute expected value:
        // Step 1: MD5("passw0rdpostgres")
        // Step 2: MD5(hex(step1) + salt_bytes)
        // Result: "md5" + hex(step2)
        use md5::Digest;
        let mut hasher = md5::Md5::new();
        hasher.update(b"passw0rd");
        hasher.update(b"postgres");
        let step1 = hasher.finalize_reset();

        hasher.update(format!("{step1:x}"));
        hasher.update(salt);
        let step2 = hasher.finalize();

        let expected = format!("md5{step2:x}");
        assert_eq!(result, expected);

        // Also verify the result starts with "md5" and has the right length (3 + 32 hex chars)
        assert!(result.starts_with("md5"));
        assert_eq!(result.len(), 35);
    }
}

#[cfg(test)]
mod mechanism_tests {
    use super::*;

    #[test]
    fn prefers_plus_when_channel_binding_available() {
        // Even if SCRAM-SHA-256 is listed first, PLUS should be selected
        let mechanisms = vec![
            sasl::SCRAM_SHA_256.to_string(),
            sasl::SCRAM_SHA_256_PLUS.to_string(),
        ];
        assert_eq!(
            select_sasl_mechanism(&mechanisms, true),
            Some(SaslMechanism::ScramSha256Plus),
        );
    }

    #[test]
    fn falls_back_to_plain_when_plus_unavailable_with_channel_binding() {
        let mechanisms = vec![sasl::SCRAM_SHA_256.to_string()];
        assert_eq!(
            select_sasl_mechanism(&mechanisms, true),
            Some(SaslMechanism::ScramSha256),
        );
    }

    #[test]
    fn selects_plain_without_channel_binding() {
        let mechanisms = vec![
            sasl::SCRAM_SHA_256_PLUS.to_string(),
            sasl::SCRAM_SHA_256.to_string(),
        ];
        // Without channel binding, PLUS must not be selected even if offered
        assert_eq!(
            select_sasl_mechanism(&mechanisms, false),
            Some(SaslMechanism::ScramSha256),
        );
    }

    #[test]
    fn returns_none_for_unsupported_mechanisms() {
        let mechanisms = vec!["SOME-OTHER-MECHANISM".to_string()];
        assert_eq!(select_sasl_mechanism(&mechanisms, true), None);
        assert_eq!(select_sasl_mechanism(&mechanisms, false), None);
    }

    #[test]
    fn returns_none_for_empty_mechanisms() {
        let empty: &[&str] = &[];
        assert_eq!(select_sasl_mechanism(empty, true), None);
        assert_eq!(select_sasl_mechanism(empty, false), None);
    }

    #[test]
    fn ignores_plus_without_channel_binding() {
        // Server only offers PLUS but client has no TLS — should return None
        let mechanisms = vec![sasl::SCRAM_SHA_256_PLUS.to_string()];
        assert_eq!(select_sasl_mechanism(&mechanisms, false), None);
    }
}

#[cfg(all(test, feature = "tokio", feature = "rustls"))]
mod tls_tests {
    use crate::tokio_connection::{new_client, TokioConnectionFactory, TokioPostgresPool};
    use crate::{ElefantClientError, PostgresConnectionSettings, TlsSettings};
    use std::sync::Arc;

    fn tls_config() -> Arc<rustls::ClientConfig> {
        use rustls_pki_types::pem::PemObject;
        use rustls_pki_types::CertificateDer;

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let ca_cert_pem = include_bytes!("../../../test-certs/ca.crt");
        let ca_certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(ca_cert_pem)
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to parse CA cert PEM");

        let mut root_store = rustls::RootCertStore::empty();
        for cert in ca_certs {
            root_store.add(cert).expect("failed to add CA cert");
        }

        Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth(),
        )
    }

    fn tls_settings_for_port(port: u16) -> PostgresConnectionSettings {
        PostgresConnectionSettings::new("localhost")
            .port(port)
            .password("passw0rd")
            .tls(TlsSettings::require(tls_config()))
    }

    #[tokio::test]
    async fn tls_connection_works() {
        let mut client = new_client(tls_settings_for_port(5603)).await.unwrap();
        let value: i32 = client.read_single_value_simple("select 1").await;
        assert_eq!(value, 1);
    }

    #[tokio::test]
    async fn tls_server_without_ssl_returns_error() {
        // Port 5415 is plain PG 15 — no SSL support
        let result = new_client(tls_settings_for_port(5415)).await;
        match result {
            Err(ElefantClientError::TlsError(_)) => {} // expected
            Err(e) => panic!("Expected TlsError, got: {e}"),
            Ok(_) => panic!("Expected error when connecting with TLS to non-SSL server"),
        }
    }

    #[tokio::test]
    async fn tls_scram_sha256_plus_channel_binding() {
        let mut client = new_client(tls_settings_for_port(5603)).await.unwrap();

        // Verify the connection is using SSL
        let ssl_in_use: bool = client
            .read_single_value_simple("select ssl from pg_stat_ssl where pid = pg_backend_pid()")
            .await;
        assert!(ssl_in_use, "Connection should be using SSL");
    }

    #[tokio::test]
    async fn tls_multiple_queries() {
        let mut client = new_client(tls_settings_for_port(5603)).await.unwrap();

        for i in 1..=5 {
            let value: i32 = client
                .read_single_value_simple(&format!("select {i}"))
                .await;
            assert_eq!(value, i);
        }
    }

    #[tokio::test]
    async fn tls_prefer_falls_back_to_plain() {
        // Port 5415 is plain PG 15 — no SSL support.
        // With "prefer", we should fall back to plaintext successfully.
        let settings = PostgresConnectionSettings::new("localhost")
            .port(5415)
            .password("passw0rd")
            .tls(TlsSettings::prefer(tls_config()));
        let mut client = new_client(settings).await.unwrap();
        let value: i32 = client.read_single_value_simple("select 1").await;
        assert_eq!(value, 1);
    }

    #[tokio::test]
    async fn tls_prefer_uses_tls_when_available() {
        // Port 5603 supports SSL — "prefer" should use TLS.
        let settings = PostgresConnectionSettings::new("localhost")
            .port(5603)
            .password("passw0rd")
            .tls(TlsSettings::prefer(tls_config()));
        let mut client = new_client(settings).await.unwrap();
        let ssl_in_use: bool = client
            .read_single_value_simple("select ssl from pg_stat_ssl where pid = pg_backend_pid()")
            .await;
        assert!(
            ssl_in_use,
            "Connection should be using SSL in prefer mode when server supports it"
        );
    }

    #[tokio::test]
    async fn tls_pool_reuses_connection() {
        let pool = TokioPostgresPool::new(TokioConnectionFactory, tls_settings_for_port(5603))
            .await
            .unwrap();

        let pid1: i32;
        {
            let mut client = pool.get_client().await.unwrap();
            pid1 = client
                .read_single_value_simple("select pg_backend_pid()")
                .await;
        }

        let mut client2 = pool.get_client().await.unwrap();
        let pid2: i32 = client2
            .read_single_value_simple("select pg_backend_pid()")
            .await;

        assert_eq!(pid1, pid2, "Pool should reuse the TLS connection");
    }
}

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
    pub(crate) async fn establish(&mut self, settings: &PostgresConnectionSettings) -> Result<(), ElefantClientError> {
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
                let supported_mechanism = sasl
                    .mechanisms
                    .iter()
                    .filter_map(|m| {
                        if m == sasl::SCRAM_SHA_256 {
                            Some(SaslMechanism::ScramSha256)
                        } else if m == sasl::SCRAM_SHA_256_PLUS {
                            Some(SaslMechanism::ScramSha256Plus)
                        } else {
                            None
                        }
                    })
                    .next();

                match supported_mechanism {
                    Some(SaslMechanism::ScramSha256) => {
                        let mut sas = sasl::ScramSha256::new(
                            settings.password.as_bytes(),
                            ChannelBinding::unsupported(),
                        );

                        let data = sas.message();

                        self.connection
                            .write_frontend_message(&FrontendMessage::FrontendPMessage(
                                FrontendPMessage::SASLInitialResponse(SASLInitialResponse {
                                    mechanism: Cow::Borrowed(sasl::SCRAM_SHA_256),
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
                                                return Err(ElefantClientError::PostgresError(format!("{er:?}")));
                                            }
                                            _ => {
                                                return Err(ElefantClientError::UnexpectedBackendMessage(format!("{msg:?}")));
                                            }
                                        }
                                    }
                                    BackendMessage::ErrorResponse(er) => {
                                        return Err(ElefantClientError::PostgresError(format!("{er:?}")));
                                    }
                                    _ => {
                                        return Err(ElefantClientError::UnexpectedBackendMessage(format!("{msg:?}")));
                                    }
                                }
                            }
                            BackendMessage::ErrorResponse(er) => {
                                return Err(ElefantClientError::PostgresError(format!("{er:?}")));
                            }
                            _ => {
                                return Err(ElefantClientError::UnexpectedBackendMessage(format!("{msg:?}")));
                            }
                        }
                    }
                    _ => {
                        return Err(ElefantClientError::UnsupportedAuthenticationMethod(
                            format!("Unsupported SASL mechanism. Server offered: {:?}", sasl.mechanisms),
                        ));
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
                        return Err(ElefantClientError::UnexpectedBackendMessage(format!("{msg:?}")));
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
                        return Err(ElefantClientError::UnexpectedBackendMessage(format!("{msg:?}")));
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
                return Err(ElefantClientError::UnexpectedBackendMessage(format!("{msg:?}")));
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

#[derive(Debug)]
enum SaslMechanism {
    ScramSha256,
    ScramSha256Plus,
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
        PostgresConnectionSettings {
            user: "postgres".to_string(),
            host: "localhost".to_string(),
            database: "postgres".to_string(),
            port,
            password: "passw0rd".to_string(),
            options: None,
        }
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
        let settings = PostgresConnectionSettings {
            password: "wrong_password".to_string(),
            ..settings_for_port(5415)
        };
        let result = new_client(settings).await;
        assert!(result.is_err(), "Connection with wrong password should fail");
    }

    #[test]
    fn md5_password_hash_correctness() {
        // Verify against the PostgreSQL MD5 auth spec:
        // md5(md5(password + username) + salt)
        let settings = PostgresConnectionSettings {
            user: "postgres".to_string(),
            password: "passw0rd".to_string(),
            ..Default::default()
        };
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

use std::borrow::Cow;
use futures::{AsyncBufRead, AsyncRead, AsyncWrite};
use md5::Digest;
use crate::{ElefantClientError, PostgresConnectionSettings};
use crate::postgres_client::PostgresClient;
use crate::protocol::{BackendMessage, FrontendMessage, FrontendPMessage, sasl, SASLInitialResponse, SASLResponse, StartupMessage, StartupMessageParameter, PasswordMessage};
use crate::protocol::sasl::ChannelBinding;

impl<C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin> PostgresClient<C> {
    pub(crate) async fn establish(&mut self) -> Result<(), ElefantClientError> {
        self.connection.write_frontend_message(&FrontendMessage::StartupMessage(StartupMessage {
            parameters: vec![
                StartupMessageParameter::new("user", &self.settings.user),
                StartupMessageParameter::new("database", &self.settings.database),
                StartupMessageParameter::new("client_encoding", "UTF8"),
            ]
        })).await?;
        self.connection.flush().await?;

        let msg = self.connection.read_backend_message().await?;

        match msg {
            BackendMessage::AuthenticationSASL(ref sasl) => {
                let supported_mechanism = sasl.mechanisms.iter().filter_map(|m| {
                    if m == sasl::SCRAM_SHA_256 {
                        Some(SaslMechanism::ScramSha256)
                    } else if m == sasl::SCRAM_SHA_256_PLUS {
                        Some(SaslMechanism::ScramSha256Plus)
                    } else {
                        None
                    }
                }).next();

                match supported_mechanism {
                    Some(SaslMechanism::ScramSha256) => {

                        let mut sas = sasl::ScramSha256::new(self.settings.password.as_bytes(), ChannelBinding::unsupported());

                        let data = sas.message();

                        self.connection.write_frontend_message(&FrontendMessage::FrontendPMessage(FrontendPMessage::SASLInitialResponse(SASLInitialResponse{
                            mechanism: Cow::Borrowed(sasl::SCRAM_SHA_256),
                            data: Some(data),
                        }))).await?;
                        self.connection.flush().await?;

                        let msg = self.connection.read_backend_message().await?;

                        match msg {
                            BackendMessage::AuthenticationSASLContinue(ref sasl_continue) => {
                                sas.update(sasl_continue.data)?;
                                let data = sas.message();

                                self.connection.write_frontend_message(&FrontendMessage::FrontendPMessage(FrontendPMessage::SASLResponse(SASLResponse{
                                    data,
                                }))).await?;
                                self.connection.flush().await?;

                                let msg = self.connection.read_backend_message().await?;

                                match msg {
                                    BackendMessage::AuthenticationSASLFinal(fin) => {
                                        sas.finish(fin.outcome)?;

                                        let msg = self.connection.read_backend_message().await?;

                                        match msg {
                                            BackendMessage::AuthenticationOk => {
                                                // Authentication successful, whoop whoop!
                                            },
                                            _ => todo!("Unexpected message: {:?}", msg),
                                        }
                                    }
                                    _ => todo!("Unexpected message: {:?}", msg),
                                }
                            },
                            _ => todo!("Unexpected message: {:?}", msg),
                        }

                    },
                    _ => todo!("Implement SASL mechanism: {:?}", supported_mechanism),
                }

            },
            BackendMessage::AuthenticationMD5Password(md5Pw) => {
                let pw = calculate_md5_password_message(&self.settings, md5Pw.salt);
                self.connection.write_frontend_message(&FrontendMessage::FrontendPMessage(FrontendPMessage::PasswordMessage(PasswordMessage {
                    password: pw.into(),
                }))).await?;
                self.connection.flush().await?;

                let msg = self.connection.read_backend_message().await?;
                match msg {
                    BackendMessage::AuthenticationOk => {
                        // Authentication successful, whoop whoop!
                    },
                    _ => {
                        panic!("Unexpected message: {:?}", msg);
                    }
                }
            },
            _ => {
                panic!("Unexpected message: {:?}", msg);
            }
        }

        
        
        loop {
            let msg = self.connection.read_backend_message().await?;

            match msg {
                BackendMessage::ParameterStatus(_) => {
                },
                BackendMessage::BackendKeyData(_) => {
                },
                BackendMessage::ReadyForQuery(_) => {
                    self.ready_for_query = true;
                    break;
                },
                _ => {
                    return Err(ElefantClientError::UnexpectedBackendMessage(format!("{:?}", msg)));
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
    hasher.update(format!("{:x}", username_password_md5));
    hasher.update(salt);
    let password_md5 = hasher.finalize_reset();

    // let username_password_md5 = md5::Md5::digest(format!("{}{}", &settings.password, &settings.user));
    // let salt_hex = base16ct::lower::encode_string(&salt);
    // let password_md5 = md5::Md5::digest(format!("{:x}{}", username_password_md5, salt_hex));
    format!("md5{:x}", password_md5)
}
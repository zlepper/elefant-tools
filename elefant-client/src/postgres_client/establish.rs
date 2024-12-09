use std::borrow::Cow;
use futures::{AsyncBufRead, AsyncRead, AsyncWrite};
use crate::ElefantClientError;
use crate::postgres_client::PostgresClient;
use crate::protocol::{BackendMessage, FrontendMessage, FrontendPMessage, sasl, SASLInitialResponse, SASLResponse, StartupMessage, StartupMessageParameter};
use crate::protocol::sasl::ChannelBinding;

impl<C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin> PostgresClient<C> {
    pub(crate) async fn establish(&mut self) -> Result<(), ElefantClientError> {
        self.connection.write_frontend_message(&FrontendMessage::StartupMessage(StartupMessage {
            parameters: vec![
                StartupMessageParameter::new("user", &self.settings.user),
                StartupMessageParameter::new("database", &self.settings.database),
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
            _ => {
                panic!("Unexpected message: {:?}", msg);
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

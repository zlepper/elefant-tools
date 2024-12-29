use crate::protocol::{BackendMessage, CopyData, CopyResponse};
use crate::{ElefantClientError, PostgresClient, Statement, ToSql};
use futures::{AsyncBufRead, AsyncRead, AsyncWrite};
use tracing::debug;

impl<C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin> PostgresClient<C> {
    pub async fn copy_out(
        &mut self,
        query: &(impl Statement + ?Sized),
        parameters: &[&(dyn ToSql)],
    ) -> Result<CopyReader<C>, ElefantClientError> {
        query.send(self, parameters).await?;

        let msg = self.read_next_backend_message().await?;

        match msg {
            BackendMessage::CopyOutResponse(_) => Ok(CopyReader { client: self }),
            _ => Err(ElefantClientError::UnexpectedBackendMessage(format!(
                "Expected CopyOutResponse{:?}",
                msg
            ))),
        }
    }

    pub async fn copy_in(
        &mut self,
        query: &(impl Statement + ?Sized),
        parameters: &[&(dyn ToSql)],
    ) -> Result<CopyWriter<C>, ElefantClientError> {
        query.send(self, parameters).await?;

        let msg = self.read_next_backend_message().await?;

        match msg {
            BackendMessage::CopyInResponse(_) => Ok(CopyWriter { client: self }),
            _ => Err(ElefantClientError::UnexpectedBackendMessage(format!(
                "Expected CopyInResponse{:?}",
                msg
            ))),
        }
    }
}

pub struct CopyWriter<'a, C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin> {
    client: &'a mut PostgresClient<C>,
}

impl<'a, C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin> CopyWriter<'a, C> {
    pub async fn write(&mut self, data: &[u8]) -> Result<(), ElefantClientError> {
        self.client
            .connection
            .write_frontend_message(&crate::protocol::FrontendMessage::CopyData(
                CopyData { data },
            ))
            .await?;
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), ElefantClientError> {
        self.client.connection.flush().await?;
        Ok(())
    }

    pub async fn end(self) -> Result<(), ElefantClientError> {
        self.client
            .connection
            .write_frontend_message(&crate::protocol::FrontendMessage::CopyDone)
            .await?;

        if self.client.sync_required {
            self.client
                .connection
                .write_frontend_message(&crate::protocol::FrontendMessage::Sync)
                .await?;
            self.client.sync_required = false;
        }

        self.client.connection.flush().await?;

        loop {
            let msg = self.client.read_next_backend_message().await?;
            match msg {
                BackendMessage::CommandComplete(_) => {
                    debug!("Copy command completed");
                }
                BackendMessage::ReadyForQuery(_) => {
                    self.client.ready_for_query = true;
                    break;
                }
                _ => {
                    return Err(ElefantClientError::UnexpectedBackendMessage(format!(
                        "Expected CommandComplete or ReadyForQuery, got {:?}",
                        msg
                    )));
                }
            }
        }

        Ok(())
    }
}

pub struct CopyReader<'a, C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin> {
    client: &'a mut PostgresClient<C>,
}

impl<'a, C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin> CopyReader<'a, C> {
    pub async fn read(&mut self) -> Result<Option<CopyData>, ElefantClientError> {
        let msg = self.client.read_next_backend_message().await?;
        match msg {
            BackendMessage::CopyData(cd) => Ok(Some(cd)),
            BackendMessage::CopyDone => Ok(None),
            _ => Err(ElefantClientError::UnexpectedBackendMessage(format!(
                "Expected CopyData or CopyDone, got {:?}",
                msg
            ))),
        }
    }

    pub async fn end(self) -> Result<(), ElefantClientError> {
        loop {
            let msg = self.client.read_next_backend_message().await?;
            match msg {
                BackendMessage::CopyData(_) | BackendMessage::CopyDone => {
                    // Ignore extra copy data messages
                }
                BackendMessage::CommandComplete(_) => {
                    debug!("Copy command completed");
                }
                BackendMessage::ReadyForQuery(_) => {
                    self.client.ready_for_query = true;
                    break;
                }
                _ => {
                    return Err(ElefantClientError::UnexpectedBackendMessage(format!(
                        "Expected CommandComplete, ReadyForQuery or CopyData, got {:?}",
                        msg
                    )));
                }
            }
        }

        Ok(())
    }

    pub async fn write_to<W: AsyncRead + AsyncBufRead + AsyncWrite + Unpin>(
        mut self,
        target: &mut CopyWriter<'_, W>,
    ) -> Result<(), ElefantClientError> {
        while let Some(cd) = self.read().await? {
            target.write(cd.data).await?;
        }

        target.flush().await?;
        self.end().await?;

        Ok(())
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use crate::test_helpers::get_tokio_test_client;

    #[tokio::test]
    async fn copies_data() {
        let mut source = get_tokio_test_client().await;
        source.execute_non_query(r#"
            drop table if exists source_table;
            create table source_table(id bigint generated by default as identity primary key, value int, txt text);
            insert into source_table(value, txt) values (1, 'one'), (2, 'two'), (3, 'three');
            "#, &[]).await.unwrap();

        let mut target = get_tokio_test_client().await;
        target.execute_non_query(r#"
            drop table if exists target_table;
            create table target_table(id bigint generated by default as identity primary key, value int, txt text);
            "#, &[]).await.unwrap();

        let copy_out = source.copy_out("COPY source_table(id, value, txt) TO STDOUT(format binary)", &[]).await.unwrap();
        let mut copy_in = target.copy_in("COPY target_table(id, value, txt) FROM STDIN(format binary)", &[]).await.unwrap();

        copy_out.write_to(&mut copy_in).await.unwrap();
        copy_in.end().await.unwrap();

        let values = target.query("select id, value, txt from target_table order by id", &[]).await.unwrap().collect_to_vec::<(i64, i32, String)>().await.unwrap();
        assert_eq!(values, vec![(1, 1, "one".to_string()), (2, 2, "two".to_string()), (3, 3, "three".to_string())]);
    }
}
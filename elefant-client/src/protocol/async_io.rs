use std::io;

pub trait ElefantAsyncRead {
    /// Asynchronously reads data into the provided buffer.
    ///
    /// Returns the number of bytes read, or an error if the operation fails.
    fn read(&mut self, buf: &mut [u8]) -> impl std::future::Future<Output = io::Result<usize>>;
}

pub trait ElefantAsyncWrite {
    /// Asynchronously writes all data from the provided buffer.
    ///
    /// Returns an error if the operation fails.
    fn write_all(&mut self, buf: &[u8]) -> impl std::future::Future<Output = io::Result<()>>;

    /// Asynchronously flushes any buffered data.
    ///
    /// Returns an error if the operation fails.
    fn flush(&mut self) -> impl std::future::Future<Output = io::Result<()>>;
}

/// Custom trait that abstracts over async I/O operations needed by elefant-client.
/// This allows us to support both tokio and monoio runtimes with a unified interface.
pub trait ElefantAsyncReadWrite: ElefantAsyncWrite + ElefantAsyncRead + Unpin {
}

impl <T: ElefantAsyncRead + ElefantAsyncWrite + Unpin> ElefantAsyncReadWrite for T {}


#[cfg(feature = "futures")]
mod futures_support {
    use crate::protocol::async_io::{ElefantAsyncRead, ElefantAsyncWrite};

    impl<T: futures::AsyncRead + Unpin> ElefantAsyncRead for T {
        async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            futures::AsyncReadExt::read(self, buf).await
        }
    }

    impl<T: futures::AsyncWrite + Unpin> ElefantAsyncWrite for T {
        async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
            futures::AsyncWriteExt::write_all(self, buf).await
        }

        async fn flush(&mut self) -> std::io::Result<()> {
            futures::AsyncWriteExt::flush(self).await
        }
    }

    // impl<T: AsRef<[u8]> + Unpin> ElefantAsyncWrite for futures::io::Cursor<T> {
    //     async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
    //         futures::io::AsyncWriteExt::write_all(self, buf).await
    //     }
    //
    //     async fn flush(&mut self) -> std::io::Result<()> {
    //         futures::io::AsyncWriteExt::flush(self).await
    //     }
    // }
}


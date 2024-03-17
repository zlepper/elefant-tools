use std::future::Future;
use futures::Stream;
use bytes::Bytes;
use crate::storage::data_format::DataFormat;
use crate::Result;

/// Data in a table. This data is a stream which can be read from the data source.
/// 
/// Make sure to call `cleanup` when you have read all the data from the stream.
pub struct TableData<S: Stream<Item=Result<Bytes>> + Send, C: AsyncCleanup> {
    pub data: S,
    pub data_format: DataFormat,
    pub cleanup: C,
}

pub trait AsyncCleanup: Send {
    fn cleanup(self) -> impl Future<Output = Result<()>> + Send;
}

impl AsyncCleanup for () {
    async fn cleanup(self) -> Result<()> {
        Ok(())
    }
}

impl<S: Stream<Item=Result<Bytes>> + Send, C: AsyncCleanup> AsyncCleanup for TableData<S, C> {
    fn cleanup(self) -> impl Future<Output=Result<()>> + Send {
        self.cleanup.cleanup()
    }
}
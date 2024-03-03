use futures::Stream;
use bytes::Bytes;
use crate::storage::data_format::DataFormat;
use crate::Result;


pub struct TableData<S: Stream<Item=Result<Bytes>> + Send, C: AsyncCleanup> {
    pub data: S,
    pub data_format: DataFormat,
    pub cleanup: C,
}

pub trait AsyncCleanup: Send {
    fn cleanup(self) -> impl std::future::Future<Output = Result<()>> + Send;
}

impl AsyncCleanup for () {
    async fn cleanup(self) -> Result<()> {
        Ok(())
    }
}

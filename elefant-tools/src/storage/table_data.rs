use crate::storage::data_format::DataFormat;
use crate::Result;
use std::future::Future;

pub trait TableDataReader: Send {
    fn read_chunk(&mut self) -> impl Future<Output = Result<Option<&[u8]>>> + '_;
}

/// Data in a table. This data can be read from the data source using the reader.
///
/// Make sure to call `cleanup` when you have read all the data from the reader.
pub struct TableData<R: TableDataReader, C: AsyncCleanup> {
    pub data: R,
    pub data_format: DataFormat,
    pub cleanup: C,
}

pub trait AsyncCleanup: Send {
    fn cleanup(self) -> impl Future<Output = Result<()>>;
}

impl AsyncCleanup for () {
    async fn cleanup(self) -> Result<()> {
        Ok(())
    }
}

impl<R: TableDataReader, C: AsyncCleanup> AsyncCleanup for TableData<R, C> {
    fn cleanup(self) -> impl Future<Output = Result<()>> {
        self.cleanup.cleanup()
    }
}

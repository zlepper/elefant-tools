/*use std::fs::File;
use std::io::{Read, Write};
use std::sync::Arc;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use crate::models::PostgresDatabase;
use crate::storage::CopyDestination;
use tar::Builder;
use tokio::sync::{Mutex};
use crate::{BaseCopyTarget, DataFormat, PostgresSchema, PostgresTable, Result, TableData};

pub struct ElefantFileDestinationStorage<F: Read + Write + Send> {
    archive: Arc<Mutex<Builder<F>>>,
}


impl<F: Read + Write + Send> ElefantFileDestinationStorage<F> {
    pub fn new(file_path: &str) -> Result<ElefantFileDestinationStorage<File>> {
        let file = File::create(file_path)?;
        let archive = Builder::new(file);

        Ok(ElefantFileDestinationStorage {
            archive: Arc::new(Mutex::new(archive)),
        })
    }
}

#[async_trait]
impl<F: Read + Write + Send> BaseCopyTarget for ElefantFileDestinationStorage<F> {
    async fn supported_data_format(&self) -> Result<Vec<DataFormat>> {

        Ok(vec![
            DataFormat::Text,
            DataFormat::PostgresBinary {
                postgres_version: None,
            },
        ])
    }
}

#[async_trait]
impl<F: Read + Write + Send> CopyDestination for ElefantFileDestinationStorage<F> {
    async fn apply_structure(&mut self, db: &PostgresDatabase) -> Result<()> {
        let mut archive = self.archive.lock().await;

        archive.







        todo!()
    }

    async fn apply_data<S: Stream<Item=Result<Bytes>> + Send>(&mut self, schema: &PostgresSchema, table: &PostgresTable, data: TableData<S>) -> Result<()> {
        todo!()
    }

    async fn apply_post_structure(&mut self, _db: &PostgresDatabase) -> Result<()> {
        Ok(())
    }
}*/
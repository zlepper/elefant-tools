use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::vec;
use bytes::Bytes;
use futures::{pin_mut, SinkExt, Stream, StreamExt};
use itertools::Itertools;
use tokio::fs::File;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};
use tracing::instrument;
use uuid::Uuid;
use crate::models::SimplifiedDataType;
use crate::models::PostgresSchema;
use crate::models::PostgresTable;
use crate::storage::{BaseCopyTarget, CopyDestination};
use crate::{AsyncCleanup, CopyDestinationFactory, ParallelCopyDestinationNotAvailable, PostgresClientWrapper, Result, SequentialOrParallel, SupportedParallelism};
use crate::chunk_reader::{ChunkResult, StringChunkReader};
use crate::quoting::IdentifierQuoter;
use crate::storage::data_format::DataFormat;
use crate::storage::table_data::TableData;

#[cfg(test)]
mod tests;

pub struct SqlFileOptions {
    pub max_rows_per_insert: usize,
    pub chunk_separator: String,
    pub max_commands_per_chunk: usize,
    pub data_mode: SqlDataMode,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SqlDataMode {
    InsertStatements,
    CopyStatements,
}

impl Display for SqlDataMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlDataMode::InsertStatements => write!(f, "InsertStatements"),
            SqlDataMode::CopyStatements => write!(f, "CopyStatements"),
        }
    }
}

impl From<String> for SqlDataMode {
    fn from(value: String) -> Self {
        match value.as_str() {
            "InsertStatements" => SqlDataMode::InsertStatements,
            "CopyStatements" => SqlDataMode::CopyStatements,
            _ => panic!("Invalid value for SqlDataMode")
        }
    }
}

impl Default for SqlFileOptions {
    fn default() -> Self {
        Self {
            max_rows_per_insert: 1000,
            chunk_separator: Uuid::new_v4().to_string(),
            max_commands_per_chunk: 10,
            data_mode: SqlDataMode::InsertStatements,
        }
    }
}

pub struct SqlFile<F: AsyncWrite + Unpin + Send + Sync> {
    file: F,
    is_empty: bool,
    options: SqlFileOptions,
    quoter: Arc<IdentifierQuoter>,
    current_command_count: usize,
    chunk_separator: Vec<u8>,
}

impl SqlFile<BufWriter<File>> {
    #[instrument(skip_all)]
    pub async fn new_file(path: &str, identifier_quoter: Arc<IdentifierQuoter>, options: SqlFileOptions) -> Result<Self> {
        let file = File::create(path).await?;

        let file = BufWriter::new(file);

        SqlFile::new(file, identifier_quoter, options).await
    }
}

static CHUNK_SEPARATOR_PREFIX: &str = "-- chunk-separator-";

impl<F: AsyncWrite + Unpin + Send + Sync> SqlFile<F> {
    pub async fn new(file: F, identifier_quoter: Arc<IdentifierQuoter>, options: SqlFileOptions) -> Result<Self> {
        let chunk_separator = format!("{}{} --", CHUNK_SEPARATOR_PREFIX, options.chunk_separator).into_bytes();

        Ok(SqlFile {
            file,
            is_empty: true,
            options,
            quoter: identifier_quoter,
            current_command_count: 0,
            chunk_separator,
        })
    }
}

impl<F: AsyncWrite + Unpin + Send + Sync> BaseCopyTarget for SqlFile<F> {
    async fn supported_data_format(&self) -> Result<Vec<DataFormat>> {
        Ok(vec![DataFormat::Text])
    }
}


impl<'a, F: AsyncWrite + Unpin + Send + Sync + 'a> CopyDestinationFactory<'a> for SqlFile<F> {
    type SequentialDestination = &'a mut SqlFile<F>;
    type ParallelDestination = ParallelCopyDestinationNotAvailable;

    async fn create_destination(&'a mut self) -> Result<SequentialOrParallel<Self::SequentialDestination, Self::ParallelDestination>> {
        Ok(SequentialOrParallel::Sequential(self))
    }

    async fn create_sequential_destination(&'a mut self) -> Result<Self::SequentialDestination>{
        Ok(self)
    }

    fn supported_parallelism(&self) -> SupportedParallelism {
        SupportedParallelism::Sequential
    }
}

impl<F: AsyncWrite + Unpin + Send + Sync> CopyDestination for &mut SqlFile<F> {
    #[instrument(skip_all)]
    async fn apply_data<S: Stream<Item=Result<Bytes>> + Send, C: AsyncCleanup>(&mut self, schema: &PostgresSchema, table: &PostgresTable, data: TableData<S, C>) -> Result<()> {
        let file = &mut self.file;
        if self.current_command_count > 0 {
            file.write_all(b"\n").await?;
            self.current_command_count = 0;
        }

        let stream = data.data;

        pin_mut!(stream);

        if self.options.data_mode == SqlDataMode::InsertStatements {
            self.write_data_stream_to_insert_statements(&mut stream, schema, table).await?;
        } else {
            self.write_data_stream_to_copy_statements(&mut stream, schema, table).await?;
        }

        Ok(())
    }

    #[instrument(skip_all)]
    async fn apply_transactional_statement(&mut self, statement: &str) -> Result<()> {
        if self.current_command_count % self.options.max_commands_per_chunk == 0 {
            if !self.is_empty {
                self.file.write_all(b"\n\n").await?;
            }

            self.file.write_all(&self.chunk_separator).await?;
            self.file.write_all(b"\n").await?;
            self.is_empty = true;
        }

        if self.is_empty {
            self.file.write_all(statement.as_bytes()).await?;
            self.is_empty = false;
        } else {
            self.file.write_all(b"\n\n").await?;
            self.file.write_all(statement.as_bytes()).await?;
        }

        self.current_command_count += 1;

        Ok(())
    }

    #[instrument(skip_all)]
    async fn apply_non_transactional_statement(&mut self, statement: &str) -> Result<()> {
        self.apply_transactional_statement(statement).await
    }

    async fn begin_transaction(&mut self) -> Result<()> {
        Ok(())
    }

    async fn commit_transaction(&mut self) -> Result<()> {
        Ok(())
    }

    fn get_identifier_quoter(&self) -> Arc<IdentifierQuoter> {
        self.quoter.clone()
    }
}

impl<F: AsyncWrite + Unpin + Send + Sync> SqlFile<F> {
    #[instrument(skip_all)]
    async fn write_data_stream_to_insert_statements<S: Stream<Item=Result<Bytes>> + Send + Unpin>(&mut self, stream: &mut S, schema: &PostgresSchema, table: &PostgresTable) -> Result<()> {
        let file = &mut self.file;

        let column_types = table.columns.iter().map(|c| c.get_simplified_data_type()).collect_vec();

        let mut count = 0;
        while let Some(bytes) = stream.next().await {
            if count == 0 {
                file.write_all(b"\n").await?;
                file.write_all(&self.chunk_separator).await?;
                file.write_all(b"\n").await?;
            }
            match bytes {
                Ok(bytes) => {
                    if count % self.options.max_rows_per_insert == 0 {
                        if count > 0 {
                            file.write_all(b";\n").await?;
                            file.write_all(&self.chunk_separator).await?;
                            file.write_all(b"\n").await?;
                        }

                        file.write_all(b"insert into ").await?;
                        file.write_all(schema.name.as_bytes()).await?;
                        file.write_all(b".").await?;
                        file.write_all(table.name.as_bytes()).await?;
                        file.write_all(b" (").await?;
                        for (index, column) in table.columns.iter().enumerate() {
                            if index != 0 {
                                file.write_all(b", ").await?;
                            }
                            file.write_all(column.name.as_bytes()).await?;
                        }
                        file.write_all(b")").await?;
                        file.write_all(b" values").await?;

                        file.write_all(b"\n").await?;
                        count = 0;
                    } else {
                        file.write_all(b",\n").await?;
                    }
                    count += 1;


                    write_row(file, &column_types, bytes).await?;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        if count > 0 {
            file.write_all(b";\n").await?;
        }

        file.flush().await?;

        Ok(())
    }

    #[instrument(skip_all)]
    async fn write_data_stream_to_copy_statements<S: Stream<Item=Result<Bytes>> + Send + Unpin>(&mut self, stream: &mut S, schema: &PostgresSchema, table: &PostgresTable) -> Result<()> {

        let file = &mut self.file;

        let mut count = 0;
        while let Some(bytes) = stream.next().await {
            if count == 0 {
                file.write_all(b"\n").await?;
                file.write_all(&self.chunk_separator).await?;
                file.write_all(b"\n").await?;
                
                let copy_command = table.get_copy_in_command(schema, &DataFormat::Text, &self.quoter);
                file.write_all(copy_command.as_bytes()).await?;
                
                file.write_all(b"\n").await?;
                file.write_all(&self.chunk_separator).await?;
                file.write_all(b"\n").await?;
            }
            match bytes {
                Ok(bytes) => {
                    file.write_all(&bytes).await?;
                    count += 1;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        if count > 0 {
            file.write_all(b"\\.\n").await?;
            file.flush().await?;
        }


        Ok(())
    }
}

async fn write_row<F: AsyncWrite + Unpin + Send + Sync>(file: &mut F, column_types: &[SimplifiedDataType], bytes: Bytes) -> Result<()> {
    let without_line_break = bytes.slice(0..bytes.len() - 1);
    let column_bytes = without_line_break.split(|b| *b == b'\t');

    let cols = column_bytes.zip(column_types.iter());
    file.write_all(b"(").await?;
    for (index, (bytes, col_data_type)) in cols.enumerate() {
        if index != 0 {
            file.write_all(b", ").await?;
        }

        write_column(file, bytes, col_data_type).await?;
    }
    file.write_all(b")").await?;

    Ok(())
}

async fn write_column<F: AsyncWrite + Unpin + Send + Sync>(content: &mut F, bytes: &[u8], col_data_type: &SimplifiedDataType) -> Result<()> {
    if bytes == [b'\\', b'N'] {
        content.write_all(b"null").await?;
        return Ok(());
    }


    match col_data_type {
        SimplifiedDataType::Number => {
            write_number_column(content, bytes).await?;
        }
        SimplifiedDataType::Text => {
            write_text_column(content, bytes).await?;
        }
        SimplifiedDataType::Bool => {
            write_bool_column(content, bytes).await?;
        }
    }

    Ok(())
}

async fn write_bool_column<F: AsyncWrite + Unpin + Send + Sync>(content: &mut F, bytes: &[u8]) -> Result<()> {
    let value = bytes[0] == b't';
    content.write_all(format!("{}", value).as_bytes()).await?;
    Ok(())
}

async fn write_text_column<F: AsyncWrite + Unpin + Send + Sync>(content: &mut F, bytes: &[u8]) -> Result<()> {
    content.write_all(b"E'").await?;

    if bytes.contains(&b'\'') {
        let s = std::str::from_utf8(bytes).unwrap();
        let s = s.replace('\'', "''");
        content.write_all(s.as_bytes()).await?;
    } else {
        content.write_all(bytes).await?;
    }
    content.write_all(b"'").await?;

    Ok(())
}

async fn write_number_column<F: AsyncWrite + Unpin + Send + Sync>(content: &mut F, bytes: &[u8]) -> Result<()> {
    match bytes[..] {
        [b'N', b'a', b'N'] | [b'I', b'n', b'f', b'i', b'n', b'i', b't', b'y'] | [b'-', b'I', b'n', b'f', b'i', b'n', b'i', b't', b'y'] => {
            content.write_all(b"'").await?;
            content.write_all(bytes).await?;
            content.write_all(b"'").await?;
        }
        _ => {
            content.write_all(bytes).await?;
        }
    }

    Ok(())
}

#[instrument(skip_all)]
pub async fn apply_sql_file<F: AsyncBufRead + Unpin + Send + Sync>(content: &mut F, target_connection: &PostgresClientWrapper) -> Result<()> {
    let mut sql_chunk = String::with_capacity(10000);

    let read = content.read_line(&mut sql_chunk).await?;

    if read == 0 {
        return Ok(());
    }

    if sql_chunk.starts_with(CHUNK_SEPARATOR_PREFIX) {
        let separator = sql_chunk.clone();

        loop {
            sql_chunk.clear();

            let read = content.read_lines_until_separator_line(&separator, &mut sql_chunk).await?;
            match read {
                ChunkResult::Chunk(_) => {
                    if sql_chunk.starts_with("copy ") && sql_chunk.ends_with(" from stdin with (format text, header false);\n") {
                        let copy_in_stream = target_connection.copy_in::<Bytes>(&sql_chunk).await?;


                        pin_mut!(copy_in_stream);

                        loop {
                            sql_chunk.clear();
                            let read = content.read_line(&mut sql_chunk).await?;
                            if read == 0 {
                                break;
                            }
                            if sql_chunk.starts_with("\\.") {
                                break;
                            }
                            let byt = Bytes::from(sql_chunk.clone());

                            copy_in_stream.send(byt).await?;
                        }

                        copy_in_stream.close().await?;
                    } else {
                        target_connection.execute_non_query(&sql_chunk).await?;
                    }
                }
                ChunkResult::End(read) => {
                    if read > 0 {
                        target_connection.execute_non_query(&sql_chunk).await?;
                    }
                    break;
                }
            }
        }
    } else {
        content.read_to_string(&mut sql_chunk).await?;
        target_connection.execute_non_query(&sql_chunk).await?;
    }

    Ok(())
}

pub async fn apply_sql_string(file_content: &str, target_connection: &PostgresClientWrapper) -> Result<()> {
    let mut bytes = file_content.as_bytes();
    apply_sql_file(&mut bytes, target_connection).await
}

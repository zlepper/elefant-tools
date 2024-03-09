use std::collections::HashMap;
use std::sync::Arc;
use tokio_postgres::Row;
use tracing::instrument;
use crate::{BaseCopyTarget, CopyDestinationFactory, CopySourceFactory, DataFormat, ElefantToolsError, IdentifierQuoter, PostgresClientWrapper, SequentialOrParallel, SupportedParallelism};
use crate::postgres_client_wrapper::{FromPgChar, FromRow, RowEnumExt};
use crate::quoting::AllowedKeywordUsage;
use crate::storage::postgres::parallel_copy_destination::ParallelSafePostgresInstanceCopyDestinationStorage;
use crate::storage::postgres::parallel_copy_source::ParallelSafePostgresInstanceCopySourceStorage;
use crate::storage::postgres::sequential_copy_destination::SequentialSafePostgresInstanceCopyDestinationStorage;
use crate::storage::postgres::sequential_copy_source::SequentialSafePostgresInstanceCopySourceStorage;

pub struct PostgresInstanceStorage<'a> {
    pub(crate) connection: &'a PostgresClientWrapper,
    pub(crate) postgres_version: String,
    pub(crate) identifier_quoter: Arc<IdentifierQuoter>,
}

impl<'a> PostgresInstanceStorage<'a> {
    #[instrument(skip_all)]
    pub async fn new(connection: &'a PostgresClientWrapper) -> crate::Result<Self> {
        let postgres_version = connection.get_single_result("select version()").await?;

        let keywords = connection
            .get_results::<Keyword>(
                "select word, catcode from pg_get_keywords() where catcode <> 'U'",
            )
            .await?;

        let mut keyword_info = HashMap::new();

        for keyword in keywords {
            keyword_info.insert(
                keyword.word,
                AllowedKeywordUsage {
                    column_name: keyword.category == KeywordType::AllowedInColumnName
                        || keyword.category == KeywordType::AllowedInTypeOrFunctionName,
                    type_or_function_name: keyword.category
                        == KeywordType::AllowedInTypeOrFunctionName,
                },
            );
        }

        let quoter = IdentifierQuoter::new(keyword_info);

        Ok(PostgresInstanceStorage {
            connection,
            postgres_version,
            identifier_quoter: Arc::new(quoter),
        })
    }

    pub fn get_identifier_quoter(&self) -> Arc<IdentifierQuoter> {
        self.identifier_quoter.clone()
    }
}

struct Keyword {
    word: String,
    category: KeywordType,
}

impl FromRow for Keyword {
    fn from_row(row: Row) -> crate::Result<Self> {
        Ok(Keyword {
            word: row.try_get(0)?,
            category: row.try_get_enum_value(1)?,
        })
    }
}

#[derive(Eq, PartialEq, Debug)]
enum KeywordType {
    Unreserved,
    AllowedInColumnName,
    AllowedInTypeOrFunctionName,
    Reserved,
}

impl FromPgChar for KeywordType {
    fn from_pg_char(c: char) -> crate::Result<Self> {
        match c {
            'U' => Ok(KeywordType::Unreserved),
            'C' => Ok(KeywordType::AllowedInColumnName),
            'T' => Ok(KeywordType::AllowedInTypeOrFunctionName),
            'R' => Ok(KeywordType::Reserved),
            _ => Err(ElefantToolsError::InvalidKeywordType(c.to_string())),
        }
    }
}

impl BaseCopyTarget for PostgresInstanceStorage<'_> {
    async fn supported_data_format(&self) -> crate::Result<Vec<DataFormat>> {
        Ok(vec![
            DataFormat::Text,
            DataFormat::PostgresBinary {
                postgres_version: Some(self.postgres_version.clone()),
            },
        ])
    }
}


impl<'a> CopySourceFactory for PostgresInstanceStorage<'a> {
    type SequentialSource = SequentialSafePostgresInstanceCopySourceStorage<'a>;
    type ParallelSource = ParallelSafePostgresInstanceCopySourceStorage<'a>;

    async fn create_source(
        &self,
    ) -> crate::Result<SequentialOrParallel<Self::SequentialSource, Self::ParallelSource>> {
        let parallel = ParallelSafePostgresInstanceCopySourceStorage::new(self).await?;

        Ok(SequentialOrParallel::Parallel(parallel))
    }

    async fn create_sequential_source(&self) -> crate::Result<Self::SequentialSource> {
        let seq = SequentialSafePostgresInstanceCopySourceStorage::new(self).await?;

        Ok(seq)
    }

    fn supported_parallelism(&self) -> SupportedParallelism {
        SupportedParallelism::Parallel
    }
}

impl<'a> CopyDestinationFactory<'a> for PostgresInstanceStorage<'a> {
    type SequentialDestination = SequentialSafePostgresInstanceCopyDestinationStorage<'a>;
    type ParallelDestination = ParallelSafePostgresInstanceCopyDestinationStorage<'a>;

    async fn create_destination(
        &'a mut self,
    ) -> crate::Result<SequentialOrParallel<Self::SequentialDestination, Self::ParallelDestination>> {
        let par = ParallelSafePostgresInstanceCopyDestinationStorage::new(self).await?;

        Ok(SequentialOrParallel::Parallel(par))
    }

    async fn create_sequential_destination(&'a mut self) -> crate::Result<Self::SequentialDestination> {
        let seq = SequentialSafePostgresInstanceCopyDestinationStorage::new(self).await?;

        Ok(seq)
    }

    fn supported_parallelism(&self) -> SupportedParallelism {
        SupportedParallelism::Parallel
    }
}


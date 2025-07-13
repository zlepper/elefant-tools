use crate::cli::{Commands, CopyArgs, ExportDbArgs, ImportDbArgs, Storage};
use clap::Parser;
use elefant_tools::PostgresClientWrapper;
use elefant_tools::{
    apply_sql_file, copy_data, CopyDataOptions, PostgresInstanceStorage, Result, SqlFileOptions,
};
use std::num::NonZeroUsize;
use tracing::instrument;

mod cli;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = cli::Cli::parse();

    run(cli).await?;

    Ok(())
}

#[instrument(skip_all)]
async fn run(cli: cli::Cli) -> Result<()> {
    match cli.command {
        Commands::Export {
            db_args,
            destination,
        } => {
            do_export(db_args, destination, cli.max_parallelism).await?;
        }
        Commands::Import { db_args, source } => {
            do_import(db_args, source, cli.max_parallelism).await?;
        }
        Commands::Copy(copy_args) => {
            do_copy(copy_args, cli.max_parallelism).await?;
        }
    }

    Ok(())
}

#[instrument(skip_all)]
async fn do_export(
    db_args: ExportDbArgs,
    destination: Storage,
    max_parallelism: NonZeroUsize,
) -> Result<()> {
    let connection_string = db_args.get_connection_string();

    let source_connection = PostgresClientWrapper::new(&connection_string).await?;
    let source = PostgresInstanceStorage::new(&source_connection).await?;

    let copy_data_options = CopyDataOptions {
        max_parallel: Some(max_parallelism),
        target_schema: db_args.source_schema.clone(),
        schema_only: db_args.schema_only,
        data_format: None,
        rename_schema_to: None,
        differential: false,
    };

    match destination {
        Storage::SqlFile {
            path,
            max_rows_per_insert,
            format,
            max_commands_per_chunk,
        } => {
            let mut sql_file_destination = elefant_tools::SqlFile::new_file(
                &path,
                source.get_identifier_quoter(),
                SqlFileOptions {
                    max_rows_per_insert,
                    data_mode: format,
                    max_commands_per_chunk,
                    chunk_separator: SqlFileOptions::default().chunk_separator,
                },
            )
            .await?;

            copy_data(&source, &mut sql_file_destination, copy_data_options).await?;
        } // Storage::SqlDirectory { path } => Box::new(crate::SqlDirectoryDestination::new(path)),
          // Storage::ElefantFile { path } => Box::new(crate::ElefantFileDestination::new(path)),
          // Storage::ElefantDirectory { path } => Box::new(crate::ElefantDirectoryDestination::new(path)),
    }

    Ok(())
}

#[instrument(skip_all)]
async fn do_import(db_args: ImportDbArgs, source: Storage, _usize: NonZeroUsize) -> Result<()> {
    let connection_string = db_args.get_connection_string();

    let target_connection = PostgresClientWrapper::new(&connection_string).await?;
    match source {
        Storage::SqlFile { path, .. } => {
            let file = tokio::fs::File::open(path).await?;
            let mut reader = tokio::io::BufReader::new(file);
            apply_sql_file(&mut reader, &target_connection).await?;
        }
    }

    Ok(())
}

#[instrument(skip_all)]
async fn do_copy(copy_args: CopyArgs, max_parallel: NonZeroUsize) -> Result<()> {
    let source_connection =
        PostgresClientWrapper::new(&copy_args.source.get_connection_string()).await?;
    let source = PostgresInstanceStorage::new(&source_connection).await?;

    let target_connection =
        PostgresClientWrapper::new(&copy_args.target.get_connection_string()).await?;
    let mut target = PostgresInstanceStorage::new(&target_connection).await?;

    copy_data(
        &source,
        &mut target,
        CopyDataOptions {
            data_format: None,
            max_parallel: Some(max_parallel),
            rename_schema_to: copy_args.target.target_schema,
            target_schema: copy_args.source.source_schema.clone(),
            schema_only: copy_args.source.schema_only,
            differential: copy_args.differential,
        },
    )
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use elefant_test_macros::pg_test;
    use elefant_tools::test_helpers::TestHelper;
    use elefant_tools::{test_helpers, SqlDataMode};

    #[pg_test(arg(postgres = 16), arg(postgres = 16))]
    async fn test_export_import(source: &TestHelper, destination: &TestHelper) {
        source
            .execute_not_query(
                r#"
        create table test_table(id int);
        insert into test_table(id) values (1);
        "#,
            )
            .await;

        let sql_file_path = format!(
            "test_items/import_export_{}_{}_inserts.sql",
            source.port, destination.port
        );
        let export_parameters = cli::Cli {
            max_parallelism: NonZeroUsize::new(1).unwrap(),
            command: Commands::Export {
                destination: Storage::SqlFile {
                    path: sql_file_path.clone(),
                    max_rows_per_insert: 1000,
                    format: SqlDataMode::InsertStatements,
                    max_commands_per_chunk: 5,
                },
                db_args: ExportDbArgs::from_test_helper(source),
            },
        };

        run(export_parameters).await.unwrap();

        let import_parameters = cli::Cli {
            max_parallelism: NonZeroUsize::new(1).unwrap(),
            command: Commands::Import {
                source: Storage::SqlFile {
                    path: sql_file_path,
                    max_rows_per_insert: 1000,
                    format: SqlDataMode::InsertStatements,
                    max_commands_per_chunk: 5,
                },
                db_args: ImportDbArgs::from_test_helper(destination),
            },
        };

        run(import_parameters).await.unwrap();

        let rows = destination
            .get_single_results::<i32>("select id from test_table;")
            .await;
        assert_eq!(rows, vec![1]);
    }

    #[pg_test(arg(postgres = 16), arg(postgres = 16))]
    async fn test_export_import_sql_file_copy(source: &TestHelper, destination: &TestHelper) {
        source
            .execute_not_query(
                r#"
        create table test_table(id int);
        insert into test_table(id) values (1);
        "#,
            )
            .await;

        let sql_file_path = format!(
            "test_items/import_export_{}_{}_copy.sql",
            source.port, destination.port
        );
        let export_parameters = cli::Cli {
            max_parallelism: NonZeroUsize::new(1).unwrap(),
            command: Commands::Export {
                destination: Storage::SqlFile {
                    path: sql_file_path.clone(),
                    max_rows_per_insert: 1000,
                    format: SqlDataMode::CopyStatements,
                    max_commands_per_chunk: 5,
                },
                db_args: ExportDbArgs::from_test_helper(source),
            },
        };

        run(export_parameters).await.unwrap();

        let import_parameters = cli::Cli {
            max_parallelism: NonZeroUsize::new(1).unwrap(),
            command: Commands::Import {
                source: Storage::SqlFile {
                    path: sql_file_path,
                    max_rows_per_insert: 1000,
                    format: SqlDataMode::CopyStatements,
                    max_commands_per_chunk: 5,
                },
                db_args: ImportDbArgs::from_test_helper(destination),
            },
        };

        run(import_parameters).await.unwrap();

        let rows = destination
            .get_single_results::<i32>("select id from test_table;")
            .await;
        assert_eq!(rows, vec![1]);
    }

    #[pg_test(arg(postgres = 16), arg(postgres = 16))]
    async fn test_copy(source: &TestHelper, destination: &TestHelper) {
        source
            .execute_not_query(
                r#"
        create table test_table(id int);
        insert into test_table(id) values (1);
        "#,
            )
            .await;

        let parameters = cli::Cli {
            max_parallelism: NonZeroUsize::new(1).unwrap(),
            command: Commands::Copy(CopyArgs {
                source: ExportDbArgs::from_test_helper(source),
                target: ImportDbArgs::from_test_helper(destination),
                differential: false,
            }),
        };

        run(parameters).await.unwrap();

        let rows = destination
            .get_single_results::<i32>("select id from test_table;")
            .await;
        assert_eq!(rows, vec![1]);
    }

    #[pg_test(arg(postgres = 16), arg(postgres = 16))]
    async fn test_copy_between_schemas(source: &TestHelper, destination: &TestHelper) {
        source
            .execute_not_query(
                r#"
        create schema source;
        set search_path = source;
        create table test_table(id int);
        insert into test_table(id) values (1);
        "#,
            )
            .await;

        let parameters = cli::Cli {
            max_parallelism: NonZeroUsize::new(1).unwrap(),
            command: Commands::Copy(CopyArgs {
                source: ExportDbArgs {
                    source_schema: Some("source".to_string()),
                    ..ExportDbArgs::from_test_helper(source)
                },
                target: ImportDbArgs {
                    target_schema: Some("target".to_string()),
                    ..ImportDbArgs::from_test_helper(destination)
                },
                differential: false,
            }),
        };

        run(parameters).await.unwrap();

        let rows = destination
            .get_single_results::<i32>("select id from target.test_table;")
            .await;
        assert_eq!(rows, vec![1]);
    }
}

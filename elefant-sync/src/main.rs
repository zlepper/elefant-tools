use clap::Parser;
use crate::cli::{Commands, ExportDbArgs, ImportDbArgs, Storage};

mod cli;

use elefant_tools::{copy_data, CopyDataOptions, PostgresInstanceStorage, Result, SqlFileOptions};
use elefant_tools::PostgresClientWrapper;


#[tokio::main]
async fn main() -> Result<()> {
    let cli = cli::Cli::parse();

    println!("cli: {:?}", cli);

    run(cli).await
}

async fn run(cli: cli::Cli) -> Result<()> {

    match cli.command {
        Commands::Export {
            db_args, destination
        } => {
            do_export(db_args, destination).await?;
        }
        Commands::Import { .. } => {}
        Commands::Copy(_) => {}
    }

    Ok(())
}

async fn do_export(db_args: ExportDbArgs, destination: Storage) -> Result<()> {
    
    let connection_string = db_args.get_connection_string();

    let source_connection = PostgresClientWrapper::new(&connection_string).await?;
    let source = PostgresInstanceStorage::new(&source_connection).await?;

    match destination {
        Storage::SqlFile { path , max_rows_per_insert } => {
            let mut sql_file_destination = elefant_tools::SqlFile::new_file(&path, source.get_identifier_quoter(), SqlFileOptions {
                max_rows_per_insert,
                ..SqlFileOptions::default()
            }).await?;
            
            copy_data(&source, &mut sql_file_destination, CopyDataOptions::default()).await?;
        },
        // Storage::SqlDirectory { path } => Box::new(crate::SqlDirectoryDestination::new(path)),
        // Storage::ElefantFile { path } => Box::new(crate::ElefantFileDestination::new(path)),
        // Storage::ElefantDirectory { path } => Box::new(crate::ElefantDirectoryDestination::new(path)),
    }
    
    Ok(())
}

async fn do_import(db_args: ImportDbArgs, source: Storage) -> Result<()> {


    let connection_string = db_args.get_connection_string();

    let target_connection = PostgresClientWrapper::new(&connection_string).await?;
    match source {
        Storage::SqlFile { path, .. } => {
            
        }
    }
    
    Ok(())
}
use std::thread;
use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about)]
#[command(propagate_version = true)]
/// A replacement for db_dump and db_restore that supports advanced processing such as moving between schemas.
///
/// This tool is currently experimental and any use in production is purely on the user. Backups are recommended.
pub struct Cli {
    #[clap(subcommand)]
    pub command: Commands,

    /// How many threads to use when exporting or importing. Defaults to the number of number of estimated cores
    /// on the machine. If the available parallelism cannot be determined, it defaults to 1.
    #[arg(long, default_value_t = get_default_max_parallelism())]
    pub max_parallelism: usize,
}

fn get_default_max_parallelism() -> usize {
    thread::available_parallelism().map(|v| v.get()).unwrap_or(1)
}


#[derive(Subcommand, Debug, Clone)]
pub enum Commands {
    /// Export a database schema to a file or directory to be imported later on
    Export {
        #[command(flatten)]
        db_args: ExportDbArgs,

        #[clap(subcommand)]
        destination: Storage,
    },
    /// Import a database schema from a file or directory that was made using the export command
    Import {
        #[command(flatten)]
        db_args: ImportDbArgs,

        #[clap(subcommand)]
        source: Storage,
    },
    /// Copy a database schema from one database to another
    Copy(CopyArgs),
}

#[derive(Args, Debug, Clone)]
pub struct ExportDbArgs {

    /// The host of the source database to export from
    #[arg(long)]
    pub source_db_host: String,

    /// The port of the source database to export from
    #[arg(long, default_value_t = 5432)]
    pub source_db_port: u16,

    /// The username to use when connecting to the source database
    #[arg(long)]
    pub source_db_user: String,

    /// The password to use when connecting to the source database
    #[arg(long)]
    pub source_db_password: String,

    /// The name of the source database to export from
    #[arg(long)]
    pub source_db_name: String,

    /// The schema to export. If not specified, all schemas will be exported
    #[arg(long)]
    pub schema: Option<String>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Storage {

    /// Export to a single SQL file. This files can be run directly against postgres without needing the
    /// elefant-sync tool to import it, however no additional processing can be done during import.
    /// This is only recommended for very small databases. For larger databases, use one of the Elefant options.
    SqlFile {
        #[arg(long)]
        path: String,
    },

    /// Export to a directory of SQL files. This directory can be run directly against postgres without needing the
    /// elefant-sync tool to import it, however no additional processing can be done during import.
    /// Filenames are specified so files can be imported in alphabetical order.
    /// This is only recommended for very small databases. For larger databases, use one of the Elefant options.
    SqlDirectory {
        #[arg(long)]
        path: String,
    },

    /// Export to a single 'Elefant' file. This file can be imported later on using the import command
    /// and supports advanced processing such as moving between schemas or only importing certain schemas or tables
    ElefantFile {
        #[arg(long)]
        path: String,
    },

    /// Export to a directory of 'Elefant' files. This directory can be imported later on using the import command
    /// and supports advanced processing such as moving between schemas or only importing certain schemas or tables.
    /// The benefit of this over the single file is that it is easier to manage in source control
    /// and supports parallel processing of the export and import command.
    ElefantDirectory {
        #[arg(long)]
        path: String,
    },
}

#[derive(Args, Debug, Clone)]
pub struct ImportDbArgs {
    /// The host of the target database to import to
    #[arg(long)]
    pub target_db_host: String,
    /// The port of the target database to import to
    #[arg(long, default_value_t = 5432)]
    pub target_db_port: u16,
    /// The username to use when connecting to the target database
    #[arg(long)]
    pub target_db_user: String,
    /// The password to use when connecting to the target database
    #[arg(long)]
    pub target_db_password: String,
    /// The name of the target database to import to
    #[arg(long)]
    pub target_db_name: String,
}

#[derive(Args, Debug, Clone)]
pub struct CopyArgs {
    #[command(flatten)]
    pub source: ExportDbArgs,
    #[command(flatten)]
    pub target: ImportDbArgs,
}


#[test]
fn verify_cli() {
    use clap::CommandFactory;
    Cli::command().debug_assert()
}

use clap::Parser;
use crate::cli::Commands;

mod cli;


fn main() {
    let cli = cli::Cli::parse();

    println!("cli: {:?}", cli);

    match cli.command {
        Commands::Export {
            db_args, destination
        } => {

        }
        Commands::Import { .. } => {}
        Commands::Copy(_) => {}
    }

}

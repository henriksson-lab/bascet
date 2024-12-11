use std::process::ExitCode;

use clap::{Parser, Subcommand};
use robert::command;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Prepare(command::Prepare),
    Index(command::Index),
    Query(command::Query),
    Count(command::Count),
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Prepare(mut cmd) => cmd.try_execute(),
        Commands::Query(mut cmd) => cmd.try_execute(),
        Commands::Index(mut cmd) => cmd.try_execute(),
        Commands::Count(mut cmd) => cmd.try_execute(),
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        return ExitCode::FAILURE;
    }
    return ExitCode::SUCCESS;
}

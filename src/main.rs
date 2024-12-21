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
    Assemble(command::Assemble),
    Count(command::Count),
    Featurise(command::Featurise),
    Index(command::Index),
    Prepare(command::Prepare),
    Query(command::Query),
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Assemble(mut cmd) => cmd.try_execute(),
        Commands::Count(mut cmd) => cmd.try_execute(),
        Commands::Featurise(mut cmd) => cmd.try_execute(),
        Commands::Index(mut cmd) => cmd.try_execute(),
        Commands::Prepare(mut cmd) => cmd.try_execute(),
        Commands::Query(mut cmd) => cmd.try_execute(),
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        return ExitCode::FAILURE;
    }
    return ExitCode::SUCCESS;
}

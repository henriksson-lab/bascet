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
    Features(command::Markers),
    Split(command::Split),
    Query(command::Query),
}

fn main() {
    let mut cli = Cli::parse();

    let result = match cli.command {
        Commands::Features(ref mut cmd) => cmd.try_execute(),
        Commands::Split(ref mut cmd) => cmd.try_execute(),
        Commands::Query(ref mut cmd) => cmd.try_execute(),
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

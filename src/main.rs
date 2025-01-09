use std::process::ExitCode;

use clap::{Parser, Subcommand};

use robert::command;
use robert::cmd;


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
    Query(command::Query),

    Getraw(cmd::GetRawCMD),
    Mapcell(cmd::MapCellCMD),
    Extract(cmd::ExtractCMD),
    Shardify(cmd::ShardifyCMD),
    BuildKMERdatabase(cmd::BuildKMERdatabaseCMD)
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    

    env_logger::init();

    let result = match cli.command {
        Commands::Assemble(mut cmd) => cmd.try_execute(),
        Commands::Count(mut cmd) => cmd.try_execute(),
        Commands::Featurise(mut cmd) => cmd.try_execute(),
        Commands::Query(mut cmd) => cmd.try_execute(),
        Commands::Getraw(mut cmd) => cmd.try_execute(),
        Commands::Mapcell(mut cmd) => cmd.try_execute(),
        Commands::Extract(mut cmd) => cmd.try_execute(),
        Commands::Shardify(mut cmd) => cmd.try_execute(),
        Commands::BuildKMERdatabase(mut cmd) => cmd.try_execute(),
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        return ExitCode::FAILURE;
    }
    return ExitCode::SUCCESS;
}

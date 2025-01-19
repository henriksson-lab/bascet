use std::process::ExitCode;

use clap::{Parser, Subcommand};

use robert::cmd;


#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Getraw(cmd::GetRawCMD),
    Mapcell(cmd::MapCellCMD),
    Extract(cmd::ExtractCMD),
    Shardify(cmd::ShardifyCMD),
    Transform(cmd::TransformCMD),
    Featurise(cmd::FeaturiseCMD),
    Query(cmd::QueryCMD),
    Bam2fragments(cmd::Bam2FragmentsCMD),
    Kraken(cmd::KrakenCMD),
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    

    env_logger::init();

    let result = match cli.command {
        Commands::Getraw(mut cmd) => cmd.try_execute(),
        Commands::Mapcell(mut cmd) => cmd.try_execute(),
        Commands::Extract(mut cmd) => cmd.try_execute(),
        Commands::Shardify(mut cmd) => cmd.try_execute(),
        Commands::Transform(mut cmd) => cmd.try_execute(),
        Commands::Featurise(mut cmd) => cmd.try_execute(),
        Commands::Query(mut cmd) => cmd.try_execute(),
        Commands::Bam2fragments(mut cmd) => cmd.try_execute(),
        Commands::Kraken(mut cmd) => cmd.try_execute(),
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        return ExitCode::FAILURE;
    }
    return ExitCode::SUCCESS;
}

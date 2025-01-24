use std::process::ExitCode;

use clap::{Parser, Subcommand};

use bascet::subcommands;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Prepare(subcommands::Prepare),
    Mapcell(subcommands::MapCellCMD),
    Extract(subcommands::ExtractCMD),
    Shardify(subcommands::ShardifyCMD),
    Transform(subcommands::TransformCMD),
    Featurise(subcommands::FeaturiseCMD),
    Query(subcommands::QueryCMD),
    Bam2fragments(subcommands::Bam2FragmentsCMD),
    Kraken(subcommands::KrakenCMD),
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    env_logger::init();

    let result = match cli.command {
        Commands::Prepare(mut cmd) => cmd.try_execute(),
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

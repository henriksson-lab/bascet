use std::{panic, process::ExitCode};

use clap::{Parser, Subcommand};

use bascet::command;


#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Getraw(command::GetRawCMD),
    Mapcell(command::MapCellCMD),
    Extract(command::ExtractCMD),
    Shardify(command::ShardifyCMD),
    Transform(command::TransformCMD),
    Featurise(command::FeaturiseKmcCMD),
    MinhashHist(command::MinhashHistCMD),
    QueryKmc(command::QueryKmcCMD),
    QueryFq(command::QueryFqCMD),
    Bam2fragments(command::Bam2FragmentsCMD),
    Kraken(command::KrakenCMD),
    Countchrom(command::CountChromCMD),
    PipeSamAddTags(command::PipeSamAddTagsCMD),
    Countsketch(command::countsketch_mat::CountsketchCMD)
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    

    env_logger::init();

    //Ensure that a panic in a thread results in the entire program terminating
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        orig_hook(panic_info);
        std::process::exit(1);
    }));

    let result = match cli.command {
        Commands::Getraw(mut cmd) => cmd.try_execute(),
        Commands::Mapcell(mut cmd) => cmd.try_execute(),
        Commands::Extract(mut cmd) => cmd.try_execute(),
        Commands::Shardify(mut cmd) => cmd.try_execute(),
        Commands::Transform(mut cmd) => cmd.try_execute(),
        Commands::Featurise(mut cmd) => cmd.try_execute(),
        Commands::MinhashHist(mut cmd) => cmd.try_execute(),
        Commands::QueryKmc(mut cmd) => cmd.try_execute(),
        Commands::QueryFq(mut cmd) => cmd.try_execute(),
        Commands::Bam2fragments(mut cmd) => cmd.try_execute(),
        Commands::Kraken(mut cmd) => cmd.try_execute(),
        Commands::Countchrom(mut cmd) => cmd.try_execute(),
        Commands::PipeSamAddTags(mut cmd) => cmd.try_execute(),
        Commands::Countsketch(mut cmd) => cmd.try_execute()
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        return ExitCode::FAILURE;
    }
    return ExitCode::SUCCESS;
}

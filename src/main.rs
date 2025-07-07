use bascet::{command, runtime};
use clap::{Parser, Subcommand};
use std::{process::ExitCode};

///////////////////////////////
/// Parser for commandline options, top level
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    
    #[arg(long, default_value = "skip")]
    error_mode: runtime::ErrorMode,

    #[arg(long, default_value = "trace")]
    log_level: runtime::LogLevel,

    #[arg(long, default_value = "both")]
    log_mode: runtime::LogMode,

    #[arg(long, default_value = "./latest.log")]
    log_path: std::path::PathBuf,
}

///////////////////////////////
/// Possible subcommands to parse
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
    Countfeature(command::CountFeatureCMD),
    PipeSamAddTags(command::PipeSamAddTagsCMD),
    Countsketch(command::countsketch_mat::CountsketchCMD),
    ExtractStream(command::ExtractStreamCMD),
}

///////////////////////////////
/// Entry point into the software
fn main() -> ExitCode {
    let start = std::time::Instant::now();
    let cli = Cli::parse();

    let _config = runtime::CONFIG.set(runtime::Config {
        error_mode: cli.error_mode.clone(),
        log_level: cli.log_level.clone(),
        log_mode: cli.log_mode.clone(),
        log_path: cli.log_path.clone()
    });

    let _logger = runtime::setup_global_logger(
        runtime::CONFIG.get().unwrap().log_level,
        runtime::CONFIG.get().unwrap().log_mode,
        runtime::CONFIG.get().unwrap().log_path.clone()
    );
    
    // Now you can use slog_scope::info!, error!, etc. anywhere
    slog_scope::info!(
        "Application starting"; 
        "Log Level" => ?cli.log_level,
        "Error Handling Mode" => ?cli.error_mode
    );

    // let tirp_path = std::path::PathBuf::from("./data/filtered.1.tirp.gz");
    // let file = bascet::io::File::new(tirp_path).unwrap();
    // let mut reader =
    //     bascet::io::TirpDefaultReader::from_file(&file).expect("Failed to open TIRP file");
    // let cell_ids = reader.list_cells();
    // println!("TIRP cell IDs: {:?}", cell_ids);

    
    // env_logger::init();

    // //Ensure that a panic in a thread results in the entire program terminating
    // let orig_hook = panic::take_hook();
    // panic::set_hook(Box::new(move |panic_info| {
    //     orig_hook(panic_info);
    //     std::process::exit(1);
    // }));

    // let result = match cli.command {
    //     Commands::Getraw(mut cmd) => cmd.try_execute(),

    //     Commands::Mapcell(mut cmd) => cmd.try_execute(), // NOTE

    //     Commands::Extract(mut cmd) => cmd.try_execute(),
    //     Commands::Shardify(mut cmd) => cmd.try_execute(),
    //     Commands::Transform(mut cmd) => cmd.try_execute(),
    //     Commands::Featurise(mut cmd) => cmd.try_execute(),
    //     Commands::MinhashHist(mut cmd) => cmd.try_execute(),
    //     Commands::QueryKmc(mut cmd) => cmd.try_execute(),
    //     Commands::QueryFq(mut cmd) => cmd.try_execute(),
    //     Commands::Bam2fragments(mut cmd) => cmd.try_execute(),
    //     Commands::Kraken(mut cmd) => cmd.try_execute(),
    //     Commands::Countchrom(mut cmd) => cmd.try_execute(),
    //     Commands::Countfeature(mut cmd) => cmd.try_execute(),
    //     Commands::PipeSamAddTags(mut cmd) => cmd.try_execute(),
    //     Commands::Countsketch(mut cmd) => cmd.try_execute(),
    //     Commands::ExtractStream(mut cmd) => cmd.try_execute(),
    // };

    // if let Err(e) = result {
    //     eprintln!("Error: {}", e);
    //     return ExitCode::FAILURE;
    // }

    // let duration = start.elapsed();
    // eprintln!("Total time elapsed: {:?}", duration);

    // 
    // eprintln!("Total time elapsed: {:?}", duration);

    // return ExitCode::SUCCESS;

    let duration = start.elapsed();

    slog_scope::info!(
        "Application exiting"; 
        "took" => ?duration
    );
    
    return ExitCode::SUCCESS;
}

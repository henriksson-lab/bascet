use bascet::{
    command,
    io::{BascetRead, BascetStream, StreamToken, TIRP},
    log_critical, log_error, log_info, runtime,
};
use clap::{Parser, Subcommand};
use std::{fmt, panic, process::ExitCode};

///////////////////////////////
/// Parser for commandline options, top level
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: runtime::Commands,

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
/// Entry point into the software
fn main() -> ExitCode {
    let start = std::time::Instant::now();
    let cli = Cli::parse();

    let _config = runtime::CONFIG.set(runtime::Config {
        error_mode: cli.error_mode,
        log_level: cli.log_level,
        log_mode: cli.log_mode,
        log_path: cli.log_path.clone(),
    });

    let _logger = runtime::setup_global_logger(
        runtime::CONFIG.get().unwrap().log_level,
        runtime::CONFIG.get().unwrap().log_mode,
        runtime::CONFIG.get().unwrap().log_path.clone(),
    );

    // Ensure that a panic in a thread results in the entire program terminating
    let panic_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // nicer formatting
        let msg = format!("\n\n{}\n", panic_info).replace('\n', "\n\t");
        slog_scope::crit!(""; "panicked" => %msg);

        if let Ok(mut guard) = runtime::ASYNC_GUARD.lock() {
            if let Some(async_guard) = guard.take() {
                drop(async_guard); // Waits for all logs to flush
            }
        }

        println!("Exiting, took: {:#?}", start.elapsed());

        panic_hook(panic_info);
        std::process::exit(1);
    }));

    log_info!("================================================");
    log_info!("Running Bascet"; "v" => env!("CARGO_PKG_VERSION"));
    log_info!(""; "Command" => ?cli.command, "Error Handling Mode" => %cli.error_mode);
    log_info!(""; "Log Mode" => %cli.log_mode, "Log Level" => %cli.log_level);

    match cli.log_mode {
        runtime::LogMode::Both | runtime::LogMode::Path => {
            log_info!(""; "Log Path" => cli.log_path.display());
        }
        _ => {}
    }
    log_info!("================================================");
    let path = "./data/filtered.1.tirp.gz";
    let tirp_file = match TIRP::File::new(path) {
        Ok(f) => f,
        Err((_, f)) => f,
    };
    let mut tirp_stream = TIRP::DefaultStream::from_tirp(&tirp_file);
    tirp_stream.set_reader_threads(6);
    tirp_stream.set_worker_threads(6);
    tirp_stream.par_map(|token| match token {
        StreamToken::Memory { cell_id, reads } => (),
        StreamToken::Disk { cell_id, path } => todo!(),
    });

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

    if let Ok(mut guard) = runtime::ASYNC_GUARD.lock() {
        if let Some(async_guard) = guard.take() {
            drop(async_guard); // Waits for all logs to flush
        }
    }

    println!("Exiting, took: {:#?}", start.elapsed());

    return ExitCode::SUCCESS;
}

use std::sync::atomic::{Ordering};

use bascet::command::{self, Commands};
use bascet_runtime::logging::{
    log_filter_parser, log_mode_parser, log_ordered_parser, log_strictness_parser,
    LogConfig, LogGuard, LogLevel, LogMode, LogOrdered, LogStrictness, LogStrictnessLayer
};
use clap::Parser;
use tracing::{error, info};

///////////////////////////////
/// Parser for commandline options, top level
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: command::Commands,

    #[arg(
        long = "log-strictness",
        default_value = "ignore",
        value_parser = log_strictness_parser!(LogStrictness)
    )]
    log_strictness: LogStrictness,

    #[arg(
        long = "log-level",
        default_value = "info",
        value_parser = log_filter_parser!(LogLevel)
    )]
    log_level: LogLevel,

    #[arg(
        long = "log-mode",
        default_value = "./latest.log",
        value_parser = log_mode_parser!(LogMode)
    )]
    log_mode: LogMode,

    #[arg(
        long = "log-ordered",
        default_value = "terminal",
        value_parser = log_ordered_parser!(LogOrdered)
    )]
    log_ordered: LogOrdered,
}

///////////////////////////////
/// Entry point into the software
fn main() -> std::process::ExitCode {
    let start = std::time::Instant::now();
    let cli = Cli::parse();

    //Output from these commands need to get out without any log text
    if let Commands::Sysinfo(mut cmd) = cli.command {
        _ = cmd.try_execute();
        return std::process::ExitCode::SUCCESS;
    } else if let Commands::Sysinfo(mut cmd) = cli.command {
        _ = cmd.try_execute();
        return std::process::ExitCode::SUCCESS;
    };

    LogGuard::with_config(LogConfig {
        level: cli.log_level,
        mode: cli.log_mode,
        order: cli.log_ordered,
        strictness: cli.log_strictness,
    });

    std::panic::set_hook(Box::new(move |panic_info| {
        LogStrictnessLayer::is_poisoned().store(true, Ordering::Release);

        let message = if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "Unknown panic".to_string()
        };

        let location = panic_info
            .location()
            .map(|loc| format!("{}:{}:{}", loc.file(), loc.line(), loc.column()))
            .unwrap_or_else(|| "unknown location".to_string());

        error!("Panic at {}:", location);
        for line in message.lines() {
            error!("  {}", line);
        }

        // HACK Capture backtrace if enabled. Otherwise output isn't pretty :)
        let backtrace = std::backtrace::Backtrace::capture();
        if backtrace.status() == std::backtrace::BacktraceStatus::Captured {
            error!("Backtrace:");
            for line in backtrace.to_string().lines() {
                error!("  {}", line.trim());
            }
        }

        error!(elapsed = ?start.elapsed(), "Failure!");
        LogGuard::flush();
        std::process::abort();
    }));

    info!("*=========================================================================*");
    info!(version = env!("CARGO_PKG_VERSION"), command = %cli.command, "Running Bascet");
    info!("---------------------------------------------------------------------------");

    let _result = match cli.command {
        Commands::Align(mut cmd) => cmd.try_execute(),
        Commands::Bam2fragments(mut cmd) => cmd.try_execute(),
        Commands::Countchrom(mut cmd) => cmd.try_execute(),
        Commands::Countfeature(mut cmd) => cmd.try_execute(),
        Commands::Countsketch(mut cmd) => cmd.try_execute(),
        Commands::Extract(mut cmd) => cmd.try_execute(),
        Commands::ExtractStream(mut cmd) => cmd.try_execute(),
        Commands::Featurise(mut cmd) => cmd.try_execute(),
        Commands::GetRaw(mut cmd) => cmd.try_execute(),
        Commands::Mapcell(mut cmd) => cmd.try_execute(),
        Commands::MinhashHist(mut cmd) => cmd.try_execute(),
        //Commands::KmcReads(mut cmd) => cmd.try_execute(),
        Commands::Kraken(mut cmd) => cmd.try_execute(),
        Commands::PipeSamAddTags(mut _cmd) => _cmd.try_execute(), // no longer needed?
        Commands::Qc(mut cmd) => cmd.try_execute(),
        Commands::Shardify(mut cmd) => cmd.try_execute(),
        Commands::Sysinfo(_cmd) => panic!("Captured in the wrong place"),
        Commands::ToFastq(mut cmd) => cmd.try_execute(),
        Commands::Transform(mut cmd) => cmd.try_execute(),
        Commands::DetectKmerKmc(mut cmd) => cmd.try_execute(),
        Commands::DetectKmerFq(mut cmd) => cmd.try_execute(),
    };

    info!(elapsed = ?start.elapsed(), "Success!");
    LogGuard::flush();

    return std::process::ExitCode::SUCCESS;
}

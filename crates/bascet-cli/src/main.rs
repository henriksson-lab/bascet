#[cfg(not(windows))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::sync::atomic::Ordering;

use bascet_cli::command::{self, Commands};
use bascet_runtime::logging::{
    LogConfig, LogGuard, LogLevel, LogMode, LogOrdered, LogStrictness, LogStrictnessLayer,
    log_filter_parser, log_mode_parser, log_ordered_parser, log_strictness_parser,
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

    //Output from these commands need to get out without any log text. The commands are responsible for some type of error handing
    //as Zorn must be able to parse the output
    match &cli.command {
        Commands::Sysinfo(cmd) => {
            return match cmd.try_execute() {
                Ok(()) => std::process::ExitCode::SUCCESS,
                Err(err) => {
                    eprintln!("Error: {err:#}");
                    std::process::ExitCode::FAILURE
                }
            };
        }
        Commands::ExtractStream(cmd) => {
            return match cmd.try_execute() {
                Ok(()) => std::process::ExitCode::SUCCESS,
                Err(err) => {
                    if !err.to_string().is_empty() {
                        eprintln!("Error: {err:#}");
                    }
                    std::process::ExitCode::FAILURE
                }
            };
        }
        Commands::Exttool(cmd) => {
            return match cmd.try_execute() {
                Ok(code) => code,
                Err(err) => {
                    eprintln!("Error: {err:#}");
                    std::process::ExitCode::FAILURE
                }
            };
        }
        _ => (),
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

    let result = match cli.command {
        Commands::Align(mut cmd) => cmd.try_execute(),
        Commands::Bam2fragments(mut cmd) => cmd.try_execute(),
        Commands::BamSort(mut cmd) => cmd.try_execute(),
        Commands::Countchrom(mut cmd) => cmd.try_execute(),
        Commands::Countfeature(mut cmd) => cmd.try_execute(),
        Commands::Countsketch(mut cmd) => cmd.try_execute(),
        Commands::Extract(mut cmd) => cmd.try_execute(),
        Commands::ExtractStream(_cmd) => panic!("Command handled in the wrong place"),
        Commands::Exttool(_cmd) => panic!("Command handled in the wrong place"),
        Commands::Filterbam(mut cmd) => cmd.try_execute(),
        #[cfg(feature = "fastqc")]
        Commands::Fastqc(mut cmd) => cmd.try_execute(),
        Commands::Featurise(mut cmd) => cmd.try_execute(),
        #[cfg(feature = "gecco")]
        Commands::Gecco(mut cmd) => cmd.try_execute(),
        Commands::Debarcode(mut cmd) => {
            // #[cfg(all(target_os = "linux", target_env = "gnu"))]
            // unsafe {
            //     // glibc otherwise creates many per-thread malloc arenas. On debarcode's
            //     // Rayon-heavy path those arenas showed up as dozens of resident 64 MiB
            //     // anonymous mappings, bypassing the command's own memory budget.
            //     libc::mallopt(libc::M_ARENA_MAX, 2);
            //     libc::mallopt(libc::M_MMAP_THRESHOLD, 4 * 1024);
            //     libc::mallopt(libc::M_TRIM_THRESHOLD, 128 * 1024);
            // }
            cmd.try_execute()
        }
        Commands::ImportSra(mut cmd) => cmd.try_execute(),
        Commands::Mapcell(mut cmd) => cmd.try_execute(),
        Commands::MinhashHist(mut cmd) => cmd.try_execute(),
        Commands::NcbiGenomeDownload(mut cmd) => cmd.try_execute(),
        //Commands::KmcReads(mut cmd) => cmd.try_execute(),
        Commands::Kraken(mut cmd) => cmd.try_execute(),
        Commands::PipeSamAddTags(mut _cmd) => _cmd.try_execute(), // no longer needed?
        Commands::Qc(mut cmd) => cmd.try_execute(),
        Commands::Shardify(mut cmd) => cmd.try_execute(),
        #[cfg(feature = "skesa")]
        Commands::Skesa(mut cmd) => cmd.try_execute(),
        Commands::Sysinfo(_cmd) => panic!("Command handled in the wrong place"),
        Commands::Tobigwig(mut cmd) => cmd.try_execute(),
        Commands::ToFastq(mut cmd) => cmd.try_execute(),
        Commands::Transform(mut cmd) => cmd.try_execute(),
        Commands::DetectKmerKmc(mut cmd) => cmd.try_execute(),
        Commands::DetectKmerFq(mut cmd) => cmd.try_execute(),
    };

    if let Err(e) = result {
        error!("Error occurred: {:#}", e);
        for (index, cause) in e.chain().skip(1).enumerate() {
            error!("  caused by {}: {}", index + 1, cause);
        }
        error!(elapsed = ?start.elapsed(), "Failure!");
        LogGuard::flush();
        return std::process::ExitCode::FAILURE;
    } else {
        info!(elapsed = ?start.elapsed(), "Success!");
    }

    LogGuard::flush();

    return std::process::ExitCode::SUCCESS;
}

#[cfg(test)]
mod tests {
    use super::*;
    use bascet_cli::command::getraw::GetRawChemistryCMD;
    use clap::Parser;
    use std::fs::File;

    fn fastq_path(dir: &tempfile::TempDir, name: &str) -> String {
        let path = dir.path().join(name);
        File::create(&path).unwrap();
        path.to_string_lossy().into_owned()
    }

    fn debarcode_command(argv: Vec<String>) -> command::getraw::GetRawCMD {
        let cli = Cli::try_parse_from(argv).unwrap();
        match cli.command {
            Commands::Debarcode(cmd) => cmd,
            _ => panic!("expected debarcode command"),
        }
    }

    #[test]
    fn debarcode_accepts_space_separated_fastq_options_before_chemistry() {
        let dir = tempfile::tempdir().unwrap();
        let r1 = fastq_path(&dir, "reads_R1.fastq.gz");
        let r2 = fastq_path(&dir, "reads_R2.fastq.gz");

        let cmd = debarcode_command(vec![
            "bascet".into(),
            "debarcode".into(),
            "-1".into(),
            r1,
            "-2".into(),
            r2,
            "atrandi-wgs".into(),
        ]);

        assert_eq!(cmd.paths_r1.len(), 1);
        assert_eq!(cmd.paths_r2.len(), 1);
        assert!(matches!(cmd.chemistry, GetRawChemistryCMD::AtrandiWGS));
    }

    #[test]
    fn debarcode_accepts_equals_separated_fastq_options() {
        let dir = tempfile::tempdir().unwrap();
        let r1 = fastq_path(&dir, "reads_R1.fastq.gz");
        let r2 = fastq_path(&dir, "reads_R2.fastq.gz");

        let cmd = debarcode_command(vec![
            "bascet".into(),
            "debarcode".into(),
            format!("--r1={r1}"),
            format!("--r2={r2}"),
            "atrandi-wgs".into(),
        ]);

        assert_eq!(cmd.paths_r1.len(), 1);
        assert_eq!(cmd.paths_r2.len(), 1);
        assert!(matches!(cmd.chemistry, GetRawChemistryCMD::AtrandiWGS));
    }

    #[test]
    fn debarcode_accepts_comma_separated_fastq_lists_without_swallowing_chemistry() {
        let dir = tempfile::tempdir().unwrap();
        let r1_a = fastq_path(&dir, "sample_a_R1.fastq.gz");
        let r1_b = fastq_path(&dir, "sample_b_R1.fastq.gz");
        let r2_a = fastq_path(&dir, "sample_a_R2.fastq.gz");
        let r2_b = fastq_path(&dir, "sample_b_R2.fastq.gz");

        let cmd = debarcode_command(vec![
            "bascet".into(),
            "debarcode".into(),
            "--r1".into(),
            format!("{r1_a},{r1_b}"),
            "--r2".into(),
            format!("{r2_a},{r2_b}"),
            "atrandi-wgs".into(),
        ]);

        assert_eq!(cmd.paths_r1.len(), 2);
        assert_eq!(cmd.paths_r2.len(), 2);
        assert!(matches!(cmd.chemistry, GetRawChemistryCMD::AtrandiWGS));
    }

    #[test]
    fn debarcode_rejects_chemistry_before_debarcode_options() {
        let dir = tempfile::tempdir().unwrap();
        let r1 = fastq_path(&dir, "reads_R1.fastq.gz");
        let r2 = fastq_path(&dir, "reads_R2.fastq.gz");

        let result = Cli::try_parse_from(vec![
            "bascet".into(),
            "debarcode".into(),
            "atrandi-wgs".into(),
            "-1".into(),
            r1,
            "-2".into(),
            r2,
        ]);

        let err = match result {
            Ok(_) => panic!("expected parser error"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
    }
}

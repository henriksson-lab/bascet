use std::process::ExitCode;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};

#[derive(Args)]
pub struct ExttoolCMD {
    #[command(subcommand)]
    tool: Exttool,
}

#[derive(Subcommand)]
enum Exttool {
    /// Run the wrapped BWA-MEM2 CLI.
    #[cfg(feature = "bwa-mem2-rs-align")]
    #[command(name = "bwa-mem2")]
    BwaMem2(NativeArgs),

    /// Run the wrapped GECCO CLI.
    #[cfg(feature = "gecco")]
    #[command(name = "gecco")]
    Gecco(NativeArgs),

    /// Run the wrapped FastQC CLI.
    #[cfg(feature = "fastqc")]
    #[command(name = "fastqc")]
    Fastqc(NativeArgs),

    /// Run the wrapped Kraken 2 CLI.
    #[command(name = "kraken2")]
    Kraken2(NativeArgs),

    /// Run the wrapped SKESA CLI.
    #[cfg(feature = "skesa")]
    #[command(name = "skesa")]
    Skesa(NativeArgs),

    /// Run the wrapped minimap2 CLI.
    #[cfg(feature = "minimap2-rs-align")]
    #[command(name = "minimap2")]
    Minimap2(NativeArgs),

    /// Run the wrapped STAR CLI.
    #[cfg(feature = "star-rs-align")]
    #[command(name = "STAR", visible_alias = "star")]
    Star(NativeArgs),
}

#[derive(Args)]
#[command(disable_help_flag = true)]
struct NativeArgs {
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    args: Vec<String>,
}

impl ExttoolCMD {
    pub fn try_execute(&self) -> Result<ExitCode> {
        match &self.tool {
            #[cfg(feature = "bwa-mem2-rs-align")]
            Exttool::BwaMem2(args) => run_bwa_mem2(&args.args),

            #[cfg(feature = "gecco")]
            Exttool::Gecco(args) => run_gecco(&args.args),

            #[cfg(feature = "fastqc")]
            Exttool::Fastqc(args) => run_fastqc(&args.args),

            Exttool::Kraken2(args) => run_kraken2(&args.args),

            #[cfg(feature = "skesa")]
            Exttool::Skesa(args) => run_skesa(&args.args),

            #[cfg(feature = "minimap2-rs-align")]
            Exttool::Minimap2(args) => run_minimap2(&args.args),

            #[cfg(feature = "star-rs-align")]
            Exttool::Star(args) => run_star(&args.args),
        }
    }
}

#[cfg(feature = "bwa-mem2-rs-align")]
fn run_bwa_mem2(args: &[String]) -> Result<ExitCode> {
    let argv = native_argv("bwa-mem2", args);
    let code = bwa_mem2_rs::generated::main_cpp::main(&argv);
    Ok(exit_code_from_i32(code))
}

#[cfg(feature = "gecco")]
fn run_gecco(args: &[String]) -> Result<ExitCode> {
    let cli = gecco::cli::Cli::try_parse_from(native_argv("gecco", args))?;
    match cli.command {
        gecco::cli::Commands::Run(args) => args.execute()?,
        gecco::cli::Commands::Annotate(args) => args.execute()?,
        gecco::cli::Commands::Predict(args) => args.execute()?,
        gecco::cli::Commands::Train(args) => args.execute()?,
        gecco::cli::Commands::Cv(args) => args.execute()?,
        gecco::cli::Commands::Convert(args) => args.execute()?,
        gecco::cli::Commands::BuildData(args) => args.execute()?,
        gecco::cli::Commands::UpdateInterpro(args) => args.execute()?,
    }
    Ok(ExitCode::SUCCESS)
}

#[cfg(feature = "fastqc")]
fn run_fastqc(args: &[String]) -> Result<ExitCode> {
    let code = fastqc_rs::cli::run_cli_from(native_argv("fastqc-compliant-rs", args));
    Ok(exit_code_from_i32(code))
}

fn run_kraken2(args: &[String]) -> Result<ExitCode> {
    let code = kraken2_pure_rs::cli::run_cli_from(native_argv("kraken2", args));
    Ok(exit_code_from_i32(code))
}

#[cfg(feature = "skesa")]
fn run_skesa(args: &[String]) -> Result<ExitCode> {
    let code = skesa_rs::cli::run_cli_from(native_argv("skesa-rs", args));
    Ok(exit_code_from_i32(code))
}

#[cfg(feature = "minimap2-rs-align")]
fn run_minimap2(args: &[String]) -> Result<ExitCode> {
    let code = minimap2::cli::run_cli_from(native_argv("minimap2", args));
    Ok(exit_code_from_i32(code))
}

#[cfg(feature = "star-rs-align")]
fn run_star(args: &[String]) -> Result<ExitCode> {
    let argv = native_argv("STAR", args);
    match star_rs::cli::run_cli(&argv) {
        Ok(result) => {
            let code = result.exit_code;
            star_rs::cli::print_result(&result);
            Ok(exit_code_from_i32(code))
        }
        Err(err) => anyhow::bail!(err),
    }
}

fn native_argv(program: &str, args: &[String]) -> Vec<String> {
    let mut argv = Vec::with_capacity(args.len() + 1);
    argv.push(program.to_string());
    argv.extend(args.iter().cloned());
    argv
}

fn exit_code_from_i32(code: i32) -> ExitCode {
    if code == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(code.clamp(1, u8::MAX as i32) as u8)
    }
}

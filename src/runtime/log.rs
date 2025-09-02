use slog::{o, Drain, Logger};
use slog_async::AsyncGuard;
use std::fmt;
use std::fs::{File};
use std::path::PathBuf;

use crate::utils::expand_and_resolve;

#[derive(Clone, Copy, Debug)]
pub enum ErrorMode {
    Suppress,
    Skip,
    Fail,
}

#[derive(Clone, Copy, Debug)]
pub struct LogLevel(pub slog::FilterLevel);
impl std::str::FromStr for LogLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let level = match s.to_lowercase().as_str() {
            "trace" => slog::FilterLevel::Trace,
            "debug" => slog::FilterLevel::Debug,
            "info" => slog::FilterLevel::Info,
            "warn" | "warning" => slog::FilterLevel::Warning,
            "error" => slog::FilterLevel::Error,
            "critical" | "crit" => slog::FilterLevel::Critical,
            "off" => slog::FilterLevel::Off,
            _ => return Err(format!("Invalid log level: {}", s)),
        };
        Ok(LogLevel(level))
    }
}
impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self.0 {
            slog::FilterLevel::Off => "off",
            slog::FilterLevel::Critical => "critical",
            slog::FilterLevel::Error => "errors",
            slog::FilterLevel::Warning => "warnings",
            slog::FilterLevel::Info => "info",
            slog::FilterLevel::Debug => "debug",
            slog::FilterLevel::Trace => "trace",
        };
        write!(f, "{}", name)
    }
}
impl From<LogLevel> for slog::FilterLevel {
    fn from(level: LogLevel) -> Self {
        level.0
    }
}

#[derive(Clone, Copy, Debug)]
pub enum LogMode {
    Both,
    Path,
    Terminal,
    Discard,
}
impl std::str::FromStr for LogMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mode = match s.to_lowercase().as_str() {
            "both" => LogMode::Both,
            "path" | "file" => LogMode::Path,
            "terminal" | "term" | "cli" => LogMode::Terminal,
            "discard" | "none" => LogMode::Discard,
            _ => return Err(format!("Invalid log mode: {}", s)),
        };
        Ok(mode)
    }
}
impl fmt::Display for LogMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            LogMode::Terminal => "terminal",
            LogMode::Path => "path",
            LogMode::Both => "file and path",
            LogMode::Discard => "discard",
        };
        write!(f, "{}", name)
    }
}

pub static ASYNC_GUARD: std::sync::Mutex<Option<AsyncGuard>> = std::sync::Mutex::new(None);
pub fn setup_global_logger(
    log_level: LogLevel,
    log_output: LogMode,
    log_path: PathBuf,
) -> Option<slog_scope::GlobalLoggerGuard> {
    let drain: Box<dyn Drain<Ok = (), Err = slog::Never> + Send> = match log_output {
        LogMode::Discard => {
            let drain = slog::Discard;
            Box::new(drain)
        }

        LogMode::Terminal => {
            let decorator = slog_term::TermDecorator::new().build();

            // Terminal drain (with colors)
            let drain = slog_term::FullFormat::new(decorator).build().fuse();
            Box::new(drain)
        }

        LogMode::Path => {
            let path = expand_and_resolve(log_path).unwrap();
            let file = File::create(path).expect("Failed to open log file");

            let decorator = slog_term::PlainDecorator::new(file);

            // File drain (same format, no colors)
            let drain = slog_term::FullFormat::new(decorator).build().fuse();
            Box::new(drain)
        }

        LogMode::Both => {
            let path = expand_and_resolve(log_path).unwrap();
            let file = File::create(path).expect("Failed to open log file");

            // Terminal drain (with colors)
            let term_decorator = slog_term::TermDecorator::new().build();
            let term_drain = slog_term::FullFormat::new(term_decorator).build().fuse();

            // File drain (same format, no colors)
            let file_decorator = slog_term::PlainDecorator::new(file);
            let file_drain = slog_term::FullFormat::new(file_decorator).build().fuse();

            // Combine both drains
            let drain = slog::Duplicate::new(term_drain, file_drain).fuse();
            Box::new(drain)
        }
    };

    // let drain = slog::LevelFilter::new(drain, log_level.0).fuse();
    // let drain = slog::Filter::new(drain, cond).fuse();
    let (drain, guard) = slog_async::Async::new(drain)
        .chan_size(10000)
        .build_with_guard();
    let drain = drain.fuse();
    // set global guard
    *ASYNC_GUARD.lock().unwrap() = Some(guard);

    let logger = Logger::root(drain, o!());
    Some(slog_scope::set_global_logger(logger))
}

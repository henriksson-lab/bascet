use slog::{Drain, Logger, o};
use std::path::PathBuf;
use std::fs::OpenOptions;

use crate::utils::expand_and_resolve_path;

#[derive(Clone, Copy, Debug)]
pub struct LogLevel(pub slog::Level);
impl std::str::FromStr for LogLevel {
    type Err = String;
    
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let level = match s.to_lowercase().as_str() {
            "trace" => slog::Level::Trace,
            "debug" => slog::Level::Debug,
            "info" => slog::Level::Info,
            "warn" | "warning" => slog::Level::Warning,
            "error" => slog::Level::Error,
            "critical" | "crit" => slog::Level::Critical,
            _ => return Err(format!("Invalid log level: {}", s)),
        };
        Ok(LogLevel(level))
    }
}

impl From<LogLevel> for slog::Level {
    fn from(level: LogLevel) -> Self {
        level.0
    }
}

#[derive(Clone, Copy, Debug)]
pub enum LogMode {
    Both,
    Path,
    Terminal,
    Discard
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


pub fn setup_global_logger(
    log_level: LogLevel, 
    log_output: LogMode, 
    log_path: PathBuf
) -> Option<slog_scope::GlobalLoggerGuard> {
    
    let drain: Box<dyn Drain<Ok = (), Err = slog::Never> + Send> = match log_output {
        LogMode::Discard => {
            let drain = slog::Discard;
            Box::new(drain)
        },
        
        LogMode::Terminal => {
            let decorator = slog_term::TermDecorator::new().build();

             // Terminal drain (with colors)
            let drain = slog_term::FullFormat::new(decorator).build().fuse();
            Box::new(drain)
        },
        
        LogMode::Path => {
            let path = expand_and_resolve_path(log_path).unwrap();
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(path)
                .expect("Failed to open log file");
            
            let decorator = slog_term::PlainDecorator::new(file);

            // File drain (same format, no colors)
            let drain = slog_term::FullFormat::new(decorator).build().fuse();
            Box::new(drain)
        },
        
        LogMode::Both => {
            let path = expand_and_resolve_path(log_path).unwrap();
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(path)
                .expect("Failed to open log file");
            
            // Terminal drain (with colors)
            let term_decorator = slog_term::TermDecorator::new().build();
            let term_drain = slog_term::FullFormat::new(term_decorator).build().fuse();
            
            // File drain (same format, no colors)
            let file_decorator = slog_term::PlainDecorator::new(file);
            let file_drain = slog_term::FullFormat::new(file_decorator).build().fuse();
            
            // Combine both drains
            let drain = slog::Duplicate::new(term_drain, file_drain).fuse();
            Box::new(drain)
        },
    };
    
    // Apply filtering and async
    let drain = slog::LevelFilter::new(drain, log_level.0).fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    
    let logger = Logger::root(drain, o!());
    Some(slog_scope::set_global_logger(logger))
}
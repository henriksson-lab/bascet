use tracing_subscriber::EnvFilter;

use crate::{LogMode, LogOrdered, LogStrictness};

pub type LogLevel = EnvFilter;
pub struct LogConfig {
    pub level: LogLevel,
    pub mode: LogMode,
    pub order: LogOrdered,
    pub strictness: LogStrictness,
}

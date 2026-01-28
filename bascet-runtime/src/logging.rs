mod config;
mod clap_parser;
mod logger;
mod strictness;
mod writer;

pub use config::{LogConfig, LogLevel};
pub use logger::{LogGuard, LogMode, LogOrdered};
pub use strictness::{LogStrictness, LogStrictnessLayer};

pub use tracing::{debug, error, info, trace, warn};

pub use crate::log_filter_parser;
pub use crate::log_mode_parser;
pub use crate::log_ordered_parser;
pub use crate::log_strictness_parser;

use std::sync::OnceLock;

use crate::runtime;

///////////////////////////////
/// Global Config Options
pub static CONFIG: OnceLock<Config> = OnceLock::new();

#[derive(Clone, Debug)]
pub struct Config {
    pub error_mode: runtime::ErrorMode,
    pub log_level: runtime::LogLevel,
    pub log_mode: runtime::LogMode,
    pub log_path: std::path::PathBuf
}

impl Config {
    pub fn get() -> &'static Config {
        CONFIG.get().expect("Config not initialized")
    }
}
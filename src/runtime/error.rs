use std::{fmt, str};

#[derive(Clone, Copy, Debug)]
pub enum ErrorMode {
    Suppress,
    Skip,
    Fail,
}

impl str::FromStr for ErrorMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let mode = match s.to_lowercase().as_str() {
            "supress" => ErrorMode::Suppress,
            "skip" => ErrorMode::Skip,
            "fail" => ErrorMode::Fail,
            _ => return Err(format!("Invalid error mode: {}", s)),
        };
        Ok(mode)
    }
}
impl fmt::Display for ErrorMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            ErrorMode::Fail => "fail",
            ErrorMode::Skip => "skip",
            ErrorMode::Suppress => "suppress",
        };
        write!(f, "{}", name)
    }
}

#[macro_export]
macro_rules! log_trace {
    ($($args:tt)*) => {
        slog_scope::trace!($($args)*);
    };
}

#[macro_export]
macro_rules! log_debug {
    ($($args:tt)*) => {
        slog_scope::debug!($($args)*);
    };
}

#[macro_export]
macro_rules! log_info {
    ($($args:tt)*) => {
        slog_scope::info!($($args)*);
    };
}

#[macro_export]
macro_rules! log_warning {
    // For direct warning logging
    ($msg:expr) => {{
        slog_scope::warn!($msg);
        if let Some(config) = $crate::runtime::CONFIG.get() {
            match config.error_mode {
                $crate::runtime::ErrorMode::Fail => {
                    panic!("Warning treated as fatal due to error mode");
                }
                _ => {}
            }
        }
    }};
    ($msg:expr; $($kv:tt)*) => {{
        slog_scope::warn!($msg; $($kv)*);
        if let Some(config) = $crate::runtime::CONFIG.get() {
            match config.error_mode {
                $crate::runtime::ErrorMode::Fail => {
                    panic!("Warning treated as fatal due to error mode");
                }
                _ => {}
            }
        }
    }};
    // For Result handling
    ($result:expr, $msg:expr) => {{
        match $result.as_ref() {
            Ok(_) => {},
            Err(e) => {
                slog_scope::warn!($msg; "error" => %e);
                if let Some(config) = $crate::runtime::CONFIG.get() {
                    match config.error_mode {
                        $crate::runtime::ErrorMode::Fail => {
                            panic!("Warning treated as fatal due to error mode");
                        }
                        _ => {}
                    }
                }
            }
        }
        $result
    }};
    ($result:expr, $msg:expr; $($kv:tt)*) => {{
        match $result.as_ref() {
            Ok(_) => {},
            Err(e) => {
                slog_scope::warn!($msg; $($kv)*, "error" => %e);
                if let Some(config) = $crate::runtime::CONFIG.get() {
                    match config.error_mode {
                        $crate::runtime::ErrorMode::Fail => {
                            panic!("Warning treated as fatal due to error mode");
                        }
                        _ => {}
                    }
                }
            }
        }
        $result
    }};
}

#[macro_export]
macro_rules! log_error {
    // For direct error logging
    ($msg:expr) => {{
        slog_scope::error!($msg);
        if let Some(config) = $crate::runtime::CONFIG.get() {
            match config.error_mode {
                $crate::runtime::ErrorMode::Fail => {
                    panic!("Error treated as fatal due to error mode");
                }
                _ => {}
            }
        }
    }};
    ($msg:expr; $($kv:tt)*) => {{
        slog_scope::error!($msg; $($kv)*);
        if let Some(config) = $crate::runtime::CONFIG.get() {
            match config.error_mode {
                $crate::runtime::ErrorMode::Fail => {
                    panic!("Error treated as fatal due to error mode");
                }
                _ => {}
            }
        }
    }};
    // For Result handling
    ($result:expr, $msg:expr) => {{
        match $result.as_ref() {
            Ok(_) => {},
            Err(e) => {
                slog_scope::error!($msg; "error" => %e);
                if let Some(config) = $crate::runtime::CONFIG.get() {
                    match config.error_mode {
                        $crate::runtime::ErrorMode::Fail => {
                            panic!("Error treated as fatal due to error mode");
                        }
                        _ => {}
                    }
                }
            }
        }
        $result
    }};
    ($result:expr, $msg:expr; $($kv:tt)*) => {{
        match $result.as_ref() {
            Ok(_) => {},
            Err(e) => {
                slog_scope::error!($msg; $($kv)*, "error" => %e);
                if let Some(config) = $crate::runtime::CONFIG.get() {
                    match config.error_mode {
                        $crate::runtime::ErrorMode::Fail => {
                            panic!("Error treated as fatal due to error mode");
                        }
                        _ => {}
                    }
                }
            }
        }
        $result
    }};
}

#[macro_export]
macro_rules! log_critical {
    // For direct critical logging and panic
    ($msg:expr) => {{
        slog_scope::crit!($msg);
        panic!("Critical error occurred");
    }};
    ($msg:expr; $($kv:tt)*) => {{
        slog_scope::crit!($msg; $($kv)*);
        panic!("Critical error occurred");
    }};
    // For Result handling - returns unwrapped Ok value
    ($result:expr, $msg:expr) => {{
        match $result {
            Ok(val) => val,
            Err(e) => {
                slog_scope::crit!($msg; "error" => %e);
                panic!("Critical error occurred");
            }
        }
    }};
    ($result:expr, $msg:expr; $($kv:tt)*) => {{
        match $result {
            Ok(val) => val,
            Err(e) => {
                slog_scope::crit!($msg; $($kv)*, "error" => %e);
                panic!("Critical error occurred");
            }
        }
    }};
}

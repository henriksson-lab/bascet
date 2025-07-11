use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("File at {:?} not found.", path)]
    FileNotFound { path: std::path::PathBuf },

    #[error("File at {:?} is invalid{}.", path, Error::format_msg_as_detail(msg))]
    FileNotValid {
        path: std::path::PathBuf,
        msg: Option<String>,
    },

    #[error(
        "Utility '{}' failed on execute \'{}\'{}",
        utility,
        cmd,
        Error::format_msg_as_detail(msg)
    )]
    UtilityExecutionError {
        utility: String,
        cmd: String,
        msg: Option<String>,
    },

    #[error(
        "Failed trying to execute utility '{utility}'. Make sure it is in your $PATH and you have execution permissions."
    )]
    UtilityNotExecutable { utility: String },

    #[error("Failed parsing {}{}", context, Error::format_msg_as_detail(msg))]
    ParseError {
        context: String,
        msg: Option<String>,
    },
}

impl Error {
    pub fn format_msg_as_detail(msg: &Option<String>) -> String {
        match msg {
            Some(m) => format!(" ({})", m),
            None => String::new(),
        }
    }
}

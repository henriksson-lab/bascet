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
    #[cold]
    pub fn file_not_found<P: AsRef<std::path::Path>>(path: P) -> Self {
        Error::FileNotFound {
            path: path.as_ref().to_path_buf(),
        }
    }

    #[cold]
    pub fn file_not_valid<P: AsRef<std::path::Path>, M: Into<String>>(
        path: P,
        msg: Option<M>,
    ) -> Self {
        Error::FileNotValid {
            path: path.as_ref().to_path_buf(),
            msg: msg.map(|m| m.into()),
        }
    }

    #[cold]
    pub fn utility_execution_error<U: Into<String>, C: Into<String>, M: Into<String>>(
        utility: U,
        cmd: C,
        msg: Option<M>,
    ) -> Self {
        Error::UtilityExecutionError {
            utility: utility.into(),
            cmd: cmd.into(),
            msg: msg.map(|m| m.into()),
        }
    }

    #[cold]
    pub fn utility_not_executable<U: Into<String>>(utility: U) -> Self {
        Error::UtilityNotExecutable {
            utility: utility.into(),
        }
    }

    #[cold]
    pub fn parse_error<C: Into<String>, M: Into<String>>(context: C, msg: Option<M>) -> Self {
        Error::ParseError {
            context: context.into(),
            msg: msg.map(|m| m.into()),
        }
    }

    pub fn format_msg_as_detail(msg: &Option<String>) -> String {
        match msg {
            Some(m) => format!(" ({})", m),
            None => String::new(),
        }
    }
}
use tracing::Level;

use crate::apply;
use crate::arena;
use crate::runtime;

#[derive(Debug, thiserror::Error)]
pub enum Exception {
    #[error(transparent)]
    Runtime(#[from] runtime::exception::Exception),
    #[error(transparent)]
    Arena(#[from] arena::Exception),
    #[error(transparent)]
    Apply(#[from] apply::Error),
}

pub trait Raise: std::error::Error + Into<Exception> + Sized {
    fn level(&self) -> Level;

    fn annotate(self) -> Raised<Self> {
        Raised {
            error: self,
            fixed: String::new(),
            help: String::new(),
            suggestion: String::new(),
        }
    }
}

pub struct Raised<E> {
    error: E,
    fixed: String,
    help: String,
    suggestion: String,
}

impl<E: Raise> Raised<E> {
    pub fn fixed(mut self, text: impl Into<String>) -> Self {
        self.fixed = text.into();
        self
    }

    pub fn help(mut self, text: impl Into<String>) -> Self {
        self.help = text.into();
        self
    }

    pub fn suggestion(mut self, text: impl Into<String>) -> Self {
        self.suggestion = text.into();
        self
    }

    pub fn raise(self) -> Result<(), E> {
        let Raised {
            error,
            fixed,
            help,
            suggestion,
        } = self;

        macro_rules! emit {
            ($level:ident) => {
                tracing::$level!(
                    fixed = fixed.as_str(),
                    help = help.as_str(),
                    suggestion = suggestion.as_str(),
                    "{error}"
                )
            };
        }

        let level = error.level();
        if level == Level::ERROR {
            emit!(error);
            return Err(error);
        }
        if level == Level::WARN {
            emit!(warn);
        } else if level == Level::INFO {
            emit!(info);
        } else if level == Level::DEBUG {
            emit!(debug);
        } else {
            emit!(trace);
        }
        Ok(())
    }
}

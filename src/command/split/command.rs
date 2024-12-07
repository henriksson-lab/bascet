use clap::Args;
use clio::{Input, Output};

use super::constants::{SPLIT_DEFAULT_PATH_IN, SPLIT_DEFAULT_PATH_OUT};

#[derive(Args)]
pub struct Command {
    #[arg(value_parser, default_value = SPLIT_DEFAULT_PATH_IN)]
    path_in: clio::Input,
    #[arg(value_parser, default_value = SPLIT_DEFAULT_PATH_OUT)]
    path_out: clio::Output,
}

impl Command {
    pub fn try_execute(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

use clap::Args;
use clio::{Input, Output};

use super::constants::{SPLIT_DEFAULT_ASSEMBLE, SPLIT_DEFAULT_PATH_OUT};

#[derive(Args)]
pub struct Command {
    #[arg(value_parser)]
    path_in: clio::Input,
    #[arg(value_parser, default_value = SPLIT_DEFAULT_PATH_OUT)]
    path_out: clio::Output,
    #[arg(value_parser, default_value = SPLIT_DEFAULT_ASSEMBLE)]
    assemble: bool,
    #[arg(long, value_parser = clap::value_parser!(u32))]
    pub threads_read: Option<u32>,
    #[arg(long, value_parser = clap::value_parser!(u32))]
    pub threads_write: Option<u32>,
}

impl Command {
    pub fn try_execute(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

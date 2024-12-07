use clap::Args;
use clio::{Input, Output};

use crate::command::features::constants::MARKERS_DEFAULT_PATH_OUT;

use super::constants::{
    QUERY_DEFAULT_PATH_OUT, QUERY_DEFAULT_PATH_RINDEX, QUERY_DEFAULT_PATH_RSPLIT,
};

#[derive(Args)]
pub struct Command {
    #[arg(value_parser, default_value = MARKERS_DEFAULT_PATH_OUT)]
    path_markers: clio::Input,
    #[arg(value_parser, default_value = QUERY_DEFAULT_PATH_RINDEX)]
    path_rindex: Input,
    #[arg(value_parser, default_value = QUERY_DEFAULT_PATH_RSPLIT)]
    path_rsplit: Input,
    #[arg(value_parser, default_value = QUERY_DEFAULT_PATH_OUT)]
    path_out: clio::Output,
}

impl Command {
    pub fn try_execute(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

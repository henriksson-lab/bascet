//! Alignment pipeline. The `command::align` module is a thin CLI wrapper that builds the
//! budget + thread allocation and dispatches to the per-aligner implementations here.
//!
//! Layout:
//! - `bwa` (+ `bwa_stock_driver`): pipelined BWAMEM2 stock-driver port (TIRP → BAM)
//! - `minimap2`, `star`: their respective aligner integrations
//! - `output`: shared BAM writer wrapper + cell-tag injection (used by minimap2/star)
//! - `stream_helpers`: memory-budget helpers shared by Stream-based aligners (minimap2/star)
//! - `common`: helpers used by the CLI dispatch (`warn_if_index_disk_size_exceeds_memory`)
//!   and the `tofq` subcommand (`write_tirp_to_2fq`)

pub mod common;
pub mod output;
#[cfg(any(feature = "star-rs-align", feature = "minimap2-rs-align"))]
pub mod stream_helpers;

#[cfg(feature = "bwa-mem2-rs-align")]
pub mod bwa;
#[cfg(feature = "bwa-mem2-rs-align")]
pub mod bwa_stock_driver;
#[cfg(feature = "minimap2-rs-align")]
pub mod minimap2;
#[cfg(feature = "star-rs-align")]
pub mod star;

//! Vendored copy of `samtools-rs` (https://github.com/henriksson-lab/samtools-rs at the time
//! of import) — pure-Rust port of `samtools sort` + `samtools index`. Used by the `bam-sort`
//! subcommand and by the post-alignment sort/index step in every aligner module.
//!
//! Only difference from the upstream sources: `crossbeam_channel::*` imports are rewritten
//! to `crossbeam::channel::*` to use the umbrella `crossbeam` crate already in our workspace
//! (instead of pulling in the standalone `crossbeam-channel` crate).

#![allow(dead_code)]

pub mod bam;
pub mod bgzf;
pub mod index;
pub mod sort;

pub(crate) mod bbiwrite;
pub mod beddata;
pub(crate) mod bigwigwrite;

use serde::{Deserialize, Serialize};

pub(crate) const BIGWIG_MAGIC: u32 = 0x888F_FC26;

pub(crate) const CIR_TREE_MAGIC: u32 = 0x2468_ACE0;
pub(crate) const CHROM_TREE_MAGIC: u32 = 0x78CA_8C91;

/// Info on a specific zoom level in a bbi file
#[derive(Copy, Clone, Debug)]
pub struct ZoomHeader {
    pub reduction_level: u32,
    pub(crate) data_offset: u64,
    pub(crate) index_offset: u64,
}

/// A single zoom item
#[derive(Copy, Clone, Debug)]
pub struct ZoomRecord {
    pub(crate) chrom: u32,
    pub start: u32,
    pub end: u32,
    pub summary: Summary,
}

/// A summary of a section of data (may be an entire file)
#[derive(Copy, Clone, Debug)]
pub struct Summary {
    pub total_items: u64,
    pub bases_covered: u64,
    pub min_val: f64,
    pub max_val: f64,
    pub sum: f64,
    pub sum_squares: f64,
}

/// Represents a single value in a bigWig file
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Value {
    pub start: u32,
    pub end: u32,
    pub value: f32,
}

pub use bbiwrite::*;
pub use bigwigwrite::*;

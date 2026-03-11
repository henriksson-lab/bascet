mod bbgz;
pub mod fastq;
pub mod tirp;

pub use bbgz::bbgz::{BBGZParser, BBGZBlock, bbgz_parser};
pub use fastq::Fastq;
pub use tirp::Tirp;

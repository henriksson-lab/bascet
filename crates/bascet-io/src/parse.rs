mod bbgz;
pub mod fastq;
pub mod tirp;

pub use bbgz::bbgz::{BBGZBlock, BBGZParser, bbgz_parser};
pub use fastq::Fastq;
pub use tirp::Tirp;

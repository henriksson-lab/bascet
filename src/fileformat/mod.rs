pub mod zip;
pub mod tirp;
pub mod mapcell_script;
pub mod shard;
pub mod cell_readpair_reader;
pub mod cell_list_file;
pub mod cram;
mod detect_fileformat;

pub use shard::CellID;
pub use shard::ReadPair;
pub use shard::CellUMI;

pub use shard::try_get_cells_in_file;

pub use cell_list_file::read_cell_list_file;

pub use detect_fileformat::DetectedFileformat;
pub use detect_fileformat::verify_input_fq_file;
pub use detect_fileformat::detect_shard_format;
pub use detect_fileformat::get_suitable_shard_reader;

pub use zip::ZipBascetShardReader;
pub use tirp::TirpBascetShardReader;
pub use shard::ShardReader;


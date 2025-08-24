////// File formats
pub mod cram;
pub mod list_fastq;
pub mod paired_fastq;
pub mod shard;
pub mod single_fastq;
pub mod tirp;
pub mod zip;
//pub mod count_matrix;
pub mod bam;

////// Utility
pub mod cell_list_file;
pub mod cell_readpair_reader;
mod detect_fileformat;
pub mod uuencode;

pub mod new_anndata;

pub mod iterate_shard_reader;

////// Re-exports
pub use shard::CellID;
pub use shard::CellUMI;
pub use shard::ReadPair;

pub use shard::try_get_cells_in_file;
pub use shard::ConstructFromPath;
pub use shard::ReadPairReader;
pub use shard::ReadPairWriter;
pub use shard::ShardCellDictionary;
pub use shard::ShardFileExtractor;
pub use shard::ShardRandomFileExtractor;
pub use shard::ShardStreamingFileExtractor;
pub use shard::StreamingReadPairReader;

pub use cell_list_file::read_cell_list_file;

pub use detect_fileformat::detect_shard_format;
pub use detect_fileformat::get_suitable_file_extractor;
pub use detect_fileformat::verify_input_fq_file;
pub use detect_fileformat::DetectedFileformat;

//pub use count_matrix::SparseCountMatrix;

//Readers
pub use zip::ZipBascetShardReader;

pub use tirp::TirpBascetShardReader;
pub use tirp::TirpStreamingShardReaderFactory;

pub use single_fastq::BascetSingleFastqWriter;

pub use list_fastq::ListFastqReader;

//Factories
pub use bam::BAMStreamingReadPairReaderFactory;
pub use list_fastq::ListFastqReaderFactory;
pub use single_fastq::BascetSingleFastqWriterFactory;
pub use tirp::TirpBascetShardReaderFactory;
pub use tirp::TirpStreamingReadPairReaderFactory;
pub use zip::ZipBascetShardReaderFactory;

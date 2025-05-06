
////// File formats
pub mod zip;
pub mod tirp;
pub mod shard;
pub mod cram;
pub mod single_fastq;
pub mod paired_fastq;
pub mod list_fastq;
//pub mod count_matrix;
pub mod bam;

////// Utility
pub mod cell_readpair_reader;
pub mod cell_list_file;
mod detect_fileformat;
pub mod uuencode;

pub mod new_anndata;


////// Re-exports
pub use shard::CellID;
pub use shard::ReadPair;
pub use shard::CellUMI;


pub use shard::StreamingReadPairReader;
pub use shard::ReadPairWriter;
pub use shard::ReadPairReader;
pub use shard::ShardCellDictionary;
pub use shard::ConstructFromPath;
pub use shard::ShardRandomFileExtractor;
pub use shard::ShardStreamingFileExtractor;
pub use shard::ShardFileExtractor;
pub use shard::try_get_cells_in_file;

pub use cell_list_file::read_cell_list_file;

pub use detect_fileformat::DetectedFileformat;
pub use detect_fileformat::verify_input_fq_file;
pub use detect_fileformat::detect_shard_format;
pub use detect_fileformat::get_suitable_file_extractor;


//pub use count_matrix::SparseCountMatrix;

//Readers
pub use zip::ZipBascetShardReader;

pub use tirp::TirpBascetShardReader;
pub use tirp::TirpStreamingShardReaderFactory;

pub use single_fastq::BascetSingleFastqWriter;

pub use list_fastq::ListFastqReader;


//Factories
pub use zip::ZipBascetShardReaderFactory;
pub use tirp::TirpBascetShardReaderFactory;
pub use tirp::TirpStreamingReadPairReaderFactory;
pub use bam::BAMStreamingReadPairReaderFactory;
pub use single_fastq::BascetSingleFastqWriterFactory;
pub use list_fastq::ListFastqReaderFactory;

// Module declarations (alphabetical)
pub mod _depreciated_getraw;
pub mod bam2fragments;
pub mod countchrom;
pub mod countfeature;
pub mod countsketch;
pub mod countsketch_mat;
pub mod extract;
pub mod extract_terminal;
pub mod featurise_kmc;
pub mod getraw;
pub mod kraken;
pub mod mapcell;
pub mod minhash_hist;
pub mod query_fq;
pub mod query_kmc;
pub mod sam_add_barcode_tag_cmd;
// pub mod shardify;
pub mod snpcall;
pub mod threadcount;
pub mod transform;

// BAM/SAM operations
pub use bam2fragments::{Bam2Fragments, Bam2FragmentsCMD};
pub use sam_add_barcode_tag_cmd::PipeSamAddTagsCMD;

// Count operations
pub use countchrom::{CountChrom, CountChromCMD};
pub use countfeature::{CountFeature, CountFeatureCMD};
pub use countsketch::CountsketchCMD;
pub use countsketch_mat::CountsketchMatCMD;

// Data processing operations
pub use _depreciated_getraw::{_depreciated_GetRaw, _depreciated_GetRawCMD};
// pub use shardify::ShardifyCMD;
pub use transform::{TransformCMD, TransformFile};

// Extract operations
pub use extract::ExtractCMD;
pub use extract_terminal::ExtractStreamCMD;

// Feature operations
pub use featurise_kmc::{FeaturiseKMC, FeaturiseKmcCMD, FeaturiseParamsKMC};

// Hashing operations
pub use minhash_hist::{MinhashHist, MinhashHistCMD};

// Mapping operations
pub use mapcell::{MapCell, MapCellCMD};

// Query operations
pub use query_fq::{QueryFq, QueryFqCMD};
pub use query_kmc::{QueryKmc, QueryKmcCMD, QueryKmcParams};

// Taxonomic classification
pub use kraken::KrakenCMD;

// Thread management
pub use threadcount::{
    determine_thread_counts_1, determine_thread_counts_2, determine_thread_counts_3,
};

// debarcoding
pub use getraw::GetRawCMD;

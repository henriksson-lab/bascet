use clap::Subcommand;

// Module declarations (alphabetical)
pub mod align;
pub mod bam2fragments;
pub mod countchrom;
pub mod countfeature;
pub mod countsketch;
pub mod sysinfo;
pub mod extract;
pub mod extract_terminal;
pub mod featurise_kmc;
pub mod getraw;
pub mod kraken;
pub mod kmc_reads;
//pub mod kmc_new;
pub mod mapcell;
pub mod minhash_hist;
pub mod query_fq;
pub mod query_kmc;
pub mod sam_add_barcode_tag_cmd;
pub mod shardify;
pub mod snpcall;
pub mod threadcount;
pub mod transform;
pub mod tofq;


// BAM/SAM operations
pub use bam2fragments::{Bam2Fragments, Bam2FragmentsCMD};
pub use sam_add_barcode_tag_cmd::PipeSamAddTagsCMD;
pub use align::AlignCMD;
pub use kmc_reads::KmcReadsCMD;

// Count operations
pub use countchrom::{CountChrom, CountChromCMD};
pub use countfeature::{CountFeature, CountFeatureCMD};
pub use countsketch::CountsketchCMD;
pub use extract::ExtractCMD;
pub use extract_terminal::ExtractStreamCMD;
pub use featurise_kmc::{FeaturiseKMC, FeaturiseKmcCMD, FeaturiseParamsKMC};
pub use getraw::GetRawCMD;
pub use mapcell::{MapCell, MapCellCMD};
pub use minhash_hist::{MinhashHist, MinhashHistCMD};
pub use query_fq::{QueryFq, QueryFqCMD};
pub use query_kmc::{QueryKmc, QueryKmcCMD, QueryKmcParams};

// Taxonomic classification
pub use kraken::KrakenCMD;

// Thread management
pub use shardify::ShardifyCMD;
pub use threadcount::{
    determine_thread_counts_1, determine_thread_counts_2, determine_thread_counts_3,
};
pub use transform::{TransformCMD, TransformFile};

use crate::command::{sysinfo::SysinfoCMD, tofq::ToFastqCMD};

///////////////////////////////
/// Possible subcommands to parse
#[derive(Subcommand, strum_macros::Display)]
#[allow(non_camel_case_types)]
pub enum Commands {
    Align(AlignCMD),
    Bam2fragments(Bam2FragmentsCMD),
    Countchrom(CountChromCMD),
    Countfeature(CountFeatureCMD),
    Countsketch(CountsketchCMD),
    //CountsketchMat(CountsketchMatCMD),
    Extract(ExtractCMD),
    ExtractStream(ExtractStreamCMD),
    Featurise(FeaturiseKmcCMD),
    GetRaw(GetRawCMD),
    //KmcReads(KmcReadsCMD),
    Kraken(KrakenCMD),
    Mapcell(MapCellCMD),
    MinhashHist(MinhashHistCMD),
    PipeSamAddTags(PipeSamAddTagsCMD), //Not needed for bascet anymore, but useful if anyone needs to use a non-standard aligner
    Shardify(ShardifyCMD),
    Sysinfo(SysinfoCMD),
    ToFastq(ToFastqCMD),
    Transform(TransformCMD),
    QueryKmc(QueryKmcCMD),
    QueryFq(QueryFqCMD),
}
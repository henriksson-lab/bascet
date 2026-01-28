use clap::Subcommand;

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
pub mod shardify;
pub mod snpcall;
pub mod threadcount;
pub mod transform;

pub use _depreciated_getraw::{_depreciated_GetRaw, _depreciated_GetRawCMD};
pub use bam2fragments::{Bam2Fragments, Bam2FragmentsCMD};
pub use countchrom::{CountChrom, CountChromCMD};
pub use countfeature::{CountFeature, CountFeatureCMD};
pub use countsketch::CountsketchCMD;
pub use countsketch_mat::CountsketchMatCMD;
pub use extract::ExtractCMD;
pub use extract_terminal::ExtractStreamCMD;
pub use featurise_kmc::{FeaturiseKMC, FeaturiseKmcCMD, FeaturiseParamsKMC};
pub use getraw::GetRawCMD;
pub use kraken::KrakenCMD;
pub use mapcell::{MapCell, MapCellCMD};
pub use minhash_hist::{MinhashHist, MinhashHistCMD};
pub use query_fq::{QueryFq, QueryFqCMD};
pub use query_kmc::{QueryKmc, QueryKmcCMD, QueryKmcParams};
pub use sam_add_barcode_tag_cmd::PipeSamAddTagsCMD;
pub use shardify::ShardifyCMD;
pub use threadcount::{
    determine_thread_counts_1, determine_thread_counts_2, determine_thread_counts_3,
};
pub use transform::{TransformCMD, TransformFile};

///////////////////////////////
/// Possible subcommands to parse
#[derive(Subcommand, strum_macros::Display)]
#[allow(non_camel_case_types)]
pub enum Commands {
    #[strum(to_string = "Getraw (depreciated)")]
    _depreciated_GetRaw(_depreciated_GetRawCMD),
    GetRaw(GetRawCMD),
    Mapcell(MapCellCMD),
    Extract(ExtractCMD),
    Shardify(ShardifyCMD),
    Transform(TransformCMD),
    Featurise(FeaturiseKmcCMD),
    MinhashHist(MinhashHistCMD),
    QueryKmc(QueryKmcCMD),
    QueryFq(QueryFqCMD),
    Bam2fragments(Bam2FragmentsCMD),
    Kraken(KrakenCMD),
    Countchrom(CountChromCMD),
    Countfeature(CountFeatureCMD),
    PipeSamAddTags(PipeSamAddTagsCMD),
    Countsketch(CountsketchCMD),
    CountsketchMat(CountsketchMatCMD),
    ExtractStream(ExtractStreamCMD),
}
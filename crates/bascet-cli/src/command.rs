use clap::Subcommand;

// Module declarations (alphabetical)
pub mod align;
#[cfg(feature = "bwa-mem2-rs-align")]
pub mod align_bwa;
pub mod bam2fragments;
pub mod countchrom;
pub mod countfeature;
pub mod countsketch;
pub mod extract;
pub mod extract_terminal;
#[cfg(feature = "fastqc")]
pub mod fastqc;
pub mod featurise_kmc;
#[cfg(feature = "gecco")]
pub mod gecco;
pub mod getraw;
pub mod kmc_reads;
pub mod kraken;
pub mod sysinfo;
//pub mod kmc_new;
pub mod detect_kmer_fq;
pub mod detect_kmer_kmc;
pub mod mapcell;
pub mod minhash_hist;
pub mod qc;
pub mod sam_add_barcode_tag_cmd;
pub mod shardify;
#[cfg(feature = "skesa")]
pub mod skesa;
pub mod snpcall;
pub mod threadcount;
pub mod tofq;
pub mod transform;

// BAM/SAM operations
pub use align::AlignCMD;
pub use bam2fragments::{Bam2Fragments, Bam2FragmentsCMD};
pub use kmc_reads::KmcReadsCMD;
pub use sam_add_barcode_tag_cmd::PipeSamAddTagsCMD;

// Count operations
pub use countchrom::{CountChrom, CountChromCMD};
pub use countfeature::{CountFeature, CountFeatureCMD};
pub use countsketch::CountsketchCMD;
pub use detect_kmer_fq::{DetectKmerFq, DetectKmerFqCMD};
pub use detect_kmer_kmc::{DetectKmerKmcCMD, QueryKmc, QueryKmcParams};
pub use extract::ExtractCMD;
pub use extract_terminal::ExtractStreamCMD;
#[cfg(feature = "fastqc")]
pub use fastqc::FastqcCMD;
pub use featurise_kmc::{FeaturiseKMC, FeaturiseKmcCMD, FeaturiseParamsKMC};
#[cfg(feature = "gecco")]
pub use gecco::GeccoCMD;
pub use getraw::GetRawCMD;
pub use mapcell::{MapCell, MapCellCMD};
pub use minhash_hist::{MinhashHist, MinhashHistCMD};
pub use qc::QcCMD;

// Taxonomic classification
pub use kraken::KrakenCMD;

// Thread management
pub use shardify::ShardifyCMD;
#[cfg(feature = "skesa")]
pub use skesa::SkesaCMD;
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
    DetectKmerKmc(DetectKmerKmcCMD),
    DetectKmerFq(DetectKmerFqCMD),
    Extract(ExtractCMD),
    ExtractStream(ExtractStreamCMD),
    #[cfg(feature = "fastqc")]
    Fastqc(FastqcCMD),
    Featurise(FeaturiseKmcCMD),
    #[cfg(feature = "gecco")]
    Gecco(GeccoCMD),
    GetRaw(GetRawCMD),
    //KmcReads(KmcReadsCMD),
    Kraken(KrakenCMD),
    Mapcell(MapCellCMD),
    MinhashHist(MinhashHistCMD),
    PipeSamAddTags(PipeSamAddTagsCMD), //Not needed for bascet anymore, but useful if anyone needs to use a non-standard aligner
    Shardify(ShardifyCMD),
    #[cfg(feature = "skesa")]
    Skesa(SkesaCMD),
    Sysinfo(SysinfoCMD),
    ToFastq(ToFastqCMD),
    Transform(TransformCMD),
    Qc(QcCMD),
}

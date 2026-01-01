use std::fmt;

use clap::Subcommand;

use crate::command;

///////////////////////////////
/// Possible subcommands to parse
#[derive(Subcommand)]
#[allow(non_camel_case_types)]
pub enum Commands {
    _depreciated_GetRaw(command::_depreciated_GetRawCMD),
    GetRaw(command::GetRawCMD),
    Mapcell(command::MapCellCMD),
    Extract(command::ExtractCMD),
    // Shardify(command::ShardifyCMD),
    Transform(command::TransformCMD),
    Featurise(command::FeaturiseKmcCMD),
    MinhashHist(command::MinhashHistCMD),
    QueryKmc(command::QueryKmcCMD),
    QueryFq(command::QueryFqCMD),
    Bam2fragments(command::Bam2FragmentsCMD),
    Kraken(command::KrakenCMD),
    Countchrom(command::CountChromCMD),
    Countfeature(command::CountFeatureCMD),
    PipeSamAddTags(command::PipeSamAddTagsCMD),
    Countsketch(command::CountsketchCMD),
    CountsketchMat(command::CountsketchMatCMD),
    ExtractStream(command::ExtractStreamCMD),
}

impl fmt::Debug for Commands {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cmd = match self {
            Commands::_depreciated_GetRaw(_) => "Getraw (depreciated)",
            Commands::GetRaw(_) => "Getraw",
            Commands::Mapcell(_) => "Mapcell",
            Commands::Extract(_) => "Extract",
            // Commands::Shardify(_) => "Shardify",
            Commands::Transform(_) => "Transform",
            Commands::Featurise(_) => "Featurise",
            Commands::MinhashHist(_) => "MinhashHist",
            Commands::QueryKmc(_) => "QueryKmc",
            Commands::QueryFq(_) => "QueryFq",
            Commands::Bam2fragments(_) => "Bam2fragments",
            Commands::Kraken(_) => "Kraken",
            Commands::Countchrom(_) => "Countchrom",
            Commands::Countfeature(_) => "Countfeature",
            Commands::PipeSamAddTags(_) => "PipeSamAddTags",
            Commands::Countsketch(_) => "Countsketch",
            Commands::ExtractStream(_) => "ExtractStream",
            Commands::CountsketchMat(_) => "Countsketch Mat",
        };
        write!(f, "{}", cmd)
    }
}

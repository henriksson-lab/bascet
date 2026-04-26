use std::path::PathBuf;

//use tracing::debug;

use crate::mapcell::CompressionMode;
use crate::mapcell::MapCellFunction;
use crate::mapcell::MissingFileMode;
/*
//use gecco::Gecco;
use gecco::orf::SeqRecord;
use std::fs::File;
use gecco::io::tables::{ClusterTable, GeneTable, FeatureTable};
//use gecco::io::genbank::write_cluster_gbk;
use gecco::io::genbank::write_clusters_merged;
 */

#[derive(Clone, Debug)]
pub struct MapCellMinHashFQ {}
impl MapCellFunction for MapCellMinHashFQ {
    fn invoke(
        &self,
        _input_dir: &PathBuf,
        _output_dir: &PathBuf,
        _num_threads: usize,
    ) -> anyhow::Result<(bool, String)> {
        /*

                //Define files
                let input_file_contigs = input_dir.join("contigs.fq");

                let output_file = output_dir.join("minhash.txt");





                // Load your sequences (e.g. from a FASTA file)
                let records = vec![SeqRecord {
                    id: "contig_1".into(),
                    seq: std::fs::read_to_string("genome.fna")?,
                }];

                // Run the full pipeline: gene finding → annotation → CRF → clustering
                // Get both genes (with probabilities) and clusters
                let (genes, clusters) = pipeline.scan_detailed(&records)?;

                for gene in &genes {
                    println!("{}: p={:.3}", gene.id, gene.average_p.unwrap_or(0.0));
                }



                // Write TSV tables
                GeneTable::write_from_genes(File::create("output.genes.tsv")?, &genes)?;
                FeatureTable::write_from_genes(File::create("output.features.tsv")?, &genes)?;
                ClusterTable::write_from_clusters(File::create("output.clusters.tsv")?, &clusters)?;  //// make a method instead

        //        write_cluster_merged(&clusters);

                let out = File::create("output.clusters.gbk")?;
                write_clusters_merged(
                    out,
                    &clusters,
                    &source_seqs
                )?;







                */

        Ok((true, String::from("")))
    }

    fn get_missing_file_mode(&self) -> MissingFileMode {
        MissingFileMode::Skip
    }

    fn get_compression_mode(&self, _fname: &str) -> CompressionMode {
        CompressionMode::Default
    }

    fn get_expect_files(&self) -> Vec<String> {
        let mut expect = Vec::new();
        expect.push("contigs.fq".to_string());
        expect
    }

    fn get_recommend_threads(&self) -> usize {
        1
    }

    fn preflight_check(&self) -> bool {
        /*
               //Load databases once only
               let pipeline = Gecco::builder()
                   .jobs(1) //Multithread on higher level
                   .build()?;

        */

        true
    }
}

use anyhow::Result;
use clap::Args;
use linya::Progress;
use rev_buf_reader::RevBufReader;
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Seek, SeekFrom},
    path::PathBuf,
    thread,
};
use zip::{write::FileOptions, ZipArchive, ZipWriter};

use super::constants::{COUNT_DEFAULT_PATH_IN, COUNT_DEFAULT_PATH_INDEX, COUNT_DEFAULT_PATH_TEMP};

#[derive(Args)]
pub struct Command {
    #[arg(short = 'i', value_parser, default_value = COUNT_DEFAULT_PATH_IN)]
    pub path_in: PathBuf,
    #[arg(short = 'j', value_parser, default_value = COUNT_DEFAULT_PATH_INDEX)]
    pub path_index: PathBuf,
    #[arg(short = 't', value_parser, default_value = COUNT_DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,
    #[arg(short = 'k', long, value_parser = clap::value_parser!(usize))]
    pub kmer_size: usize,
    #[arg(long, value_parser = clap::value_parser!(usize))]
    pub threads: Option<usize>,
    #[arg(long, value_parser = clap::value_parser!(usize))]
    pub seed: Option<u64>,
}

impl Command {
    pub fn try_execute(&mut self) -> Result<()> {
        self.verify_input_file()?;
        let kmer_size = self.verify_kmer_size()?;
        let threads = self.resolve_thread_config()?;

        let file_rdb = File::open(&self.path_in)?;
        let mut handle_rdb = ZipArchive::new(&file_rdb)?;
        let mut bufwriter_rdb = BufWriter::new(&file_rdb);
        let mut zipwriter_rdb = ZipWriter::new(&mut bufwriter_rdb);

        // basically all of this is just to get the progress bar ...
        let index_file = File::open(&self.path_index)?;
        let mut index_reader = BufReader::new(&index_file);
        let mut index_rev_reader = RevBufReader::new(&mut index_reader);
        let mut index_last_line = String::new();
        index_rev_reader.read_line(&mut index_last_line)?;
        let index_last = index_last_line
            .split(",")
            .next()
            .unwrap()
            .parse::<usize>()?;
        index_reader.seek(SeekFrom::Start(0))?;

        let union_dir = self.path_tmp.join("union");
        fs::create_dir_all(&union_dir)?;

        // create an empty fastq to create an empty kmc database as a merge target
        let path_empty_reads = union_dir.join("reads").with_extension("fastq");
        let _ = File::create(&path_empty_reads);
        let path_kmc_union = union_dir.join("kmc");
        let path_kmc_union_new = union_dir.join("kmc_new");
        let _ = std::process::Command::new("kmc")
            .arg(format!("-cs{}", u32::MAX))
            .arg(format!("-k{}", &self.kmer_size))
            .arg(&path_empty_reads)
            .arg(&path_kmc_union)
            .arg(&self.path_tmp)
            .output()?;

        let mut progress = Progress::new();
        let bar = progress.bar(index_last, "Counting KMERs");

        for line in index_reader.lines() {
            let line = line?;
            let index = line
                .split(',')
                .next()
                .ok_or_else(|| anyhow::anyhow!("Error parsing index file"))?
                .parse::<usize>()?;

            let mut file = handle_rdb.by_index(index)?;
            let file_pathbuf = file.mangled_name();
            let barcode = file_pathbuf.parent().unwrap();

            let path_dir_barcode = self.path_tmp.join(&barcode);
            let _ = fs::create_dir(&path_dir_barcode);

            let path_dir_barcode_reads = path_dir_barcode.join("reads.fastq");
            let mut file_dir_barcode_reads = File::create(&path_dir_barcode_reads)?;
            std::io::copy(&mut file, &mut file_dir_barcode_reads)?;

            let kmc_path_db = path_dir_barcode.join("kmc");
            let kmc_path_dump = path_dir_barcode.join("dump.txt");
            let kmc = std::process::Command::new("kmc")
                .arg(format!("-cs{}", u32::MAX - 1))
                .arg(format!("-k{}", &kmer_size))
                .arg(&path_dir_barcode_reads)
                .arg(&kmc_path_db)
                .arg(&self.path_tmp)
                .arg("-t").arg(format!("{threads}"))
                .output()?;

            if !kmc.status.success() {
                anyhow::bail!("KMC failed: {}", String::from_utf8_lossy(&kmc.stderr));
            }

            let kmc_dump = std::process::Command::new("kmc_tools")
                .arg("transform")
                .arg(&kmc_path_db)
                .arg("dump")
                .arg(&kmc_path_dump)
                .arg("-t").arg(format!("{threads}"))
                .output()?;

            if !kmc_dump.status.success() {
                anyhow::bail!("KMC dump failed: {}", String::from_utf8_lossy(&kmc.stderr));
            }
            let kmc_union = std::process::Command::new("kmc_tools")
                .arg("simple")
                .arg(&path_kmc_union)
                .arg(&kmc_path_db)
                .arg("union")
                .arg(&path_kmc_union_new)
                .arg("-t").arg(format!("{threads}"))
                .output()?;

            if !kmc_union.status.success() {
                anyhow::bail!("KMC union failed: {}", String::from_utf8_lossy(&kmc.stderr));
            }

            let opts: FileOptions<'_, ()> =
                FileOptions::default().compression_method(zip::CompressionMethod::Deflated);
            let mut dump_file = File::open(&kmc_path_dump)?;
            if let Ok(_) = zipwriter_rdb.start_file_from_path(&kmc_path_dump, opts) {
                std::io::copy(&mut dump_file, &mut zipwriter_rdb)?;
            }

            let _ = fs::rename(
                &path_kmc_union_new.with_extension("kmc_pre"),
                &path_kmc_union.with_extension("kmc_pre"),
            );
            let _ = fs::rename(
                &path_kmc_union_new.with_extension("kmc_suf"),
                &path_kmc_union.with_extension("kmc_suf"),
            );
            let _ = fs::remove_dir_all(&path_dir_barcode);
            progress.inc_and_draw(&bar, 1);
            // break;
        }
        Ok(())
    }

    fn verify_input_file(&mut self) -> anyhow::Result<()> {
        if let Ok(file) = File::open(&self.path_in) {
            if file.metadata()?.len() == 0 {
                anyhow::bail!("Empty input file");
            }
        }
        match self.path_in.extension().and_then(|ext| ext.to_str()) {
            Some("zip") => return Ok(()),
            _ => anyhow::bail!("Input file must be a zip archive"),
        };
    }

    fn verify_kmer_size(&self) -> Result<usize> {
        if self.kmer_size < 48 {
            return Ok(self.kmer_size);
        }

        anyhow::bail!("Invalid kmer size k:{}", self.kmer_size);
    }

    fn resolve_thread_config(&self) -> anyhow::Result<usize> {
        let available_threads = thread::available_parallelism()
            .map_err(|e| anyhow::anyhow!("Failed to get available threads: {}", e))?
            .get();

        if let Some(given_threads) = self.threads {
            if given_threads == 0 {
                anyhow::bail!("At least one thread required");
            }

            return Ok(given_threads);
        }

        return Ok(available_threads);
    }
}

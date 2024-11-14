use std::{
    fs::{self, File},
    io::{BufReader, BufWriter},
    path::Path,
    process::Command,
};

use bio::io::fasta::{Reader, Writer};
use linya::{Bar, Progress};

pub enum ISSModel {
    HiSeq,
    MiSeq,
    MiSeq20,
    MiSeq24,
    MiSeq28,
    MiSeq32,
    MiSeq36,
    NextSeq,
    NovaSeq,
}

impl ISSModel {
    fn to_str(&self) -> &'static str {
        match self {
            ISSModel::HiSeq => "HiSeq",
            ISSModel::MiSeq => "MiSeq",
            ISSModel::MiSeq20 => "MiSeq-20",
            ISSModel::MiSeq24 => "MiSeq-24",
            ISSModel::MiSeq28 => "MiSeq-28",
            ISSModel::MiSeq32 => "MiSeq-32",
            ISSModel::MiSeq36 => "MiSeq-36",
            ISSModel::NextSeq => "NextSeq",
            ISSModel::NovaSeq => "NovaSeq",
        }
    }
}
pub struct ISS {}

impl ISS {
    const EXT_FASTA: &'static str = "fasta";
    const EXT_FASTQ: &'static str = "fastq";

    const ISS_TEMPDIR: &'static str = "temp";
    const ISS_CMD: &'static str = "iss";
    const ISS_GEN: &'static str = "generate";
    const ISS_ARG_GENOME: &'static str = "--genome";
    const ISS_ARG_MODEL: &'static str = "--model";
    const ISS_ARG_PATH_OUT: &'static str = "--output";
    const ISS_ARG_CPUS: &'static str = "--cpus";
    const ISS_ARG_N_READS: &'static str = "--n_reads";

    pub fn simulate<P: AsRef<Path>>(path_ref: P, path_out: P, n_samples: i32, n_reads: i32) {
        let path_ref = path_ref.as_ref();
        let path_out = path_out.as_ref();

        let handle_ref_file = match path_ref.try_exists() {
            Ok(true) => File::open(path_ref).expect("Could not read reference genome file."),
            Ok(false) => panic!(),
            Err(_) => panic!(),
        };
        let bufreader_ref_file = BufReader::new(handle_ref_file);
        let reader_ref_file = Reader::from_bufread(bufreader_ref_file);

        let mut progress = Progress::new();
        for res_record in reader_ref_file.records() {
            let record = res_record.unwrap();

            let path_temp_ref_file = path_ref
                .parent()
                .unwrap()
                .join(Self::ISS_TEMPDIR)
                .join(record.id())
                .with_extension(Self::EXT_FASTA);
            let _ = fs::create_dir_all(&path_temp_ref_file.parent().unwrap());

            let handle_temp_ref_file = File::create(&path_temp_ref_file).unwrap();
            let bufwriter_temp_ref_file = BufWriter::new(handle_temp_ref_file);
            let mut writer_temp_ref_file = Writer::from_bufwriter(bufwriter_temp_ref_file);
            writer_temp_ref_file
                .write(&record.id(), record.desc(), &record.seq())
                .expect("Could not write to temp file");

            writer_temp_ref_file
                .flush()
                .expect("Could not write to temp file");

            let bar = progress.bar(
                n_samples as usize,
                format!("Simulating {} reads for {}", n_samples, record.id()),
            );
            progress.set_and_draw(&bar, 0);

            for n in 1..=n_samples {
                let path_sample_out = path_out
                    .join(format!("{}-{}", &record.id(), n))
                    .join("Reads");
                let _ = fs::create_dir_all(&path_sample_out.parent().unwrap());

                let iss_out = Command::new(Self::ISS_CMD)
                    .arg(Self::ISS_GEN)
                    .args([Self::ISS_ARG_GENOME, path_temp_ref_file.to_str().unwrap()])
                    .args([Self::ISS_ARG_MODEL, ISSModel::NovaSeq.to_str()])
                    .args([Self::ISS_ARG_PATH_OUT, path_sample_out.to_str().unwrap()])
                    .args([Self::ISS_ARG_CPUS, "16"])
                    .args([Self::ISS_ARG_N_READS, &n_reads.to_string()])
                    .output()
                    .expect("Failed to execute iss command");

                progress.set_and_draw(&bar, n as usize);
                if iss_out.status.success() {
                } else {
                    eprintln!("Command failed to execute.");
                    eprintln!("stderr: {}", String::from_utf8_lossy(&iss_out.stderr));
                }
            }
        }
    }
}

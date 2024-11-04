use bio::io::{fasta, fastq};
use rand::{distributions::Uniform, prelude::Distribution, rngs::SmallRng, SeedableRng};
use std::{
    fs::File,
    io::{BufReader, BufWriter},
    path::Path,
};

pub struct Reads<const N: usize> {
    pub p_read_open: f32,
    pub p_read_coverage_change: f32,

    pub reader_buffer_handle: std::io::BufReader<std::fs::File>,
    pub phred33_quality_annotation: [u8; N],
}

impl<const N: usize> Reads<N> {
    pub fn new(path_in: &Path, p_read_open: f32, p_read_coverage_change: f32) -> Self {
        let reader_handle = match path_in.try_exists() {
            Ok(true) => BufReader::new(File::open(path_in).unwrap()),
            Ok(false) => panic!("File {path_in:?} does not exist and cannot be read."),
            Err(_) => panic!("File {path_in:?} cannot be read. It may exist."),
        };

        match p_read_open {
            0.0 => panic!("p_read_open cannot be 0"),
            0.0..=1.0 => {}
            _ => panic!("p_read_open of ({p_read_open}) is invalid"),
        }

        match p_read_coverage_change {
            0.0..=1.0 => {}
            _ => panic!("p_read_coverage_change of ({p_read_coverage_change}) is invalid"),
        }

        return Self {
            p_read_open,
            p_read_coverage_change,

            reader_buffer_handle: reader_handle,
            phred33_quality_annotation: [33 + 56; N],
        };
    }

    pub fn simulate(&self, path_out: &Path) {
        let reader_fasta = fasta::Reader::new(self.reader_buffer_handle.get_ref());

        let writer_file = File::create(path_out).expect(&format!("Unable to write to file: {path_out:?}."));
        let writer_buffer_handle = BufWriter::new(writer_file);
        let mut writer_fastq = fastq::Writer::new(writer_buffer_handle);

        let mut rng = SmallRng::from_entropy();
        let read_open_range             = Uniform::new_inclusive(0.0, 1.0);
        let read_change_coverage_range  = Uniform::new_inclusive(0.0, 1.0);
        // NOTE: here important that the upper bound is 2.0! if it was 1.0 it would effectively half p_read_open
        let read_coverage_range         = Uniform::new_inclusive(0.0, 2.0);

        for record_opt in reader_fasta.records() {
            let record = record_opt.unwrap();

            let n = record.seq().len();
            if n < N {
                continue;
            }

            let mut read_coverage_factor = read_coverage_range.sample(&mut rng);
            (0..(n - N)).into_iter().for_each(|i| {
                let c_read_change_coverage = read_change_coverage_range.sample(&mut rng);
                if c_read_change_coverage <= self.p_read_coverage_change {
                    read_coverage_factor = read_coverage_range.sample(&mut rng);
                }

                let c_read_open_range = read_open_range.sample(&mut rng);
                if c_read_open_range <= self.p_read_open * read_coverage_factor {
                    let (s, e) = (i, i + N);
                    let read_slice = &record.seq()[s as usize..e as usize];
                    let read_id = format!("{}::{}..{}", record.id(), s, e);

                    writer_fastq
                        .write(
                            &read_id,
                            record.desc(),
                            read_slice,
                            &self.phred33_quality_annotation,
                        )
                        .expect("File is unable to be opened or written to.");
                }
            });
        }
        writer_fastq
            .flush()
            .expect("File is unable to be opened or written to.");
    }
}

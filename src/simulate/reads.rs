use bio::io::{fasta, fastq};
use rand::{distributions::{self, Uniform}, prelude::Distribution, rngs::SmallRng, SeedableRng};
use std::{
    fs::{self, File},
    io::{BufReader, BufWriter},
    path::Path,
};

pub struct Reads<const N: usize> {
    pub p_read_open: f32,
    pub p_read_coverage_change: f32,

    pub in_bufreader: std::io::BufReader<std::fs::File>,
}

impl<const N: usize> Reads<N> {
    //NOTE: I read that 40 is the default being assigned to NT reads by some aligners, so i am using 40
    const PHRED33_QUALITY_ANNOTATION: [u8; N] = [33 + 40; N];
    const READERR: f32 = 0.01;

    pub fn new(path_in: &Path, p_read_open: f32, p_read_coverage_change: f32) -> Self {
        let in_bufreader: BufReader<File> = match path_in.try_exists() {
            Ok(true) => BufReader::new(File::open(path_in).unwrap()),
            Ok(false) => panic!("File {path_in:?} does not exist and cannot be read."),
            Err(_) => panic!("File {path_in:?} cannot be read. It may exist."),
        };

        match p_read_open {
            0.0 => panic!("p_read_open cannot be ."),
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

            in_bufreader,
        };
    }

    pub fn simulate(self, samples: i32, path_out: &Path) {
        let in_bufreader = fasta::Reader::new(self.in_bufreader.get_ref());

        let path_out_dir = path_out.parent().expect(&format!(
            "Invalid path: {path_out:?}. Path parent cannot be resolved."
        ));
        let _ = fs::create_dir_all(&path_out_dir);
        let concat_file =
            File::create(&path_out).expect(&format!("Unable to write to file: {path_out:?}."));

        let record_writer_dir = path_out_dir.join(Path::new(path_out.file_stem().unwrap()));
        let _ = fs::create_dir_all(&record_writer_dir);

        let concat_bufwriter = BufWriter::new(concat_file);
        let mut concat_writer_fastq = fastq::Writer::new(concat_bufwriter);

        let mut rng = SmallRng::from_entropy();

        let read_open_range             = Uniform::new_inclusive(0.0, 1.0);
        let read_change_coverage_range  = Uniform::new_inclusive(0.0, 1.0);
        let read_coverage_range         = Uniform::new_inclusive(0.0, 2.0);
        // NOTE: here important that the upper bound is 2.0! if it was 1.0 it would effectively half p_read_open

        for record_opt in in_bufreader.records() {
            let record = record_opt.unwrap();

            let n = record.seq().len();
            if n < N {
                continue;
            }
            for i in 1..=samples {
                let record_name = format!("{}::{i}", record.id().replace(".", "_"));
                
                // io is always fun :))))))))
                let record_writer_path_out =
                    record_writer_dir.join(&record_name).with_extension("fastq");
                let record_writer_file = File::create(&record_writer_path_out).expect(&format!(
                    "Unable to write to file: {record_writer_path_out:?}."
                ));
                let record_bufwriter_handle = BufWriter::new(record_writer_file);
                let mut record_writer_fastq = fastq::Writer::new(record_bufwriter_handle);
                
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
                        let read_id = format!("{}::{}..{}", &record_name, s, e);

                        record_writer_fastq
                            .write(
                                &read_id,
                                record.desc(),
                                &read_slice,
                                &Self::PHRED33_QUALITY_ANNOTATION,
                            )
                            .expect(&format!(
                                "File at path {record_writer_path_out:?} is unable to be opened or written to."
                            ));
                        
                        concat_writer_fastq
                            .write(
                                &read_id,
                                record.desc(),
                                &read_slice,
                                &Self::PHRED33_QUALITY_ANNOTATION,
                            )
                            .expect(&format!(
                                "File at path {path_out:?} is unable to be opened or written to."
                            ));
                    }
                });

                record_writer_fastq.flush().expect(&format!(
                    "File at path {path_out:?} is unable to be opened or written to."
                ));
            }
        }
        concat_writer_fastq.flush().expect(&format!(
            "File at path {path_out:?} is unable to be opened or written to."
        ));
    }
}

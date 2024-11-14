use std::path::Path;

use bio::io::fasta;
use rand::{distributions::Uniform, prelude::Distribution, rngs::SmallRng, SeedableRng};

pub struct ReadsSimulator {
    pub p_read_open: f32,
    pub p_read_coverage_change: f32,
    pub read_length: u32,
}

impl ReadsSimulator {
    pub fn simulate_with(&self, in_path: &Path, out_path: &Path) {
        let reader = fasta::Reader::from_file(in_path)
            .expect("File does not exist or is unable to be opened.");

        let writer_file =
            std::fs::File::create(out_path).expect("File is unable to be opened and written to.");
        let writer_handle = std::io::BufWriter::new(writer_file);
        let mut writer = fasta::Writer::new(writer_handle);

        let mut rng = SmallRng::from_entropy();
        for record_opt in reader.records() {
            let record = record_opt.unwrap();

            let n = record.seq().len() as u32;
            if n < self.read_length {
                continue;
            }

            let read_open_range = Uniform::new(0.0, 1.0);
            let read_change_coverage_range = Uniform::new(0.0, 1.0);
            let read_coverage_range = Uniform::new(0.0, 2.0);

            let mut read_coverage_factor = read_coverage_range.sample(&mut rng);
            (0..(n - self.read_length)).into_iter().for_each(|i| {
                let c_read_change_coverage = read_change_coverage_range.sample(&mut rng);
                if c_read_change_coverage <= self.p_read_coverage_change {
                    read_coverage_factor = read_coverage_range.sample(&mut rng);
                }

                let c_read_open_range = read_open_range.sample(&mut rng);
                if c_read_open_range <= self.p_read_open * read_coverage_factor {
                    let (s, e) = (i, i + self.read_length);
                    let read_slice = &record.seq()[s as usize..e as usize];
                    let read_id = format!("{}::{}..{}", record.id(), s, e);

                    writer
                        .write(&read_id, record.desc(), read_slice)
                        .expect("File is unable to be opened or written to.");
                }
            });
        }
        writer
            .flush()
            .expect("File is unable to be opened or written to.");
    }
}

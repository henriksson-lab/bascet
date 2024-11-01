use std::path::Path;

use bio::io::fasta;
use rand::{distributions::Uniform, prelude::Distribution, rngs::StdRng, SeedableRng};

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

        for record_opt in reader.records() {
            let record = record_opt.unwrap();
            let n = record.seq().len() as u32;

            if n < 150 {
                continue;
            }

            let mut rng = StdRng::from_entropy();
            let read_open_range = Uniform::new(0.0, 1.0);
            let read_change_coverage_range = Uniform::new(0.0, 1.0);
            let read_coverage_range = Uniform::new(0.0, 2.0);
            let mut read_coverage_factor = read_coverage_range.sample(&mut rng);

            let read_indices: Vec<(u32, u32)> = (0..=(n - self.read_length))
                .into_iter()
                .filter_map(|i| {
                    if read_change_coverage_range.sample(&mut rng) <= self.p_read_coverage_change {
                        read_coverage_factor = read_coverage_range.sample(&mut rng);
                    }
                    if read_open_range.sample(&mut rng) <= self.p_read_open * read_coverage_factor {
                        Some((i, i + self.read_length))
                    } else {
                        None
                    }
                })
                .collect();

            for &(s, e) in &read_indices {
                let read_slice = &record.seq()[s as usize..e as usize];
                let new_id = format!("{}::{}..{}", record.id(), s, e);

                let _ = writer.write(&new_id, None, read_slice);
                // with description, skipping for now as it makes parsing a little harder in R
                // let _ = writer.write(&new_id, record.desc(), read_slice);
            }
        }
        let _ = writer.flush();
    }
}

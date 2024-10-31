use bio::io::fasta;
use rand::distributions::{Distribution, Uniform};
use rayon::prelude::*;
use std::path::Path;

pub struct ReadsSimulator {
    pub p_read_open: f32,
    pub read_length: u32,
}

impl ReadsSimulator {
    pub fn simulate_with(&self, in_path: &Path, out_path: &Path) {
        let reader = fasta::Reader::from_file(in_path)
            .expect("File does not exist or is unable to be opened.");
        let mut writer = fasta::Writer::to_file(out_path)
            .expect("File is unable to be opened and written to.");

        for record_opt in reader.records() {
            let record = record_opt.unwrap();
            let n = record.seq().len() as u32;

            if n < 150 {
                continue;
            }

            let range = Uniform::new(0.0, 1.0);
            let read_indices: Vec<(u32, u32)> = (0..=(n - 150))
                .into_par_iter()
                .map(|i| {
                    let mut rng = rand::thread_rng();
                    if range.sample(&mut rng) <= self.p_read_open {
                        Some((i, i + self.read_length))
                    } else {
                        None
                    }
                })
                .filter_map(|x| x)
                .collect();
            
            
            for &(s, e) in &read_indices {
                let read_slice = &record.seq()[s as usize..e as usize];
                let new_id = format!("{}::{}..{}", record.id(), s, e);
                let _ = writer.write_record(&fasta::Record::with_attrs(
                    &new_id, 
                    None, 
                    read_slice
                ));
            }
        }
    }
}

use std::io::{BufWriter, Write};

use anyhow::Result;
use clap::Args;
use clio::{InputPath, OutputPath};
use rust_htslib::bam::{Read, Reader};
use rust_htslib::bam::record::Record as BamRecord;
use tracing::info;

#[derive(Args)]
pub struct QcAlignedCoverageCMD {
    #[arg(
        short = 'i',
        long = "in",
        num_args = 1..,
        value_delimiter = ',',
        help = "List of BAM input files (comma-separated). Assumed sorted by cell (qname)."
    )]
    pub paths_in: Vec<InputPath>,

    #[arg(
        short = 'o',
        long = "out",
        help = "Output file path"
    )]
    pub path_out: OutputPath,
}

struct RefPileup {
    diff: Vec<i64>,
    countof_alignments: u64,
    sumof_aligned_bases: u64,
}

impl RefPileup {
    fn new(sizeof_reference: usize) -> Self {
        Self {
            diff: vec![0i64; sizeof_reference + 1],
            sumof_aligned_bases: 0,
            countof_alignments: 0,
        }
    }

    fn add_alignment(&mut self, start: i64, end: i64, sizeof_seq: i64) {
        let end = end as usize;
        if end >= self.diff.len() {
            self.diff.resize(end + 1, 0);
        }
        self.diff[start as usize] += 1;
        self.diff[end] -= 1;
        self.sumof_aligned_bases += TryInto::<u64>::try_into(sizeof_seq).unwrap();
        self.countof_alignments += 1;
    }

    fn finalize(&self) -> (u64, Vec<u8>) {
        let mut depth = 0i64;
        let mut union_aligned_bases = 0u64;
        let mut buf: Vec<u8> = Vec::new();
        let mut run_start = 0usize;
        let mut run_depth = 0i64;
        let mut run_len = 0usize;

        for (pos, &d) in self.diff.iter().enumerate() {
            depth += d;
            if depth > 0 {
                union_aligned_bases += 1;
            }
            if depth != run_depth {
                if run_depth > 0 {
                    if !buf.is_empty() {
                        buf.push(b',');
                    }
                    buf.extend_from_slice(run_start.to_string().as_bytes());
                    buf.push(b':');
                    buf.extend_from_slice(run_depth.to_string().as_bytes());
                    buf.push(b':');
                    buf.extend_from_slice(run_len.to_string().as_bytes());
                }
                run_start = pos;
                run_depth = depth;
                run_len = 1;
            } else {
                run_len += 1;
            }
        }
        if run_depth > 0 {
            if !buf.is_empty() {
                buf.push(b',');
            }
            buf.extend_from_slice(run_start.to_string().as_bytes());
            buf.push(b':');
            buf.extend_from_slice(run_depth.to_string().as_bytes());
            buf.push(b':');
            buf.extend_from_slice(run_len.to_string().as_bytes());
        }

        (union_aligned_bases, buf)
    }
}

impl QcAlignedCoverageCMD {
    pub fn try_execute(&mut self) -> Result<()> {
        info!("Running QC aligned-coverage");

        let output_file = self.path_out.clone().create()?;
        let mut bufwriter = BufWriter::new(output_file);

        bufwriter.write_all(b"id\treference\tsizeof_reference\tunion_aligned_bases\tsumof_aligned_bases\tcountof_alignments\tpileup\n")?;

        for input in &self.paths_in {
            info!(path = %input, "Processing BAM");
            let mut reader = Reader::from_path(&**input.path())?;
            let header = reader.header().clone();

            let countof_refs = header.target_names().len();

            let ref_names: Vec<String> = header
                .target_names()
                .iter()
                .map(|name| String::from_utf8_lossy(name).into_owned())
                .collect();

            let ref_lengths: Vec<usize> = (0..countof_refs)
                .map(|tid| header.target_len(tid as u32).unwrap_or(0) as usize)
                .collect();

            let mut cell_pileups: Vec<Option<RefPileup>> = (0..countof_refs).map(|_| None).collect();
            let mut record_id_last: Vec<u8> = Vec::new();
            let mut countof_cells_processed = 0u64;

            let mut record = BamRecord::new();
            while let Some(Ok(_)) = reader.read(&mut record) {
                if record.is_unmapped() {
                    continue;
                }

                let record_qname = record.qname();
                let record_id = match memchr::memmem::find(record_qname, b"::") {
                    Some(pos) => &record_qname[..pos],
                    None => record_qname,
                };

                if record_id != record_id_last.as_slice() {
                    if !record_id_last.is_empty() {
                        write_cell(&record_id_last, &ref_names, &ref_lengths, &cell_pileups, &mut bufwriter)?;
                        countof_cells_processed += 1;
                        if countof_cells_processed % 100 == 0 {
                            info!(countof_cells_processed = countof_cells_processed, current_cell = ?String::from_utf8_lossy(&record_id_last), "Progress");
                        }
                        for p in cell_pileups.iter_mut() {
                            *p = None;
                        }
                    }

                    assert!(
                        record_id_last.is_empty() || record_id > record_id_last.as_slice(),
                        "BAM not sorted by cell: {:?} after {:?}",
                        String::from_utf8_lossy(record_id),
                        String::from_utf8_lossy(&record_id_last),
                    );

                    record_id_last = record_id.to_vec();
                }

                let tid = record.tid() as usize;
                let cigar = record.cigar();
                let pos_start = cigar.pos();
                let pos_end = cigar.end_pos();
                let sizeof_seq = pos_end - pos_start;

                if cell_pileups[tid].is_none() {
                    cell_pileups[tid] = Some(RefPileup::new(ref_lengths[tid]));
                }
                cell_pileups[tid].as_mut().unwrap().add_alignment(pos_start, pos_end, sizeof_seq);
            }

            if !record_id_last.is_empty() {
                write_cell(&record_id_last, &ref_names, &ref_lengths, &cell_pileups, &mut bufwriter)?;
                countof_cells_processed += 1;
            }

            info!(countof_cells_processed = countof_cells_processed, path = %input, "Finished BAM");
        }

        bufwriter.flush()?;
        Ok(())
    }
}

fn write_cell(
    id: &[u8],
    reference_names: &[String],
    ref_lengths: &[usize],
    cell_pileups: &[Option<RefPileup>],
    bufwriter: &mut BufWriter<impl Write>,
) -> Result<()> {
    for (tid, pileup_opt) in cell_pileups.iter().enumerate() {
        if let Some(pileup) = pileup_opt {
            let (union_aligned_bases, pileup_bytes) = pileup.finalize();

            bufwriter.write_all(id)?;
            bufwriter.write_all(b"\t")?;
            bufwriter.write_all(reference_names[tid].as_bytes())?;
            bufwriter.write_all(b"\t")?;
            bufwriter.write_all(ref_lengths[tid].to_string().as_bytes())?;
            bufwriter.write_all(b"\t")?;
            bufwriter.write_all(union_aligned_bases.to_string().as_bytes())?;
            bufwriter.write_all(b"\t")?;
            bufwriter.write_all(pileup.sumof_aligned_bases.to_string().as_bytes())?;
            bufwriter.write_all(b"\t")?;
            bufwriter.write_all(pileup.countof_alignments.to_string().as_bytes())?;
            bufwriter.write_all(b"\t")?;
            bufwriter.write_all(&pileup_bytes)?;
            bufwriter.write_all(b"\n")?;
        }
    }
    Ok(())
}

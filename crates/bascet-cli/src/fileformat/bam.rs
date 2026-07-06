use std::sync::Arc;
use std::{fs::File, num::NonZeroUsize, path::PathBuf};
use tracing::info;

use super::ConstructFromPath;
use super::shard::StreamingReadPairReader;
use crate::fileformat::shard::ReadPair;

use super::CellID;
use noodles::sam::alignment::RecordBuf as BamRecord;

type ListReadWithBarcode = Arc<(CellID, Arc<Vec<ReadPair>>)>;

///////////////////////////////
/// One BAM record, decoded into the fields we care about. The sequence/quality are restored
/// to original (sequenced) orientation: BAM stores reverse-strand alignments as the
/// reverse-complement, so for those we reverse-complement SEQ and reverse QUAL.
struct BamHalf {
    cell_id: Vec<u8>,
    umi: Vec<u8>,
    seq: Vec<u8>,
    qual: Vec<u8>,
    is_segmented: bool,
    is_first: bool,
    is_last: bool,
}

///////////////////////////////
/// A streaming reader of BAM files, providing read pairs.
///
/// Paired-end reads are reconstructed by assuming the two mates of a pair are adjacent in the
/// file (R1 immediately followed by R2), which holds for name-collated / name-sorted BAMs. The
/// adjacency is verified while reading (see `next_readpair`). Records without the segmented
/// (0x1) flag are emitted as single-end (R1 only, R2 empty), so mixed files work. Secondary
/// (0x100) and supplementary (0x800) alignments are skipped so they do not break mate adjacency.
pub struct BAMStreamingReadPairReader {
    reader: noodles::bam::io::Reader<noodles::bgzf::io::MultithreadedReader<File>>,
    header: noodles::sam::Header,
    /// One-record lookahead, used when an expected mate turns out not to be one.
    pushback: Option<BamHalf>,
    /// One-readpair lookahead, used to detect cell boundaries.
    last_rp: Option<(Vec<u8>, ReadPair)>,
}
impl BAMStreamingReadPairReader {
    /// Create a new reader from a BAM file
    pub fn new(fname: &PathBuf) -> anyhow::Result<BAMStreamingReadPairReader> {
        let worker_count = std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN);
        Self::new_with_worker_count(fname, worker_count)
    }

    pub fn new_with_threads(
        fname: &PathBuf,
        threads: usize,
    ) -> anyhow::Result<BAMStreamingReadPairReader> {
        let worker_count = NonZeroUsize::new(threads.max(1)).unwrap();
        Self::new_with_worker_count(fname, worker_count)
    }

    fn new_with_worker_count(
        fname: &PathBuf,
        worker_count: NonZeroUsize,
    ) -> anyhow::Result<BAMStreamingReadPairReader> {
        let file = File::open(fname)?;
        let bgzf_reader =
            noodles::bgzf::io::MultithreadedReader::with_worker_count(worker_count, file);
        let mut reader = noodles::bam::io::Reader::from(bgzf_reader);
        let header = reader.read_header()?;

        let mut me = BAMStreamingReadPairReader {
            reader,
            header,
            pushback: None,
            last_rp: None,
        };

        //Read the first read pair right away
        me.last_rp = me.next_readpair()?;
        if me.last_rp.is_none() {
            //The BAM file is empty!
            info!("Warning: empty input BAM");
        }

        Ok(me)
    }

    pub fn next_readpair_for_transform(&mut self) -> anyhow::Result<Option<(Vec<u8>, ReadPair)>> {
        if let Some(rp) = self.last_rp.take() {
            Ok(Some(rp))
        } else {
            self.next_readpair()
        }
    }

    /// Read the next BAM record, decoded into a `BamHalf`. Skips secondary/supplementary
    /// alignments so they do not interfere with mate adjacency. Returns None at end of file.
    fn next_half(&mut self) -> anyhow::Result<Option<BamHalf>> {
        if let Some(h) = self.pushback.take() {
            return Ok(Some(h));
        }
        let mut record = BamRecord::default();
        loop {
            if self.reader.read_record_buf(&self.header, &mut record)? > 0 {
                let flags = record.flags();
                if flags.is_secondary() || flags.is_supplementary() {
                    continue;
                }
                return Ok(Some(read_to_half(&record)));
            } else {
                return Ok(None);
            }
        }
    }

    /// Produce the next read pair, consuming one record (single-end) or two adjacent records
    /// (paired-end). For a paired R1 (segmented + first-segment) the next record is verified to
    /// be its mate (segmented + last-segment, same cell+UMI); if not, the pairing assumption is
    /// violated and we error out rather than silently emit corrupt data.
    fn next_readpair(&mut self) -> anyhow::Result<Option<(Vec<u8>, ReadPair)>> {
        let first = match self.next_half()? {
            Some(h) => h,
            None => return Ok(None),
        };

        //A paired R1 must be immediately followed by its R2 mate
        if first.is_segmented && first.is_first {
            let second = self.next_half()?.ok_or_else(|| {
                anyhow::anyhow!(
                    "Paired R1 read for cell {} has no following R2 mate (end of file). \
                     The BAM must be name-collated so mates are adjacent.",
                    String::from_utf8_lossy(&first.cell_id)
                )
            })?;

            let is_mate = second.is_segmented
                && second.is_last
                && second.cell_id == first.cell_id
                && second.umi == first.umi;
            if !is_mate {
                anyhow::bail!(
                    "Expected R2 mate to follow R1 for cell {} (UMI {}), but the next record did \
                     not match. The BAM must be name-collated (e.g. `samtools collate`) so that \
                     each R1 is immediately followed by its R2.",
                    String::from_utf8_lossy(&first.cell_id),
                    String::from_utf8_lossy(&first.umi),
                );
            }

            let rp = ReadPair {
                r1: first.seq,
                r2: second.seq,
                q1: first.qual,
                q2: second.qual,
                umi: first.umi,
            };
            return Ok(Some((first.cell_id, rp)));
        }

        //Single-end (non-segmented), or a lone R2 in a filtered file: emit as R1 only
        let rp = ReadPair {
            r1: first.seq,
            r2: Vec::new(),
            q1: first.qual,
            q2: Vec::new(),
            umi: first.umi,
        };
        Ok(Some((first.cell_id, rp)))
    }
}
impl StreamingReadPairReader for BAMStreamingReadPairReader {
    fn get_reads_for_next_cell(&mut self) -> anyhow::Result<Option<ListReadWithBarcode>> {
        //Check if we arrived at the end already
        if let Some((current_cell, last_rp)) = self.last_rp.take() {
            //First push the last read pair we had
            let mut reads: Vec<ReadPair> = Vec::new();
            reads.push(last_rp);

            //Keep reading read pairs until we reach the next cell or the end
            while let Some((cell_id, rp)) = self.next_readpair()? {
                if cell_id == current_cell {
                    //This read belongs to this cell, so add to the list and continue
                    reads.push(rp);
                } else {
                    //This read belongs to the next cell, so stop reading for now
                    self.last_rp = Some((cell_id, rp));
                    break;
                }
            }

            //Package and return data
            let reads = Arc::new(reads);
            let cellid_reads = (String::from_utf8(current_cell).unwrap(), reads);

            Ok(Some(Arc::new(cellid_reads)))
        } else {
            //There is nothing more to read
            Ok(None)
        }
    }
}

///////////////////////////////
/// Given the name of a read, divide into cell ID and UMI
pub fn readname_to_cell_umi(read_name: &[u8]) -> (&[u8], &[u8]) {
    let mut splitter = read_name.split(|b| *b == b':');
    let cell_id = splitter
        .next()
        .expect("Could not parse cellID from read name");
    let umi = splitter.next().expect("Could not parse UMI from read name");

    (cell_id, umi)
}

///////////////////////////////
/// Complement a single ASCII nucleotide base (non-ACGT bases pass through unchanged)
fn complement_base(b: u8) -> u8 {
    match b {
        b'A' => b'T',
        b'C' => b'G',
        b'G' => b'C',
        b'T' => b'A',
        b'a' => b't',
        b'c' => b'g',
        b'g' => b'c',
        b't' => b'a',
        other => other,
    }
}

///////////////////////////////
/// Parse one BAM entry into a `BamHalf`, restoring original sequenced orientation
fn read_to_half(record: &BamRecord) -> BamHalf {
    let read_name: &[u8] = record.name().expect("missing read name").as_ref();
    let (cell_id, umi) = readname_to_cell_umi(read_name);

    let flags = record.flags();
    let is_reverse = flags.is_reverse_complemented();

    //BAM stores SEQ/QUAL in alignment orientation; restore the original read orientation
    let seq: Vec<u8> = if is_reverse {
        record
            .sequence()
            .as_ref()
            .iter()
            .rev()
            .map(|b| complement_base(*b))
            .collect()
    } else {
        record.sequence().as_ref().to_vec()
    };

    let qual_iter = record.quality_scores().as_ref().iter().map(|x| x + 33);
    let qual: Vec<u8> = if is_reverse {
        let mut q: Vec<u8> = qual_iter.collect();
        q.reverse();
        q
    } else {
        qual_iter.collect()
    };

    BamHalf {
        cell_id: cell_id.to_vec(),
        umi: umi.to_vec(),
        seq,
        qual,
        is_segmented: flags.is_segmented(),
        is_first: flags.is_first_segment(),
        is_last: flags.is_last_segment(),
    }
}

#[derive(Debug, Clone)]
pub struct BAMStreamingReadPairReaderFactory {}
impl BAMStreamingReadPairReaderFactory {
    pub fn new() -> BAMStreamingReadPairReaderFactory {
        BAMStreamingReadPairReaderFactory {}
    }
}
impl ConstructFromPath<BAMStreamingReadPairReader> for BAMStreamingReadPairReaderFactory {
    fn new_from_path(&self, fname: &PathBuf) -> anyhow::Result<BAMStreamingReadPairReader> {
        ///////// maybe anyhow prevents spec of reader?
        BAMStreamingReadPairReader::new(fname)
    }
}

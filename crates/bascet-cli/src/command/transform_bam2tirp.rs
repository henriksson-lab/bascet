use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    thread,
};

use anyhow::{Context, Result, bail};
use bytesize::ByteSize;
use crossbeam::channel::{Receiver, Sender, bounded};
use noodles::{bgzf::VirtualPosition, csi::binning_index::index::reference_sequence::bin::Chunk};
use tracing::info;

use super::samtools_rs::bgzf::{self, CompressedBlockMetadata};
use crate::{
    fileformat::{ReadPair, bam::BAMStreamingReadPairReader, tirp::get_tbi_path_for_tirp},
    utils::{BedTabixIndexer, atomic_temp_path, publish_atomic_output},
};

const TIRP_CHUNK_SIZE: usize = 16 * 1024 * 1024;
const MIN_INFLIGHT_CHUNKS: usize = 2;
const BGZF_LEVEL: u8 = 6;

#[derive(Debug)]
struct RawChunk {
    id: u64,
    bytes: Vec<u8>,
    index_records: Vec<LocalIndexRecord>,
    permit: ChunkPermit,
}

#[derive(Debug)]
struct CompressedChunk {
    id: u64,
    bytes: Vec<u8>,
    blocks: Vec<CompressedBlockMetadata>,
    uncompressed_len: usize,
    index_records: Vec<LocalIndexRecord>,
    _permit: ChunkPermit,
}

#[derive(Debug)]
struct LocalIndexRecord {
    cell_id: Vec<u8>,
    start_offset: usize,
    end_offset: usize,
}

#[derive(Debug)]
struct ChunkPermit {
    release_tx: Sender<()>,
}

impl Drop for ChunkPermit {
    fn drop(&mut self) {
        let _ = self.release_tx.send(());
    }
}

pub fn try_bam_to_tirp_fast_path(
    path_in: &Path,
    path_out: &Path,
    threads: usize,
    memory: Option<ByteSize>,
) -> Result<()> {
    let memory_plan = BamToTirpMemoryPlan::new(memory)?;
    info!(
        input = %path_in.display(),
        output = %path_out.display(),
        threads,
        memory = ?memory,
        current_rss = ?memory_plan.current_rss,
        memory_headroom = %memory_plan.headroom,
        available_pipeline_memory = %memory_plan.available,
        chunk_size = TIRP_CHUNK_SIZE,
        max_inflight_chunks = memory_plan.max_inflight_chunks,
        queue_capacity = memory_plan.queue_capacity,
        "BAM->TIRP fast path: starting"
    );

    let path_tmp = atomic_temp_path(&path_out.to_path_buf());
    let path_tmp_tbi = get_tbi_path_for_tirp(&path_tmp);

    let (raw_tx, raw_rx) = bounded::<RawChunk>(memory_plan.queue_capacity);
    let (compressed_tx, compressed_rx) =
        bounded::<Result<CompressedChunk>>(memory_plan.queue_capacity);
    let (permit_tx, permit_rx) = bounded::<()>(memory_plan.max_inflight_chunks);
    for _ in 0..memory_plan.max_inflight_chunks {
        permit_tx
            .send(())
            .expect("initial BAM->TIRP chunk permit send failed");
    }

    let reader_path = path_in.to_path_buf();
    let reader_threads = threads.max(1);
    let reader = thread::spawn(move || {
        read_bam_as_tirp_chunks(reader_path, reader_threads, raw_tx, permit_rx, permit_tx)
    });

    let mut workers = Vec::with_capacity(threads.max(1));
    for _ in 0..threads.max(1) {
        let rx = raw_rx.clone();
        let tx = compressed_tx.clone();
        workers.push(thread::spawn(move || compress_chunks(rx, tx)));
    }
    drop(raw_rx);
    drop(compressed_tx);

    let write_result = write_ordered_chunks(&path_tmp, compressed_rx).and_then(|indexer| {
        indexer.write_to_path(&path_tmp_tbi).with_context(|| {
            format!(
                "failed to write TIRP tabix index {}",
                path_tmp_tbi.display()
            )
        })
    });

    let reader_result = reader
        .join()
        .map_err(|_| anyhow::anyhow!("BAM->TIRP reader thread panicked"))?;
    for worker in workers {
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("BAM->TIRP compression worker panicked"))?;
    }

    reader_result?;
    write_result?;
    publish_atomic_output(&path_tmp, path_out)?;
    publish_atomic_output(path_tmp_tbi, get_tbi_path_for_tirp(&path_out.to_path_buf()))?;

    info!("BAM->TIRP fast path: finished");
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct BamToTirpMemoryPlan {
    max_inflight_chunks: usize,
    queue_capacity: usize,
    current_rss: Option<ByteSize>,
    headroom: ByteSize,
    available: ByteSize,
}

impl BamToTirpMemoryPlan {
    fn new(memory: Option<ByteSize>) -> Result<Self> {
        let Some(total_memory) = memory else {
            let default_available = ByteSize((TIRP_CHUNK_SIZE * 4 * 4) as u64);
            return Ok(Self {
                max_inflight_chunks: 4,
                queue_capacity: 2,
                current_rss: None,
                headroom: ByteSize(0),
                available: default_available,
            });
        };

        let headroom = ByteSize(
            ByteSize::mib(512)
                .as_u64()
                .max((total_memory.as_u64() as f64 * 0.10) as u64),
        );
        let current_rss =
            memory_stats::memory_stats().map(|memory| ByteSize(memory.physical_mem as u64));
        let available = match current_rss {
            Some(rss) => total_memory
                .as_u64()
                .saturating_sub(rss.as_u64())
                .saturating_sub(headroom.as_u64()),
            None => total_memory.as_u64().saturating_sub(headroom.as_u64()),
        };

        let bytes_per_inflight_chunk = (TIRP_CHUNK_SIZE as u64) * 4;
        let min_available = bytes_per_inflight_chunk * MIN_INFLIGHT_CHUNKS as u64;
        if available < min_available {
            bail!(
                "BAM->TIRP current RSS ({}) leaves only {} after reserving {headroom}; need at least {} for {MIN_INFLIGHT_CHUNKS} in-flight chunks under --memory {total_memory}",
                current_rss
                    .map(|rss| rss.to_string())
                    .unwrap_or_else(|| "unknown".to_string()),
                ByteSize(available),
                ByteSize(min_available)
            );
        }

        let max_inflight_chunks = (available / bytes_per_inflight_chunk)
            .max(MIN_INFLIGHT_CHUNKS as u64)
            .min(usize::MAX as u64) as usize;

        Ok(Self {
            max_inflight_chunks,
            queue_capacity: queue_capacity_for_inflight_chunks(max_inflight_chunks),
            current_rss,
            headroom,
            available: ByteSize(available),
        })
    }
}

fn queue_capacity_for_inflight_chunks(max_inflight_chunks: usize) -> usize {
    (max_inflight_chunks / 2).max(1)
}

fn read_bam_as_tirp_chunks(
    path_in: PathBuf,
    threads: usize,
    raw_tx: Sender<RawChunk>,
    permit_rx: Receiver<()>,
    permit_tx: Sender<()>,
) -> Result<()> {
    let mut reader = BAMStreamingReadPairReader::new_with_threads(&path_in, threads)
        .with_context(|| format!("failed to open BAM input {}", path_in.display()))?;

    let mut chunk = RawChunk {
        id: 0,
        bytes: Vec::with_capacity(TIRP_CHUNK_SIZE),
        index_records: Vec::new(),
        permit: acquire_chunk_permit(&permit_rx, &permit_tx)?,
    };
    let mut records = 0_u64;
    let mut last_cell_id: Option<Vec<u8>> = None;

    while let Some((cell_id, read_pair)) = reader.next_readpair_for_transform()? {
        validate_cell_order(&last_cell_id, &cell_id)?;

        if chunk.bytes.len() >= TIRP_CHUNK_SIZE && !chunk.bytes.is_empty() {
            chunk = send_chunk(&raw_tx, chunk, &permit_rx, &permit_tx)?;
        }

        let start_offset = chunk.bytes.len();
        write_tirp_record(&mut chunk.bytes, &cell_id, &read_pair)?;
        let end_offset = chunk.bytes.len();
        chunk.index_records.push(LocalIndexRecord {
            cell_id,
            start_offset,
            end_offset,
        });
        last_cell_id = Some(chunk.index_records.last().unwrap().cell_id.clone());
        records += 1;
    }

    if !chunk.bytes.is_empty() {
        send_final_chunk(&raw_tx, chunk)?;
    }

    info!(records, "BAM->TIRP fast path: reader finished");
    Ok(())
}

fn acquire_chunk_permit(permit_rx: &Receiver<()>, permit_tx: &Sender<()>) -> Result<ChunkPermit> {
    permit_rx
        .recv()
        .map_err(|_| anyhow::anyhow!("BAM->TIRP chunk permit channel closed"))?;
    Ok(ChunkPermit {
        release_tx: permit_tx.clone(),
    })
}

fn send_chunk(
    raw_tx: &Sender<RawChunk>,
    chunk: RawChunk,
    permit_rx: &Receiver<()>,
    permit_tx: &Sender<()>,
) -> Result<RawChunk> {
    let next_id = chunk.id + 1;
    raw_tx
        .send(chunk)
        .map_err(|_| anyhow::anyhow!("BAM->TIRP compression workers stopped unexpectedly"))?;

    Ok(RawChunk {
        id: next_id,
        bytes: Vec::with_capacity(TIRP_CHUNK_SIZE),
        index_records: Vec::new(),
        permit: acquire_chunk_permit(permit_rx, permit_tx)?,
    })
}

fn send_final_chunk(raw_tx: &Sender<RawChunk>, chunk: RawChunk) -> Result<()> {
    raw_tx
        .send(chunk)
        .map_err(|_| anyhow::anyhow!("BAM->TIRP compression workers stopped unexpectedly"))
}

fn validate_cell_order(last_cell_id: &Option<Vec<u8>>, cell_id: &[u8]) -> Result<()> {
    let Some(last_cell_id) = last_cell_id else {
        return Ok(());
    };

    if cell_id < last_cell_id.as_slice() {
        bail!(
            "BAM->TIRP requires reads sorted/grouped by cell id for tabix indexing, but saw cell {} after later cell {}. The input may be position-sorted; provide a cell/name-collated BAM or use a sorted TIRP input.",
            String::from_utf8_lossy(cell_id),
            String::from_utf8_lossy(last_cell_id)
        );
    }

    Ok(())
}

fn write_tirp_record(buf: &mut Vec<u8>, cell_id: &[u8], read: &ReadPair) -> Result<()> {
    buf.write_all(cell_id)?;
    buf.write_all(b"\t1\t1\t")?;
    buf.write_all(&read.r1)?;
    buf.write_all(b"\t")?;
    buf.write_all(&read.r2)?;
    buf.write_all(b"\t")?;
    buf.write_all(&read.q1)?;
    buf.write_all(b"\t")?;
    buf.write_all(&read.q2)?;
    buf.write_all(b"\t")?;
    buf.write_all(&read.umi)?;
    buf.write_all(b"\n")?;
    Ok(())
}

fn compress_chunks(rx: Receiver<RawChunk>, tx: Sender<Result<CompressedChunk>>) {
    while let Ok(raw) = rx.recv() {
        let result = compress_one_chunk(raw);
        if tx.send(result).is_err() {
            return;
        }
    }
}

fn compress_one_chunk(raw: RawChunk) -> Result<CompressedChunk> {
    let uncompressed_len = raw.bytes.len();
    let (bytes, blocks) = bgzf::compress_chunk_with_block_metadata(&raw.bytes, BGZF_LEVEL)
        .context("failed to compress TIRP BGZF chunk")?;
    Ok(CompressedChunk {
        id: raw.id,
        bytes,
        blocks,
        uncompressed_len,
        index_records: raw.index_records,
        _permit: raw.permit,
    })
}

fn write_ordered_chunks(
    path_tmp: &Path,
    rx: Receiver<Result<CompressedChunk>>,
) -> Result<BedTabixIndexer> {
    let file = File::create(path_tmp)
        .with_context(|| format!("failed to create TIRP output {}", path_tmp.display()))?;
    let mut writer = BufWriter::new(file);
    let mut indexer = BedTabixIndexer::new();
    let mut pending: BinaryHeap<Reverse<PendingCompressedChunk>> = BinaryHeap::new();
    let mut next_id = 0_u64;
    let mut compressed_offset = 0_u64;

    while let Ok(result) = rx.recv() {
        let chunk = result?;
        pending.push(Reverse(PendingCompressedChunk(chunk)));

        while let Some(Reverse(PendingCompressedChunk(chunk))) = pending.peek() {
            if chunk.id != next_id {
                break;
            }
            let Reverse(PendingCompressedChunk(chunk)) = pending.pop().unwrap();
            write_one_chunk(&mut writer, &mut indexer, &chunk, compressed_offset)?;
            compressed_offset += chunk.bytes.len() as u64;
            next_id += 1;
        }
    }

    if !pending.is_empty() {
        bail!("BAM->TIRP writer exited with pending out-of-order chunks");
    }

    writer.write_all(&bgzf::EOF_BLOCK)?;
    writer.flush()?;
    Ok(indexer)
}

fn write_one_chunk(
    writer: &mut impl Write,
    indexer: &mut BedTabixIndexer,
    chunk: &CompressedChunk,
    compressed_chunk_start: u64,
) -> Result<()> {
    writer.write_all(&chunk.bytes)?;

    for record in &chunk.index_records {
        let start = virtual_position_for_offset(
            &chunk.blocks,
            chunk.uncompressed_len,
            compressed_chunk_start,
            record.start_offset,
        )?;
        let end = virtual_position_for_offset(
            &chunk.blocks,
            chunk.uncompressed_len,
            compressed_chunk_start,
            record.end_offset,
        )?;
        let cell_id = std::str::from_utf8(&record.cell_id)
            .context("TIRP cell id is not valid UTF-8 and cannot be tabix-indexed")?;
        indexer.add_record(cell_id, 1, 1, Chunk::new(start, end))?;
    }

    Ok(())
}

fn virtual_position_for_offset(
    blocks: &[CompressedBlockMetadata],
    uncompressed_len: usize,
    compressed_chunk_start: u64,
    offset: usize,
) -> Result<VirtualPosition> {
    for block in blocks {
        let block_end = block.uncompressed_start + block.uncompressed_len;
        if offset < block_end {
            let in_block = u16::try_from(offset - block.uncompressed_start)
                .context("BGZF in-block offset exceeded u16")?;
            return VirtualPosition::try_from((
                compressed_chunk_start + block.compressed_start as u64,
                in_block,
            ))
            .context("failed to build BGZF virtual position");
        }

        if offset == block_end {
            let next_compressed_start = compressed_chunk_start
                + block.compressed_start as u64
                + block.compressed_len as u64;
            return VirtualPosition::try_from((next_compressed_start, 0))
                .context("failed to build BGZF virtual position");
        }
    }

    if offset == uncompressed_len {
        let compressed_len: u64 = blocks.iter().map(|block| block.compressed_len as u64).sum();
        return VirtualPosition::try_from((compressed_chunk_start + compressed_len, 0))
            .context("failed to build BGZF virtual position");
    }

    bail!("offset {offset} is outside compressed chunk of {uncompressed_len} bytes")
}

struct PendingCompressedChunk(CompressedChunk);

impl PartialEq for PendingCompressedChunk {
    fn eq(&self, other: &Self) -> bool {
        self.0.id == other.0.id
    }
}

impl Eq for PendingCompressedChunk {}

impl PartialOrd for PendingCompressedChunk {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingCompressedChunk {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.id.cmp(&other.0.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam::channel::unbounded;
    use std::{fs::File, io::Read};

    fn test_chunk_permit() -> ChunkPermit {
        let (release_tx, _release_rx) = unbounded();
        ChunkPermit { release_tx }
    }

    #[test]
    fn queue_capacity_is_derived_from_total_inflight_chunks() {
        assert_eq!(queue_capacity_for_inflight_chunks(2), 1);
        assert_eq!(queue_capacity_for_inflight_chunks(3), 1);
        assert_eq!(queue_capacity_for_inflight_chunks(4), 2);
        assert_eq!(queue_capacity_for_inflight_chunks(9), 4);
    }

    #[test]
    fn memory_plan_without_budget_uses_default_queueing() {
        let plan = BamToTirpMemoryPlan::new(None).unwrap();

        assert_eq!(plan.max_inflight_chunks, 4);
        assert_eq!(plan.queue_capacity, 2);
        assert_eq!(plan.current_rss, None);
    }

    #[test]
    fn memory_plan_rejects_budget_too_small_after_rss_and_headroom() {
        let err = BamToTirpMemoryPlan::new(Some(ByteSize::mib(64))).unwrap_err();

        assert!(
            err.to_string().contains("need at least"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn tirp_record_encoding_matches_expected_columns() {
        let read = ReadPair {
            r1: b"ACGT".to_vec(),
            r2: b"TGCA".to_vec(),
            q1: b"IIII".to_vec(),
            q2: b"JJJJ".to_vec(),
            umi: b"UMI".to_vec(),
        };
        let mut buf = Vec::new();

        write_tirp_record(&mut buf, b"cell-1", &read).unwrap();

        assert_eq!(buf, b"cell-1\t1\t1\tACGT\tTGCA\tIIII\tJJJJ\tUMI\n");
    }

    #[test]
    fn cell_order_check_accepts_sorted_cells() {
        assert!(validate_cell_order(&None, b"cell-a").is_ok());
        assert!(validate_cell_order(&Some(b"cell-a".to_vec()), b"cell-a").is_ok());
        assert!(validate_cell_order(&Some(b"cell-a".to_vec()), b"cell-b").is_ok());
    }

    #[test]
    fn cell_order_check_rejects_decreasing_cells() {
        let err = validate_cell_order(&Some(b"cell-z".to_vec()), b"cell-a").unwrap_err();

        assert!(
            err.to_string().contains("position-sorted"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn ordered_writer_emits_bgzf_chunks_in_chunk_id_order() {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("out.tirp.gz");
        let (tx, rx) = unbounded();

        let first = compress_one_chunk(RawChunk {
            id: 0,
            bytes: b"cell-a\t1\t1\tAC\tTG\tII\tJJ\tUMI1\n".to_vec(),
            index_records: vec![LocalIndexRecord {
                cell_id: b"cell-a".to_vec(),
                start_offset: 0,
                end_offset: b"cell-a\t1\t1\tAC\tTG\tII\tJJ\tUMI1\n".len(),
            }],
            permit: test_chunk_permit(),
        })
        .unwrap();
        let second = compress_one_chunk(RawChunk {
            id: 1,
            bytes: b"cell-b\t1\t1\tGA\tTC\tHH\tKK\tUMI2\n".to_vec(),
            index_records: vec![LocalIndexRecord {
                cell_id: b"cell-b".to_vec(),
                start_offset: 0,
                end_offset: b"cell-b\t1\t1\tGA\tTC\tHH\tKK\tUMI2\n".len(),
            }],
            permit: test_chunk_permit(),
        })
        .unwrap();

        tx.send(Ok(second)).unwrap();
        tx.send(Ok(first)).unwrap();
        drop(tx);

        let _indexer = write_ordered_chunks(&path, rx).unwrap();

        let mut decoded = String::new();
        bgzf::Reader::new(File::open(path).unwrap())
            .read_to_string(&mut decoded)
            .unwrap();
        assert_eq!(
            decoded,
            "cell-a\t1\t1\tAC\tTG\tII\tJJ\tUMI1\ncell-b\t1\t1\tGA\tTC\tHH\tKK\tUMI2\n"
        );
    }

    #[test]
    fn virtual_position_translation_handles_block_boundaries() {
        let blocks = vec![
            CompressedBlockMetadata {
                uncompressed_start: 0,
                uncompressed_len: 100,
                compressed_start: 0,
                compressed_len: 40,
            },
            CompressedBlockMetadata {
                uncompressed_start: 100,
                uncompressed_len: 50,
                compressed_start: 40,
                compressed_len: 30,
            },
        ];

        assert_eq!(
            virtual_position_for_offset(&blocks, 150, 1_000, 25).unwrap(),
            VirtualPosition::try_from((1_000, 25)).unwrap()
        );
        assert_eq!(
            virtual_position_for_offset(&blocks, 150, 1_000, 100).unwrap(),
            VirtualPosition::try_from((1_040, 0)).unwrap()
        );
        assert_eq!(
            virtual_position_for_offset(&blocks, 150, 1_000, 149).unwrap(),
            VirtualPosition::try_from((1_040, 49)).unwrap()
        );
        assert_eq!(
            virtual_position_for_offset(&blocks, 150, 1_000, 150).unwrap(),
            VirtualPosition::try_from((1_070, 0)).unwrap()
        );
    }
}

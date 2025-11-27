use std::{num::NonZero, ptr::NonNull, sync::atomic::AtomicU64};

use bascet_core::*;
use bascet_io::{FASTQRecordParser, bgzf::BGZFDecoder};
use bounded_integer::{BoundedU64, BoundedUsize};
use bytesize::ByteSize;
use smallvec::SmallVec;

#[derive(Composite, Default)]
#[attrs(Id, Sequence, Quality)]
#[backing(ArenaBacking)]
struct FASTQCell {
    id: &'static [u8],
    sequence: &'static [u8],
    quality: &'static [u8],
    arena_backing: SmallVec<[ArenaView<u8>; 2]>,
}

#[test]
fn test_stream_bgzf_fastq() {
    let decoder = BGZFDecoder::builder()
        .path("../data/P32705_1002_S1_L002_R1_001.fastq.gz")
        .num_threads(BoundedU64::const_new::<11>())
        .build()
        .unwrap();
    let parser = FASTQRecordParser::new();
    let mut stream = Stream::builder()
        .decoder(decoder)
        .parser(parser)
        .n_buffers(BoundedUsize::new(1024).unwrap())
        .sizeof_arena(ByteSize::mib(32))
        .sizeof_buffer(ByteSize::gib(14))
        .build()
        .unwrap();

    use std::collections::VecDeque;
    use std::time::Instant;

    let start = Instant::now();
    let mut last_print = start;
    let mut i = 0;
    let mut throughputs = VecDeque::with_capacity(60);

    while let Ok(Some(cell)) = stream.next::<FASTQCell>() {
        i += 1;
        if i % 1_000_000 == 0 {
            let now = Instant::now();
            let elapsed = now.duration_since(last_print).as_secs_f64();
            let throughput = 1_000_000.0 / elapsed / 1_000_000.0;

            throughputs.push_back(throughput);
            if throughputs.len() > 60 {
                throughputs.pop_front();
            }

            let avg_throughput: f64 = throughputs.iter().sum::<f64>() / throughputs.len() as f64;

            println!(
                "{}M records | {:.2} M/rec/s (rolling avg: {:.2} M/rec/s)",
                i / 1_000_000,
                throughput,
                avg_throughput
            );

            last_print = now;
        }
    }

    let total_elapsed = start.elapsed().as_secs_f64();
    let overall_throughput = i as f64 / total_elapsed / 1_000_000.0;
    println!(
        "\nCompleted: {} records in {:.2}s | Overall: {:.2} M/rec/s",
        i, total_elapsed, overall_throughput
    );
}

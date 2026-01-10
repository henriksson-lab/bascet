use bascet_core::*;
use bascet_io::{codec, parse, tirp};
use bounded_integer::{BoundedU64, BoundedUsize};
use bytesize::ByteSize;

use std::collections::VecDeque;
use std::time::Instant;

// #[derive(Composite, Default)]
// #[bascet(attrs = (Id, SequencePair = vec_sequence_pairs, QualityPair = vec_quality_pairs, Umi = vec_umis), backing = ArenaBacking, marker = AsCell<Accumulate>)]
// pub struct Cell {
//     id: &'static [u8],
//     #[collection]
//     vec_sequence_pairs: Vec<(&'static [u8], &'static [u8])>,
//     #[collection]
//     vec_quality_pairs: Vec<(&'static [u8], &'static [u8])>,
//     #[collection]
//     vec_umis: Vec<&'static [u8]>,

//     // SAFETY: exposed ONLY to allow conversion outside this crate.
//     //         be VERY careful modifying this at all
//     arena_backing: smallvec::SmallVec<[ArenaView<u8>; 2]>,
// }

#[test]
fn test_stream_bgzf_tirp() {
    let decoder = codec::BBGZDecoder::builder()
        .with_path("../temp/29433167_merge_0_1.tirp.gz")
        .countof_threads(BoundedU64::const_new::<11>())
        .build()
        .unwrap();
    let parser = parse::Tirp::builder().build().unwrap();

    let mut stream = Stream::builder()
        .with_decoder(decoder)
        .with_parser(parser)
        .countof_buffers(BoundedUsize::const_new::<1024>())
        .sizeof_decode_arena(ByteSize::mib(8))
        .sizeof_decode_buffer(ByteSize::gib(1))
        .build()
        .unwrap();

    let mut query = stream
        .query::<tirp::Cell>()
        .group_relaxed_with_context::<Id, Id, _>(|id: &&'static [u8], id_ctx: &&'static [u8]| {
            match id.cmp(id_ctx) {
                std::cmp::Ordering::Less => panic!("Unordered record list"),
                std::cmp::Ordering::Equal => QueryResult::Keep,
                std::cmp::Ordering::Greater => QueryResult::Emit,
            }
        });

    let start = Instant::now();
    let mut last_print = start;
    let mut i = 0;
    let mut throughputs = VecDeque::with_capacity(60);

    while let Ok(Some(cell)) = query.next() {
        i += 1;

        if i % 1_000 == 0 {
            let now = Instant::now();
            let elapsed = now.duration_since(last_print).as_secs_f64();
            let throughput = 1_000.0 / elapsed / 1_000.0;

            throughputs.push_back(throughput);
            if throughputs.len() > 60 {
                throughputs.pop_front();
            }

            let avg_throughput: f64 = throughputs.iter().sum::<f64>() / throughputs.len() as f64;

            println!(
                "{}K cells | {:.2} K cells/s (rolling avg: {:.2} K cells/s). Current ID: {:?}",
                i / 1_000,
                throughput,
                avg_throughput,
                String::from_utf8_lossy(cell.get_ref::<Id>())
            );
            last_print = now;
        }
    }

    let total_elapsed = start.elapsed().as_secs_f64();
    let overall_throughput = i as f64 / total_elapsed / 1_000.0;
    println!(
        "\nCompleted: {} cells in {:.2}s | Overall: {:.2} K cells/s",
        i, total_elapsed, overall_throughput
    );
}

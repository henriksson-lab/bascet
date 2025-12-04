use bascet_core::*;
use bascet_io::{decode, parse, tirp};
use bounded_integer::{BoundedU64, BoundedUsize};
use bytesize::ByteSize;

#[test]
fn test_stream_bgzf_tirp() {
    let decoder = decode::Bgzf::builder()
        .path("../data/shard.1.tirp.gz")
        .num_threads(BoundedU64::const_new::<11>())
        .build()
        .unwrap();
    let parser = parse::Tirp::builder().build().unwrap();

    let mut stream = Stream::builder()
        .with_decoder(decoder)
        .with_parser(parser)
        .countof_buffers(BoundedUsize::const_new::<1024>())
        .sizeof_arena(ByteSize::mib(8))
        .sizeof_buffer(ByteSize::gib(1))
        .build()
        .unwrap();

    use std::collections::VecDeque;
    use std::time::Instant;

    let start = Instant::now();
    let mut last_print = start;
    let mut i = 0;
    let mut throughputs = VecDeque::with_capacity(60);

    while let Ok(Some(cell)) = stream.next::<tirp::Record>() {
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
                "{}M records | {:.2} M/rec/s (rolling avg: {:.2} M/rec/s). Current: {:?}",
                i / 1_000_000,
                throughput,
                avg_throughput,
                String::from_utf8_lossy(cell.get_ref::<Id>())
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

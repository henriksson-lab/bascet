use std::{
    collections::VecDeque,
    fs::File,
    io::{BufWriter, Write},
    num::NonZero,
    ptr::NonNull,
    sync::atomic::AtomicU64,
    time::Instant,
};

use bascet_core::*;
use bascet_io::{codec, fastq, parse, MARKER_EOF};
use binrw::io::BufReader;
use bounded_integer::{BoundedU64, BoundedUsize};
use bytesize::ByteSize;
use smallvec::SmallVec;

#[test]
fn test_stream_bbgz_blocks() {
    let file = File::open("../temp/1768323488_merge_0_1.tirp.bbgz").unwrap();
    let reader = BufReader::new(file);
    let decoder = codec::plain::PlaintextDecoder::builder()
        .with_reader(reader)
        .build();
    let parser = parse::bbgz::parser();

    let mut stream = Stream::builder()
        .with_decoder(decoder)
        .with_parser(parser)
        .countof_buffers(BoundedUsize::const_new::<1024>())
        .sizeof_decode_arena(ByteSize::mib(32))
        .sizeof_decode_buffer(ByteSize::gib(1))
        .build();

    // Create BBGZ writer with compression level 0
    let output_file = File::create("../temp/output.tirp.bbgz").unwrap();
    let mut output_writer = BufWriter::new(output_file);

    let start = Instant::now();
    let mut last_print = start;
    let mut i = 0;
    let mut total_bytes: u64 = 0;
    let mut bytes_since_last_print: u64 = 0;
    let mut throughputs = VecDeque::with_capacity(60);

    while let Ok(Some(block)) = stream.query::<parse::bbgz::Block>().next() {
        i += 1;

        // Write the block to the output file
        let header = block.as_bytes::<Header>();
        let raw = block.as_bytes::<Raw>();
        let trailer = block.as_bytes::<Trailer>();

        output_writer.write_all(header).unwrap();
        output_writer.write_all(raw).unwrap();
        output_writer.write_all(trailer).unwrap();

        let block_size = (header.len() + raw.len() + trailer.len()) as u64;
        total_bytes += block_size;
        bytes_since_last_print += block_size;

        if i % 10_000 == 0 {
            let now = Instant::now();
            let elapsed = now.duration_since(last_print).as_secs_f64();
            let throughput_bytes_per_sec = bytes_since_last_print as f64 / elapsed;

            throughputs.push_back(throughput_bytes_per_sec);
            if throughputs.len() > 60 {
                throughputs.pop_front();
            }

            let avg_throughput: f64 = throughputs.iter().sum::<f64>() / throughputs.len() as f64;

            println!(
                "{}K records | {} | {}/s (rolling avg: {}/s). Current: {:?}",
                i / 1000,
                ByteSize::b(total_bytes),
                ByteSize::b(throughput_bytes_per_sec as u64),
                ByteSize::b(avg_throughput as u64),
                String::from_utf8_lossy(block.get_ref::<Id>())
            );

            last_print = now;
            bytes_since_last_print = 0;
        }
    }

    let total_elapsed = start.elapsed().as_secs_f64();
    let overall_throughput = total_bytes as f64 / total_elapsed;
    println!(
        "\nCompleted: {} records ({}) in {:.2}s | Overall: {}/s",
        i,
        ByteSize::b(total_bytes),
        total_elapsed,
        ByteSize::b(overall_throughput as u64)
    );

    output_writer.write_all(MARKER_EOF).unwrap();
    output_writer.flush().unwrap();
    drop(output_writer);

    let input_file = File::open("../temp/1768323488_merge_0_1.tirp.bbgz").unwrap();
    let input_reader = BufReader::new(input_file);
    let input_decoder = codec::plain::PlaintextDecoder::builder()
        .with_reader(input_reader)
        .build();
    let input_parser = parse::bbgz::parser();
    let mut input_stream = Stream::builder()
        .with_decoder(input_decoder)
        .with_parser(input_parser)
        .countof_buffers(BoundedUsize::const_new::<1024>())
        .sizeof_decode_arena(ByteSize::mib(32))
        .sizeof_decode_buffer(ByteSize::gib(1))
        .build();

    let output_file = File::open("../temp/output.tirp.bbgz").unwrap();
    let output_reader = BufReader::new(output_file);
    let output_decoder = codec::plain::PlaintextDecoder::builder()
        .with_reader(output_reader)
        .build();
    let output_parser = parse::bbgz::parser();
    let mut output_stream = Stream::builder()
        .with_decoder(output_decoder)
        .with_parser(output_parser)
        .countof_buffers(BoundedUsize::const_new::<1024>())
        .sizeof_decode_arena(ByteSize::mib(32))
        .sizeof_decode_buffer(ByteSize::gib(1))
        .build();

    let mut compared: u64 = 0;
    loop {
        let input_block = input_stream.query::<parse::bbgz::Block>().next();
        let output_block = output_stream.query::<parse::bbgz::Block>().next();

        match (input_block, output_block) {
            (Ok(Some(input)), Ok(Some(output))) => {
                compared += 1;

                // Compare IDs
                assert_eq!(
                    input.as_bytes::<Id>(),
                    output.as_bytes::<Id>(),
                    "Block {} ID mismatch",
                    compared
                );

                // Compare raw block data
                let input_raw = input.as_bytes::<Raw>();
                let output_raw = output.as_bytes::<Raw>();

                if input_raw != output_raw {
                    println!("\nBlock {} data mismatch", compared);
                    println!("Input length: {}", input_raw.len());
                    println!("Output length: {}", output_raw.len());
                    println!(
                        "\nInput as string:\n{:?}",
                        String::from_utf8_lossy(input_raw)
                    );
                    println!(
                        "\nOutput as string:\n{:?}",
                        String::from_utf8_lossy(output_raw)
                    );
                    panic!("Block data mismatch");
                }
            }
            (Ok(None), Ok(None)) => {
                break;
            }
            (Ok(None), Ok(Some(_))) => {
                panic!("Output file has more blocks than input file");
            }
            (Ok(Some(_)), Ok(None)) => {
                panic!("Input file has more blocks than output file");
            }
            (Err(e), _) => {
                panic!("Error reading input file: {:?}", e);
            }
            (_, Err(e)) => {
                panic!("Error reading output file: {:?}", e);
            }
        }
    }
}

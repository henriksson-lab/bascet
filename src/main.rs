use bascet::{
    command,
    io::{
        detect::{self},
        format, parse_readpair, tirp, AutoCountSketchStream, BascetFile, BascetRead, BascetStream,
        BascetStreamToken,
    },
    kmer::{
        kmc_counter::{CountSketch, KmerCounter},
        KMERCodec,
    },
    log_critical, log_error, log_info, runtime,
};
use clap::{Parser, Subcommand};
use itertools::Itertools;
use libc::exit;
use std::{
    fmt,
    fs::File,
    io::{BufWriter, Write},
    panic,
    path::{Path, PathBuf},
    process::ExitCode,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

///////////////////////////////
/// Parser for commandline options, top level
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: runtime::Commands,

    #[arg(long, default_value = "skip")]
    error_mode: runtime::ErrorMode,

    #[arg(long, default_value = "trace")]
    log_level: runtime::LogLevel,

    #[arg(long, default_value = "both")]
    log_mode: runtime::LogMode,

    #[arg(long, default_value = "./latest.log")]
    log_path: std::path::PathBuf,
}

///////////////////////////////
/// Entry point into the software
fn main() -> ExitCode {
    let start = std::time::Instant::now();
    let cli = Cli::parse();

    let _config = runtime::CONFIG.set(runtime::Config {
        error_mode: cli.error_mode,
        log_level: cli.log_level,
        log_mode: cli.log_mode,
        log_path: cli.log_path.clone(),
    });

    let _logger = runtime::setup_global_logger(
        runtime::CONFIG.get().unwrap().log_level,
        runtime::CONFIG.get().unwrap().log_mode,
        runtime::CONFIG.get().unwrap().log_path.clone(),
    );

    // Ensure that a panic in a thread results in the entire program terminating
    let panic_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // nicer formatting
        let msg = format!("\n\n{}\n", panic_info).replace('\n', "\n\t");
        slog_scope::crit!(""; "panicked" => %msg);

        if let Ok(mut guard) = runtime::ASYNC_GUARD.lock() {
            if let Some(async_guard) = guard.take() {
                drop(async_guard); // Waits for all logs to flush
            }
        }

        println!("Exiting, took: {:#?}", start.elapsed());

        panic_hook(panic_info);
        std::process::exit(1);
    }));

    log_info!("================================================");
    log_info!("Running Bascet"; "v" => env!("CARGO_PKG_VERSION"));
    log_info!(""; "Command" => ?cli.command, "Error Handling Mode" => %cli.error_mode);
    log_info!(""; "Log Mode" => %cli.log_mode, "Log Level" => %cli.log_level);

    match cli.log_mode {
        runtime::LogMode::Both | runtime::LogMode::Path => {
            log_info!(""; "Log Path" => cli.log_path.display());
        }
        _ => {}
    }
    log_info!("================================================");

    for i in 1..=20 {
        // let input_path = format!("./data/assembled/skesa.{}.zip", i);
        let input_path = format!("./data/reads/filtered.{}.tirp.gz", i);
        let output_path = format!("./out/countsketch.{}.txt", i - 1);
        log_info!("Processing"; "input" => &input_path, "output" => &output_path);

        #[derive(Clone, Debug)]
        struct LocalState {
            sketch: CountSketch,
        }

        #[derive(Debug)]
        struct GlobalState {
            kmer_size: Arc<usize>,
            buf_writer: Arc<Mutex<BufWriter<File>>>,
        }
        unsafe impl Send for GlobalState {}
        unsafe impl Sync for GlobalState {}

        let global_state = GlobalState {
            kmer_size: Arc::new(31),
            buf_writer: Arc::new(Mutex::new(BufWriter::new(
                File::create(&output_path).unwrap(),
            ))),
        };
        let local_states = (0..4)
            .map(|_| LocalState {
                sketch: CountSketch::new(100),
            })
            .collect_vec();

        let file = detect::which_file(&input_path).unwrap();
        println!("{:?}", file);
        struct CountSketchToken {
            id: Vec<u8>,           // Fixed field name
            payload: Vec<Vec<u8>>, // Fixed field name
        }

        impl BascetStreamToken<Vec<u8>, Vec<Vec<u8>>> for CountSketchToken {
            fn new(id: Vec<u8>, payload: Vec<Vec<u8>>) -> Self {
                Self { id, payload }
            }

            fn id(&self) -> &Vec<u8> {
                &self.id
            }

            fn payload(&self) -> &Vec<Vec<u8>> {
                &self.payload
            }
        }
        let mut tirp_stream: AutoCountSketchStream<CountSketchToken, Vec<u8>, Vec<Vec<u8>>> =
            detect::which_countsketch_stream(file).expect("Unsupported!");
        tirp_stream.set_reader_threads(8);
        tirp_stream.set_worker_threads(4);
        tirp_stream.par_map(global_state, local_states, |token, global, local| {
            let reads = token.payload;
            let k = Arc::clone(&global.kmer_size);
            let sketch = &mut local.sketch;
            sketch.reset();

            for unparsed_rp in reads.iter() {
                let rp = parse_readpair(&unparsed_rp).unwrap();
                for window in rp.r1.windows(*k) {
                    sketch.add(window);
                }

                for window in rp.r2.windows(*k) {
                    sketch.add(window);
                }
            }
            // and then write to disk but thats boring code
            let mut result = token.id.clone();
            result.push(b'\t');
            result.extend_from_slice(&reads.len().to_string().as_bytes());
            result.push(b'\t');
            for (i, &value) in sketch.sketch.iter().enumerate() {
                if i > 0 {
                    result.push(b'\t');
                }
                result.extend_from_slice(value.to_string().as_bytes());
            }
            result.push(b'\n');

            if let Ok(mut buf_writer) = global.buf_writer.try_lock() {
                let _ = buf_writer.write_all(&result);
            }
        });
    }
    //     tirp_stream.set_reader_threads(8);
    //     tirp_stream.set_worker_threads(4);
    //     tirp_stream.par_map(
    //         global_state,
    //         local_states,
    //         |token, global, local| match token {
    //             AutoToken::tirp(StreamToken::Full { cell_id, reads }) => {
    //                 let k = Arc::clone(&global.kmer_size);
    //                 let sketch = &mut local.sketch;
    //                 sketch.reset();

    //                 for unparsed_rp in reads.iter() {
    //                     let rp = parse_readpair(&unparsed_rp).unwrap();
    //                     for window in rp.r1.windows(*k) {
    //                         sketch.add(window);
    //                     }

    //                     for window in rp.r2.windows(*k) {
    //                         sketch.add(window);
    //                     }
    //                 }
    //                 // and then write to disk but thats boring code
    //                 let mut result = cell_id.clone();
    //                 result.push(b'\t');
    //                 result.extend_from_slice(&reads.len().to_string().as_bytes());
    //                 result.push(b'\t');
    //                 for (i, &value) in sketch.sketch.iter().enumerate() {
    //                     if i > 0 {
    //                         result.push(b'\t');
    //                     }
    //                     result.extend_from_slice(value.to_string().as_bytes());
    //                 }
    //                 result.push(b'\n');

    //                 if let Ok(mut buf_writer) = global.buf_writer.try_lock() {
    //                     let _ = buf_writer.write_all(&result);
    //                 }
    //             }
    //             _ => todo!(),
    //         },
    //     );
    // }

    // let result = match cli.command {
    //     Commands::Getraw(mut cmd) => cmd.try_execute(),

    //     Commands::Mapcell(mut cmd) => cmd.try_execute(), // NOTE

    //     Commands::Extract(mut cmd) => cmd.try_execute(),
    //     Commands::Shardify(mut cmd) => cmd.try_execute(),
    //     Commands::Transform(mut cmd) => cmd.try_execute(),
    //     Commands::Featurise(mut cmd) => cmd.try_execute(),
    //     Commands::MinhashHist(mut cmd) => cmd.try_execute(),
    //     Commands::QueryKmc(mut cmd) => cmd.try_execute(),
    //     Commands::QueryFq(mut cmd) => cmd.try_execute(),
    //     Commands::Bam2fragments(mut cmd) => cmd.try_execute(),
    //     Commands::Kraken(mut cmd) => cmd.try_execute(),
    //     Commands::Countchrom(mut cmd) => cmd.try_execute(),
    //     Commands::Countfeature(mut cmd) => cmd.try_execute(),
    //     Commands::PipeSamAddTags(mut cmd) => cmd.try_execute(),
    //     Commands::Countsketch(mut cmd) => cmd.try_execute(),
    //     Commands::ExtractStream(mut cmd) => cmd.try_execute(),
    // };

    // if let Err(e) = result {
    //     eprintln!("Error: {}", e);
    //     return ExitCode::FAILURE;
    // }

    if let Ok(mut guard) = runtime::ASYNC_GUARD.lock() {
        if let Some(async_guard) = guard.take() {
            drop(async_guard); // Waits for all logs to flush
        }
    }

    println!("Exiting, took: {:#?}", start.elapsed());

    return ExitCode::SUCCESS;
}

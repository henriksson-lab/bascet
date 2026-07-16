use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bascet_core::set::Set;
use bascet_core::{Apply, Emit, Error, Pipeline, Runtime, sink};

const WORK: u32 = 1_000_000;
const BURST: usize = 2 << 16;
const FAST: usize = 100;
const SLOW: usize = 10000;
const STALL: u64 = 2000;
const ITERS: usize = 5;

fn main() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    let items: Vec<u32> = (0..(FAST + SLOW * ITERS) as u32).collect();
    let start = Instant::now();
    let runner = Runtime::builder().build().pipeline::<()>(
        Pipeline::builder()
            .source(Burst::new(items, FAST, SLOW, STALL))
            .layer(Job)
            .sink(sink::drain::<u32>()),
    );
    runner.join().unwrap();
    println!("bursty: {:?}", start.elapsed());
}

#[derive(Clone)]
struct Burst {
    state: Arc<Mutex<BurstState>>,
}

struct BurstState {
    items: Vec<u32>,
    pos: usize,
    fast: usize,
    slow: usize,
    stall: u64,
    buf: VecDeque<u32>,
}

impl Burst {
    fn new(items: Vec<u32>, fast: usize, slow: usize, stall: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new(BurstState {
                items,
                pos: 0,
                fast,
                slow,
                stall,
                buf: VecDeque::new(),
            })),
        }
    }
}

impl Apply for Burst {
    type Input = ();
    type Output = u32;
    type Provides = ();
    type Requires = ();

    fn apply<W: Set>(&mut self, _: (), out: &mut Emit<u32, W>) -> Result<(), Error> {
        let item = {
            let mut state = self.state.lock().expect("burst source lock poisoned");
            match state.buf.pop_front() {
                Some(item) => item,
                None => {
                    if state.pos >= state.items.len() {
                        out.finish();
                        return Ok(());
                    }
                    let end = if state.pos < state.fast {
                        (state.pos + BURST).min(state.fast).min(state.items.len())
                    } else {
                        std::thread::sleep(Duration::from_millis(state.stall));
                        (state.pos + state.slow).min(state.items.len())
                    };
                    let pos = state.pos;
                    let items: Vec<u32> = state.items[pos..end].to_vec();
                    state.buf.extend(items);
                    state.pos = end;
                    state.buf.pop_front().unwrap()
                }
            }
        };
        out.push(item);
        Ok(())
    }
}

#[derive(Clone)]
struct Job;

impl Job {
    fn work(seed: u32) -> u32 {
        let mut x = std::hint::black_box(seed);
        for _ in 0..WORK {
            x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        }
        std::hint::black_box(x)
    }
}

impl Apply for Job {
    type Input = u32;
    type Output = u32;
    type Provides = ();
    type Requires = ();

    fn apply<W: Set>(&mut self, input: u32, out: &mut Emit<u32, W>) -> Result<(), Error> {
        out.push(Self::work(input).wrapping_add(42));
        Ok(())
    }
}

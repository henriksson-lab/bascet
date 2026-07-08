use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bascet_core::{
    Apply, Async, Auto, Contract, Error, Executable, Pipeline, Pull, Runtime, Set, Sync,
};

const WORK: u32 = 1_000_000;
const BURST: usize = 2 << 16;

fn main() {
    let _ = tracing_subscriber::fmt::try_init();

    let start = Instant::now();
    Bursty::run();
    println!("bursty: {:?}", start.elapsed());
}

struct Bursty;

impl Bursty {
    fn run() {
        const FAST: usize = 100;
        const SLOW: usize = 10000;
        const STALL: u64 = 2000;
        const ITERS: usize = 5;
        const N: usize = FAST + SLOW * ITERS;

        let items: Vec<u32> = (0..N as u32).collect();
        let runner = Runtime::builder().build().pipeline::<()>(
            Pipeline::builder()
                .layer(Burst::new(items, FAST, SLOW, STALL))
                .layer(Job),
        );
        runner.join();
    }
}

struct Burst {
    state: Arc<Mutex<BurstState>>,
}

struct BurstState {
    items: Vec<u32>,
    pos: usize,
    fast: usize,
    slow: usize,
    stall: u64,
    buf: std::collections::VecDeque<u32>,
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
                buf: std::collections::VecDeque::new(),
            })),
        }
    }
}

impl Clone for Burst {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl Contract for Burst {
    type Input = ();
    type Output = u32;
    type Provides = ();
    type Requires = ();
    type Resources = ();
}

impl Apply for Burst {
    type Runtime = Async;
    type Coordinate = Auto;

    fn apply<'this, W: Set>(
        &'this mut self,
        _want: &Pull,
        _: (),
    ) -> <Self::Runtime as Executable>::Outcome<'this, Self::Output> {
        Box::pin(async move {
            let (item, stall) = {
                let mut state = self.state.lock().expect("burst source lock poisoned");
                if let Some(item) = state.buf.pop_front() {
                    return Ok(Some(item));
                }
                if state.pos >= state.items.len() {
                    return Ok(None);
                }
                if state.pos < state.fast {
                    let end = (state.pos + BURST).min(state.fast).min(state.items.len());
                    let pos = state.pos;
                    let items: Vec<_> = state.items[pos..end].to_vec();
                    state.buf.extend(items);
                    state.pos = end;
                    return Ok(Some(state.buf.pop_front().unwrap()));
                }

                let end = (state.pos + state.slow).min(state.items.len());
                let pos = state.pos;
                let items: Vec<_> = state.items[pos..end].to_vec();
                state.buf.extend(items);
                state.pos = end;
                (state.buf.pop_front().unwrap(), state.stall)
            };
            std::thread::sleep(Duration::from_millis(stall));
            Ok(Some(item))
        })
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

impl Contract for Job {
    type Input = u32;
    type Output = u32;
    type Provides = ();
    type Requires = ();
    type Resources = ();
}

impl Apply for Job {
    type Runtime = Sync;
    type Coordinate = Auto;

    fn apply<W: Set>(&mut self, _want: &Pull, input: u32) -> Result<Option<u32>, Error> {
        Ok(Some(Self::work(input).wrapping_add(42)))
    }
}

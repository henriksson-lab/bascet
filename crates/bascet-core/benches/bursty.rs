use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bascet_core::attr::Attr;
use bascet_core::attr::store::Store;
use bascet_core::pipeline::batch::Batch;
use bascet_core::{Apply, Error, Fields, Pipeline, Report, Runtime, sink};
use bascet_derive::attr_id;

const WORK: u32 = 1_000_000;
const CHUNK: usize = 256;
const FAST: usize = 100;
const SLOW: usize = 10000;
const STALL: u64 = 2000;
const ITERS: usize = 5;

struct Value;

impl Attr for Value {
    type Id = attr_id!(1);
}

struct Column(Vec<u32>);

impl Store for Column {
    type Key = Value;
    type Item<'a> = u32;
    fn get(&self, row: usize) -> u32 {
        self.0[row]
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}

fn main() {
    let _ = tracing_subscriber::fmt()
        .event_format(Report)
        .fmt_fields(Fields)
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    let items: Vec<u32> = (0..(FAST + SLOW * ITERS) as u32).collect();
    let start = Instant::now();
    let runner = Runtime::default().pipeline::<()>(
        Pipeline::builder()
            .source(Burst::new(items, FAST, SLOW, STALL))
            .layer(Job)
            .sink(sink::drain::<(Column, ())>()),
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
            })),
        }
    }
}

impl Apply<()> for Burst {
    type Produces = (Column, ());
    type Requires = ();

    fn apply_batch(&mut self, _: &Batch<()>) -> Result<Option<Self::Produces>, Error> {
        let mut state = self.state.lock().expect("burst source lock poisoned");
        if state.pos >= state.items.len() {
            return Ok(None);
        }
        if state.pos >= state.fast && (state.pos - state.fast) % state.slow == 0 {
            std::thread::sleep(Duration::from_millis(state.stall));
        }
        let region_end = if state.pos < state.fast {
            state.fast
        } else {
            let region = (state.pos - state.fast) / state.slow;
            state.fast + (region + 1) * state.slow
        };
        let pos = state.pos;
        let end = (pos + CHUNK).min(region_end).min(state.items.len());
        let burst: Vec<u32> = state.items[pos..end].to_vec();
        state.pos = end;
        Ok(Some((Column(burst), ())))
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

impl Apply<(Column, ())> for Job {
    type Produces = (Column, ());
    type Requires = Value;

    fn apply_batch(
        &mut self,
        batch: &Batch<(Column, ())>,
    ) -> Result<Option<Self::Produces>, Error> {
        let out: Vec<u32> = batch
            .store::<Value>()
            .0
            .iter()
            .map(|&seed| Self::work(seed).wrapping_add(42))
            .collect();
        Ok(Some((Column(out), ())))
    }
}

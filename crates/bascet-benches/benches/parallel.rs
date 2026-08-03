use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bascet_cli::logging;
use bascet_core::attr::Attr;
use bascet_core::attr::store::Store;
use bascet_core::pipeline::batch::Batch;
use bascet_core::{Apply, Error, Pipeline, Runtime, sink};
use bascet_derive::attr_id;
use tracing::Level;

const WORK: usize = 1_000;
const ITEMS: usize = 100_000_000;
const CHUNK: usize = 1024;
const SCRATCH: usize = 1 << 12;

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
    logging::init(Level::DEBUG);

    let elapsed = run(Runtime::default());
    println!(
        "parallel: {:?} ({:.0} items/s)",
        elapsed,
        ITEMS as f64 / elapsed.as_secs_f64()
    );
}

fn run(runtime: Runtime) -> Duration {
    let start = Instant::now();
    let runner = runtime.pipeline(
        Pipeline::builder()
            .source(Count {
                pos: Arc::new(AtomicUsize::new(0)),
            })
            .layer(Job::new())
            .sink(sink::drain::<(Column, ())>()),
    );
    runner.join().unwrap();
    start.elapsed()
}

#[derive(Clone)]
struct Count {
    pos: Arc<AtomicUsize>,
}

impl Apply<()> for Count {
    type Produces = (Column, ());
    type Requires = ();

    fn apply_batch(&mut self, _: &Batch<()>) -> Result<Option<Self::Produces>, Error> {
        let start = self.pos.fetch_add(CHUNK, Ordering::Relaxed);
        if start >= ITEMS {
            return Ok(None);
        }
        let end = (start + CHUNK).min(ITEMS);
        Ok(Some((Column((start..end).map(|i| i as u32).collect()), ())))
    }
}

#[derive(Clone)]
struct Job {
    chase: Vec<u32>,
}

impl Job {
    fn new() -> Self {
        let mut perm: Vec<u32> = (0..SCRATCH as u32).collect();
        let mut rng = 0x9E37_79B9u32;
        for i in (1..SCRATCH).rev() {
            rng = rng.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            perm.swap(i, rng as usize % (i + 1));
        }
        let mut chase = vec![0u32; SCRATCH];
        for k in 0..SCRATCH {
            chase[perm[k] as usize] = perm[(k + 1) % SCRATCH];
        }
        Self { chase }
    }

    fn work(&self, seed: u32) -> u32 {
        let mut idx = seed as usize & (self.chase.len() - 1);
        for _ in 0..WORK {
            idx = self.chase[idx] as usize;
        }
        std::hint::black_box(idx as u32)
    }
}

impl Apply<(Column, ())> for Job {
    type Produces = (Column, ());
    type Requires = (Value,);

    fn apply_batch(
        &mut self,
        batch: &Batch<(Column, ())>,
    ) -> Result<Option<Self::Produces>, Error> {
        let out: Vec<u32> = batch
            .store::<Value>()
            .0
            .iter()
            .map(|&seed| self.work(seed))
            .collect();
        Ok(Some((Column(out), ())))
    }
}

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bascet_core::set::Set;
use bascet_core::{Apply, Emit, Error, Pipeline, Runtime, sink};
use tracing::info;

const WORK: u32 = 100_000_000;
const ITEMS: usize = 100;
const THREADS: usize = 18;
const SCRATCH: usize = 1 << 28;

fn main() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    let burn = run(Runtime::builder().burn(THREADS).jobs(0).build());
    info!(
        "burn: {:?} ({:.0} items/s)",
        burn,
        ITEMS as f64 / burn.as_secs_f64()
    );
    let noburn = run(Runtime::builder().burn(0).jobs(THREADS).build());
    info!(
        "noburn: {:?} ({:.0} items/s)",
        noburn,
        ITEMS as f64 / noburn.as_secs_f64()
    );
    info!(
        "burn/noburn: {:.2}",
        burn.as_secs_f64() / noburn.as_secs_f64()
    )
}

fn run(runtime: Runtime) -> Duration {
    let start = Instant::now();
    let runner = runtime.pipeline::<()>(
        Pipeline::builder()
            .source(Count {
                pos: Arc::new(AtomicUsize::new(0)),
            })
            .layer(Job {
                scratch: vec![1; SCRATCH],
            })
            .sink(sink::drain::<u32>()),
    );
    runner.join().unwrap();
    start.elapsed()
}

#[derive(Clone)]
struct Count {
    pos: Arc<AtomicUsize>,
}

impl Apply for Count {
    type Input = ();
    type Output = u32;
    type Provides = ();
    type Requires = ();

    fn apply<W: Set>(&mut self, _: (), out: &mut Emit<u32, W>) -> Result<(), Error> {
        let pos = self.pos.fetch_add(1, Ordering::Relaxed);
        if pos < ITEMS {
            out.push(pos as u32);
        } else {
            out.finish();
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Job {
    scratch: Vec<u32>,
}

impl Job {
    fn work(&mut self, seed: u32) -> u32 {
        let mut x = std::hint::black_box(seed | 1);
        let mask = self.scratch.len() - 1;
        let mut acc = 0u32;
        for _ in 0..WORK {
            x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let i = (x as usize) & mask;
            acc = acc.wrapping_add(self.scratch[i]);
            self.scratch[i] = acc ^ x;
        }
        std::hint::black_box(acc)
    }
}

impl Apply for Job {
    type Input = u32;
    type Output = u32;
    type Provides = ();
    type Requires = ();

    fn apply<W: Set>(&mut self, input: u32, out: &mut Emit<u32, W>) -> Result<(), Error> {
        let value = self.work(input);
        out.push(value.wrapping_add(7));
        Ok(())
    }
}

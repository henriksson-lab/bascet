use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use bascet_core::{
    Apply, Async, Auto, Contract, Error, Executable, Pipeline, Pull, Runtime, Set, Sync,
};

const WORK: u32 = 1_000_000;
const ITEMS: usize = 100000;

fn main() {
    let _ = tracing_subscriber::fmt::try_init();

    let start = Instant::now();
    Parallel::run();
    println!("parallel: {:?}", start.elapsed());
}

struct Parallel;

impl Parallel {
    fn run() {
        let items: Arc<Vec<u32>> = Arc::new((0..ITEMS as u32).collect());
        let runner = Runtime::builder().build().pipeline::<()>(
            Pipeline::builder()
                .layer(Count {
                    items,
                    pos: Arc::new(AtomicUsize::new(0)),
                })
                .layer(Job),
        );
        runner.join();
    }
}

struct Count {
    items: Arc<Vec<u32>>,
    pos: Arc<AtomicUsize>,
}

impl Clone for Count {
    fn clone(&self) -> Self {
        Self {
            items: Arc::clone(&self.items),
            pos: Arc::clone(&self.pos),
        }
    }
}

impl Contract for Count {
    type Input = ();
    type Output = u32;
    type Provides = ();
    type Requires = ();
    type Resources = ();
}

impl Apply for Count {
    type Runtime = Async;
    type Coordinate = Auto;

    fn apply<'this, W: Set>(
        &'this mut self,
        _want: &Pull,
        _: (),
    ) -> <Self::Runtime as Executable>::Outcome<'this, Self::Output> {
        Box::pin(async move {
            let pos = self.pos.fetch_add(1, Ordering::Relaxed);
            if pos < self.items.len() {
                let item = self.items[pos];
                Ok(Some(item))
            } else {
                Ok(None)
            }
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
        Ok(Some(Self::work(input).wrapping_add(100)))
    }
}

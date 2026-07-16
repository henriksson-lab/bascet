use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use bascet_core::set::Set;
use bascet_core::{Apply, Emit, Error, Pipeline, Runtime, sink};

#[derive(Clone)]
struct Count {
    limit: u32,
    at: Arc<AtomicU32>,
}

impl Count {
    fn upto(limit: u32) -> Self {
        Self {
            limit,
            at: Arc::new(AtomicU32::new(0)),
        }
    }
}

impl Apply for Count {
    type Input = ();
    type Output = u32;
    type Provides = ();
    type Requires = ();

    fn apply<W: Set>(&mut self, _: (), out: &mut Emit<u32, W>) -> Result<(), Error> {
        let n = self.at.fetch_add(1, Ordering::Relaxed);
        if n >= self.limit {
            out.finish();
        } else {
            out.push(n);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Double;

impl Apply for Double {
    type Input = u32;
    type Output = u32;
    type Provides = ();
    type Requires = ();

    fn apply<W: Set>(&mut self, input: u32, out: &mut Emit<u32, W>) -> Result<(), Error> {
        out.push(input * 2);
        Ok(())
    }
}

struct Slow {
    clones: Arc<AtomicU32>,
}

impl Clone for Slow {
    fn clone(&self) -> Self {
        self.clones.fetch_add(1, Ordering::Relaxed);
        Self {
            clones: Arc::clone(&self.clones),
        }
    }
}

impl Apply for Slow {
    type Input = u32;
    type Output = u32;
    type Provides = ();
    type Requires = ();

    fn apply<W: Set>(&mut self, input: u32, out: &mut Emit<u32, W>) -> Result<(), Error> {
        std::thread::sleep(std::time::Duration::from_micros(200));
        out.push(input);
        Ok(())
    }
}

#[derive(Clone)]
struct Explode;

impl Apply for Explode {
    type Input = u32;
    type Output = ();
    type Provides = ();
    type Requires = ();

    fn apply<W: Set>(&mut self, _input: u32, _out: &mut Emit<(), W>) -> Result<(), Error> {
        Err(())
    }
}

#[derive(Clone)]
struct FanOut {
    per_item: u32,
}

impl Apply for FanOut {
    type Input = u32;
    type Output = u32;
    type Provides = ();
    type Requires = ();

    fn apply<W: Set>(&mut self, input: u32, out: &mut Emit<u32, W>) -> Result<(), Error> {
        for offset in 0..self.per_item {
            out.push(input.wrapping_add(offset));
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
struct Total {
    sum: u64,
}

impl Apply for Total {
    type Input = u32;
    type Output = u64;
    type Provides = ();
    type Requires = ();

    fn apply<W: Set>(&mut self, input: u32, _out: &mut Emit<u64, W>) -> Result<(), Error> {
        self.sum += u64::from(input);
        Ok(())
    }

    fn finish<W: Set>(&mut self, out: &mut Emit<u64, W>) -> Result<(), Error> {
        out.push(self.sum);
        Ok(())
    }
}

fn collect<T: Send + 'static>(out_rx: &kanal::Receiver<T>) -> Vec<T> {
    let mut collected = Vec::new();
    while let Ok(Some(value)) = out_rx.try_recv() {
        collected.push(value);
    }
    collected
}

#[test]
fn linear_pipeline_runs_to_completion() {
    let runtime = Runtime::builder().burn(0).jobs(4).tasks(0).build();
    let (write, out_rx) = sink::channel::<u32>();

    let runner = runtime.pipeline::<()>(
        Pipeline::builder()
            .source(Count::upto(1000))
            .layer(Double)
            .sink(write),
    );
    assert!(runner.join().is_ok());

    let mut collected = collect(&out_rx);
    collected.sort_unstable();
    assert_eq!(collected, (0..1000).map(|n| n * 2).collect::<Vec<_>>());
}

#[test]
fn slow_layer_scales_to_multiple_workers() {
    let runtime = Runtime::builder().burn(0).jobs(4).tasks(0).build();
    let (write, out_rx) = sink::channel::<u32>();
    let clones = Arc::new(AtomicU32::new(0));
    let slow = Slow {
        clones: Arc::clone(&clones),
    };

    let runner = runtime.pipeline::<()>(
        Pipeline::builder()
            .source(Count::upto(500))
            .layer(slow)
            .sink(write),
    );
    assert!(runner.join().is_ok());

    let mut collected = collect(&out_rx);
    collected.sort_unstable();
    assert_eq!(collected, (0..500).collect::<Vec<_>>());
    assert!(
        clones.load(Ordering::Relaxed) > 1,
        "slow layer never scaled"
    );
}

#[test]
fn failing_sink_errors_join_without_hanging() {
    let runtime = Runtime::builder().burn(0).jobs(4).tasks(0).build();

    let runner = runtime.pipeline::<()>(
        Pipeline::builder()
            .source(Count::upto(1000))
            .layer(Double)
            .sink(Explode),
    );
    assert!(runner.join().is_err());
}

#[test]
fn single_thread_pool_drives_three_layers() {
    let runtime = Runtime::builder().burn(0).jobs(1).tasks(0).build();
    let (write, out_rx) = sink::channel::<u32>();

    let runner = runtime.pipeline::<()>(
        Pipeline::builder()
            .source(Count::upto(10_000))
            .layer(Double)
            .sink(write),
    );
    assert!(runner.join().is_ok());

    assert_eq!(collect(&out_rx).len(), 10_000);
}

#[test]
fn flat_map_overshoot_survives() {
    let runtime = Runtime::builder().burn(0).jobs(2).tasks(0).build();

    let runner = runtime.pipeline::<()>(
        Pipeline::builder()
            .source(Count::upto(64))
            .layer(FanOut { per_item: 5000 })
            .sink(sink::drain::<u32>()),
    );
    assert!(runner.join().is_ok());
}

#[test]
fn finalize_emits_the_accumulated_result() {
    let runtime = Runtime::builder().burn(0).jobs(1).tasks(0).build();
    let (write, out_rx) = sink::channel::<u64>();

    let runner = runtime.pipeline::<()>(
        Pipeline::builder()
            .source(Count::upto(1000))
            .layer(Total::default())
            .sink(write),
    );
    assert!(runner.join().is_ok());

    let total: u64 = collect(&out_rx).into_iter().sum();
    assert_eq!(total, (0..1000u64).sum::<u64>());
}

#[test]
fn double_eof_retires_once_and_join_returns() {
    let runtime = Runtime::builder().burn(0).jobs(4).tasks(0).build();

    let runner = runtime.pipeline::<()>(
        Pipeline::builder()
            .source(Count::upto(100_000))
            .layer(Double)
            .sink(sink::drain::<u32>()),
    );
    assert!(runner.join().is_ok());
}

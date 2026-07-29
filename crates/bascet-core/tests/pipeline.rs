use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use bascet_core::attr::Attr;
use bascet_core::attr::store::Store;
use bascet_core::pipeline::batch::Batch;
use bascet_core::{Apply, Error, Pipeline, Runtime, sink};
use bascet_derive::attr_id;

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

impl Apply<()> for Count {
    type Produces = (Column, ());
    type Requires = ();

    fn apply_batch(&mut self, _: &Batch<()>) -> Result<Option<Self::Produces>, Error> {
        let n = self.at.fetch_add(1, Ordering::Relaxed);
        if n >= self.limit {
            Ok(None)
        } else {
            Ok(Some((Column(vec![n]), ())))
        }
    }
}

#[derive(Clone)]
struct Double;

impl Apply<(Column, ())> for Double {
    type Produces = (Column, ());
    type Requires = Value;

    fn apply_batch(&mut self, batch: &Batch<(Column, ())>) -> Result<Option<Self::Produces>, Error> {
        let out: Vec<u32> = batch.store::<Value>().0.iter().map(|&n| n * 2).collect();
        Ok(Some((Column(out), ())))
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

impl Apply<(Column, ())> for Slow {
    type Produces = (Column, ());
    type Requires = Value;

    fn apply_batch(&mut self, batch: &Batch<(Column, ())>) -> Result<Option<Self::Produces>, Error> {
        std::thread::sleep(std::time::Duration::from_micros(200));
        let out: Vec<u32> = batch.store::<Value>().0.iter().copied().collect();
        Ok(Some((Column(out), ())))
    }
}

#[derive(Clone)]
struct Explode;

impl Apply<(Column, ())> for Explode {
    type Produces = ();
    type Requires = ();

    fn apply_batch(&mut self, _: &Batch<(Column, ())>) -> Result<Option<()>, Error> {
        Err(Error::Layer("explode".into()))
    }
}

#[derive(Clone)]
struct Collect {
    seen: Arc<Mutex<Vec<u32>>>,
}

impl Apply<(Column, ())> for Collect {
    type Produces = ();
    type Requires = Value;

    fn apply_batch(&mut self, batch: &Batch<(Column, ())>) -> Result<Option<()>, Error> {
        self.seen
            .lock()
            .unwrap()
            .extend(batch.store::<Value>().0.iter().copied());
        Ok(Some(()))
    }
}

#[test]
fn linear_pipeline_runs_to_completion() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let runtime = Runtime::builder().with_burn(0).with_jobs(4).with_tasks(0).build();

    let runner = runtime.pipeline::<()>(
        Pipeline::builder()
            .source(Count::upto(1000))
            .layer(Double)
            .sink(Collect {
                seen: Arc::clone(&seen),
            }),
    );
    assert!(runner.join().is_ok());

    let mut collected = seen.lock().unwrap().clone();
    collected.sort_unstable();
    assert_eq!(collected, (0..1000).map(|n| n * 2).collect::<Vec<_>>());
}

#[test]
fn slow_layer_scales_to_multiple_workers() {
    let clones = Arc::new(AtomicU32::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let runtime = Runtime::builder().with_burn(0).with_jobs(4).with_tasks(0).build();

    let runner = runtime.pipeline::<()>(
        Pipeline::builder()
            .source(Count::upto(500))
            .layer(Slow {
                clones: Arc::clone(&clones),
            })
            .sink(Collect {
                seen: Arc::clone(&seen),
            }),
    );
    assert!(runner.join().is_ok());

    let mut collected = seen.lock().unwrap().clone();
    collected.sort_unstable();
    assert_eq!(collected, (0..500).collect::<Vec<_>>());
    assert!(clones.load(Ordering::Relaxed) > 1, "slow layer never scaled");
}

#[test]
fn failing_sink_errors_join_without_hanging() {
    let runtime = Runtime::builder().with_burn(0).with_jobs(4).with_tasks(0).build();

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
    let seen = Arc::new(Mutex::new(Vec::new()));
    let runtime = Runtime::builder().with_burn(0).with_jobs(1).with_tasks(0).build();

    let runner = runtime.pipeline::<()>(
        Pipeline::builder()
            .source(Count::upto(10_000))
            .layer(Double)
            .sink(Collect {
                seen: Arc::clone(&seen),
            }),
    );
    assert!(runner.join().is_ok());

    assert_eq!(seen.lock().unwrap().len(), 10_000);
}

#[test]
fn double_eof_retires_once_and_join_returns() {
    let runtime = Runtime::builder().with_burn(0).with_jobs(4).with_tasks(0).build();

    let runner = runtime.pipeline::<()>(
        Pipeline::builder()
            .source(Count::upto(100_000))
            .layer(Double)
            .sink(sink::drain::<(Column, ())>()),
    );
    assert!(runner.join().is_ok());
}

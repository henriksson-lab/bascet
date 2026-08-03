use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
struct Burst {
    limit: u32,
    at: Arc<AtomicU32>,
}

impl Burst {
    fn upto(limit: u32) -> Self {
        Self {
            limit,
            at: Arc::new(AtomicU32::new(0)),
        }
    }
}

impl Apply<()> for Burst {
    type Produces = (Column, ());
    type Requires = ();

    fn apply_batch(&mut self, _: &Batch<()>) -> Result<Option<Self::Produces>, Error> {
        let n = self.at.fetch_add(1, Ordering::Relaxed);
        if n >= self.limit {
            return Ok(None);
        }
        if n % 64 == 0 {
            std::thread::sleep(Duration::from_micros(50));
        }
        Ok(Some((Column(vec![n]), ())))
    }
}

#[derive(Clone)]
struct Double;

impl Apply<(Column, ())> for Double {
    type Produces = (Column, ());
    type Requires = Value;

    fn apply_batch(
        &mut self,
        batch: &Batch<(Column, ())>,
    ) -> Result<Option<Self::Produces>, Error> {
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

    fn apply_batch(
        &mut self,
        batch: &Batch<(Column, ())>,
    ) -> Result<Option<Self::Produces>, Error> {
        std::thread::sleep(Duration::from_micros(200));
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

fn jobs(n: u64) -> Runtime {
    Runtime::builder()
        .with_burn(0)
        .with_jobs(n)
        .with_tasks(0)
        .build()
        .unwrap()
}

fn sorted(seen: &Arc<Mutex<Vec<u32>>>) -> Vec<u32> {
    let mut out = seen.lock().unwrap().clone();
    out.sort_unstable();
    out
}

#[test]
fn runs_all_items() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let runner = jobs(4).pipeline(
        Pipeline::builder()
            .source(Count::upto(1000))
            .layer(Double)
            .sink(Collect {
                seen: Arc::clone(&seen),
            }),
    );
    assert!(runner.join().is_ok());
    assert_eq!(sorted(&seen), (0..1000).map(|n| n * 2).collect::<Vec<_>>());
}

#[test]
fn surfaces_errors() {
    let runner = jobs(4).pipeline(
        Pipeline::builder()
            .source(Count::upto(1000))
            .layer(Double)
            .sink(Explode),
    );
    assert!(runner.join().is_err());
}

#[test]
fn scales_slow_layers() {
    let clones = Arc::new(AtomicU32::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let runner = jobs(4).pipeline(
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
    assert_eq!(sorted(&seen), (0..500).collect::<Vec<_>>());
    assert!(
        clones.load(Ordering::Relaxed) > 1,
        "slow layer never scaled"
    );
}

#[test]
fn retires_once_on_repeat_eof() {
    let runner = Runtime::default().pipeline(
        Pipeline::builder()
            .source(Count::upto(100_000))
            .layer(Double)
            .sink(sink::drain::<(Column, ())>()),
    );
    assert!(runner.join().is_ok());
}

#[test]
fn holds_under_starvation() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let runner = jobs(8).pipeline(
        Pipeline::builder()
            .source(Burst::upto(20_000))
            .layer(Double)
            .sink(Collect {
                seen: Arc::clone(&seen),
            }),
    );
    assert!(runner.join().is_ok());
    assert_eq!(
        sorted(&seen),
        (0..20_000).map(|n| n * 2).collect::<Vec<_>>()
    );
}

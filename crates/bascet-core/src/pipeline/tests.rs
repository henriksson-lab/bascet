use std::future::Future;
use std::thread::available_parallelism;
use std::time::Duration;

use futures::FutureExt;
use tracing::info;

use crate::layer::Layer;
use crate::set::Set;
use crate::source::{Pull, Source};
use crate::stage::{Emit, Output, Stage};

use super::pipeline::Runner;

fn do_work(seed: u32, iters: u32) -> u32 {
    let mut x = std::hint::black_box(seed);
    for _ in 0..iters {
        x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
    }
    std::hint::black_box(x)
}

const WORK_ITERS: u32 = 1_000_000;
const BENCH_ITEMS: usize = 100000;

fn init_tracing() {
    let _ = tracing_subscriber::fmt::try_init();
}

#[derive(Clone, bascet_derive::Scheduling)]
struct CountSource {
    items: Vec<u32>,
    pos: usize,
}

impl Layer for CountSource {
    type Provides = ();
    type Requires = ();
    type Resources = ();
}

impl Source for CountSource {
    type Output = u32;
    fn produce<W: Set>(&mut self, _req: Pull) -> impl Future<Output = Output<u32>> + Send {
        let result = if self.pos < self.items.len() {
            let v = self.items[self.pos];
            self.pos += 1;
            Output::Value(v)
        } else {
            Output::Shutdown
        };
        std::future::ready(result)
    }
}

#[derive(bascet_derive::Scheduling, Clone)]
struct StageA;

impl Layer for StageA {
    type Provides = ();
    type Requires = ();
    type Resources = ();
}

impl Stage for StageA {
    type Input<'a> = &'a u32;
    type Output = u32;
    fn apply<W: Set>(&mut self, input: &u32) -> Emit<u32> {
        Emit::Value(do_work(*input, WORK_ITERS).wrapping_mul(3).wrapping_add(7))
    }
}

#[derive(bascet_derive::Scheduling, Clone)]
struct StageB;

impl Layer for StageB {
    type Provides = ();
    type Requires = ();
    type Resources = ();
}

impl Stage for StageB {
    type Input<'a> = &'a u32;
    type Output = u32;
    fn apply<W: Set>(&mut self, input: &u32) -> Emit<u32> {
        Emit::Value(do_work(*input, WORK_ITERS).wrapping_add(100))
    }
}

#[derive(bascet_derive::Scheduling, Clone)]
struct StageC;

impl Layer for StageC {
    type Provides = ();
    type Requires = ();
    type Resources = ();
}

impl Stage for StageC {
    type Input<'a> = &'a u32;
    type Output = u32;
    fn apply<W: Set>(&mut self, input: &u32) -> Emit<u32> {
        Emit::Value(do_work(*input, WORK_ITERS).wrapping_mul(2).wrapping_add(1))
    }
}

#[test]
fn batch_pipeline_speedup() {
    init_tracing();
    let items: Vec<u32> = (0..BENCH_ITEMS as u32).collect();
    let runner = Runner::builder()
        .source(CountSource { items, pos: 0 })
        .stage(StageA)
        .stage(StageB)
        .stage(StageC)
        .build::<()>();
    runner.join();
}

#[derive(Clone, bascet_derive::Scheduling)]
#[scheduling(Auto)]
struct SatTask;

impl Layer for SatTask {
    type Provides = ();
    type Requires = ();
    type Resources = ();
}

impl Stage for SatTask {
    type Input<'a> = &'a u32;
    type Output = u32;
    fn apply<W: Set>(&mut self, input: &u32) -> Emit<u32> {
        Emit::Value(do_work(*input, WORK_ITERS).wrapping_add(100))
    }
}

#[test]
fn parallel_task_speedup() {
    init_tracing();
    let items: Vec<u32> = (0..BENCH_ITEMS as u32).collect();
    let runner = Runner::builder()
        .source(CountSource { items, pos: 0 })
        .stage(SatTask)
        .build::<()>();
    runner.join();
}

const BURST_BATCH: usize = 2 << 16;

// Uses Task strategy so produce can async-sleep during the stall period.
// Maintains an internal buffer: when a burst fires, BURST_BATCH items are loaded
// and returned individually (no sleep) on consecutive produce calls, flooding the
// downstream channel with individual u32 work items to trigger Job worker promotion.
#[derive(Clone, bascet_derive::Scheduling)]
struct BurstThenStallSource {
    items: Vec<u32>,
    pos: usize,
    fast_count: usize,
    slow_batch: usize,
    stall_ms: u64,
    buf: std::collections::VecDeque<u32>,
}

impl BurstThenStallSource {
    fn new(items: Vec<u32>, fast_count: usize, slow_batch: usize, stall_ms: u64) -> Self {
        Self {
            items,
            pos: 0,
            fast_count,
            slow_batch,
            stall_ms,
            buf: std::collections::VecDeque::new(),
        }
    }
}

impl Layer for BurstThenStallSource {
    type Provides = ();
    type Requires = ();
    type Resources = ();
}

impl Source for BurstThenStallSource {
    type Output = u32;
    fn produce<W: Set>(&mut self, _req: Pull) -> impl Future<Output = Output<u32>> + Send {
        if let Some(v) = self.buf.pop_front() {
            return async move { Output::Value(v) }.boxed();
        }
        if self.pos >= self.items.len() {
            return async { Output::Shutdown }.boxed();
        }
        if self.pos < self.fast_count {
            let end = (self.pos + BURST_BATCH)
                .min(self.fast_count)
                .min(self.items.len());
            self.buf.extend(self.items[self.pos..end].iter().copied());
            self.pos = end;
            let v = self.buf.pop_front().unwrap();
            return async move { Output::Value(v) }.boxed();
        }
        let end = (self.pos + self.slow_batch).min(self.items.len());
        self.buf.extend(self.items[self.pos..end].iter().copied());
        self.pos = end;
        let v = self.buf.pop_front().unwrap();
        let stall = Duration::from_millis(self.stall_ms);
        async move {
            tokio::time::sleep(stall).await;
            Output::Value(v)
        }
        .boxed()
    }
}

#[derive(bascet_derive::Scheduling, Clone)]
struct LightJob;

impl Layer for LightJob {
    type Provides = ();
    type Requires = ();
    type Resources = ();
}

impl Stage for LightJob {
    type Input<'a> = &'a u32;
    type Output = u32;
    fn apply<W: Set>(&mut self, input: &u32) -> Emit<u32> {
        Emit::Value(do_work(*input, WORK_ITERS).wrapping_add(42))
    }
}

#[test]
fn stall_recovery_correctness() {
    init_tracing();
    info!("{:?}", available_parallelism());

    const FAST: usize = 100;
    const SLOW: usize = 10000;
    const STALL: u64 = 2000;
    const ITERS: usize = 5;
    const N: usize = FAST + SLOW * ITERS;

    let items: Vec<u32> = (0..N as u32).collect();
    let runner = Runner::builder()
        .source(BurstThenStallSource::new(items, FAST, SLOW, STALL))
        .stage(LightJob {})
        .build::<()>();
    runner.join();
}

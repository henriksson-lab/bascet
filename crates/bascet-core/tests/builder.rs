use std::any::TypeId;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bascet_core::Attr;
use bascet_core::apply::execute::Synchronous;
use bascet_core::pipeline::Wanted;
use bascet_core::set::{Intersect, Set, Union};
use bascet_core::{Apply, ApplyAsync, Emit, Error, Pipeline, Runtime, sink};
use bascet_derive::attr_id;

struct Header;
struct Blocks;

impl Attr for Header {
    type Id = attr_id!(1);
}

impl Attr for Blocks {
    type Id = attr_id!(2);
}

#[derive(Clone)]
struct Numbers;

impl Apply for Numbers {
    type Input = ();
    type Output = u32;
    type Provides = (Header,);
    type Requires = ();

    fn apply<W: Set>(&mut self, _: (), out: &mut Emit<u32, W>) -> Result<(), Error> {
        out.finish();
        Ok(())
    }
}

#[derive(Clone)]
struct Double;

impl Apply for Double {
    type Input = u32;
    type Output = u32;
    type Provides = (Header,);
    type Requires = (Header,);

    fn apply<W: Set>(&mut self, input: u32, out: &mut Emit<u32, W>) -> Result<(), Error> {
        out.push(input * 2);
        Ok(())
    }
}

#[derive(Clone)]
struct Slow;

impl ApplyAsync for Slow {
    type Input = u32;
    type Output = u32;
    type Provides = (Header,);
    type Requires = ();

    async fn apply<W: Set>(&mut self, input: u32, out: &mut Emit<u32, W>) -> Result<(), Error> {
        out.push(input);
        Ok(())
    }
}

#[derive(Clone)]
struct Consume;

impl Apply for Consume {
    type Input = u32;
    type Output = ();
    type Provides = ();
    type Requires = (Header,);

    fn apply<W: Set>(&mut self, _: u32, _: &mut Emit<(), W>) -> Result<(), Error> {
        Ok(())
    }
}

#[derive(Clone)]
struct Wants {
    header: Arc<AtomicBool>,
}

impl Apply for Wants {
    type Input = u32;
    type Output = u32;
    type Provides = (Header,);
    type Requires = ();

    fn apply<W: Set>(&mut self, input: u32, out: &mut Emit<u32, W>) -> Result<(), Error> {
        out.push(input);
        Ok(())
    }

    fn finish<W: Set>(&mut self, out: &mut Emit<u32, W>) -> Result<(), Error> {
        self.header.store(out.wants::<Header>(), Ordering::Relaxed);
        Ok(())
    }
}

fn eq<T: 'static, U: 'static>() -> bool {
    TypeId::of::<T>() == TypeId::of::<U>()
}

#[test]
fn chain_builds_with_inferred_markers() {
    let _ = Pipeline::builder()
        .source(Numbers)
        .layer(Double)
        .layer(Slow)
        .sink(Consume);
}

#[test]
fn wants_algebra_normalizes() {
    assert!(eq::<
        Union<(Header,), Intersect<(Blocks,), (Blocks, Header)>>,
        (Header, Blocks),
    >());
}

#[test]
fn wanted_accumulates_requires_over_wants() {
    assert!(eq::<Wanted<Consume, Synchronous, ()>, (Header,)>());
    assert!(eq::<Wanted<Double, Synchronous, (Blocks,)>, (Header, Blocks)>());
}

#[test]
fn sink_requires_reach_the_producer_emit() {
    let seen = Arc::new(AtomicBool::new(false));
    let runtime = Runtime::builder().burn(0).jobs(1).tasks(0).build();
    let runner = runtime.pipeline::<()>(
        Pipeline::builder()
            .source(Numbers)
            .layer(Wants {
                header: Arc::clone(&seen),
            })
            .sink(Consume),
    );
    assert!(runner.join().is_ok());
    assert!(seen.load(Ordering::Relaxed));
}

#[test]
fn unwanted_attrs_stay_unwanted() {
    let seen = Arc::new(AtomicBool::new(true));
    let runtime = Runtime::builder().burn(0).jobs(1).tasks(0).build();
    let runner = runtime.pipeline::<()>(
        Pipeline::builder()
            .source(Numbers)
            .layer(Wants {
                header: Arc::clone(&seen),
            })
            .sink(sink::drain::<u32>()),
    );
    assert!(runner.join().is_ok());
    assert!(!seen.load(Ordering::Relaxed));
}

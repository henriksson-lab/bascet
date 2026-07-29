use std::any::TypeId;

use bascet_core::attr::Attr;
use bascet_core::attr::store::Store;
use bascet_core::pipeline::Wanted;
use bascet_core::pipeline::batch::Batch;
use bascet_core::set::{Intersect, Lower, Union};
use bascet_core::{Apply, Error, Pipeline, Runtime};
use bascet_derive::attr_id;

struct Header;
struct Blocks;

impl Attr for Header {
    type Id = attr_id!(1);
}

impl Attr for Blocks {
    type Id = attr_id!(2);
}

struct Head(Vec<u32>);

impl Store for Head {
    type Key = Header;
    type Item<'a> = u32;
    fn get(&self, row: usize) -> u32 {
        self.0[row]
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}

#[derive(Clone)]
struct Numbers;

impl Apply<()> for Numbers {
    type Produces = (Head, ());
    type Requires = ();

    fn apply_batch(&mut self, _: &Batch<()>) -> Result<Option<Self::Produces>, Error> {
        Ok(None)
    }
}

#[derive(Clone)]
struct Double;

impl Apply<(Head, ())> for Double {
    type Produces = (Head, ());
    type Requires = Header;

    fn apply_batch(&mut self, batch: &Batch<(Head, ())>) -> Result<Option<Self::Produces>, Error> {
        let out: Vec<u32> = batch.store::<Header>().0.iter().map(|&n| n * 2).collect();
        Ok(Some((Head(out), ())))
    }
}

#[derive(Clone)]
struct Consume;

impl Apply<(Head, ())> for Consume {
    type Produces = ();
    type Requires = Header;

    fn apply_batch(&mut self, _: &Batch<(Head, ())>) -> Result<Option<()>, Error> {
        Ok(Some(()))
    }
}

fn eq<T: 'static, U: 'static>() -> bool {
    TypeId::of::<T>() == TypeId::of::<U>()
}

type L<S> = <S as Lower>::Out;

#[test]
fn chain_builds_and_runs() {
    let runtime = Runtime::builder().with_burn(0).with_jobs(1).with_tasks(0).build();
    let runner = runtime.pipeline::<()>(
        Pipeline::builder()
            .source(Numbers)
            .layer(Double)
            .sink(Consume),
    );
    assert!(runner.join().is_ok());
}

#[test]
fn wants_algebra_normalizes() {
    assert!(eq::<
        <L<Header> as Union<<L<Blocks> as Intersect<L<(Blocks, Header)>>>::Output>>::Output,
        L<(Header, Blocks)>,
    >());
}

#[test]
fn wanted_accumulates_requires_over_wants() {
    assert!(eq::<Wanted<Consume, (Head, ()), ()>, L<Header>>());
    assert!(eq::<Wanted<Double, (Head, ()), L<Blocks>>, L<(Header, Blocks)>>());
}

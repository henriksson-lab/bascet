use std::marker::PhantomData;

use bascet_core::Record;
use bascet_core::attr::Attr;
use bascet_core::attr::store::Store;
use bascet_core::pipeline::batch::Batch;
use bascet_core::set::ops::partition::Partition;
use bascet_core::set::Chain;
use bascet_derive::attr_id;

struct A;
struct B;
struct C;

impl Attr for A {
    type Id = attr_id!(1);
}
impl Attr for B {
    type Id = attr_id!(2);
}
impl Attr for C {
    type Id = attr_id!(3);
}

struct Bag<K> {
    rows: Vec<Vec<u8>>,
    key: PhantomData<fn() -> K>,
}

impl<K: Attr> Store for Bag<K> {
    type Key = K;
    type Item<'a> = &'a [u8];
    fn get(&self, row: usize) -> &[u8] {
        self.rows[row].as_slice()
    }
    fn len(&self) -> usize {
        self.rows.len()
    }
}

fn bag<K>(value: &[u8]) -> Bag<K> {
    Bag {
        rows: vec![value.to_vec()],
        key: PhantomData,
    }
}

fn eq<T: 'static, U: 'static>() -> bool {
    std::any::TypeId::of::<T>() == std::any::TypeId::of::<U>()
}

#[test]
fn forward_moves_wanted_drops_unwanted() {
    let input: (Bag<A>, (Bag<B>, ())) = (bag(b"a"), (bag(b"b"), ()));
    let out: (Bag<A>, ()) =
        <(Bag<A>, (Bag<B>, ())) as Partition<(), (A, ())>>::partition(input);
    let batch = Batch::new(out);
    let row = batch.iter().next().unwrap();
    assert_eq!(row.get::<A>(), &b"a"[..]);
}

#[test]
fn override_replaces_input_with_produced() {
    let input: (Bag<A>, (Bag<B>, ())) = (bag(b"old"), (bag(b"b"), ()));
    let produced: (Bag<A>, ()) = (bag(b"new"), ());
    let forwarded =
        <(Bag<A>, (Bag<B>, ())) as Partition<(A, ()), (A, (B, ()))>>::partition(input);
    let out: (Bag<B>, (Bag<A>, ())) = forwarded.chain(produced);
    let batch = Batch::new(out);
    let row = batch.iter().next().unwrap();
    assert_eq!(row.get::<A>(), &b"new"[..]);
    assert_eq!(row.get::<B>(), &b"b"[..]);
}

#[test]
fn narrowing_drops_middle_store() {
    let input: (Bag<A>, (Bag<B>, (Bag<C>, ()))) = (bag(b"a"), (bag(b"b"), (bag(b"c"), ())));
    let out: (Bag<A>, (Bag<C>, ())) =
        <(Bag<A>, (Bag<B>, (Bag<C>, ()))) as Partition<(), (A, (C, ()))>>::partition(input);
    let batch = Batch::new(out);
    let row = batch.iter().next().unwrap();
    assert_eq!(row.get::<A>(), &b"a"[..]);
    assert_eq!(row.get::<C>(), &b"c"[..]);
}

#[test]
fn output_attrs_are_forward_then_produced() {
    assert!(eq::<<Batch<(Bag<B>, (Bag<A>, ()))> as Record>::Attrs, (B, (A, ()))>());
}

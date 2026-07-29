use std::marker::PhantomData;

use bascet_core::Record;
use bascet_core::attr::Attr;
use bascet_core::attr::store::Store;
use bascet_core::pipeline::batch::Batch;
use bascet_derive::attr_id;

struct Sequence;
struct Quality;
struct Count;

impl Attr for Sequence {
    type Id = attr_id!(1);
}
impl Attr for Quality {
    type Id = attr_id!(2);
}
impl Attr for Count {
    type Id = attr_id!(3);
}

struct Bag<K> {
    rows: Vec<Vec<u8>>,
    key: PhantomData<fn() -> K>,
}

impl<K> Bag<K> {
    fn new(rows: Vec<Vec<u8>>) -> Self {
        Self {
            rows,
            key: PhantomData,
        }
    }
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

fn eq<T: 'static, U: 'static>() -> bool {
    std::any::TypeId::of::<T>() == std::any::TypeId::of::<U>()
}

#[test]
fn resolves_stores_by_key() {
    let batch = Batch::new((
        Bag::<Sequence>::new(vec![b"ACGT".to_vec(), b"TT".to_vec()]),
        (
            Bag::<Quality>::new(vec![vec![40, 40, 10, 10], vec![35, 35]]),
            (),
        ),
    ));
    let mut rows = batch.iter();
    let r0 = rows.next().unwrap();
    assert_eq!(r0.get::<Sequence>(), &b"ACGT"[..]);
    assert_eq!(r0.get::<Quality>(), &[40u8, 40, 10, 10][..]);
    let r1 = rows.next().unwrap();
    assert_eq!(r1.get::<Sequence>(), &b"TT"[..]);
    assert_eq!(r1.get::<Quality>(), &[35u8, 35][..]);
}

#[test]
fn resolves_deeper_store() {
    let batch = Batch::new((
        Bag::<Sequence>::new(vec![b"A".to_vec()]),
        (
            Bag::<Quality>::new(vec![b"B".to_vec()]),
            (Bag::<Count>::new(vec![b"5".to_vec()]), ()),
        ),
    ));
    let r = batch.iter().next().unwrap();
    assert_eq!(r.get::<Sequence>(), &b"A"[..]);
    assert_eq!(r.get::<Count>(), &b"5"[..]);
}

#[test]
fn attrs_projects_keys_to_flat_set() {
    assert!(eq::<
        <Batch<(Bag<Sequence>, (Bag<Quality>, ()))> as Record>::Attrs,
        (Sequence, (Quality, ())),
    >());
}

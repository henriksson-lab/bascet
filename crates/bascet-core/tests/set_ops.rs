use std::any::TypeId;

use bascet_core::attr::block::{Header, Offset, Trailer};
use bascet_core::set::{Hit, In, Intersect, Miss, Set, Subset, Union};

fn eq<T: 'static, U: 'static>() -> bool {
    TypeId::of::<T>() == TypeId::of::<U>()
}

#[test]
fn membership_folds_by_id() {
    assert!(eq::<<Header<1> as In<(Header<1>, Offset<1>)>>::Verdict, Hit>());
    assert!(eq::<<Trailer<1> as In<(Header<1>, Offset<1>)>>::Verdict, Miss>());
    assert!(eq::<<Header<1> as In<()>>::Verdict, Miss>());
}

#[test]
fn contains_answers_by_id() {
    assert!(<(Header<1>, Offset<1>) as Set>::contains::<Header<1>>());
    assert!(!<(Header<1>, Offset<1>) as Set>::contains::<Trailer<1>>());
    assert!(!<() as Set>::contains::<Header<1>>());
}

fn requires_set<S: Set>() {}

#[test]
fn distinct_tuple_is_a_set() {
    requires_set::<()>();
    requires_set::<(Header<1>,)>();
    requires_set::<(Header<1>, Offset<1>, Trailer<1>)>();
}

#[test]
fn union_dedups_preserving_order() {
    assert!(eq::<
        Union<(Header<1>, Offset<1>), (Offset<1>, Trailer<1>)>,
        (Header<1>, Offset<1>, Trailer<1>),
    >());
    assert!(eq::<Union<(), (Header<1>,)>, (Header<1>,)>());
    assert!(eq::<Union<(Header<1>,), ()>, (Header<1>,)>());
}

#[test]
fn intersect_keeps_overlap() {
    assert!(eq::<
        Intersect<(Header<1>, Offset<1>, Trailer<1>), (Trailer<1>, Header<1>)>,
        (Header<1>, Trailer<1>),
    >());
    assert!(eq::<Intersect<(Header<1>,), (Offset<1>,)>, ()>());
}

fn requires_subset<S: Subset<(Header<1>, Offset<1>, Trailer<1>)>>() {}

#[test]
fn subset_is_a_bound() {
    requires_subset::<(Header<1>, Trailer<1>)>();
    requires_subset::<()>();
}

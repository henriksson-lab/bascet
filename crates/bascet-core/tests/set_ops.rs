use std::any::TypeId;

use bascet_core::attr::block::{Header, Offset, Trailer};
use bascet_core::set::{Hit, In, Intersect, Lower, Miss, Set, Subset, Union};

fn eq<T: 'static, U: 'static>() -> bool {
    TypeId::of::<T>() == TypeId::of::<U>()
}

type L<S> = <S as Lower>::Out;

#[test]
fn membership_folds_by_id() {
    assert!(eq::<
        <Header<1> as In<L<(Header<1>, Offset<1>)>>>::Result,
        Hit,
    >());
    assert!(eq::<
        <Trailer<1> as In<L<(Header<1>, Offset<1>)>>>::Result,
        Miss,
    >());
    assert!(eq::<<Header<1> as In<()>>::Result, Miss>());
}

#[test]
fn contains_answers_by_id() {
    assert!(<L<(Header<1>, Offset<1>)> as Set>::contains::<Header<1>>());
    assert!(!<L<(Header<1>, Offset<1>)> as Set>::contains::<Trailer<1>>());
    assert!(!<() as Set>::contains::<Header<1>>());
}

fn requires_set<S: Set>() {}

#[test]
fn distinct_tuple_is_a_set() {
    requires_set::<()>();
    requires_set::<L<Header<1>>>();
    requires_set::<L<(Header<1>, Offset<1>, Trailer<1>)>>();
}

#[test]
fn union_dedups_preserving_order() {
    assert!(eq::<
        <L<(Header<1>, Offset<1>)> as Union<L<(Offset<1>, Trailer<1>)>>>::Output,
        L<(Header<1>, Offset<1>, Trailer<1>)>,
    >());
    assert!(eq::<<() as Union<L<Header<1>>>>::Output, L<Header<1>>>());
    assert!(eq::<<L<Header<1>> as Union<()>>::Output, L<Header<1>>>());
}

#[test]
fn intersect_keeps_overlap() {
    assert!(eq::<
        <L<(Header<1>, Offset<1>, Trailer<1>)> as Intersect<L<(Trailer<1>, Header<1>)>>>::Output,
        L<(Header<1>, Trailer<1>)>,
    >());
    assert!(eq::<<L<Header<1>> as Intersect<L<Offset<1>>>>::Output, ()>());
}

fn requires_subset<S: Subset<L<(Header<1>, Offset<1>, Trailer<1>)>>>() {}

#[test]
fn subset_is_a_bound() {
    requires_subset::<L<(Header<1>, Trailer<1>)>>();
    requires_subset::<()>();
}

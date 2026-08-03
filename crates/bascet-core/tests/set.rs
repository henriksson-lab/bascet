use std::any::TypeId;

use bascet_core::attr::block::{Header, Offset, Trailer};
use bascet_core::set::{Intersect, Lower, Subset, Union};

fn eq<T: 'static, U: 'static>() -> bool {
    TypeId::of::<T>() == TypeId::of::<U>()
}

type L<S> = <S as Lower>::Out;

#[test]
fn union_dedups_and_orders() {
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

#[test]
fn wants_normalize() {
    assert!(eq::<
        <L<Header<1>> as Union<
            <L<Offset<1>> as Intersect<L<(Offset<1>, Header<1>)>>>::Output,
        >>::Output,
        L<(Header<1>, Offset<1>)>,
    >());
}

fn requires_subset<S: Subset<L<(Header<1>, Offset<1>, Trailer<1>)>>>() {}

#[test]
fn subset_bounds() {
    requires_subset::<L<(Header<1>, Trailer<1>)>>();
    requires_subset::<()>();
}

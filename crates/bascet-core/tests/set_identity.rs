use std::any::TypeId;

use bascet_core::attr::AttrId;
use bascet_core::attr::attr_id::{D0, D1};
use bascet_core::set::{Hit, Matches, Membership, Miss};
use bascet_derive::attr_id;

fn eq<T: 'static, U: 'static>() -> bool {
    TypeId::of::<T>() == TypeId::of::<U>()
}

type A = attr_id!(0);
type B = attr_id!(1);

#[test]
fn attr_id_folds_to_const() {
    assert_eq!(<B as AttrId>::ID, 1);
    assert_eq!(<A as AttrId>::ID, 0);
}

#[test]
fn result_folds() {
    assert!(eq::<<Hit as Membership>::And<Miss>, Miss>());
    assert!(eq::<<Miss as Membership>::Or<Hit>, Hit>());
}

#[test]
fn matches_relates_both_levels() {
    assert!(eq::<<D1 as Matches<D1>>::Result, Hit>());
    assert!(eq::<<D1 as Matches<D0>>::Result, Miss>());
    assert!(eq::<<(D0, D1) as Matches<(D0, D1)>>::Result, Hit>());
    assert!(eq::<<(D0, D1) as Matches<(D0, D0)>>::Result, Miss>());
}

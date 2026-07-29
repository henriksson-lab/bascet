use std::any::TypeId;

use bascet_core::attr::AttrId;
use bascet_core::attr::attr_id::{D0, D1};
use bascet_core::set::{Hit, Matches, Miss, SetOps};
use bascet_derive::attr_id;

fn eq<T: 'static, U: 'static>() -> bool {
    TypeId::of::<T>() == TypeId::of::<U>()
}

type One = attr_id!(1);
type Big = attr_id!(0xDEAD_BEEF_0000_0001);

#[test]
fn attr_id_folds_to_const() {
    assert_eq!(<One as AttrId>::ID, 1);
    assert_eq!(<Big as AttrId>::ID, 0xDEAD_BEEF_0000_0001);
}

#[test]
fn result_folds() {
    assert!(eq::<<Hit as SetOps>::Intersect<Miss>, Miss>());
    assert!(eq::<<Miss as SetOps>::Union<Hit>, Hit>());
}

#[test]
fn matches_relates_both_levels() {
    assert!(eq::<<D1 as Matches<D1>>::Result, Hit>());
    assert!(eq::<<D1 as Matches<D0>>::Result, Miss>());
    assert!(eq::<<(D0, D1) as Matches<(D0, D1)>>::Result, Hit>());
    assert!(eq::<<(D0, D1) as Matches<(D0, D0)>>::Result, Miss>());
}

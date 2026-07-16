use std::any::TypeId;

use bascet_core::set::{AttrId, Bool, Hit, Miss};
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
fn verdict_folds() {
    assert!(eq::<<Hit as Bool>::And<Miss>, Miss>());
    assert!(eq::<<Miss as Bool>::Or<Hit>, Hit>());
}

use std::marker::PhantomData;

pub struct AsRecord;
pub struct AsCell<Mode = Direct>(PhantomData<Mode>);
pub struct AsBlock;

pub struct Accumulate;
pub struct Direct;

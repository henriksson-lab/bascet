use bascet_derive::Attr;
use derive_more::Display;

#[derive(Attr, Display)]
pub struct Id<const N: usize = 1>;
#[derive(Attr, Display)]
pub struct Umi<const N: usize = 1>;

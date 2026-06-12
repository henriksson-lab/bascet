use bascet_derive::Attr;
use derive_more::Display;

#[derive(Attr, Display)]
pub struct Offset<const N: usize = 1>;
#[derive(Attr, Display)]
pub struct Header<const N: usize = 1>;
#[derive(Attr, Display)]
pub struct Compressed<const N: usize = 1>;
#[derive(Attr, Display)]
pub struct Trailer<const N: usize = 1>;

use bascet_derive::Attr;
use derive_more::Display;

#[derive(Attr, Display)]
#[variadic(N = 1..=16)]
#[display("Read[{N}]")]
pub struct Read<const N: usize = 1>;

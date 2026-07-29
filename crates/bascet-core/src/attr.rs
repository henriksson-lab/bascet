pub mod attr_id;
pub mod backing;
pub mod block;
pub mod ext;
pub mod meta;
pub mod phred;
pub mod reads;
pub mod store;

pub use attr_id::AttrId;
pub use backing::*;
pub use ext::*;
pub use phred::*;
pub use reads::*;
pub use store::Store;

use crate::utils::TEq;

pub trait Attr: 'static {
    type Id: AttrId;
}

pub trait Record {
    type Attrs: crate::set::Set;
}

#[derive(Debug)]
pub struct AttrEntry {
    pub id: u64,
    pub name: &'static str,
}
inventory::collect!(AttrEntry);

pub trait Represents<A: Attr> {}

pub trait Coerce<A: Attr, B: Attr> {
    type Output;
    fn coerce(self) -> Self::Output;
}

impl<A: Attr, B: Attr, V> Coerce<A, B> for V
where
    V: Represents<A> + Represents<B>,
{
    type Output = V;
    fn coerce(self) -> V {
        self
    }
}

#[diagnostic::on_unimplemented(
    message = "attribute id collision: `{Self}` and `{A}` hash to the same `AttrId` but are different attributes",
    label = "resolving `{A}` matched a store keyed to `{Self}` by id-hash — they are not the same attr",
    note = "give one of them a distinct id; two attrs sharing an `AttrId` would silently alias"
)]
pub trait AttrEq<A> {}
impl<A, B: TEq<A>> AttrEq<A> for B {}

pub trait Ref<T> {
    type Value<'a>
    where
        Self: 'a;
    fn get_ref<'a>(&'a self) -> Self::Value<'a>;
}

pub trait Mut<T> {
    type Stored;
    fn get_mut(&mut self) -> &mut Self::Stored;
}

pub trait Put<A: Attr, V> {
    fn put(&mut self, value: V);
}

impl<A: Attr, S, V> Put<A, V> for S
where
    S: Mut<A>,
    V: Represents<A> + Into<S::Stored>,
{
    fn put(&mut self, value: V) {
        *self.get_mut() = value.into();
    }
}

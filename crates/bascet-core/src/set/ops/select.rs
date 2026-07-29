use crate::attr::Attr;
use crate::set::{Hit, Miss};

pub trait Select<V> {
    type Output;
}

impl<A: Attr> Select<Hit> for A {
    type Output = (A, ());
}

impl<A: Attr> Select<Miss> for A {
    type Output = ();
}

use super::traits::SerialiseAttr;
use crate::{Attr, Get};

impl<C, A> SerialiseAttr<C> for A
where
    A: Attr + 'static,
    C: Get<A>,
    for<'a> <C as Get<A>>::Value: serde::Serialize + 'a,
{
    type Output<'a>
        = &'a <C as Get<A>>::Value
    where
        C: 'a,
        A: 'a;

    fn serialise<'a>(composite: &'a C) -> Self::Output<'a> {
        composite.as_ref()
    }
}

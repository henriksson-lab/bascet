pub enum SerialiseResult<E> {
    Serialised(usize),
    Error(E),
}

pub trait Serialise {
    fn serialise_into<S: serde::Serialize>(
        &mut self,
        value: &S,
        buf: &mut [u8],
    ) -> SerialiseResult<()>;
}

pub trait SerialiseAttr<C>: crate::Attr + Sized
where
    C: crate::Get<Self> + Sized,
{
    type Output<'a>: serde::Serialize
    where
        C: 'a,
        Self: 'a;

    fn serialise<'a>(cell: &'a C) -> Self::Output<'a>;
}

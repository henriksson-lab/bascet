use crate::{Composite, Get};

use crate::SerialiseAttr;

pub struct SerialiseView<'a, C, A> {
    composite: &'a C,
    _marker: std::marker::PhantomData<A>,
}

impl<'a, C, A> SerialiseView<'a, C, A> {
    pub fn new(composite: &'a C) -> Self {
        Self {
            composite,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'a, C> serde::Serialize for SerialiseView<'a, C, ()>
where
    C: Composite,
{
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeTuple;
        serializer.serialize_tuple(0)?.end()
    }
}

impl<'a, C, A0> serde::Serialize for SerialiseView<'a, C, (A0,)>
where
    C: Get<A0>,
    A0: SerialiseAttr<C>,
{
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeTuple;
        let mut tuple = serializer.serialize_tuple(1)?;
        tuple.serialize_element(&A0::serialise(self.composite))?;
        tuple.end()
    }
}

bascet_variadic::variadic! {
    #[expand(n = 2..=16)]
    impl<'a, C, @n[A~#](sep=",")> serde::Serialize for SerialiseView<'a, C, (@n[A~#](sep=","))>
    where
        C: @n[Get<A~#>](sep = "+"),
        @n[A~#: SerialiseAttr<C>](sep=","),
    {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            use serde::ser::SerializeTuple;
            let mut tuple = serializer.serialize_tuple(@n{#})?;
            @n[tuple.serialize_element(&A~#::serialise(self.composite))?;]
            tuple.end()
        }
    }
}

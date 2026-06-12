pub mod backing;
pub mod block;
pub mod ext;
pub mod meta;
pub mod phred;
pub mod reads;

pub use backing::*;
pub use ext::*;
pub use phred::*;
pub use reads::*;

pub trait Attr: 'static {
    const ID: u64;
}

#[derive(Debug)]
pub struct AttrEntry {
    pub id: u64,
    pub name: &'static str,
}
inventory::collect!(AttrEntry);

#[test]
pub fn assert_unique_attr_ids() {
    let mut seen = std::collections::HashMap::new();
    for entry in inventory::iter::<AttrEntry> {
        let prev = seen.insert(entry.id, entry.name);
        assert!(
            prev.is_none(),
            "Attr ID collision: {:?} and {:?} both have id {:#018x}",
            prev.unwrap(),
            entry.name,
            entry.id
        );
    }
}

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

pub trait Ref<T> {
    type Value<'a>
    where
        Self: 'a;
    fn get_ref<'a>(&'a self) -> Self::Value<'a>;
    fn get_as<'a, B: Attr>(&'a self) -> <Self::Value<'a> as Coerce<T, B>>::Output
    where
        T: Attr,
        Self::Value<'a>: Coerce<T, B>,
    {
        self.get_ref().coerce()
    }
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

bascet_variadic::variadic!(N = 2..=16, for N in N => {
    impl<S, @N[A~#](sep=",")> Ref<(@N[A~#](sep=","),)> for S
    where
        @N[S: Ref<A~#>](sep=","),
    {
        type Value<'a> = (@N[<S as Ref<A~#>>::Value<'a>](sep=", "),) where S: 'a;
        fn get_ref<'a>(&'a self) -> Self::Value<'a> {
            (@N[Ref::<A~#>::get_ref(self)](sep=", "),)
        }
    }
});

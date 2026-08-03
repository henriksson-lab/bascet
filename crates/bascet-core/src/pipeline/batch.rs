use crate::attr::Record;
use crate::attr::store::Store;
use crate::set::Set;
use crate::set::holds::Holds;

pub struct Batch<Stores> {
    stores: Stores,
}

impl<Stores> Batch<Stores> {
    pub fn new(stores: Stores) -> Self {
        Self { stores }
    }

    pub fn len(&self) -> usize
    where
        Stores: Len,
    {
        self.stores.len()
    }

    pub fn is_empty(&self) -> bool
    where
        Stores: Len,
    {
        self.len() == 0
    }

    pub fn into_parts(self) -> Stores {
        self.stores
    }

    pub fn iter<'a>(&'a self) -> Iter<'a, Stores>
    where
        Stores: Len,
    {
        Iter {
            stores: &self.stores,
            range: 0..self.stores.len(),
        }
    }

    pub fn store<A>(&self) -> &<Stores as Holds<A>>::Held
    where
        Stores: Holds<A>,
    {
        <Stores as Holds<A>>::store(&self.stores)
    }
}

pub struct Iter<'b, Stores> {
    stores: &'b Stores,
    range: std::ops::Range<usize>,
}

impl<'b, Stores> Iterator for Iter<'b, Stores> {
    type Item = View<'b, Stores>;
    fn next(&mut self) -> Option<Self::Item> {
        self.range.next().map(|i| View {
            stores: self.stores,
            i,
        })
    }
}

impl<'b, Stores: Len> IntoIterator for &'b Batch<Stores> {
    type Item = View<'b, Stores>;
    type IntoIter = Iter<'b, Stores>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct View<'b, Stores> {
    stores: &'b Stores,
    i: usize,
}

pub trait Get<A> {
    type Value<'x>
    where
        Self: 'x;
    fn get(&self) -> Self::Value<'_>;
}

impl<'b, A, Stores> Get<A> for View<'b, Stores>
where
    Stores: Holds<A>,
{
    type Value<'x>
        = <<Stores as Holds<A>>::Held as Store>::Item<'x>
    where
        Self: 'x;
    fn get(&self) -> Self::Value<'_> {
        <Stores as Holds<A>>::store(self.stores).get(self.i)
    }
}

impl<'b, Stores> View<'b, Stores> {
    pub fn get<A>(&self) -> <Self as Get<A>>::Value<'_>
    where
        Self: Get<A>,
    {
        <Self as Get<A>>::get(self)
    }
}

pub trait Keys {
    type Output;
}

impl Keys for () {
    type Output = ();
}

impl<S: Store, Rest: Keys> Keys for (S, Rest) {
    type Output = (<S as Store>::Key, <Rest as Keys>::Output);
}

impl<Stores> Record for Batch<Stores>
where
    Stores: Keys,
    <Stores as Keys>::Output: Set,
{
    type Attrs = <Stores as Keys>::Output;
}

impl<'b, Stores> Record for View<'b, Stores>
where
    Stores: Keys,
    <Stores as Keys>::Output: Set,
{
    type Attrs = <Stores as Keys>::Output;
}

pub trait Len {
    fn len(&self) -> usize;
}

impl Len for () {
    fn len(&self) -> usize {
        0
    }
}

impl<S: Store, Rest> Len for (S, Rest) {
    fn len(&self) -> usize {
        self.0.len()
    }
}

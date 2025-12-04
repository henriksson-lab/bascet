use crate::*;

pub trait Next<M> {
    type Intermediate;

    fn next_with<C, A>(&mut self) -> Result<Option<C>, ()>
    where
        C: Composite<Marker = M> + Default,
        C: FromParsed<A, Self::Intermediate> + FromBacking<Self::Intermediate, C::Backing>;

    fn next<C>(&mut self) -> Result<Option<C>, ()>
    where
        C: Composite<Marker = M> + Default,
        C: FromParsed<C::Attrs, Self::Intermediate> + FromBacking<Self::Intermediate, C::Backing>,
    {
        self.next_with::<C, C::Attrs>()
    }
}

pub trait NextBy<M>: Next<M> {
    fn next_by<C, F>(&mut self, predicate: F) -> Result<Option<C>, ()>
    where
        F: Fn(&Self::Intermediate, &Self::Intermediate) -> bool,
        C: Composite<Marker = M> + Default,
        C: FromParsed<C::Attrs, Self::Intermediate> + FromBacking<Self::Intermediate, C::Backing>;
}

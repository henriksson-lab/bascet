use crate::*;

pub trait Next<C>
where
    C: Composite,
{
    fn next_with<Q>(&mut self, query: &Q) -> Result<Option<C>, ()>
    where
        Q: QueryApply<C::Intermediate, C>;

    fn next(&mut self) -> Result<Option<C>, ()>
    where
        (): QueryApply<C::Intermediate, C>,
    {
        self.next_with(&())
    }
}

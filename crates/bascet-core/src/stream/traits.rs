use crate::*;

pub trait Next<C>
where
    C: Composite,
{
    fn next_with<Q>(&mut self, query: &Q) -> anyhow::Result<Option<C>>
    where
        Q: QueryApply<C::Intermediate, C>;

    fn next(&mut self) -> anyhow::Result<Option<C>>
    where
        (): QueryApply<C::Intermediate, C>,
    {
        self.next_with(&())
    }

    fn next_batch_with<Q>(&mut self, query: &Q, capacity: usize) -> anyhow::Result<Vec<C>>
    where
        Q: QueryApply<C::Intermediate, C>,
    {
        let mut batch = Vec::with_capacity(capacity);
        while batch.len() < capacity {
            match self.next_with(query)? {
                Some(record) => batch.push(record),
                None => break,
            }
        }
        Ok(batch)
    }

    fn next_batch_with_retained_bytes<Q>(
        &mut self,
        query: &Q,
        capacity: usize,
    ) -> anyhow::Result<Vec<(C, usize)>>
    where
        Q: QueryApply<C::Intermediate, C>,
    {
        let records = self.next_batch_with(query, capacity)?;
        Ok(records.into_iter().map(|record| (record, 0)).collect())
    }

    fn next_batch(&mut self, capacity: usize) -> anyhow::Result<Vec<C>>
    where
        (): QueryApply<C::Intermediate, C>,
    {
        self.next_batch_with(&(), capacity)
    }
}

use super::{Composite, FromCollectionIndexed, FromDirect};

pub trait GetRecords {
    fn records(&self) -> RecordIter<'_, Self>
    where
        Self: Composite<Marker = super::AsCell<super::Accumulate>> + Sized;
}

impl<C> GetRecords for C
where
    C: Composite<Marker = super::AsCell<super::Accumulate>> + super::CompositeLen,
    C::Intermediate: Default + FromDirect<C::Single, C> + FromCollectionIndexed<C::Collection, C>,
{
    fn records(&self) -> RecordIter<'_, Self> {
        RecordIter::new(self, self.len())
    }
}

pub struct RecordIter<'a, C>
where
    C: Composite<Marker = super::AsCell<super::Accumulate>>,
{
    cell: &'a C,
    index: usize,
    len: usize,
}

impl<'a, C> RecordIter<'a, C>
where
    C: Composite<Marker = super::AsCell<super::Accumulate>>,
{
    pub fn new(cell: &'a C, len: usize) -> Self {
        Self {
            cell,
            index: 0,
            len,
        }
    }
}

impl<'a, C> Iterator for RecordIter<'a, C>
where
    C: Composite<Marker = super::AsCell<super::Accumulate>>,
    C::Intermediate: Default + FromDirect<C::Single, C> + FromCollectionIndexed<C::Collection, C>,
{
    type Item = C::Intermediate;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.len {
            let mut record = C::Intermediate::default();
            record.from_direct(self.cell);
            record.from_collection_indexed(self.cell, self.index);

            self.index += 1;
            Some(record)
        } else {
            None
        }
    }
}

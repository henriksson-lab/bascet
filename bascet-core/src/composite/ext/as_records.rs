use crate::composite::{FromCollectionIndexed, FromDirect};
use crate::{Composite, Get};

pub trait CompositeLen {
    type LenAttr: crate::Attr;
}

pub trait AsRecords: CompositeLen {
    type Record;

    fn as_records(&self) -> RecordIterator<'_, Self, Self::Record>
    where
        Self: Sized;
}

pub struct RecordIterator<'a, Cell, Record> {
    cell: &'a Cell,
    index: usize,
    len: usize,
    _phantom: std::marker::PhantomData<Record>,
}

impl<'a, Cell, Record> RecordIterator<'a, Cell, Record> {
    pub fn new(cell: &'a Cell, len: usize) -> Self {
        Self {
            cell,
            index: 0,
            len,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<'a, Cell, Record> Iterator for RecordIterator<'a, Cell, Record>
where
    Cell: Composite,
    Record: Composite + Default,
    Record: FromDirect<Cell::Single, Cell>,
    Record: FromCollectionIndexed<Cell::Collection, Cell>,
{
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.len {
            return None;
        }

        let mut record = Record::default();
        record.from_direct(self.cell);
        record.from_collection_indexed(self.cell, self.index);

        self.index += 1;
        Some(record)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len - self.index;
        (remaining, Some(remaining))
    }
}

impl<'a, Cell, Record> ExactSizeIterator for RecordIterator<'a, Cell, Record>
where
    Cell: Composite,
    Record: Composite + Default,
    Record: FromDirect<Cell::Single, Cell>,
    Record: FromCollectionIndexed<Cell::Collection, Cell>,
{
    fn len(&self) -> usize {
        self.len - self.index
    }
}

impl<C> AsRecords for C
where
    C: Composite<Marker = crate::AsCell<crate::Accumulate>>,
    C: CompositeLen,
    C: Get<C::LenAttr>,
    <C as Get<C::LenAttr>>::Value: crate::attr::AsCollection,
    C::Intermediate: Composite + Default,
    C::Intermediate: FromDirect<C::Single, C>,
    C::Intermediate: FromCollectionIndexed<C::Collection, C>,
{
    type Record = C::Intermediate;

    fn as_records(&self) -> RecordIterator<'_, Self, Self::Record> {
        let len = crate::attr::AsCollection::len(<C as Get<C::LenAttr>>::as_ref(self));
        RecordIterator::new(self, len)
    }
}

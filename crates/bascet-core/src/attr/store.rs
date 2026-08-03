use crate::attr::Attr;

pub trait Store: 'static {
    type Key: Attr;
    type Item<'a>
    where
        Self: 'a;
    fn get(&self, row: usize) -> Self::Item<'_>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

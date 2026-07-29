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

#[diagnostic::on_unimplemented(
    message = "store `{Self}` is not readable as `&[u8]`",
    label = "this layer reads its attribute as `&[u8]`, but `{Self}` yields a different representation"
)]
pub trait Bytes: for<'a> Store<Item<'a> = &'a [u8]> {}
impl<S> Bytes for S where S: for<'a> Store<Item<'a> = &'a [u8]> {}

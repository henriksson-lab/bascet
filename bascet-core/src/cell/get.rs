pub trait Get<'a, T> {
    fn get(cell: &'a T) -> Self;
}

pub trait GetMut<'a, T> {
    fn get_mut(cell: &'a mut T) -> Self;
}

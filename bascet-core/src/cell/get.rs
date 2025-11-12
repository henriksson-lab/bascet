pub trait Get<T> {
    fn get(cell: &T) -> Self;
}

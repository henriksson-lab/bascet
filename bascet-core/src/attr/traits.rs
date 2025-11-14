pub trait Attr {}

pub trait GetRef<'a, T> {
    type Output;
    fn get_ref(_: &'a T) -> Self::Output;
}

pub trait GetMut<'a, T> {
    type Output;
    fn get_mut(_: &'a mut T) -> Self::Output;
}

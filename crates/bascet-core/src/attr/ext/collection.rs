pub trait Len {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait Collection<T>: Len {
    type Iter<'a>: Iterator<Item = &'a T>
    where
        Self: 'a,
        T: 'a;
    fn iter(&self) -> Self::Iter<'_>;
}

pub trait CollectionMut<T>: Collection<T> {
    fn push(&mut self, value: T);
}

impl<T> Len for Vec<T> {
    fn len(&self) -> usize {
        Vec::len(self)
    }
}

impl<T> Len for &[T] {
    fn len(&self) -> usize {
        <[T]>::len(self)
    }
}

impl<T, const N: usize> Len for [T; N] {
    fn len(&self) -> usize {
        N
    }
}

impl<T> Collection<T> for Vec<T> {
    type Iter<'a>
        = std::slice::Iter<'a, T>
    where
        Self: 'a;
    fn iter(&self) -> Self::Iter<'_> {
        self.as_slice().iter()
    }
}

impl<T> Collection<T> for &[T] {
    type Iter<'a>
        = std::slice::Iter<'a, T>
    where
        Self: 'a;
    fn iter(&self) -> Self::Iter<'_> {
        (*self).iter()
    }
}

impl<T, const N: usize> Collection<T> for [T; N] {
    type Iter<'a>
        = std::slice::Iter<'a, T>
    where
        Self: 'a;
    fn iter(&self) -> Self::Iter<'_> {
        self.as_slice().iter()
    }
}

impl<T> CollectionMut<T> for Vec<T> {
    fn push(&mut self, value: T) {
        Vec::push(self, value)
    }
}

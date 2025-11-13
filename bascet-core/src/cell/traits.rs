pub trait Builder: Sized {
    type Builds: super::Cell;
    fn build(self) -> Self::Builds;

    fn with<S: Build<Self>>(self, value: S::Type) -> Self {
        S::build(self, value)
    }
}

pub trait Build<B: Builder> {
    type Type;
    fn build(builder: B, value: Self::Type) -> B;
}

pub trait Cell: Sized {
    type Builder: super::Builder<Builds = Self>;
    fn builder() -> Self::Builder;

    fn get_ref<'a, G: GetRef<'a, Self>>(&'a self) -> G::Output {
        G::get_ref(self)
    }

    fn get_mut<'a, G: GetMut<'a, Self>>(&'a mut self) -> G::Output {
        G::get_mut(self)
    }
}

pub trait GetRef<'a, T> {
    type Output;
    fn get_ref(cell: &'a T) -> Self::Output;
}

pub trait GetMut<'a, T> {
    type Output;
    fn get_mut(cell: &'a mut T) -> Self::Output;
}

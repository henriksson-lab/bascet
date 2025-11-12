pub trait Build<B: Builder> {
    type Type;
    fn build(builder: B, value: Self::Type) -> B;
}

pub trait Builder: Sized {
    type Builds: super::Cell;
    fn build(self) -> Self::Builds;

    fn with<S: Build<Self>>(self, value: S::Type) -> Self {
        S::build(self, value)
    }
}

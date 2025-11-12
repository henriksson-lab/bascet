pub trait UseManagedRef {
    type Ref: crate::mem::ManagedRef;
    fn value(&self) -> Self::Ref;
}

pub trait ProvideID {
    type Type;
    fn value(&self) -> Self::Type;
}
pub trait ProvideReadPair {
    type Type;
    fn value(&self) -> Self::Type;
}
pub trait ProvideRead {
    type Type;
    fn value(&self) -> Self::Type;
}
pub trait ProvideQualityPair {
    type Type;
    fn value(&self) -> Self::Type;
}
pub trait ProvideQuality {
    type Type;
    fn value(&self) -> Self::Type;
}
pub trait ProvideUMI {
    type Type;
    fn value(&self) -> Self::Type;
}
pub trait ProvideMetadata {
    type Type;
    fn value(&self) -> Self::Type;
}

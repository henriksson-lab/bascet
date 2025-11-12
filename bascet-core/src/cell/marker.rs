pub trait UseManagedRef {
    type Ref: crate::mem::ManagedRef;
    fn value(&self) -> Self::Ref;
}

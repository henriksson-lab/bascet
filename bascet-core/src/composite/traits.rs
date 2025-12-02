pub trait Composite: Sized {
    type Attrs;
    type Backing: crate::Backing;
    type Kind: CompositeKind;

    #[inline(always)]
    fn get_ref<'a, G: crate::Ref<'a, Self>>(&'a self) -> G::Output {
        G::get_ref(self)
    }
    #[inline(always)]
    fn get_mut<'a, G: crate::Mut<'a, Self>>(&'a mut self) -> G::Output {
        G::get_mut(self)
    }
}

pub trait CompositeKind {}

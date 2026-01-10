use crate::OwnedBacking;

// Null-variant. Not even sure what needs this anymore but removing it breaks things
impl Composite for () {
    type Attrs = ();
    type Single = ();
    type Collection = ();

    type Marker = ();
    type Intermediate = Self;

    type Backing = OwnedBacking;
}

pub trait Composite: Sized {
    type Attrs;
    type Single;
    type Collection;

    type Marker;
    type Intermediate: Composite;

    type Backing: crate::Backing;

    #[inline(always)]
    fn get_ref<'a, G: crate::Ref<'a, Self>>(&'a self) -> G::Output {
        G::get_ref(self)
    }
    #[inline(always)]
    fn get_mut<'a, G: crate::Mut<'a, Self>>(&'a mut self) -> G::Output {
        G::get_mut(self)
    }
    #[inline(always)]                                                                         
    fn as_bytes<'a, G: crate::AsBytes<'a, Self>>(&'a self) -> G::Output {                     
        G::as_bytes(self)                                                                     
    }
}

pub trait FromDirect<AttrTuple, Source> {
    fn from_direct(&mut self, source: &Source);
}
pub trait FromCollectionIndexed<AttrTuple, Source> {
    fn from_collection_indexed(&mut self, source: &Source, index: usize);
}

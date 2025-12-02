use smallvec::SmallVec;

use crate::{ArenaView, FromBacking};

bascet_derive::define_backing!(OwnedBacking, ArenaBacking);

// OwnedBacking with () no backing needed
impl<T, S> FromBacking<S, OwnedBacking> for T
where
    T: crate::Get<OwnedBacking, Value = ()>,
    S: crate::Get<OwnedBacking, Value = ()>,
{
    fn take_backing(&mut self, _source: S) {}
}

impl<T, S, U, const N: usize> FromBacking<S, ArenaBacking> for T
where
    T: crate::Get<ArenaBacking, Value = SmallVec<[ArenaView<U>; N]>>,
    S: crate::Get<ArenaBacking, Value = SmallVec<[ArenaView<U>; N]>>,
    U: bytemuck::Pod,
{
    fn take_backing(&mut self, source: S) {
        *self.as_mut() = source.as_ref().clone();
    }
}

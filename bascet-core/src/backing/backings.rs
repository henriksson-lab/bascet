use smallvec::SmallVec;

use crate::{ArenaView, PushBacking, TakeBacking};

bascet_derive::define_backing!(OwnedBacking, ArenaBacking);

impl<T> TakeBacking<OwnedBacking> for T
where
    T: crate::Get<OwnedBacking, Value = ()>,
{
    #[inline(always)]
    fn take_backing(self) -> () {}
}

impl<T, S> PushBacking<S, OwnedBacking> for T
where
    T: crate::Get<OwnedBacking, Value = ()>,
    S: crate::Get<OwnedBacking, Value = ()>,
{
    #[inline(always)]
    fn push_backing(&mut self, _backing: ()) {}
}

impl<T, U, const N: usize> TakeBacking<ArenaBacking> for T
where
    T: crate::Get<ArenaBacking, Value = SmallVec<[ArenaView<U>; N]>>,
    U: bytemuck::Pod,
{
    #[inline(always)]
    fn take_backing(mut self) -> SmallVec<[ArenaView<U>; N]> {
        std::mem::take(self.as_mut())
    }
}

impl<T, S, U, const N: usize, const M: usize> PushBacking<S, ArenaBacking> for T
where
    T: crate::Get<ArenaBacking, Value = SmallVec<[ArenaView<U>; N]>>,
    S: crate::Get<ArenaBacking, Value = SmallVec<[ArenaView<U>; M]>>,
    U: bytemuck::Pod,
{
    #[inline(always)]
    fn push_backing(&mut self, backing: SmallVec<[ArenaView<U>; M]>) {
        self.as_mut().extend(backing);
    }
}

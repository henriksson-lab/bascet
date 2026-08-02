use crate::attr::Attr;
use crate::set::Membership;
use crate::set::ops::chain::Chain;
use crate::set::ops::member::In;
use crate::set::ops::select::Select;

pub trait Absorb<L> {
    type Output;
}

impl<A: Attr, L> Absorb<L> for A
where
    A: In<L>,
    A: Select<<<A as In<L>>::Result as Membership>::Not>,
    L: Chain<<A as Select<<<A as In<L>>::Result as Membership>::Not>>::Output>,
{
    type Output =
        <L as Chain<<A as Select<<<A as In<L>>::Result as Membership>::Not>>::Output>>::Output;
}

pub trait Union<R> {
    type Output;
}

impl<L> Union<()> for L {
    type Output = L;
}

impl<L, H: Attr, Rest> Union<(H, Rest)> for L
where
    H: Absorb<L>,
    <H as Absorb<L>>::Output: Union<Rest>,
{
    type Output = <<H as Absorb<L>>::Output as Union<Rest>>::Output;
}

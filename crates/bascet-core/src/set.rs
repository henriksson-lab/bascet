pub mod matches;
pub mod ops;

pub use matches::Matches;
pub use ops::{Chain, In, Intersect, Lower, Union};

use crate::attr::{Attr, AttrId};

pub struct Hit;
pub struct Miss;

pub trait Membership {
    type And<Other: Membership>: Membership;
    type Or<Other: Membership>: Membership;
    type Not: Membership;
}

impl Membership for Hit {
    type And<Other: Membership> = Other;
    type Or<Other: Membership> = Hit;
    type Not = Miss;
}

impl Membership for Miss {
    type And<Other: Membership> = Miss;
    type Or<Other: Membership> = Other;
    type Not = Hit;
}

pub trait Set: 'static {
    fn contains<A: Attr>() -> bool;
}

impl Set for () {
    fn contains<A: Attr>() -> bool {
        false
    }
}

impl<H: Attr, Rest: Set> Set for (H, Rest)
where
    H: In<Rest, Result = Miss>,
{
    fn contains<A: Attr>() -> bool {
        <A::Id as AttrId>::ID == <H::Id as AttrId>::ID || Rest::contains::<A>()
    }
}

#[diagnostic::on_unimplemented(
    message = "`{Self}` requires attributes not provided upstream",
    label = "the producer's `Provides` must cover this layer's `Requires`"
)]
pub trait Subset<Sup: Set> {}

impl<Sup: Set> Subset<Sup> for () {}

impl<Sup: Set, H: Attr, Rest> Subset<Sup> for (H, Rest)
where
    H: In<Sup, Result = Hit>,
    Rest: Subset<Sup>,
{
}

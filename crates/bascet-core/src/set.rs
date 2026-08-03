pub mod chain;
pub mod holds;
pub mod intersect;
pub mod lower;
pub mod matches;
pub mod member;
pub mod partition;
pub mod select;
pub mod union;

pub use chain::Chain;
pub use holds::Holds;
pub use intersect::{Intersect, Keep};
pub use lower::Lower;
pub use matches::Matches;
pub use member::In;
pub use partition::{Compose, Partition};
pub use select::Select;
pub use union::{Absorb, Union};

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

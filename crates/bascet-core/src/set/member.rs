use crate::attr::Attr;
use crate::set::matches::Matches;
use crate::set::{Membership, Miss};

pub trait In<S> {
    type Result: Membership;
}

impl<X: Attr> In<()> for X {
    type Result = Miss;
}

impl<X: Attr, H: Attr, Rest> In<(H, Rest)> for X
where
    X::Id: Matches<H::Id>,
    X: In<Rest>,
{
    type Result = <<X::Id as Matches<H::Id>>::Result as Membership>::Or<<X as In<Rest>>::Result>;
}

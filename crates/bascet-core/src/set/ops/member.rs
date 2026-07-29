use crate::attr::Attr;
use crate::set::matches::Matches;
use crate::set::{Miss, SetOps};

pub trait In<S> {
    type Result: SetOps;
}

impl<X: Attr> In<()> for X {
    type Result = Miss;
}

impl<X: Attr, H: Attr, Rest> In<(H, Rest)> for X
where
    X::Id: Matches<H::Id>,
    X: In<Rest>,
{
    type Result = <<X::Id as Matches<H::Id>>::Result as SetOps>::Union<<X as In<Rest>>::Result>;
}

use crate::set::Set;

pub trait Contract {
    type Input;
    type Output;
    type Provides: Set;
    type Requires: Set;
    type Resources;
}

impl Contract for () {
    type Input = ();
    type Output = ();
    type Provides = ();
    type Requires = ();
    type Resources = ();
}

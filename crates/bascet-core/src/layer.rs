use crate::set::Set;

pub trait Layer {
    type Provides: Set;
    type Requires: Set;
    type Resources;
}

impl Layer for () {
    type Provides = ();
    type Requires = ();
    type Resources = ();
}

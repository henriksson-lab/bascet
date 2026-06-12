pub trait Backing {
    type Storage: Default;
}

pub struct OwnedBacking;
pub struct ArenaBacking;

impl Backing for OwnedBacking {
    type Storage = ();
}

impl Backing for ArenaBacking {
    type Storage = Vec<crate::ArenaView>;
}

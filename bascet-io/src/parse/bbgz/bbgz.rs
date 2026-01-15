use bascet_core::{ArenaView, Composite, Header, Id, Raw, Trailer};
use serde::Serialize;

pub struct BBGZParser {
    pub(crate) inner_cursor: usize,
}

pub fn parser() -> BBGZParser {
    BBGZParser { inner_cursor: 0 }
}

#[derive(Composite, Clone, Default, Serialize)]
#[bascet(attrs = (Id, Header, Raw, Trailer), backing = ArenaBacking, marker = AsBlock)]
pub struct Block {
    pub id: &'static [u8],
    // SAFETY: exposed ONLY to allow conversion outside this crate.
    //         be VERY careful modifying this at all\
    pub header: &'static [u8],
    pub raw: &'static [u8],
    pub trailer: &'static [u8],
    #[serde(skip)]
    pub(crate) arena_backing: smallvec::SmallVec<[ArenaView<u8>; 2]>,
}

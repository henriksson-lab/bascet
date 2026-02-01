use bascet_core::{
    ArenaView, Composite,
    attr::{block::*, meta::*},
};
use serde::Serialize;
use smallvec::SmallVec;

pub struct BBGZParser {
    pub(crate) inner_cursor: usize,
    pub(crate) inner_absolute_cursor: u64,
}

pub fn parser() -> BBGZParser {
    BBGZParser {
        inner_cursor: 0,
        inner_absolute_cursor: 0,
    }
}

#[derive(Composite, Clone, Default)]
#[bascet(
    attrs = (Id, Offset, Header, Compressed, Trailer),
    backing = ArenaBacking,
    marker = AsBlock
)]
pub struct Block {
    pub id: &'static [u8],
    pub offset: u64,
    pub header: &'static [u8],
    pub compressed: &'static [u8],
    pub trailer: &'static [u8],

    // SAFETY: exposed ONLY to allow conversion outside this crate.
    //         be VERY careful modifying this at all
    pub(crate) arena_backing: smallvec::SmallVec<[ArenaView<u8>; 2]>,
}

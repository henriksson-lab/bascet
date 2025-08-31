use crate::{common, field_traits};

field_traits! {
    Id => {
        Accessor::get() -> &'static [u8],
        Builder::set(id: &'static [u8]),
    },
    OwnedId => {
        Accessor::get_vec() -> &Vec<u8>,
        Builder::set(data: Vec<u8>),
    },

    PairedRead => {
        Accessor::get_vec() -> &Vec<(&'static [u8], &'static [u8])>,
        Builder::push(r1: &'static [u8], r2: &'static [u8]),
    },
    OwnedPairedRead => {
        Accessor::get_vec() -> &Vec<(Vec<u8>, Vec<u8>)>,
        Builder::push(r1: Vec<u8>, r2: Vec<u8>),
    },

    UnpairedRead => {
        Accessor::get_vec() -> &Vec<&'static [u8]>,
        Builder::push(r0: &'static [u8]),
    },
    OwnedUnpairedRead => {
        Accessor::get_vec() -> &Vec<Vec<u8>>,
        Builder::push(r0: Vec<u8>),
    },

    PairedQuality => {
        Accessor::get_vec() -> &Vec<(&'static [u8], &'static [u8])>,
        Builder::push(q1: &'static [u8], q2: &'static [u8]),
    },
    OwnedPairedQuality => {
        Accessor::get_vec() -> &Vec<(Vec<u8>, Vec<u8>)>,
        Builder::push(q1: Vec<u8>, q2: Vec<u8>),
    },

    UnpairedQuality => {
        Accessor::get_vec() -> &Vec<&'static [u8]>,
        Builder::push(q0: &'static [u8]),
    },
    OwnedUnpairedQuality => {
        Accessor::get_vec() -> &Vec<Vec<u8>>,
        Builder::push(q0: Vec<u8>),
    },

    Umi => {
        Accessor::get_vec() -> &Vec<&'static [u8]>,
        Builder::push(umi: &'static [u8]),
    },
    OwnedUmi => {
        Accessor::get_vec() -> &Vec<Vec<u8>>,
        Builder::push(umi: Vec<u8>),
    },

    Pageref => {
        Accessor::get_vec() -> &common::UnsafeMutPtr<common::PageBuffer>,
        Builder::push(ptr: common::UnsafeMutPtr<common::PageBuffer>)
    }
}

pub trait BascetCell: Send + Sized {
    type Builder: BascetCellBuilder<Cell = Self>;
    fn builder() -> Self::Builder;
}

pub trait BascetCellBuilder: Sized {
    type Cell: BascetCell;
    fn build(self) -> Self::Cell;
}

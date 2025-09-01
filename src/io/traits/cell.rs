use crate::{common, field_traits};

field_traits! {
    Id => {
        Accessor::get() -> &'static [u8],
        Builder::set(id: &'static [u8]),
    },
    OwnedId => {
        Accessor::get() -> &Vec<u8>,
        Builder::set(data: Vec<u8>),
    },

    PairedReads => {
        Accessor::get_vec() -> &Vec<(&'static [u8], &'static [u8])>,
        Builder::push(r1: &'static [u8], r2: &'static [u8]),
    },
    OwnedPairedReads => {
        Accessor::get_vec() -> &Vec<(Vec<u8>, Vec<u8>)>,
        Builder::push(r1: Vec<u8>, r2: Vec<u8>),
    },

    UnpairedReads => {
        Accessor::get_vec() -> &Vec<&'static [u8]>,
        Builder::push(r0: &'static [u8]),
    },
    OwnedUnpairedReads => {
        Accessor::get_vec() -> &Vec<Vec<u8>>,
        Builder::push(r0: Vec<u8>),
    },

    PairedQualities => {
        Accessor::get_vec() -> &Vec<(&'static [u8], &'static [u8])>,
        Builder::push(q1: &'static [u8], q2: &'static [u8]),
    },
    OwnedPairedQualities => {
        Accessor::get_vec() -> &Vec<(Vec<u8>, Vec<u8>)>,
        Builder::push(q1: Vec<u8>, q2: Vec<u8>),
    },

    UnpairedQualities => {
        Accessor::get_vec() -> &Vec<&'static [u8]>,
        Builder::push(q0: &'static [u8]),
    },
    OwnedUnpairedQualities => {
        Accessor::get_vec() -> &Vec<Vec<u8>>,
        Builder::push(q0: Vec<u8>),
    },

    Umis => {
        Accessor::get_vec() -> &Vec<&'static [u8]>,
        Builder::push(umi: &'static [u8]),
    },
    OwnedUmis => {
        Accessor::get_vec() -> &Vec<Vec<u8>>,
        Builder::push(umi: Vec<u8>),
    },

    Pagerefs => {
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

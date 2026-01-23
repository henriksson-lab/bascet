use bascet_core::attr::{meta::*, quality::*, sequence::*};
use bascet_core::*;
use serde::Serialize;

pub struct Tirp {
    pub(crate) inner_cursor: usize,
}

#[bon::bon]
impl Tirp {
    #[builder]
    pub fn new() -> Self {
        Self { inner_cursor: 0 }
    }
}

#[derive(Composite, Default, Clone, Serialize)]
#[bascet(attrs = (Id, R1, R2, Q1, Q2, Umi), backing = ArenaBacking, marker = AsRecord)]
pub struct Record {
    id: &'static [u8],
    r1: &'static [u8],
    r2: &'static [u8],
    q1: &'static [u8],
    q2: &'static [u8],
    umi: &'static [u8],

    // SAFETY: exposed ONLY to allow conversion outside this crate.
    //         be VERY careful modifying this at all
    #[serde(skip)]
    pub(crate) arena_backing: smallvec::SmallVec<[ArenaView<u8>; 2]>,
}

#[derive(Composite, Default, Clone, Serialize)]
#[bascet(attrs = (Id, R1, R2, Q1, Q2, Umi), backing = OwnedBacking, marker = AsRecord)]
pub struct OwnedRecord {
    id: Vec<u8>,
    r1: Vec<u8>,
    r2: Vec<u8>,
    q1: Vec<u8>,
    q2: Vec<u8>,
    umi: Vec<u8>,

    #[serde(skip)]
    owned_backing: (),
}

impl Into<OwnedRecord> for Record {
    fn into(self) -> OwnedRecord {
        OwnedRecord {
            id: self.id.to_vec(),
            r1: self.r1.to_vec(),
            r2: self.r2.to_vec(),
            q1: self.q1.to_vec(),
            q2: self.q2.to_vec(),
            umi: self.umi.to_vec(),
            owned_backing: (),
        }
    }
}

impl Record {
    pub unsafe fn from_raw(
        buf_record: &[u8],
        pos_tab: [usize; 7],
        arena_view: ArenaView<u8>,
    ) -> Self {
        // SAFETY: Caller guarantees pos_newline indices are valid
        let id = buf_record.get_unchecked(..pos_tab[0]);
        let r1 = buf_record.get_unchecked(pos_tab[2] + 1..pos_tab[3]);
        let r2 = buf_record.get_unchecked(pos_tab[3] + 1..pos_tab[4]);
        let q1 = buf_record.get_unchecked(pos_tab[4] + 1..pos_tab[5]);
        let q2 = buf_record.get_unchecked(pos_tab[5] + 1..pos_tab[6]);
        let umi = buf_record.get_unchecked(pos_tab[6] + 1..);

        if likely_unlikely::unlikely(r1.len() != q1.len()) {
            panic!(
                "r1/q1 length mismatch: {:?} != {:?} in {:?}",
                r1.len(),
                q1.len(),
                String::from_utf8_lossy(buf_record)
            );
        }
        if likely_unlikely::unlikely(r2.len() != q2.len()) {
            panic!(
                "r1/q1 length mismatch: {:?} != {:?} in {:?}",
                r2.len(),
                q2.len(),
                String::from_utf8_lossy(buf_record)
            );
        }

        // SAFETY: transmute slices to static lifetime kept alive by ArenaView refcount
        let static_id: &'static [u8] = unsafe { std::mem::transmute(id) };
        let static_r1: &'static [u8] = unsafe { std::mem::transmute(r1) };
        let static_r2: &'static [u8] = unsafe { std::mem::transmute(r2) };
        let static_q1: &'static [u8] = unsafe { std::mem::transmute(q1) };
        let static_q2: &'static [u8] = unsafe { std::mem::transmute(q2) };
        let static_umi: &'static [u8] = unsafe { std::mem::transmute(umi) };

        Self {
            id: static_id,
            r1: static_r1,
            r2: static_r2,
            q1: static_q1,
            q2: static_q2,
            umi: static_umi,

            arena_backing: smallvec::smallvec![arena_view],
        }
    }
}

#[derive(Composite, Default, Serialize)]
#[bascet(
    attrs = (Id, R1 = vec_r1, R2 = vec_r2, Q1 = vec_q1, Q2 = vec_q2, Umi = vec_umis),
    backing = ArenaBacking,
    marker = AsCell<Accumulate>,
    intermediate = Record
)]
pub struct Cell {
    id: &'static [u8],

    #[collection]
    vec_r1: Vec<&'static [u8]>,
    #[collection]
    vec_r2: Vec<&'static [u8]>,
    #[collection]
    vec_q1: Vec<&'static [u8]>,
    #[collection]
    vec_q2: Vec<&'static [u8]>,
    #[collection]
    vec_umis: Vec<&'static [u8]>,

    // SAFETY: exposed ONLY to allow conversion outside this crate.
    //         be VERY careful modifying this at all
    #[serde(skip)]
    pub(crate) arena_backing: smallvec::SmallVec<[ArenaView<u8>; 2]>,
}

#[derive(Composite, Default, Serialize)]
#[bascet(
    attrs = (Id, R1 = vec_r1, R2 = vec_r2, Q1 = vec_q1, Q2 = vec_q2, Umi = vec_umis),
    backing = OwnedBacking,
    marker = AsCell<Accumulate>,
    intermediate = Record
)]
pub struct OwnedCell {
    id: Vec<u8>,
    #[collection]
    vec_r1: Vec<Vec<u8>>,
    #[collection]
    vec_r2: Vec<Vec<u8>>,
    #[collection]
    vec_q1: Vec<Vec<u8>>,
    #[collection]
    vec_q2: Vec<Vec<u8>>,
    #[collection]
    vec_umis: Vec<Vec<u8>>,

    #[serde(skip)]
    owned_backing: (),
}

impl Into<OwnedCell> for Cell {
    fn into(self) -> OwnedCell {
        OwnedCell {
            id: self.id.to_vec(),
            vec_r1: self.vec_r1.iter().map(|r1| r1.to_vec()).collect(),
            vec_r2: self.vec_r2.iter().map(|r2| r2.to_vec()).collect(),
            vec_q1: self.vec_q1.iter().map(|q1| q1.to_vec()).collect(),
            vec_q2: self.vec_q2.iter().map(|q2| q2.to_vec()).collect(),
            vec_umis: self.vec_umis.iter().map(|umi| umi.to_vec()).collect(),
            owned_backing: (),
        }
    }
}

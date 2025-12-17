use bascet_core::*;

pub struct Tirp {
    pub(crate) inner_cursor: usize,
}

#[bon::bon]
impl Tirp {
    #[builder]
    pub fn new() -> Result<Self, ()> {
        Ok(Tirp { inner_cursor: 0 })
    }
}

#[derive(Composite, Default, Clone)]
#[bascet(attrs = (Id, SequencePair, QualityPair, Umi), backing = ArenaBacking, marker = AsRecord)]
pub struct Record {
    id: &'static [u8],
    sequence_pair: (&'static [u8], &'static [u8]),
    quality_pair: (&'static [u8], &'static [u8]),
    umi: &'static [u8],

    // SAFETY: exposed ONLY to allow conversion outside this crate.
    //         be VERY careful modifying this at all
    pub(crate) arena_backing: smallvec::SmallVec<[ArenaView<u8>; 2]>,
}

#[derive(Composite, Default, Clone)]
#[bascet(attrs = (Id, SequencePair, QualityPair, Umi), backing = OwnedBacking, marker = AsRecord)]
pub struct OwnedRecord {
    id: Vec<u8>,
    sequence_pair: (Vec<u8>, Vec<u8>),
    quality_pair: (Vec<u8>, Vec<u8>),
    umi: Vec<u8>,

    owned_backing: (),
}

impl Into<OwnedRecord> for Record {
    fn into(self) -> OwnedRecord {
        OwnedRecord {
            id: self.id.to_vec(),
            sequence_pair: (self.sequence_pair.0.to_vec(), self.sequence_pair.1.to_vec()),
            quality_pair: (self.quality_pair.0.to_vec(), self.quality_pair.1.to_vec()),
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
            panic!("r1/q1 length mismatch: {:?} != {:?}", r1.len(), q1.len());
        }
        if likely_unlikely::unlikely(r2.len() != q2.len()) {
            panic!("r1/q1 length mismatch: {:?} != {:?}", r2.len(), q2.len());
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
            sequence_pair: (static_r1, static_r2),
            quality_pair: (static_q1, static_q2),
            umi: static_umi,

            arena_backing: smallvec::smallvec![arena_view],
        }
    }
}

#[derive(Composite, Default)]
#[bascet(
    attrs = (Id, SequencePair = vec_sequence_pairs, QualityPair = vec_quality_pairs, Umi = vec_umis),
    backing = ArenaBacking,
    marker = AsCell<Accumulate>,
    intermediate = Record
)]
pub struct Cell {
    id: &'static [u8],
    #[collection]
    vec_sequence_pairs: Vec<(&'static [u8], &'static [u8])>,
    #[collection]
    vec_quality_pairs: Vec<(&'static [u8], &'static [u8])>,
    #[collection]
    vec_umis: Vec<&'static [u8]>,

    // SAFETY: exposed ONLY to allow conversion outside this crate.
    //         be VERY careful modifying this at all
    pub(crate) arena_backing: smallvec::SmallVec<[ArenaView<u8>; 2]>,
}

#[derive(Composite, Default)]
#[bascet(
    attrs = (Id, SequencePair = vec_sequence_pairs, QualityPair = vec_quality_pairs, Umi = vec_umis),
    backing = OwnedBacking,
    marker = AsCell<Accumulate>,
    intermediate = Record
)]
pub struct OwnedCell {
    id: Vec<u8>,
    #[collection]
    vec_sequence_pairs: Vec<(Vec<u8>, Vec<u8>)>,
    #[collection]
    vec_quality_pairs: Vec<(Vec<u8>, Vec<u8>)>,
    #[collection]
    vec_umis: Vec<Vec<u8>>,

    owned_backing: (),
}

impl Into<OwnedCell> for Cell {
    fn into(self) -> OwnedCell {
        OwnedCell {
            id: self.id.to_vec(),
            vec_sequence_pairs: self
                .vec_sequence_pairs
                .iter()
                .map(|(r1, r2)| (r1.to_vec(), r2.to_vec()))
                .collect(),
            vec_quality_pairs: self
                .vec_quality_pairs
                .iter()
                .map(|(r1, r2)| (r1.to_vec(), r2.to_vec()))
                .collect(),
            vec_umis: self.vec_umis.iter().map(|umi| umi.to_vec()).collect(),
            owned_backing: (),
        }
    }
}

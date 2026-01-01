use bascet_core::*;

pub struct Fastq {
    pub(crate) inner_cursor: usize,
}

#[bon::bon]
impl Fastq {
    #[builder]
    pub fn new() -> Result<Self, ()> {
        Ok(Fastq { inner_cursor: 0 })
    }
}

#[derive(Composite, Default)]
#[bascet(attrs = (Id, Sequence, Quality), backing = ArenaBacking, marker = AsRecord)]
pub struct Record {
    pub id: &'static [u8],
    pub sequence: &'static [u8],
    pub quality: &'static [u8],

    // SAFETY: exposed ONLY to allow conversion outside this crate.
    //         be VERY careful modifying this at all
    pub(crate) arena_backing: smallvec::SmallVec<[ArenaView<u8>; 2]>,
}

#[derive(Composite, Default, Clone)]
#[bascet(attrs = (Id, Sequence, Quality), backing = OwnedBacking, marker = AsRecord)]
pub struct OwnedRecord {
    id: Vec<u8>,
    sequence: Vec<u8>,
    quality: Vec<u8>,

    owned_backing: (),
}

impl Into<OwnedRecord> for Record {
    fn into(self) -> OwnedRecord {
        OwnedRecord {
            id: self.id.to_vec(),
            sequence: self.sequence.to_vec(),
            quality: self.quality.to_vec(),

            owned_backing: (),
        }
    }
}

impl Record {
    pub unsafe fn from_raw(
        buf_record: &[u8],
        pos_newline: [usize; 4],
        arena_view: ArenaView<u8>,
    ) -> Self {
        // SAFETY: Caller guarantees pos_newline indices are valid
        let hdr = buf_record.get_unchecked(..pos_newline[0]);
        let seq = buf_record.get_unchecked(pos_newline[0] + 1..pos_newline[1]);
        let sep = buf_record.get_unchecked(pos_newline[1] + 1..pos_newline[2]);
        let qal = buf_record.get_unchecked(pos_newline[2] + 1..pos_newline[3]);

        if likely_unlikely::unlikely(hdr.get(0) != Some(&b'@')) {
            let hdr_start = 0usize;
            let hdr_end = pos_newline[0];
            let context_start = hdr_start.saturating_sub(512);
            let context_end = (hdr_end + 512).min(buf_record.len());

            panic!(
                "Invalid FASTQ header: expected '@', got {:?}\n\
                Header range: {}..{}\n\
                Header content: {:?}\n\
                Context (512 bytes around, {}..{}): {:?}\n\
                Full record: {:?}",
                hdr.get(0).map(|&b| b as char),
                hdr_start, hdr_end,
                String::from_utf8_lossy(hdr),
                context_start, context_end,
                String::from_utf8_lossy(&buf_record[context_start..context_end]),
                String::from_utf8_lossy(buf_record),
            );
        }
        if likely_unlikely::unlikely(sep.get(0) != Some(&b'+')) {
            let sep_start = pos_newline[1] + 1;
            let sep_end = pos_newline[2];
            let context_start = sep_start.saturating_sub(512);
            let context_end = (sep_end + 512).min(buf_record.len());

            panic!(
                "Invalid FASTQ separator: expected '+', got {:?}\n\
                Separator range: {}..{}\n\
                Separator content: {:?}\n\
                Context (512 bytes around, {}..{}): {:?}\n\
                Full record: {:?}",
                sep.get(0).map(|&b| b as char),
                sep_start, sep_end,
                String::from_utf8_lossy(sep),
                context_start, context_end,
                String::from_utf8_lossy(&buf_record[context_start..context_end]),
                String::from_utf8_lossy(buf_record),
            );
        }
        if likely_unlikely::unlikely(seq.len() != qal.len()) {
            let seq_start = pos_newline[0] + 1;
            let seq_end = pos_newline[1];
            let qal_start = pos_newline[2] + 1;
            let qal_end = pos_newline[3];

            let seq_context_start = seq_start.saturating_sub(512);
            let seq_context_end = (seq_end + 512).min(buf_record.len());
            let qal_context_start = qal_start.saturating_sub(512);
            let qal_context_end = (qal_end + 512).min(buf_record.len());

            panic!(
                "Sequence and quality length mismatch: {} != {}\n\
                Sequence range: {}..{}\n\
                Quality range: {}..{}\n\
                Sequence content: {:?}\n\
                Quality content: {:?}\n\
                Sequence context (512 bytes around, {}..{}): {:?}\n\
                Quality context (512 bytes around, {}..{}): {:?}",
                seq.len(), qal.len(),
                seq_start, seq_end,
                qal_start, qal_end,
                String::from_utf8_lossy(seq),
                String::from_utf8_lossy(qal),
                seq_context_start, seq_context_end,
                String::from_utf8_lossy(&buf_record[seq_context_start..seq_context_end]),
                qal_context_start, qal_context_end,
                String::from_utf8_lossy(&buf_record[qal_context_start..qal_context_end])
            );
        }

        // SAFETY: transmute slices to static lifetime kept alive by ArenaView refcount
        let static_id: &'static [u8] = unsafe { std::mem::transmute(hdr) };
        let static_seq: &'static [u8] = unsafe { std::mem::transmute(seq) };
        let static_qal: &'static [u8] = unsafe { std::mem::transmute(qal) };

        Self {
            id: static_id,
            sequence: static_seq,
            quality: static_qal,
            arena_backing: smallvec::smallvec![arena_view],
        }
    }
}

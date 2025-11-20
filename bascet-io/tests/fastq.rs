use std::{ptr::NonNull, sync::atomic::AtomicU64};

use bascet_core::*;
use bascet_io::{bgzf::BGZFDecoder, FASTQRecordParser};
use bounded_integer::BoundedU64;
use bytesize::ByteSize;

#[derive(Composite)]
#[attrs(Id, Read, Quality, RefCount)]
struct FASTQCell {
    id: &'static [u8],
    read: &'static [u8],
    quality: &'static [u8],
    ref_count: UnsafePtr<Arena<u8>>,
}

impl Default for FASTQCell {
    fn default() -> Self {
        Self {
            id: Default::default(),
            read: Default::default(),
            quality: Default::default(),
            ref_count: unsafe { UnsafePtr::new_unchecked(NonNull::dangling().as_ptr()) },
        }
    }
}

impl Drop for FASTQCell {
    fn drop(&mut self) {
        unsafe { self.ref_count.as_mut().dec_ref() };
    }
}

#[test]
fn test_stream_bgzf_fastq() {
    let decoder = BGZFDecoder::builder()
        .path("../data/P32705_1002_S1_L002_R1_001.fastq.gz")
        .sizeof_buffer(ByteSize::gib(32))
        .num_threads(BoundedU64::const_new::<16>())
        .build()
        .unwrap();
    let parser = FASTQRecordParser::new();
    let mut stream = Stream::new(decoder, parser);

    let mut i = 0;
    while let Ok(Some(cell)) = stream.next::<FASTQCell>() {
        i += 1;
        if i % 1_000_000 == 0 {
            println!("{:?}M Records Parsed", i / 1_000_000);
            // println!("{:?}", String::from_utf8_lossy(cell.get_ref::<Id>()));
            // println!("{:?}", String::from_utf8_lossy(cell.get_ref::<Read>()));
            // println!("{:?}", String::from_utf8_lossy(cell.get_ref::<Quality>()));
        }
    }
    dbg!("end");
}

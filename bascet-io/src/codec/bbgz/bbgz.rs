use std::io::Write;

use bounded_integer::{BoundedI32, BoundedU64};
use libdeflater::CompressionLvl;
use threadpool::ThreadPool;


pub type Compression = BoundedI32<0, 12>;
pub struct BBGZ<W> {
    pub(crate) inner: W,
    pub(crate) inner_threadpool: ThreadPool,

    pub(crate) inner_compression_level: CompressionLvl
}

#[bon::bon]
impl<W> BBGZ<W> {
    #[builder]
    pub fn new(
        with_write: W,
        #[builder(default = BoundedU64::const_new::<1>())] countof_threads: BoundedU64<1, { u64::MAX }>,

        // FIXME:   itd be better to get these values from libdeflater consts (MIN_COMPRESSION_LVL, MAX_COMPRESSION_LVL) but this is currently impossible
        // NOTE:    6 is the default compression level
        #[builder(default = Compression::const_new::<6>())] compression: Compression,
    ) -> Result<Self, ()> 
    where 
        W: Write {

        Err(())
    }
}
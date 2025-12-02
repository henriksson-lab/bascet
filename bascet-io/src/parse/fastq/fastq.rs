pub struct FASTQ {
    pub(crate) inner_cursor: usize,
}

#[bon::bon]
impl FASTQ {
    #[builder]
    pub fn new() -> Result<Self, ()> {
        Ok(FASTQ { inner_cursor: 0 })
    }
}

#[derive(Debug, Clone)]
pub struct ReadPair<'a> {
    pub r1: &'a [u8],
    pub r2: &'a [u8],
    pub q1: &'a [u8],
    pub q2: &'a [u8],
    pub umi: &'a [u8],
}

impl<'a> std::fmt::Display for ReadPair<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "({}, {}, {})",
            String::from_utf8_lossy(self.r1),
            String::from_utf8_lossy(self.r2),
            String::from_utf8_lossy(self.umi)
        )
    }
}

#[derive(Debug, Clone)]
pub struct ReadPair {
    pub r1: Vec<u8>,
    pub r2: Vec<u8>,
    pub q1: Vec<u8>,
    pub q2: Vec<u8>,
    pub umi: Vec<u8>,
}

impl std::fmt::Display for ReadPair {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "({}, {}, {})",
            String::from_utf8_lossy(self.r1.as_slice()),
            String::from_utf8_lossy(self.r2.as_slice()),
            String::from_utf8_lossy(self.umi.as_slice())
        )
    }
}

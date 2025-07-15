use crate::{
    common::{self, ReadPair},
    io::TIRP,
};

pub fn parse_readpair(buf_record: &[u8]) -> Result<ReadPair, TIRP::Error> {
    let parts: Vec<&[u8]> = buf_record.split(|&b| b == common::U8_CHAR_TAB).collect();
    let id = parts[0];
    let r1 = parts[3];
    let r2 = parts[4];
    let q1 = parts[5];
    let q2 = parts[6];
    let umi = parts[7];

    if r1.len() != q1.len() {
        return Err(TIRP::Error::ParseError {
            context: "readpair".into(),
            msg: Some("r1 and q1 are of different length".into()),
        });
    }
    if r2.len() != q2.len() {
        return Err(TIRP::Error::ParseError {
            context: "readpair".into(),
            msg: Some("r2 and q2 are of different length".into()),
        });
    }

    Ok(ReadPair {
        r1,
        r2,
        q1,
        q2,
        umi,
    })
}

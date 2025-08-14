use crate::{
    common::{self, ReadPair},
};

#[inline]
pub fn parse_readpair(buf_record: &[u8]) -> Result<(&[u8], ReadPair), crate::runtime::Error> {
    let mut tab_iter = memchr::memchr_iter(common::U8_CHAR_TAB, buf_record);

    let tabs: [usize; 7] = [
        tab_iter
            .next()
            .ok_or_else(|| crate::runtime::Error::parse_error("readpair", Some("missing tab 0")))?,
        tab_iter
            .next()
            .ok_or_else(|| crate::runtime::Error::parse_error("readpair", Some("missing tab 1")))?,
        tab_iter
            .next()
            .ok_or_else(|| crate::runtime::Error::parse_error("readpair", Some("missing tab 2")))?,
        tab_iter
            .next()
            .ok_or_else(|| crate::runtime::Error::parse_error("readpair", Some("missing tab 3")))?,
        tab_iter
            .next()
            .ok_or_else(|| crate::runtime::Error::parse_error("readpair", Some("missing tab 4")))?,
        tab_iter
            .next()
            .ok_or_else(|| crate::runtime::Error::parse_error("readpair", Some("missing tab 5")))?,
        tab_iter
            .next()
            .ok_or_else(|| crate::runtime::Error::parse_error("readpair", Some("missing tab 6")))?,
    ];

    let id = &buf_record[0..tabs[0]];
    let r1 = &buf_record[tabs[2] + 1..tabs[3]];
    let r2 = &buf_record[tabs[3] + 1..tabs[4]];
    let q1 = &buf_record[tabs[4] + 1..tabs[5]];
    let q2 = &buf_record[tabs[5] + 1..tabs[6]];
    let umi = &buf_record[tabs[6] + 1..];

    if r1.len() != q1.len() || r2.len() != q2.len() {
        return Err(crate::runtime::Error::parse_error(
            "readpair",
            Some("r1/q1 or r2/q2 length mismatch"),
        ));
    }

    Ok((
        id,
        ReadPair {
            r1,
            r2,
            q1,
            q2,
            umi,
        },
    ))
}

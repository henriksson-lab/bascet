use crate::{
    common::{self, ReadPair},
    io::format,
};

#[inline]
pub fn parse_readpair(buf_record: &[u8]) -> Result<(&[u8], ReadPair), format::Error> {
    let mut tab_iter = memchr::memchr_iter(common::U8_CHAR_TAB, buf_record);

    let tabs: [usize; 7] = [
        tab_iter.next().ok_or_else(|| format::Error::ParseError {
            context: "readpair".into(),
            msg: Some("missing tab 0".into()),
        })?,
        tab_iter.next().ok_or_else(|| format::Error::ParseError {
            context: "readpair".into(),
            msg: Some("missing tab 1".into()),
        })?,
        tab_iter.next().ok_or_else(|| format::Error::ParseError {
            context: "readpair".into(),
            msg: Some("missing tab 2".into()),
        })?,
        tab_iter.next().ok_or_else(|| format::Error::ParseError {
            context: "readpair".into(),
            msg: Some("missing tab 3".into()),
        })?,
        tab_iter.next().ok_or_else(|| format::Error::ParseError {
            context: "readpair".into(),
            msg: Some("missing tab 4".into()),
        })?,
        tab_iter.next().ok_or_else(|| format::Error::ParseError {
            context: "readpair".into(),
            msg: Some("missing tab 5".into()),
        })?,
        tab_iter.next().ok_or_else(|| format::Error::ParseError {
            context: "readpair".into(),
            msg: Some("missing tab 6".into()),
        })?,
    ];

    let id = &buf_record[0..tabs[0]];
    let r1 = &buf_record[tabs[2] + 1..tabs[3]];
    let r2 = &buf_record[tabs[3] + 1..tabs[4]];
    let q1 = &buf_record[tabs[4] + 1..tabs[5]];
    let q2 = &buf_record[tabs[5] + 1..tabs[6]];
    let umi = &buf_record[tabs[6] + 1..];

    if r1.len() != q1.len() || r2.len() != q2.len() {
        return Err(format::Error::ParseError {
            context: "readpair".into(),
            msg: Some("length mismatch".into()),
        });
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

use crate::{common, runtime};

#[inline(always)]
pub fn parse_record(buf_record: &[u8]) -> Result<(&[u8], common::ReadPair), crate::runtime::Error> {
    let mut column_iter = memchr::memchr_iter(common::U8_CHAR_TAB, buf_record);

    let columns: [usize; 7] = [
        column_iter
            .next()
            .ok_or_else(|| runtime::Error::parse_error("record", Some("malformed record")))?,
        column_iter
            .next()
            .ok_or_else(|| runtime::Error::parse_error("record", Some("malformed record")))?,
        column_iter
            .next()
            .ok_or_else(|| runtime::Error::parse_error("record", Some("malformed record")))?,
        column_iter
            .next()
            .ok_or_else(|| runtime::Error::parse_error("record", Some("malformed record")))?,
        column_iter
            .next()
            .ok_or_else(|| runtime::Error::parse_error("record", Some("malformed record")))?,
        column_iter
            .next()
            .ok_or_else(|| runtime::Error::parse_error("record", Some("malformed record")))?,
        column_iter
            .next()
            .ok_or_else(|| runtime::Error::parse_error("record", Some("malformed record")))?,
    ];

    let id = &buf_record[0..columns[0]];
    let r1 = &buf_record[columns[2] + 1..columns[3]];
    let r2 = &buf_record[columns[3] + 1..columns[4]];
    let q1 = &buf_record[columns[4] + 1..columns[5]];
    let q2 = &buf_record[columns[5] + 1..columns[6]];
    let umi = &buf_record[columns[6] + 1..];

    if r1.len() != q1.len() || r2.len() != q2.len() {
        return Err(crate::runtime::Error::parse_error(
            "record",
            Some("r1/q1 or r2/q2 length mismatch"),
        ));
    }

    Ok((
        id,
        common::ReadPair {
            r1,
            r2,
            q1,
            q2,
            umi,
        },
    ))
}

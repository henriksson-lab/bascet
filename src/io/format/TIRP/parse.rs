use crate::{
    common::{self, ReadPair},
    io::{format, tirp},
};

pub fn parse_readpair(buf_record: &[u8]) -> Result<(&[u8], ReadPair), format::Error> {
    let mut start = 0;
    let mut tab_iter = memchr::memchr_iter(common::U8_CHAR_TAB, buf_record);

    let tab0 = tab_iter.next().unwrap();
    let id = &buf_record[0..tab0];

    // Skip to tab 2 (we need parts[3])
    let _tab1 = tab_iter.next().unwrap();
    let _tab2 = tab_iter.next().unwrap();
    let tab3 = tab_iter.next().unwrap();
    let r1 = &buf_record[_tab2 + 1..tab3];

    let tab4 = tab_iter.next().unwrap();
    let r2 = &buf_record[tab3 + 1..tab4];

    let tab5 = tab_iter.next().unwrap();
    let q1 = &buf_record[tab4 + 1..tab5];

    let tab6 = tab_iter.next().unwrap();
    let q2 = &buf_record[tab5 + 1..tab6];

    let umi = &buf_record[tab6 + 1..];

    if r1.len() != q1.len() {
        return Err(format::Error::ParseError {
            context: "readpair".into(),
            msg: Some("r1 and q1 are of different length".into()),
        });
    }
    if r2.len() != q2.len() {
        return Err(format::Error::ParseError {
            context: "readpair".into(),
            msg: Some("r2 and q2 are of different length".into()),
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

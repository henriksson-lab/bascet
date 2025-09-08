use crate::{common, runtime};

#[inline(always)]
pub fn parse_record<'record>(
    hdr: &'record [u8],
    seq: &'record [u8],
    sep: &'record [u8],
    qal: &'record [u8],
) -> Result<(&'record [u8], common::ReadPair<'record>), crate::runtime::Error> {
    if hdr.is_empty() || hdr[0] != b'@' {
        return Err(runtime::Error::parse_error(
            "record",
            Some("invalid header"),
        ));
    }

    if sep.is_empty() || sep[0] != b'+' {
        return Err(runtime::Error::parse_error(
            "record",
            Some("invalid plus line"),
        ));
    }

    if seq.len() != qal.len() {
        return Err(runtime::Error::parse_error(
            "record",
            Some("sequence/quality length mismatch"),
        ));
    }

    let id = &hdr[1..]; // Remove @ prefix

    Ok((
        id,
        common::ReadPair {
            r1: seq,
            r2: &[],
            q1: qal,
            q2: &[],
            umi: &[],
        },
    ))
}

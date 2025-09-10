use crate::{common, runtime};

#[inline(always)]
pub fn parse_record<'record>(
    hdr: &'record [u8],
    seq: &'record [u8],
    sep: &'record [u8],
    qal: &'record [u8],
) -> Result<(&'record [u8], common::ReadPair<'record>), crate::runtime::Error> {
    println!("PARSE_RECORD: hdr.len()={}, seq.len()={}, sep.len()={}, qal.len()={}", 
        hdr.len(), seq.len(), sep.len(), qal.len());

    println!("About to check header");
    if hdr.is_empty() || hdr[0] != b'@' {
        println!("Header validation failed");
        return Err(runtime::Error::parse_error(
            "record",
            Some("invalid header"),
        ));
    }
    println!("Header validation passed");

    println!("About to check separator");
    if sep.is_empty() || sep[0] != b'+' {
        println!("Separator validation failed");
        return Err(runtime::Error::parse_error(
            "record",
            Some("invalid plus line"),
        ));
    }
    println!("Separator validation passed");

    println!("About to check lengths: seq={}, qal={}", seq.len(), qal.len());
    if seq.len() != qal.len() {
        println!("Length mismatch detected");
        return Ok((hdr, common::ReadPair { r1: &[], r2: &[], q1: &[], q2: &[], umi: &[] }));
    }
    println!("Length validation passed");

    // Remove @
    let id = &hdr[1..];

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

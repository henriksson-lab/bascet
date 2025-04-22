
//inspiration
//https://github.com/sstadick/rumi/blob/master/src/lib.rs

//suggests a BK-tree https://en.wikipedia.org/wiki/BK-tree  (not in above code)


// use basebits::{hamming_dist_none, BaseBits};  interesting lib!
// https://github.com/sstadick/basebits
// same guy
// https://www.biorxiv.org/content/10.1101/648683v1.full
// constant time hamming distance
// at biorad https://github.com/sstadick  


// https://peerj.com/articles/8275/

// could use this also to scan for barcodes if errors are present

// there are several bk-tree implementations in rust:
// https://www.google.com/search?q=rust+bk-tree&oq=rust+bk-tree&gs_lcrp=EgZjaHJvbWUyBggAEEUYOTIJCAEQABgNGIAEMggIAhAAGA0YHjIICAMQABgNGB4yCggEEAAYCBgNGB4yCggFEAAYCBgNGB4yDQgGEAAYhgMYgAQYigUyDQgHEAAYhgMYgAQYigUyDQgIEAAYhgMYgAQYigXSAQgxOTkxajBqN6gCALACAA&sourceid=chrome&ie=UTF-8

// https://github.com/sstadick/rumi  -- can use. wants htslib Record; keep all the way to the end?


use std::collections::HashSet;


/// Given a list of sequenced UMIs, figure out how many 
/// 
/// For now: dummy, assuming no errors in sequencing
pub fn dedup_umi(umis: &Vec<Vec<u8>>) -> usize {

    let mut unique_umi = HashSet::new();
    for umi in umis {
        unique_umi.insert(umi);
    }

    unique_umi.len()
}
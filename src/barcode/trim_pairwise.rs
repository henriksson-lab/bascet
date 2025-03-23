



/*

Need to find and trim from truseq adapter at minimum. then, opposite side len cannot be longer than bc side

*/

/*
https://github.com/Daniel-Liu-c0deb0t/block-aligner
*/

/*
https://www.biorxiv.org/content/10.1101/082214v2
*/

/*
 * 
 * FASTP is MIT-license
 * 
 * How FASTP does paired trimming. this is not enabled by default! so likely why the code can be quite
 * slow
 * https://github.com/OpenGene/fastp/blob/master/src/overlapanalysis.cpp
 * 
 * 
 */

 /*
 base correction for PE data
fastp perform overlap analysis for PE data, which try to find an overlap of each pair of reads. If an proper overlap is found, it can correct mismatched base pairs in overlapped regions of paired end reads, if one base is with high quality while the other is with ultra low quality. If a base is corrected, the quality of its paired base will be assigned to it so that they will share the same quality.  

This function is not enabled by default, specify -c or --correction to enable it. This function is based on overlapping detection, which has adjustable parameters overlap_len_require (default 30), overlap_diff_limit (default 5) and overlap_diff_percent_limit (default 20%). Please note that the reads should meet these three conditions simultaneously.
  */




/// Implementation is taken from https://doi.org/10.1101/082214
/// This function handles ATCG
pub fn revcomp(seq: &[u8]) -> Vec<u8> {
    seq.iter()
        .rev()
        .map(|c| if c & 2 != 0 { c ^ 4 } else { c ^ 21 })
        .collect()
}


/// Using the trick from https://doi.org/10.1101/082214 , extended to handle N
/// This function handles ATCGN
pub fn revcomp_n(seq: &[u8]) -> Vec<u8> {
    seq.iter()
        .rev()
        .map(|c| if c & 2 != 0 {
            if c & 8 != 0 {
                //N
                b'N'
            } else {
                //G or C
                c ^ 4 
            }
        } else { 
            //A or T
            c ^ 21 
        })
        .collect()
}

// C and G have their bit 2 set, whereas A and T do not
// C hex 43 bin 01000011
// G hex 47 bin 01000111
// A hex 41 bin 01000001
// T hex 54 bin 01010100
// N hex 4e bin 01001110  //4th bit is set to 1 ; 2nd bit is 1


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_revcomp() {
        let seq = b"ATGCTTCCAGAA";
        let actual = revcomp(seq);
        let expected = b"TTCTGGAAGCAT";
        assert_eq!(actual, expected)
    }

    #[test]
    fn test_revcomp_n() {

        let seq = b"ATGCTTCCAGNAA";
        let actual = revcomp_n(seq);
        let expected = b"TTNCTGGAAGCAT";
        assert_eq!(actual, expected)  
    }
}
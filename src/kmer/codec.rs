use bitfield_struct::bitfield;
use rand::{
    distributions::{Distribution, Uniform},
    rngs::SmallRng, Rng,
};

const NT_LOOKUP: [u8; 256] = {
    let mut table = [0u8; 256];
    table[b'A' as usize] = 0b00;
    table[b'T' as usize] = 0b01;
    table[b'G' as usize] = 0b10;
    table[b'C' as usize] = 0b11;
    table
};
// TODO: Improve lookup via this scheme
// Lookup table for 4-nucleotide encoding
// A=00, T=01, G=10, C=11
const NT4_DIMSIZE: usize = 20;

// First index:  nt1 - b'A'
// Second index: (nt2 - b'A') * NT4_DIMSIZE
// Third index:  (nt3 - b'A') * NT4_DIMSIZE * NT4_DIMSIZE
// Fourth index: (nt4 - b'A') * NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE

const NT4_LOOKUP: [u8; NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE] = {
    let mut table = [0u8; NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE * NT4_DIMSIZE];
    // A block
    // AA combinations
    table[0] = 0b00000000; // AAAA
    table[152000] = 0b00000001; // AAAT
    table[48000] = 0b00000010; // AAAG
    table[16000] = 0b00000011; // AAAC
    table[7600] = 0b00000100; // AATA
    table[159600] = 0b00000101; // AATT
    table[55600] = 0b00000110; // AATG
    table[23600] = 0b00000111; // AATC
    table[2400] = 0b00001000; // AAGA
    table[154400] = 0b00001001; // AAGT
    table[50400] = 0b00001010; // AAGG
    table[18400] = 0b00001011; // AAGC
    table[800] = 0b00001100; // AACA
    table[152800] = 0b00001101; // AACT
    table[48800] = 0b00001110; // AACG
    table[16800] = 0b00001111; // AACC

    // AT combinations
    table[380] = 0b00010000; // ATAA
    table[152380] = 0b00010001; // ATAT
    table[48380] = 0b00010010; // ATAG
    table[16380] = 0b00010011; // ATAC
    table[7980] = 0b00010100; // ATTA
    table[159980] = 0b00010101; // ATTT
    table[55980] = 0b00010110; // ATTG
    table[23980] = 0b00010111; // ATTC
    table[2780] = 0b00011000; // ATGA
    table[154780] = 0b00011001; // ATGT
    table[50780] = 0b00011010; // ATGG
    table[18780] = 0b00011011; // ATGC
    table[1180] = 0b00011100; // ATCA
    table[153180] = 0b00011101; // ATCT
    table[49180] = 0b00011110; // ATCG
    table[17180] = 0b00011111; // ATCC

    // AG combinations
    table[120] = 0b00100000; // AGAA
    table[152120] = 0b00100001; // AGAT
    table[48120] = 0b00100010; // AGAG
    table[16120] = 0b00100011; // AGAC
    table[7720] = 0b00100100; // AGTA
    table[159720] = 0b00100101; // AGTT
    table[55720] = 0b00100110; // AGTG
    table[23720] = 0b00100111; // AGTC
    table[2520] = 0b00101000; // AGGA
    table[154520] = 0b00101001; // AGGT
    table[50520] = 0b00101010; // AGGG
    table[18520] = 0b00101011; // AGGC
    table[920] = 0b00101100; // AGCA
    table[152920] = 0b00101101; // AGCT
    table[48920] = 0b00101110; // AGCG
    table[16920] = 0b00101111; // AGCC

    // AC combinations
    table[40] = 0b00110000; // ACAA
    table[152040] = 0b00110001; // ACAT
    table[48040] = 0b00110010; // ACAG
    table[16040] = 0b00110011; // ACAC
    table[7640] = 0b00110100; // ACTA
    table[159640] = 0b00110101; // ACTT
    table[55640] = 0b00110110; // ACTG
    table[23640] = 0b00110111; // ACTC
    table[2440] = 0b00111000; // ACGA
    table[154440] = 0b00111001; // ACGT
    table[50440] = 0b00111010; // ACGG
    table[18440] = 0b00111011; // ACGC
    table[840] = 0b00111100; // ACCA
    table[152840] = 0b00111101; // ACCT
    table[48840] = 0b00111110; // ACCG
    table[16840] = 0b00111111; // ACCC

    // T block
    // TA combinations
    table[19] = 0b01000000; // TAAA
    table[152019] = 0b01000001; // TAAT
    table[48019] = 0b01000010; // TAAG
    table[16019] = 0b01000011; // TAAC
    table[7619] = 0b01000100; // TATA
    table[159619] = 0b01000101; // TATT
    table[55619] = 0b01000110; // TATG
    table[23619] = 0b01000111; // TATC
    table[2419] = 0b01001000; // TAGA
    table[154419] = 0b01001001; // TAGT
    table[50419] = 0b01001010; // TAGG
    table[18419] = 0b01001011; // TAGC
    table[819] = 0b01001100; // TACA
    table[152819] = 0b01001101; // TACT
    table[48819] = 0b01001110; // TACG
    table[16819] = 0b01001111; // TACC

    // TT combinations
    table[399] = 0b01010000; // TTAA
    table[152399] = 0b01010001; // TTAT
    table[48399] = 0b01010010; // TTAG
    table[16399] = 0b01010011; // TTAC
    table[7999] = 0b01010100; // TTTA
    table[159999] = 0b01010101; // TTTT
    table[55999] = 0b01010110; // TTTG
    table[23999] = 0b01010111; // TTTC
    table[2799] = 0b01011000; // TTGA
    table[154799] = 0b01011001; // TTGT
    table[50799] = 0b01011010; // TTGG
    table[18799] = 0b01011011; // TTGC
    table[1199] = 0b01011100; // TTCA
    table[153199] = 0b01011101; // TTCT
    table[49199] = 0b01011110; // TTCG
    table[17199] = 0b01011111; // TTCC

    // TG combinations
    table[139] = 0b01100000; // TGAA
    table[152139] = 0b01100001; // TGAT
    table[48139] = 0b01100010; // TGAG
    table[16139] = 0b01100011; // TGAC
    table[7739] = 0b01100100; // TGTA
    table[159739] = 0b01100101; // TGTT
    table[55739] = 0b01100110; // TGTG
    table[23739] = 0b01100111; // TGTC
    table[2539] = 0b01101000; // TGGA
    table[154539] = 0b01101001; // TGGT
    table[50539] = 0b01101010; // TGGG
    table[18539] = 0b01101011; // TGGC
    table[939] = 0b01101100; // TGCA
    table[152939] = 0b01101101; // TGCT
    table[48939] = 0b01101110; // TGCG
    table[16939] = 0b01101111; // TGCC

    // TC combinations
    table[59] = 0b01110000; // TCAA
    table[152059] = 0b01110001; // TCAT
    table[48059] = 0b01110010; // TCAG
    table[16059] = 0b01110011; // TCAC
    table[7659] = 0b01110100; // TCTA
    table[159659] = 0b01110101; // TCTT
    table[55659] = 0b01110110; // TCTG
    table[23659] = 0b01110111; // TCTC
    table[2459] = 0b01111000; // TCGA
    table[154459] = 0b01111001; // TCGT
    table[50459] = 0b01111010; // TCGG
    table[18459] = 0b01111011; // TCGC
    table[859] = 0b01111100; // TCCA
    table[152859] = 0b01111101; // TCCT
    table[48859] = 0b01111110; // TCCG
    table[16859] = 0b01111111; // TCCC

    // G block
    // GA combinations
    table[6] = 0b10000000; // GAAA
    table[152006] = 0b10000001; // GAAT
    table[48006] = 0b10000010; // GAAG
    table[16006] = 0b10000011; // GAAC
    table[7606] = 0b10000100; // GATA
    table[159606] = 0b10000101; // GATT
    table[55606] = 0b10000110; // GATG
    table[23606] = 0b10000111; // GATC
    table[2406] = 0b10001000; // GAGA
    table[154406] = 0b10001001; // GAGT
    table[50406] = 0b10001010; // GAGG
    table[18406] = 0b10001011; // GAGC
    table[806] = 0b10001100; // GACA
    table[152806] = 0b10001101; // GACT
    table[48806] = 0b10001110; // GACG
    table[16806] = 0b10001111; // GACC

    // GT combinations
    table[386] = 0b10010000; // GTAA
    table[152386] = 0b10010001; // GTAT
    table[48386] = 0b10010010; // GTAG
    table[16386] = 0b10010011; // GTAC
    table[7986] = 0b10010100; // GTTA
    table[159986] = 0b10010101; // GTTT
    table[55986] = 0b10010110; // GTTG
    table[23986] = 0b10010111; // GTTC
    table[2786] = 0b10011000; // GTGA
    table[154786] = 0b10011001; // GTGT
    table[50786] = 0b10011010; // GTGG
    table[18786] = 0b10011011; // GTGC
    table[1186] = 0b10011100; // GTCA
    table[153186] = 0b10011101; // GTCT
    table[49186] = 0b10011110; // GTCG
    table[17186] = 0b10011111; // GTCC

    // GG combinations
    table[126] = 0b10100000; // GGAA
    table[152126] = 0b10100001; // GGAT
    table[48126] = 0b10100010; // GGAG
    table[16126] = 0b10100011; // GGAC
    table[7726] = 0b10100100; // GGTA
    table[159726] = 0b10100101; // GGTT
    table[55726] = 0b10100110; // GGTG
    table[23726] = 0b10100111; // GGTC
    table[2526] = 0b10101000; // GGGA
    table[154526] = 0b10101001; // GGGT
    table[50526] = 0b10101010; // GGGG
    table[18526] = 0b10101011; // GGGC
    table[926] = 0b10101100; // GGCA
    table[152926] = 0b10101101; // GGCT
    table[48926] = 0b10101110; // GGCG
    table[16926] = 0b10101111; // GGCC

    // GC combinations
    table[46] = 0b10110000; // GCAA
    table[152046] = 0b10110001; // GCAT
    table[48046] = 0b10110010; // GCAG
    table[16046] = 0b10110011; // GCAC
    table[7646] = 0b10110100; // GCTA
    table[159646] = 0b10110101; // GCTT
    table[55646] = 0b10110110; // GCTG
    table[23646] = 0b10110111; // GCTC
    table[2446] = 0b10111000; // GCGA
    table[154446] = 0b10111001; // GCGT
    table[50446] = 0b10111010; // GCGG
    table[18446] = 0b10111011; // GCGC
    table[846] = 0b10111100; // GCCA
    table[152846] = 0b10111101; // GCCT
    table[48846] = 0b10111110; // GCCG
    table[16846] = 0b10111111; // GCCC

    // C block
    // CA combinations
    table[2] = 0b11000000; // CAAA
    table[152002] = 0b11000001; // CAAT
    table[48002] = 0b11000010; // CAAG
    table[16002] = 0b11000011; // CAAC
    table[7602] = 0b11000100; // CATA
    table[159602] = 0b11000101; // CATT
    table[55602] = 0b11000110; // CATG
    table[23602] = 0b11000111; // CATC
    table[2402] = 0b11001000; // CAGA
    table[154402] = 0b11001001; // CAGT
    table[50402] = 0b11001010; // CAGG
    table[18402] = 0b11001011; // CAGC
    table[802] = 0b11001100; // CACA
    table[152802] = 0b11001101; // CACT
    table[48802] = 0b11001110; // CACG
    table[16802] = 0b11001111; // CACC

    // CT combinations
    table[382] = 0b11010000; // CTAA
    table[152382] = 0b11010001; // CTAT
    table[48382] = 0b11010010; // CTAG
    table[16382] = 0b11010011; // CTAC
    table[7982] = 0b11010100; // CTTA
    table[159982] = 0b11010101; // CTTT
    table[55982] = 0b11010110; // CTTG
    table[23982] = 0b11010111; // CTTC
    table[2782] = 0b11011000; // CTGA
    table[154782] = 0b11011001; // CTGT
    table[50782] = 0b11011010; // CTGG
    table[18782] = 0b11011011; // CTGC
    table[1182] = 0b11011100; // CTCA
    table[153182] = 0b11011101; // CTCT
    table[49182] = 0b11011110; // CTCG
    table[17182] = 0b11011111; // CTCC

    // CG combinations
    table[122] = 0b11100000; // CGAA
    table[152122] = 0b11100001; // CGAT
    table[48122] = 0b11100010; // CGAG
    table[16122] = 0b11100011; // CGAC
    table[7722] = 0b11100100; // CGTA
    table[159722] = 0b11100101; // CGTT
    table[55722] = 0b11100110; // CGTG
    table[23722] = 0b11100111; // CGTC
    table[2522] = 0b11101000; // CGGA
    table[154522] = 0b11101001; // CGGT
    table[50522] = 0b11101010; // CGGG
    table[18522] = 0b11101011; // CGGC
    table[922] = 0b11101100; // CGCA
    table[152922] = 0b11101101; // CGCT
    table[48922] = 0b11101110; // CGCG
    table[16922] = 0b11101111; // CGCC

    // CC combinations
    table[42] = 0b11110000; // CCAA
    table[152042] = 0b11110001; // CCAT
    table[48042] = 0b11110010; // CCAG
    table[16042] = 0b11110011; // CCAC
    table[7642] = 0b11110100; // CCTA
    table[159642] = 0b11110101; // CCTT
    table[55642] = 0b11110110; // CCTG
    table[23642] = 0b11110111; // CCTC
    table[2442] = 0b11111000; // CCGA
    table[154442] = 0b11111001; // CCGT
    table[50442] = 0b11111010; // CCGG
    table[18442] = 0b11111011; // CCGC
    table[842] = 0b11111100; // CCCA
    table[152842] = 0b11111101; // CCCT
    table[48842] = 0b11111110; // CCCG
    table[16842] = 0b11111111; // CCCC

    table
};
const NT_REVERSE: [u8; 4] = [b'A', b'T', b'G', b'C'];

pub struct Codec<const K: usize>;
impl<const K: usize> Codec<K> {
    const KMER_SIZE: usize = K;

    pub const fn new() -> Self {
        Codec
    }

    #[inline(always)]
    pub unsafe fn encode(
        &self,
        bytes: &[u8],
        count: u32,
        rng: &mut impl Rng,
        range: Uniform<u16>,
    ) -> EncodedKMER {
        let chunk_size: usize = 4;
        let kmer_size = Self::KMER_SIZE as usize;
        let full_chunks = kmer_size / chunk_size;
        let remainder = kmer_size % chunk_size;

        let mut encoded: u128 = 0;
        let ptr = bytes.as_ptr();

        // Process chunks of 4 nucleotides
        for i in 0..full_chunks {
            let chunk_ptr = ptr.add(i * chunk_size);
            let idx = unsafe {
                (*chunk_ptr.offset(0) - b'A') as usize
                    + ((*chunk_ptr.offset(1) - b'A') as usize * NT4_DIMSIZE)
                    + ((*chunk_ptr.offset(2) - b'A') as usize * NT4_DIMSIZE * NT4_DIMSIZE)
                    + ((*chunk_ptr.offset(3) - b'A') as usize
                        * NT4_DIMSIZE
                        * NT4_DIMSIZE
                        * NT4_DIMSIZE)
            };
            encoded = (encoded << 8) | u128::from(NT4_LOOKUP[idx]);
        }

        // Handle remaining nucleotides
        let start = full_chunks * chunk_size;
        for i in 0..remainder {
            encoded = (encoded << 2) | u128::from(NT_LOOKUP[bytes[start + i] as usize]);
        }

        EncodedKMER::new()
            .with_kmer(encoded)
            .with_count(count as u16)
            .with_rand(range.sample(rng))
    }

    #[inline(always)]
    pub unsafe fn encode_str(
        &self,
        kmer: &str,
        count: u16,
        rng: &mut impl Rng,
        range: Uniform<u16>,
    ) -> EncodedKMER {
        let chunk_size: usize = 4;
        let kmer_size = Self::KMER_SIZE as usize;
        let full_chunks = kmer_size / chunk_size;
        let remainder = kmer_size % chunk_size;

        let bytes = kmer.as_bytes();
        let mut encoded: u128 = 0;
        let ptr = bytes.as_ptr();

        // Process chunks of 4 nucleotides
        for i in 0..full_chunks {
            let chunk_ptr = ptr.add(i * chunk_size);
            let idx = unsafe {
                (*chunk_ptr.offset(0) - b'A') as usize
                    + ((*chunk_ptr.offset(1) - b'A') as usize * NT4_DIMSIZE)
                    + ((*chunk_ptr.offset(2) - b'A') as usize * NT4_DIMSIZE * NT4_DIMSIZE)
                    + ((*chunk_ptr.offset(3) - b'A') as usize
                        * NT4_DIMSIZE
                        * NT4_DIMSIZE
                        * NT4_DIMSIZE)
            };
            encoded = (encoded << 8) | u128::from(NT4_LOOKUP[idx]);
        }

        // Handle remaining nucleotides
        let start = full_chunks * chunk_size;
        for i in 0..remainder {
            encoded = (encoded << 2) | u128::from(NT_LOOKUP[bytes[start + i] as usize]);
        }

        EncodedKMER::new()
            .with_kmer(encoded)
            .with_count(count)
            .with_rand(range.sample(rng))
    }
    
    #[inline(always)]
    pub unsafe fn decode(&self, encoded: u128) -> String {
        let mut sequence = Vec::with_capacity(Self::KMER_SIZE);
        let mut temp = EncodedKMER::from_bits(encoded).kmer();
        for _ in 0..Self::KMER_SIZE {
            let nuc = (temp & 0b11) as usize;
            sequence.push(NT_REVERSE[nuc]);
            temp >>= 2;
        }
        sequence.reverse();
        String::from_utf8_unchecked(sequence)
    }
}

#[bitfield(u128)]
pub struct EncodedKMER {
    #[bits(96)]
    pub kmer: u128,

    #[bits(16)]
    pub rand: u16,

    #[bits(16)]
    pub count: u16,
}

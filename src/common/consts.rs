// [JD] probably not necesarry as consts but I prefer reading this over literals
pub const U8_CHAR_TAB: u8 = b'\t';
pub const U8_CHAR_1: u8 = b'1';
pub const U8_CHAR_NEWLINE: u8 = b'\n';
pub const U8_CHAR_FASTA_RECORD: u8 = b'>';
pub const U8_CHAR_FASTQ_RECORD: u8 = b'@';
pub const U8_CHAR_FASTQ_SEPERATOR: u8 = b'+';

// HACK: [JD] there should be some way to determine this at compile time. No clue how though!
// pub const HUGE_PAGE_SIZE: usize = 12632 * 50;
// 4096 x 32 bytes = 128KiB. Small enough!
pub const PAGE_BUFFER_MAX_PAGES: usize = 4096;

//! Notes for the Zheng 2022 / Microbe-seq barcode parser.
//!
//! This module is intentionally not wired into `crate::barcode::mod` yet. The
//! original paper code (`01_sort_SAGs.ipynb` in
//! <https://github.com/shijiezhao/Microbe-seq>) is structurally close to
//! bascet's existing combinatorial barcode system, but we still need raw
//! R1/R2 examples and the barcode table before implementing it.
//!
//! High-level parser in the paper code:
//!
//! - Inputs are `S{sample}_R1.fastq`, `S{sample}_R2.fastq`, and
//!   `bc1andbc2.xlsx`.
//! - The barcode table has 96 BC1 sequences and 384 BC2 sequences, giving
//!   36,864 possible droplet barcodes per sample.
//! - R1 is used for barcode detection. R2 is routed by ordinal pairing after
//!   R1 determines the barcode.
//! - The paper code does not encode the droplet barcode into FASTQ read names.
//!   It reads each FASTQ header into `line_loc` and writes that header back
//!   unchanged. Barcode identity is represented by the per-barcode output file
//!   name and the `hit_map`, not by modifying the read header.
//! - The code searches R1 near offset 8 for:
//!
//!   `GAGTGATTGCTTGTGACGCCTT`
//!
//!   This is called `W1` in the paper code.
//! - It extracts:
//!
//!   `bc1 = reverse_complement(r1[..w1_start])`
//!   `bc2 = reverse_complement(r1[w1_start + 22..w1_start + 30])`
//!
//! - Each barcode part is accepted by exact match, or by a unique closest
//!   barcode with one mismatch.
//! - The combined barcode index is:
//!
//!   `bc1_index * 384 + bc2_index`
//!
//! - For paired-end NextSeq-style data, the paper code emits:
//!
//!   `trimmed_r1 = r1[w1_start + 63..]`
//!   `trimmed_r2 = r2`
//!
//! - For the NovaSeq single-end-ish path, R1 is used only for barcode
//!   detection and the code emits `R2_nova` for each barcode.
//!
//! Implementation sketch for bascet:
//!
//! - Model this as a two-part combinatorial barcode with variable BC1 length,
//!   fixed 8 bp BC2, and a fixed W1 linker anchor.
//! - Use the existing "known barcode pool plus Hamming threshold" machinery
//!   rather than a new reader design.
//! - Keep I1 as sample-demultiplexing metadata; the droplet barcode is in R1,
//!   not I1.
//! - Do not use SRA-normalized `SRR17944416_1.fastq` as a validation fixture
//!   until we confirm it still contains raw barcode-bearing R1 sequence.

# Zheng 2022 / Microbe-seq chemistry notes

This directory records the preliminary chemistry definition for the method from:

- Zheng et al. 2022, "High-throughput, single-microbe genomics with strain resolution, applied to a human gut microbiome", Science 376, eabm1483, doi:10.1126/science.abm1483.
- Example SRA experiment: SRX14101794 / SRR17944416.

Do not wire this into `GetRawChemistry` yet.

## Current evidence

The public SRA runinfo summary for `SRX14101794` describes `SRR17944416` as:

- original file: `Combined_15.fastq`
- library layout: `SINGLE`
- spots with mates: `0`
- average read length: `89`
- library strategy/source/selection: `WGS` / `GENOMIC` / `RANDOM`

However, the SRA table itself contains two biological read segments per spot.
For example, `vdb-dump SRR17944416 -R 1-5 -C READ,READ_START,READ_LEN,READ_TYPE,SPOT_GROUP`
shows `READ_TYPE: SRA_READ_TYPE_BIOLOGICAL, SRA_READ_TYPE_BIOLOGICAL`, and
`fastq-dump --split-files` writes both `SRR17944416_1.fastq` and
`SRR17944416_2.fastq`.

The article describes Microbe-seq reads as having an inline droplet barcode
sequence plus a microbial genomic insert sequence. For a 100 bp sequencing run,
raw read 1 is 45 bp and contains the barcode sequence; index 1 is 8 bp; read 2
contains microbial sequence. For a 300 bp sequencing run, raw read 1 is 150 bp:
the first 45 bp are barcode sequence, the middle bases are adapter sequence,
and the final 75 bp are microbial sequence; index 1 is 8 bp; read 2 contains
microbial sequence.

The barcode is added after Nextera-style tagmentation in droplets using
barcode beads. The final barcoding bead library has two barcode regions: one
96-way barcode and one 384-way barcode, for 36,864 possible droplet barcodes.

The `SRR17944416_1.fastq` emitted by SRA Toolkit does not visibly expose a
stable 45 bp barcode block in the first dumped reads; the sequence looks like
processed/clipped biological sequence. The SRA `SPOT_GROUP` contains values
such as `ACCGATCG`, which matches an 8 bp sample index, not the 36,864-way
droplet barcode.

## Consequence for bascet

For raw sequencer output, expect `R1`, `R2`, and `I1`, but not necessarily
`I2`: `R1` carries the 45 bp droplet barcode at its start, `I1` carries an
8 bp sample index, and `R2` carries microbial sequence. For SRA Toolkit output
from `SRR17944416`, the dumped `_1.fastq`/`_2.fastq` appear to be processed
biological read segments rather than raw reads with an exposed read-1 barcode.

This does not fit cleanly into the current `Chemistry` trait as a complete
implementation yet:

- `Chemistry::detect_barcode_and_trim` receives exactly `r1/r2` and qualities.
- The `getraw` router batches exactly two read streams.
- Single-end input is represented by a real R1 plus an empty dummy R2, which
  could be used mechanically but would hide the fact that this chemistry is
  inline-barcode WGS rather than paired-end RNA-style reads.
- The exact barcode boundaries and adapter trimming rules still need to be
  confirmed from the final supplementary methods or a larger read inspection.

## Proposed implementation path

1. Treat Zheng 2022 as a raw `R1` inline-barcode chemistry with microbial
   sequence in `R2` and possibly in the tail of `R1`.
2. Add a small read-layout abstraction before implementing the chemistry:
   `SingleInlineBarcode`, `PairedInlineBarcode`, and, later if needed,
   `IndexedBarcode`.
3. Change the debarcode worker input from hard-coded `ReadPairBatch` to a
   record carrying named read segments, or add a separate single-read
   debarcoding path.
4. Only then add a real `Zheng2022Chemistry` implementation that emits a
   trimmed genomic read and droplet barcode-derived cell id.

## Open items

- Recover the exact 45 bp barcode structure from table S8: barcode-1 length,
  barcode-2 length, and any constant sequence between/around them.
- Confirm whether the two barcode regions appear contiguous in the read or are
  separated by primer/adapter sequence.
- Confirm whether SRA Toolkit output has clipped the raw 45 bp barcode from
  `SRR17944416_1.fastq`; if yes, the public normalized FASTQs cannot be used
  directly for debarcoding without another source of barcode assignments.
- Confirm whether reads should be reverse-complement tested.
- Decide whether output should be single-read TIRP with empty R2, or whether
  TIRP/readpair storage needs a single-end representation.

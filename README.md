[![Build](https://github.com/JulianDicken/KMER-Select/actions/workflows/rust.yml/badge.svg?branch=main)](https://github.com/JulianDicken/KMER-Select/actions/workflows/rust.yml)


# Bascet (Bacterial Single CEll Toolkit)

Bascet is a complete solution for single-cell analysis, with focus on microbial analysis. It is however design to also do RNA-seq, ATAC-seq (you-name-it), agnostic from
the instrument used. It has also been designed to analyze large numbers of bulk samples in a manner analogous to single-cell analysis (i.e. with a focus on clustering and data-driven analysis).

Bascet is an advanced command-line tool aimed primarily to be used through the Zorn R library, which offers multi-node compute capability and ease of use.

TODO: link to Zorn here

## Installation

_TODO_

## Quick Usage

`bascet` consists of several subcommands:

- `bascet prepare`: demultiplex and trim fastq files and store reads in .cram, .bed, or .zip files

## Overview -- old --

Right now the tool works in four main steps:

1. Counts 31-mers in input sequences using KMC
2. Processes the k-mer counts to identify discriminative features
3. Creates feature vectors for each input sequence
4. Outputs a CSV matrix of k-mer frequencies for downstream analysis




## Technical Details -- old --

- Written in Rust with a focus on performance
- Uses a thread pool for parallel processing

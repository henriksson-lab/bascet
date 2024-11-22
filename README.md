[![Build](https://github.com/JulianDicken/KMER-Select/actions/workflows/rust.yml/badge.svg?branch=main)](https://github.com/JulianDicken/KMER-Select/actions/workflows/rust.yml)
# ROBERD (Rapid Organization of Bacterial Elements for Read-based Typing)

ROBERT is a command-line tool that reduces bacterial genome sequences into feature vectors based on k-mer frequencies.

## Overview

Right now the tool works in four main steps:

1. Counts 31-mers in input sequences using KMC
2. Processes the k-mer counts to identify discriminative features
3. Creates feature vectors for each input sequence
4. Outputs a CSV matrix of k-mer frequencies for downstream analysis

## Technical Details

- Written in Rust with a focus on performance
- Uses a thread pool for parallel processing
- Supports both FASTA and FASTQ inputs
- Outputs simple CSV format for compatibility
- Memory-efficient processing using streaming and shared memory
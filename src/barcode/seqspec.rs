/*


seqspec: https://github.com/pachterlab/seqspec




https://github.com/pachterlab/seqspec/blob/main/docs/UNIFORM.md
can technically use their parser; but better if we fully integrate it

https://academic.oup.com/bioinformatics/article/40/4/btae168/7641535

specs are downloadable from https://zenodo.org/records/13932232


yaml is not well maintained in rust but see https://github.com/sebastienrousseau/serde_yml


seqspec index -m rna -t kb -s file spec.yaml
0,0,16:0,16,28:1,0,102


seqspec file -m rna -s region -k filename spec.yaml
RNA-737K-arc-v1.txt


seqspec file -m rna -s read -f paired -k filename spec.yaml  | tr "\t\n" "  "
rna_R1_SRR18677638.fastq.gz rna_R2_SRR18677638.fastq.gz 




(base) mahogny@beagle:~/github/seqspec/examples/specs/dogmaseq-dig$ ls fastqs/
atac_R1_SRR18677642.fastq.gz  atac_R3_SRR18677642.fastq.gz     protein_R2_SRR18677644.fastq.gz  rna_R2_SRR18677638.fastq.gz  tag_R2_SRR18677640.fastq.gz
atac_R2_SRR18677642.fastq.gz  protein_R1_SRR18677644.fastq.gz  rna_R1_SRR18677638.fastq.gz      tag_R1_SRR18677640.fastq.gz



from spec, need only pick out which whitelist to use for each read. then the position of the umi. automatic download of whitelists to temp dir is possible (caching possible but may cause issues on cluster)

need to mark parts of reads that are adapters of sorts (everything but dna/rna). can then use a general pairwise alignment tool

can use a 


*/

/*

https://www.kallistobus.tools
https://www.nature.com/articles/s41587-021-00870-2
does correction using hamming distance
https://github.com/BUStools/bustools/


see also https://academic.oup.com/bioinformatics/article/35/21/4472/5487510
their correction https://github.com/BUStools/bustools/blob/master/src/bustools_correct.cpp
they split up the BC. 


10x describes cellranger hamming distance correction here:
https://www.10xgenomics.com/support/software/cell-ranger/latest/algorithms-overview/cr-gex-algorithm

at most 1 hamming distance is ok

uses phred score to pick closest whitelist entry



rust libs
https://crates.io/crates/hamming-bitwise-fast
https://emschwartz.me/unnecessary-optimization-in-rust-hamming-distances-simd-and-auto-vectorization/

*/


/* 
=======================

Splitcode: https://github.com/pachterlab/splitcode
https://github.com/pachterlab/splitcode/tree/main/src
https://splitcode.readthedocs.io/
https://academic.oup.com/bioinformatics/article/40/6/btae331/7693695?login=false

this is a completely different project; nothing to take from here

also
https://github.com/ashvardanian/SimSIMD
https://crates.io/crates/simsimd/4.3.0

and code, just one function
https://github.com/emschwartz/hamming-bitwise-fast/blob/main/src/lib.rs






*/
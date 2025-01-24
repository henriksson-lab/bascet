




/*


wrap https://github.com/single-cell-genetics/cellsnp-lite

note that it is for biallelic genomes. can we easily make it haplo?


#If SNPs are known
cellSNP -s $BAM -b $BARCODE -O $OUT_DIR -R $REGION_VCF -p 20 --minMAF 0.1 --minCOUNT 20

humans SNPs are here: https://sourceforge.net/projects/cellsnp/files/SNPlist/



# no SNPs are known
# 10x sample with cell barcodes
cellSNP -s $BAM -b $BARCODE -O $OUT_DIR -p 22 --minMAF 0.1 --minCOUNT 100


check:

- mpileup.c in bcftools: https://github.com/samtools/bcftools/blob/develop/mpileup.c                                                                                
- bam_plcmd.c in samtools: https://github.com/samtools/samtools/blob/develop/bam_plcmd.c  refer to the cmdline options in this file too.




(base) mahogny@beagle:~$ bcftools mpileup

Usage: bcftools mpileup [options] in1.bam [in2.bam [...]] 







(base) mahogny@beagle:~$ bcftools call         --ploidy-file <file>        space/tab-delimited list of CHROM,FROM,TO,SEX,PLOIDY

 
About:   SNP/indel variant calling from VCF/BCF. To be used in conjunction with samtools mpileup.
         This command replaces the former "bcftools view" caller. Some of the original
         functionality has been temporarily lost in the process of transition to htslib,
         but will be added back on popular demand. The original calling model can be
         invoked with the -c option.
Usage:   bcftools call [options] <in.vcf.gz>   --ploidy-file <file>        space/tab-delimited list of CHROM,FROM,TO,SEX,PLOIDY


*/
all:
	cargo +nightly build

test:
	cargo +nightly test

fix:
	cargo +nightly fix --lib -p bascet --allow-dirty

install_rust:
	rustup toolchain install nightly

loc:
	wc -l src/*.rs src/*/*.rs src/*/*/*.rs  src/*/*.sh


#########
######### test of RNAseq
#########


test_raw_rna:
	rm -Rf temp; cargo +nightly run getraw --chemistry=atrandi_rnaseq  \
		--r1 testrna/part_raw/part_R1.fastq.gz \
		--r2 testrna/part_raw/part_R2.fastq.gz \
		--out-complete   testrna/out_complete.0.tirp.gz \
		--out-incomplete testrna/out_incomplete.0.tirp.gz \
		--libname mylib


test_pipe_sam_add_tags:
	head -n 100 miseqdata/some_sam.sam | cargo +nightly run pipe-sam-add-tags



#########
#########
#########


# unzip -l testdata/kmc.0.zip


test_countchrom:
	rm -Rf temp; cargo +nightly run countchrom -i testdata/sorted_aligned.1.bam -o testdata/cnt_al.hdf5


test_kraken:
	rm -Rf temp; cargo +nightly run kraken -i testdata/kraken_out.1.kraken_out -o testdata/kraken_count.hdf5

test_bam2fragments:
	rm -Rf temp; cargo +nightly run bam2fragments -i testdata/sorted_aligned.1.bam -o testdata/fragments.1.tsv.gz

test_minhash_kmc:
	rm -Rf temp; cargo +nightly run mapcell -i testdata/kmc.0.zip -o testdata/minhash.0.zip -s _minhash_kmc --show-script-output --keep-files 

test_minhash_fq:
	rm -Rf temp; cargo +nightly run mapcell -i testdata/filtered.0.tirp.gz -o testdata/minhash.0.zip -s _minhash_fq # --show-script-output --keep-files 
	#rm -Rf temp; cargo +nightly run mapcell -i miseqdata/filtered.1.tirp.gz -o miseqdata/minhash.1.zip -s _minhash_fq # --show-script-output --keep-files 

test_query_fq:
	rm -Rf temp; cargo +nightly run query-fq -i miseqdata/filtered.1.tirp.gz   -o miseqdata/counts.h5ad -f miseqdata/chosen_features.txt



test_minhashhist:
	#rm -Rf temp; cargo +nightly run minhash-hist -i testdata/minhash.0.zip -o testdata/minhash_hist
	rm -Rf temp; cargo +nightly run minhash-hist -i miseqdata/minhash.1.zip -o miseqdata/minhash_hist.csv

test_quast_custom:	
	rm -Rf temp; cargo +nightly run mapcell -i testdata/kmc.0.zip -o testdata/minhash.0.zip -s testdata/badscript.sh --show-script-output --keep-files 


test_query:
	rm -Rf temp; cargo +nightly run query -i testdata/kmc.0.zip -o testdata/counts.h5ad -f testdata/chosen_features.txt

test_script:
	rm -Rf temp; cargo +nightly run mapcell -i testdata/filtered.0.tirp.gz -o testdata/kmc.0.zip -s ./script.sh --show-script-output --keep-files 



test_featurise:
	#this makes a joint DB. call it something else!
	rm -Rf temp; cargo +nightly run featurise -i testdata/kmc.0.zip -o testdata/all_kmc




test_kmc_reads:
	rm -Rf temp; cargo +nightly run mapcell -i testdata/filtered.0.tirp.gz -o testdata/kmc.0.zip -s _kmc_process_reads --show-script-output
	#rm -Rf temp; cargo +nightly run mapcell -i testdata/out_complete.0.tirp.gz -o testdata/kmc.0.zip -s _kmc_process_contigs --show-script-output


test_transform_tirp_fastq:
	rm -Rf temp; cargo +nightly run transform -i testdata/out_complete.0.tirp.gz  -o testdata/newout.fq.gz

test_transform_tirp_2fastq:
	rm -Rf temp; cargo +nightly run transform -i testdata/out_complete.0.tirp.gz  -o testdata/newout.R1.fq.gz


test_mapcell:
	rm -Rf temp; cargo +nightly run mapcell -i testdata/shard/shard.0.zip  -o testdata/out.zip   -s _test


test_extract:
	cargo +nightly run extract -i testdata/out_complete.0.gascet.gz -o testdata/forskesa -b A1_H5_D9_H12


test_skesa:
	rm -Rf temp; cargo +nightly run mapcell -i testdata/out_complete.0.tirp.gz -o testdata/skesa.0.zip -s _skesa --show-script-output

test_shardify:
	rm -Rf temp; cargo +nightly run shardify -i testdata/out_complete.0.tirp.gz -o testdata/filtered.0.tirp.gz --cells testdata/list_cell.txt



test_getraw:
	rm -Rf temp; cargo +nightly run getraw \
		--r1 testdata/foo_R1.fastq.gz \
		--r2 testdata/foo_R2.fastq.gz \
		--out-complete testdata/out_complete.0.tirp.gz \
		--out-incomplete testdata/out_incomplete.0.tirp.gz



test_rna_3_1:
	rm -Rf temp; cargo +nightly run getraw --chemistry=atrandi_rnaseq  \
		--r1 rnaseq/1/Bac-Single-Cell_S1_L001_R1_001.fastq.gz \
		--r2 rnaseq/1/Bac-Single-Cell_S1_L001_R2_001.fastq.gz \
		--out-complete   rnaseq/out_complete.0.tirp.gz \
		--out-incomplete rnaseq/out_incomplete.0.tirp.gz

test_rna_3_2:
	rm -Rf temp; cargo +nightly run getraw --chemistry=atrandi_rnaseq  \
		--r1 rnaseq/2/Bac-Single-Cell_S2_L001_R1_001.fastq.gz \
		--r2 rnaseq/2/Bac-Single-Cell_S2_L001_R2_001.fastq.gz \
		--out-complete   rnaseq/out_complete.0.tirp.gz \
		--out-incomplete rnaseq/out_incomplete.0.tirp.gz

test_quast:
	cargo +nightly run extract -i testdata/quast.zip  -o  testdata/out.temp -b a  -f report.txt



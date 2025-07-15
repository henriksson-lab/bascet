all:
	cargo +nightly build --profile=release

test:
	cargo +nightly test

fix:
	cargo +nightly fix --lib -p bascet --allow-dirty

install_rust:
	rustup toolchain install nightly

loc:
	wc -l src/*.rs src/*/*.rs src/*/*/*.rs  src/*/*.sh

docker:
	#docker build .
	docker build -t henriksson-lab/bascet .

docker_upload: docker
	docker save -o docker_image/bascet.tar henriksson-lab/bascet
	md5sum bascet.tar > bascet.md5

	# scp docker_image/bascet.tar beagle:/corgi/public_http/public/bascet/  #it landed without og+r permission using scp!
	# scp docker_image/bascet.tar hpc2n:~/mystore/
	# cp docker_image/bascet.tar /corgi/public_http/public/bascet/

	# http://beagle.henlab.org/public/bascet/bascet.tar


docker_load:
	#just as an example
	docker load -i docker_image/bascet.tar

publish_test:
	# find . -name .DS_Store -print0 | xargs -0 git rm -f --ignore-unmatch
	cargo publish --dry-run




#########
######### test of stream
#########

stream:
	cargo +nightly run extract-stream
	#cargo +nightly run extract-stream -i testdata/minhash.0.zip


#########
######### test of parse RNAseq
#########


test_raw_parse_rna:
        rm -Rf temp; cargo +nightly run --profile=release getraw --chemistry=pb_rnaseq --r1 testparse/parse_R1_001.fastq.gz --r2 testparse/parse_R2_001.fastq.gz --out-complete   testparse/out_complete.0.tirp.gz --out-incomplete testparse/out_incomplete.0.tirp.gz \
                --libname mylib



#########
######### test of 10x RNAseq
#########


test_raw_10x_rna:
	rm -Rf temp; cargo +nightly run --profile=release getraw --chemistry=10xrna --r1 test10x/part_R1_001.fastq.gz --r2 test10x/part_R2_001.fastq.gz --out-complete   test10x/out_complete.0.tirp.gz --out-incomplete test10x/out_incomplete.0.tirp.gz \
		--libname mylib


#########
######### test of atrandi RNAseq
#########


test_raw_atrandi_rna:
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




test_countfeature2:
	rm -Rf temp; cargo +nightly run countfeature -i testdata/sorted_aligned.1.bam -o testdata/cnt_al.1.h5 -g counttest/all.gff3

test_countfeature:
	rm -Rf temp; cargo +nightly run countfeature -i counttest/aligned.1.bam -o counttest/cnt_al.1.h5 -g counttest/all.gff3

test_countchrom:
	rm -Rf temp; cargo +nightly run countchrom -i counttest/aligned.1.bam -o counttest/cnt_feature.1.h5 


test_kraken:
	#rm -Rf temp; cargo +nightly run kraken -i testdata/kraken_out.1.kraken_out -o testdata/kraken_count.1.h5
	rm -Rf temp; cargo +nightly run kraken -i testkraken/subkraken.kraken_out -o testkraken/kraken_count.1.h5

test_bam2fragments:
	rm -Rf temp; cargo +nightly run bam2fragments -i testdata/sorted_aligned.1.bam -o testdata/fragments.1.tsv.gz

test_minhash_kmc:
	rm -Rf temp; cargo +nightly run mapcell -i testdata/kmc.0.zip -o testdata/minhash.0.zip -s _minhash_kmc --show-script-output --keep-files 

test_countsketch_fq:
	#rm -Rf temp; cargo +nightly run mapcell -i testdata/filtered.0.tirp.gz -o testdata/countsketch.0.zip -s _countsketch_fq # --show-script-output --keep-files 
	rm -Rf temp; cargo +nightly run mapcell -i miseqdata/filtered.1.tirp.gz -o miseqdata/countsketch.0.zip -s _countsketch_fq # --show-script-output --keep-files 

test_countsketch_mat:
	rm -Rf temp; cargo +nightly run countsketch -i miseqdata/countsketch.0.zip -o miseqdata/countsketch_mat.csv
	#rm -Rf temp; cargo +nightly run countsketch -i testdata/countsketch.0.zip -o testdata/countsketch_mat.csv

test_minhash_fq:
	rm -Rf temp; cargo +nightly run mapcell -i testdata/filtered.0.tirp.gz -o testdata/minhash.0.zip -s _minhash_fq # --show-script-output --keep-files 
	#rm -Rf temp; cargo +nightly run mapcell -i miseqdata/filtered.1.tirp.gz -o miseqdata/minhash.1.zip -s _minhash_fq # --show-script-output --keep-files 

test_query_fq:
	#rm -Rf temp; cargo +nightly run query-fq -i miseqdata/filtered.1.tirp.gz   -o miseqdata/counts.h5ad -f miseqdata/chosen_features.txt
	rm -Rf temp; cargo +nightly run query-fq -i testquery/new.tirp.gz   -o  testquery/counts.h5ad -f testquery/use_kmers.txt


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

test_transform_2fastq_tirp:
	rm -Rf temp; cargo +nightly run transform -i testdata/newout.R1.fq.gz -o  testdata/out_complete.0.tirp.gz


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

test_getraw_hard:
	rm -Rf temp; cargo +nightly run getraw \
		--r1 hardtest/P32705_1001_S1_L001_R1_001.fastq.gz \
		--r2 hardtest/P32705_1001_S1_L001_R2_001.fastq.gz \
		--out-complete hardtest/out_complete.0.tirp.gz \
		--out-incomplete hardtest/out_incomplete.0.tirp.gz




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



CARGO ?= cargo
ZIGBUILD ?= cargo zigbuild
RUSTUP ?= rustup
LIPO ?= $(shell command -v llvm-lipo 2>/dev/null || command -v llvm-lipo-14 2>/dev/null || command -v lipo 2>/dev/null || printf lipo)
CROSS_PROFILE ?= release
CROSS_PACKAGE ?= bascet-cli
CROSS_FEATURES ?= --all-features
BASCET_BIN ?= bascet
MAC_UNIVERSAL_OUT ?= target/universal-apple-darwin/$(CROSS_PROFILE)/$(BASCET_BIN)
LINUX_TARGET ?= x86_64-unknown-linux-gnu
LINUX_BIN_OUT ?= target/$(LINUX_TARGET)/$(CROSS_PROFILE)/$(BASCET_BIN)
WINDOWS_BIN_OUT ?= target/x86_64-pc-windows-gnu/$(CROSS_PROFILE)/$(BASCET_BIN).exe
BINS_PUBLISH_DIR ?= /corgi/public_http/public/bascet/bins
LINUX_PUBLISH_BIN ?= bascet-linux-x86_64
WINDOWS_PUBLISH_BIN ?= bascet-windows-x86_64.exe
MAC_PUBLISH_BIN ?= bascet-macos-universal
LINUX_TARGETS ?= $(LINUX_CROSS_TARGET)
WINDOWS_TARGETS ?= x86_64-pc-windows-gnu
MAC_TARGETS ?= \
	x86_64-apple-darwin \
# 	aarch64-apple-darwin
CROSS_TARGETS ?= $(LINUX_TARGETS) $(WINDOWS_TARGETS) $(MAC_TARGETS)
MACOS_X86_CFLAGS ?= -DLIBDEFLATE_ASSEMBLER_DOES_NOT_SUPPORT_AVX512VNNI -DLIBDEFLATE_ASSEMBLER_DOES_NOT_SUPPORT_AVX_VNNI -DLIBDEFLATE_ASSEMBLER_DOES_NOT_SUPPORT_VPCLMULQDQ
MACOS_ARM_CFLAGS ?= -march=armv8-a+nocryp
LINUX_CFLAGS ?= -mno-avx512f -DLIBDEFLATE_ASSEMBLER_DOES_NOT_SUPPORT_AVX512VNNI -DLIBDEFLATE_ASSEMBLER_DOES_NOT_SUPPORT_AVX_VNNI -DLIBDEFLATE_ASSEMBLER_DOES_NOT_SUPPORT_VPCLMULQDQ

.PHONY: all test fix install_rust install_cross install_crosscompile loc all_linux all_win all_windows cross cross_targets all_mac linux_release mac_universal publish_bins FORCE

all:
	cargo +nightly build --profile=release

test:
	cargo +nightly test

fix:
	cargo +nightly fix --lib -p bascet --allow-dirty

install_rust:
	rustup toolchain install nightly

install_cross install_crosscompile: install_rust cross_targets install_mingw
	$(CARGO) install cargo-zigbuild --locked

loc:
	wc -l \
	src/*.rs \
	src/*/*.rs \
	src/*/*/*.rs \
	src/*/*/*/*.rs \
	src/*/*.sh \
	bascet*/*.rs \
	bascet*/*/*.rs \
	bascet*/*/*/*.rs \
	bascet*/*/*/*/*.rs

all_win all_windows: $(addprefix cross-,$(WINDOWS_TARGETS))

all_linux: $(addprefix cross-,$(LINUX_TARGETS))

all_mac: $(addprefix cross-,$(MAC_TARGETS))

linux_release:
	CFLAGS_x86_64_unknown_linux_gnu="$(LINUX_CFLAGS)" $(ZIGBUILD) --profile=$(CROSS_PROFILE) --target $(LINUX_TARGET) -p $(CROSS_PACKAGE) $(CROSS_FEATURES)

mac_universal: all_mac
	mkdir -p $(dir $(MAC_UNIVERSAL_OUT))
	$(LIPO) -create \
		target/x86_64-apple-darwin/$(CROSS_PROFILE)/$(BASCET_BIN) \
		target/aarch64-apple-darwin/$(CROSS_PROFILE)/$(BASCET_BIN) \
		-output $(MAC_UNIVERSAL_OUT)

publish_bins: linux_release all_win mac_universal
	mkdir -p $(BINS_PUBLISH_DIR)
	cp $(LINUX_BIN_OUT) $(BINS_PUBLISH_DIR)/$(LINUX_PUBLISH_BIN)
	cp $(WINDOWS_BIN_OUT) $(BINS_PUBLISH_DIR)/$(WINDOWS_PUBLISH_BIN)
	cp $(MAC_UNIVERSAL_OUT) $(BINS_PUBLISH_DIR)/$(MAC_PUBLISH_BIN)
	cd $(BINS_PUBLISH_DIR) && md5sum $(LINUX_PUBLISH_BIN) > $(LINUX_PUBLISH_BIN).md5
	cd $(BINS_PUBLISH_DIR) && md5sum $(WINDOWS_PUBLISH_BIN) > $(WINDOWS_PUBLISH_BIN).md5
	cd $(BINS_PUBLISH_DIR) && md5sum $(MAC_PUBLISH_BIN) > $(MAC_PUBLISH_BIN).md5

cross: $(addprefix cross-,$(CROSS_TARGETS))

cross_targets:
	$(RUSTUP) target add $(CROSS_TARGETS)

cross-%: FORCE
	$(CARGO) build --profile=$(CROSS_PROFILE) --target $* -p $(CROSS_PACKAGE) $(CROSS_FEATURES)

cross-x86_64-unknown-linux-gnu: FORCE
	XDG_CACHE_HOME="$(XDG_CACHE_HOME)" $(ZIGBUILD) --profile=$(CROSS_PROFILE) --target $(@:cross-%=%) -p $(CROSS_PACKAGE) $(CROSS_FEATURES)

cross-x86_64-apple-darwin: FORCE
	XDG_CACHE_HOME="$(XDG_CACHE_HOME)" CFLAGS_x86_64_apple_darwin="$(MACOS_X86_CFLAGS) $(CFLAGS_x86_64_apple_darwin)" $(ZIGBUILD) --profile=$(CROSS_PROFILE) --target $(@:cross-%=%) -p $(CROSS_PACKAGE) $(CROSS_FEATURES)

cross-aarch64-apple-darwin: FORCE
	CFLAGS_aarch64_apple_darwin="$(MACOS_ARM_CFLAGS)" $(ZIGBUILD) --profile=$(CROSS_PROFILE) --target $(@:cross-%=%) -p $(CROSS_PACKAGE) $(CROSS_FEATURES)

FORCE:

podman:
#docker:
	git rev-parse --abbrev-ref HEAD > git_branch.txt
	git rev-parse --short HEAD > git_hash.txt
	podman build -t henriksson-lab/bascet .
	podman save henriksson-lab/bascet | pigz --to-stdout > docker_image/bascet.tar.gz
	md5sum docker_image/bascet.tar.gz > docker_image/bascet.tar.gz.md5
	#try if faster: singularity pull singularity/bascet.sif  docker-archive:bascet.tar.gz  #can do both tar and tar.gz

podman_upload: #docker
	#md5sum docker_image/bascet.tar > docker_image/bascet.tar.md5
	#scp docker_image/bascet.tar docker_image/bascet.tar.md5  /corgi/public_http/public/bascet/
	cp docker_image/bascet.tar.gz  docker_image/bascet.tar.gz.md5	/corgi/public_http/public/bascet/

	#make singularity image. Careful! this command takes from eithe docker or podman, whichever is running. stick to one!
	#singularity pull --force singularity/bascet.sif  docker-daemon:henriksson-lab/bascet:latest
	podman save henriksson-lab/bascet:latest | singularity pull --force singularity/bascet.sif docker-archive:/dev/stdin
	md5sum singularity/bascet.sif > singularity/bascet.sif.md5

	cp singularity/bascet.sif  singularity/bascet.sif.md5		/corgi/public_http/public/bascet/

	# scp docker_image/bascet.tar beagle:/corgi/public_http/public/bascet/  #it landed without og+r permission using scp!
	# scp docker_image/bascet.tar hpc2n:~/mystore/
	# cp docker_image/bascet.tar /corgi/public_http/public/bascet/

	# http://beagle.henlab.org/public/bascet/bascet.tar

docker_hpc2n:
	scp singularity/bascet.sif hpc2n:~/mystore/

docker_load:
	#just as an example
	docker load -i docker_image/bascet.tar

publish_test:
	# find . -name .DS_Store -print0 | xargs -0 git rm -f --ignore-unmatch
	cargo publish --dry-run


install_mingw:
	#needed to cross-compile to windows
	sudo apt-get install mingw-w64


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
	rm -Rf temp; cargo +nightly run --profile=release getraw --chemistry=pb_rnaseq --r1 testparse/parse_R1_001.fastq.gz --r2 testparse/parse_R2_001.fastq.gz --out-complete   testparse/out_complete.0.tirp.gz --out-incomplete testparse/out_incomplete.0.tirp.gz --libname mylib



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


test_countfeature_bed:
	rm -Rf temp; cargo +nightly run countfeature -i testdata/sorted_aligned.1.bam -o testdata/cnt_al.1.h5 -g testdata/features.bed

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

#//NOTE
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

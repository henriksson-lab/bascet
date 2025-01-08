all:
	cargo +nightly build


install_rust:
	rustup toolchain install nightly

loc:
	wc -l src/*.rs src/*/*.rs src/*/*/*.rs src/*/*/*/*.rs  src/*/*/*.sh


#########
#########
#########

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
		--r1 rnaseq/Bac-Single-Cell_S1_L001_R1_001.fastq.gz \
		--r2 rnaseq/Bac-Single-Cell_S1_L001_R2_001.fastq.gz \
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



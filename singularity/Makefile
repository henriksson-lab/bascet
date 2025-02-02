all:

build:
	sudo singularity build bascet_container.sif bascet_v1.def

bascet:
	sudo singularity build all.sif with_bascet.def


#get:
#	singularity pull --name bascet_container.sif shub://todo/bascet-singularity

example_run:
	singularity exec bascet_container.sif metaquast contigs.fa




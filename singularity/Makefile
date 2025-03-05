all:


trivial:
	sudo singularity build trivial.sif trivial.def

deps:
	sudo singularity build deps.sif deps.def

bascet:
	sudo singularity build bascet.sif bascet.def

#bascet:
#	sudo singularity build all.sif with_bascet.def


#get:
#	singularity pull --name bascet_container.sif shub://todo/bascet-singularity

example_run:
	singularity exec bascet.sif metaquast contigs.fa




test:
	sudo singularity build test.sif test.def

test2:
	singularity exec trivial.sif ls

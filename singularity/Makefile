all:


trivial:
	sudo singularity build trivial.sif trivial.def

deps:
	sudo singularity build deps.sif deps.def

bascet:
	sudo singularity build bascet.sif bascet.def

upload:
	cp bascet.sif /corgi/public_http/public/bascet/

#get:
#	singularity pull --name bascet_container.sif shub://todo/bascet-singularity

example_run:
	singularity exec bascet.sif metaquast contigs.fa

hpc2n:
	scp bascet.sif hpc2n:~/mystore/


test:
	sudo singularity build test.sif test.def

test2:
	singularity exec trivial.sif ls

all:

deps:
	sudo singularity build --force deps.sif deps.def

bascet:
	### Store git info
	git rev-parse --abbrev-ref HEAD > git_branch.txt
	git rev-parse --short HEAD > git_hash.txt

	sudo singularity build --force bascet.sif bascet.def
	md5sum bascet.sif > bascet.sif.md5

upload:
	cp bascet.sif bascet.sif.md5 /corgi/public_http/public/bascet/

example_run:
	singularity exec bascet.sif metaquast contigs.fa

hpc2n:
	scp bascet.sif hpc2n:~/mystore/

info:
	singularity run bascet.sif cat /git_branch.txt
	singularity run bascet.sif cat /git_hash.txt

#get:
#	singularity pull --name bascet_container.sif shub://todo/bascet-singularity


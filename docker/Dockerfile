FROM ubuntu:24.10 AS spython-base
ENV LC_ALL=C
RUN SINGULARITY_SHELL=/bin/bash
RUN apt-get update
RUN apt install -y build-essential
RUN apt-get install -y libz-dev
RUN apt-get install -y wget
RUN apt-get install -y make
RUN apt-get install -y curl
RUN apt install -y bc
RUN mkdir -p /opt/software
RUN cd /opt/software
RUN curl -O https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
RUN sh ./Miniconda3-latest-Linux-x86_64.sh -p /opt/software/conda -b
RUN /opt/software/conda/bin/conda config --add channels defaults
RUN /opt/software/conda/bin/conda config --add channels conda-forge
RUN /opt/software/conda/bin/conda config --add channels bioconda
RUN /opt/software/conda/bin/conda create -p /opt/software/conda_env -y abricate ariba bakta bamtools checkm-genome diamond fastani fastqc gecco genomad gtdbtk kmc kraken2 mash mlst mmseqs2 ncbi-amrfinderplus prokka quast skani skesa snippy spades star
CMD exec "$@"
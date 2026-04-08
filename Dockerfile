FROM ubuntu:25.04
ENV LC_ALL=C
RUN SINGULARITY_SHELL=/bin/bash

RUN apt-get update

#Required for bascet compilation
RUN apt install -y build-essential git cmake libhdf5-serial-dev libclang-dev

RUN apt-get install -y libz-dev wget make curl fastp bc fastqc kraken2 bamtools mash fastani ariba kmc skesa rna-star spades bowtie2


RUN mkdir -p /opt/software

RUN cd /opt/software
#RUN curl -O https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
#RUN sh ./Miniconda3-latest-Linux-x86_64.sh -p /opt/software/conda -b

RUN curl -L https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh -o miniforge.sh && \
    sh miniforge.sh -p /opt/software/conda -b && \
    rm miniforge.sh

RUN /opt/software/conda/bin/conda config --add channels bioconda

#python gecco removed

RUN /opt/software/conda/bin/mamba create -p /opt/software/conda_env -y abricate cellsnp-lite ncbi-amrfinderplus prokka vireoSNP quast bakta tabix
#now out: diamond genomad mlst snippy skani mmseqs2 checkm-genome trust4


######## install rust

RUN mkdir -p /src
WORKDIR /src
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN . "$HOME/.cargo/env"
RUN rustup toolchain install nightly

######## install gecco-rs

RUN cargo install --git https://github.com/henriksson-lab/gecco-rs
RUN gecco build-data
RUN cp /root/.cargo/bin/gecco /bin/gecco

######## install bascet

COPY assets /src/bascet/assets
COPY crates /src/bascet/crates
COPY .cargo /src/bascet/.cargo
COPY Cargo.toml /src/bascet/Cargo.toml
COPY git_branch.txt /git_branch.txt
COPY git_hash.txt /git_hash.txt

WORKDIR /src/bascet
#RUN cd /src/bascet
RUN cargo +nightly build --profile=release
RUN cp /src/bascet/target/release/bascet /bin/bascet
RUN rm -Rf /src/bascet

#CMD exec "$@"   #what is default?



# RUN cd /opt/software    is dead code TODO

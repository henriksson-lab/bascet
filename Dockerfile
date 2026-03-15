FROM ubuntu:25.04
ENV LC_ALL=C
RUN SINGULARITY_SHELL=/bin/bash

RUN apt-get update

#Required for bascet compilation
RUN apt install -y build-essential git cmake libhdf5-serial-dev libclang-dev

RUN apt-get install -y libz-dev wget make curl fastp bc fastqc kraken2 bamtools mash fastani ariba kmc skesa rna-star spades bowtie2


RUN mkdir -p /opt/software
RUN cd /opt/software
RUN curl -O https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
RUN sh ./Miniconda3-latest-Linux-x86_64.sh -p /opt/software/conda -b
RUN /opt/software/conda/bin/conda config --add channels defaults
RUN /opt/software/conda/bin/conda config --add channels conda-forge
RUN /opt/software/conda/bin/conda config --add channels bioconda

RUN /opt/software/conda/bin/conda tos accept --override-channels --channel conda-forge
RUN /opt/software/conda/bin/conda tos accept --override-channels --channel bioconda
RUN /opt/software/conda/bin/conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main
RUN /opt/software/conda/bin/conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r

RUN /opt/software/conda/bin/conda create -p /opt/software/conda_env -y abricate bakta checkm-genome cellsnp-lite diamond gecco genomad mlst \
                                                                       mmseqs2 ncbi-amrfinderplus prokka quast skani tabix trust4 vireosnp snippy


######## install rust

RUN mkdir -p /src
WORKDIR /src
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN . "$HOME/.cargo/env"
RUN rustup toolchain install nightly

######## install bascet

COPY assets /src/bascet/assets
COPY crates /src/bascet/crates
COPY .cargo /src/bascet/.cargo
COPY Cargo.toml /src/bascet/Cargo.toml
#    git_branch.txt /git_branch.txt
#    git_hash.txt /git_hash.txt				TODO

WORKDIR /src/bascet
#RUN cd /src/bascet
RUN cargo +nightly build --profile=release
RUN cp /src/bascet/target/release/bascet /bin/bascet
RUN rm -Rf /src/bascet
#CMD exec "$@"   #what is default?

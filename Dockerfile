FROM ubuntu:24.10
ENV LC_ALL=C
RUN SINGULARITY_SHELL=/bin/bash


RUN apt-get update
RUN apt install -y build-essential
RUN apt-get install -y libz-dev
RUN apt-get install -y wget
RUN apt-get install -y make
RUN apt-get install -y curl
RUN apt-get install -y fastp
RUN apt-get install -y bc
RUN apt-get install -y git cmake libhdf5-serial-dev libclang-dev



RUN mkdir -p /opt/software
RUN cd /opt/software
RUN curl -O https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
RUN sh ./Miniconda3-latest-Linux-x86_64.sh -p /opt/software/conda -b
RUN /opt/software/conda/bin/conda config --add channels defaults
RUN /opt/software/conda/bin/conda config --add channels conda-forge
RUN /opt/software/conda/bin/conda config --add channels bioconda
RUN /opt/software/conda/bin/conda create -p /opt/software/conda_env -y abricate ariba bakta bamtools checkm-genome cellsnp-lite diamond fastani fastqc gecco genomad gtdbtk kmc kraken2 mash mlst mmseqs2 ncbi-amrfinderplus prokka quast skani skesa snippy spades star tabix trust4 vireosnp


######## install rust

RUN mkdir -p /src
WORKDIR /src
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN . "$HOME/.cargo/env"
RUN rustup toolchain install nightly

######## install bascet

COPY src /src/bascet/src
COPY Cargo.toml /src/bascet/Cargo.toml
WORKDIR /src/bascet
#RUN cd /src/bascet
RUN cargo +nightly build --profile=release
RUN cp /src/bascet/target/release/bascet /bin/bascet
RUN rm -Rf /src/bascet
CMD exec "$@"

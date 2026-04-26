#################################################
FROM docker.io/library/rust:1.94-slim as chef
RUN cargo install cargo-chef

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake build-essential git cmake libhdf5-serial-dev libclang-dev \
    && rm -rf /var/lib/apt/lists/*


#################################################
# Build gecco in its own stage so caching is clean
FROM chef AS gecco-builder
ARG CACHEBUST=14
RUN cargo install --git https://github.com/henriksson-lab/gecco-rs
RUN gecco build-data


##################################################
# Having this separate means that all differences in COPY will not propagate; is recipe.json is the same as before, it will not trigger the next stage
FROM chef AS planner
WORKDIR /app

COPY assets/			./assets/
COPY crates/bascet-cli/		./crates/bascet-cli/
COPY crates/bascet-core/	./crates/bascet-core/
COPY crates/bascet-derive/	./crates/bascet-derive/
COPY crates/bascet-io/		./crates/bascet-io/
COPY crates/bascet-runtime/	./crates/bascet-runtime/
COPY crates/bascet-variadic/	./crates/bascet-variadic/
COPY .cargo/			./.cargo/
COPY Cargo.toml			./

RUN cargo chef prepare --recipe-path recipe.json


##################################################
FROM chef AS builder
WORKDIR /app
COPY --from=planner /app/recipe.json recipe.json

# Copy only config/assets needed by chef cook (not source code)
COPY .cargo/			./.cargo/
COPY assets/			./assets/

#ARG CACHEBUST=100

# Build dependencies only — cook uses recipe.json and replaces source with stubs
RUN cargo +nightly chef cook --release --recipe-path recipe.json

# Now copy real source AFTER cook, so it overwrites the stubs
COPY crates/bascet-cli/		./crates/bascet-cli/
COPY crates/bascet-core/	./crates/bascet-core/
COPY crates/bascet-derive/	./crates/bascet-derive/
COPY crates/bascet-io/		./crates/bascet-io/
COPY crates/bascet-runtime/	./crates/bascet-runtime/
COPY crates/bascet-variadic/	./crates/bascet-variadic/
COPY Cargo.toml			./

# Build application (dependencies are cached from cook step)
RUN cargo +nightly build --release

RUN du -hc /app/target/release/bascet

##################################################
#The image we ship ###########
FROM ubuntu:25.04 AS runtime
ENV LC_ALL=C
RUN SINGULARITY_SHELL=/bin/bash

RUN apt-get update
RUN apt-get install -y wget make curl fastp bc fastqc kraken2 bamtools mash fastani ariba kmc skesa rna-star spades bowtie2

RUN mkdir -p /opt/software

RUN curl -L https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh -o miniforge.sh && \
    sh miniforge.sh -p /opt/software/conda -b && \
    rm miniforge.sh

RUN /opt/software/conda/bin/conda config --add channels bioconda

#python gecco removed

RUN /opt/software/conda/bin/mamba create -p /opt/software/conda_env -y abricate cellsnp-lite ncbi-amrfinderplus prokka vireoSNP quast bakta tabix        gecco
#now out: diamond genomad mlst snippy skani mmseqs2 checkm-genome trust4


############################ Copy rust items on top
COPY --from=builder       /app/target/release/bascet /bin/bascet
COPY --from=gecco-builder /usr/local/cargo/bin/gecco /bin/gecco
COPY --from=gecco-builder /gecco_data /bin/gecco_data

COPY git_branch.txt /git_branch.txt
COPY git_hash.txt /git_hash.txt




# libhdf5-serial-dev    use ldd to see what files are needed by bascet

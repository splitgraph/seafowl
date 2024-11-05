# Image that wraps Seafowl with a bytehound binary and records
# memory allocations for profiling
#
# To build run just the bytehound layer run
# docker build --target bytehound -f Dockerfile.profile -t splitgraph/bytehound .
#
# To build the full image run
# docker build -f Dockerfile.profile -t splitgraph/seafowl:profile ..

FROM rust:slim AS bytehound

RUN apt-get update && \
    apt-get install -y git protobuf-compiler ca-certificates npm && \
    npm install -g yarn

# Fetch bytehound source and compile
RUN git clone https://github.com/koute/bytehound.git && \
    cd bytehound && \
    cargo build --release -p bytehound-preload && \
    cargo build --release -p bytehound-cli

FROM bytehound AS profile

RUN rustup default nightly-2024-10-30

# Compile an empty project, so as to cache the compiled deps and avoid unneeded re-compilation.
# Adapted from https://gist.github.com/noelbundick/6922d26667616e2ba5c3aff59f0824cd
RUN cargo new seafowl
WORKDIR seafowl
COPY Cargo.toml Cargo.lock build.rs .
COPY clade clade
COPY datafusion_remote_tables datafusion_remote_tables
COPY migrations migrations
COPY object_store_factory object_store_factory
RUN --mount=type=cache,target=/usr/local/cargo/registry cargo build --profile release-with-debug

# Copy the rest of the code now and update timestamps to force a new build only for it
COPY src src
RUN --mount=type=cache,target=/usr/local/cargo/registry set -e && \
    touch src/lib.rs src/main.rs && \
    cargo build --profile release-with-debug

RUN cd .. && mkdir seafowl-data

ENV MEMORY_PROFILER_OUTPUT=profiles/memory-profiling_%e_%t_%p.dat
ENV LD_PRELOAD=/bytehound/target/release/libbytehound.so
ENTRYPOINT [ "./target/release-with-debug/seafowl" ]

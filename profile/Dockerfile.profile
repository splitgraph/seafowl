# Image that wraps Seafowl with a bytehound binary and records
# memory allocations for profiling
#
# To build run just the bytehound layer run
# DOCKER_BUILDKIT=1 docker build --target bytehound -f Dockerfile.profile -t splitgraph/bytehound .
#
# To build the full image run
# DOCKER_BUILDKIT=1 docker build -f Dockerfile.profile -t splitgraph/seafowl:profile ..

FROM rust:slim AS bytehound

RUN apt-get update && \
    apt-get install -y git protobuf-compiler ca-certificates npm && \
    npm install -g yarn && \
    git clone https://github.com/koute/bytehound.git && \
    cd bytehound && \
    cargo build --release -p bytehound-preload && \
    cargo build --release -p bytehound-cli

FROM ubuntu AS profile

RUN mkdir profiles && mkdir seafowl-data
COPY target/aarch64-unknown-linux-gnu/release-with-debug/seafowl seafowl
COPY --from=bytehound /bytehound/target/release/libbytehound.so libbytehound.so

ENV MEMORY_PROFILER_OUTPUT=profiles/memory-profiling_%e_%t_%p.dat
ENV LD_PRELOAD=./libbytehound.so
ENTRYPOINT [ "./seafowl" ]

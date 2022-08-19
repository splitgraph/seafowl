FROM debian:bullseye-slim

RUN \
    # Get Seafowl
    apt update -qq && apt install -y curl unzip && \
    cd /usr/local/bin && \
    curl -LO https://nightly.link/splitgraph/seafowl/workflows/nightly/main/seafowl-nightly-x86_64-unknown-linux-gnu.zip && \
    unzip seafowl-nightly-x86_64-unknown-linux-gnu.zip && \
    chmod a+x ./seafowl && \
    ./seafowl --version && \
    rm seafowl-nightly-x86_64-unknown-linux-gnu.zip

ENTRYPOINT [ "/usr/local/bin/seafowl", "-c", "/etc/seafowl/seafowl.toml" ]
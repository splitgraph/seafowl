FROM debian:bullseye-slim

RUN apt update -qq && apt install -y curl

# Use this instead of the below commented item while we wait for nightly.link / GHA to be happy again
COPY seafowl /usr/local/bin/seafowl
RUN chmod a+x /usr/local/bin/seafowl

# RUN \
#     # Get Seafowl
#     cd /usr/local/bin && \
#     wget https://nightly.link/splitgraph/seafowl/workflows/nightly/main/seafowl-nightly-x86_64-unknown-linux-gnu.zip && \
#     unzip seafowl-nightly-x86_64-unknown-linux-gnu.zip && \
#     chmod a+x ./seafowl && \
#     ./seafowl --version && \
#     rm seafowl-nightly-x86_64-unknown-linux-gnu.zip

ENTRYPOINT [ "/usr/local/bin/seafowl", "-c", "/etc/seafowl/seafowl.toml" ]
FROM litestream/litestream

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

ENTRYPOINT [ "litestream", "replicate", "-config", "/etc/seafowl/litestream.yml", "-exec", "/usr/local/bin/seafowl", "-c", "/etc/seafowl/seafowl.toml"]
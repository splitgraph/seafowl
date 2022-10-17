FROM splitgraph/seafowl:nightly

RUN \
    apt update -qq && apt install -y wget fuse && \
    wget https://github.com/superfly/litefs/releases/download/v0.2.0/litefs-v0.2.0-linux-amd64.tar.gz && \
    tar -xzf litefs-v0.2.0-linux-amd64.tar.gz && \
    rm litefs-v0.2.0-linux-amd64.tar.gz && \
    mv litefs /usr/local/bin/litefs && \
    chmod a+x /usr/local/bin/litefs && \
    mkdir -p /data/seafowl/db && mkdir -p /data/seafowl/litefs

ENTRYPOINT [ "litefs", "-config", "/etc/seafowl/litefs.yml" ]
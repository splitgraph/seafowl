# syntax=docker/dockerfile:1

# Dockerfile used to build a Seafowl Docker image
#
# Assumes Seafowl is already built (target/release/seafowl).
# Build this with Buildkit in order to avoid sending the whole source root
# to the Docker daemon.
#
# You can also build it manually:
# cargo build --release
# DOCKER_BUILDKIT=1 docker build . -t splitgraph/seafowl:nightly

FROM ubuntu:22.04

COPY target/release/seafowl /usr/local/bin/seafowl

# Make sure to install ca-certificates so that we can use HTTPS
# https://github.com/debuerreotype/docker-debian-artifacts/issues/15
RUN \
    apt-get update -qq && \
    apt-get install -y --no-install-recommends ca-certificates && \
    update-ca-certificates && \
    mkdir -p /seafowl-data && \
    mkdir -p /etc/seafowl && \
    # Build the default config file
    cat > /etc/seafowl/seafowl.toml <<EOF
[object_store]
type = "local"
data_dir = "/seafowl-data"

[catalog]
type = "sqlite"
dsn = "sqlite:///seafowl-data/seafowl.sqlite"

[frontend.http]
# Listen on all interfaces so that we are accessible over Docker
bind_host = "0.0.0.0"
bind_port = 8080
read_access = "any"

# Disable write access by default, since the image user can derive an image
# from this and use the --one-off command to freeze a dataset, or
# set the SEAFOWL__FRONTEND__HTTP__WRITE_ACCESS=some_sha envvar.
write_access = "off"
EOF

# Make /seafowl-data a volume
# This is not required for bind mounting, but will create an anonymous volume
# on startup for persistence
# https://docs.docker.com/engine/reference/builder/#volume
VOLUME [ "/seafowl-data" ]

ENTRYPOINT [ "/usr/local/bin/seafowl" ]
CMD [ "-c", "/etc/seafowl/seafowl.toml" ]
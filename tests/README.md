# HOWTO

Want to run the Seafowl tests locally? This HOWTO is intended to help you more easily understand what the tests need.

In short, invoking `cargo test` depends on:

- the containers in [docker-compose.yml](../docker-compose.yml)
- A Postgres instance (or equivalent)
- `DATABASE_URL` env var pointing to that Postgres instance

### macOS
#### Get the containers running 
After installing [Docker for Mac](https://docs.docker.com/desktop/install/mac-install/) please make sure `cd`'d into the Seafowl repo and run 
```shell
docker compose up -d
```

#### Make Postgres available
Next, make a Postgres instance available. 

One way to do that is to via `sgr`, which can be installed [a few different ways](https://www.splitgraph.com/docs/sgr-advanced/getting-started/installation) (including [Homebrew](https://www.splitgraph.com/docs/sgr-advanced/getting-started/installation#homebrew)).
```shell
brew install sgr
sgr engine add
```

At the time of writing (May 2023) `docker ps` should look something like this:
```shell
CONTAINER ID   IMAGE                      COMMAND                  CREATED             STATUS             PORTS                    NAMES
406aea792280   minio/minio:latest         "/usr/bin/docker-ent…"   About an hour ago   Up About an hour   0.0.0.0:9000->9000/tcp   seafowl-minio-1
658f1956a1e4   fsouza/fake-gcs-server     "/bin/fake-gcs-serve…"   About an hour ago   Up About an hour   0.0.0.0:4443->4443/tcp   seafowl-fake-gcs-1
041cc2cfab42   splitgraph/engine:0.3.12   "docker-entrypoint.s…"   5 days ago          Up 2 hours         0.0.0.0:6432->5432/tcp   splitgraph_engine_default
```
  
#### Provide the DATABASE_URL environment variable

Finally, run
```shell
export DATABASE_URL=postgresql://sgr:1234@localhost:6432/splitgraph
```
(where '1234' is whatever password you assigned in previous step)


#### You're done
You're now ready to run the tests! 🎉
```
cargo test
```
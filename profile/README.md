## Setup

If you're on MacOS you'll need cross-compilation tools, since the profiling Seafowl binary is
compiled on the host (compiling inside the container leads to OOMs). Grab one from
https://github.com/messense/homebrew-macos-cross-toolchains.

The build bytehound and the profiler (Seafowl wrapped in bytehound) images run

```shell
$ just build-bytehound
$ just build-profiler
```

## Measuring

To actually start profiling run

```shell
$ just profile
docker run -p 8080:8080 -p 47470:47470 -v .:/profiles -v `realpath ../seafowl.toml`:/seafowl.toml -v `realpath ../../seafowl-data`:/seafowl-data splitgraph/seafowl:profile -c /seafowl.toml
2024-11-06T14:12:11.519272Z  INFO main ThreadId(01) seafowl: Starting Seafowl 0.5.8
2024-11-06T14:12:11.519390Z  INFO main ThreadId(01) seafowl: Loading the configuration from /seafowl.toml
2024-11-06T14:12:11.538033Z  INFO tokio-runtime-worker ThreadId(12) seafowl: Starting the Arrow Flight frontend on 0.0.0.0:47470
2024-11-06T14:12:11.538268Z  INFO tokio-runtime-worker ThreadId(12) seafowl: Starting the PostgreSQL frontend on 127.0.0.1:6432
2024-11-06T14:12:11.538275Z  WARN tokio-runtime-worker ThreadId(12) seafowl: The PostgreSQL frontend doesn't have authentication or encryption and should only be used in development!
2024-11-06T14:12:11.538321Z  INFO tokio-runtime-worker ThreadId(12) seafowl: Starting the HTTP frontend on 0.0.0.0:8080
...
```

and then run your workload against the HTTP/gRPC endpoint.

Bytehound continually dumps the allocation data into a single file for each run

```shell
$ tree -h | grep mem
├── [3.8G]  memory-profiling_seafowl_1730902150_1.dat
├── [4.3G]  memory-profiling_seafowl_1730902331_1.dat
```

Once you're done with profiling press ctrl + c to stop the container.

## Browsing

To see the data in a web UI either explicitly load one or more files

```shell
just view memory-profiling_seafowl_1730902331_1.dat
```

or load all recorded files

```shell
just view
```

and open `localhost:9999`.

Note that you should strive to keep the recorded profiles under 5GB since otherwise the server will
fail to load them. If you must profile a long running process, but want to work around this consider
toggling the recording on and off via `docker kill -s SIGUSR1 seafowl-profiler`.

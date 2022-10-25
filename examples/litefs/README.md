# Running Seafowl in single-reader-multiple-writers mode with LiteFS

This example is a Docker Compose file that shows you how to run multiple instances of Seafowl with:

- SQLite as a catalog
- MinIO as an object store
- [LiteFS](https://github.com/superfly/litefs) to replicate the SQLite catalog from the writer
  instance to reader instances
- NGINX to direct reads and writes to relevant Seafowl nodes

As a result, you can quickly scale Seafowl for low latency read-only queries without having to
deploy a PostgreSQL cluster. There will be a single Seafowl writer instance that you can send write
requests to. Reader Seafowl replicas will pull changes to the SQLite database from the writer and
will be otherwise completely stateless.

## Notes on LiteFS

This example uses LiteFS's ["static leasing"](https://fly.io/docs/litefs/config/#static-leasing)
option, which allows us to use a single static Seafowl instance as a "leader".

It's also possible to run LiteFS with dynamic leasing, which uses Consul to choose a leader
instance. This is what happens when running LiteFS on [Fly.io](https://fly.io). In addition, in that
environment, the application can use the `Fly-Replay` header to redirect writes to the currently
running writer instance.

We don't support this in this example. Instead, the "ingress" NGINX load balancer sends all requests
that begin with `/w/` to the writer Seafowl and the rest to the reader Seafowl instances.

In terms of getting this running with LiteFS on Docker Compose, note we have to:

- Add some capabilities to the Seafowl containers to allow it to perform FUSE operations (see
  `cap_add` and `devices` sections in the Compose file)
- Run the "reader" Seafowl replicas in read-only mode (obviously), otherwise they try to create a
  fresh SQLite database in a read-only file system
- Use the `off` `journal_mode` on the reader and `delete` on the writer (LiteFS currently doesn't
  support the `wal` journal mode)

## Running the example

Start the stack with several reader nodes:

```bash
docker compose up --scale seafowl-reader=3 -d
```

Create a table on the writer instance:

```bash
curl -i -H "Content-Type: application/json" \
"http://localhost:8080/w/q/" -d@-<<EOF
{"query": "
  CREATE EXTERNAL TABLE cambridge_weather (
    timestamp TIMESTAMP,
    temp_c_10 INT NULL,
    humidity_perc INT NULL,
    dew_point_c_10 INT NULL,
    pressure_mbar INT NULL,
    mean_wind_speed_knots_10 INT NULL,
    average_wind_bearing_degrees INT NULL,
    sunshine_hours_100 INT,
    rainfall_mm_1000 INT,
    max_wind_speed_knots_10 INT NULL
  )
  STORED AS CSV
  LOCATION 'https://www.cl.cam.ac.uk/research/dtg/weather/weather-raw.csv';

  CREATE TABLE cambridge_weather AS SELECT * FROM staging.cambridge_weather
"}
EOF
```

Read the data from any reader instance:

```bash
curl -i -H "Content-Type: application/json" \
"http://localhost:8080/q" -d@-<<EOF
{"query": "
 SELECT
  date_trunc('month', timestamp) AS month,
  AVG(temp_c_10 * 0.1) AS avg_temp,
  MIN(temp_c_10 * 0.1) AS min_temp,
  MAX(temp_c_10 * 0.1) AS max_temp
 FROM cambridge_weather
 GROUP BY 1 ORDER BY 1 DESC LIMIT 12
"}
EOF
```

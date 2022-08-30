# Caching Seafowl query results with Varnish

This example contains a Seafowl instance behind the Varnish HTTP cache.

The Seafowl instance has a pre-baked dataset with a CSV file from
<https://www.cl.cam.ac.uk/research/dtg/weather/>.

See the [Seafowl docs](https://www.splitgraph.com/docs/seafowl/guides/querying-cache-cdn) and the
[Varnish tutorial](https://www.splitgraph.com/docs/seafowl/getting-started/tutorial-fly-io/part-4-1-caching-with-varnish)
for more information.

## Running the example

```bash
docker compose up -d
```

You can query the cached GET API with the wrapper [query.sh](./query.sh) script:

```bash
$ ./query.sh "SELECT date_trunc('month', timestamp) AS month, AVG(temp_c_10 * 0.1) AS avg_temp FROM cambridge_weather GROUP BY 1 ORDER BY 2 DESC LIMIT 5" | tail -n10

Via: 1.1 varnish (Varnish/6.0)
x-cache: hit cached
Accept-Ranges: bytes
Connection: keep-alive

{"month":"1995-06-01 00:00:00","avg_temp":22.62631578947368}
{"month":"2006-07-01 00:00:00","avg_temp":21.615631399317408}
{"month":"1997-08-01 00:00:00","avg_temp":21.097033618984867}
{"month":"1995-07-01 00:00:00","avg_temp":20.36686991869917}
{"month":"2003-08-01 00:00:00","avg_temp":20.07346938775517}
```

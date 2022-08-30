# Baking a dataset into a Docker image

This example extends the Seafowl Docker image with a pre-baked dataset with a CSV file from
<https://www.cl.cam.ac.uk/research/dtg/weather/>.

![Architecture diagram](/static-v2/seafowl/diagrams/deployment-no-w-multiple-r.png)

See the [Seafowl docs](https://www.splitgraph.com/docs/seafowl/guides/baking-dataset-docker-image)
for more information.

## Running the example

```bash
docker compose up -d
```

You can now query this dataset as follows:

```bash
curl \
   -H "Content-Type: application/json" \
   http://localhost:8080/q -d@- <<EOF
{"query": "SELECT date_trunc('month', timestamp) AS month,
  AVG(temp_c_10 * 0.1) AS avg_temp
  FROM cambridge_weather GROUP BY 1 ORDER BY 2 ASC LIMIT 10"}
EOF

{"month":"2010-12-01 00:00:00","avg_temp":-0.18382749326145384}
{"month":"2018-02-01 00:00:00","avg_temp":1.0846681922196781}
{"month":"2010-01-01 00:00:00","avg_temp":1.3368279569892467}
{"month":"2021-01-01 00:00:00","avg_temp":1.5615591397849464}
{"month":"2017-01-01 00:00:00","avg_temp":1.7540322580645156}
{"month":"2009-01-01 00:00:00","avg_temp":2.276196898179361}
{"month":"2013-03-01 00:00:00","avg_temp":2.369500674763832}
{"month":"2019-01-01 00:00:00","avg_temp":2.500806451612902}
{"month":"1995-12-01 00:00:00","avg_temp":2.592955892034235}
{"month":"2022-01-01 00:00:00","avg_temp":2.6318611987381733}
```

# Running Seafowl with PostgreSQL and MinIO

This example is a Docker Compose file that shows you how to run multiple instances of Seafowl with:

- PostgreSQL as a catalog
- MinIO as an object store

This example supports running multiple instances of Seafowl with the `--scale` Compose option and
comes with NGINX as a load balancer.

Note that Seafowl uses asynchronous IO and so will be able to efficiently utilize all cores on a
single machine. This means running multiple Seafowl instances on a single node won't bring any
performance improvements. This configuration here is an example of how you would scale Seafowl to
multiple nodes.

## Running the example

```bash
docker compose up --scale seafowl=3 -d seafowl
```

You can now access an NGINX instance that load balances between multiple Seafowl instances:

```bash
curl -H "Content-Type: application/json" \
    "http://localhost:8080/q/" -d '{"query": "SELECT 1"}'
```

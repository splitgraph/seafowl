#!/bin/bash -ex

SEAFOWL_HOST=${SEAFOWL_HOST-"http://localhost:8080"}
query=$1
etag=$2

# Hash the query
hash=$(echo -n "$query" | sha256sum | cut -f 1 -d " ")

headers=(-H "X-Seafowl-Query: $query")

# Add an ETag (if passed) for cache revalidation
if [[ -n "$etag" ]]; then
  headers+=(-H "If-None-Match: $etag")
fi

curl -i "${headers[@]}" "$SEAFOWL_HOST/q/$hash"

import os
import argparse
from typing import Optional
import sys

from requests import HTTPError
from .seafowl import query
from .types import SeafowlConnectionParams
import json

DEFAULT_ENDPOINT = "http://localhost:8080/q"

parser = argparse.ArgumentParser()
# readonly mode is currently unsupported by the CLI inteface matches the
# nodejs client's.
parser.add_argument(
    "-r",
    "--readonly",
    help="Read only mode (using HTTP GET requests)",
    action="store_true",
)
parser.add_argument(
    "-f",
    "--fromfile",
    help="Read SQL query from file",
    action="store",
    type=str,
    default=None,
)
parser.add_argument("rest", nargs=argparse.REMAINDER)
args = parser.parse_args()

endpoint: str = os.environ["ENDPOINT"] or DEFAULT_ENDPOINT
password: Optional[str] = os.environ["PASSWORD"]

if args.readonly:
    print("Read-only mode currently unsupported!")
    exit(1)


if args.fromfile:
    with open(args.fromfile) as f:
        sql = "; ".join(f.readlines())
else:
    sql = " ".join(args.rest)  # type:ignore
try:
    status_code, data = query(
        SeafowlConnectionParams(endpoint, password, None), sql, append_url_suffix=False
    )
    print(f"code: {status_code}")
    if data:
        print(json.dumps(data, indent=4))
except HTTPError as error:
    print(f"Error: {error.response.status_code}", file=sys.stderr)
    print(error.response.text, file=sys.stderr)
    exit(1)

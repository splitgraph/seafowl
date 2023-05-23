from dataclasses import dataclass
from datetime import datetime
from functools import partial
import json
import math
from numbers import Number
from typing import Any, Dict, List, Optional, Tuple, Union

import requests

from .types import RowInserter, SeafowlConnectionParams

# Default Seafowl DB name that gets queried when we send a request to `seafowl-endpoint/q`
DEFAULT_DB = "default"
DEFAULT_SCHEMA = "public"
URL_SUFFIX = "/q"

SeafowlRow = Dict[str, Any]


def sql_esc_str(s: str) -> str:
    return s.replace("'", "''")


def sql_esc_ident(s: str) -> str:
    return s.replace('"', '""')


def emit_value(value: Any) -> str:
    if value is None:
        return "NULL"

    if isinstance(value, float):
        if math.isnan(value):
            return "NULL"
        return f"{value:.20f}"

    if isinstance(value, Number) and not isinstance(value, bool):
        return str(value)

    if isinstance(value, datetime):
        return f"'{value.isoformat()}'"

    quoted = sql_esc_str(str(value))
    return f"'{quoted}'"


def none_for_empty(s: str) -> Optional[str]:
    return s if len(s) > 0 else None


def get_db_endpoint(endpoint: str, database_name: Optional[str] = DEFAULT_DB) -> str:
    return (
        endpoint
        if (database_name is None) or database_name == DEFAULT_DB
        else endpoint + "/" + database_name
    )


def query(
    conn: SeafowlConnectionParams,
    sql: str,
    raise_for_status: bool = True,
    append_url_suffix: bool = True,
) -> Tuple[int, Union[List[SeafowlRow], Optional[str]]]:
    url = get_db_endpoint(conn.url, conn.database or DEFAULT_DB)
    headers = {"Content-Type": "application/json"}
    if conn.secret:
        headers["Authorization"] = f"Bearer {conn.secret}"
    response = requests.post(
        f"{url}{URL_SUFFIX if append_url_suffix else ''}",
        json={"query": sql},
        headers=headers,
    )
    if raise_for_status:
        response.raise_for_status()
    return (
        response.status_code,
        [json.loads(t) for t in response.text.strip().split("\n")]
        if response.ok and response.text
        else none_for_empty(response.text),
    )

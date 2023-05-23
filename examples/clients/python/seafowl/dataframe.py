from contextlib import contextmanager
from functools import partial
from typing import Any, Generator, List, Optional
import pandas as pd
from sqlalchemy import create_engine
import tempfile
import requests

# mypy doesn't recognize that pandas.io.sql exports pandasSQL_builder
from pandas.io.sql import pandasSQL_builder, SQLTable, PandasSQL  # type:ignore

from .seafowl import (
    DEFAULT_SCHEMA,
    emit_value,
    get_db_endpoint,
    sql_esc_ident,
    query,
)
from .types import QualifiedTableName, RowInserter, SeafowlConnectionParams


@contextmanager
def with_pandas_sql() -> Generator[PandasSQL, None, None]:
    engine = create_engine("sqlite://", echo=True)
    with pandasSQL_builder(engine, need_transaction=False) as pandas_sql:
        yield pandas_sql


def get_table(
    pandas_sql: PandasSQL, schema_name, table_name: str, data: pd.DataFrame
) -> SQLTable:
    return SQLTable(
        table_name,
        pandas_sql,
        frame=data,
        index=True,
        schema=schema_name,
    )


def get_sql_create_statement(table: SQLTable) -> str:
    return table.sql_schema()


def make_insert_request(
    insert_row: RowInserter,
    table: SQLTable,
    _conn: Any,
    keys: List[str],
    data_iter: Any,
) -> int:
    fields = ", ".join([f'"{sql_esc_ident(k)}"' for k in keys])
    for row in data_iter:
        sql = f'INSERT INTO "{sql_esc_ident(table.schema)}"."{sql_esc_ident(table.name)}" ({fields}) VALUES ({", ".join([emit_value(val) for val in row])});'
        insert_row(sql)
    return 0


def execute_insert(
    table: SQLTable, insert_row: RowInserter, chunksize: Optional[int] = None
) -> int | None:
    # mypy doesn't recognize that the 'method' argument can be a function, so ignore type errors
    return table.insert(
        chunksize=chunksize,
        method=partial(make_insert_request, insert_row),  # type:ignore
    )


def make_row_inserter(conn: SeafowlConnectionParams) -> RowInserter:
    return partial(query, conn)


def export_to_parquet_and_upload(
    data: pd.DataFrame,
    conn: SeafowlConnectionParams,
    destination_table: QualifiedTableName,
) -> None:
    url = f"{get_db_endpoint(conn.url, conn.database)}/upload/{destination_table.schema or DEFAULT_SCHEMA}/{destination_table.table}"
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmpfile:
        data.to_parquet(tmpfile, compression="gzip")
        tmpfile.flush()
        tmpfile.seek(0)
        headers = {}
        if conn.secret:
            headers["Authorization"] = f"Bearer {conn.secret}"
        requests.post(url, files={"data": ("upload.parquet", tmpfile)}, headers=headers)


def create_table_and_insert(
    data: pd.DataFrame,
    conn: SeafowlConnectionParams,
    destination_table: QualifiedTableName,
) -> None:
    with with_pandas_sql() as pandas_sql:
        table = get_table(
            pandas_sql,
            destination_table.schema or DEFAULT_SCHEMA,
            destination_table.table,
            data,
        )
        # create table
        query(conn, get_sql_create_statement(table))
        execute_insert(table, make_row_inserter(conn))


def dataframe_to_seafowl(
    data: pd.DataFrame,
    conn: SeafowlConnectionParams,
    destination_table: QualifiedTableName,
) -> None:
    try:
        # if pyarrow can't be imported fall back to uploading rows with INSERT.
        import pyarrow  # type:ignore

        export_to_parquet_and_upload(data, conn, destination_table)
    except ImportError:
        create_table_and_insert(data, conn, destination_table)

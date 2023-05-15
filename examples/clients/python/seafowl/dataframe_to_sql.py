from contextlib import contextmanager
from functools import partial
from typing import Any, Callable, Generator, List
import pandas as pd
from sqlalchemy import create_engine
# mypy doesn't recognize that pandas.io.sql exports pandasSQL_builder
from pandas.io.sql import pandasSQL_builder, SQLTable, PandasSQL  # type:ignore
import numpy as np

from .seafowl import DEFAULT_DB, DEFAULT_SCHEMA, emit_value, sql_esc_ident, query
from .types import QualifiedTableName, RowInserter, SeafowlConnectionParams

@contextmanager
def with_pandas_sql() -> Generator[PandasSQL, None, None]:
  engine=create_engine('sqlite://',echo=True)
  with pandasSQL_builder(engine, need_transaction=False) as pandas_sql:
    yield pandas_sql

def get_table(pandas_sql: PandasSQL, schema_name, table_name: str, data: pd.DataFrame)->SQLTable:
    return SQLTable(
        table_name,
        pandas_sql,
        frame=data,
        index=True,
        #if_exists='replace',
        #index_label=index_label,
        schema=schema_name,
        #dtype=dtype,
    )

def get_sql_create_statement(table:SQLTable)->str:
  return table.sql_schema()

def make_insert_request(insert_row:RowInserter, table:SQLTable, _conn:Any, keys:List[str], data_iter:Any)->int:
   fields = ", ".join([f'"{sql_esc_ident(k)}"' for k in keys])
   for row in data_iter:
    sql = f'INSERT INTO "{sql_esc_ident(table.schema)}"."{sql_esc_ident(table.name)}" ({fields}) VALUES ({", ".join([emit_value(val) for val in row])});'
    insert_row(sql)
   return 0

def execute_insert(table:SQLTable, insert_row:RowInserter, chunksize: int | None = None)->int|None:
    # mypy doesn't recognize that the 'method' argument can be a function, so ignore type errors
    return table.insert(chunksize=chunksize, method=partial(make_insert_request, insert_row)) # type:ignore

def make_row_inserter(conn: SeafowlConnectionParams) -> RowInserter:
    return partial(query, conn)

def dataframe_to_seafowl(
    data: pd.DataFrame,
    conn: SeafowlConnectionParams,
    destination_table: QualifiedTableName
) -> None:
    with with_pandas_sql() as pandas_sql:
        table = get_table(
            pandas_sql,
            destination_table.schema or DEFAULT_SCHEMA,
            destination_table.table,
            data)
        # create table
        query(conn, get_sql_create_statement(table))
        # insert rows from dataframe
        execute_insert(table, make_row_inserter(conn))

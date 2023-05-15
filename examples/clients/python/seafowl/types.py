from dataclasses import dataclass
from typing import Any, Callable, Optional


RowInserter = Callable[[str], Any]


@dataclass
class SeafowlConnectionParams:
    url: str
    secret: Optional[str]
    database: Optional[str]


@dataclass
class QualifiedTableName:
    schema: Optional[str]
    table: str

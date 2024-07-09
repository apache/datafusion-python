from __future__ import annotations

import datafusion._internal as df_internal

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow


class Catalog:
    def __init__(self, catalog: df_internal.Catalog) -> None:
        self.catalog = catalog

    def names(self) -> list[str]:
        return self.catalog.names()

    def database(self, name: str = "public") -> Database:
        return Database(self.catalog.database(name))


class Database:
    def __init__(self, db: df_internal.Database) -> None:
        self.db = db

    def names(self) -> set[str]:
        return self.db.names()

    def table(self, name: str) -> Table:
        return Table(self.db.table(name))


class Table:
    def __init__(self, table: df_internal.Table) -> None:
        self.table = table

    def schema(self) -> pyarrow.Schema:
        return self.table.schema()

    @property
    def kind(self) -> str:
        return self.table.kind()

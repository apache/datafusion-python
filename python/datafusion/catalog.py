# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Data catalog providers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Protocol

import datafusion._internal as df_internal

if TYPE_CHECKING:
    import pyarrow as pa

try:
    from warnings import deprecated  # Python 3.13+
except ImportError:
    from typing_extensions import deprecated  # Python 3.12


__all__ = [
    "Catalog",
    "CatalogProvider",
    "Schema",
    "SchemaProvider",
    "Table",
]


class Catalog:
    """DataFusion data catalog."""

    def __init__(self, catalog: df_internal.catalog.RawCatalog) -> None:
        """This constructor is not typically called by the end user."""
        self.catalog = catalog

    def __repr__(self) -> str:
        """Print a string representation of the catalog."""
        return self.catalog.__repr__()

    def names(self) -> set[str]:
        """This is an alias for `schema_names`."""
        return self.schema_names()

    def schema_names(self) -> set[str]:
        """Returns the list of schemas in this catalog."""
        return self.catalog.schema_names()

    @staticmethod
    def memory_catalog() -> Catalog:
        """Create an in-memory catalog provider."""
        catalog = df_internal.catalog.RawCatalog.memory_catalog()
        return Catalog(catalog)

    def schema(self, name: str = "public") -> Schema:
        """Returns the database with the given ``name`` from this catalog."""
        schema = self.catalog.schema(name)

        return (
            Schema(schema)
            if isinstance(schema, df_internal.catalog.RawSchema)
            else schema
        )

    @deprecated("Use `schema` instead.")
    def database(self, name: str = "public") -> Schema:
        """Returns the database with the given ``name`` from this catalog."""
        return self.schema(name)

    def register_schema(self, name, schema) -> Schema | None:
        """Register a schema with this catalog."""
        if isinstance(schema, Schema):
            return self.catalog.register_schema(name, schema._raw_schema)
        return self.catalog.register_schema(name, schema)

    def deregister_schema(self, name: str, cascade: bool = True) -> Schema | None:
        """Deregister a schema from this catalog."""
        return self.catalog.deregister_schema(name, cascade)


class Schema:
    """DataFusion Schema."""

    def __init__(self, schema: df_internal.catalog.RawSchema) -> None:
        """This constructor is not typically called by the end user."""
        self._raw_schema = schema

    def __repr__(self) -> str:
        """Print a string representation of the schema."""
        return self._raw_schema.__repr__()

    @staticmethod
    def memory_schema() -> Schema:
        """Create an in-memory schema provider."""
        schema = df_internal.catalog.RawSchema.memory_schema()
        return Schema(schema)

    def names(self) -> set[str]:
        """This is an alias for `table_names`."""
        return self.table_names()

    def table_names(self) -> set[str]:
        """Returns the list of all tables in this schema."""
        return self._raw_schema.table_names

    def table(self, name: str) -> Table:
        """Return the table with the given ``name`` from this schema."""
        return Table(self._raw_schema.table(name))

    def register_table(self, name, table) -> None:
        """Register a table provider in this schema."""
        if isinstance(table, Table):
            return self._raw_schema.register_table(name, table.table)
        return self._raw_schema.register_table(name, table)

    def deregister_table(self, name: str) -> None:
        """Deregister a table provider from this schema."""
        return self._raw_schema.deregister_table(name)


@deprecated("Use `Schema` instead.")
class Database(Schema):
    """See `Schema`."""


class Table:
    """DataFusion table."""

    def __init__(self, table: df_internal.catalog.RawTable) -> None:
        """This constructor is not typically called by the end user."""
        self.table = table

    def __repr__(self) -> str:
        """Print a string representation of the table."""
        return self.table.__repr__()

    @staticmethod
    def from_dataset(dataset: pa.dataset.Dataset) -> Table:
        """Turn a pyarrow Dataset into a Table."""
        return Table(df_internal.catalog.RawTable.from_dataset(dataset))

    @property
    def schema(self) -> pa.Schema:
        """Returns the schema associated with this table."""
        return self.table.schema

    @property
    def kind(self) -> str:
        """Returns the kind of table."""
        return self.table.kind


class CatalogProvider(ABC):
    """Abstract class for defining a Python based Catalog Provider."""

    @abstractmethod
    def schema_names(self) -> set[str]:
        """Set of the names of all schemas in this catalog."""
        ...

    @abstractmethod
    def schema(self, name: str) -> Schema | None:
        """Retrieve a specific schema from this catalog."""
        ...

    def register_schema(  # noqa: B027
        self, name: str, schema: SchemaProviderExportable | SchemaProvider | Schema
    ) -> None:
        """Add a schema to this catalog.

        This method is optional. If your catalog provides a fixed list of schemas, you
        do not need to implement this method.
        """

    def deregister_schema(self, name: str, cascade: bool) -> None:  # noqa: B027
        """Remove a schema from this catalog.

        This method is optional. If your catalog provides a fixed list of schemas, you
        do not need to implement this method.

        Args:
            name: The name of the schema to remove.
            cascade: If true, deregister the tables within the schema.
        """


class SchemaProvider(ABC):
    """Abstract class for defining a Python based Schema Provider."""

    def owner_name(self) -> str | None:
        """Returns the owner of the schema.

        This is an optional method. The default return is None.
        """
        return None

    @abstractmethod
    def table_names(self) -> set[str]:
        """Set of the names of all tables in this schema."""
        ...

    @abstractmethod
    def table(self, name: str) -> Table | None:
        """Retrieve a specific table from this schema."""
        ...

    def register_table(self, name: str, table: Table) -> None:  # noqa: B027
        """Add a table from this schema.

        This method is optional. If your schema provides a fixed list of tables, you do
        not need to implement this method.
        """

    def deregister_table(self, name, cascade: bool) -> None:  # noqa: B027
        """Remove a table from this schema.

        This method is optional. If your schema provides a fixed list of tables, you do
        not need to implement this method.
        """

    @abstractmethod
    def table_exist(self, name: str) -> bool:
        """Returns true if the table exists in this schema."""
        ...


class SchemaProviderExportable(Protocol):
    """Type hint for object that has __datafusion_schema_provider__ PyCapsule.

    https://docs.rs/datafusion/latest/datafusion/catalog/trait.SchemaProvider.html
    """

    def __datafusion_schema_provider__(self) -> object: ...

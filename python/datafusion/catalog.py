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
from typing import TYPE_CHECKING, Any, Protocol

import warnings

import datafusion._internal as df_internal
from datafusion._internal import EXPECTED_PROVIDER_MSG
from datafusion.utils import _normalize_table_provider

if TYPE_CHECKING:
    import pyarrow as pa

    from datafusion.context import TableProviderExportable

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

    def register_schema(
        self,
        name: str,
        schema: Schema | SchemaProvider | SchemaProviderExportable,
    ) -> Schema | None:
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

    def register_table(
        self, name: str, table: Table | TableProviderExportable | Any
    ) -> None:
        """Register a table or table provider in this schema.

        Objects implementing ``__datafusion_table_provider__`` are also supported
        and treated as table provider instances.
        """
        provider = _normalize_table_provider(table)
        return self._raw_schema.register_table(name, provider)

    def deregister_table(self, name: str) -> None:
        """Deregister a table provider from this schema."""
        return self._raw_schema.deregister_table(name)


@deprecated("Use `Schema` instead.")
class Database(Schema):
    """See `Schema`."""


_InternalRawTable = df_internal.catalog.RawTable
_InternalTableProvider = df_internal.TableProvider

# Keep in sync with ``datafusion._internal.TableProvider.from_view``.
_FROM_VIEW_WARN_STACKLEVEL = 2


class Table:
    """DataFusion table or table provider wrapper."""

    __slots__ = ("_table",)

    def __init__(
        self,
        table: _InternalRawTable | _InternalTableProvider | Table,
    ) -> None:
        """Wrap a low level table or table provider."""

        if isinstance(table, Table):
            table = table.table

        if not isinstance(table, (_InternalRawTable, _InternalTableProvider)):
            raise TypeError(EXPECTED_PROVIDER_MSG)

        self._table = table

    def __getattribute__(self, name: str) -> Any:
        """Restrict provider-specific helpers to compatible tables."""

        if name == "__datafusion_table_provider__":
            table = object.__getattribute__(self, "_table")
            if not hasattr(table, "__datafusion_table_provider__"):
                raise AttributeError(name)
        return object.__getattribute__(self, name)

    def __repr__(self) -> str:
        """Print a string representation of the table."""
        return repr(self._table)

    @property
    def table(self) -> _InternalRawTable | _InternalTableProvider:
        """Return the wrapped low level table object."""
        return self._table

    @classmethod
    def from_dataset(cls, dataset: pa.dataset.Dataset) -> Table:
        """Turn a :mod:`pyarrow.dataset` ``Dataset`` into a :class:`Table`."""

        return cls(_InternalRawTable.from_dataset(dataset))

    @classmethod
    def from_capsule(cls, capsule: Any) -> Table:
        """Create a :class:`Table` from a PyCapsule exported provider."""

        provider = _InternalTableProvider.from_capsule(capsule)
        return cls(provider)

    @classmethod
    def from_dataframe(cls, df: Any) -> Table:
        """Create a :class:`Table` from tabular data."""

        from datafusion.dataframe import DataFrame as DataFrameWrapper

        dataframe = df if isinstance(df, DataFrameWrapper) else DataFrameWrapper(df)
        return dataframe.into_view()

    @classmethod
    def from_view(cls, df: Any) -> Table:
        """Deprecated helper for constructing tables from views."""

        from datafusion.dataframe import DataFrame as DataFrameWrapper

        if isinstance(df, DataFrameWrapper):
            df = df.df

        provider = _InternalTableProvider.from_view(df)
        warnings.warn(
            "Table.from_view is deprecated; use DataFrame.into_view or "
            "Table.from_dataframe instead.",
            category=DeprecationWarning,
            stacklevel=_FROM_VIEW_WARN_STACKLEVEL,
        )
        return cls(provider)

    @property
    def schema(self) -> pa.Schema:
        """Returns the schema associated with this table."""
        return self._table.schema

    @property
    def kind(self) -> str:
        """Returns the kind of table."""
        return self._table.kind

    def __datafusion_table_provider__(self) -> Any:
        """Expose the wrapped provider for FFI integrations."""

        exporter = getattr(self._table, "__datafusion_table_provider__", None)
        if exporter is None:
            msg = "Underlying object does not export __datafusion_table_provider__()"
            raise AttributeError(msg)
        return exporter()


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

    def register_table(  # noqa: B027
        self, name: str, table: Table | TableProviderExportable | Any
    ) -> None:
        """Add a table to this schema.

        This method is optional. If your schema provides a fixed list of tables, you do
        not need to implement this method.

        Objects implementing ``__datafusion_table_provider__`` are also supported
        and treated as table provider instances.
        """

    def deregister_table(self, name: str, cascade: bool) -> None:  # noqa: B027
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

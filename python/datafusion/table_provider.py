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
"""Wrapper helpers for :mod:`datafusion._internal.TableProvider`."""

from __future__ import annotations

import warnings
from typing import Any

import datafusion._internal as df_internal
from datafusion._internal import EXPECTED_PROVIDER_MSG

_InternalTableProvider = df_internal.TableProvider


class TableProvider:
    """High level wrapper around :mod:`datafusion._internal.TableProvider`."""

    __slots__ = ("_table_provider",)

    def __init__(self, table_provider: _InternalTableProvider) -> None:
        """Wrap a low level :class:`~datafusion._internal.TableProvider`."""
        if isinstance(table_provider, TableProvider):
            table_provider = table_provider._table_provider

        if not isinstance(table_provider, _InternalTableProvider):
            raise TypeError(EXPECTED_PROVIDER_MSG)

        self._table_provider = table_provider

    @classmethod
    def from_capsule(cls, capsule: Any) -> TableProvider:
        """Create a :class:`TableProvider` from a PyCapsule."""
        provider = _InternalTableProvider.from_capsule(capsule)
        return cls(provider)

    @classmethod
    def from_dataframe(cls, df: Any) -> TableProvider:
        """Create a :class:`TableProvider` from a :class:`DataFrame`."""
        from datafusion.dataframe import DataFrame as DataFrameWrapper

        if isinstance(df, DataFrameWrapper):
            df = df.df

        provider = _InternalTableProvider.from_dataframe(df)
        return cls(provider)

    @classmethod
    def from_view(cls, df: Any) -> TableProvider:
        """Deprecated.

        Use :meth:`DataFrame.into_view` or :meth:`TableProvider.from_dataframe`.
        """
        from datafusion.dataframe import DataFrame as DataFrameWrapper

        if isinstance(df, DataFrameWrapper):
            df = df.df

        provider = _InternalTableProvider.from_view(df)
        warnings.warn(
            "TableProvider.from_view is deprecated; use DataFrame.into_view or "
            "TableProvider.from_dataframe instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return cls(provider)

    # ------------------------------------------------------------------
    # passthrough helpers
    # ------------------------------------------------------------------
    def __getattr__(self, name: str) -> Any:
        """Delegate attribute lookup to the wrapped provider."""
        return getattr(self._table_provider, name)

    def __dir__(self) -> list[str]:
        """Expose delegated attributes via :func:`dir`."""
        return dir(self._table_provider) + super().__dir__()

    def __repr__(self) -> str:  # pragma: no cover - simple delegation
        """Return a representation of the wrapped provider."""
        return repr(self._table_provider)

    def __datafusion_table_provider__(self) -> Any:
        """Expose the wrapped provider for FFI integrations."""
        return self._table_provider.__datafusion_table_provider__()


__all__ = ["TableProvider"]

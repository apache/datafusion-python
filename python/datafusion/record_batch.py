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

"""This module provides the classes for handling record batches.

These are typically the result of dataframe
:py:func:`datafusion.dataframe.execute_stream` operations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa
    import typing_extensions

    import datafusion._internal as df_internal


class RecordBatch:
    """This class is essentially a wrapper for :py:class:`pa.RecordBatch`."""

    def __init__(self, record_batch: df_internal.RecordBatch) -> None:
        """This constructor is generally not called by the end user.

        See the :py:class:`RecordBatchStream` iterator for generating this class.
        """
        self.record_batch = record_batch

    def to_pyarrow(self) -> pa.RecordBatch:
        """Convert to :py:class:`pa.RecordBatch`."""
        return self.record_batch.to_pyarrow()

    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> tuple[object, object]:
        """Export the record batch via the Arrow C Data Interface.

        This allows zero-copy interchange with libraries that support the
        `Arrow PyCapsule interface <https://arrow.apache.org/docs/format/
        CDataInterface/PyCapsuleInterface.html>`_.

        Args:
            requested_schema: Attempt to provide the record batch using this
                schema. Only straightforward projections such as column
                selection or reordering are applied.

        Returns:
            Two Arrow PyCapsule objects representing the ``ArrowArray`` and
            ``ArrowSchema``.
        """
        return self.record_batch.__arrow_c_array__(requested_schema)


class RecordBatchStream:
    """This class represents a stream of record batches.

    These are typically the result of a
    :py:func:`~datafusion.dataframe.DataFrame.execute_stream` operation.
    """

    def __init__(self, record_batch_stream: df_internal.RecordBatchStream) -> None:
        """This constructor is typically not called by the end user."""
        self.rbs = record_batch_stream

    def next(self) -> RecordBatch:
        """See :py:func:`__next__` for the iterator function."""
        return next(self)

    async def __anext__(self) -> RecordBatch:
        """Return the next :py:class:`RecordBatch` in the stream asynchronously."""
        next_batch = await self.rbs.__anext__()
        return RecordBatch(next_batch)

    def __next__(self) -> RecordBatch:
        """Return the next :py:class:`RecordBatch` in the stream."""
        next_batch = next(self.rbs)
        return RecordBatch(next_batch)

    def __aiter__(self) -> typing_extensions.Self:
        """Return an asynchronous iterator over record batches."""
        return self

    def __iter__(self) -> typing_extensions.Self:
        """Return an iterator over record batches."""
        return self

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow
    import datafusion._internal as df_internal


class RecordBatch:
    def __init__(self, record_batch: df_internal.RecordBatch) -> None:
        self.record_batch = record_batch

    def to_pyarrow(self) -> pyarrow.RecordBatch:
        return self.record_batch.to_pyarrow()


class RecordBatchStream:
    def __init__(self, record_batch_stream: df_internal.RecordBatchStream) -> None:
        self.rbs = record_batch_stream

    def next(self) -> RecordBatch | None:
        try:
            next_batch = next(self)
        except StopIteration:
            return None

        return next_batch

    def __next__(self) -> RecordBatch | None:
        next_batch = next(self.rbs)
        return RecordBatch(next_batch) if next_batch is not None else None

    def __iter__(self) -> RecordBatchStream:
        return self

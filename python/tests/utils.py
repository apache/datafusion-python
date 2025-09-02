"""Testing-only helpers for datafusion-python.

This module contains utilities used by the test-suite that should not be
exposed as part of the public API. Keep the implementation minimal and
documented so reviewers can easily see it's test-only.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import DataFrame
    from datafusion.context import SessionContext


def range_table(
    ctx: SessionContext,
    start: int,
    stop: int | None = None,
    step: int = 1,
    partitions: int | None = None,
) -> DataFrame:
    """Create a DataFrame containing a sequence of numbers using SQL RANGE.

    This mirrors the previous ``SessionContext.range`` convenience method but
    lives in a testing-only module so it doesn't expand the public surface.

    Args:
        ctx: SessionContext instance to run the SQL against.
        start: Starting value for the sequence or exclusive stop when ``stop``
            is ``None``.
        stop: Exclusive upper bound of the sequence.
        step: Increment between successive values.
        partitions: Optional number of partitions for the generated data.

    Returns:
        DataFrame produced by the range table function.
    """
    if stop is None:
        start, stop = 0, start

    parts = f", {int(partitions)}" if partitions is not None else ""
    sql = f"SELECT * FROM range({int(start)}, {int(stop)}, {int(step)}{parts})"
    return ctx.sql(sql)

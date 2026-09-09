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

"""One session factory, used by the driver and by every worker.

This module is the answer to the question the rest of the example exists to
raise: *what exactly does a worker have to reproduce?*

There is no way to snapshot a :class:`~datafusion.SessionContext` and restore
it somewhere else. :class:`~datafusion.SessionConfig` is write-only from
Python, and ``information_schema.df_settings`` -- which can be read -- lists
``datafusion.runtime.*`` keys that have no config namespace to set them back
into. So worker parity cannot be automated away; it has to be *built the same
way twice*, from data small enough to put in a message.

That is what :class:`SessionSpec` is, and why both sides call
:func:`build_session` rather than each assembling a context of their own.
Anything a query depends on that is not in the spec is a bug waiting for a
worker to find it.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

import dfx_storage
import dfx_udfs
from datafusion import SessionConfig, SessionContext, udaf, udf, udwf

from dfx_engine import _internal

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ["SessionSpec", "build_session", "expected_codec_ids"]


@dataclasses.dataclass(frozen=True)
class SessionSpec:
    """Everything needed to rebuild an equivalent session.

    Small and explicit on purpose: it travels to workers as JSON, so anything
    that cannot be written down here cannot be relied on by a shipped plan.
    """

    tables: Mapping[str, str]
    """Table name to the directory ``dfx_storage`` should scan for it."""

    shuffle_dir: str
    """Where stages exchange results. Empty means "run in this process"."""

    target_partitions: int = 2
    """Pinned rather than defaulted to the core count.

    Two machines with different core counts would otherwise disagree about
    how many partitions a re-planned query has.
    """

    def to_json(self) -> dict:
        """Render for a worker's task envelope."""
        return {
            "tables": dict(self.tables),
            "shuffle_dir": self.shuffle_dir,
            "target_partitions": self.target_partitions,
            "codec_ids": expected_codec_ids(),
        }

    @staticmethod
    def from_json(payload: Mapping) -> SessionSpec:
        """Rebuild from a task envelope, ignoring the codec ids.

        The ids are checked against the session after it is built rather than
        used to build it -- see :func:`build_session`.
        """
        return SessionSpec(
            tables=payload["tables"],
            shuffle_dir=payload["shuffle_dir"],
            target_partitions=payload["target_partitions"],
        )


def expected_codec_ids() -> list[str]:
    """The physical codec ids a correctly-built session carries.

    Read from the libraries rather than written out here, so adding a library
    to :func:`build_session` and forgetting this list is not possible.
    """
    return sorted(
        [
            dfx_storage.DfxStorageExtension.physical_codec_id(),
            _internal.DfxEngineExtension.physical_codec_id(),
            "dfx_udfs.physical.v1",
        ]
    )


def build_session(
    spec: SessionSpec,
) -> tuple[
    SessionContext, _internal.DfxEngineExtension, dfx_storage.DfxStorageExtension
]:
    """Build the session both the driver and the workers run on.

    The order below is not arbitrary:

    1. The engine's config extension is registered on the ``SessionConfig``
       *before* the context exists, because ``dfx_engine.shuffle_dir`` cannot
       be set into a namespace that has not been declared.
    2. The two bundle libraries go in through a single
       :meth:`~datafusion.SessionContext.with_extensions` call, so their
       codecs are all installed before the engine's planner is bound. Passing
       them in separate calls would bind the planner against a partial chain.
    3. ``dfx_udfs`` is installed by hand, because it ships no bundle hook.
       Its codecs must go on before anything serializes a plan referencing its
       functions.
    4. Tables are registered last. Registration order does not matter to
       ``with_extensions``, but doing it after means the same code path builds
       a driver and a worker.

    Returns the context plus the two bundles, whose counters let a test assert
    which codec carried which node.
    """
    engine = _internal.DfxEngineExtension()
    storage = dfx_storage.DfxStorageExtension()

    config = SessionConfig().with_target_partitions(spec.target_partitions)
    # Declares the `dfx_engine` namespace. Without this, setting
    # `dfx_engine.shuffle_dir` raises rather than being ignored.
    config = config.with_extension(_internal.DfxEngineConfig(spec.shuffle_dir))

    ctx = SessionContext(config)
    ctx = ctx.with_extensions(storage, engine)

    # The manual path, for the library that has no bundle. Two codec installs
    # and three registrations, in place of one call.
    observations = dfx_udfs.CodecObservations()
    ctx = ctx.with_logical_extension_codec(observations.logical_codec())
    ctx = ctx.with_physical_extension_codec(observations.physical_codec())
    ctx.register_udf(udf(dfx_udfs.NetRevenueUDF()))
    ctx.register_udaf(udaf(dfx_udfs.WeightedAvgUDAF()))
    ctx.register_udwf(udwf(dfx_udfs.RevenueRankUDWF()))

    for name, directory in spec.tables.items():
        ctx.register_table(name, dfx_storage.PartitionedParquetTable(directory))

    installed = sorted(ctx.physical_extension_codec_ids())
    expected = expected_codec_ids()
    if installed != expected:
        message = (
            f"session codec ids {installed} do not match the expected "
            f"{expected}; a plan encoded elsewhere will fail to decode"
        )
        raise RuntimeError(message)

    return ctx, engine, storage

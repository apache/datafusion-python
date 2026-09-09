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

"""One worker: rebuild the session, decode one stage, run one partition.

Run as ``python -m dfx_engine.worker <envelope.json>``. Started as a fresh
interpreter rather than a :mod:`multiprocessing` child on purpose:

- The tokio runtime behind these extensions is a process-global, so ``fork``
  is unsafe. ``spawn`` would be fine but buys nothing here.
- Launching ``sys.executable`` makes the Python minor version match the
  driver's by construction. Inline Python UDFs travel as cloudpickle payloads
  stamped with the sender's ``(major, minor)``, and a mismatch is a hard
  error -- so a hardcoded ``python`` on ``PATH`` would be a real bug.

The order of operations in :func:`main` is the interesting part, and every
step is there because getting it wrong fails somewhere unhelpful.
"""

from __future__ import annotations

import json
import pathlib
import sys

from datafusion.plan import ExecutionPlan

from dfx_engine import _internal
from dfx_engine.session import SessionSpec, build_session


def run_task(envelope: dict) -> int:
    """Execute one ``(stage, partition)`` and publish the result.

    Returns the number of rows written.
    """
    spec = SessionSpec.from_json(envelope["spec"])
    partition = envelope["partition"]

    # 1. Build the session exactly as the driver did. Anything the driver
    #    relied on that is not in the spec is missing here.
    ctx, _engine, _storage = build_session(spec)

    # 2. Check the codec ids *before* decoding. Without this the failure is a
    #    decode error naming a codec id, which is legible but arrives after
    #    the work of building a session; with it the mismatch is reported
    #    against the envelope that caused it.
    expected = sorted(envelope["spec"]["codec_ids"])
    installed = sorted(ctx.physical_extension_codec_ids())
    if installed != expected:
        message = f"worker codec ids {installed} do not match driver's {expected}"
        raise RuntimeError(message)

    # 3. Decode. The stage node's shuffle directory travels inside the plan,
    #    so the worker cannot write somewhere the driver will not look.
    plan = ExecutionPlan.from_bytes(ctx, pathlib.Path(envelope["plan"]).read_bytes())

    # 4. Bounds-check before executing. A plan's partition count is a property
    #    of the plan, not of the spec, so a driver that miscounted is caught
    #    here rather than deep inside a scan.
    if partition >= plan.partition_count:
        message = (
            f"partition {partition} is out of range for a stage with "
            f"{plan.partition_count} partition(s)"
        )
        raise RuntimeError(message)

    # 5. Execute, and drain the stream. The stage node finds no result file
    #    for this partition -- this worker is the one producing it -- so it
    #    computes its child and writes the file as the batches go past.
    #    Draining is what makes that happen: the node does the work lazily,
    #    so a caller that dropped the stream would publish nothing.
    rows = sum(batch.to_pyarrow().num_rows for batch in ctx.execute(plan, partition))

    published = pathlib.Path(
        _internal.partition_path(spec.shuffle_dir, envelope["stage_id"], partition)
    )
    if not published.exists():
        message = f"stage partition {partition} produced no file at {published}"
        raise RuntimeError(message)

    return rows


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 1:
        sys.stderr.write("usage: python -m dfx_engine.worker <envelope.json>\n")
        return 2

    envelope = json.loads(pathlib.Path(argv[0]).read_text())
    rows = run_task(envelope)
    # Read back by the driver, so it can report what each worker did.
    print(json.dumps({"partition": envelope["partition"], "rows": rows}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

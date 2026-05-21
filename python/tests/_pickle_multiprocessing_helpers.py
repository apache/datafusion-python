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

# The leading underscore is load-bearing: pytest with --import-mode=importlib
# (used in CI) assigns synthetic module names to test modules, which breaks
# subprocess imports during multiprocessing. An underscore-prefixed module is
# not collected as a test module, so it imports under its normal __name__
# inside worker processes.

from __future__ import annotations

import os
import tempfile
import time
import traceback
from pathlib import Path

# Diagnostic log path for multiprocessing worker timing.
# Workers write here so a CI-side `cat` after a job timeout can show
# where each worker stalled (e.g. inside `import datafusion`). Lives in
# the system temp dir so it persists across Pool worker exits and is
# readable by a follow-up workflow step. Override via env var when
# debugging locally.
_DIAG_LOG = Path(
    os.environ.get(
        "DF_MP_DIAG_LOG",
        str(Path(tempfile.gettempdir()) / "df_mp_worker_diag.log"),
    )
)


def _diag(event: str) -> None:
    """Append a diagnostic line: timestamp, pid, parent pid, event.

    Opens / flushes / closes per call so a hang mid-import still leaves
    a partial trail on disk. Parent pid distinguishes forkserver-born
    workers (parent = forkserver) from spawn-born workers (parent =
    main pytest process).
    """
    try:
        with _DIAG_LOG.open("a", encoding="utf-8") as fh:
            fh.write(
                f"{time.time():.3f} pid={os.getpid()} ppid={os.getppid()} {event}\n"
            )
            fh.flush()
            os.fsync(fh.fileno())
    except OSError:
        # Best-effort diagnostic; never let logging itself break a test.
        pass


_diag("helpers module: starting imports")
import pyarrow as pa  # noqa: E402

_diag("helpers module: pyarrow imported")
from datafusion import SessionContext, udf  # noqa: E402

_diag("helpers module: datafusion imported")
from datafusion.ipc import clear_worker_ctx, set_worker_ctx  # noqa: E402

_diag("helpers module: all imports complete")


def make_double_udf():
    """Build the canonical UDF used in the multiprocessing tests."""
    return udf(
        lambda arr: pa.array([(v.as_py() or 0) * 2 for v in arr]),
        [pa.int64()],
        pa.int64(),
        volatility="immutable",
        name="double",
    )


def make_times_seven_udf():
    """Closure-capturing UDF — verifies cloudpickle preserves closed-over state."""
    multiplier = 7

    def fn(arr):
        return pa.array([(v.as_py() or 0) * multiplier for v in arr])

    return udf(
        fn,
        [pa.int64()],
        pa.int64(),
        volatility="immutable",
        name="times_seven",
    )


def init_worker_empty():
    """Pool initializer: install an empty SessionContext (no UDFs)."""
    set_worker_ctx(SessionContext())


def init_worker_clear():
    """Pool initializer: explicitly clear any prior worker context."""
    clear_worker_ctx()


def diag_init():
    """Pool initializer used by the diagnostic-instrumented tests.

    Logs that a worker process is alive and has finished its module
    imports. If this line never appears for a given pid, the hang is
    inside import / Rust extension init (before any task runs).
    """
    _diag("worker init: ready for tasks")


def _read_text(path: str) -> str:
    """Read a /proc file; return ``"<unreadable>"`` if not accessible."""
    try:
        return Path(path).read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return "<unreadable>"


def _descendants(root_pid: int) -> list[int]:
    """Return ``root_pid`` plus all descendant pids via /proc/<pid>/task/.../children.

    Linux-only; returns ``[root_pid]`` on platforms without ``/proc``.
    """
    out: list[int] = [root_pid]
    if not Path("/proc").is_dir():
        return out
    queue = [root_pid]
    while queue:
        pid = queue.pop()
        try:
            task_dir = Path(f"/proc/{pid}/task")
            if not task_dir.is_dir():
                continue
            for tdir in task_dir.iterdir():
                children_file = tdir / "children"
                try:
                    children = children_file.read_text(encoding="utf-8").split()
                except OSError:
                    continue
                for child in children:
                    try:
                        cpid = int(child)
                    except ValueError:
                        continue
                    out.append(cpid)
                    queue.append(cpid)
        except OSError:
            continue
    return out


def snapshot_processes(label: str, root_pid: int | None = None) -> None:
    """Dump a process-state snapshot to the diagnostic log.

    For each descendant of ``root_pid`` (default: current process), record
    cmdline, status (``R``/``S``/``D``), wchan (kernel function the task
    is blocked in), and kernel stack. Use this to localize a worker hang:
    a wchan of ``do_futex`` points at a lock; ``poll_schedule_timeout``
    points at a blocking I/O wait; ``do_select`` at multiprocessing's
    pipe read.
    """
    pid = root_pid if root_pid is not None else os.getpid()
    _diag(f"snapshot[{label}] root_pid={pid}")
    for cpid in _descendants(pid):
        cmd = _read_text(f"/proc/{cpid}/cmdline").replace("\x00", " ").strip()
        stat = _read_text(f"/proc/{cpid}/status").splitlines()
        state_line = next((s for s in stat if s.startswith("State:")), "State: ?")
        wchan = _read_text(f"/proc/{cpid}/wchan")
        stack = _read_text(f"/proc/{cpid}/stack")
        _diag(f"snapshot[{label}] pid={cpid} {state_line} wchan={wchan} cmd={cmd!r}")
        if stack and stack != "<unreadable>":
            for line in stack.splitlines()[:10]:
                _diag(f"snapshot[{label}] pid={cpid} stack: {line}")


def unpickle_and_describe(blob: bytes) -> str:
    """Unpickle a proto-bytes blob and return its canonical name."""
    import pickle

    _diag("unpickle_and_describe: enter")
    try:
        expr = pickle.loads(blob)  # noqa: S301
        _diag("unpickle_and_describe: pickle.loads done")
        name = expr.canonical_name()
    except BaseException as exc:
        _diag(f"unpickle_and_describe: raised {type(exc).__name__}: {exc}")
        _diag(traceback.format_exc())
        raise
    _diag(f"unpickle_and_describe: returning name={name!r}")
    return name


def unpickle_and_evaluate(blob: bytes, batch: list[int]) -> list[int]:
    """Unpickle an expression and evaluate it against an in-memory batch.

    Returns the result column as a Python list. Used to verify that
    cloudpickled UDFs (including closure state) execute correctly in
    a fresh worker process.
    """
    import pickle

    _diag(f"unpickle_and_evaluate: enter batch_len={len(batch)}")
    try:
        expr = pickle.loads(blob)  # noqa: S301
        _diag("unpickle_and_evaluate: pickle.loads done")
        ctx = SessionContext()
        _diag("unpickle_and_evaluate: SessionContext built")
        df = ctx.from_pydict({"a": batch})
        out = df.with_column("result", expr).select("result")
        _diag("unpickle_and_evaluate: plan built, collecting")
        result = out.to_pydict()["result"]
    except BaseException as exc:
        _diag(f"unpickle_and_evaluate: raised {type(exc).__name__}: {exc}")
        _diag(traceback.format_exc())
        raise
    _diag(f"unpickle_and_evaluate: returning len={len(result)}")
    return result

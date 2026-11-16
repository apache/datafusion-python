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

from __future__ import annotations

import doctest
import inspect
import io

import pyarrow as pa
from datafusion import SessionContext
from datafusion.extensions import PhysicalOptimizerRuleExportable
from datafusion_ffi_example import MyPhysicalOptimizerRule


def test_ffi_physical_optimizer_rule_runs_during_planning():
    """A rule added via add_physical_optimizer_rule is invoked while the
    physical plan is built, and the query still returns correct results."""
    rule = MyPhysicalOptimizerRule()
    ctx = SessionContext()
    ctx.add_physical_optimizer_rule(rule)
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3])],
        names=["a"],
    )
    ctx.register_record_batches("t", [[batch]])

    before = rule.optimize_calls()
    result = ctx.sql("SELECT a FROM t").collect()
    after = rule.optimize_calls()

    assert after > before, (
        f"Expected user FFI physical optimizer rule to fire, "
        f"before={before} after={after}"
    )
    assert result[0].column(0).to_pylist() == [1, 2, 3]


def test_physical_optimizer_rule_docstring_example_still_runs():
    """Run the ``PhysicalOptimizerRuleExportable`` docstring example verbatim.

    The example is marked ``+SKIP`` because the main suite has no built FFI
    extension to import, which is exactly how such an example rots. Here the
    statements are parsed out of the live docstring, the skip is dropped, and
    each one is executed.

    Only the ``+SKIP`` statements are taken. The docstring also carries a
    runnable example above them, which the main suite already executes under
    ``--doctest-modules``; running it again here would prove nothing.

    Only ``ctx`` is supplied, because the skipped statements go on using the
    context the runnable block opened. Everything else resolves for real: a
    renamed class, a changed signature, or a wrong expected output in the
    docstring fails here.
    """
    examples = [
        example
        for example in doctest.DocTestParser().get_examples(
            inspect.getdoc(PhysicalOptimizerRuleExportable)
        )
        if example.options.pop(doctest.SKIP, False)
    ]
    assert examples, "docstring has no skipped examples to check"

    test = doctest.DocTest(
        examples,
        {"ctx": SessionContext()},
        "PhysicalOptimizerRuleExportable",
        None,
        None,
        None,
    )
    output = io.StringIO()
    results = doctest.DocTestRunner().run(test, out=output.write)
    assert results.failed == 0, output.getvalue()

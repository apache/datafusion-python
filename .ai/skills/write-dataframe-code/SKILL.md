<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

---
name: write-dataframe-code
description: Contributor-facing guidance for writing idiomatic datafusion-python DataFrame code inside the repo — examples, docstrings, tests, and benchmark queries. Use when adding or reviewing Python code in this project that builds DataFrames or expressions. Composes on top of the user-facing guide at the repo-root SKILL.md.
argument-hint: [area] (e.g., "tpch", "docstrings", "plan-comparison")
---

# Writing DataFrame Code in datafusion-python

This skill is for contributors working **on** the datafusion-python project
(examples, tests, docstrings, benchmark queries). The primary reference for
**how** to write DataFrame and expression code — imports, data loading, the
DataFrame API, idiomatic patterns, common pitfalls, and the function
catalog — is the repo-root [`SKILL.md`](../../SKILL.md). Read that first.

This file layers on contributor-specific extras:

1. The TPC-H pattern index — which example to use as a template for which API.
2. The plan-comparison workflow — a diagnostic for checking a DataFrame
   translation against a reference SQL query.
3. Docstring conventions enforced by this project (already summarized in
   `CLAUDE.md`; repeated here so the rule is on-hand while writing examples).

## TPC-H pattern index

`examples/tpch/q01..q22*.py` is the largest collection of idiomatic DataFrame
code in the repo. Each query file pairs a DataFrame translation with the
canonical TPC-H reference SQL embedded in the module docstring. When adding
a new example or demo, pick the query that already exercises the pattern
rather than re-deriving from scratch.

| Pattern | Canonical TPC-H example |
|---|---|
| Simple filter + aggregate + sort | `q01_pricing_summary_report.py` |
| Multi-table join with date-range filter | `q03_shipping_priority.py` |
| `DISTINCT` via `.select(...).distinct()` | `q04_order_priority_checking.py` |
| Multi-hop region/nation/customer join | `q05_local_supplier_volume.py` |
| `F.in_list(col, [...])` in place of CASE/array tricks | `q07_volume_shipping.py`, `q12_ship_mode_order_priority.py` |
| Searched `F.when(...).otherwise(...)` against SQL `CASE WHEN` | `q08_market_share.py` |
| Reusing computed expressions as variables | `q09_product_type_profit_measure.py` |
| Window function in place of correlated scalar subquery | `q02_minimum_cost_supplier.py`, `q11_important_stock_identification.py`, `q15_top_supplier.py`, `q17_small_quantity_order.py`, `q22_global_sales_opportunity.py` |
| `F.regexp_like(col, pattern)` for matching | `q16_part_supplier_relationship.py` |
| Compound disjunctive predicate (OR of per-brand conditions) | `q19_discounted_revenue.py` |
| Semi/anti joins for `EXISTS` / `NOT EXISTS` | `q21_suppliers_kept_orders_waiting.py` |
| `F.starts_with(...)` for prefix matching | `q20_potential_part_promotion.py` |

The queries are correctness-gated against `examples/tpch/answers_sf1/` via
`examples/tpch/_tests.py` at scale factor 1.

## Plan-comparison diagnostic workflow

When translating a SQL query to DataFrame form — TPC-H, a benchmark, or an
answer to a user question — the answer-file comparison proves *correctness*
but does not prove the translation is *equivalent at the plan level*. The
optimizer usually smooths over surface differences (filter pushdown, join
reordering, predicate simplification), so two surface-different builders that
resolve to the same optimized plan are effectively identical queries.

Use this ad-hoc diagnostic when you suspect a DataFrame translation is doing
more work than the SQL form:

```python
from datafusion import SessionContext

ctx = SessionContext()
# register the tables the SQL query expects
# ...

sql_plan = ctx.sql(reference_sql).optimized_logical_plan()
df_plan = dataframe_under_test.optimized_logical_plan()

if sql_plan == df_plan:
    print("Plans match exactly.")
else:
    print("=== SQL plan ===")
    print(sql_plan.display_indent())
    print("=== DataFrame plan ===")
    print(df_plan.display_indent())
```

- `LogicalPlan.__eq__` compares structurally.
- `LogicalPlan.display_indent()` is the readable form for eyeballing diffs.
- `DataFrame.optimized_logical_plan()` is the optimizer output — use it, not
  the unoptimized plan, because trivial differences (e.g. column order in a
  projection) will otherwise be reported as mismatches.

This is **a diagnostic, not a gate**. Answer-file comparison is the
correctness gate. A plan-level mismatch does not mean the DataFrame form is
wrong — it means the two forms are not literally the same plan, which is
sometimes fine (e.g. the DataFrame form forces a particular partitioning the
SQL form leaves to the optimizer).

## Docstring conventions

Every Python function added or modified in this project must include a
docstring with at least one doctest-verified example. Pre-commit and the
`pytest --doctest-modules` default in `pyproject.toml` will enforce that
examples actually execute.

Rules (also in `CLAUDE.md`):

- Examples must run under the doctest harness. The `conftest.py` injects
  `dfn` (the `datafusion` module), `col`, `lit`, `F` (functions), `pa`
  (pyarrow), and `np` (numpy) so you do not need to import them inside
  examples.
- Optional parameters: write a second example that passes the optional
  argument **by keyword** (`step=dfn.lit(3)`) so the reader sees which
  parameter is being demonstrated.
- Reuse input data across examples for the same function so the effect of
  each optional argument is visible against a constant baseline.
- Alias functions (one function that just wraps another — for example
  `list_sort` forwarding to `array_sort`) only need a one-line description
  and a `See Also` reference to the primary function. They do not need their
  own example.

## Aggregate and window function documentation

When adding or updating an aggregate or window function, update the matching
site page:

- Aggregate functions → `docs/source/user-guide/common-operations/aggregations.rst`
- Window functions → `docs/source/user-guide/common-operations/windows.rst`

Add the function to the function list at the bottom of the page and, if the
function exposes a non-obvious option, add a short usage example.

## Related

- Repo-root [`SKILL.md`](../../SKILL.md) — primary DataFrame API guide
  (users and agents).
- `.ai/skills/check-upstream/` — audit upstream Apache DataFusion features
  and flag what the Python bindings do not yet expose.

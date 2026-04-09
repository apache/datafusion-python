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
name: make-pythonic
description: Audit and improve datafusion-python functions to accept native Python types (int, float, str, bool) instead of requiring explicit lit() or col() wrapping. Analyzes function signatures, checks upstream Rust implementations for type constraints, and applies the appropriate coercion pattern.
argument-hint: [scope] (e.g., "string functions", "datetime functions", "array functions", "math functions", "all", or a specific function name like "split_part")
---

# Make Python API Functions More Pythonic

You are improving the datafusion-python API to feel more natural to Python users. The goal is to allow functions to accept native Python types (int, float, str, bool, etc.) for arguments that are contextually always or typically literal values, instead of requiring users to manually wrap them in `lit()`.

**Core principle:** A Python user should be able to write `split_part(col("a"), ",", 2)` instead of `split_part(col("a"), lit(","), lit(2))` when the arguments are contextually obvious literals.

## How to Identify Candidates

The user may specify a scope via `$ARGUMENTS`. If no scope is given or "all" is specified, audit all functions in `python/datafusion/functions.py`.

For each function, determine if any parameter can accept native Python types by evaluating **two complementary signals**:

### Signal 1: Contextual Understanding

Some arguments are contextually always or almost always literal values based on what the function does:

| Context | Typical Arguments | Examples |
|---------|------------------|----------|
| **String position/count** | Character counts, indices, repetition counts | `left(str, n)`, `right(str, n)`, `repeat(str, n)`, `lpad(str, count, ...)` |
| **Delimiters/separators** | Fixed separator characters | `split_part(str, delim, idx)`, `concat_ws(sep, ...)` |
| **Search/replace patterns** | Literal search strings, replacements | `replace(str, from, to)`, `regexp_replace(str, pattern, replacement, flags)` |
| **Date/time parts** | Part names from a fixed set | `date_part(part, date)`, `date_trunc(part, date)` |
| **Rounding precision** | Decimal place counts | `round(val, places)`, `trunc(val, places)` |
| **Fill characters** | Padding characters | `lpad(str, count, fill)`, `rpad(str, count, fill)` |

### Signal 2: Upstream Rust Implementation

Check the Rust binding in `crates/core/src/functions.rs` and the upstream DataFusion function implementation to determine type constraints. The upstream source is cached locally at:

```
~/.cargo/registry/src/index.crates.io-*/datafusion-functions-<VERSION>/src/
```

Check the DataFusion version in `crates/core/Cargo.toml` to find the right directory. Key subdirectories: `string/`, `datetime/`, `math/`, `regex/`.

There are three concrete techniques to check, in order of signal strength:

#### Technique 1: Check `invoke_with_args()` for literal-only enforcement (strongest signal)

Some functions pattern-match on `ColumnarValue::Scalar` in their `invoke_with_args()` method and **return an error** if the argument is a column/array. This means the argument **must** be a literal — passing a column expression will fail at runtime.

Example from `date_trunc.rs`:
```rust
let granularity_str = if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) = granularity {
    v.to_lowercase()
} else {
    return exec_err!("Granularity of `date_trunc` must be non-null scalar Utf8");
};
```

**If you find this pattern:** The argument is **Category B** — accept only the corresponding native Python type (e.g., `str`), not `Expr`. The function will error at runtime with a column expression anyway.

#### Technique 2: Check the `Signature` for data type constraints

Each function defines a `Signature::coercible(...)` that specifies what data types each argument accepts, using `Coercion` entries. This tells you the expected **data type** even if it doesn't enforce literal-only.

Example from `repeat.rs`:
```rust
signature: Signature::coercible(
    vec![
        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
        Coercion::new_implicit(
            TypeSignatureClass::Native(logical_int64()),
            vec![TypeSignatureClass::Integer],
            NativeType::Int64,
        ),
    ],
    Volatility::Immutable,
),
```

This tells you arg 2 (`n`) must be an integer type coerced to Int64. Use this to choose the correct Python type (e.g., `int` not `str` or `float`).

Common mappings:
| Rust Type Constraint | Python Type |
|---------------------|-------------|
| `logical_int64()` / `TypeSignatureClass::Integer` | `int` |
| `logical_float64()` / `TypeSignatureClass::Numeric` | `int \| float` |
| `logical_string()` / `TypeSignatureClass::String` | `str` |
| `LogicalType::Boolean` | `bool` |

#### Technique 3: Check `return_field_from_args()` for `scalar_arguments` usage

Functions that inspect literal values at query planning time use `args.scalar_arguments.get(n)` in their `return_field_from_args()` method. This indicates the argument is **expected to be a literal** for optimal behavior (e.g., to determine output type precision), but may still work as a column.

Example from `round.rs`:
```rust
let decimal_places: Option<i32> = match args.scalar_arguments.get(1) {
    None => Some(0),
    Some(None) => None,        // argument is not a literal (column)
    Some(Some(scalar)) if scalar.is_null() => Some(0),
    Some(Some(scalar)) => Some(decimal_places_from_scalar(scalar)?),
};
```

**If you find this pattern:** The argument is **Category A** — accept native types AND `Expr`. It works as a column but is primarily used as a literal.

#### Decision flow

```
Is argument rejected at runtime if not a literal?
  (check invoke_with_args for ColumnarValue::Scalar-only match + exec_err!)
    → YES: Category B — accept only native type, no Expr
    → NO: Does the Signature constrain it to a specific data type?
        → YES: Category A — accept Expr | <native type matching the constraint>
        → NO: Leave as Expr only
```

## Coercion Categories

When making a function more pythonic, apply the correct coercion pattern based on **what the argument represents**:

### Category A: Arguments That Should Accept Native Types AND Expr

These are arguments that are *typically* literals but *could* be column references in advanced use cases. For these, accept a union type and coerce native types to `Expr.literal()`.

**Type hint pattern:** `Expr | int`, `Expr | str`, `Expr | int | str`, etc.

**When to use:** When the argument could plausibly come from a column in some use case (e.g., the repeat count might come from a column in a data-driven scenario).

```python
def repeat(string: Expr, n: Expr | int) -> Expr:
    """Repeats the ``string`` to ``n`` times.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["ha"]})
        >>> result = df.select(
        ...     dfn.functions.repeat(dfn.col("a"), 3).alias("r"))
        >>> result.collect_column("r")[0].as_py()
        'hahaha'
    """
    if not isinstance(n, Expr):
        n = Expr.literal(n)
    return Expr(f.repeat(string.expr, n.expr))
```

### Category B: Arguments That Should ONLY Accept Specific Native Types

These are arguments where an `Expr` never makes sense because the value must be a fixed literal known at query-planning time (not a per-row value). For these, accept only the native type(s) and wrap internally.

**Type hint pattern:** `str`, `int`, `list[str]`, etc. (no `Expr` in the union)

**When to use:** When the argument is from a fixed enumeration or is always a compile-time constant:
- Date/time part names (`"year"`, `"month"`, `"day"`, etc.)
- Regex flags (`"g"`, `"i"`, etc.)
- Values that the Rust implementation already accepts as native types

```python
def date_part(part: str, date: Expr) -> Expr:
    """Extracts a subfield from the date.

    Args:
        part: The part of the date to extract. Must be one of "year", "month",
            "day", "hour", "minute", "second", etc.
        date: The date expression to extract from.

    Examples:
        >>> ctx = dfn.SessionContext()
        >>> df = ctx.from_pydict({"a": ["2021-07-15T00:00:00"]})
        >>> df = df.select(dfn.functions.to_timestamp(dfn.col("a")).alias("a"))
        >>> result = df.select(
        ...     dfn.functions.date_part("year", dfn.col("a")).alias("y"))
        >>> result.collect_column("y")[0].as_py()
        2021
    """
    part = Expr.literal(part)
    return Expr(f.date_part(part.expr, date.expr))
```

### Category C: Arguments That Should Accept str as Column Name

In some contexts a string argument naturally refers to a column name rather than a literal. This is the pattern used by DataFrame methods.

**Type hint pattern:** `Expr | str`

**When to use:** Only when the string contextually means a column name (rare in `functions.py`, more common in DataFrame methods).

```python
# Use _to_raw_expr() from expr.py for this pattern
from datafusion.expr import _to_raw_expr

def some_function(column: Expr | str) -> Expr:
    raw = _to_raw_expr(column)  # str -> col(str)
    return Expr(f.some_function(raw))
```

**IMPORTANT:** In `functions.py`, string arguments almost never mean column names. Functions operate on expressions, and column references should use `col()`. Category C applies mainly to DataFrame methods and context APIs, not to scalar/aggregate/window functions. Do NOT convert string arguments to column expressions in `functions.py` unless there is a very clear reason to do so.

## Implementation Steps

For each function being updated:

### Step 1: Analyze the Function

1. Read the current Python function signature in `python/datafusion/functions.py`
2. Read the Rust binding in `crates/core/src/functions.rs`
3. Optionally check the upstream DataFusion docs for the function
4. Determine which category (A, B, or C) applies to each parameter

### Step 2: Update the Python Function

1. **Change the type hints** to accept native types (e.g., `Expr` -> `Expr | int`)
2. **Add coercion logic** at the top of the function body
3. **Update the docstring** examples to use the simpler calling convention
4. **Preserve backward compatibility** — existing code using `Expr` must still work

### Step 3: Update Alias Type Hints

After updating a primary function, find all alias functions that delegate to it (e.g., `instr` and `position` delegate to `strpos`). Update each alias's **parameter type hints** to match the primary function's new signature. Do not add coercion logic to aliases — the primary function handles that.

### Step 4: Update Docstring Examples (primary functions only)

Per the project's CLAUDE.md rules:
- Every function must have doctest-style examples
- Optional parameters need examples both without and with the optional args, using keyword argument syntax
- Reuse the same input data across examples where possible

**Update examples to demonstrate the pythonic calling convention:**

```python
# BEFORE (old style - still works but verbose)
dfn.functions.left(dfn.col("a"), dfn.lit(3))

# AFTER (new style - shown in examples)
dfn.functions.left(dfn.col("a"), 3)
```

### Step 5: Run Tests

After making changes, run the doctests to verify:
```bash
python -m pytest --doctest-modules python/datafusion/functions.py -v
```

## Coercion Helper Pattern

When adding coercion to a function, use this inline pattern:

```python
if not isinstance(arg, Expr):
    arg = Expr.literal(arg)
```

Do NOT create a new helper function for this — the inline check is clear and explicit. The project intentionally uses `ensure_expr()` to reject non-Expr values in contexts where coercion is not wanted; the pythonic coercion is the opposite pattern and should be visually distinct.

## What NOT to Change

- **Do not change arguments that represent data columns.** If an argument is the primary data being operated on (e.g., the `string` in `left(string, n)` or the `array` in `array_sort(array)`), it should remain `Expr` only. Users should use `col()` for column references.
- **Do not change variadic `*args: Expr` parameters.** These represent multiple expressions and should stay as `Expr`.
- **Do not change arguments where the coercion is ambiguous.** If it is unclear whether a string should be a column name or a literal, leave it as `Expr` and let the user be explicit.
- **Do not add coercion logic to simple aliases.** If a function is just `return other_function(...)`, the primary function handles coercion. However, you **must update the alias's type hints** to match the primary function's signature so that type checkers and documentation accurately reflect what the alias accepts.
- **Do not change the Rust bindings.** All coercion happens in the Python layer. The Rust functions continue to accept `PyExpr`.

## Priority Order

When auditing functions, process them in this order:

1. **Date/time functions** — `date_part`, `date_trunc`, `date_bin` — these have the clearest literal arguments
2. **String functions** — `left`, `right`, `repeat`, `lpad`, `rpad`, `split_part`, `substring`, `replace`, `regexp_replace`, `regexp_match`, `regexp_count` — common and verbose without coercion
3. **Math functions** — `round`, `trunc`, `power` — numeric literal arguments
4. **Array functions** — `array_slice`, `array_position`, `array_remove_n`, `array_replace_n`, `array_resize`, `array_element` — index and count arguments
5. **Other functions** — any remaining functions with literal arguments

## Output Format

For each function analyzed, report:

```
## [Function Name]

**Current signature:** `function(arg1: Expr, arg2: Expr) -> Expr`
**Proposed signature:** `function(arg1: Expr, arg2: Expr | int) -> Expr`
**Category:** A (accepts native + Expr)
**Arguments changed:**
- `arg2`: Expr -> Expr | int (always a literal count)
**Rust binding:** Takes PyExpr, wraps to literal internally
**Status:** [Changed / Skipped / Needs Discussion]
```

If asked to implement (not just audit), make the changes directly and show a summary of what was updated.

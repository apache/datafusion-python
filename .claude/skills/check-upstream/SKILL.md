---
name: check-upstream
description: Check if upstream Apache DataFusion features (functions, DataFrame ops, SessionContext methods) are exposed in this Python project. Use when adding missing functions, auditing API coverage, or ensuring parity with upstream.
argument-hint: [area] (e.g., "scalar functions", "aggregate functions", "window functions", "dataframe", "session context", "all")
---

# Check Upstream DataFusion Feature Coverage

You are auditing the datafusion-python project to find features from the upstream Apache DataFusion Rust library that are **not yet exposed** in this Python binding project. Your goal is to identify gaps and, if asked, implement the missing bindings.

## Areas to Check

The user may specify an area via `$ARGUMENTS`. If no area is specified or "all" is given, check all areas.

### 1. Scalar Functions

**Upstream source of truth:**
- Rust docs: https://docs.rs/datafusion/latest/datafusion/functions/index.html
- User docs: https://datafusion.apache.org/user-guide/sql/scalar_functions.html

**Where they are exposed in this project:**
- Python API: `python/datafusion/functions.py` — each function wraps a call to `datafusion._internal.functions`
- Rust bindings: `crates/core/src/functions.rs` — `#[pyfunction]` definitions registered via `init_module()`

**How to check:**
1. Fetch the upstream scalar function documentation page
2. Compare against functions listed in `python/datafusion/functions.py` (check the `__all__` list)
3. Also check `crates/core/src/functions.rs` for what's registered in `init_module()`
4. Report functions that exist upstream but are missing from this project

### 2. Aggregate Functions

**Upstream source of truth:**
- Rust docs: https://docs.rs/datafusion/latest/datafusion/functions_aggregate/index.html
- User docs: https://datafusion.apache.org/user-guide/sql/aggregate_functions.html

**Where they are exposed in this project:**
- Python API: `python/datafusion/functions.py` (aggregate functions are mixed in with scalar functions)
- Rust bindings: `crates/core/src/functions.rs`

**How to check:**
1. Fetch the upstream aggregate function documentation page
2. Compare against aggregate functions in `python/datafusion/functions.py`
3. Report missing aggregate functions

### 3. Window Functions

**Upstream source of truth:**
- Rust docs: https://docs.rs/datafusion/latest/datafusion/functions_window/index.html
- User docs: https://datafusion.apache.org/user-guide/sql/window_functions.html

**Where they are exposed in this project:**
- Python API: `python/datafusion/functions.py` (window functions like `rank`, `dense_rank`, `lag`, `lead`, etc.)
- Rust bindings: `crates/core/src/functions.rs`

**How to check:**
1. Fetch the upstream window function documentation page
2. Compare against window functions in `python/datafusion/functions.py`
3. Report missing window functions

### 4. Table Functions

**Upstream source of truth:**
- Rust docs: https://docs.rs/datafusion/latest/datafusion/functions_table/index.html
- User docs: https://datafusion.apache.org/user-guide/sql/table_functions.html (if available)

**Where they are exposed in this project:**
- Python API: `python/datafusion/functions.py` and `python/datafusion/user_defined.py` (TableFunction/udtf)
- Rust bindings: `crates/core/src/functions.rs` and `crates/core/src/udtf.rs`

**How to check:**
1. Fetch the upstream table function documentation
2. Compare against what's available in this project
3. Report missing table functions

### 5. DataFrame Operations

**Upstream source of truth:**
- Rust docs: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html

**Where they are exposed in this project:**
- Python API: `python/datafusion/dataframe.py` — the `DataFrame` class
- Rust bindings: `crates/core/src/dataframe.rs` — `PyDataFrame` with `#[pymethods]`

**How to check:**
1. Fetch the upstream DataFrame documentation page listing all methods
2. Compare against methods in `python/datafusion/dataframe.py`
3. Also check `crates/core/src/dataframe.rs` for what's implemented
4. Report DataFrame methods that exist upstream but are missing

### 6. SessionContext Methods

**Upstream source of truth:**
- Rust docs: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html

**Where they are exposed in this project:**
- Python API: `python/datafusion/context.py` — the `SessionContext` class
- Rust bindings: `crates/core/src/context.rs` — `PySessionContext` with `#[pymethods]`

**How to check:**
1. Fetch the upstream SessionContext documentation page listing all methods
2. Compare against methods in `python/datafusion/context.py`
3. Also check `crates/core/src/context.rs` for what's implemented
4. Report SessionContext methods that exist upstream but are missing

## Output Format

For each area checked, produce a report like:

```
## [Area Name] Coverage Report

### Currently Exposed (X functions/methods)
- list of what's already available

### Missing from Upstream (Y functions/methods)
- function_name — brief description of what it does
- function_name — brief description of what it does

### Notes
- Any relevant observations about partial implementations, naming differences, etc.
```

## Implementation Pattern

If the user asks you to implement missing features, follow these patterns:

### Adding a New Function (Scalar/Aggregate/Window)

**Step 1: Rust binding** in `crates/core/src/functions.rs`:
```rust
#[pyfunction]
#[pyo3(signature = (arg1, arg2))]
fn new_function_name(arg1: PyExpr, arg2: PyExpr) -> PyResult<PyExpr> {
    Ok(datafusion::functions::module::expr_fn::new_function_name(arg1.expr, arg2.expr).into())
}
```
Then register in `init_module()`:
```rust
m.add_wrapped(wrap_pyfunction!(new_function_name))?;
```

**Step 2: Python wrapper** in `python/datafusion/functions.py`:
```python
def new_function_name(arg1: Expr, arg2: Expr) -> Expr:
    """Description of what the function does.

    Args:
        arg1: Description of first argument.
        arg2: Description of second argument.

    Returns:
        Description of return value.
    """
    return Expr(f.new_function_name(arg1.expr, arg2.expr))
```
Add to `__all__` list.

### Adding a New DataFrame Method

**Step 1: Rust binding** in `crates/core/src/dataframe.rs`:
```rust
#[pymethods]
impl PyDataFrame {
    fn new_method(&self, py: Python, param: PyExpr) -> PyDataFusionResult<Self> {
        let df = self.df.as_ref().clone().new_method(param.into())?;
        Ok(Self::new(df))
    }
}
```

**Step 2: Python wrapper** in `python/datafusion/dataframe.py`:
```python
def new_method(self, param: Expr) -> DataFrame:
    """Description of the method."""
    return DataFrame(self.df.new_method(param.expr))
```

### Adding a New SessionContext Method

**Step 1: Rust binding** in `crates/core/src/context.rs`:
```rust
#[pymethods]
impl PySessionContext {
    pub fn new_method(&self, py: Python, param: String) -> PyDataFusionResult<PyDataFrame> {
        let df = wait_for_future(py, self.ctx.new_method(&param))?;
        Ok(PyDataFrame::new(df))
    }
}
```

**Step 2: Python wrapper** in `python/datafusion/context.py`:
```python
def new_method(self, param: str) -> DataFrame:
    """Description of the method."""
    return DataFrame(self.ctx.new_method(param))
```

## Important Notes

- The upstream DataFusion version used by this project is specified in `crates/core/Cargo.toml` — check the `datafusion` dependency version to ensure you're comparing against the right upstream version.
- Some upstream features may intentionally not be exposed (e.g., internal-only APIs). Use judgment about what's user-facing.
- When fetching upstream docs, prefer the published docs.rs documentation as it matches the crate version.
- Function aliases (e.g., `array_append` / `list_append`) should both be exposed if upstream supports them.
- Check the `__all__` list in `functions.py` to see what's publicly exported vs just defined.

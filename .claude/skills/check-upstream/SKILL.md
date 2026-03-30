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

### Adding a New FFI Type

FFI types require a full pipeline from C struct through to a typed Python wrapper. Each layer must be present.

**Step 1: Rust PyO3 wrapper class** in a new or existing file under `crates/core/src/`:
```rust
use datafusion_ffi::new_type::FFI_NewType;

#[pyclass(from_py_object, frozen, name = "RawNewType", module = "datafusion.module_name", subclass)]
pub struct PyNewType {
    pub inner: Arc<dyn NewTypeTrait>,
}

#[pymethods]
impl PyNewType {
    #[staticmethod]
    fn from_pycapsule(obj: &Bound<'_, PyAny>) -> PyDataFusionResult<Self> {
        let capsule = obj
            .getattr("__datafusion_new_type__")?
            .call0()?
            .downcast::<PyCapsule>()?;
        let ffi_ptr = unsafe { capsule.reference::<FFI_NewType>() };
        let provider: Arc<dyn NewTypeTrait> = ffi_ptr.into();
        Ok(Self { inner: provider })
    }

    fn some_method(&self) -> PyResult<...> {
        // wrap inner trait method
    }
}
```
Register in the appropriate `init_module()`:
```rust
m.add_class::<PyNewType>()?;
```

**Step 2: Python Protocol type** in the appropriate Python module (e.g., `python/datafusion/catalog.py`):
```python
class NewTypeExportable(Protocol):
    """Type hint for objects providing a __datafusion_new_type__ PyCapsule."""

    def __datafusion_new_type__(self) -> object: ...
```

**Step 3: Python wrapper class** in the same module:
```python
class NewType:
    """Description of the type.

    This class wraps a DataFusion NewType, which can be created from a native
    Python implementation or imported from an FFI-compatible library.
    """

    def __init__(
        self,
        new_type: df_internal.module_name.RawNewType | NewTypeExportable,
    ) -> None:
        if isinstance(new_type, df_internal.module_name.RawNewType):
            self._raw = new_type
        else:
            self._raw = df_internal.module_name.RawNewType.from_pycapsule(new_type)

    def some_method(self) -> ReturnType:
        """Description of the method."""
        return self._raw.some_method()
```

**Step 4: ABC base class** (if users should be able to subclass and provide custom implementations in Python):
```python
from abc import ABC, abstractmethod

class NewTypeProvider(ABC):
    """Abstract base class for implementing a custom NewType in Python."""

    @abstractmethod
    def some_method(self) -> ReturnType:
        """Description of the method."""
        ...
```

**Step 5: Module exports** — add to the appropriate `__init__.py`:
- Add the wrapper class (`NewType`) to `python/datafusion/__init__.py`
- Add the ABC (`NewTypeProvider`) if applicable
- Add the Protocol type (`NewTypeExportable`) if it should be public

**Step 6: FFI example** — add an example implementation under `examples/datafusion-ffi-example/src/`:
```rust
// examples/datafusion-ffi-example/src/new_type.rs
use datafusion_ffi::new_type::FFI_NewType;
// ... example showing how an external Rust library exposes this type via PyCapsule
```

**Checklist for each FFI type:**
- [ ] Rust PyO3 wrapper with `from_pycapsule()` method
- [ ] Python Protocol type (e.g., `NewTypeExportable`) for FFI objects
- [ ] Python wrapper class with full type hints on all public methods
- [ ] ABC base class (if the type can be user-implemented)
- [ ] Registered in Rust `init_module()` and Python `__init__.py`
- [ ] FFI example in `examples/datafusion-ffi-example/`
- [ ] Type appears in union type hints where accepted (e.g., `Table | TableProviderExportable`)

### 7. FFI Types (datafusion-ffi)

**Upstream source of truth:**
- Crate source: https://github.com/apache/datafusion/tree/main/datafusion/ffi/src
- Rust docs: https://docs.rs/datafusion-ffi/latest/datafusion_ffi/

**Where they are exposed in this project:**
- Rust bindings: various files under `crates/core/src/` and `crates/util/src/`
- FFI example: `examples/datafusion-ffi-example/src/`
- Dependency declared in root `Cargo.toml` and `crates/core/Cargo.toml`

**Currently supported FFI types:**
- `FFI_ScalarUDF` — `crates/core/src/udf.rs`
- `FFI_AggregateUDF` — `crates/core/src/udaf.rs`
- `FFI_WindowUDF` — `crates/core/src/udwf.rs`
- `FFI_TableFunction` — `crates/core/src/udtf.rs`
- `FFI_TableProvider` — `crates/core/src/table.rs`, `crates/util/src/lib.rs`
- `FFI_TableProviderFactory` — `crates/core/src/context.rs`
- `FFI_CatalogProvider` — `crates/core/src/catalog.rs`, `crates/core/src/context.rs`
- `FFI_CatalogProviderList` — `crates/core/src/context.rs`
- `FFI_SchemaProvider` — `crates/core/src/catalog.rs`
- `FFI_LogicalExtensionCodec` — multiple files
- `FFI_ExtensionOptions` — `crates/core/src/context.rs`
- `FFI_TaskContextProvider` — `crates/core/src/context.rs`

**Evaluated and not requiring direct Python exposure:**
These upstream FFI types have been reviewed and do not need to be independently exposed to end users:
- `FFI_ExecutionPlan` — already used indirectly through table providers; no need for direct exposure
- `FFI_PhysicalExpr` / `FFI_PhysicalSortExpr` — internal physical planning types not expected to be needed by end users
- `FFI_RecordBatchStream` — one level deeper than FFI_ExecutionPlan, used internally when execution plans stream results
- `FFI_SessionRef` / `ForeignSession` — session sharing across FFI; Python manages sessions natively via SessionContext
- `FFI_SessionConfig` — Python can configure sessions natively without FFI
- `FFI_ConfigOptions` / `FFI_TableOptions` — internal configuration plumbing
- `FFI_PlanProperties` / `FFI_Boundedness` / `FFI_EmissionType` — read from existing plans, not user-facing
- `FFI_Partitioning` — supporting type for physical planning
- Supporting/utility types (`FFI_Option`, `FFI_Result`, `WrappedSchema`, `WrappedArray`, `FFI_ColumnarValue`, `FFI_Volatility`, `FFI_InsertOp`, `FFI_AccumulatorArgs`, `FFI_Accumulator`, `FFI_GroupsAccumulator`, `FFI_EmitTo`, `FFI_AggregateOrderSensitivity`, `FFI_PartitionEvaluator`, `FFI_PartitionEvaluatorArgs`, `FFI_Range`, `FFI_SortOptions`, `FFI_Distribution`, `FFI_ExprProperties`, `FFI_SortProperties`, `FFI_Interval`, `FFI_TableProviderFilterPushDown`, `FFI_TableType`) — used as building blocks within the types above, not independently exposed

**How to check:**
1. Compare the upstream `datafusion-ffi` crate's `lib.rs` exports against the lists above
2. If new FFI types appear upstream, evaluate whether they represent a user-facing capability
3. Check against the "evaluated and not requiring exposure" list before flagging as a gap
4. Report any genuinely new types that enable user-facing functionality

## Important Notes

- The upstream DataFusion version used by this project is specified in `crates/core/Cargo.toml` — check the `datafusion` dependency version to ensure you're comparing against the right upstream version.
- Some upstream features may intentionally not be exposed (e.g., internal-only APIs). Use judgment about what's user-facing.
- When fetching upstream docs, prefer the published docs.rs documentation as it matches the crate version.
- Function aliases (e.g., `array_append` / `list_append`) should both be exposed if upstream supports them.
- Check the `__all__` list in `functions.py` to see what's publicly exported vs just defined.

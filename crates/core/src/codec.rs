// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Python-aware extension codecs.
//!
//! Datafusion-python plans can carry references to Python-defined
//! objects that the upstream protobuf codecs do not know how to
//! serialize: pure-Python scalar / aggregate / window UDFs, Python
//! query-planning extensions, and so on. Their state lives inside
//! `Py<PyAny>` callables and closures rather than being recoverable
//! from a name in the receiver's function registry. To ship a plan
//! across a process boundary (pickle, `multiprocessing`, Ray actor,
//! `datafusion-distributed`, etc.) those payloads have to be encoded
//! into the proto wire format itself.
//!
//! [`PythonLogicalCodec`] is the [`LogicalExtensionCodec`] that
//! datafusion-python parks on every `SessionContext`. It wraps a
//! chain of composable codecs and adds Python-aware in-band encoding
//! on top: when the encoder sees a Python-defined UDF, the codec
//! cloudpickles the callable + signature into the `fun_definition`
//! proto field; when the decoder sees a payload it produced, it
//! reconstructs the UDF from the bytes alone — no pre-registration on
//! the receiver. Everything the codec does not recognise is delegated
//! to the chain: each downstream FFI codec installed via
//! `SessionContext.with_logical_extension_codec(...)` is consulted in
//! most-recently-installed-first order, with
//! `DefaultLogicalExtensionCodec` as the terminal fallback.
//!
//! [`PythonPhysicalCodec`] is the symmetric wrapper around
//! [`PhysicalExtensionCodec`]. Logical and physical layers each have
//! a `try_encode_udf` / `try_decode_udf` pair, so a `ScalarUDF`
//! referenced inside a `LogicalPlan`, an `ExecutionPlan`, or a
//! `PhysicalExpr` must encode identically through either layer for
//! plans to survive a serialization round-trip. Both codecs share
//! the same payload framing for that reason.
//!
//! Payloads emitted by these codecs are framed as
//! `<family_magic: 7 bytes> <version: u8> <py_major: u8> <py_minor: u8> <cloudpickle blob>`.
//! The family magic identifies the UDF flavor; the version byte lets
//! the decoder reject too-new or too-old payloads with a clean error
//! instead of falling into an opaque `cloudpickle` tuple-unpack
//! failure when the tuple shape changes; the Python `(major, minor)`
//! bytes catch the cloudpickle-cross-minor-version case and raise an
//! actionable error instead of an opaque `marshal` failure on load
//! (cloudpickle payloads are not portable across Python minor
//! versions). Dispatch precedence on decode: **family match +
//! supported version + matching Python version → codec chain →
//! caller's `FunctionRegistry` fallback.**
//!
//! ## Wire-format family registry
//!
//! | Layer + kind                  | Family prefix |
//! | ----------------------------- | ------------- |
//! | `PythonLogicalCodec` scalar   | `DFPYUDF`     |
//! | `PythonLogicalCodec` agg      | `DFPYUDA`     |
//! | `PythonLogicalCodec` window   | `DFPYUDW`     |
//! | `PythonPhysicalCodec` scalar  | `DFPYUDF`     |
//! | `PythonPhysicalCodec` agg     | `DFPYUDA`     |
//! | `PythonPhysicalCodec` window  | `DFPYUDW`     |
//! | User FFI extension codec      | user-chosen   |
//! | Default codec                 | (none)        |
//!
//! Current wire-format version is [`WIRE_VERSION_CURRENT`]; supported
//! receive range is `WIRE_VERSION_MIN_SUPPORTED..=WIRE_VERSION_CURRENT`.
//! Bump [`WIRE_VERSION_CURRENT`] whenever the cloudpickle tuple shape
//! changes; raise [`WIRE_VERSION_MIN_SUPPORTED`] when dropping support
//! for an older shape.
//!
//! Downstream FFI codecs should pick non-colliding family prefixes
//! (use a `DF` namespace plus a crate-specific suffix) and return an
//! error for payloads and objects they do not own — that error is the
//! chain's "not mine" signal, letting the next codec take a turn. A
//! codec that answers `Ok` for objects outside its family shadows
//! every codec installed before it.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use datafusion::common::{Result, TableReference};
use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{
    AggregateUDF, AggregateUDFImpl, Extension, LogicalPlan, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility, WindowUDF, WindowUDFImpl,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
use datafusion::physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::logical_plan::{DefaultLogicalExtensionCodec, LogicalExtensionCodec};
use datafusion_proto::physical_plan::{
    DefaultPhysicalExtensionCodec, PhysicalExtensionCodec, PhysicalProtoConverterExtension,
};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyTuple};

use crate::errors::to_datafusion_err;
use crate::udaf::PythonFunctionAggregateUDF;
use crate::udf::PythonFunctionScalarUDF;
use crate::udwf::PythonFunctionWindowUDF;

// Wire-format framing for inlined Python UDF payloads.
//
// Layout: `<family_magic: 7 bytes> <version: u8> <py_major: u8> <py_minor: u8> <cloudpickle blob>`.
// The family magic identifies the UDF flavor; the version byte lets
// the decoder reject too-new or too-old payloads with a clean error
// instead of falling into an opaque `cloudpickle` tuple-unpack failure
// when the tuple shape changes; the Python `(major, minor)` bytes
// catch the cloudpickle-cross-minor-version case (cloudpickle is not
// portable across Python minor versions) and raise an actionable
// error instead of an opaque `marshal` failure on load. Bump
// [`WIRE_VERSION_CURRENT`] whenever the tuple shape changes; raise
// [`WIRE_VERSION_MIN_SUPPORTED`] when dropping support for an older
// shape.

/// Family prefix for an inlined Python scalar UDF
/// (cloudpickled tuple of name, callable, input schema, return field,
/// volatility).
pub(crate) const PY_SCALAR_UDF_FAMILY: &[u8] = b"DFPYUDF";

/// Family prefix for an inlined Python aggregate UDF
/// (cloudpickled tuple of name, accumulator factory, input schema bytes,
/// return schema bytes (single-field IPC schema), state schema bytes,
/// volatility).
pub(crate) const PY_AGG_UDF_FAMILY: &[u8] = b"DFPYUDA";

/// Family prefix for an inlined Python window UDF
/// (cloudpickled tuple of name, evaluator factory, input schema bytes,
/// return schema bytes (single-field IPC schema), volatility).
pub(crate) const PY_WINDOW_UDF_FAMILY: &[u8] = b"DFPYUDW";

/// Wire-format version this build emits.
pub(crate) const WIRE_VERSION_CURRENT: u8 = 1;

/// Oldest wire-format version this build still decodes. Bump when
/// retiring support for an older payload shape.
pub(crate) const WIRE_VERSION_MIN_SUPPORTED: u8 = 1;

/// Tag `buf` with the framing header for `family` at the current
/// wire-format version, stamping `py_version` as `(major, minor)`
/// bytes. Append-only — the caller writes the cloudpickle payload
/// after.
fn write_wire_header(buf: &mut Vec<u8>, family: &[u8], py_version: (u8, u8)) {
    buf.extend_from_slice(family);
    buf.push(WIRE_VERSION_CURRENT);
    buf.push(py_version.0);
    buf.push(py_version.1);
}

/// Inspect the framing on `buf`.
///
/// * `Ok(None)` — `buf` does not carry `family`. The caller should
///   delegate to its codec chain.
/// * `Ok(Some(payload))` — `buf` carries `family` at a version this
///   build accepts and a Python `(major, minor)` matching
///   `expected_py`; `payload` is the cloudpickle blob.
/// * `Err(_)` — `buf` carries `family` but the wire-format version
///   is outside `WIRE_VERSION_MIN_SUPPORTED..=WIRE_VERSION_CURRENT`,
///   or the stamped Python `(major, minor)` does not match
///   `expected_py`. The error names the offending values so an
///   operator can diagnose sender/receiver drift instead of seeing
///   an opaque cloudpickle tuple-unpack or `marshal` failure.
fn strip_wire_header<'a>(
    buf: &'a [u8],
    family: &[u8],
    kind: &str,
    expected_py: (u8, u8),
) -> Result<Option<&'a [u8]>> {
    if !buf.starts_with(family) {
        return Ok(None);
    }
    let version_idx = family.len();
    let Some(&version) = buf.get(version_idx) else {
        return Err(datafusion::error::DataFusionError::Execution(format!(
            "Truncated inline Python {kind} payload: missing wire-format version byte"
        )));
    };
    if !(WIRE_VERSION_MIN_SUPPORTED..=WIRE_VERSION_CURRENT).contains(&version) {
        return Err(datafusion::error::DataFusionError::Execution(format!(
            "Inline Python {kind} payload wire-format version v{version}; \
             this build supports v{WIRE_VERSION_MIN_SUPPORTED}..=v{WIRE_VERSION_CURRENT}. \
             Align datafusion-python versions on sender and receiver."
        )));
    }
    let py_major_idx = version_idx + 1;
    let Some(&encoded_major) = buf.get(py_major_idx) else {
        return Err(datafusion::error::DataFusionError::Execution(format!(
            "Truncated inline Python {kind} payload: missing Python major version byte"
        )));
    };
    let py_minor_idx = version_idx + 2;
    let Some(&encoded_minor) = buf.get(py_minor_idx) else {
        return Err(datafusion::error::DataFusionError::Execution(format!(
            "Truncated inline Python {kind} payload: missing Python minor version byte"
        )));
    };
    let (current_major, current_minor) = expected_py;
    if encoded_major != current_major || encoded_minor != current_minor {
        return Err(datafusion::error::DataFusionError::Execution(format!(
            "Inline Python {kind} payload was serialized on Python \
             {encoded_major}.{encoded_minor} but this process is running Python \
             {current_major}.{current_minor}. cloudpickle payloads are not portable \
             across Python minor versions. Align Python versions on sender and receiver."
        )));
    }
    Ok(Some(&buf[py_minor_idx + 1..]))
}

/// Run `f` against each codec in `chain`, returning the first `Ok`.
///
/// A codec signals "not mine" by returning an error, so the chain
/// keeps trying until a codec succeeds. When every codec fails and the
/// chain has more than one entry, the errors are aggregated into a
/// single message — returning only the last error would surface the
/// terminal `Default*ExtensionCodec` "not provided" message and mask
/// the more specific diagnostic from an installed codec (e.g. a
/// corrupt-token error from the codec that owns the payload family).
fn chain_try<C: ?Sized, R>(chain: &[Arc<C>], what: &str, f: impl Fn(&C) -> Result<R>) -> Result<R> {
    let mut errors: Vec<datafusion::error::DataFusionError> = Vec::new();
    for codec in chain {
        match f(codec) {
            Ok(value) => return Ok(value),
            Err(err) => errors.push(err),
        }
    }
    Err(aggregate_chain_errors(what, errors))
}

/// Collapse per-codec failures into one error. A single failure is
/// returned as-is so the one-codec (default-only) chain behaves
/// exactly like the pre-chain implementation.
fn aggregate_chain_errors(
    what: &str,
    mut errors: Vec<datafusion::error::DataFusionError>,
) -> datafusion::error::DataFusionError {
    match errors.len() {
        0 => datafusion::error::DataFusionError::Internal(format!(
            "Empty extension codec chain while handling {what}"
        )),
        1 => errors.swap_remove(0),
        _ => {
            let joined = errors
                .iter()
                .map(|err| err.to_string())
                .collect::<Vec<_>>()
                .join("; ");
            datafusion::error::DataFusionError::Execution(format!(
                "None of the {} composed extension codecs handled {what}: {joined}",
                errors.len()
            ))
        }
    }
}

/// Encode variant of [`chain_try`] for methods that write into a
/// caller-provided buffer.
///
/// Each codec encodes into a scratch buffer so a failed attempt cannot
/// leave partial bytes behind. `Ok` with bytes written commits those
/// bytes and ends the chain. `Ok` with an empty buffer is treated as
/// "no opinion" — the standard `Default*ExtensionCodec` behavior of
/// encoding a UDF by name writes nothing — so later codecs still get a
/// chance to emit a richer payload. If no codec writes bytes but at
/// least one returned `Ok`, the overall result is `Ok` with nothing
/// written (encode by name).
fn chain_encode<C: ?Sized>(
    chain: &[Arc<C>],
    buf: &mut Vec<u8>,
    what: &str,
    f: impl Fn(&C, &mut Vec<u8>) -> Result<()>,
) -> Result<()> {
    let mut saw_empty_ok = false;
    let mut errors: Vec<datafusion::error::DataFusionError> = Vec::new();
    for codec in chain {
        let mut scratch = Vec::new();
        match f(codec, &mut scratch) {
            Ok(()) if !scratch.is_empty() => {
                buf.extend_from_slice(&scratch);
                return Ok(());
            }
            Ok(()) => saw_empty_ok = true,
            Err(err) => errors.push(err),
        }
    }
    if saw_empty_ok {
        return Ok(());
    }
    Err(aggregate_chain_errors(what, errors))
}

/// `LogicalExtensionCodec` parked on every `SessionContext`. Holds
/// the Python-aware encoding hooks for logical-layer types
/// (`LogicalPlan`, `Expr`) and delegates everything it does not
/// handle to a chain of composable codecs. The chain starts as just
/// `DefaultLogicalExtensionCodec`; each downstream FFI codec installed
/// via `SessionContext.with_logical_extension_codec(...)` is prepended,
/// so the most recently installed codec is consulted first and the
/// default codec always runs last.
///
/// Chain dispatch relies on each codec recognizing its own payloads
/// (distinct family prefixes — see the module docs) and returning an
/// error for everything else so the next codec gets a chance.
///
/// Sitting at the top of the session's logical codec stack means
/// every serializer that reads `session.logical_codec()` automatically
/// picks up Python-aware encoding for free.
///
/// A codec deliberately does **not** retain the session it was built from.
/// Codecs are routinely handed to a provider that is then registered back into
/// that same session, so retaining here would close a cycle:
/// `SessionContext -> catalog -> FFI provider -> FFI codec -> here`. Keeping
/// the weak `FFI_TaskContextProvider` valid is instead a matter of never
/// replacing the session's `Arc<SessionContext>`; see
/// `PySessionContext::set_session_query_planner`.
#[derive(Debug, Clone)]
pub struct PythonLogicalCodec {
    chain: Vec<Arc<dyn LogicalExtensionCodec>>,
    python_udf_inlining: bool,
}

impl PythonLogicalCodec {
    pub fn new(inner: Arc<dyn LogicalExtensionCodec>) -> Self {
        Self {
            chain: vec![inner],
            python_udf_inlining: true,
        }
    }

    /// Return a copy of this codec with `codec` prepended to the
    /// chain, preserving the Python-UDF-inlining setting. The new
    /// codec is consulted before every previously installed codec.
    pub fn with_additional_codec(&self, codec: Arc<dyn LogicalExtensionCodec>) -> Self {
        let mut chain = Vec::with_capacity(self.chain.len() + 1);
        chain.push(codec);
        chain.extend(self.chain.iter().map(Arc::clone));
        Self {
            chain,
            python_udf_inlining: self.python_udf_inlining,
        }
    }

    /// Toggle inline encoding of Python UDFs. See
    /// `SessionContext.with_python_udf_inlining` (Python) for full
    /// behavior and use cases.
    ///
    /// Security scope: strict mode (`false`) narrows only the codec
    /// layer — it stops `Expr::from_bytes` from invoking
    /// `cloudpickle.loads` on the inline `DFPY*` payload. It does
    /// **not** make `pickle.loads(untrusted_bytes)` safe; treat every
    /// `pickle.loads` on untrusted input as unsafe regardless of this
    /// setting. See `docs/source/user-guide/io/distributing_work.rst`
    /// (Security section) for the full threat model, and Python's
    /// [pickle module security warning][1] for why `pickle.loads` is
    /// unsafe in general.
    ///
    /// [1]: https://docs.python.org/3/library/pickle.html#module-pickle
    pub fn with_python_udf_inlining(mut self, enabled: bool) -> Self {
        self.python_udf_inlining = enabled;
        self
    }

    pub fn python_udf_inlining(&self) -> bool {
        self.python_udf_inlining
    }
}

impl Default for PythonLogicalCodec {
    fn default() -> Self {
        Self::new(Arc::new(DefaultLogicalExtensionCodec {}))
    }
}

impl LogicalExtensionCodec for PythonLogicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &TaskContext,
    ) -> Result<Extension> {
        chain_try(&self.chain, "an extension logical plan node", |codec| {
            codec.try_decode(buf, inputs, ctx)
        })
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<()> {
        chain_encode(
            &self.chain,
            buf,
            "an extension logical plan node",
            |codec, buf| codec.try_encode(node, buf),
        )
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        chain_try(&self.chain, "a table provider", |codec| {
            codec.try_decode_table_provider(buf, table_ref, Arc::clone(&schema), ctx)
        })
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        chain_encode(&self.chain, buf, "a table provider", |codec, buf| {
            codec.try_encode_table_provider(table_ref, Arc::clone(&node), buf)
        })
    }

    fn try_decode_file_format(
        &self,
        buf: &[u8],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn FileFormatFactory>> {
        chain_try(&self.chain, "a file format", |codec| {
            codec.try_decode_file_format(buf, ctx)
        })
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn FileFormatFactory>,
    ) -> Result<()> {
        chain_encode(&self.chain, buf, "a file format", |codec, buf| {
            codec.try_encode_file_format(buf, Arc::clone(&node))
        })
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        if self.python_udf_inlining && try_encode_python_scalar_udf(node, buf)? {
            return Ok(());
        }
        chain_encode(&self.chain, buf, "a scalar UDF", |codec, buf| {
            codec.try_encode_udf(node, buf)
        })
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        if self.python_udf_inlining {
            if let Some(udf) = try_decode_python_scalar_udf(buf)? {
                return Ok(udf);
            }
        } else {
            refuse_if_inline(buf, PY_SCALAR_UDF_FAMILY, "scalar UDF", name)?;
        }
        chain_try(&self.chain, "a scalar UDF", |codec| {
            codec.try_decode_udf(name, buf)
        })
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        if self.python_udf_inlining && try_encode_python_udaf(node, buf)? {
            return Ok(());
        }
        chain_encode(&self.chain, buf, "an aggregate UDF", |codec, buf| {
            codec.try_encode_udaf(node, buf)
        })
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        if self.python_udf_inlining {
            if let Some(udaf) = try_decode_python_udaf(buf)? {
                return Ok(udaf);
            }
        } else {
            refuse_if_inline(buf, PY_AGG_UDF_FAMILY, "aggregate UDF", name)?;
        }
        chain_try(&self.chain, "an aggregate UDF", |codec| {
            codec.try_decode_udaf(name, buf)
        })
    }

    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
        if self.python_udf_inlining && try_encode_python_udwf(node, buf)? {
            return Ok(());
        }
        chain_encode(&self.chain, buf, "a window UDF", |codec, buf| {
            codec.try_encode_udwf(node, buf)
        })
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        if self.python_udf_inlining {
            if let Some(udwf) = try_decode_python_udwf(buf)? {
                return Ok(udwf);
            }
        } else {
            refuse_if_inline(buf, PY_WINDOW_UDF_FAMILY, "window UDF", name)?;
        }
        chain_try(&self.chain, "a window UDF", |codec| {
            codec.try_decode_udwf(name, buf)
        })
    }
}

/// Strict-mode gate: if `buf` is a well-framed inline payload for
/// `family`, return the strict-refusal error; otherwise return
/// `Ok(())` so the caller can delegate to its codec chain.
///
/// Routing through [`read_framed_payload`] (rather than a bare
/// `starts_with` probe) means malformed inline bytes — wrong
/// wire-format version, mismatched Python version, truncated header —
/// surface *their* diagnostic instead of the strict-mode message.
/// The strict message implies sender intent ("inlining is disabled"),
/// so it should fire only when the bytes really would have decoded.
///
/// Fast path: short-circuit on the family-magic prefix before
/// acquiring the GIL. Plans with many non-Python UDFs would otherwise
/// pay a GIL acquisition per decode call just to confirm "not a
/// Python UDF". `read_framed_payload` itself rejects buffers that
/// don't start with `family`, so this is purely an optimization.
fn refuse_if_inline(buf: &[u8], family: &[u8], kind: &str, name: &str) -> Result<()> {
    if !buf.starts_with(family) {
        return Ok(());
    }
    Python::attach(|py| match read_framed_payload(py, buf, family, kind)? {
        Some(_) => Err(refuse_inline_payload(kind, name)),
        None => Ok(()),
    })
}

/// Build the error returned by a strict codec when it receives an
/// inline Python-UDF payload it has been told not to deserialize.
fn refuse_inline_payload(kind: &str, name: &str) -> datafusion::error::DataFusionError {
    // `Execution`, not `Plan`: this is a wire-format decode refusal at
    // codec time, not a planner-stage failure. Downstream error
    // classification keys off the variant — surfacing this as a planner
    // error would mis-route it into "fix your SQL" buckets.
    datafusion::error::DataFusionError::Execution(format!(
        "Refusing to deserialize inline Python {kind} '{name}': Python UDF \
         inlining is disabled on this session. Two remediations: \
         (1) ask the sender to re-encode with inlining disabled so '{name}' \
         travels by name, and register '{name}' on this receiver; or \
         (2) enable inlining on this receiver (accepts the cloudpickle \
         execution risk on inbound payloads). Receivers cannot re-encode \
         bytes they did not produce."
    ))
}

/// `PhysicalExtensionCodec` mirror of [`PythonLogicalCodec`] parked
/// on the same `SessionContext`. Carries the Python-aware encoding
/// hooks for physical-layer types (`ExecutionPlan`, `PhysicalExpr`)
/// and delegates the rest to the composable codec chain (see
/// [`PythonLogicalCodec`] for chain ordering and dispatch rules).
///
/// The `PhysicalExtensionCodec` trait has its own `try_encode_udf`
/// / `try_decode_udf` pair distinct from the logical one, so a
/// `ScalarUDF` referenced inside a physical plan needs Python-aware
/// encoding on this layer too — otherwise a plan with a Python UDF
/// would round-trip at the logical level but break at the physical
/// level. Both layers reuse the shared payload framing
/// ([`PY_SCALAR_UDF_FAMILY`] et al.) so the wire format is identical.
///
/// Like [`PythonLogicalCodec`], this does not retain the session it was built
/// from; see that type for why.
#[derive(Debug, Clone)]
pub struct PythonPhysicalCodec {
    chain: Vec<Arc<dyn PhysicalExtensionCodec>>,
    python_udf_inlining: bool,
}

impl PythonPhysicalCodec {
    pub fn new(inner: Arc<dyn PhysicalExtensionCodec>) -> Self {
        Self {
            chain: vec![inner],
            python_udf_inlining: true,
        }
    }

    /// Return a copy of this codec with `codec` prepended to the
    /// chain, preserving the Python-UDF-inlining setting. The new
    /// codec is consulted before every previously installed codec.
    pub fn with_additional_codec(&self, codec: Arc<dyn PhysicalExtensionCodec>) -> Self {
        let mut chain = Vec::with_capacity(self.chain.len() + 1);
        chain.push(codec);
        chain.extend(self.chain.iter().map(Arc::clone));
        Self {
            chain,
            python_udf_inlining: self.python_udf_inlining,
        }
    }

    /// Toggle inline encoding of Python UDFs on this physical codec.
    ///
    /// Mirrors [`PythonLogicalCodec::with_python_udf_inlining`]; see
    /// that method for the full security and portability discussion.
    pub fn with_python_udf_inlining(mut self, enabled: bool) -> Self {
        self.python_udf_inlining = enabled;
        self
    }

    pub fn python_udf_inlining(&self) -> bool {
        self.python_udf_inlining
    }
}

impl Default for PythonPhysicalCodec {
    fn default() -> Self {
        Self::new(Arc::new(DefaultPhysicalExtensionCodec {}))
    }
}

impl PhysicalExtensionCodec for PythonPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        chain_try(&self.chain, "an execution plan", |codec| {
            codec.try_decode(buf, inputs, ctx, proto_converter)
        })
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        chain_encode(&self.chain, buf, "an execution plan", |codec, buf| {
            codec.try_encode(Arc::clone(&node), buf, proto_converter)
        })
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        if self.python_udf_inlining && try_encode_python_scalar_udf(node, buf)? {
            return Ok(());
        }
        chain_encode(&self.chain, buf, "a scalar UDF", |codec, buf| {
            codec.try_encode_udf(node, buf)
        })
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        if self.python_udf_inlining {
            if let Some(udf) = try_decode_python_scalar_udf(buf)? {
                return Ok(udf);
            }
        } else {
            refuse_if_inline(buf, PY_SCALAR_UDF_FAMILY, "scalar UDF", name)?;
        }
        chain_try(&self.chain, "a scalar UDF", |codec| {
            codec.try_decode_udf(name, buf)
        })
    }

    fn try_encode_expr(
        &self,
        node: &Arc<dyn PhysicalExpr>,
        buf: &mut Vec<u8>,
        ctx: &PhysicalExprEncodeCtx<'_>,
    ) -> Result<()> {
        chain_encode(&self.chain, buf, "a physical expression", |codec, buf| {
            codec.try_encode_expr(node, buf, ctx)
        })
    }

    fn try_decode_expr(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn PhysicalExpr>],
        ctx: &PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        chain_try(&self.chain, "a physical expression", |codec| {
            codec.try_decode_expr(buf, inputs, ctx)
        })
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        if self.python_udf_inlining && try_encode_python_udaf(node, buf)? {
            return Ok(());
        }
        chain_encode(&self.chain, buf, "an aggregate UDF", |codec, buf| {
            codec.try_encode_udaf(node, buf)
        })
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        if self.python_udf_inlining {
            if let Some(udaf) = try_decode_python_udaf(buf)? {
                return Ok(udaf);
            }
        } else {
            refuse_if_inline(buf, PY_AGG_UDF_FAMILY, "aggregate UDF", name)?;
        }
        chain_try(&self.chain, "an aggregate UDF", |codec| {
            codec.try_decode_udaf(name, buf)
        })
    }

    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
        if self.python_udf_inlining && try_encode_python_udwf(node, buf)? {
            return Ok(());
        }
        chain_encode(&self.chain, buf, "a window UDF", |codec, buf| {
            codec.try_encode_udwf(node, buf)
        })
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        if self.python_udf_inlining {
            if let Some(udwf) = try_decode_python_udwf(buf)? {
                return Ok(udwf);
            }
        } else {
            refuse_if_inline(buf, PY_WINDOW_UDF_FAMILY, "window UDF", name)?;
        }
        chain_try(&self.chain, "a window UDF", |codec| {
            codec.try_decode_udwf(name, buf)
        })
    }
}

// =============================================================================
// Shared Python scalar UDF encode / decode helpers
//
// Both `PythonLogicalCodec` and `PythonPhysicalCodec` consult these on
// every `try_encode_udf` / `try_decode_udf` call. Same wire format on
// both layers — a Python `ScalarUDF` referenced inside a `LogicalPlan`
// or an `ExecutionPlan` round-trips identically.
// =============================================================================

/// Encode a Python scalar UDF inline if `node` is one. Returns
/// `Ok(true)` when the payload (`DFPYUDF` family prefix, version byte,
/// cloudpickled tuple) was written and the caller should skip its
/// inner codec. Returns `Ok(false)` for any non-Python UDF, signalling
/// the caller to delegate to its codec chain.
pub(crate) fn try_encode_python_scalar_udf(node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<bool> {
    let Some(py_udf) = node.inner().downcast_ref::<PythonFunctionScalarUDF>() else {
        return Ok(false);
    };

    Python::attach(|py| -> Result<bool> {
        let bytes = encode_python_scalar_udf(py, py_udf).map_err(to_datafusion_err)?;
        append_framed_payload(py, buf, PY_SCALAR_UDF_FAMILY, &bytes)?;
        Ok(true)
    })
}

/// Decode an inline Python scalar UDF payload. Returns `Ok(None)`
/// when `buf` does not carry the `DFPYUDF` family prefix, signalling
/// the caller to delegate to its codec chain (and eventually the
/// `FunctionRegistry`).
pub(crate) fn try_decode_python_scalar_udf(buf: &[u8]) -> Result<Option<Arc<ScalarUDF>>> {
    if !buf.starts_with(PY_SCALAR_UDF_FAMILY) {
        return Ok(None);
    }
    Python::attach(|py| -> Result<Option<Arc<ScalarUDF>>> {
        let Some(payload) = read_framed_payload(py, buf, PY_SCALAR_UDF_FAMILY, "scalar UDF")?
        else {
            return Ok(None);
        };
        let udf = decode_python_scalar_udf(py, payload).map_err(to_datafusion_err)?;
        Ok(Some(Arc::new(ScalarUDF::new_from_impl(udf))))
    })
}

/// Build the cloudpickle payload for a `PythonFunctionScalarUDF`.
///
/// Layout: `cloudpickle.dumps((name, func, input_schema_bytes,
/// return_schema_bytes, volatility_str))`. Schema blobs are produced
/// by arrow-rs's native IPC stream writer (no pyarrow round-trip) and
/// decoded with the matching stream reader on the receiver. See
/// [`build_input_schema_bytes`] for what the input blob carries.
fn encode_python_scalar_udf(py: Python<'_>, udf: &PythonFunctionScalarUDF) -> PyResult<Vec<u8>> {
    let signature = udf.signature();
    let input_dtypes = signature_input_dtypes(signature, "PythonFunctionScalarUDF")?;
    let input_schema_bytes = build_input_schema_bytes(&input_dtypes)?;
    let return_schema_bytes = build_single_field_schema_bytes(udf.return_field().as_ref())?;
    let volatility = volatility_wire_str(signature.volatility);

    let payload = PyTuple::new(
        py,
        [
            udf.name().into_pyobject(py)?.into_any(),
            udf.func().bind(py).clone().into_any(),
            PyBytes::new(py, &input_schema_bytes).into_any(),
            PyBytes::new(py, &return_schema_bytes).into_any(),
            volatility.into_pyobject(py)?.into_any(),
        ],
    )?;

    cloudpickle(py)?
        .call_method1("dumps", (payload,))?
        .extract::<Vec<u8>>()
}

/// Inverse of [`encode_python_scalar_udf`].
fn decode_python_scalar_udf(py: Python<'_>, payload: &[u8]) -> PyResult<PythonFunctionScalarUDF> {
    let tuple = cloudpickle(py)?
        .call_method1("loads", (PyBytes::new(py, payload),))?
        .cast_into::<PyTuple>()?;

    let name: String = tuple.get_item(0)?.extract()?;
    let func: Py<PyAny> = tuple.get_item(1)?.unbind();
    let input_schema_bytes: Vec<u8> = tuple.get_item(2)?.extract()?;
    let return_schema_bytes: Vec<u8> = tuple.get_item(3)?.extract()?;
    let volatility_str: String = tuple.get_item(4)?.extract()?;

    let input_types = read_input_dtypes(&input_schema_bytes)?;
    let return_field = read_single_return_field(&return_schema_bytes, "PythonFunctionScalarUDF")?;
    let volatility = parse_volatility_str(&volatility_str)?;

    Ok(PythonFunctionScalarUDF::from_parts(
        name,
        func,
        input_types,
        return_field,
        volatility,
    ))
}

/// Serialize a `Schema` to a self-contained IPC stream containing
/// only the schema message (no record batches). Inverse:
/// [`schema_from_ipc_bytes`].
fn schema_to_ipc_bytes(schema: &Schema) -> arrow::error::Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema)?;
        writer.finish()?;
    }
    Ok(buf)
}

/// Decode an IPC stream containing only a schema message back into a
/// `Schema`. Inverse: [`schema_to_ipc_bytes`].
fn schema_from_ipc_bytes(bytes: &[u8]) -> arrow::error::Result<Schema> {
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    Ok(reader.schema().as_ref().clone())
}

/// Extract the per-arg `DataType`s from a `Signature` known to be
/// `TypeSignature::Exact` (all Python-defined UDFs are constructed
/// with `Signature::exact`). Any other variant indicates the impl was
/// not built by this crate's UDF/UDAF/UDWF constructors.
fn signature_input_dtypes(signature: &Signature, kind: &str) -> PyResult<Vec<DataType>> {
    match &signature.type_signature {
        TypeSignature::Exact(types) => Ok(types.clone()),
        other => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "{kind} expected Signature::Exact, got {other:?}"
        ))),
    }
}

/// Wrap per-arg `DataType`s in synthetic `arg_{i}` fields and emit
/// the IPC schema blob the encoder writes into the cloudpickle tuple.
///
/// The names and `nullable: true` are arbitrary: the underlying
/// `TypeSignature::Exact` carries no per-input nullability or
/// metadata, and the receiver collapses these fields back to
/// `Vec<DataType>` via [`read_input_dtypes`], so anything set here
/// beyond the data type is discarded on decode.
fn build_input_schema_bytes(dtypes: &[DataType]) -> PyResult<Vec<u8>> {
    let fields: Vec<Field> = dtypes
        .iter()
        .enumerate()
        .map(|(i, dt)| Field::new(format!("arg_{i}"), dt.clone(), true))
        .collect();
    schema_to_ipc_bytes(&Schema::new(fields)).map_err(arrow_to_py_err)
}

/// Emit a single-field IPC schema blob. Used for return-type and
/// state-field payloads where the receiver needs to recover field
/// metadata (names, nullability, key/value attributes) verbatim.
fn build_single_field_schema_bytes(field: &Field) -> PyResult<Vec<u8>> {
    schema_to_ipc_bytes(&Schema::new(vec![field.clone()])).map_err(arrow_to_py_err)
}

/// Emit a multi-field IPC schema blob.
fn build_schema_bytes(fields: Vec<Field>) -> PyResult<Vec<u8>> {
    schema_to_ipc_bytes(&Schema::new(fields)).map_err(arrow_to_py_err)
}

/// Decode the per-arg `DataType`s the encoder wrote via
/// [`build_input_schema_bytes`].
fn read_input_dtypes(bytes: &[u8]) -> PyResult<Vec<DataType>> {
    let schema = schema_from_ipc_bytes(bytes).map_err(arrow_to_py_err)?;
    Ok(schema
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect())
}

/// Decode a single-field IPC schema blob and return that field by
/// value. `kind` names the UDF flavor in the error message produced
/// when the blob is empty (should be unreachable for sender-side
/// payloads built via [`build_single_field_schema_bytes`]).
fn read_single_return_field(bytes: &[u8], kind: &str) -> PyResult<Field> {
    let schema = schema_from_ipc_bytes(bytes).map_err(arrow_to_py_err)?;
    let field = schema.fields().first().ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "{kind} return schema must contain exactly one field"
        ))
    })?;
    Ok(field.as_ref().clone())
}

fn arrow_to_py_err(e: arrow::error::ArrowError) -> PyErr {
    pyo3::exceptions::PyValueError::new_err(format!("{e}"))
}

fn parse_volatility_str(s: &str) -> PyResult<Volatility> {
    datafusion_python_util::parse_volatility(s)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e}")))
}

/// Stable wire-format string for a `Volatility`. Pinned to the three
/// tokens [`datafusion_python_util::parse_volatility`] accepts, so an
/// upstream change to `Volatility`'s `Debug` repr cannot silently
/// produce bytes the decoder rejects.
fn volatility_wire_str(v: Volatility) -> &'static str {
    match v {
        Volatility::Immutable => "immutable",
        Volatility::Stable => "stable",
        Volatility::Volatile => "volatile",
    }
}

/// Read the interpreter's `sys.version_info` as `(major, minor)`.
///
/// Used by encoder/decoder to stamp and verify the Python version a
/// cloudpickle payload was produced on. cloudpickle is not portable
/// across Python minor versions; the wire header carries these bytes
/// so a mismatch surfaces an actionable error instead of an opaque
/// `marshal` failure at `cloudpickle.loads` time.
fn current_python_version(py: Python<'_>) -> PyResult<(u8, u8)> {
    let version_info = py.import("sys")?.getattr("version_info")?;
    let major: u8 = version_info.getattr("major")?.extract()?;
    let minor: u8 = version_info.getattr("minor")?.extract()?;
    Ok((major, minor))
}

/// Stamp `buf` with the framing header for `family` plus the current
/// Python `(major, minor)`, then append `payload`. Bundles the
/// `current_python_version` lookup with the header write so each
/// encoder call site stays one line.
fn append_framed_payload(
    py: Python<'_>,
    buf: &mut Vec<u8>,
    family: &[u8],
    payload: &[u8],
) -> Result<()> {
    let py_version = current_python_version(py).map_err(to_datafusion_err)?;
    write_wire_header(buf, family, py_version);
    buf.extend_from_slice(payload);
    Ok(())
}

/// Inspect `buf`'s framing against `family` + the current Python
/// `(major, minor)`. Returns `Ok(None)` when `buf` does not carry
/// `family` (caller should delegate); `Ok(Some(payload))` when the
/// framing matches; `Err(_)` for a recognised family at the wrong
/// wire-format or Python version (see [`strip_wire_header`]).
fn read_framed_payload<'a>(
    py: Python<'_>,
    buf: &'a [u8],
    family: &[u8],
    kind: &str,
) -> Result<Option<&'a [u8]>> {
    let py_version = current_python_version(py).map_err(to_datafusion_err)?;
    strip_wire_header(buf, family, kind, py_version)
}

/// Cached handle to the `cloudpickle` module.
///
/// The encode/decode helpers above would otherwise re-resolve the
/// module on every call. `py.import` is backed by `sys.modules` and
/// therefore cheap, but each call still walks a dict and re-binds the
/// result; a plan with many Python UDFs pays that cost per UDF.
///
/// `PyOnceLock` scopes the cached `Py<PyAny>` to the current
/// interpreter, so the slot drops cleanly on interpreter teardown
/// (relevant under CPython subinterpreters, PEP 684) instead of
/// resurrecting a `Py` rooted in a dead interpreter on the next call.
fn cloudpickle<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    static CLOUDPICKLE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    CLOUDPICKLE
        .get_or_try_init(py, || Ok(py.import("cloudpickle")?.unbind().into_any()))
        .map(|cached| cached.bind(py).clone())
}

// =============================================================================
// Shared Python window UDF encode / decode helpers
//
// Cloudpickle tuple shape: `(name, evaluator_factory, input_schema_bytes,
// return_schema_bytes, volatility_str)`. The evaluator factory is the
// Python callable that produces a new evaluator instance per partition.
// =============================================================================

pub(crate) fn try_encode_python_udwf(node: &WindowUDF, buf: &mut Vec<u8>) -> Result<bool> {
    let Some(py_udf) = node.inner().downcast_ref::<PythonFunctionWindowUDF>() else {
        return Ok(false);
    };

    Python::attach(|py| -> Result<bool> {
        let bytes = encode_python_udwf(py, py_udf).map_err(to_datafusion_err)?;
        append_framed_payload(py, buf, PY_WINDOW_UDF_FAMILY, &bytes)?;
        Ok(true)
    })
}

pub(crate) fn try_decode_python_udwf(buf: &[u8]) -> Result<Option<Arc<WindowUDF>>> {
    if !buf.starts_with(PY_WINDOW_UDF_FAMILY) {
        return Ok(None);
    }
    Python::attach(|py| -> Result<Option<Arc<WindowUDF>>> {
        let Some(payload) = read_framed_payload(py, buf, PY_WINDOW_UDF_FAMILY, "window UDF")?
        else {
            return Ok(None);
        };
        let udf = decode_python_udwf(py, payload).map_err(to_datafusion_err)?;
        Ok(Some(Arc::new(WindowUDF::new_from_impl(udf))))
    })
}

fn encode_python_udwf(py: Python<'_>, udf: &PythonFunctionWindowUDF) -> PyResult<Vec<u8>> {
    let signature = WindowUDFImpl::signature(udf);
    let input_dtypes = signature_input_dtypes(signature, "PythonFunctionWindowUDF")?;
    let input_schema_bytes = build_input_schema_bytes(&input_dtypes)?;
    let return_field = Field::new("result", udf.return_type().clone(), true);
    let return_schema_bytes = build_single_field_schema_bytes(&return_field)?;
    let volatility = volatility_wire_str(signature.volatility);

    let payload = PyTuple::new(
        py,
        [
            WindowUDFImpl::name(udf).into_pyobject(py)?.into_any(),
            udf.evaluator().bind(py).clone().into_any(),
            PyBytes::new(py, &input_schema_bytes).into_any(),
            PyBytes::new(py, &return_schema_bytes).into_any(),
            volatility.into_pyobject(py)?.into_any(),
        ],
    )?;

    cloudpickle(py)?
        .call_method1("dumps", (payload,))?
        .extract::<Vec<u8>>()
}

fn decode_python_udwf(py: Python<'_>, payload: &[u8]) -> PyResult<PythonFunctionWindowUDF> {
    let tuple = cloudpickle(py)?
        .call_method1("loads", (PyBytes::new(py, payload),))?
        .cast_into::<PyTuple>()?;

    let name: String = tuple.get_item(0)?.extract()?;
    let evaluator: Py<PyAny> = tuple.get_item(1)?.unbind();
    let input_schema_bytes: Vec<u8> = tuple.get_item(2)?.extract()?;
    let return_schema_bytes: Vec<u8> = tuple.get_item(3)?.extract()?;
    let volatility_str: String = tuple.get_item(4)?.extract()?;

    let input_types = read_input_dtypes(&input_schema_bytes)?;
    let return_type = read_single_return_field(&return_schema_bytes, "PythonFunctionWindowUDF")?
        .data_type()
        .clone();
    let volatility = parse_volatility_str(&volatility_str)?;

    Ok(PythonFunctionWindowUDF::new(
        name,
        evaluator,
        input_types,
        return_type,
        volatility,
    ))
}

// =============================================================================
// Shared Python aggregate UDF encode / decode helpers
//
// Cloudpickle tuple shape: `(name, accumulator_factory, input_schema_bytes,
// return_schema_bytes, state_schema_bytes, volatility_str)`. The accumulator
// factory is the Python callable that produces a new accumulator instance
// per partition.
// =============================================================================

pub(crate) fn try_encode_python_udaf(node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<bool> {
    let Some(py_udf) = node.inner().downcast_ref::<PythonFunctionAggregateUDF>() else {
        return Ok(false);
    };

    Python::attach(|py| -> Result<bool> {
        let bytes = encode_python_udaf(py, py_udf).map_err(to_datafusion_err)?;
        append_framed_payload(py, buf, PY_AGG_UDF_FAMILY, &bytes)?;
        Ok(true)
    })
}

pub(crate) fn try_decode_python_udaf(buf: &[u8]) -> Result<Option<Arc<AggregateUDF>>> {
    if !buf.starts_with(PY_AGG_UDF_FAMILY) {
        return Ok(None);
    }
    Python::attach(|py| -> Result<Option<Arc<AggregateUDF>>> {
        let Some(payload) = read_framed_payload(py, buf, PY_AGG_UDF_FAMILY, "aggregate UDF")?
        else {
            return Ok(None);
        };
        let udf = decode_python_udaf(py, payload).map_err(to_datafusion_err)?;
        Ok(Some(Arc::new(AggregateUDF::new_from_impl(udf))))
    })
}

fn encode_python_udaf(py: Python<'_>, udf: &PythonFunctionAggregateUDF) -> PyResult<Vec<u8>> {
    let signature = AggregateUDFImpl::signature(udf);
    let input_dtypes = signature_input_dtypes(signature, "PythonFunctionAggregateUDF")?;
    let input_schema_bytes = build_input_schema_bytes(&input_dtypes)?;
    let return_field = Field::new("result", udf.return_type().clone(), true);
    let return_schema_bytes = build_single_field_schema_bytes(&return_field)?;
    let state_fields: Vec<Field> = udf
        .state_fields_ref()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let state_schema_bytes = build_schema_bytes(state_fields)?;
    let volatility = volatility_wire_str(signature.volatility);

    let payload = PyTuple::new(
        py,
        [
            AggregateUDFImpl::name(udf).into_pyobject(py)?.into_any(),
            udf.accumulator().bind(py).clone().into_any(),
            PyBytes::new(py, &input_schema_bytes).into_any(),
            PyBytes::new(py, &return_schema_bytes).into_any(),
            PyBytes::new(py, &state_schema_bytes).into_any(),
            volatility.into_pyobject(py)?.into_any(),
        ],
    )?;

    cloudpickle(py)?
        .call_method1("dumps", (payload,))?
        .extract::<Vec<u8>>()
}

fn decode_python_udaf(py: Python<'_>, payload: &[u8]) -> PyResult<PythonFunctionAggregateUDF> {
    let tuple = cloudpickle(py)?
        .call_method1("loads", (PyBytes::new(py, payload),))?
        .cast_into::<PyTuple>()?;

    let name: String = tuple.get_item(0)?.extract()?;
    let accumulator: Py<PyAny> = tuple.get_item(1)?.unbind();
    let input_schema_bytes: Vec<u8> = tuple.get_item(2)?.extract()?;
    let return_schema_bytes: Vec<u8> = tuple.get_item(3)?.extract()?;
    let state_schema_bytes: Vec<u8> = tuple.get_item(4)?.extract()?;
    let volatility_str: String = tuple.get_item(5)?.extract()?;

    let input_types = read_input_dtypes(&input_schema_bytes)?;
    let return_type = read_single_return_field(&return_schema_bytes, "PythonFunctionAggregateUDF")?
        .data_type()
        .clone();
    // Preserve the encoded state field metadata (names, nullability,
    // arbitrary key/value attributes) so the post-decode UDF reports
    // the same state schema as the sender's instance — important for
    // accumulators whose `StateFieldsArgs` consumers key off names or
    // nullability rather than positional `DataType`.
    let state_schema = schema_from_ipc_bytes(&state_schema_bytes).map_err(arrow_to_py_err)?;
    let state_fields: Vec<arrow::datatypes::FieldRef> =
        state_schema.fields().iter().cloned().collect();
    let volatility = parse_volatility_str(&volatility_str)?;

    Ok(PythonFunctionAggregateUDF::from_parts(
        name,
        accumulator,
        input_types,
        return_type,
        state_fields,
        volatility,
    ))
}

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

(extension_checklist)=

# Extension author checklist

The rules in this section, gathered into one list to run through before you
publish. Each links to the page that explains it.

## Protocol

- [ ] **Every getter's capsule name matches its method name.**
      `__datafusion_<thing>__` returns a capsule named `datafusion_<thing>`.
      → {ref}`extension_capsule_protocol`
- [ ] **Your getter does not inspect its argument.** Pass it to
      `ffi_logical_codec_from_pycapsule` and move on. It is not always a
      session, and when it is, it is not the Python `SessionContext` wrapper.
      → {ref}`extension_getter_argument`
- [ ] **You never construct a `SessionContext` inside your library.** Take
      what the FFI constructors need off the argument you were handed. A
      context built inline is already dropped by the time the capsule is used.
      → {ref}`extension_getter_argument`
- [ ] **You do not depend on the `datafusion-python` crate.**
      → {ref}`extension_why_ffi`

## Codecs

- [ ] **Your codec claims narrowly.** Downcast to your own types. Claiming a
      broad category takes nodes from every library installed after you and
      makes your library order-sensitive for everyone downstream.
      → {ref}`extension_codec_order`
- [ ] **You declare `__datafusion_codec_id__` if you might rename the class.**
      The default id is the exporting class's import path, so a rename stops
      older plans decoding. → {ref}`extension_codec_ids`
- [ ] **Name-only decoders check `name` before trusting `buf`.** An empty
      payload has no id to route on, so your `try_decode_udf` can be called
      with another library's function name and an empty buffer.
      → {ref}`extension_codecs`
- [ ] **You round-trip a plan in a test and assert *your* codec did the work.**
      Both being installed does not mean your node reached you.
      → {ref}`extension_codec_order`
- [ ] **You ship a logical codec too, if you contribute a table provider.** A
      physical codec is not enough: an installed query planner receives the
      logical plan, which holds your provider, and the session fails to plan
      without one. → {ref}`extension_codec_provider_logical`
- [ ] **You decode in a *different process* in at least one test.** A codec
      that parks the object in a process-global map passes every in-process
      round trip and fails the first real one.
      → {ref}`extension_codec_durable_metadata`

## Bundles and planners

- [ ] **You ship a bundle, not loose pieces**, if you have codecs or a planner.
      → {ref}`extension_bundles`
- [ ] **Your bundle is configuration-only.** Fresh components on every call,
      no cached bound components, no retaining the context passed in, no
      registering anything on it — a factory that mutates the context is not
      rolled back if a later factory raises.
      → {ref}`extension_bundles`
- [ ] **Your codecs are objects exposing the getter, not bare capsules.**
      `with_extensions` refuses a capsule, because there would be nothing to
      name the codec by. → {ref}`extension_bundles_codecs_are_objects`
- [ ] **Your planner hook wraps `fallback` and delegates to it.** Ignoring it
      replaces every layer beneath you, which is legal but not composable —
      unless your planner rewrites the plan, as in the next item.
      → {ref}`extension_bundles`
- [ ] **Your planner plans for itself if it rewrites the plan**, leaving
      `fallback` unused. The two are exclusive: delegating hands planning back
      to the host and returns nodes you can neither downcast nor split.
      Planning for yourself then means supplying your *own* optimizer rules,
      because a session that arrived over FFI carries the host's.
      → {ref}`planner_host_optimizer_rules`
- [ ] **Your planner hook returns `None`, not `fallback`, when it has nothing
      to contribute.** Returning `fallback` installs the session's own planner
      as a foreign one and adds an FFI hop that was not there.
      → {ref}`extension_bundles`
- [ ] **You read the host's codec chains in the planner hook, not the extension
      hook.** Phase one runs before anything is installed.
      → {ref}`extension_bundles_two_phases`
- [ ] **If you also offer the low-level path**, document that codecs go in
      before a layered planner. → {ref}`planner_codec_rebinding`

## Packaging and documentation

- [ ] **You state which `datafusion` version your release requires.** A
      mismatch raises an `ImportError` on import, which is a good failure — but
      only if your users know what to install. → {ref}`extension_version_mismatch`
- [ ] **You tell your users to keep a context alive** for as long as anything
      derived from it is in use. This is the rule most likely to arrive as a
      bug report against your library. → {ref}`extension_sessions`
- [ ] **Your production codec serializes durable metadata**, not a
      process-local token. The examples in this repository use tokens to make
      ownership observable; that is a demonstration, not a pattern.
      → {ref}`extension_codec_durable_metadata`
- [ ] **You have integration tests across a real FFI boundary.** The example
      trees in this repository are the pattern: build the cdylib, install the
      wheel, then exercise it from Python. `examples/distributed` additionally
      spawns worker processes, which is the only way to catch a codec that
      only works in the process that wrote it.
- [ ] **If you ship an engine, say which worker-parity items you handle** and
      which you leave to your users. A `SessionContext` cannot be snapshotted
      and restored elsewhere, so every one of them is somebody's job, and your
      users cannot tell whose from the outside.
      → {ref}`distributed_worker_parity`

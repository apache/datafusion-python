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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::physical_plan::metrics::{MetricValue, MetricsSet, Metric};
use pyo3::prelude::*;

#[pyclass(frozen, name = "MetricsSet", module = "datafusion")]
#[derive(Debug, Clone)]
pub struct PyMetricsSet {
    metrics: MetricsSet,
}

impl PyMetricsSet {
    pub fn new(metrics: MetricsSet) -> Self {
        Self { metrics }
    }
}

#[pymethods]
impl PyMetricsSet {
    fn metrics(&self) -> Vec<PyMetric> {
        self.metrics
            .iter()
            .map(|m| PyMetric::new(Arc::clone(m)))
            .collect()
    }

    fn output_rows(&self) -> Option<usize> {
        self.metrics.output_rows()
    }

    fn elapsed_compute(&self) -> Option<usize> {
        self.metrics.elapsed_compute()
    }

    fn spill_count(&self) -> Option<usize> {
        self.metrics.spill_count()
    }

    fn spilled_bytes(&self) -> Option<usize> {
        self.metrics.spilled_bytes()
    }

    fn spilled_rows(&self) -> Option<usize> {
        self.metrics.spilled_rows()
    }

    fn sum_by_name(&self, name: &str) -> Option<usize> {
        self.metrics.sum_by_name(name).map(|v| v.as_usize())
    }

    fn __repr__(&self) -> String {
        format!("{}", self.metrics)
    }
}

#[pyclass(frozen, name = "Metric", module = "datafusion")]
#[derive(Debug, Clone)]
pub struct PyMetric {
    metric: Arc<Metric>,
}

impl PyMetric {
    pub fn new(metric: Arc<Metric>) -> Self {
        Self { metric }
    }
}

#[pymethods]
impl PyMetric {
    #[getter]
    fn name(&self) -> String {
        self.metric.value().name().to_string()
    }

    /// Returns the numeric value of this metric as a `usize`, or `None` when the
    /// value is not representable as an integer.
    ///
    /// # Note
    /// `StartTimestamp` and `EndTimestamp` metrics are returned as nanoseconds
    /// since the Unix epoch (via `timestamp_nanos_opt`), which may overflow
    /// a `usize` on 32-bit platforms or return `None` if the timestamp is out
    /// of range.  Non-numeric metric variants (unrecognised future variants)
    /// also return `None`.
    #[getter]
    fn value(&self) -> Option<usize> {
        match self.metric.value() {
            MetricValue::OutputRows(c) => Some(c.value()),
            MetricValue::OutputBytes(c) => Some(c.value()),
            MetricValue::ElapsedCompute(t) => Some(t.value()),
            MetricValue::SpillCount(c) => Some(c.value()),
            MetricValue::SpilledBytes(c) => Some(c.value()),
            MetricValue::SpilledRows(c) => Some(c.value()),
            MetricValue::CurrentMemoryUsage(g) => Some(g.value()),
            MetricValue::Count { count, .. } => Some(count.value()),
            MetricValue::Gauge { gauge, .. } => Some(gauge.value()),
            MetricValue::Time { time, .. } => Some(time.value()),
            MetricValue::StartTimestamp(ts) => {
                ts.value().and_then(|dt| dt.timestamp_nanos_opt().map(|n| n as usize))
            }
            MetricValue::EndTimestamp(ts) => {
                ts.value().and_then(|dt| dt.timestamp_nanos_opt().map(|n| n as usize))
            }
            _ => None,
        }
    }

    /// Returns the value as a Python `datetime` for `StartTimestamp` / `EndTimestamp`
    /// metrics, or `None` for all other metric types.
    fn value_as_datetime<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        match self.metric.value() {
            MetricValue::StartTimestamp(ts) | MetricValue::EndTimestamp(ts) => {
                match ts.value() {
                    Some(dt) => {
                        let nanos = dt.timestamp_nanos_opt()
                            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyOverflowError, _>(
                                "timestamp out of range"
                            ))?;
                        let datetime_mod = py.import("datetime")?;
                        let datetime_cls = datetime_mod.getattr("datetime")?;
                        let tz_utc = datetime_mod.getattr("timezone")?.getattr("utc")?;
                        let secs = nanos / 1_000_000_000;
                        let micros = (nanos % 1_000_000_000) / 1_000;
                        let result = datetime_cls.call_method1(
                            "fromtimestamp",
                            (secs as f64 + micros as f64 / 1_000_000.0, tz_utc),
                        )?;
                        Ok(Some(result))
                    }
                    None => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }

    #[getter]
    fn partition(&self) -> Option<usize> {
        self.metric.partition()
    }

    fn labels(&self) -> HashMap<String, String> {
        self.metric
            .labels()
            .iter()
            .map(|l| (l.name().to_string(), l.value().to_string()))
            .collect()
    }

    fn __repr__(&self) -> String {
        format!("{}", self.metric.value())
    }
}

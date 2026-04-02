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

use datafusion::physical_plan::metrics::{MetricValue, MetricsSet, Metric, Timestamp};
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

    fn timestamp_to_pyobject<'py>(
        py: Python<'py>,
        ts: &Timestamp,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        match ts.value() {
            Some(dt) => {
                let nanos = dt.timestamp_nanos_opt().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyOverflowError, _>("timestamp out of range")
                })?;
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
}

#[pymethods]
impl PyMetric {
    #[getter]
    fn name(&self) -> String {
        self.metric.value().name().to_string()
    }

    #[getter]
    fn value<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        match self.metric.value() {
            MetricValue::OutputRows(c) => Ok(Some(c.value().into_pyobject(py)?.into_any())),
            MetricValue::OutputBytes(c) => Ok(Some(c.value().into_pyobject(py)?.into_any())),
            MetricValue::ElapsedCompute(t) => Ok(Some(t.value().into_pyobject(py)?.into_any())),
            MetricValue::SpillCount(c) => Ok(Some(c.value().into_pyobject(py)?.into_any())),
            MetricValue::SpilledBytes(c) => Ok(Some(c.value().into_pyobject(py)?.into_any())),
            MetricValue::SpilledRows(c) => Ok(Some(c.value().into_pyobject(py)?.into_any())),
            MetricValue::CurrentMemoryUsage(g) => Ok(Some(g.value().into_pyobject(py)?.into_any())),
            MetricValue::Count { count, .. } => {
                Ok(Some(count.value().into_pyobject(py)?.into_any()))
            }
            MetricValue::Gauge { gauge, .. } => {
                Ok(Some(gauge.value().into_pyobject(py)?.into_any()))
            }
            MetricValue::Time { time, .. } => Ok(Some(time.value().into_pyobject(py)?.into_any())),
            MetricValue::StartTimestamp(ts) | MetricValue::EndTimestamp(ts) => {
                Self::timestamp_to_pyobject(py, ts)
            }
            _ => Ok(None),
        }
    }

    fn value_as_datetime<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        match self.metric.value() {
            MetricValue::StartTimestamp(ts) | MetricValue::EndTimestamp(ts) => {
                Self::timestamp_to_pyobject(py, ts)
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

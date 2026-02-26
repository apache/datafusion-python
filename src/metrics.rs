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
    /// Returns all individual metrics in this set.
    fn metrics(&self) -> Vec<PyMetric> {
        self.metrics
            .iter()
            .map(|m| PyMetric::new(Arc::clone(m)))
            .collect()
    }

    /// Returns the sum of all `output_rows` metrics, or None if not present.
    fn output_rows(&self) -> Option<usize> {
        self.metrics.output_rows()
    }

    /// Returns the sum of all `elapsed_compute` metrics in nanoseconds, or None if not present.
    fn elapsed_compute(&self) -> Option<usize> {
        self.metrics.elapsed_compute()
    }

    /// Returns the sum of all `spill_count` metrics, or None if not present.
    fn spill_count(&self) -> Option<usize> {
        self.metrics.spill_count()
    }

    /// Returns the sum of all `spilled_bytes` metrics, or None if not present.
    fn spilled_bytes(&self) -> Option<usize> {
        self.metrics.spilled_bytes()
    }

    /// Returns the sum of all `spilled_rows` metrics, or None if not present.
    fn spilled_rows(&self) -> Option<usize> {
        self.metrics.spilled_rows()
    }

    /// Returns the sum of metrics matching the given name.
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
    /// Returns the name of this metric.
    #[getter]
    fn name(&self) -> String {
        self.metric.value().name().to_string()
    }

    /// Returns the numeric value of this metric, or None for non-numeric types.
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

    /// Returns the partition this metric is for, or None if it applies to all partitions.
    #[getter]
    fn partition(&self) -> Option<usize> {
        self.metric.partition()
    }

    /// Returns the labels associated with this metric as a dict.
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

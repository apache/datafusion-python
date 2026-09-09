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

//! The functions this library contributes.
//!
//! Deliberately arithmetic on TPC-H columns rather than anything clever: the
//! interesting part of this crate is that a function has to be *reachable* in
//! whichever process ends up evaluating it, not what the function computes.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::function::{
    AccumulatorArgs, PartitionEvaluatorArgs, StateFieldsArgs, WindowUDFFieldArgs,
};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, PartitionEvaluator,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility, WindowUDF, WindowUDFImpl,
};
use datafusion_functions_window::rank::rank_udwf;

/// Name of the scalar function, used by the codec as the whole encoding.
pub(crate) const NET_REVENUE: &str = "dfx_net_revenue";
/// Name of the aggregate function.
pub(crate) const WEIGHTED_AVG: &str = "dfx_weighted_avg";
/// Name of the window function.
pub(crate) const REVENUE_RANK: &str = "dfx_revenue_rank";

/// `extendedprice * (1 - discount) * (1 + tax)`, the TPC-H revenue expression.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct NetRevenue {
    signature: Signature,
}

impl Default for NetRevenue {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for NetRevenue {
    fn name(&self) -> &str {
        NET_REVENUE
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let [price, discount, tax] = arrays.as_slice() else {
            return exec_err!("{NET_REVENUE} takes 3 arguments, got {}", arrays.len());
        };
        let price = price.as_primitive::<Float64Type>();
        let discount = discount.as_primitive::<Float64Type>();
        let tax = tax.as_primitive::<Float64Type>();

        let values: Float64Array = (0..price.len())
            .map(|row| {
                if price.is_null(row) || discount.is_null(row) || tax.is_null(row) {
                    return None;
                }
                Some(price.value(row) * (1.0 - discount.value(row)) * (1.0 + tax.value(row)))
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(values)))
    }
}

/// `sum(value * weight) / sum(weight)`.
///
/// Written out rather than delegating to a built-in because the state is the
/// point: two running sums, which is what lets DataFusion compute this in a
/// partial aggregate on each worker and merge the results on the driver. An
/// aggregate that could only be evaluated over the whole input at once would
/// not survive being split across processes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct WeightedAvg {
    signature: Signature,
}

impl Default for WeightedAvg {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for WeightedAvg {
    fn name(&self) -> &str {
        WEIGHTED_AVG
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(WeightedAvgAccumulator::default()))
    }

    /// The two partial sums, in the order [`WeightedAvgAccumulator::state`]
    /// returns them.
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format!("{}[weighted_sum]", args.name),
                DataType::Float64,
                false,
            )),
            Arc::new(Field::new(
                format!("{}[weight_sum]", args.name),
                DataType::Float64,
                false,
            )),
        ])
    }
}

#[derive(Debug, Default)]
struct WeightedAvgAccumulator {
    weighted_sum: f64,
    weight_sum: f64,
}

impl Accumulator for WeightedAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [value, weight] = values else {
            return exec_err!("{WEIGHTED_AVG} takes 2 arguments, got {}", values.len());
        };
        let value = value.as_primitive::<Float64Type>();
        let weight = weight.as_primitive::<Float64Type>();
        for row in 0..value.len() {
            // A null in either argument contributes to neither sum, so the
            // result is the weighted average of the rows that had both.
            if value.is_null(row) || weight.is_null(row) {
                continue;
            }
            self.weighted_sum += value.value(row) * weight.value(row);
            self.weight_sum += weight.value(row);
        }
        Ok(())
    }

    /// Merge partial states, which is the step that runs on the driver over
    /// results computed on the workers.
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [weighted_sum, weight_sum] = states else {
            return exec_err!("{WEIGHTED_AVG} has 2 state columns, got {}", states.len());
        };
        let weighted_sum = weighted_sum.as_primitive::<Float64Type>();
        let weight_sum = weight_sum.as_primitive::<Float64Type>();
        for row in 0..weighted_sum.len() {
            self.weighted_sum += weighted_sum.value(row);
            self.weight_sum += weight_sum.value(row);
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Float64(Some(self.weighted_sum)),
            ScalarValue::Float64(Some(self.weight_sum)),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // No rows, or every weight zero: null rather than a division by zero,
        // matching what `avg` does for an empty input.
        if self.weight_sum == 0.0 {
            return Ok(ScalarValue::Float64(None));
        }
        Ok(ScalarValue::Float64(Some(
            self.weighted_sum / self.weight_sum,
        )))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

/// Ranks rows within a window, under a name this library owns.
///
/// Delegates to the built-in `rank`: the reason it is here is to give the
/// library a window function whose *name* has to resolve on whichever process
/// evaluates it, which is the same portability question the other two raise.
#[derive(Debug, Clone)]
pub(crate) struct RevenueRank {
    inner: Arc<WindowUDF>,
}

impl Default for RevenueRank {
    fn default() -> Self {
        Self { inner: rank_udwf() }
    }
}

impl PartialEq for RevenueRank {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl Eq for RevenueRank {}

impl std::hash::Hash for RevenueRank {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl WindowUDFImpl for RevenueRank {
    fn name(&self) -> &str {
        REVENUE_RANK
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn partition_evaluator(
        &self,
        args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        self.inner.inner().partition_evaluator(args)
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        self.inner.inner().field(field_args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }
}

/// Rebuild one of this library's functions from its name alone.
///
/// This is the whole decode path: the names are the encoding, so a process
/// that has this library's codec installed can reconstruct any of them
/// without the driver having sent bytes and without the function having been
/// registered locally.
pub(crate) fn scalar_by_name(name: &str) -> Option<Arc<ScalarUDF>> {
    (name == NET_REVENUE).then(|| Arc::new(ScalarUDF::from(NetRevenue::default())))
}

pub(crate) fn aggregate_by_name(name: &str) -> Option<Arc<AggregateUDF>> {
    (name == WEIGHTED_AVG).then(|| Arc::new(AggregateUDF::from(WeightedAvg::default())))
}

pub(crate) fn window_by_name(name: &str) -> Option<Arc<WindowUDF>> {
    (name == REVENUE_RANK).then(|| Arc::new(WindowUDF::from(RevenueRank::default())))
}

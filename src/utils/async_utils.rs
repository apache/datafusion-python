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

use std::future::Future;

use pyo3::Python;

use crate::errors::PyDataFusionError;
use crate::utils::wait_for_future;

/// Helper function to execute an async function and wait for the result.
///
/// This function takes a Python GIL token, an async block as a future,
/// and a function to convert the error type. It executes the future and
/// returns the unwrapped result.
///
/// # Arguments
///
/// * `py` - Python GIL token
/// * `future` - Future to execute
/// * `error_mapper` - Function to convert error type
///
/// # Returns
///
/// Result with the unwrapped value or converted error
pub fn call_async<F, T, E, M>(
    py: Python,
    future: F,
    error_mapper: M,
) -> Result<T, PyDataFusionError>
where
    F: Future<Output = Result<T, E>> + Send + 'static,
    T: Send + 'static,
    E: std::error::Error + Send + 'static,
    M: Fn(E) -> PyDataFusionError + Send + 'static,
{
    wait_for_future(py, future)?.map_err(error_mapper)
}

/// Helper function to execute an async function that returns a Result with DataFusionError
/// and wait for the result.
///
/// This is a specialized version of call_async that maps DataFusionError to PyDataFusionError
/// automatically.
///
/// # Arguments
///
/// * `py` - Python GIL token
/// * `future` - Future to execute
///
/// # Returns
///
/// Result with the unwrapped value or PyDataFusionError
pub fn call_datafusion_async<F, T>(py: Python, future: F) -> Result<T, PyDataFusionError>
where
    F: Future<Output = Result<T, datafusion::error::DataFusionError>> + Send + 'static,
    T: Send + 'static,
{
    call_async(py, future, PyDataFusionError::from)
}

/// Helper function to execute a DataFusion async function and apply ? twice to unwrap the result.
///
/// This helper is useful for functions that need to apply ?? to the result of wait_for_future.
///
/// # Arguments
///
/// * `py` - Python GIL token
/// * `future` - Future to execute
///
/// # Returns
///
/// Result with the unwrapped value or PyDataFusionError
pub fn call_datafusion_async_double_question<F, T>(
    py: Python,
    future: F,
) -> Result<T, PyDataFusionError>
where
    F: Future<
            Output = Result<
                Result<T, datafusion::error::DataFusionError>,
                datafusion::error::DataFusionError,
            >,
        > + Send
        + 'static,
    T: Send + 'static,
{
    let result = wait_for_future(py, future)??;
    Ok(result)
}

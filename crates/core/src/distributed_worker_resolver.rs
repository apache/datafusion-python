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

use datafusion::common::DataFusionError;
use datafusion_distributed::WorkerResolver;
use pyo3::Borrowed;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyString;
use url::Url;

pub(crate) struct PyWorkerResolver {
    get_urls: Py<PyAny>,
}

impl FromPyObject<'_, '_> for PyWorkerResolver {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, '_, PyAny>) -> Result<Self, Self::Error> {
        let get_urls = obj.getattr("get_urls")?;
        if !get_urls.is_callable() {
            return Err(PyTypeError::new_err(
                "Expected worker_resolver.get_urls to be callable",
            ));
        }

        Ok(Self {
            get_urls: get_urls.unbind(),
        })
    }
}

struct WorkerUrls(Vec<Url>);

impl FromPyObject<'_, '_> for WorkerUrls {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, '_, PyAny>) -> Result<Self, Self::Error> {
        if obj.is_instance_of::<PyString>() {
            return Err(PyTypeError::new_err(
                "WorkerResolver.get_urls() must return an iterable of URL strings, not a string",
            ));
        }

        let mut parsed_urls = Vec::new();
        for url in obj.try_iter()? {
            let url = url?;
            let url = url.extract::<String>()?;
            let parsed_url = Url::parse(&url).map_err(|error| {
                PyValueError::new_err(format!(
                    "WorkerResolver.get_urls() returned invalid URL {url:?}: {error}"
                ))
            })?;
            parsed_urls.push(parsed_url);
        }

        Ok(Self(parsed_urls))
    }
}

impl WorkerResolver for PyWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Python::attach(|py| -> PyResult<Vec<Url>> {
            let urls = self.get_urls.call0(py)?;
            let urls = urls.extract::<WorkerUrls>(py)?;
            Ok(urls.0)
        })
        .map_err(|error| DataFusionError::External(Box::new(error)))
    }
}

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

use std::sync::Arc;

use pyo3::prelude::*;

use object_store::aws::{AmazonS3, AmazonS3Builder};

#[pyclass(name = "AmazonS3", module = "datafusion.store", subclass, unsendable)]
#[derive(Debug)]
pub(crate) struct PyAmazonS3Context {
    pub store: Arc<AmazonS3>,
    pub bucket_name: String,
}

#[pymethods]
impl PyAmazonS3Context {
    #[allow(clippy::too_many_arguments)]
    #[args(
        region = "None",
        access_key_id = "None",
        secret_access_key = "None",
        endpoint = "None",
        imdsv1_fallback = "false",
        allow_http = "false"
    )]
    #[new]
    fn new(
        bucket_name: String,
        region: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        endpoint: Option<String>,
        //retry_config: RetryConfig,
        allow_http: bool,
        imdsv1_fallback: bool,
    ) -> Self {
        // start w/ the options that come directly from the environment
        let mut builder = AmazonS3Builder::from_env();

        if let Some(region) = region {
            builder = builder.with_region(region);
        }

        if let Some(access_key_id) = access_key_id {
            builder = builder.with_access_key_id(access_key_id);
        };

        if let Some(secret_access_key) = secret_access_key {
            builder = builder.with_secret_access_key(secret_access_key);
        };

        if let Some(endpoint) = endpoint {
            builder = builder.with_endpoint(endpoint);
        };

        if imdsv1_fallback {
            builder = builder.with_imdsv1_fallback();
        };

        let store = builder
            .with_bucket_name(bucket_name.clone())
            //.with_retry_config(retry_config) #TODO: add later
            .with_allow_http(allow_http)
            .build()
            .expect("failed to build AmazonS3");

        Self {
            store: Arc::new(store),
            bucket_name,
        }
    }
}


pub(crate) fn init_module(m: &PyModule) -> PyResult<()> {
    m.add_class::<PyAmazonS3Context>()?;
    Ok(())
}
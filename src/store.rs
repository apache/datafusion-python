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
use object_store::azure::{MicrosoftAzure, MicrosoftAzureBuilder};
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};
use object_store::http::{HttpBuilder, HttpStore};
use object_store::local::LocalFileSystem;
use pyo3::exceptions::PyValueError;
use url::Url;

#[derive(FromPyObject)]
pub enum StorageContexts {
    AmazonS3(PyAmazonS3Context),
    GoogleCloudStorage(PyGoogleCloudContext),
    MicrosoftAzure(PyMicrosoftAzureContext),
    LocalFileSystem(PyLocalFileSystemContext),
    HTTP(PyHttpContext),
}

#[pyclass(name = "LocalFileSystem", module = "datafusion.store", subclass)]
#[derive(Debug, Clone)]
pub struct PyLocalFileSystemContext {
    pub inner: Arc<LocalFileSystem>,
}

#[pymethods]
impl PyLocalFileSystemContext {
    #[pyo3(signature = (prefix=None))]
    #[new]
    fn new(prefix: Option<String>) -> Self {
        if let Some(prefix) = prefix {
            Self {
                inner: Arc::new(
                    LocalFileSystem::new_with_prefix(prefix)
                        .expect("Could not create local LocalFileSystem"),
                ),
            }
        } else {
            Self {
                inner: Arc::new(LocalFileSystem::new()),
            }
        }
    }
}

#[pyclass(name = "MicrosoftAzure", module = "datafusion.store", subclass)]
#[derive(Debug, Clone)]
pub struct PyMicrosoftAzureContext {
    pub inner: Arc<MicrosoftAzure>,
    pub container_name: String,
}

#[pymethods]
impl PyMicrosoftAzureContext {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (container_name, account=None, access_key=None, bearer_token=None, client_id=None, client_secret=None, tenant_id=None, sas_query_pairs=None, use_emulator=None, allow_http=None))]
    #[new]
    fn new(
        container_name: String,
        account: Option<String>,
        access_key: Option<String>,
        bearer_token: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
        tenant_id: Option<String>,
        sas_query_pairs: Option<Vec<(String, String)>>,
        use_emulator: Option<bool>,
        allow_http: Option<bool>,
    ) -> Self {
        let mut builder = MicrosoftAzureBuilder::from_env().with_container_name(&container_name);

        if let Some(account) = account {
            builder = builder.with_account(account);
        }

        if let Some(access_key) = access_key {
            builder = builder.with_access_key(access_key);
        }

        if let Some(bearer_token) = bearer_token {
            builder = builder.with_bearer_token_authorization(bearer_token);
        }

        match (client_id, client_secret, tenant_id) {
            (Some(client_id), Some(client_secret), Some(tenant_id)) => {
                builder =
                    builder.with_client_secret_authorization(client_id, client_secret, tenant_id);
            }
            (None, None, None) => {}
            _ => {
                panic!("client_id, client_secret, tenat_id must be all set or all None");
            }
        }

        if let Some(sas_query_pairs) = sas_query_pairs {
            builder = builder.with_sas_authorization(sas_query_pairs);
        }

        if let Some(use_emulator) = use_emulator {
            builder = builder.with_use_emulator(use_emulator);
        }

        if let Some(allow_http) = allow_http {
            builder = builder.with_allow_http(allow_http);
        }

        Self {
            inner: Arc::new(
                builder
                    .build()
                    .expect("Could not create Azure Storage context"), //TODO: change these to PyErr
            ),
            container_name,
        }
    }
}

#[pyclass(name = "GoogleCloud", module = "datafusion.store", subclass)]
#[derive(Debug, Clone)]
pub struct PyGoogleCloudContext {
    pub inner: Arc<GoogleCloudStorage>,
    pub bucket_name: String,
}

#[pymethods]
impl PyGoogleCloudContext {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (bucket_name, service_account_path=None))]
    #[new]
    fn new(bucket_name: String, service_account_path: Option<String>) -> Self {
        let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(&bucket_name);

        if let Some(credential_path) = service_account_path {
            builder = builder.with_service_account_path(credential_path);
        }

        Self {
            inner: Arc::new(
                builder
                    .build()
                    .expect("Could not create Google Cloud Storage"),
            ),
            bucket_name,
        }
    }
}

#[pyclass(name = "AmazonS3", module = "datafusion.store", subclass)]
#[derive(Debug, Clone)]
pub struct PyAmazonS3Context {
    pub inner: Arc<AmazonS3>,
    pub bucket_name: String,
}

#[pymethods]
impl PyAmazonS3Context {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (bucket_name, region=None, access_key_id=None, secret_access_key=None, endpoint=None, allow_http=false, imdsv1_fallback=false))]
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
            inner: Arc::new(store),
            bucket_name,
        }
    }
}

#[pyclass(name = "Http", module = "datafusion.store", subclass)]
#[derive(Debug, Clone)]
pub struct PyHttpContext {
    pub url: String,
    pub store: Arc<HttpStore>,
}

#[pymethods]
impl PyHttpContext {
    #[new]
    fn new(url: String) -> PyResult<Self> {
        let store = match Url::parse(url.as_str()) {
            Ok(url) => HttpBuilder::new()
                .with_url(url.origin().ascii_serialization())
                .build(),
            Err(_) => HttpBuilder::new().build(),
        }
        .map_err(|e| PyValueError::new_err(format!("Error: {:?}", e.to_string())))?;

        Ok(Self {
            url,
            store: Arc::new(store),
        })
    }
}

pub(crate) fn init_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyAmazonS3Context>()?;
    m.add_class::<PyMicrosoftAzureContext>()?;
    m.add_class::<PyGoogleCloudContext>()?;
    m.add_class::<PyLocalFileSystemContext>()?;
    m.add_class::<PyHttpContext>()?;
    Ok(())
}

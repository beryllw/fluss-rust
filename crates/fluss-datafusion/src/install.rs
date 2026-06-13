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

use datafusion::execution::context::SessionContext;
use fluss::client::FlussConnection;

use crate::config::{FlussDatafusionOptions, RegisterCatalogOptions};
use crate::error::{FlussDatafusionError, Result};

pub struct FlussDatafusion {
    inner: Arc<Inner>,
}

struct Inner {
    connection: Arc<FlussConnection>,
    options: FlussDatafusionOptions,
}

impl FlussDatafusion {
    pub async fn new(
        connection: Arc<FlussConnection>,
        options: FlussDatafusionOptions,
    ) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(Inner {
                connection,
                options,
            }),
        })
    }

    pub async fn register_catalog(
        &self,
        ctx: &SessionContext,
        catalog_name: &str,
        options: RegisterCatalogOptions,
    ) -> Result<()> {
        let _ = (
            &self.inner.connection,
            &self.inner.options,
            ctx,
            catalog_name,
            options,
        );
        Err(FlussDatafusionError::Internal(
            "register_catalog is not implemented yet",
        ))
    }
}

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

use crate::backend::SharedFlussSource;
use crate::backend::real::RealFlussSource;
use crate::config::{FlussDatafusionOptions, RegisterCatalogOptions};
use crate::error::{FlussDatafusionError, Result};

/// Shared, stateless installer for Fluss DataFusion integration.
///
/// Built once around a connection and shared metadata/cache state; a SQL session
/// then installs Fluss catalog support into its own `SessionContext` via
/// [`FlussDatafusion::register_catalog`].
pub struct FlussDatafusion {
    inner: Arc<Inner>,
}

struct Inner {
    /// The single internal access seam. `new()` wraps a real connection; tests
    /// inject a fake. The rest of the crate depends only on this trait object.
    source: SharedFlussSource,
    options: FlussDatafusionOptions,
}

impl FlussDatafusion {
    /// Builds an installer around a live Fluss connection.
    ///
    /// The connection is wrapped into an internal `Arc<dyn FlussSource>`; the
    /// crate never depends on `FlussConnection` outside this boundary.
    pub async fn new(
        connection: Arc<FlussConnection>,
        options: FlussDatafusionOptions,
    ) -> Result<Self> {
        let source: SharedFlussSource = Arc::new(RealFlussSource::new(connection));
        Ok(Self {
            inner: Arc::new(Inner { source, options }),
        })
    }

    /// Test-only constructor that injects an arbitrary [`FlussSource`].
    ///
    /// Available only under the `test-fake` feature so a fixture-backed fake can
    /// drive the full catalog/execution path with no cluster. Not part of the
    /// public API.
    #[cfg(feature = "test-fake")]
    pub fn new_with_source(source: SharedFlussSource, options: FlussDatafusionOptions) -> Self {
        Self {
            inner: Arc::new(Inner { source, options }),
        }
    }

    /// Internal accessor for the shared source, used by catalog/execution wiring
    /// in later tasks.
    #[allow(dead_code)]
    pub(crate) fn source(&self) -> &SharedFlussSource {
        &self.inner.source
    }

    /// Internal accessor for installer options.
    #[allow(dead_code)]
    pub(crate) fn options(&self) -> &FlussDatafusionOptions {
        &self.inner.options
    }

    /// Registers the Fluss catalog tree into a session context.
    ///
    /// Not implemented yet: catalog/schema/table wiring lands in Task 3+.
    pub async fn register_catalog(
        &self,
        ctx: &SessionContext,
        catalog_name: &str,
        options: RegisterCatalogOptions,
    ) -> Result<()> {
        let _ = (ctx, catalog_name, options);
        Err(FlussDatafusionError::internal(
            "register_catalog is not implemented yet",
        ))
    }
}

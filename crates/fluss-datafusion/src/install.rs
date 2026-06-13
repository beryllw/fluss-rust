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
use crate::catalog::build_catalog_provider;
use crate::config::{FlussDatafusionOptions, RegisterCatalogOptions};
use crate::error::Result;
use crate::metadata::{MetadataCache, MetadataLoader};

/// Shared, stateless installer for Fluss DataFusion integration.
///
/// Built once around a connection and shared metadata/cache state; a SQL session
/// then installs Fluss catalog support into its own `SessionContext` via
/// [`FlussDatafusion::register_catalog`].
pub struct FlussDatafusion {
    inner: Arc<Inner>,
}

struct Inner {
    options: FlussDatafusionOptions,
    /// Shared, session-agnostic metadata loader (fronts the source with a cache).
    /// One instance per installer, reused across every `SessionContext`. The loader
    /// owns the single `FlussSource` handle; the rest of the crate reaches Fluss
    /// only through it.
    loader: Arc<MetadataLoader>,
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
        Ok(Self::from_source(source, options))
    }

    /// Test-only constructor that injects an arbitrary [`FlussSource`].
    ///
    /// Available only under the `test-fake` feature so a fixture-backed fake can
    /// drive the full catalog/execution path with no cluster. Not part of the
    /// public API.
    #[cfg(feature = "test-fake")]
    pub fn new_with_source(source: SharedFlussSource, options: FlussDatafusionOptions) -> Self {
        Self::from_source(source, options)
    }

    fn from_source(source: SharedFlussSource, options: FlussDatafusionOptions) -> Self {
        let cache = Arc::new(MetadataCache::new(options.metadata_cache_ttl));
        let loader = Arc::new(MetadataLoader::new(source, cache));
        Self {
            inner: Arc::new(Inner { options, loader }),
        }
    }

    /// Internal accessor for installer options.
    #[allow(dead_code)]
    pub(crate) fn options(&self) -> &FlussDatafusionOptions {
        &self.inner.options
    }

    /// Registers the Fluss catalog tree into a session context.
    ///
    /// Builds a `CatalogProvider` backed by the shared metadata cache and installs
    /// it via `ctx.register_catalog`. Repeated calls (including on a fresh context)
    /// reuse the cached database/table listing and do not re-hit the source.
    pub async fn register_catalog(
        &self,
        ctx: &SessionContext,
        catalog_name: &str,
        _options: RegisterCatalogOptions,
    ) -> Result<()> {
        let provider = build_catalog_provider(self.inner.loader.clone()).await?;
        ctx.register_catalog(catalog_name, provider);
        Ok(())
    }
}

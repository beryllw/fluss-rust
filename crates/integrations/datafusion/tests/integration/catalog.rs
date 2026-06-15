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

//! Cluster-free catalog-registration tests: drive `register_catalog` through the
//! fixture-backed fake. Proves the metadata cache + DataFusion catalog tree wiring
//! (database -> schema, table listing, Arrow schema exposure, shared-cache reuse).
//!
//! Available only under `test-fake`. Opens zero sockets.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::DataType;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;

use fluss_datafusion::testing::{FlussSource, FlussTableMeta, LookupKey, TableRef};
use fluss_datafusion::{FlussDatafusion, FlussDatafusionOptions, RegisterCatalogOptions};

use crate::integration::utils::helpers::{CATALOG, options};
use crate::integration::utils::{fake_source, fixtures_ready, names};

/// Counts `list_databases` / `list_tables` calls so tests can prove the shared
/// cache actually prevents re-fetching from the source on a second
/// `register_catalog`.
struct CountingSource {
    inner: Arc<dyn FlussSource>,
    list_calls: Arc<AtomicUsize>,
}

impl CountingSource {
    fn wrap(inner: Arc<dyn FlussSource>) -> (Arc<dyn FlussSource>, Arc<AtomicUsize>) {
        let list_calls = Arc::new(AtomicUsize::new(0));
        let src: Arc<dyn FlussSource> = Arc::new(Self {
            inner,
            list_calls: list_calls.clone(),
        });
        (src, list_calls)
    }
}

#[async_trait::async_trait]
impl FlussSource for CountingSource {
    async fn list_databases(&self) -> fluss_datafusion::Result<Vec<String>> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.list_databases().await
    }

    async fn list_tables(&self, database: &str) -> fluss_datafusion::Result<Vec<String>> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.list_tables(database).await
    }

    async fn get_table_meta(&self, table: &TableRef) -> fluss_datafusion::Result<FlussTableMeta> {
        self.inner.get_table_meta(table).await
    }

    async fn lookup(
        &self,
        table: &TableRef,
        key: &LookupKey,
    ) -> fluss_datafusion::Result<RecordBatch> {
        self.inner.lookup(table, key).await
    }

    async fn log_scan(
        &self,
        table: &TableRef,
        projection: Option<&[usize]>,
        limit: usize,
    ) -> fluss_datafusion::Result<Vec<RecordBatch>> {
        self.inner.log_scan(table, projection, limit).await
    }
}

#[tokio::test]
async fn register_catalog_exposes_databases_and_tables() {
    if !fixtures_ready() {
        return;
    }
    let fd = FlussDatafusion::new_with_source(fake_source(), options());
    // information_schema is opt-in; enable it so the SQL listing assertion works.
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_information_schema(true),
    );
    fd.register_catalog(&ctx, CATALOG, RegisterCatalogOptions::default())
        .await
        .expect("register_catalog");

    let catalog = ctx.catalog(CATALOG).expect("catalog registered");
    let schemas = catalog.schema_names();
    assert!(
        schemas.iter().any(|s| s == names::DATABASE),
        "expected schema {} in {schemas:?}",
        names::DATABASE
    );

    let schema = catalog.schema(names::DATABASE).expect("schema present");
    let tables = schema.table_names();
    for expected in [names::KV_SIMPLE, names::KV_COMPOSITE, names::LOG_BASIC] {
        assert!(
            tables.iter().any(|t| t == expected),
            "expected table {expected} in {tables:?}"
        );
    }

    // Also via SQL through information_schema so we exercise the async `table()` path.
    let df = ctx
        .sql(&format!(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_catalog = '{CATALOG}' AND table_schema = '{}' \
             ORDER BY table_name",
            names::DATABASE
        ))
        .await
        .expect("sql plan");
    let batches = df.collect().await.expect("collect");
    let mut found: Vec<String> = Vec::new();
    for b in &batches {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("table_name string column");
        for i in 0..col.len() {
            found.push(col.value(i).to_string());
        }
    }
    for expected in [names::KV_SIMPLE, names::KV_COMPOSITE, names::LOG_BASIC] {
        assert!(
            found.iter().any(|t| t == expected),
            "information_schema missing {expected}: {found:?}"
        );
    }
}

#[tokio::test]
async fn shared_metadata_is_reused_across_contexts() {
    if !fixtures_ready() {
        return;
    }
    let (source, list_calls) = CountingSource::wrap(fake_source());
    let fd = FlussDatafusion::new_with_source(source, options());

    // First context: dirty (already has some objects registered / a query run).
    let ctx_dirty = SessionContext::new();
    ctx_dirty
        .sql("SELECT 1")
        .await
        .expect("warmup")
        .collect()
        .await
        .expect("warmup collect");
    fd.register_catalog(&ctx_dirty, CATALOG, RegisterCatalogOptions::default())
        .await
        .expect("register into dirty ctx");
    let after_first = list_calls.load(Ordering::SeqCst);
    assert!(
        after_first > 0,
        "first register_catalog must hit the source for listings"
    );

    // Second context: fresh. Must reuse the shared cache, not re-fetch listings.
    let ctx_fresh = SessionContext::new();
    fd.register_catalog(&ctx_fresh, CATALOG, RegisterCatalogOptions::default())
        .await
        .expect("register into fresh ctx");
    let after_second = list_calls.load(Ordering::SeqCst);
    assert_eq!(
        after_first, after_second,
        "second register_catalog must reuse shared cache (no extra list_* calls)"
    );

    // Both contexts independently see the tables.
    for ctx in [&ctx_dirty, &ctx_fresh] {
        let schema = ctx
            .catalog(CATALOG)
            .expect("catalog")
            .schema(names::DATABASE)
            .expect("schema");
        let tables = schema.table_names();
        for expected in [names::KV_SIMPLE, names::KV_COMPOSITE, names::LOG_BASIC] {
            assert!(
                tables.iter().any(|t| t == expected),
                "ctx missing {expected}: {tables:?}"
            );
        }
    }
}

#[tokio::test]
async fn expired_metadata_is_refetched() {
    if !fixtures_ready() {
        return;
    }
    let (source, list_calls) = CountingSource::wrap(fake_source());
    // Zero TTL: every cached listing is stale the instant it is stored, so the
    // second register_catalog must re-hit the source rather than reuse the cache.
    let fd = FlussDatafusion::new_with_source(
        source,
        FlussDatafusionOptions {
            metadata_cache_ttl: Duration::ZERO,
        },
    );

    let ctx_a = SessionContext::new();
    fd.register_catalog(&ctx_a, CATALOG, RegisterCatalogOptions::default())
        .await
        .expect("register a");
    let after_first = list_calls.load(Ordering::SeqCst);
    assert!(after_first > 0, "first register must hit the source");

    let ctx_b = SessionContext::new();
    fd.register_catalog(&ctx_b, CATALOG, RegisterCatalogOptions::default())
        .await
        .expect("register b");
    let after_second = list_calls.load(Ordering::SeqCst);
    assert!(
        after_second > after_first,
        "expired cache must re-fetch listings (first={after_first}, second={after_second})"
    );
}

#[tokio::test]
async fn table_provider_exposes_arrow_schema() {
    if !fixtures_ready() {
        return;
    }
    let fd = FlussDatafusion::new_with_source(fake_source(), options());
    let ctx = SessionContext::new();
    fd.register_catalog(&ctx, CATALOG, RegisterCatalogOptions::default())
        .await
        .expect("register_catalog");

    let schema = ctx
        .catalog(CATALOG)
        .expect("catalog")
        .schema(names::DATABASE)
        .expect("schema");
    let table = schema
        .table(names::KV_SIMPLE)
        .await
        .expect("table lookup ok")
        .expect("kv_simple present");

    let arrow_schema = table.schema();
    let field_names: Vec<&str> = arrow_schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(
        field_names.contains(&"id"),
        "expected id column, got {field_names:?}"
    );
    let id_field = arrow_schema.field_with_name("id").expect("id field");
    assert_eq!(
        id_field.data_type(),
        &DataType::Int32,
        "id should map to Int32"
    );
}

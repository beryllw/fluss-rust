# fluss-datafusion Phase 1 任务状态(临时)

> 临时跟踪文档。Phase 1 全部任务完成后删除本文件。
> 任务定义的 source of truth:`docs/fluss-datafusion-phase1-design.md`。

| 任务 | 状态 |
|---|---|
| Task 1: workspace 与 crate 骨架 | 已完成 |
| Task 2: `FlussSource` seam + fake 测试 harness | 已完成 |
| Task 3: metadata cache 与注册路径 | 已完成 |
| Task 4: KV 谓词分析与 lookup 执行 | 未开始 |
| Task 5: log 有界扫描执行 | 未开始 |
| Task 6: Docker 端到端集成测试 | 未开始 |

## Task 1: workspace 与 crate 骨架

- 状态:已完成
- 新增 `crates/fluss-datafusion` 作为 workspace member,并提供可编译的公共 API 骨架(`FlussDatafusion`、`FlussDatafusionOptions`、`RegisterCatalogOptions`、`FlussDatafusionError`)。
- 固定 `datafusion = 51.0.0`(与 workspace 的 `arrow 57` 对齐),并将 workspace 的 `rust-version` 提升到 `1.88` 以满足 DataFusion 的 MSRV。
- `register_catalog()` 刻意为失败桩(failing stub),直到 Task 3 接入真实的 catalog 注册。
- 已通过 `cargo check -p fluss-datafusion` 与 `cargo check --workspace` 验证。Task 1 未新增测试;测试 harness 与实质行为从 Task 2 起引入。

## Task 2: `FlussSource` seam + fake 测试 harness

- 状态:已完成
- 新增内部访问 seam `FlussSource`(`src/backend/mod.rs`),覆盖 Phase 1 用到的原子操作:metadata 列举/获取、KV 完整 primary-key `lookup -> RecordBatch`、log 有界 scan `-> Vec<RecordBatch>`;以 `Arc<dyn FlussSource>` 形式被 crate 内部依赖。
- `real.rs`:生产实现,包 `FlussConnection` / `FlussAdmin` / lookuper / scanner,复用 fluss 现有 Arrow helper(`LookupResult::to_record_batch` 等),不重复实现 Arrow 组装。
- `fake.rs`(feature `test-fake`):纯内存 replay,从 `tests/fixtures/phase1.json` 读取,不开任何 socket。
- `fixtures.rs`:fixture 序列化格式(metadata 用 serde,batch 用 Arrow IPC)。
- `capture.rs`(feature `integration_tests`):连真实集群(`fluss-test-cluster` / podman)建表写数据,record 响应写入 `tests/fixtures/`;已实际跑通并生成 `phase1.json`(真实集群快照,含 `df_kv_simple` / `df_kv_composite` / `df_log_basic`)。
- `TableInfo` 不支持 serde,故 seam 返回 crate 自有的精简 `FlussTableMeta`(schema + primary_keys + num_buckets 等),可经 fixtures round-trip。
- `new()` 内部构造 real source;`new_with_source(...)` 作为 `#[cfg(feature="test-fake")] pub` 测试注入入口,不进真实公共 API。
- 独立子 agent 验收通过:`cargo check --workspace` 干净;`cargo test -p fluss-datafusion --features test-fake` 8/8 通过且不依赖 Docker;tamper test(篡改 fixture 中捕获的 int32 值 `2 -> 7`)触发预期断言失败,证明 replay 测试非空洞,随后已精确还原(SHA-256 一致)。

## Task 3: metadata cache 与注册路径

- 状态:已完成
- 新增 `src/metadata/{mod,cache,loader}.rs`:`MetadataLoader` 仅依赖 `SharedFlussSource`(不直接持有 `FlussConnection`/`FlussAdmin`);`MetadataCache` 为 `RwLock` 保护的快照(db/table 列举 + 每表 `TableEntry{meta, arrow_schema}`),按条目 `Instant` 做 TTL;锁 guard 读取/克隆后即释放,绝不跨 `.await` 持有。
- 新增 `src/catalog/{mod,provider,schema,register}.rs`:`FlussCatalogProvider`(同步 `schema_names`/`schema`)把 Fluss database 映射为 schema;`FlussSchemaProvider` 在异步 `table()` 中懒加载每表 meta + Arrow schema;`build_catalog_provider` 组装并由 `register_catalog` 调 `ctx.register_catalog(...)` 生效。
- Arrow schema 经 `fluss::record::to_arrow_schema(meta.schema.row_type())` 转换,不重复实现 Fluss->Arrow 映射。
- 占位 `TableProvider`(`FlussTablePlaceholder`)只暴露正确的 Arrow `schema()`;`scan()` 返回 `UnsupportedQueryPattern`(按 `has_primary_key()` 区分 KV/log 文案),保守失败、绝不静默退化为 full scan;真实 scan 留待 Task 4/5。未新增 `src/table/*`、`src/execution/*`。
- `install.rs` 的 `Inner` 持有共享 `Arc<MetadataLoader>`(`new` 与 `new_with_source` 共用),使 cache 跨 `SessionContext` 复用;移除了过时的 `source()` accessor 与 dead `Inner.source` 字段(loader 持有唯一 source handle)。
- 独立子 agent 验收通过:`cargo check -p fluss-datafusion`(默认构建,零 warning)、`cargo check --workspace`、`cargo test -p fluss-datafusion --features test-fake` 11/11 通过(8 replay + 3 catalog)且不依赖 Docker。perturbation test:临时让 loader 列举路径跳过 cache 强制重取,`shared_metadata_is_reused_across_contexts` 如预期失败(second `register_catalog` 重打 source),随后精确还原,证明共享 cache 测试非空洞。

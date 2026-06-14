# fluss-datafusion Phase 1 任务状态(临时)

> 临时跟踪文档。Phase 1 全部任务完成后删除本文件。
> 任务定义的 source of truth:`docs/fluss-datafusion-phase1-design.md`。

| 任务 | 状态 |
|---|---|
| Task 1: workspace 与 crate 骨架 | 已完成 |
| Task 2: `FlussSource` seam + fake 测试 harness | 已完成 |
| Task 3: metadata cache 与注册路径 | 已完成 |
| Task 4: KV 谓词分析与 lookup 执行 | 已完成 |
| Task 5: log 有界扫描执行 | 已完成 |
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

## Task 4: KV 谓词分析与 lookup 执行

- 状态:已完成
- 新增 `src/types/{mod,scalar,record_batch}.rs`:`scalar_to_key_value` 严格把 `ScalarValue`(`Boolean`/`Int8/16/32/64`/`Utf8`)转 `KeyValue`,NULL 与其它类型显式返回 `TypeConversion`;`project_batch` 集中处理按 projection 裁列(`None` 透传),不重复 Arrow 组装。
- 新增 `src/table/{mod,predicate,kv}.rs`:`analyze_kv_filters` 把 DataFusion 拆开的顶层 AND conjuncts 识别为完整 primary-key 等值,按 PK 顺序产出 `LookupKey`;明确拒绝部分 PK、非 PK 列、`IN`、范围、列对列、重复/缺失 PK、空 filter,一律 `UnsupportedQueryPattern`。`FlussKvTableProvider` 的 `supports_filters_pushdown` 仅对 PK 等值 filter 标 `Exact`(被消费、不再生成 `FilterExec`),其余 `Unsupported`;`scan()` 用 predicate 分析,命中则构建 lookup plan(正确传 projection 到输出 schema),否则把 `UnsupportedQueryPattern` 直接上抛——绝不静默退化为 full scan。
- 新增 `src/execution/{mod,lookup,stream}.rs`:`FlussKvLookupExec` 为单分区 leaf `ExecutionPlan`,持有 `SharedFlussSource`/`TableRef`/`LookupKey`/projected schema,`execute()` 经 `single_batch_stream`(`futures::stream::once` + `RecordBatchStreamAdapter`,drop 即协作式取消)异步调 `source.lookup` 产出 0/1 行;实现 `DisplayAs`,`EXPLAIN` 显示可识别的 `FlussKvLookupExec`。
- 改 `src/catalog/schema.rs`:`table()` 按 `meta.has_primary_key()` 分流——KV 表注入 source+meta+arrow_schema+table_ref 返回真实 `FlussKvTableProvider`;log 表仍保持保守失败的占位(Task 5)。新增 `MetadataLoader::source()`(返回 clone)作为 KV provider 获取执行通道的最小扩展,execution 仅依赖 `FlussSource`,不触碰 `FlussConnection`/`Lookuper`。
- 关键决策:失败边界统一走 crate 本地 `UnsupportedQueryPattern`,经现有 `From<FlussDatafusionError> for DataFusionError` 在 plan 或 collect 阶段清晰冒泡;`supports_filters_pushdown` 与 `scan` 的判定共用同一 predicate 模块,避免 pushdown 标记与执行可行性漂移。
- 验证结果:`cargo check -p fluss-datafusion`(默认构建,零 warning)通过;`cargo check --workspace` 通过;`cargo test -p fluss-datafusion --features test-fake` 全绿——18 个单元测试(predicate 13 + scalar 3 + record_batch 2) + 19 个集成测试(11 原有 + 8 新增 KV)。新增 8 个 KV 集成测试:单/复合 PK 命中、缺失 key 0 行、非 PK / 部分 PK / 无 filter / `IN` 四类不支持谓词清晰失败、`EXPLAIN` 含 `FlussKvLookupExec`。
- 注:`cargo clippy` 在本机较新工具链(1.93.0)下因上游 `crates/fluss` 既有代码触发 `collapsible_if`/`type_complexity` 等 lint 而失败,与 Task 4 无关(未改动 `crates/fluss`);Task 4 新增文件本身零 clippy 发现。

## Task 5: log 有界扫描执行

- 状态:已完成
- 新增 `src/table/log.rs`:`FlussLogTableProvider` 把 log(append-only)表暴露为有界扫描。`scan()` 在 `limit` 为 `None` 时清晰返回 `LimitRequired`(经 `From<FlussDatafusionError>` 冒泡),绝不退化为 full scan;`supports_filters_pushdown` 一律 `Unsupported`(Phase 1 log 不做 filter pushdown,残余 filter 由上层 `FilterExec` 处理);projection 下推到 `FlussSource::log_scan`。
- 新增 `src/execution/log_scan.rs`:`FlussLogScanExec` 为单分区 leaf `ExecutionPlan`,`execute()` 经新增的 `bounded_batches_stream`(`src/execution/stream.rs`,Vec 版的协作式取消流)异步调 `source.log_scan(projection, limit)` 产出多个 batch;`EXPLAIN` 显示可识别的 `FlussLogScanExec`。
- 关键正确性(与 KV 的不对称):`log_scan` 在 real/fake 两侧都已按 projection 投影,故 log 执行路径**不再二次投影**(对比 KV 的 lookup 返回全列后再 `project_batch`);plan 声明的 `projected_schema` 与已投影 batch 一致。
- projection 归一化:DataFusion 对 `SELECT *` 传入 `Some([0,1,..])` 而非 `None`,`scan()` 用有序 identity 检查(`indices.iter().copied().eq(0..full_count)`)把完整 identity projection 收敛为 `None`,以命中"全列"语义;非 identity(如重排或真子集)保持原样。
- 改 `src/catalog/schema.rs`:log 分支由保守占位改为返回真实 `FlussLogTableProvider`;删除 `FlussTablePlaceholder` 及其 impl 与随之失效的 import。移除 `src/backend/mod.rs` 顶部的 `#![cfg_attr(not(feature="test-fake"), allow(dead_code))]`——默认构建现已消费 `log_scan`,无该 allow 仍零 warning。未新增 `src/types/schema.rs`(无去重收益,按 CLAUDE.md 保持最小范围)。
- 独立子 agent 验收通过:`cargo check -p fluss-datafusion`(默认构建,零 warning)、`cargo check --workspace` 通过;`cargo test -p fluss-datafusion --features test-fake` 全绿——18 单元测试 + 23 集成测试(原 19 + 新增 4 log SQL 测试)。新增 4 个 log 测试:带 `LIMIT` 命中(断言 last-N ids `[3,4,5,6]`)、projection pushdown(单列 `id`、`[3,4,5,6]`)、缺失 `LIMIT` 报 `LIMIT required`、`EXPLAIN` 含 `FlussLogScanExec`。perturbation:把 last-N 断言临时改为 `[1,2,3,4]` 触发预期失败,证明测试非空洞,随后精确还原。

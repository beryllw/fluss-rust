# fluss-datafusion 设计文档

> English version: [design.md](./design.md).

本文件描述 `fluss-datafusion` 的设计与内部结构,面向 crate 的维护者，以及未来要消费它的
`fluss-gateway` 集成工作。使用方式与公共 API 见同目录的 [`README.md`](./README.md)。

## 概述

`fluss-datafusion` 是一个**无状态**的 Rust library crate，通过 catalog / schema / table /
execution-plan 集成，把 Fluss 数据暴露给 DataFusion，使调用方可以直接用 `ctx.sql(...)` 查询
Fluss 表。

设计上刻意收窄：

- 让 `fluss-gateway` 的 SQL 读取路径成为可能；
- 保持该 crate 在 gateway 之外仍可独立复用；
- 复用现有的 Fluss Rust client，而不是发明一个 gateway 专属的 backend 抽象。

## 目标与范围

### 当前支持

1. database 与 table 发现（list / get schema / 表类型）
2. KV 表的完整 primary-key 等值谓词 SQL pushdown，下推为点查（point lookup）
3. log 表的有界扫描，且必须带 `LIMIT`，支持 projection pushdown
4. 复用 Fluss-to-Arrow 的 schema 与 row 转换
5. 通过 `CatalogProvider` / `SchemaProvider` / `TableProvider` / 自定义 `ExecutionPlan`
   完成 DataFusion 集成
6. 共享 installer + 每会话 `register_catalog(...)` 的使用模型

### 非目标

刻意排除（避免对尚未实现的 SQL 语义过度承诺）：

- PostgreSQL / MySQL 协议或兼容对象（如 `pg_catalog`）
- session / user / auth / 多 cluster 感知
- SQL DML 写入
- 直连读写的 REST / gRPC API
- KV prefix scan pushdown、batch lookup 优化
- offset 伪列、offset 谓词 pushdown
- 复杂的 filter / join / aggregate / sort pushdown

## 架构与依赖方向

依赖方向严格单向：`fluss client/core -> fluss-datafusion`，绝不反向。该 crate 不感知调用方
身份、协议、session 变量、auth 模式，也不知道自己是否运行在 gateway 内。

```
+-----------------------------+
|        SessionContext       |   每个 SQL 会话一个
+--------------+--------------+
               | register_catalog(...)
               v
+-----------------------------+
|       FlussDatafusion       |   共享 installer（Arc，无状态）
|   MetadataLoader (live)     |   无缓存；每次调用都回源 Fluss
+--------------+--------------+
               | FlussSource（内部 trait, crate 边界）
               v
+-----------------------------+
|       RealFlussSource       |   包裹 Arc<FlussConnection>
+--------------+--------------+
               v
        Fluss client / core
```

核心边界是内部 trait `FlussSource`：crate 内部只通过它访问 Fluss，`metadata/loader.rs` 与
`execution/*` 都**不直接持有** `FlussConnection` / `FlussAdmin` / `Lookuper` / scanner。
`FlussSource` 是内部测试 seam（`pub(crate)`），不是公共 gateway backend 抽象，不得塞入
session / protocol / auth 概念。

## crate 布局

```text
crates/integrations/datafusion/
  Cargo.toml
  docs/
    README.md          # 使用方式与公共 API
    design.md          # 本文件
  src/
    lib.rs             # 公共导出
    config.rs          # FlussDatafusionOptions / RegisterCatalogOptions
    error.rs           # FlussDatafusionError / Result
    install.rs         # FlussDatafusion: new / register_catalog
    runtime.rs         # 同步回调的 sync<->async 桥接
    backend/
      mod.rs           # FlussSource trait（pub(crate)）+ 共享别名
      real.rs          # 生产实现，包 FlussConnection
    metadata/
      mod.rs
      loader.rs        # 仅依赖 FlussSource 的实时直通；无缓存
    catalog/
      mod.rs
      provider.rs      # FlussCatalogProvider（实时同步 schema_names / schema）
      schema.rs        # FlussSchemaProvider（实时 table_names；异步 table()）
      register.rs      # build_catalog_provider
    table/
      mod.rs
      predicate.rs     # KV 谓词识别（analyze_kv_filters）
      kv.rs            # FlussKvTableProvider
      log.rs           # FlussLogTableProvider
    execution/
      mod.rs
      lookup.rs        # FlussKvLookupExec
      log_scan.rs      # FlussLogScanExec
      stream.rs        # 协作式取消的 stream 适配
    types/
      mod.rs
      scalar.rs        # ScalarValue -> Fluss KeyValue
      record_batch.rs  # projection 裁列
  tests/
    test_fluss_datafusion.rs
    integration/
      mod.rs
      utils.rs         # 共享表名常量 + SQL-path helper
      setup.rs         # integration_tests: 真实集群 bootstrap + 建表/灌数
      e2e.rs           # integration_tests: 真实后端端到端 SQL
```

> 注：crate 位于 `crates/integrations/datafusion/`（包名为 `fluss-datafusion`）。`integrations/`
> 这层目录用于分组未来可能新增的其他集成（如其他查询引擎/格式）。

## 公共 API

公共面刻意精简，全部从 `lib.rs` 导出：

- `FlussDatafusion`（`install.rs`）—— 共享 installer
- `FlussDatafusionOptions` / `RegisterCatalogOptions`（`config.rs`）
- `FlussDatafusionError` / `Result`（`error.rs`）

```rust
pub struct FlussDatafusionOptions {}               // 占位；catalog 实时，无可调项

pub struct RegisterCatalogOptions {}               // 占位，预留 per-catalog 选项

impl FlussDatafusion {
    pub async fn new(
        connection: Arc<fluss::client::FlussConnection>,
        options: FlussDatafusionOptions,
    ) -> Result<Self>;

    pub async fn register_catalog(
        &self,
        ctx: &datafusion::execution::context::SessionContext,
        catalog_name: &str,
        options: RegisterCatalogOptions,
    ) -> Result<()>;
}
```

`new()` 接收具体的 `Arc<FlussConnection>`，但内部立即把它包成 `Arc<dyn FlussSource>`，此后
crate 不再依赖 `FlussConnection`。`FlussSource` 是纯 `pub(crate)` 的内部 seam，不暴露任何测试
注入入口；集成测试一律走 `new()` 这条生产路径，对接真实集群。

## 内部模块职责

### `backend/`（内部访问 seam）

- `FlussSource` trait（`pub(crate)`）只覆盖 Phase 1 真正用到的原子操作：metadata 列举/获取、
  KV 完整 primary-key `lookup(key) -> RecordBatch`、分区列举（`list_partitions`）、log 有界
  `log_scan(partition_id, bucket, projection, limit) -> Vec<RecordBatch>`。`log_scan` 接收
  `Option<i64>` 分区 id（非分区表为 `None`），以便 scanner 定位分区限定的 bucket。
- `real.rs`：唯一实现，包 `FlussConnection` / `FlussAdmin` / lookuper / scanner，复用 fluss
  现有 Arrow helper（如 `LookupResult::to_record_batch`），不重复实现 Arrow 组装。
  `list_partitions` 把 `admin.list_partition_infos` 映射为 crate 的 `FlussPartition`（分区 id +
  分区键/值字符串对）；`log_scan` 经 `TableBucket::new_with_partition` 构造 bucket。
- seam 返回 crate 自有的精简 `FlussTableMeta`（schema + primary_keys + num_buckets + partition_keys
  等），把上层与 `fluss::metadata::TableInfo` 解耦，只携带 catalog 接线、谓词分析、执行真正需要的字段。

### `metadata/`

- `loader.rs`：`MetadataLoader` 仅依赖 `SharedFlussSource`，不持有缓存。`databases()` 每次都
  实时读取全 cluster 的 db/table 列举；`table_entry()` 每次都实时重读每表 meta + Arrow schema。
  Arrow schema 经 `fluss::record::to_arrow_schema(meta.schema.row_type())` 转换；该派生仍归
  loader 所有，避免调用方各自重复实现映射。

### `runtime.rs`

- DataFusion 同步 catalog 回调的 sync<->async 桥接。`block_on_with_runtime` 在同步回调里把 async
  源调用跑到完成；当已处于 tokio runtime 内时，会临时起一个短命线程对全局 runtime handle 做
  `block_on`，规避「runtime 内不能 block」的 panic。一个懒初始化的全局 runtime 兜底，避免每次
  调用都新建 runtime。

### `catalog/`

- `provider.rs`：`FlussCatalogProvider` 只持有共享 loader。同步 `schema_names()` 通过
  `block_on_with_runtime` 实时列举 database；`schema()` 实时校验 database 是否存在，未知名返回
  `None`（对齐 paimon 的 `DatabaseNotExist -> None`），否则返回 `FlussSchemaProvider`。
- `schema.rs`：`FlussSchemaProvider` 持有 database 名 + loader。同步 `table_names()` /
  `table_exist()` 实时列举 table；异步 `table()` 实时加载 entry，把 `TableNotFound -> Ok(None)`，
  再按 `meta.has_primary_key()` 分流，返回 `FlussKvTableProvider`（KV）或 `FlussLogTableProvider`（log）。
- `register.rs`：`build_catalog_provider` 仅把 loader 包成 `FlussCatalogProvider`，不做任何预列举，
  因此注册开销小、列表永不陈旧。

> 同步/异步桥接采用「同步回调里实时 block_on」（经 `runtime.rs`）而非「注册时快照」。这使 catalog
> 完全实时——DDL 在同一会话内即时可见——代价是每次 catalog 调用一次 admin RPC（外加一次线程
> 跳转），已知且接受。详见 README 的「元数据可见性」。

### `table/`

- `predicate.rs`：`analyze_kv_filters` 把 DataFusion 拆开的顶层 AND conjuncts 识别为完整
  primary-key 等值，按 PK 顺序产出 `LookupKey`；明确拒绝部分 PK、非 PK 列、`IN`、范围、列对列、
  重复/缺失 PK、空 filter，一律 `UnsupportedQueryPattern`。`analyze_partition_filters` 仅抽取
  分区列上的等值绑定（best-effort：无法渲染为分区值字符串的字面量直接跳过，不报错），
  `is_partition_equality` 为 log 表的下推判定识别单个分区列等值。
- `kv.rs`：`FlussKvTableProvider` 的 `supports_filters_pushdown` 仅对 PK 等值 filter 标 `Exact`
  （被消费、不再生成 `FilterExec`），其余 `Unsupported`；`scan()` 命中则构建 lookup plan（正确
  传 projection），否则把 `UnsupportedQueryPattern` 直接上抛，绝不静默退化为 full scan。
- `log.rs`：`FlussLogTableProvider` 在 `limit` 为 `None` 时清晰返回 `LimitRequired`；projection
  下推到 `FlussSource::log_scan`，`SELECT *` 的完整 identity projection 会归一化为 `None`。
  非分区表的 `supports_filters_pushdown` 一律 `Unsupported`；分区表则把分区列等值标为 `Inexact`
  （其上仍叠加 `FilterExec` 复核谓词——裁剪是 best-effort），其余 `Unsupported`。`scan()` 计算扫描
  目标（`Vec<(Option<i64>, i32)>`，即分区 id + bucket）：非分区表每 bucket 产出一个 `(None, bucket)`；
  分区表先列举分区，保留满足所有等值绑定的分区（无绑定则全保留），再产出保留分区与 bucket 的笛卡尔积。
  目标集为空（无分区命中，或分区表零分区）时返回带投影 schema 的 `EmptyExec`，而非零分区 scan。

### `execution/`

- `lookup.rs`：`FlussKvLookupExec`，单分区 leaf `ExecutionPlan`，`execute()` 经 `single_batch_stream`
  异步调 `source.lookup` 产出 0/1 行；实现 `DisplayAs`，`EXPLAIN` 显示 `FlussKvLookupExec`。
- `log_scan.rs`：`FlussLogScanExec`，leaf，每个扫描目标（`(partition_id, bucket)` 对）报告一个
  DataFusion partition；`execute(partition)` 扫描 `targets[partition]`，经 `bounded_batches_stream`
  异步调 `source.log_scan(partition_id, bucket, projection, limit)`；`EXPLAIN` 显示 `FlussLogScanExec`。
- `stream.rs`：把 lookup / scan 输出适配为 DataFusion stream（`futures::stream::once` /
  Vec 版），drop 即协作式取消。
- 不对称点：`log_scan` 在 `FlussSource` 一侧已按 projection 投影，故 log 路径**不再二次投影**；
  KV 的 lookup 返回全列后由 `types/record_batch.rs` 的 `project_batch` 裁列。

### `types/`

- `scalar.rs`：`scalar_to_key_value` 严格把 `ScalarValue`（`Boolean` / `Int8/16/32/64` / `Utf8`）
  转 `KeyValue`，NULL 与其它类型显式返回 `TypeConversion`。`scalar_to_partition_string` 把同样的
  受支持字面量类型渲染为 Fluss 分区值字符串用于等值裁剪，对 NULL / 不支持类型同样拒绝。
- `record_batch.rs`：`project_batch` 集中处理按 projection 裁列（`None` 透传）。
- 未设 `types/schema.rs`：Fluss->Arrow 映射直接复用 fluss 的 `to_arrow_schema`，无去重收益。

## 查询语义

### KV 表

仅支持**完整 primary-key 等值**谓词（`pk1 = v1 AND pk2 = v2 ...`，按 PK 全列）。命中返回 1 行，
未命中返回 0 行（不报错）。部分 key / prefix / range / 非 key filter / `IN` 一律以
`UnsupportedQueryPattern` 清晰失败，不静默退化为 full scan。

分区 KV 表无需额外处理：分区 PK 表的分区列本身就是主键的一部分，完整 PK 等值已绑定它们。
Fluss `Lookuper` 会从 lookup key row 解析出分区 id，因此既有点查路径会自动定位到唯一所属分区。

### Log 表

锁定决策：

1. `LIMIT` 为强制项；缺失即 `LimitRequired`。
2. 支持 projection pushdown。
3. 不暴露 offset 伪列、不支持 offset 谓词。
4. 底层 Fluss `LimitBatchScanner` 保留**末尾** `limit` 行（last-N），而非从最早 offset 起的 first-N。
5. 支持多 bucket 有界 scan：一个 bucket 映射为一个 DataFusion partition（`FlussLogScanExec`
   上报 `num_buckets` 个 partition，并行读取）。每个 bucket 各自返回自己的末尾 `limit` 行
   （per-bucket last-N），DataFusion 再在 scan 之上施加一个跨 bucket 的最终 `LIMIT`，因此合并
   结果恰好被裁剪到 `limit` 行。不做跨 bucket 的全局 last-N 协调。
6. `ORDER BY` pushdown 不在范围内；跨 bucket 全局 row 顺序不作保证。
7. 分区 Log 表支持分区裁剪，仅等值。`partition_col = 'value'` 谓词（每个分区键全等值）会把扫描裁剪
   到匹配分区；分区谓词绝非必需，因此不带分区谓词时扫描所有分区。扫描目标是保留分区与 bucket 的
   笛卡尔积，每个目标对应一个 DataFusion partition（partition x bucket => DataFusion partitions）。
   下推为 `Inexact`，故 DataFusion 仍叠加一个 `FilterExec` 复核谓词：裁剪是 best-effort 优化，
   正确性绝不依赖它。无任何分区命中的谓词经 `EmptyExec` 返回零行。

## 错误模型

`error.rs` 定义 crate 本地错误 `FlussDatafusionError`（别名 `Result<T>`），明确区分各类不支持/失败：

```rust
pub enum FlussDatafusionError {
    DatabaseNotFound(String),
    TableNotFound(String),
    UnsupportedQueryPattern(String),
    LimitRequired(String),
    SchemaMismatch(String),
    TypeConversion(String),
    FlussClient(String),
    Internal(String),
}
```

经 `From<FlussDatafusionError> for DataFusionError`（`External`）在 plan 或 collect 阶段清晰
冒泡。`From` 实现把 fluss / arrow 错误分别映射为 `FlussClient` / `SchemaMismatch`。该类型刻意**不**
编码任何 PostgreSQL / gateway 概念。

## 测试策略

单一真实集群分层：没有 fake/fixture 镜像层，避免「免集群断言」与「真实断言」两份相互漂移。
分两层：

| 层 | feature | 容器 | 作用 |
|---|---|---|---|
| 单元测试 | 默认 | 否 | schema 映射、`ScalarValue` 转 key、谓词识别、pushdown 决策、错误映射 |
| 集成测试（e2e） | `integration_tests` | 是 | 真实 SQL 走真实后端（`FlussDatafusion::new` -> `RealFlussSource`），覆盖 catalog 列举、KV 点查（单/复合 PK、缺键、非 PK / 部分 PK / `IN` / 无过滤等不支持形态）、log 有界扫描与 projection、`EXPLAIN` 自定义 plan，以及 `TableProvider` 暴露的 Arrow schema |
| 集成测试（实时元数据） | `integration_tests` | 是 | 验证实时、无缓存契约：`register_catalog` **之后**新建的表在同一会话内可查且被列出，被 drop 的表从该会话的列表 / `table()` 解析中消失 |

集成测试在单个测试函数里对一个集群顺序断言：拉起 Fluss 集群成本高，而这些断言均为只读、彼此独立，
故共用一个集群以降低开销（`setup.rs` 提供共享建表/灌数）。由于 catalog 不持有缓存，没有白盒缓存
机制需要单测；实时可见性行为直接对真实后端做端到端验证。

验证命令：

```bash
cargo check --workspace
cargo test -p fluss-datafusion                               # 单元
cargo test -p fluss-datafusion --features integration_tests  # 需容器, 真实集群
```

边界约束（对齐 CLAUDE.md）：`FlussSource` 是内部 seam，保持 `pub(crate)`，不进公共 API，也不
做成 gateway 形状的 backend 抽象。

## 与 fluss-gateway 的衔接

本 crate 只负责 `SQL -> Fluss 表访问`。`fluss-gateway` 在其上应：

1. 为每个 cluster/proxy 连接创建一个共享的 `FlussDatafusion`；
2. 当某个 SQL session 构建新的 `SessionContext` 时，调用 `register_catalog(&ctx, "fluss", ...)`；
3. 在真实 Fluss catalog 之上叠加 gateway 专属的 SQL 环境设置；
4. 把 `pg_catalog`、session 变量、auth、timeout 与 cancel 语义保留在 gateway，而非此处。

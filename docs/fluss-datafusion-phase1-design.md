# fluss-datafusion Phase 1 设计与任务拆解

## 适用读者

- 在 `fluss-rust` 内实现 `fluss-datafusion` 的维护者
- 未来要消费该 crate 的 `fluss-gateway` 集成工作

## 目标

新增一个无状态的 Rust library crate,通过 catalog、schema、table、execution-plan 集成,把 Fluss 数据暴露给 DataFusion。

Phase 1 的范围刻意收窄:

- 让 `fluss-gateway` 的 SQL 读取路径成为可能
- 保持该 crate 在 gateway 之外仍可复用
- 复用现有的 Fluss Rust client,而不是发明一个 gateway 专属的 backend 抽象

## 范围(Scope)

Phase 1 必须支持:

1. database 与 table 发现
2. KV table 的完整 primary-key 等值谓词 SQL pushdown
3. log table 的有界扫描,且必须带 `LIMIT`
4. 复用 Fluss-to-Arrow 的 schema 与 row 转换
5. 通过以下方式完成 DataFusion 集成:
   - `CatalogProvider`
   - `SchemaProvider`
   - `TableProvider`
   - 自定义 `ExecutionPlan`
6. 针对真实 Fluss test cluster 的 integration tests

## 非目标(Non-goals)

Phase 1 不包含:

- PostgreSQL 兼容对象,例如 `pg_catalog`
- session、user、auth 或多 cluster 感知
- SQL DML 写入
- 直连读写的 REST API
- MySQL 或 PostgreSQL 协议兼容
- prefix scan pushdown
- batch lookup 优化
- offset 伪列(pseudo columns)
- offset 谓词 pushdown
- 复杂的 filter、join、aggregate 或 sort pushdown

## 放置位置决策

`fluss-datafusion` 应位于:

```text
crates/fluss-datafusion/
```

不应位于:

```text
integration/fluss-datafusion/
```

理由:

1. 当前 workspace 约定是:可复用的 Rust crate 放在 `crates/` 下。
2. 本仓库中 `integration` 当前仅用于测试,例如 `crates/fluss/tests/integration/*`。
3. `fluss-datafusion` 是一等公民、可复用的 library crate,而非 test harness。
4. 与 `crates/fluss` 分离,可避免 DataFusion 依赖与 planning 逻辑泄漏进核心 client crate。

相关仓库参考:

- `Cargo.toml:29-31`
- `DEVELOPMENT.md:75-95`
- `README.md:63-69`

## 可复用的现有构件

仓库已具备 Phase 1 所需的大部分底层能力。

### Metadata API

使用 `crates/fluss/src/client/admin.rs` 中现有的 admin API:

- `FlussAdmin::list_databases`
- `FlussAdmin::list_tables`
- `FlussAdmin::get_table_info`
- `FlussAdmin::get_table_schema`

这些已覆盖 Phase 1 的 metadata 发现需求,无需新增 Fluss RPC 抽象。

### KV point lookup API

使用 `crates/fluss/src/client/table/lookup.rs` 中现有的 table lookup 路径:

- `TableLookup::create_lookuper`
- `Lookuper::lookup`
- `LookupResult::to_record_batch`

这是完整 primary-key 等值 pushdown 的天然 execution backend。

### Log 有界读取 API

现有的有界读取有两个方向:

1. `crates/fluss/src/client/table/batch_scanner.rs`
   - 适合一次性的有界读取
   - 当前未暴露显式的 `start_offset`
2. `crates/fluss/src/client/table/reader.rs`
   - `RecordBatchLogReader::new_until_offsets`
   - 当需要显式 stop-offset 语义时更合适

Phase 1 的 log execution 应优先选择能保持最窄、最清晰 SQL 契约的路径。

### Arrow 转换 helper

复用 `crates/fluss/src/record/arrow.rs` 中现有的 helper:

- `to_arrow_schema`
- `from_arrow_field`
- `RowAppendRecordBatchBuilder`

不要从零重新实现 Fluss-to-Arrow 映射。

### 测试基础设施

复用支持 crate:

- `crates/fluss-test-cluster`

遵循现有 integration tests 的模式:

- `crates/fluss/tests/test_fluss.rs`
- `crates/fluss/tests/integration/utils.rs`
- `crates/fluss/tests/integration/kv_table.rs`
- `crates/fluss/tests/integration/log_table.rs`
- `crates/fluss/tests/integration/record_batch_log_reader.rs`

注意:`crates/fluss/tests/integration/utils.rs` 并非可复用的 library。`crates/fluss-datafusion` 中的新测试必须自建本地 `tests/integration/utils.rs`,通常做法是移植最小 helper 模式,同时依赖 `fluss-test-cluster`。

## 建议的 crate 布局

```text
Cargo.toml

docs/
  fluss-datafusion-phase1-design.md

crates/
  fluss-datafusion/
    Cargo.toml
    src/
      lib.rs
      config.rs
      error.rs
      install.rs

      backend/
        mod.rs
        real.rs
        fake.rs        # 仅测试: feature = "test-fake"

      metadata/
        mod.rs
        cache.rs
        loader.rs

      catalog/
        mod.rs
        provider.rs
        schema.rs
        register.rs

      table/
        mod.rs
        predicate.rs
        kv.rs
        log.rs

      execution/
        mod.rs
        lookup.rs
        log_scan.rs
        stream.rs

      types/
        mod.rs
        schema.rs
        scalar.rs
        record_batch.rs

    tests/
      test_fluss_datafusion.rs
      fixtures/                  # 从真实集群抓取的快照(committed)
      integration/
        utils.rs                 # 建表/写数据 + 构造 fake 或 real backend
        capture.rs               # gate: integration_tests, 抓快照写入 fixtures
        catalog.rs               # SQL 断言: 默认 fake, integration_tests 切 real
        kv_lookup.rs
        log_scan.rs
        explain.rs
```

## 公共 API 形状

Phase 1 的公共 API 应保持精简。

### `src/lib.rs`

仅导出公共入口:

- `FlussDatafusion`
- `FlussDatafusionOptions`
- `RegisterCatalogOptions`
- `FlussDatafusionError`
- 需要时再加 crate 内的 `Result<T>` alias

### `src/config.rs`

定义:

```rust
pub struct FlussDatafusionOptions {
    pub metadata_cache_ttl: Option<std::time::Duration>,
    pub table_cache_capacity: usize,
}

pub struct RegisterCatalogOptions {}
```

Phase 1 中 options 保持最小化,不要在此引入 session 专属的开关。

### `src/install.rs`

定义主入口:

```rust
pub struct FlussDatafusion {
    inner: std::sync::Arc<Inner>,
}

impl FlussDatafusion {
    pub async fn new(
        connection: std::sync::Arc<fluss::client::FlussConnection>,
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

Phase 1 不应暴露 gateway 专属的抽象。

内部实现说明:`new()` 接收具体的 `Arc<FlussConnection>`,但 crate 内部不直接依赖它。`new()` 会把连接包装成内部的 `Arc<dyn FlussSource>`(见 `src/backend/`),所有 metadata / lookup / log scan 调用都走这个 seam。crate 另外提供一个 `pub(crate)` 测试构造入口(例如 `new_with_source(source)`),用于注入 fake,而不把它暴露为公共 API。详见测试策略中的 fake 小节。

## 内部模块职责

### `src/backend/`(内部访问 seam)

职责:

- 定义 crate 内部唯一依赖的 Fluss 访问 trait `FlussSource`(`pub(crate)`,非公共 API)
- `real.rs`:基于 `FlussConnection` / `FlussAdmin` / `Lookuper` / scanner 的生产实现
- `fake.rs`:仅测试用的内存实现,从快照 fixtures replay(见测试策略)

`FlussSource` 只覆盖 Phase 1 真正用到的操作:

- metadata:`list_databases` / `list_tables` / `get_table_info` / `get_table_schema`
- KV:对完整 primary key 的 `lookup(key) -> RecordBatch`
- log:有界 scan,产出 `RecordBatch` 流

关键规则:

- `metadata/loader.rs` 与 `execution/*` 不直接持有 `FlussConnection` / `FlussAdmin` / `Lookuper`,只依赖 `FlussSource`
- 该 trait 是内部测试 seam,不是公共 gateway backend 抽象;不要把 session / protocol / auth 概念塞进它
- 该 seam(完整 surface)在 Task 2 引入:`real.rs` 包 fluss client,`fake.rs` 从快照 replay;Task 3/4/5 在其上做 DataFusion 集成

### `src/metadata/cache.rs`

职责:

- 为 databases、tables 与 table metadata 提供共享 cache
- cache 条目可跨 session 与 `SessionContext` 复用
- 不持有 gateway 的 session 状态

期望被缓存的对象:

- database 名称
- 每个 database 下的 table 名称
- 每张表的 `TableInfo`
- 需要时,每张表最新的 schema 信息

### `src/metadata/loader.rs`

职责:

- 使用 `FlussAdmin` 进行异步加载
- 把 DataFusion 同步风格的 trait 方法桥接到缓存数据
- 强制 lazy loading,而非预加载整个 cluster

关键设计规则:

- DataFusion 同步 trait 入口应从共享 cache 读取
- 异步刷新路径应置于 cache loader 之后
- 避免每次查询都做全量 metadata 扫描

### `src/catalog/provider.rs`

职责:

- 实现顶层 DataFusion `CatalogProvider`
- 把 Fluss databases 暴露为 schemas

### `src/catalog/schema.rs`

职责:

- 实现 DataFusion `SchemaProvider`
- 暴露某个 database 内的 Fluss tables
- 构造 KV 或 log 的 `TableProvider`

### `src/catalog/register.rs`

职责:

- 构建 catalog provider 图
- 将其注册进 `SessionContext`
- 让注册逻辑保持在 `lib.rs` 之外

### `src/table/predicate.rs`

职责:

- 检查 DataFusion filter 表达式
- 为 KV tables 识别完整 primary-key 等值谓词
- 清晰地归类不支持的模式

Phase 1 接受的 KV 模式:

- 针对完整 primary key 的 `pk1 = value AND pk2 = value ...`

Phase 1 拒绝的 KV 模式:

- 部分 primary key 过滤
- `IN (...)`
- 非 primary-key 过滤
- prefix 模式
- range scan

### `src/table/kv.rs`

职责:

- 实现 KV `TableProvider`
- 声明所支持的 filter pushdown
- 当 filter 完全匹配 primary key 时,构建 lookup execution plan

重要规则:

- 当查询不匹配所支持的 KV 模式时,不要静默退化为 full scan
- 应返回清晰的 unsupported-query 错误

### `src/table/log.rs`

职责:

- 实现 log `TableProvider`
- 强制要求 `LIMIT`
- 构建有界的 log execution plan

重要规则:

- 没有 `LIMIT` 的 log table 必须以 `LimitRequired` 失败

### `src/execution/lookup.rs`

职责:

- 为 KV point lookup 实现 DataFusion `ExecutionPlan`
- 通过 `Lookuper::lookup` 执行
- 产出 `SendableRecordBatchStream`

### `src/execution/log_scan.rs`

职责:

- 为有界 log 读取实现 DataFusion `ExecutionPlan`
- 复用现有的 Fluss scan/reader 构件
- 产出 `SendableRecordBatchStream`

### `src/execution/stream.rs`

职责:

- 将 lookup 或 scan 的输出适配为 DataFusion streams
- 保持 drop 行为简单且具备协作式(cooperative)取消

### `src/types/schema.rs`

职责:

- 集中处理 DataFusion 集成特有的 schema 转换胶水代码
- 尽量复用 `fluss::record::arrow::to_arrow_schema`

### `src/types/scalar.rs`

职责:

- 将 DataFusion `ScalarValue` 转换为 lookup execution 所需的 Fluss row/key 表示
- 严格校验类型,遇到不支持的转换时显式失败

### `src/types/record_batch.rs`

职责:

- 将现有的 Fluss batch 结果适配为对 DataFusion 友好的 batch 输出
- 避免重复 `fluss` 中已有的底层 Arrow 组装逻辑

### `src/error.rs`

定义 crate 专属的错误模型,例如:

```rust
pub enum FlussDatafusionError {
    DatabaseNotFound,
    TableNotFound,
    UnsupportedQueryPattern,
    LimitRequired,
    SchemaMismatch,
    TypeConversion,
    MetadataLoad,
    FlussClient,
    Internal,
}
```

该 crate 不得编码 PostgreSQL 或 gateway 专属的错误。

## Phase 1 查询语义

## KV tables

Phase 1 的 KV 支持刻意严格。

支持:

- 仅完整 primary-key 等值谓词

不支持:

- 部分 key scan
- prefix scan
- range scan
- 非 key 的 filter pushdown
- 隐藏的退化 full scan

建议:

- 对不支持的 KV SQL 直接清晰失败,而非假装支持超出实际实现的能力

## Log tables

Phase 1 的 log 支持必须保守。

Phase 1 已锁定的决策:

1. `LIMIT` 为强制项。
2. 支持 projection pushdown。
3. 不暴露 offset 伪列。
4. 不支持 offset 谓词。
5. bucket 内顺序沿用现有的 Fluss log 读取语义。
6. 除非未来版本实现更强的 ordering 语义,否则跨 bucket 的全局 row 顺序不作保证。

建议的初始行为:

- 从最早可用的 offset 开始读取
- 由 SQL `LIMIT` 限定读取量
- 保持契约狭窄,并明确文档说明 `ORDER BY` pushdown 不在范围内

这样可使 crate 与现有 Fluss client 能力保持一致,避免对尚未实现的 SQL 语义过度承诺。

## 依赖计划

### 根 `Cargo.toml`

更新 workspace 根文件:

- 在 `[workspace].members` 中加入 `crates/fluss-datafusion`
- 在 `[workspace.dependencies]` 中加入 `datafusion`
- 若 workspace 希望统一版本,可在此加入其他直接需要的支持 crate

### `crates/fluss-datafusion/Cargo.toml`

预期依赖:

- `fluss = { workspace = true }`
- `arrow = { workspace = true }`
- `tokio = { workspace = true }`
- 仅在需要时引入 `serde` 与 `serde_json`
- `datafusion`
- 仅在确有必要时引入 `async-trait`
- 仅在合理时引入小型 utility crate

准则:

- 保持新 crate 的依赖面狭窄
- 不要把 gateway 或 PostgreSQL 兼容 crate 拉进本 crate

## 测试策略

### 单元测试(Unit tests)

把聚焦的单元测试与其所验证的模块放在一起。

必需覆盖:

- predicate 识别
- `ScalarValue` 转换
- schema 映射胶水代码
- pushdown 决策行为
- 错误映射行为

### 集成测试(Integration tests)

创建:

- `crates/fluss-datafusion/tests/test_fluss_datafusion.rs`
- `crates/fluss-datafusion/tests/integration/*`

建议模式:

- 参照 `crates/fluss/tests/test_fluss.rs`
- 用 `integration_tests` feature 对依赖 cluster 的集成测试进行 gate
- 在 `fluss-test-cluster` 之上构建本地 `tests/integration/utils.rs`

Phase 1 必需的集成用例:

1. `catalog.rs`
   - 注册 catalog 后能暴露 databases 与 tables
2. `kv_lookup.rs`
   - 带完整 primary-key 等值的 SQL 返回期望 row
   - 复合 primary-key 等值同样可用
   - 不支持的谓词清晰失败
3. `log_scan.rs`
   - log table 查询要求 `LIMIT`
   - 有界 log scan 返回 rows
   - projection pushdown 可用
4. `explain.rs`
   - `EXPLAIN` 显示自定义 execution plan 名称
   - 不支持的 plan 不会被误报为已 pushdown

### 基于 fake 的快速集成测试(免集群)

Docker 集成测试是真值,但慢且依赖环境。为让 catalog 接线、谓词 pushdown、schema 转换、错误映射这类纯逻辑在没有 Docker 时也能快速验证,引入一条 fake 路径:

- crate 内部统一依赖 `FlussSource` seam(见 `src/backend/`),生产实现包 `FlussConnection`,fake 实现走内存数据。
- fake 的数据来自真实集群快照(record/replay),而不是手写假数据,这样 fake 与真实 Fluss 的 schema、编码、行内容不易漂移。

数据流:

```text
real cluster --capture(Docker)--> tests/fixtures (committed) --replay(no cluster)--> fake FlussSource --> DataFusion
```

- capture:`tests/integration/capture.rs`,受 `integration_tests` gate,连真实集群,记录 Phase 1 用到的响应(list_databases / list_tables / get_table_info / get_table_schema、KV `lookup` 的 key -> RecordBatch、log 有界 scan 的 RecordBatch 序列)。batch 用 Arrow IPC 序列化,metadata 用 serde,统一落到 `tests/fixtures/` 并 commit。
- replay:`src/backend/fake.rs`(feature `test-fake`)从 fixtures 加载并按请求 replay,不开任何 socket;`tests/integration/*` 的 SQL 测试经 `utils.rs` 的 helper 选择 backend,默认用 fake 跑 `register_catalog` + `ctx.sql(...)`。

fake 路径覆盖:

- catalog / schema / table 列举
- KV 完整 primary-key 谓词命中 lookup,不支持谓词清晰失败
- log 必须带 `LIMIT`、有界 scan 产出 rows、projection pushdown
- schema 转换与错误映射
- `EXPLAIN` 显示自定义 plan 名称

fake 路径不覆盖(仍由 Docker 集成测试兜底):

- 真实 RPC / 网络 / leader 路由 / 重试语义
- 真实并发与一致性行为
- fixtures 过期与真实 cluster 的偏差(靠重跑 capture 刷新)

边界约束(对齐 CLAUDE.md):`FlussSource` 与 fake 都是内部测试 seam,保持 `pub(crate)` / feature-gated,不进公共 API,也不做成 gateway 形状的 backend 抽象。

### 验证命令

Phase 1 工作至少应通过:

```bash
cargo check --workspace
cargo test -p fluss-datafusion
cargo test -p fluss-datafusion --features test-fake          # 免集群, replay fixtures
cargo test -p fluss-datafusion --features integration_tests  # 需 Docker, 真实集群
```

若集成测试较慢或对环境敏感,保持 feature gate 显式。

## 文件级实现任务

> 任务进度不记录在本文件中。各 Task 的状态见独立的临时文档 `docs/fluss-datafusion-phase1-status.md`,Phase 1 全部完成后删除该文档。

## Task 1: workspace 与 crate 骨架

目标:

- 将新 crate 干净地引入 workspace

文件:

- `Cargo.toml`
- `crates/fluss-datafusion/Cargo.toml`
- `crates/fluss-datafusion/src/lib.rs`
- `crates/fluss-datafusion/src/config.rs`
- `crates/fluss-datafusion/src/error.rs`
- `crates/fluss-datafusion/src/install.rs`

交付物:

- 加入 workspace member
- 加入依赖行
- crate 可构建
- 即使内部为桩(stub),公共 API 形状也能编译

验证:

- `cargo check -p fluss-datafusion`

## Task 2: `FlussSource` seam + fake 测试 harness(测试前置)

目标:

- 先立起测试基础设施,让 Task 3/4/5 都能 test-first(RED-GREEN)开发
- 引入内部 `FlussSource` seam,并提供一个真实数据驱动的 fake 作为免集群快速测试入口

为何前置:

- 测试用来加速开发、提高交付质量,不排在最后
- seam + fake 一旦就位,catalog / KV / log 都可先写 fake-backed 测试再实现
- capture 走底层 fluss client(已存在),不依赖 fluss-datafusion 的 real source,因此可尽早抓到真实快照

文件:

- `crates/fluss-datafusion/src/backend/mod.rs`         # `FlussSource` trait(`pub(crate)`, 完整 surface)
- `crates/fluss-datafusion/src/backend/real.rs`        # 基于 fluss client 的薄数据访问适配
- `crates/fluss-datafusion/src/backend/fake.rs`        # feature `test-fake`, 从 fixtures replay
- `crates/fluss-datafusion/tests/integration/utils.rs` # 建表/写数据 + 构造 fake 或 real backend 的 helper
- `crates/fluss-datafusion/tests/integration/capture.rs` # gate `integration_tests`, 连真实集群抓快照
- `crates/fluss-datafusion/tests/fixtures/*`           # committed 快照

交付物:

- 完整定义 `FlussSource`(metadata + KV `lookup` + log 有界 scan 的原子操作),`pub(crate)`, 非公共 API
- `real.rs`:用 `FlussConnection` / `FlussAdmin` / `Lookuper` / scanner 包装上述操作,返回 `RecordBatch`(复用 fluss 现有 Arrow helper)
- `capture.rs`:用 `fluss-test-cluster` 建好已知测试表并写入数据,记录 Phase 1 用到的响应写入 `tests/fixtures/`
  - `list_databases` / `list_tables` / `get_table_info` / `get_table_schema`
  - KV `lookup`:key -> `RecordBatch`
  - log 有界 scan:`RecordBatch` 序列
  - batch 用 Arrow IPC 序列化,metadata 用 serde
- `fake.rs`:从 fixtures 加载并按 key / 表名 replay,不开任何 socket,实现完整 `FlussSource`
- `new()` 内部构造 real source;提供 `pub(crate)` 测试注入入口(例如 `new_with_source(source)`)供测试注入 fake

验证:

- `cargo test -p fluss-datafusion --features integration_tests -- capture`  # 抓取/刷新 fixtures(需 Docker)
- `cargo test -p fluss-datafusion --features test-fake`                       # fake 可注入并 replay(免集群)

## Task 3: metadata cache 与注册路径

目标:

- 让 `register_catalog()` 创建一棵由共享 metadata 支撑的真实 Fluss catalog 树

文件:

- `crates/fluss-datafusion/src/metadata/mod.rs`
- `crates/fluss-datafusion/src/metadata/cache.rs`
- `crates/fluss-datafusion/src/metadata/loader.rs`
- `crates/fluss-datafusion/src/catalog/mod.rs`
- `crates/fluss-datafusion/src/catalog/provider.rs`
- `crates/fluss-datafusion/src/catalog/schema.rs`
- `crates/fluss-datafusion/src/catalog/register.rs`

交付物:

- `metadata/loader.rs` 依赖 `FlussSource` 而非直接持有 `FlussConnection`
- catalog 注册可对 `SessionContext` 生效
- database 与 table 列举使用共享 metadata 状态
- 不做 per-session 的全量 metadata 预热

验证(test-first, 默认走 Task 2 的 fake):

- `register_catalog` + 列举 databases/tables 的测试
- 共享 metadata 在 dirty / 新 `SessionContext` 间复用的测试

## Task 4: KV 谓词分析与 lookup 执行

目标:

- 支持针对完整 primary-key 等值的狭窄 KV pushdown 路径

文件:

- `crates/fluss-datafusion/src/table/mod.rs`
- `crates/fluss-datafusion/src/table/predicate.rs`
- `crates/fluss-datafusion/src/table/kv.rs`
- `crates/fluss-datafusion/src/execution/mod.rs`
- `crates/fluss-datafusion/src/execution/lookup.rs`
- `crates/fluss-datafusion/src/execution/stream.rs`
- `crates/fluss-datafusion/src/types/mod.rs`
- `crates/fluss-datafusion/src/types/scalar.rs`
- `crates/fluss-datafusion/src/types/record_batch.rs`

交付物:

- 能识别完整 primary-key 等值
- lookup 执行经 `FlussSource::lookup` 走现有 Fluss lookup API
- 不支持的 KV SQL 返回清晰错误

验证(test-first, 默认走 Task 2 的 fake):

- predicate 匹配与 scalar 转换的单元测试
- 单 key 与复合 key SQL lookup 的测试
- 不支持谓词清晰失败的测试
- `EXPLAIN` 显示自定义 lookup plan

## Task 5: log 有界扫描执行

目标:

- 支持要求 `LIMIT` 的狭窄 log 读取路径

文件:

- `crates/fluss-datafusion/src/table/log.rs`
- `crates/fluss-datafusion/src/execution/log_scan.rs`
- `crates/fluss-datafusion/src/types/schema.rs`
- `crates/fluss-datafusion/src/types/record_batch.rs`

交付物:

- log `TableProvider` 要求 `LIMIT`
- 有界执行经 `FlussSource` 的 scan 产出 `RecordBatch` stream 输出
- projection pushdown 可用

验证(test-first, 默认走 Task 2 的 fake):

- 带 `LIMIT` 的 log 查询测试
- 缺失 `LIMIT` 报错的测试
- projection pushdown 的测试
- `EXPLAIN` 显示自定义 log scan plan

## Task 6: Docker 端到端集成测试(真值兜底)

目标:

- 在真实 cluster 上端到端验证完整 real 路径,作为 fake 测试的真值兜底

文件:

- `crates/fluss-datafusion/tests/test_fluss_datafusion.rs`
- `crates/fluss-datafusion/tests/integration/catalog.rs`
- `crates/fluss-datafusion/tests/integration/kv_lookup.rs`
- `crates/fluss-datafusion/tests/integration/log_scan.rs`
- `crates/fluss-datafusion/tests/integration/explain.rs`
- (复用 Task 2 的 `tests/integration/utils.rs`)

交付物:

- 受 `integration_tests` gate 的入口:把 Task 3/4/5 的 SQL 测试切到 real backend,在真实 cluster 上跑通
- 覆盖 Phase 1 SQL 路径:catalog 列举、KV point lookup、log 有界 scan、`EXPLAIN`
- 拥有 fake 覆盖不到的真值断言:真实 RPC / 网络 / leader 路由 / 并发语义
- 校验 fixtures 与真实 cluster 不漂移(必要时重跑 capture 刷新)

验证:

- `cargo test -p fluss-datafusion --features integration_tests`

## 推荐的 sub-agent 执行顺序

按以下顺序串行推进 sub-agent 工作:

1. Task 1: workspace 与 crate 骨架
2. Task 2: `FlussSource` seam + fake 测试 harness(测试前置)
3. Task 3: metadata cache 与注册路径
4. Task 4: KV 谓词分析与 lookup 执行
5. Task 5: log 有界扫描执行
6. Task 6: Docker 端到端集成测试(真值兜底)

为何采用此顺序:

- 测试前置:Task 2 先立起 seam + fake + 真实数据 capture,让 Task 3/4/5 都能 test-first 开发
- Task 2 的 capture 走底层 fluss client,不依赖后续 real source,可尽早抓到真实快照
- Task 3 / 4 / 5 依赖骨架(Task 1)与 fake harness(Task 2),边写 fake-backed 测试边实现
- Task 6 收尾:在真实 cluster 上验证 real 路径,补齐 fake 覆盖不到的 RPC / 网络 / 并发语义

## 仓库外的后续工作

本文档只覆盖 `fluss-rust` 内的工作。

Phase 1 在此落地后,`fluss-gateway` 的后续工作应:

1. 为每个 cluster/proxy 连接创建一个共享的 `FlussDatafusion`
2. 当某个 SQL session 构建新的 `SessionContext` 时,调用 `register_catalog(&ctx, "fluss", ...)`
3. 在真实 Fluss catalog 之上叠加 gateway 专属的 SQL 环境设置
4. 把 `pg_catalog`、session 变量、auth、timeout 与 cancel 语义保留在 gateway,而非此处

## 小结

Phase 1 应在 `crates/fluss-datafusion/` 新增一个可复用 crate,而非放在 `integration/` 下。

实现应保持狭窄:

- 由共享 metadata 支撑的 catalog 注册
- KV 完整 primary-key 等值 pushdown
- 要求 `LIMIT` 的 log 有界扫描
- 真实 cluster 的集成测试

这样可使该 crate 与现有 `fluss` client 架构保持一致,并为日后 `fluss-gateway` 的工作提供稳定的 installer 式集成点。

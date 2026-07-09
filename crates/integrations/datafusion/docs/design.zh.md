# fluss-datafusion 设计文档

> English version: [design.md](./design.md).
>
> 本文采用接近 FIP 的组织方式，系统描述当前 `fluss-datafusion` 模块的设计、
> 公共契约、已经落地的代码路径，以及仍处于收敛阶段的部分。它刻意以当前实现为准，
> 而不是描述一个理想化但尚未落地的 gateway。

使用方式与快速上手见 [`README.md`](./README.md)。
本模块正式的 FIP 风格提案见 [`FIP.md`](./FIP.md) 与
[`FIP.en.md`](./FIP.en.md)。
lake-enabled 表的 adapter 边界与读取路由规则，另见
[`fip-lake-integration.zh.md`](./fip-lake-integration.zh.md)。

## Motivation

`fluss-datafusion` 的目标，是通过 DataFusion 的标准扩展点把 Fluss 表暴露给
SQL 执行层：

- `CatalogProvider`
- `SchemaProvider`
- `TableProvider`
- 自定义 `ExecutionPlan`

它解决的是 Rust 侧 Fluss 生态中的一个非常具体的问题：Fluss 原生提供的是
KV / log 读取能力与客户端 API，而不是一个现成的 DataFusion catalog；但上层
SQL 消费方希望直接使用 `ctx.sql(...)` 查询 Fluss 表。

这个模块必须同时桥接三类读取路径：

1. **非 lake KV 表**
   - 完整 primary-key equality 的点查；
   - 完整 bucket-key prefix equality 的前缀查；
   - 仅在存在 `LIMIT` 时才支持有界扫描。
2. **非 lake log 表**
   - earliest 到 latest 的有限 snapshot scan；
   - 可选 `LIMIT`，语义是 head-first 的 first-N。
3. **lake-enabled 表**
   - 基于 lake snapshot + residual Fluss log tail 的 unified read；
   - 通过 `<table>$lake` 暴露 lake-only read；
   - 对符合条件的主键查询保留直接的 KV fast path。

这里最重要的设计压力，是让这个 crate 保持 **thin and reusable**：

- 它应该是一个 library，而不是协议服务器；
- 它不应该拥有 session / auth / protocol 语义；
- 它不应该重复实现本应归属 `fluss-lake` 的 lake+log union 语义；
- 它不应该用内部缓存掩盖 metadata freshness。

换句话说，`fluss-datafusion` 是 Fluss 的 DataFusion adapter 层，而不是第二个
storage kernel，也不是一个已经完整成型的 query gateway。

## Public Interfaces

这个 crate 的公共接口刻意很小。

### Rust API

当前导出的只有：

- `FlussDatafusion`
- `FlussDatafusionOptions`
- `RegisterCatalogOptions`
- `FlussDatafusionError`
- `Result`

当前操作 API 如下：

```rust
pub struct FlussDatafusionOptions {
    pub lake_storage_options: std::collections::HashMap<String, String>,
}

pub struct RegisterCatalogOptions {}

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

    pub fn swap_connection(
        &self,
        new: Arc<fluss::client::FlussConnection>,
    ) -> Result<()>;
}
```

这些 API 对外的关键行为是：

- `new(...)` 基于一个 live 的 Fluss connection 构造共享 installer；
- `register_catalog(...)` 把一个 fully live 的 Fluss catalog 安装到
  `SessionContext`；
- `swap_connection(...)` 支持在不重新注册 catalog 的前提下热切换底层连接。

### SQL 可见契约

通过 DataFusion 暴露给 SQL 使用者的契约包括：

- 表路径形态为 `<catalog>.<database>.<table>`；
- `<table>$options` 作为 key/value metadata table；
- 在 `lake` feature 下暴露 `<table>$lake` 作为 lake-only read；
- 在 `EXPLAIN` 中可见自定义 physical plan 名称。

当前主要可见的执行计划名称有：

- `FlussKvLookupExec`
- `FlussKvPrefixLookupExec`
- `FlussKvScanExec`
- `FlussLogScanExec`
- `FlussUnionScanExec`

### Feature gating

这个 crate 的行为会被 feature 实质性改变：

- 默认构建：仅包含核心 DataFusion 集成，不强依赖 `fluss-lake`；
- `lake`：开启 lake-enabled 表路由与 `<table>$lake`；
- `integration_tests`：开启真实集群集成测试与从 `fluss-lake` re-export 的
  test-only seam helper。

## Proposed Changes

这一节描述本文希望固化的模块契约，以及当前已经落地的实现行为。

### 1. 模块范围与定位

`fluss-datafusion` 被定义为一个 **stateless 的 Fluss DataFusion adapter**。

它负责：

- 把 live 的 Fluss catalog 注册进 DataFusion session；
- 按需加载 table metadata 与 Arrow schema；
- 识别受支持的 SQL predicate 形态；
- 将读取路由到 KV fast path、有界 scan、log snapshot scan，或
  `fluss-lake` kernel；
- 把 Fluss / `fluss-lake` 的输出适配为 DataFusion `ExecutionPlan`。

它明确 **不** 负责：

- PostgreSQL / Flight SQL / REST / gRPC 协议前端；
- session variable、user identity、auth；
- SQL 写入或 DML；
- 为任意 source 提供通用 backend abstraction；
- lake seam 解释、append stitch、PK merge 逻辑本身。

因此依赖方向必须保持单向：

```text
+-----------------------------+
|       SessionContext        |
+--------------+--------------+
               |
               v
+-----------------------------+
|       FlussDatafusion       |
|     live catalog installer  |
+--------------+--------------+
               |
               v
+-----------------------------+
|        MetadataLoader       |
|         FlussSource         |
+--------------+--------------+
               |
               v
+-----------------------------+
|       RealFlussSource       |
|   Fluss client / fluss-lake |
+-----------------------------+
```

### 2. 架构概览

当前 crate 可以分成四层。

#### 2.1 Installer 与生命周期层

- `src/install.rs`
- `src/config.rs`
- `src/runtime.rs`

这一层负责：

- 共享 `FlussDatafusion` installer；
- session 级的 `register_catalog(...)`；
- connection hot-swap；
- 为 DataFusion 同步 catalog callback 提供 sync/async bridge。

一个 `FlussDatafusion` 实例预期会被多个 `SessionContext` 复用。

#### 2.2 Metadata 与 catalog 层

- `src/metadata/loader.rs`
- `src/catalog/provider.rs`
- `src/catalog/schema.rs`
- `src/catalog/register.rs`

这一层把 Fluss metadata 映射为 DataFusion catalog 模型：

- 一个 DataFusion catalog 包含多个 Fluss database；
- 一个 Fluss database 映射到一个 `SchemaProvider`；
- 一张 Fluss table 动态映射为一个 `TableProvider`。

这里的刻意设计是 metadata **live 且无缓存**：

- `schema_names()` 实时列库；
- `table_names()` 实时列表；
- `table()` 每次都重载该表 metadata 与 Arrow schema；
- `register_catalog(...)` 之后的 DDL 在同一个 session 中立即可见。

#### 2.3 Table-provider 路由层

- `src/table/kv.rs`
- `src/table/log.rs`
- `src/table/union.rs`
- `src/table/predicate.rs`

这一层承载读路径识别逻辑。

它决定一次查询究竟应走：

- point lookup；
- prefix lookup；
- bounded KV scan；
- finite log snapshot scan；
- lake+log unified read；
- lake-only read。

#### 2.4 Physical execution 层

- `src/execution/lookup.rs`
- `src/execution/prefix_lookup.rs`
- `src/execution/log_scan.rs`
- `src/execution/union_scan.rs`
- `src/execution/stream.rs`

这一层暴露真正的 DataFusion `ExecutionPlan`，包装 Fluss 或
`fluss-lake` 的读取，再适配成 `SendableRecordBatchStream`。

### 3. 公共 API 与生命周期模型

推荐的使用生命周期是：

1. 调用方先构造 `FlussConnection`；
2. 再构造一个共享的 `FlussDatafusion` installer；
3. 每个 SQL session 创建自己的 `SessionContext`；
4. 调用 `register_catalog(&ctx, "fluss", ...)`；
5. 之后所有查询都经过这个 live catalog。

当前实现中已经成立的关键运维属性包括：

- installer 本身与 session 无关，可重复复用；
- catalog provider 不在注册时做 metadata snapshot；
- `swap_connection(...)` 允许已注册的 session 在后续访问中透明切换到新连接。

### 4. Catalog / schema / table 映射

当前 table 路由逻辑实现于 `src/catalog/schema.rs`。

解析优先级为：

1. `<table>$options`
2. `lake` feature 下的 `<table>$lake`
3. `<table>$lake$<system>` -> 显式 unsupported error
4. 普通表名 -> 按 metadata 选择 provider

对普通表，provider 路由规则如下：

| 表形态 | Provider |
| --- | --- |
| `lake` feature 开启且表为 lake-enabled Paimon 表 | `FlussUnionTableProvider` |
| 非 lake 主键表 | `FlussKvTableProvider` |
| 非 lake append/log 表 | `FlussLogTableProvider` |

这个优先级非常关键：lake-enabled 的 PK 表绝不能直接走非 lake KV provider，
因为 Fluss-only KV 读取会漏掉已经 tier 到 lake 里的数据。

#### `<table>$options`

`<table>$options` 是一个正式的、用户可见的 introspection surface。
它会暴露：

- table id；
- schema id；
- primary keys；
- bucket keys；
- partition keys；
- datalake enablement 与 format；
- lake catalog properties。

其中 secret-like 值在暴露前会被 redaction。

#### `<table>$lake`

`<table>$lake` 是正式的 lake-only read 形态。
其语义是：

- 只读取 Paimon lake snapshot；
- 不再拼接 residual Fluss log tail；
- 即使是主键表，也跳过 KV fast-path routing。

### 5. 查询路由与 planner 行为

这是模块契约里最核心的部分。

#### 5.1 非 lake KV 表

`FlussKvTableProvider` 当前支持如下优先级：

| 查询形态 | 路径 | 说明 |
| --- | --- | --- |
| 完整 primary-key equality | `FlussKvLookupExec` | 精确点查 |
| 完整 bucket-key prefix equality | `FlussKvPrefixLookupExec` | 精确前缀查 |
| 其它任意形态 + `LIMIT` | `FlussKvScanExec` | 对计算出的 scan target 做有界扫描 |
| 其它任意形态且无 `LIMIT` | unsupported | 非 lake KV 不暴露 full current-state scan |

当前语义要点：

- 完整 primary-key equality 指每个 PK 列都被等值绑定；
- 完整 bucket-key prefix equality 指每个必需的 prefix 列都被绑定，分区表时还要包含分区键；
- 不支持的形态会保守失败，而不是悄悄退化成全表扫描。

#### 5.2 非 lake log 表

`FlussLogTableProvider` 暴露的是有限 snapshot scan。

| 查询形态 | 路径 | 说明 |
| --- | --- | --- |
| 存在 `LIMIT n` | `FlussLogScanExec` | 从 earliest offset 开始的 head-first first-N |
| 不带 `LIMIT` | `FlussLogScanExec` | 查询启动时刻冻结边界的完整 earliest-to-latest snapshot |

当前语义要点：

- 在当前实现里，`LIMIT` 已经是可选的；
- projection 会下推到 source scan；
- 分区等值可以裁剪 partition，但 pushdown 为 `Inexact`，scan 之上仍保留 residual `FilterExec`；
- 不承诺跨 bucket 的全局顺序。

#### 5.3 Lake-enabled 表

lake-enabled 表统一先路由到 `FlussUnionTableProvider`，它本质上是
`fluss-lake` 之上的 thin adapter。

当前优先级为：

| 查询形态 | 路径 | 说明 |
| --- | --- | --- |
| PK 表 + 完整 primary-key equality | `FlussKvLookupExec` | 直接从 Fluss current state 服务 |
| PK 表 + 完整 bucket-key prefix equality | `FlussKvPrefixLookupExec` | 直接从 Fluss current state 服务 |
| `<table>$lake` | lake-only `fluss-lake` 路径 | 不包含 residual log tail |
| 其余任何 lake-enabled 读 | `FlussUnionScanExec` | 统一的 lake snapshot + residual Fluss tail 读取 |

当前行为要点：

- 仅在 `!lake_only` 且表存在主键时才启用 KV fast path；
- 普通 union path 当前还不向 kernel 下推 value filter；
- residual filter 继续由 DataFusion 上层负责；
- `num_buckets == 0` 会直接返回 `EmptyExec`。

### 6. Filter pushdown 契约

filter pushdown 的整体策略是**保守且显式**的。

#### KV provider

`FlussKvTableProvider::supports_filters_pushdown(...)` 是 precedence-aware 且
sound 的：

- 只有真正会进入 point lookup / prefix lookup 的查询才标 `Exact`；
- partition equality 在 scan 路径上最多标 `Inexact`；
- 其余一律 `Unsupported`。

这样可以避免不正确的情况：DataFusion 误以为 source 已经消费了某个 filter，
从而把 `FilterExec` 去掉。

#### Log provider

`FlussLogTableProvider` 的规则是：

- 非分区表所有 filter 都是 `Unsupported`；
- 分区表的 partition equality 为 `Inexact`；
- 其余情况都是 `Unsupported`。

#### Lake provider

`FlussUnionTableProvider` 的规则是：

- 只有 KV fast-path 谓词可标 `Exact`；
- 正常 union path 下，所有 filter 仍视为 `Unsupported`。

因此当前 unified lake read 的正确性仍依赖 DataFusion residual filtering。

### 7. 执行层行为

当前 physical execution 层主要包含四类计划。

#### `FlussKvLookupExec`

- 单次 lookup；
- 返回 0 或 1 行；
- fetch 后再做 projection trim。

#### `FlussKvPrefixLookupExec`

- 基于 Fluss KV current state 的前缀查；
- 返回所有命中的行。

#### `FlussLogScanExec`

- 每个 `(partition_id, bucket)` target 对应一个 DataFusion partition；
- 被 log snapshot scan 与 bounded KV scan 共同复用；
- 在 log 模式下读取 earliest-to-latest 或 first-N；
- 在 KV bounded-scan 模式下读取每 bucket 的有界扫描路径。

#### `FlussUnionScanExec`

- 每个 `FlussLakeReadSplit` 对应一个 DataFusion partition；
- 包装 opaque 的 `FlussLakeReadPlan` 与 `FlussLakeRead`；
- 自身不理解 seam、physical lake split、append stitch 或 PK merge 细节。

### 8. 与 `fluss-lake` 的边界

推荐固定的职责分工如下：

| 层 | 职责 |
| --- | --- |
| `fluss-rust` | metadata、KV read、log read、Arrow helper |
| `fluss-lake` | seam resolution、snapshot planning、append stitch、PK merge、logical split |
| `fluss-datafusion` | predicate recognition、route selection、DataFusion wrapping、config injection |

因此，DataFusion 模块应该始终保持为 `fluss-lake` 之上的 **thin adapter**。

这在当前代码中已经大体成立：

- `src/table/union.rs` 负责构造 `FlussLakeTable` 与 `FlussLakeScan`；
- 由它调用 kernel 的 `plan()` 与 `new_read()`；
- `src/execution/union_scan.rs` 执行 public `FlussLakeReadSplit`；
- connector 自身不解释 seam DTO，也不做 PK merge。

### 9. 配置契约

当前 installer 层面真正有意义的配置项是：

- `FlussDatafusionOptions.lake_storage_options`

它存在的原因是：服务端 metadata 中可能不会保留完整的 lake storage
credentials，因此 adapter 需要在打开 lake catalog 之前做 caller-side
reinjection。

合并规则为：

- 服务端 `table.datalake.*` properties 作为基础配置；
- 调用方传入的 `lake_storage_options` 合并覆盖到其上；
- 冲突时 **caller wins**。

这部分是 adapter 层的 transport/configuration glue，而不是 lake 语义本身。

`RegisterCatalogOptions` 当前仍然只是预留，没有承载实际行为。

### 10. 错误模型

当前 crate 的本地错误类型是 `FlussDatafusionError`，主要包括：

- `DatabaseNotFound`
- `TableNotFound`
- `UnsupportedQueryPattern`
- `LimitRequired`
- `SchemaMismatch`
- `TypeConversion`
- `FlussClient`
- `Internal`

这里的设计原则是：

- 对不支持的 SQL 形态保守失败；
- 对 storage/client 错误保持显式；
- 清晰桥接到 `DataFusionError::External`。

需要说明的是：尽管 log scan 已不再要求 `LIMIT`，`LimitRequired` 仍保留在
错误类型中，作为整个 crate 错误模型的一部分。

### 11. 当前限制与收敛项

当前实现已经可用，但仍有几处明显的 transitional 状态。

#### 11.1 过渡命名

当前仍使用 `Union` 命名：

- `FlussUnionTableProvider`
- `FlussUnionScanExec`

但从语义上看，它们已经是 lake adapter 路径。未来改名为 `FlussLake*`
会更清晰，且不应改变行为。

#### 11.2 Lake 路径还没有接上 limit pushdown

`FlussUnionTableProvider::scan(...)` 当前会忽略 SQL `LIMIT` 在 kernel 路径上的
传递，因此 DataFusion 只能在 lake scan 之上再做 limit。
这在语义上正确，但可能造成 over-read。

#### 11.3 Lake 路径的 filter pushdown 仍然刻意收窄

普通 union path 还没有 value-filter pushdown；
residual filtering 继续由 DataFusion 负责。

#### 11.4 Lake 路径仍依赖 `RealFlussSource` 下转型

当前 lake path 需要 downcast 到 `RealFlussSource`，以拿到底层生产连接与
caller-supplied lake storage options。这是当前的务实边界，也说明 lake 路径
尚未完全被更广义的 source trait 抽象掉。

#### 11.5 Packaging 仍依赖 feature gating

Lake 支持位于 `lake` feature 下。这个边界在当前是合理的，但也意味着文档必须
明确说明 feature，而不能默认暗示 unified lake read 永远可用。

#### 11.6 上游 `fluss-lake` 能力缺口会透传到这里

DataFusion adapter 会继承当前 `fluss-lake` 的限制，例如：

- partitioned lake read 仍在收敛中；
- historical snapshot 还不是主要受支持路径；
- kernel 侧 statistics、limit/predicate pushdown 仍然较薄。

## Compatibility, Deprecation, and Migration Plan

这个设计不会要求 Fluss 服务端 wire protocol 或 storage format 发生变化。

兼容性说明：

- 现有 Fluss 集群无需服务端改造即可使用该 crate；
- 对采用它的调用方来说，这是一个附加能力；
- lake 行为是 additive 且 feature-gated 的；
- `<table>$lake` 是对已存在行为的正式文档化，而不是引入新的不兼容模式。

迁移方面：

- 不需要数据迁移；
- 不需要 SQL migration tooling；
- 未来若从 `FlussUnion*` 改名为 `FlussLake*`，应与 `EXPLAIN` 断言和相关文档同步推进。

## Test Plan

测试策略刻意以**真实行为**为中心，而不只是 mock 单测。

### Unit tests

当前单测主要覆盖：

- predicate recognition 与 pushdown classification；
- projection normalization；
- scalar 到 key 的类型转换；
- metadata property merge 与 redaction 行为。

### 基于真实 Fluss 集群的集成测试

当前主集成测试会验证：

- catalog listing 与 `information_schema` 可见性；
- 无需重新注册的 live post-DDL metadata visibility；
- KV point lookup 与 prefix lookup 行为；
- 非 lake KV 路径上的 unsupported-query failure；
- bounded 与 full snapshot 的 log 读取；
- partitioned log / KV scan 的分区裁剪；
- connection hot-swap；
- injected-seam 与 real-tiering 两条 lake-enabled unified read 路径；
- `$options` 与 `$lake` 行为；
- `EXPLAIN` 是否展示预期的自定义 plan。

### 验证命令

```bash
cargo check --workspace
cargo test -p fluss-datafusion
cargo test -p fluss-datafusion --features integration_tests
```

## Rejected Alternatives

### 1. 把 `fluss-datafusion` 做成完整 gateway backend abstraction

拒绝原因：这样会把 protocol/auth/session 语义带进 engine adapter 层，降低该 crate
独立复用的价值。

### 2. 在 DataFusion 层重做一套 lake+log union 语义

拒绝原因：这些语义应当归属 `fluss-lake`，并被不同 Arrow engine 复用。

### 3. 用 catalog cache 隐藏 metadata 访问成本

拒绝原因：在当前阶段，同 session 内 DDL 立即可见的契约比减少 admin RPC 更重要。

### 4. 让所有 lake-enabled PK 读都统一走 `fluss-lake`

拒绝原因：完整 PK 与可判定的 prefix 查询可以更高效地直接从 Fluss current state 服务。

### 5. 把不支持的谓词悄悄退化成 full scan

拒绝原因：正确性与可预期行为比“看起来什么 SQL 都支持”更重要。

## Future Work and Open Questions

后续值得继续推进的方向包括：

1. 将 `FlussUnion*` 内部命名收敛为 `FlussLake*`；
2. 给 lake scan path 接上 limit 传递；
3. 仅在可证明 sound 的前提下逐步扩展 pushdown；
4. 减少 lake 读取路径上对 `RealFlussSource` 特定下转型的依赖；
5. 随着 `fluss-lake` 在 partitioned read 与 richer statistics 上成熟，继续做边界收敛；
6. 即使未来上层有 Fluss gateway，这个 crate 也继续保持 library-scoped 的 adapter 定位。

这里最关键的不变约束仍然是：`fluss-datafusion` 应持续作为 Fluss 的 DataFusion
adapter，而不是 lake storage model 的语义所有者。
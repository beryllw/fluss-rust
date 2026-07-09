# FIP-XX: Apache Fluss DataFusion 集成模块

| 字段 | 内容 |
|------|------|
| **作者** | TBD |
| **讨论线程** | TBD |
| **投票线程** | TBD |
| **ISSUE** | TBD |
| **发布版本** | TBD |

请将讨论放在邮件列表中，而不是直接在文档页面上评论。

## 1. 动机

Apache Fluss 已经提供了面向 KV 表、日志表和 lake-enabled 表的客户端读取能力，但对 Rust 生态中的 SQL 使用者而言，仍缺少一个正式、稳定、可复用的 DataFusion 集成层。

`fluss-datafusion` 的目标不是再做一个查询网关，而是通过 DataFusion 的原生概念把 Fluss 表正式暴露出来，使调用方可以直接通过 `SessionContext` 和 `ctx.sql(...)` 查询 Fluss，而不需要自行实现：

- catalog / schema / table 注册；
- Fluss schema 到 Arrow schema 的映射；
- 主键点查与前缀查的谓词识别；
- 日志表有限快照扫描；
- lake-enabled 表的统一读取入口。

该模块必须同时覆盖三类读取语义：

1. **非 lake KV 表**
   - 完整主键等值的点查；
   - 完整 bucket-key prefix equality 的前缀查；
   - 带 `LIMIT` 的有界扫描。
2. **非 lake Log 表**
   - earliest 到 latest 的有限 snapshot scan；
   - 可选 `LIMIT` 的 head-first first-N 读取。
3. **lake-enabled 表**
   - 通过普通表名 `<table>` 的 unified read；
   - 通过 `<table>$lake` 的 lake-only read；
   - 对符合条件的主键查询保留直接的 KV fast path。

本 FIP 的核心目标，是把 `fluss-datafusion` 固定为一个**正式、稳定的 DataFusion adapter**：

- 它是 library，不是协议服务器；
- 它负责 DataFusion 集成，不负责 session / auth / protocol；
- 它复用 `fluss-rust` 与 `fluss-lake`，而不重复实现 lake 读取语义；
- 它采用 live metadata 模型，而不是内部 metadata cache。

## 2. 公共接口

本 FIP 定义并稳定以下公共接口与用户可见契约。

### 2.1 Rust API

公开类型：

- `FlussDatafusion`
- `FlussDatafusionOptions`
- `RegisterCatalogOptions`
- `FlussDatafusionError`
- `Result`

其中最关键的公开接口可以用如下 Rust 形态表达：

```rust
pub struct FlussDatafusionOptions {
    pub lake_storage_options: std::collections::HashMap<String, String>,
}

pub struct RegisterCatalogOptions {}

pub struct FlussDatafusion;

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

其正式语义为：

- `new(...)`：基于一个 live 的 `FlussConnection` 创建共享 installer；
- `register_catalog(...)`：将 Fluss catalog 安装到一个 `SessionContext`；
- `swap_connection(...)`：在不重新注册 catalog 的前提下热切换底层连接；
- `FlussDatafusionOptions`：承载 installer 级选项；当前主要用于 lake storage 参数注入；
- `RegisterCatalogOptions`：保留为 catalog 安装时的扩展配置入口。

### 2.2 SQL 可见接口

本模块对 SQL 使用者暴露以下正式接口：

- 表路径：`<catalog>.<database>.<table>`；
- metadata table：`<table>$options`；
- lake-only read：`<table>$lake`；
- 通过 `EXPLAIN` 可见的自定义物理计划。

关键 SQL 接口内容如下：

1. **普通表路径**
   - 形态：`<catalog>.<database>.<table>`
   - 示例：`fluss.sales.orders`

2. **`<table>$options`**
   - 形态：`<catalog>.<database>.<table>$options`
   - 输出 schema：

```text
key: Utf8
value: Utf8
```

   - 典型内容包括：`table.id`、`schema.id`、`num.buckets`、`primary.keys`、`bucket.keys`、`partition.keys`、`datalake.enabled`、`datalake.format` 以及脱敏后的 datalake properties。

3. **`<table>$lake`**
   - 形态：`<catalog>.<database>.<table>$lake`
   - 语义：只读 lake snapshot，不拼接 residual Fluss log tail。
   - 说明：这是 lake-only 视图，不包含尚未 tiering 到 lake 的 Fluss log tail，因此对主键表可能落后于普通路径 `<table>` 的统一视图；需要完整 current view 时应使用 `<table>`。

4. **`EXPLAIN` 可见计划**
   - 当前正式可见的计划名称包括：
     - `FlussKvLookupExec`
     - `FlussKvPrefixLookupExec`
     - `FlussKvScanExec`
     - `FlussLogScanExec`
     - `FlussLakeScanExec`

### 2.3 配置接口

当前正式配置项为：

- `FlussDatafusionOptions.lake_storage_options`

其接口内容如下：

```rust
pub struct FlussDatafusionOptions {
    pub lake_storage_options: std::collections::HashMap<String, String>,
}
```

该配置用于在打开 lake catalog 前补充或覆盖 lake storage 连接参数，例如：

- `s3.endpoint`
- `s3.access-key`
- `s3.secret-key`
- `s3.region`

合并规则为 **caller wins**，即：

- server-derived `table.datalake.*` properties 作为基础配置；
- caller-supplied `lake_storage_options` 合并覆盖到其上；
- 相同 key 冲突时，以调用方传入值为准。

该配置的作用域为 **connection / 集群级**：一个 `FlussDatafusion` installer 绑定单个 `FlussConnection`（即单个 Fluss 集群），`lake_storage_options` 对该 installer 上 `register_catalog(...)` 注册的所有 catalog 一致生效——因为 lake storage 绑定的是集群的 lake warehouse，而 `catalog_name` 只是 DataFusion 内的命名空间别名。因此本 FIP 不在 catalog 级重复承载 storage 配置：`RegisterCatalogOptions` 保留为空，仅作未来 catalog 级扩展的占位。相应地，`swap_connection(...)` 只热切换底层连接、不改变 `lake_storage_options`；它面向同一集群的连接刷新，而非切换到使用不同 lake storage 凭证的集群。

### 2.4 DataFusion 集成接口

除了公开 Rust API，本模块还通过 DataFusion 的标准扩展点暴露正式集成契约：

- `CatalogProvider`
- `SchemaProvider`
- `TableProvider`
- `ExecutionPlan`

这些接口在本模块中的职责分别是：

- `FlussCatalogProvider`：暴露 database 级目录；
- `FlussSchemaProvider`：暴露 table listing 与 table resolution；
- `FlussKvTableProvider` / `FlussLogTableProvider` / `FlussLakeTableProvider`：根据表语义暴露读取路径；
- 自定义 `ExecutionPlan`：把底层读取适配成 DataFusion 可执行的物理计划。

其中 `TableProvider` 在本模块中的关键接口内容可以概括为：

```rust
trait TableProvider {
    fn schema(&self) -> SchemaRef;
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>>;
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}
```

其正式语义是：

- `schema()`：暴露该表在 DataFusion 中的 Arrow schema；
- `supports_filters_pushdown(...)`：显式声明哪些谓词可以被精确下推、近似下推或完全不下推；
- `scan(...)`：根据 projection / filters / limit 生成最终物理读取计划。

## 3. 拟议变更

### 3.1 模块定位与边界

`fluss-datafusion` 被定义为 Fluss 的 **DataFusion 集成模块**。它位于 `fluss-rust` 和 `fluss-lake` 之上，负责把 Fluss 表接入 DataFusion，而不拥有更高层的协议语义。

其职责包括：

- 注册 Fluss catalog；
- 按需加载 metadata 与 Arrow schema；
- 识别受支持的 SQL 谓词形态；
- 将查询路由到 KV fast path、有界扫描、有限日志扫描或 lake read path；
- 将底层读取结果适配为 DataFusion `ExecutionPlan` 与 `RecordBatchStream`。

其非目标包括：

- PostgreSQL / Flight SQL / REST / gRPC 协议适配；
- session、鉴权、租户、多集群路由；
- SQL DML / DDL 执行；
- 重新实现 lake seam、append stitch、PK merge 逻辑；
- 将自己扩展成 gateway 本体。

### 3.2 总体架构

```text
+-------------------------+
| SessionContext          |
+------------+------------+
             |
             v
+-------------------------+
| FlussDatafusion         |
| register_catalog(...)   |
+------------+------------+
             |
             v
+-------------------------+
| FlussCatalogProvider    |
| FlussSchemaProvider     |
+------------+------------+
             |
             v
+-------------------------+
| KV / Log / Lake Tables  |
| Predicate Routing       |
+------------+------------+
             |
             v
+-------------------------+
| Custom Execution Plans  |
| RecordBatch Streams     |
+------------+------------+
             |
             v
+-------------------------+
| fluss-rust              |
| fluss-lake              |
+-------------------------+
```

该架构表达了三个正式原则：

1. **每个 SQL session 有自己的 `SessionContext`。**
2. **一个 `FlussDatafusion` installer 可被多个 session 复用。**
3. **底层表语义仍归属 `fluss-rust` 与 `fluss-lake`，DataFusion 模块只做引擎适配。**

### 3.3 生命周期与 metadata 可见性

正式使用模型为：

1. 创建一个共享的 `FlussDatafusion`；
2. 每个 SQL session 创建自己的 `SessionContext`；
3. 调用 `register_catalog(...)` 安装 catalog；
4. 之后使用 `ctx.sql(...)` 查询 Fluss 表。

本 FIP 固定以下 metadata 契约：

- catalog 为 **live catalog**；
- `schema_names()`、`table_names()`、`table()` 等调用均实时回源 Fluss；
- 在 `register_catalog(...)` 之后发生的 DDL，对同一 `SessionContext` 立即可见；
- 模块内部不引入 metadata cache。

### 3.4 Catalog / Schema / Table 映射

本模块将 Fluss metadata 映射为 DataFusion catalog 模型：

- 一个 DataFusion catalog 下包含多个 Fluss database；
- 一个 Fluss database 对应一个 `SchemaProvider`；
- 一张 Fluss table 对应一个 `TableProvider`。

关键 provider 接口内容可以概括为：

```rust
trait CatalogProvider {
    fn schema_names(&self) -> Vec<String>;
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>>;
}

trait SchemaProvider {
    fn table_names(&self) -> Vec<String>;
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>>;
}
```

在本模块中的正式语义是：

- `schema_names()`：实时列出 Fluss databases；
- `schema(name)`：把一个 Fluss database 解析为对应的 `SchemaProvider`；
- `table_names()`：实时列出该 database 下的 tables；
- `table(name)`：根据 metadata 动态路由到 KV / Log / Lake 对应的 `TableProvider`。

表解析遵循以下正式规则：

| 输入名称 | 路由结果 |
|---|---|
| `<table>$options` | metadata key/value table |
| `<table>$lake` | lake-only table provider |
| 普通 lake-enabled 表 | lake read provider |
| 普通非 lake KV 表 | KV table provider |
| 普通非 lake Log 表 | Log table provider |

这意味着：

- `<table>$options` 是正式的 introspection surface；
- `<table>$lake` 是正式的 lake-only read shape，而不是临时调试入口；
- lake-enabled 主键表必须优先进入 lake 路由，再在其内部决定是否命中 KV fast path。

### 3.5 查询路由矩阵

#### 3.5.1 非 lake KV 表

非 lake KV 表支持如下路由矩阵：

| 查询形态 | 路由 |
|---|---|
| 完整 primary-key equality | 点查 |
| 完整 bucket-key prefix equality | 前缀查 |
| 其它形态 + `LIMIT` | 有界 KV scan |
| 其它形态且不带 `LIMIT` | 显式拒绝 |

这一定义表达了正式产品语义：非 lake KV 表不承诺任意 SQL 形态的 current-state full scan，而是对可证明正确且行为清晰的查询形态提供明确支持。

#### 3.5.2 非 lake Log 表

非 lake Log 表支持如下路由：

| 查询形态 | 路由 |
|---|---|
| `LIMIT n` | first-N finite snapshot scan |
| 不带 `LIMIT` | earliest-to-latest finite snapshot scan |

该路径支持：

- projection pushdown；
- 分区等值裁剪；
- 多 bucket 并行扫描。

该路径不承诺（仅指非 lake Log 表；lake 表的 value predicate 下推见 §3.5.3 / §3.8）：

- 跨 bucket 全局顺序；
- 通用 value predicate pushdown；
- offset pseudo-column 语义。

#### 3.5.3 Lake-enabled 表

lake-enabled 表正式支持两类用户可见读形态：

1. `<table>`：统一视图（unified view）
2. `<table>$lake`：lake-only 视图

统一视图的定义是：

- append/log 表：lake snapshot + residual Fluss log tail；
- primary-key 表：对调用方呈现 current-view 语义。

对于 lake-enabled primary-key 表，以下查询形态允许由 adapter 直接命中 KV fast path：

- 完整 primary-key equality；
- 完整 bucket-key prefix equality。

但这只是执行优化，不改变 `<table>` 的正式表级语义：对于调用方，`<table>` 仍表示统一后的可读视图。

此外，lake-enabled 表（append/log 与 primary-key）支持**通用 value predicate 的精确下推**：adapter 将可翻译的 DataFusion 谓词转换为 `fluss-lake` 的统一 predicate 并下推；kernel 在 plan 期用它做分区裁剪，并在输出前对每个 RecordBatch 做精确过滤，因此下推是 `Exact` 的、无需上层 residual `FilterExec`（详见 §3.8）。这一能力为 lake 表专属；非 lake KV / Log 表不提供通用 value predicate pushdown。

分区支持：lake-enabled 表已支持基于分区谓词的分区裁剪；`FlussLakeReadSplit` 携带 `FlussLakePartitionIdentity`，unified read 路径对分区表与非分区表共享同一公共形态。

### 3.6 物理执行契约

本模块通过自定义 DataFusion physical plan 暴露正式的执行路径，包括：

- KV point lookup plan；
- KV prefix lookup plan；
- KV bounded scan plan；
- Log snapshot scan plan；
- Lake read plan。

本 FIP 固定以下执行规则：

- 一个 KV/log `(partition, bucket)` 目标映射为一个 DataFusion partition；
- 一个 `FlussLakeReadSplit` 映射为一个 DataFusion partition；
- 上游 source 已完成 projection 时，不再重复投影；
- residual filter、limit、join、aggregate 等算子仍由 DataFusion 上层负责。

### 3.7 与 `fluss-lake` 的正式边界

本 FIP 明确规定 `fluss-datafusion` 与 `fluss-lake` 的职责边界：

| 模块 | 职责 |
|---|---|
| `fluss-rust` | metadata、KV read、log read、Arrow helper |
| `fluss-lake` | seam resolution、snapshot planning、append stitch、PK merge、logical split |
| `fluss-datafusion` | predicate recognition、route selection、DataFusion wrapping、config injection |

因此，`fluss-datafusion` 不应重复承载：

- seam 解释；
- Paimon split planning；
- append stitch；
- PK merge。

这些语义在正式设计中统一归属于 `fluss-lake`。

### 3.8 Filter pushdown 契约

filter pushdown 的正式策略是：**只要下推可被证明 sound，就精确下推；无法保证的形态一律留给 DataFusion 上层 residual `FilterExec`**。

具体规则：

- 完整 PK equality（KV / lake-enabled PK 表）：`Exact` 下推，由点查消费；
- 完整 bucket-key prefix equality：`Exact` 下推，由前缀查消费；
- 非 lake Log / KV 表的分区等值：作为 `Inexact` pushdown 驱动分区裁剪，residual `FilterExec` 仍会重放；
- **lake-enabled 表的通用 value predicate**：adapter 将可翻译的谓词转换为 `fluss-lake` 统一 predicate 下推；kernel 用它在 plan 期做分区裁剪、并在输出前对每个 RecordBatch 精确过滤，故这些谓词标记为 `Exact`、无残留；无法翻译的谓词保持 `Unsupported`，由上层 residual `FilterExec` 处理。

pushdown 分类与 `scan` 实际消费严格一致：被标记 `Exact` 的谓词一定会被对应执行路径精确应用；不被消费的谓词绝不标记 `Exact`。这一定义优先保证正确性，同时在 kernel 提供 exact filter 的前提下尽可能减少上层 residual 计算，而不是制造“看起来支持更多 SQL”的假象。

### 3.9 错误模型

公开错误类型 `FlussDatafusionError` 继续作为模块级稳定错误模型，覆盖：

- metadata not found；
- unsupported query pattern；
- schema/type conversion mismatch；
- Fluss client failure；
- internal error。

所有错误均通过 DataFusion 标准路径冒泡，不引入 gateway 协议相关错误语义。

## 4. 兼容性、弃用与迁移计划

本 FIP 定义的是一个新增并稳定化的模块设计，对现有 Fluss 服务端协议与存储格式没有破坏性影响。

兼容性影响：

- **Fluss 服务端**：无需修改；
- **Fluss 存储格式**：无需修改；
- **现有客户端**：无需迁移；
- **lake-enabled 表**：继续依赖 `fluss-lake` 的统一读语义；
- **调用方**：按需接入该 crate，不影响未使用它的用户。

迁移计划：

- 不需要数据迁移；
- 不需要 schema migration；
- 不需要兼容层转换工具。

## 5. 测试计划

本 FIP 关注系统级语义验证，而不是仅关注局部单测。

### 5.1 单元测试

应覆盖：

- predicate recognition；
- pushdown classification；
- DataFusion `Expr` 到 `fluss-lake` 统一 predicate 的翻译（可翻译形态与无法翻译时的回退）；
- projection normalization；
- Fluss -> Arrow 类型转换；
- metadata property redaction；
- lake storage option 合并语义。

### 5.2 真实集群集成测试

应覆盖：

- catalog listing 与 `information_schema` 暴露；
- DDL 在同一 session 内立即可见；
- KV point lookup 与 prefix lookup；
- 非法 KV 查询的保守失败；
- Log 表的带 `LIMIT` 与不带 `LIMIT` snapshot read；
- partition pruning；
- connection swap 后 catalog 持续可用。

### 5.3 Lake-enabled 端到端测试

应覆盖：

- `<table>` 的 unified read；
- `<table>$lake` 的 lake-only read；
- lake-enabled 主键表的 fast path；
- lake-enabled 表的 value predicate 精确下推（含分区裁剪）与 residual-free 结果；
- caller-supplied storage options 对 lake catalog 打开的支持；
- 基于真实 tiering 的全链路验证。

### 5.4 验证命令

```bash
cargo check --workspace
cargo test -p fluss-datafusion
cargo test -p fluss-datafusion --features integration_tests
```

## 6. 被拒绝的替代方案

### 6.1 把 `fluss-datafusion` 直接做成 query gateway

拒绝原因：协议、鉴权与 session 语义并不属于 engine adapter 这一层。将两者混在一起会削弱模块复用性与职责边界。

### 6.2 在 DataFusion 模块内部重新实现 lake+log 统一读语义

拒绝原因：这会重复 `fluss-lake` 的职责，并使 lake read contract 难以被其他 Arrow engine 复用。

### 6.3 使用 metadata cache 提升 catalog 性能

拒绝原因：本模块优先保证 live metadata 语义。同一 session 内的即时 DDL 可见性，比减少 catalog RPC 更重要。

### 6.4 让所有 lake-enabled 主键查询统一经过 lake kernel

拒绝原因：完整 PK equality 和完整 prefix equality 可以更高效地直接由 Fluss current state 服务，且不会改变最终 unified read 语义。

正确性论证：Fluss KV 点查 / 前缀查读取的是 KV store 的**实时快照**，KV store 持有该表的完整 current state，不会因数据 tiering 到 lake 而丢失任何 key。因此对完整 PK / prefix equality 走 KV fast path 与经 lake kernel 的 unified read 得到相同结果，绕过 kernel 只是执行优化，不牺牲正确性。反之，full scan / `LIMIT` 等形态需要读取 changelog 才能重建 current state，必须交由 kernel 的 lake+log 统一读处理。

### 6.5 对不支持的查询形态静默退化成 full scan

拒绝原因：这样会制造误导性的 SQL 能力假象，并削弱正确性与性能边界。本模块选择显式报错。
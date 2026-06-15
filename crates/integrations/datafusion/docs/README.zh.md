# fluss-datafusion

> English version: [README.md](./README.md).

`fluss-datafusion` 是一个**无状态**的 DataFusion 集成 crate，职责单一：把 `SQL` 转换成对
Apache Fluss 表的访问。它把 Fluss 的 KV 表与 Log 表暴露成 DataFusion 的
`CatalogProvider` / `SchemaProvider` / `TableProvider`，并为受支持的查询形态提供自定义
`ExecutionPlan`，让你直接用 `ctx.sql(...)` 查询 Fluss。

> 本 crate 不感知调用方身份、协议、会话变量或鉴权模式，也不知道自己是否运行在 gateway 内。
> 依赖方向是单向的：`fluss client/core -> fluss-datafusion`，绝不反向。

## 当前能力（Phase 1）

- 列举 database / table，获取表的 schema 与表类型
- **KV 表**：完整主键等值谓词下推为点查（point lookup），支持单主键与复合主键
- **Log 表**：带 `LIMIT` 的有界扫描（bounded scan），支持投影下推与多 bucket 表（一个 bucket
  对应一个并行 partition；per-bucket last-N，再施加跨 bucket 的最终 `LIMIT`；不保证跨 bucket 顺序）
- **分区裁剪（partition pruning，仅等值）**：分区 Log 表上，凡是提供了分区列等值谓词，就会按已提供的
  分区键绑定值把扫描裁剪到匹配分区；不带分区谓词时扫描所有分区（裁剪是可选优化，绝非必需）。分区 KV
  表通过既有的完整主键点查解析，点查本身已定位到唯一所属分区
- Fluss schema 到 Arrow schema、Fluss row 到 `RecordBatch` 的转换
- 共享 installer + 每会话 `register_catalog(...)` 的使用模型
- 不支持的查询形态会**保守失败**（明确报错），而不会静默退化成误导性的全表扫描

Phase 1 **不包含**：PostgreSQL / MySQL 兼容对象、REST/gRPC 接口、会话或操作生命周期、
principal 路由、多集群、SQL 写入（DML）、gateway 鉴权 / 审计。

## 使用方式

整体是「一个共享 installer + 每个 SQL 会话各自安装」的模型：

1. 用一个 `FlussConnection` 构造一个共享的 `FlussDatafusion`（无状态，不持有元数据缓存）。
2. 每个 SQL 会话创建自己的 `SessionContext`。
3. 会话通过 `register_catalog(...)` 把 Fluss catalog 装入该 context。
4. 之后照常 `ctx.sql(...)`。

```rust,no_run
use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use fluss::client::FlussConnection;
use fluss::config::Config;

use fluss_datafusion::{FlussDatafusion, FlussDatafusionOptions, RegisterCatalogOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1) 连接 Fluss。
    let mut config = Config::default();
    config.bootstrap_servers = "127.0.0.1:9123".to_string();
    let connection = Arc::new(FlussConnection::new(config).await?);

    // 2) 构造共享 installer（一次构造，跨会话复用；元数据始终是实时的）。
    let fd = FlussDatafusion::new(connection, FlussDatafusionOptions::default()).await?;

    // 3) 每个会话各自创建 context 并安装 catalog。
    let ctx = SessionContext::new();
    fd.register_catalog(&ctx, "fluss", RegisterCatalogOptions::default())
        .await?;

    // 4) 正常查询。表名形如 <catalog>.<database>.<table>。
    let df = ctx
        .sql("SELECT id, name, age FROM fluss.my_db.users WHERE id = 2")
        .await?;
    df.show().await?;

    Ok(())
}
```

`FlussDatafusion` 是 `Arc` 共享、无状态的：构造一次即可被多个 `SessionContext` 复用。
catalog 完全实时——不持有任何 database/table 列表缓存，因此每次列举/取表都会回源 Fluss，
DDL 在同一会话内即时可见。

## 受支持的查询形态

| 表类型 | 受支持的形态 | 说明 |
|---|---|---|
| KV（主键表） | `WHERE <完整主键> = <值>`（复合主键用 `AND` 全列等值） | 下推为点查，命中返回 1 行，未命中返回 0 行（不报错）。分区 KV 表的分区键是主键的一部分，点查会自动解析到所属分区 |
| Log（日志表） | `... LIMIT n` | 有界扫描；投影下推；多 bucket（一个 bucket 对应一个并行 partition）；每个 bucket 各保留**末尾** `n` 行（与 Fluss `LimitBatchScanner` 语义一致），再施加跨 bucket 的最终 `LIMIT`；不保证跨 bucket 顺序 |
| 分区 Log 表 | `WHERE partition_col = 'v' ... LIMIT n` | 仅等值的分区裁剪：扫描仅命中匹配的分区（保留分区与 bucket 的笛卡尔积）；不带分区谓词则扫描全部分区。下推为 `Inexact`，因此其上仍会叠加一个 `FilterExec` 复核谓词 |

### 会保守失败的形态

下面这些会在**计划阶段**直接报错，而不会退化成全表扫描：

| 查询 | 报错 |
|---|---|
| KV 表用非主键列做谓词，如 `WHERE name = 'x'` | `unsupported query pattern: ...` |
| Log 表不带 `LIMIT`，如 `SELECT * FROM log_t` | `LIMIT required: ...` |

可以用 `EXPLAIN` 确认走的是自定义执行计划：KV 点查显示 `FlussKvLookupExec`，
Log 有界扫描显示 `FlussLogScanExec`。

## 配置

`FlussDatafusionOptions` 与 `RegisterCatalogOptions` 目前都是空占位，为后续选项预留。
catalog 完全实时、无缓存可调，因此当前没有需要配置的项。

## 元数据可见性

catalog **完全实时、零快照、无缓存**：每次列举/取表调用（`schema_names()`、`schema()`、
`table_names()`、`table_exist()`、`table()`）都从 Fluss 现拉。这与 SQL catalog 语义一致——
DDL 在同一 `SessionContext` 内即时可见：

- `register_catalog` **之后**新建的表，在同一会话内即可查询，无需重新注册。
- 被 drop 的表会立刻从该会话的列表与 `table()` 解析中消失。
- 表的 schema/meta 每次访问都重读，因此 schema 变更即时反映。

DataFusion 的 `CatalogProvider` / `SchemaProvider` 列举方法是**同步**的，而 Fluss 的元数据
接口是 async 的。为保持实时，crate 在二者之间做桥接：同步回调通过一个小的 `block_on` 助手
（`src/runtime.rs`）把 async 源调用跑到完成；当已处于 tokio runtime 内时，会临时起一个短命
线程以规避嵌套 runtime 的 panic。已知且接受的代价是**每次 catalog 调用一次 admin RPC**（同步
路径还多一次线程跳转）。本阶段刻意如此，不在其上叠加任何缓存。

## 错误模型

公开错误类型 `FlussDatafusionError`（别名 `Result<T>`），明确区分各类不支持/失败：
`DatabaseNotFound` / `TableNotFound` / `UnsupportedQueryPattern` / `LimitRequired` /
`SchemaMismatch` / `TypeConversion` / `FlussClient` / `Internal`。
它会被桥接成 DataFusion 的 `DataFusionError::External`，因此能通过标准 DataFusion 路径
（如 `ctx.sql(...).collect()`）正常冒泡。该类型刻意**不**编码任何 PostgreSQL / gateway 概念。

## 测试与 feature

| feature | 用途 | 是否需要容器 |
|---|---|---|
| （默认） | 单元测试：schema 映射、`ScalarValue` 转 key、谓词识别、下推决策、错误映射 | 否 |
| `integration_tests` | 真实 Fluss 集群集成测试：e2e（真实 SQL 走真实后端）+ 实时元数据可见性 | 是（Docker / podman） |

常用命令：

```bash
# 单元测试（快，CI 默认门禁）
cargo test -p fluss-datafusion

# 真实集群 e2e（需要可用的容器运行时）
cargo test -p fluss-datafusion --features integration_tests -- e2e
```

测试只有两层：单元测试（不依赖集群）+ 真实集群集成测试。没有 fake/fixture 镜像层，因此不存在
「免集群断言」与「真实断言」相互漂移的风险；集成测试直接对真实后端验证完整的 catalog / 执行 /
pushdown 契约（`setup.rs` 提供共享建表/灌数）。

## 架构与边界

```
+-----------------------------+
|        SessionContext       |   每个 SQL 会话一个
+--------------+--------------+
               | register_catalog(...)
               v
+-----------------------------+
|       FlussDatafusion       |   共享 installer（Arc，无状态）
|  - MetadataLoader (live)    |   无缓存；每次调用都回源 Fluss
+--------------+--------------+
               | FlussSource（内部 trait，crate 边界）
               v
+-----------------------------+
|       RealFlussSource       |   包裹 Arc<FlussConnection>
+--------------+--------------+
               v
        Fluss client / core
```

模块职责：

- `metadata/`：共享实时元数据加载（无缓存）
- `catalog/`：`CatalogProvider` / `SchemaProvider`
- `table/`：`TableProvider` 与谓词识别
- `execution/`：自定义 `ExecutionPlan`（`FlussKvLookupExec` / `FlussLogScanExec`）
- `types/`：Fluss <-> Arrow 类型桥接
- `install.rs`：共享 installer / 注册入口

这里**不**包含任何服务端代码、协议适配、`pg_catalog`、REST handler、鉴权、会话注册或操作生命周期逻辑。

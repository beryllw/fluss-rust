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
- **Log 表**：带 `LIMIT` 的有界扫描（bounded scan），支持投影下推
- Fluss schema 到 Arrow schema、Fluss row 到 `RecordBatch` 的转换
- 共享 installer + 每会话 `register_catalog(...)` 的使用模型
- 不支持的查询形态会**保守失败**（明确报错），而不会静默退化成误导性的全表扫描

Phase 1 **不包含**：PostgreSQL / MySQL 兼容对象、REST/gRPC 接口、会话或操作生命周期、
principal 路由、多集群、SQL 写入（DML）、gateway 鉴权 / 审计。

## 使用方式

整体是「一个共享 installer + 每个 SQL 会话各自安装」的模型：

1. 用一个 `FlussConnection` 构造一个共享的 `FlussDatafusion`（内部持有元数据缓存）。
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

    // 2) 构造共享 installer（一次构造，跨会话复用元数据缓存）。
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

`FlussDatafusion` 是 `Arc` 共享、无状态的：构造一次即可被多个 `SessionContext` 复用，
重复 `register_catalog`（包括在新建 context 上）会复用已缓存的 database/table 列表，
不会重复打远端元数据请求。

## 受支持的查询形态

| 表类型 | 受支持的形态 | 说明 |
|---|---|---|
| KV（主键表） | `WHERE <完整主键> = <值>`（复合主键用 `AND` 全列等值） | 下推为点查，命中返回 1 行，未命中返回 0 行（不报错） |
| Log（日志表） | `... LIMIT n` | 有界扫描；投影下推；保留**末尾** `n` 行（与 Fluss `LimitBatchScanner` 语义一致） |

### 会保守失败的形态

下面这些会在**计划阶段**直接报错，而不会退化成全表扫描：

| 查询 | 报错 |
|---|---|
| KV 表用非主键列做谓词，如 `WHERE name = 'x'` | `unsupported query pattern: ...` |
| Log 表不带 `LIMIT`，如 `SELECT * FROM log_t` | `LIMIT required: ...` |

可以用 `EXPLAIN` 确认走的是自定义执行计划：KV 点查显示 `FlussKvLookupExec`，
Log 有界扫描显示 `FlussLogScanExec`。

## 配置

`FlussDatafusionOptions`：

| 字段 | 默认值 | 含义 |
|---|---|---|
| `metadata_cache_ttl` | `300s` | 共享元数据缓存的 TTL；database/table 列表在此期间不重新拉取 |

`RegisterCatalogOptions` 目前为空占位，为后续 per-catalog 选项预留。

## 元数据可见性与已知限制（待讨论）

> 这是一个**已知的开放问题**，当前实现刻意保持快照语义，修复方案后续再讨论。

DataFusion 的 `CatalogProvider` / `SchemaProvider` 列举方法（`schema_names()`、
`table_names()`、`table_exist()`）是**同步**的，而 Fluss 的元数据接口是 async 的。要在
同步上下文里拿到 async 结果，只有两条路：提前把数据放进内存（快照），或在同步上下文里
`block_on` 阻塞等待远程返回（需要绕开 tokio 嵌套运行时，每次列举都付一次阻塞 RPC + 线程
创建的代价）。本 crate 选择了**快照**，以换取同步读的纳秒级开销和干净的实现。

由此带来两层快照语义：

| 层 | 冻结点 | 行为 |
|---|---|---|
| Provider 快照 | `register_catalog` 那一刻把 database/table 名冻成 `Vec<String>` | 该 Provider 在其 `SessionContext` 生命周期内**不再回源**；即使缓存 TTL 过期也不刷新 |
| 缓存 TTL（`metadata_cache_ttl`，默认 300s） | 下一次 `build_catalog_provider` / 首次加载某表 meta 时 | 决定重新构建 Provider 或加载表 schema 时是否回源 |

**当前可见性契约：**

- catalog 的 database/table 列表是 `register_catalog` 时刻的快照。
- 注册**之后**新建的表，对该 `SessionContext` **不可见**（`table()` 对不在快照中的名字直接
  返回 `None`，不回源），与缓存 TTL 无关。要看到新表，需重新 `register_catalog`。
- 表的 schema/meta 在首次访问时按 TTL 缓存；TTL 内的 schema 变更不会立即反映。

**待讨论的修复方向**（尚未实现，仅备忘）：

- 写清契约、不改代码——若消费方本就每请求/每会话重新注册，快照陈旧几乎不构成问题。
- 在 `FlussDatafusion` 上提供显式 `invalidate()` / `refresh()` 原语，由消费方在 DDL 后按需
  调用——保住同步读的快，把刷新做成事件驱动。
- Provider 改为同步窥探缓存而非冻结 `Vec`（半解，缓存空/过期时同步方法仍无法自行回源）。
- 走 `block_on` 现查（永远新鲜，但代价是每次列举一次阻塞 RPC + 嵌套运行时线程 hack）。

选型取决于消费方的访问模式（每请求注册 vs 长生命周期 session）与 DDL 来源（是否经过本
crate / gateway）。这部分留待后续结合 gateway 的实际用法再定。

## 错误模型

公开错误类型 `FlussDatafusionError`（别名 `Result<T>`），明确区分各类不支持/失败：
`DatabaseNotFound` / `TableNotFound` / `UnsupportedQueryPattern` / `LimitRequired` /
`SchemaMismatch` / `TypeConversion` / `FlussClient` / `Internal`。
它会被桥接成 DataFusion 的 `DataFusionError::External`，因此能通过标准 DataFusion 路径
（如 `ctx.sql(...).collect()`）正常冒泡。该类型刻意**不**编码任何 PostgreSQL / gateway 概念。

## 测试与 feature

| feature | 用途 | 是否需要容器 |
|---|---|---|
| （默认） | 单元测试：schema 映射、`ScalarValue` 转 key、谓词识别、下推决策、错误映射，以及 metadata cache 的 TTL / 命中复用逻辑 | 否 |
| `integration_tests` | 真实 Fluss 集群集成测试：e2e（真实 SQL 走真实后端） | 是（Docker / podman） |

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
|        FlussDatafusion       |   共享 installer（Arc，无状态）
|  - MetadataLoader + Cache    |   跨会话复用元数据
+--------------+--------------+
               | FlussSource（内部 trait，crate 边界）
               v
+-----------------------------+
|       RealFlussSource        |   包裹 Arc<FlussConnection>
+--------------+--------------+
               v
        Fluss client / core
```

模块职责：

- `metadata/`：共享元数据加载与缓存
- `catalog/`：`CatalogProvider` / `SchemaProvider`
- `table/`：`TableProvider` 与谓词识别
- `execution/`：自定义 `ExecutionPlan`（`FlussKvLookupExec` / `FlussLogScanExec`）
- `types/`：Fluss <-> Arrow 类型桥接
- `install.rs`：共享 installer / 注册入口

这里**不**包含任何服务端代码、协议适配、`pg_catalog`、REST handler、鉴权、会话注册或操作生命周期逻辑。

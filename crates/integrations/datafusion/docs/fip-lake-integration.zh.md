# FIP: `fluss-datafusion` lake 交互与适配边界

> For the English version, see [fip-lake-integration.md](./fip-lake-integration.md).

|                   | ------ |
| ----------------- | ------ |
| Discussion thread | TBD    |
| Vote thread       | TBD    |
| ISSUE             | TBD    |
| Release           | TBD    |

# Motivation

`fluss-datafusion` 已经具备 lake-enabled 表的可工作路径，但在继续演进之前，需要先把它的职责边界写清楚。

核心问题是：`fluss-datafusion` 不应该变成第二个实现 lake+log union 语义的地方。真正的 union 语义应当收敛在 `fluss-lake`，而 DataFusion 集成层只负责：

1. 识别一张表是否应该进入 lake kernel；
2. 为主键表保留少量明确的 Fluss KV fast path；
3. 把其余 lake-enabled 读取统一委托给 `fluss-lake`；
4. 将 kernel split 映射成 DataFusion execution partition。

如果没有一份明确的 FIP，后续实现很容易把 seam 处理、lake split regrouping、append stitch 或 PK merge 等逻辑重新长回 connector，导致语义重复、其他 Arrow engine 难以复用、依赖方向也会变得模糊。

第二个动机是命名与范围收敛。当前实现仍使用 `FlussUnionTableProvider` 和 `FlussUnionScanExec`，但真实语义已经更接近“`fluss-lake` 之上的 DataFusion adapter”。本文档的目标就是把这种 steady state 固化下来。

# Public Interfaces

本提案主要澄清 lake-enabled DataFusion 路径的行为契约与 adapter 边界，而不是引入一套新的终端用户 Rust API。

本文涉及的 public contract 主要有：

1. **lake-enabled SQL 读路由**
   - 对 lake-enabled 表，connector 可能选择：
     - KV point lookup fast path；
     - KV prefix lookup fast path；
     - 通过 `fluss-lake` 执行正常 lake+log union read；
     - 通过 `<table>$lake` 执行 lake-only read。

2. **用户可见读形态**
   - `<table>` 表示统一视图：lake snapshot + residual Fluss log tail。
   - `<table>$lake` 表示只读 lake snapshot。

3. **与 `fluss-lake` 的 engine boundary contract**
   - `FlussLakeTable`
   - `FlussLakeScan`
   - `FlussLakeReadPlan`
   - `FlussLakeRead`
   - `FlussLakeReadSplit`

4. **执行分区映射**
   - 一个 `FlussLakeReadSplit` 映射为一个 DataFusion partition。

5. **当前 pushdown 契约**
   - PK point/prefix lookup 对应的谓词可被 fast path 精确消费；
   - lake union path 当前不提供 value-filter pushdown，residual filter 仍保留在 scan 之上。

对于 `FlussUnionTableProvider`、`FlussUnionScanExec` 这类当前过渡性的内部命名，本文不提供长期兼容承诺；它们可以在后续收敛中改名。

# Proposed Changes

## 1. 固定 `fluss-datafusion` 的角色

对于 lake-enabled 表，`fluss-datafusion` 的正式定位是 **thin adapter**。

职责分工如下：

- `fluss-rust`
  - 表元数据；
  - lake snapshot / seam RPC；
  - Fluss log 读取；
  - Arrow 转换辅助。
- `fluss-lake`
  - bounded read boundary freeze；
  - logical split planning；
  - append/log stitch；
  - PK merge；
  - lake-only 读语义。
- `fluss-datafusion`
  - SQL 谓词识别；
  - 为符合条件的 PK 读选择 fast path；
  - 构造 `FlussLakeTable` / `FlussLakeScan`；
  - 把 `FlussLakeReadPlan` 和 `FlussLakeRead` 包装为 DataFusion execution plan。

`fluss-datafusion` 不应持有以下职责：

- seam 解释；
- Paimon `DataSplit` planning；
- append/log stitch 行为；
- PK merge 行为；
- 物理 lake/log task graph 设计。

## 2. 固化查询路由矩阵

对于 lake-enabled 表，`TableProvider::scan()` 应遵循如下优先级：

| 查询形态 | 路径 | 原因 |
| --- | --- | --- |
| PK 表 + 完整 primary-key equality | `FlussKvLookupExec` | 当前状态已在 Fluss KV 中，可直接点查，无需打开 lake |
| PK 表 + 完整 bucket-key prefix equality | `FlussKvPrefixLookupExec` | 前缀查仍可直接从 Fluss current state 服务 |
| `<table>$lake` | `fluss-lake` + `lake_only` | 调用方明确要求只读 lake snapshot |
| 其他所有 lake-enabled 读 | `fluss-lake` union read | kernel 统一负责 seam、snapshot、append stitch、PK merge |

当前实现说明：`fluss-lake` union path 目前只稳定覆盖 **非分区** lake 表。分区表的 public API shape 已经预留，但 kernel 仍显式拒绝真正的 partitioned lake read。

额外约束：

- fast path 只在 `!lake_only && !primary_keys.is_empty()` 时启用；
- union path 当前把所有 filter 标记为 `Unsupported`，由上层残留 `FilterExec` 继续处理；
- `num_buckets == 0` 时直接返回 `EmptyExec`。

## 3. 固化 planning / execution 流程

### `TableProvider::scan()`

lake path 下，connector 负责：

1. 规范化 projection；
2. 尝试 PK point lookup fast path；
3. 尝试 PK prefix lookup fast path；
4. 验证底层 source 是生产实现 `RealFlussSource`；
5. 将 caller-supplied lake storage options 合并覆盖到 server-derived `table.datalake.*` properties 上，采用 **caller wins**；
6. 构造 `FlussLakeTable`；
7. 创建 `FlussLakeScan`，按需设置 projection 与 `lake_only`；
8. 调用 `plan()` 获取 `FlussLakeReadPlan`；
9. 调用 `new_read()` 获取 `FlussLakeRead`；
10. 返回包装了 `plan + read` 的 DataFusion execution plan。

### `ExecutionPlan::execute(i)`

执行阶段：

1. 选择 `plan.splits()[i]`；
2. 调用 `read.read_split(split)`；
3. 把 kernel `RecordBatchStream` 适配成 DataFusion 的 `SendableRecordBatchStream`；
4. 将 residual filter、limit 等其他算子继续交给 DataFusion 处理。

这里明确保持 DataFusion 不感知：

- seam DTO；
- 物理 Paimon `DataSplit`；
- append-vs-PK 内部任务形态；
- log start/stop offset 的具体语义。

## 4. 把 `<table>$lake` 固化为正式读形态

lake-only read 不是偶然的实现细节，而是一级契约：

- `<table>` 返回统一视图：lake snapshot + residual Fluss tail；
- `<table>$lake` 只返回 Paimon lake snapshot；
- `<table>$lake` 不允许走 PK fast path，因为调用方明确要看 lake side，而不是 Fluss current state。

因此本文把 `lake_only` 视为 connector 可以正式依赖的稳定行为。

## 5. 把配置注入保留在 adapter 边界

connector 还承担一个部署相关职责：当服务端表属性中无法完整保留敏感 lake storage 配置时，DataFusion 侧需要在打开 Paimon catalog 前重新注入这些参数。

契约如下：

- server-derived `table.datalake.*` properties 提供逻辑 catalog 配置；
- caller-supplied storage options 可以补充或覆盖 credentials、endpoint、region 等连接参数；
- 合并策略为 **caller wins**。

这部分属于 adapter 层的 transport/configuration glue，不属于 lake read 语义本身。

## 6. 记录收敛路线

### Phase 1: 文档收敛

- 明确 `fluss-datafusion` 是 lake kernel adapter；
- 固化路由矩阵；
- 明确 `<table>` 与 `<table>$lake` 两种读形态。

### Phase 2: 命名收敛

建议将当前过渡性内部命名改为：

- `FlussUnionTableProvider` -> `FlussLakeTableProvider`
- `FlussUnionScanExec` -> `FlussLakeScanExec`

要求：

- 不改变行为；
- `EXPLAIN` 输出同步调整；
- 测试与维护文档同步更新。

### Phase 3: 继续缩窄 connector 的长期依赖面

随着 `fluss-lake` split contract 逐步稳定，`fluss-datafusion` 应继续收敛到：

- 谓词识别；
- execution-plan 包装；
- 配置注入。

# Compatibility, Deprecation, and Migration Plan

- **对现有用户的影响：** 当前不要求引入 SQL 层面的破坏性变更，更多是对既有行为的文档化与稳定化。
- **当前行为边界：** lake union path 在真正支持 partitioned lake read 之前，仍然等价于只支持非分区 lake 表。
- **内部改名：** `FlussUnion*` 到 `FlussLake*` 的改名主要是维护者视角的收敛，应与 `EXPLAIN` 字符串、测试和文档同步推进。
- **`<table>$lake`：** 它是对已存在行为的正式化，不是新增的不兼容变化。
- **迁移工具：** 预期不需要。
- **移除时机：** 当代码、测试与文档都切换到 `Lake` 命名后，可以移除旧的 `Union` 内部命名。

# Test Plan

这里主要关注系统行为而不是单元实现细节。

现有和建议的验证方式包括：

- `cargo test -p fluss-datafusion`
- `cargo test -p fluss-datafusion --features integration_tests`

重点回归场景：

1. lake-enabled append/log 表以 `<table>` 查询时返回 lake + log unified result；
2. lake-enabled PK 表的完整 PK equality 走 `FlussKvLookupExec`；
3. lake-enabled PK 表的完整 bucket-key prefix equality 走 `FlussKvPrefixLookupExec`；
4. 未命中 fast path 的 lake-enabled 读走 lake scan execution plan；
5. `<table>$lake` 只返回 lake snapshot，且不命中 PK fast path；
6. caller-supplied storage options 打开 lake catalog 时能正确覆盖 endpoint / credentials；
7. `EXPLAIN` 能正确反映 fast path 与 lake scan path。

# Rejected Alternatives

## 1. 继续把 lake+log union 语义保留在 `fluss-datafusion` 内部

拒绝原因：这会重复本应由 `fluss-lake` 统一承载的语义，也会让其他 Arrow engine 复用变得困难。

## 2. 所有 lake-enabled PK 读都统一走 `fluss-lake`

拒绝原因：完整 PK equality 和完整 bucket-key prefix equality 已经存在高效的 Fluss current-state fast path；对这些查询再次打开 lake 是不必要开销。

## 3. 把 `<table>$lake` 当作纯内部实现细节

拒绝原因：它已经是用户可见的独立读形态，应当被正式文档化。

## 4. 向 DataFusion 暴露 seam 和 physical split 细节

拒绝原因：这会把 engine adapter 与 kernel 内部实现绑死，降低复用性，也增加未来演进成本。

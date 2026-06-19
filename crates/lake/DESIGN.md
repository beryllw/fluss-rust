# fluss-lake 正式设计文档

> 本文档是 `fluss-lake` 的唯一正式设计来源。  
> 设计讨论、接口调研、阶段性实现说明等都不构成规范；如有冲突，以本文档为准。

---

## 1. 目标与范围

`fluss-lake` 是 Fluss 的 **client-side lake+log read kernel**。

它解决的问题是：
- 一张 lake-enabled Fluss 表的物理数据同时存在于：
  - **lake**：Paimon snapshot（历史部分）
  - **log**：Fluss log tail（增量部分）
- 但对调用方来说，这张表应表现为一张**普通可读的表**，而不是两份独立数据源的组合。

因此，`fluss-lake` 的职责是：
- 接收一次读取请求（scan）
- 规划（plan）出可执行的 **logical read splits**
- 在执行期读取这些 split，并在 kernel 内部完成：
  - append/log 表的 stitch
  - PK 表的 merge
- 最终对外只暴露统一的 Arrow 读取接口

### v1 范围

v1 **只覆盖 bounded batch read**：
- 读取边界在 planning 阶段冻结
- 结果是可重复的 bounded snapshot view
- `split_count()` / `splits()` 的意义稳定

### v1 不覆盖

以下能力不属于 v1 的正式 public contract：
- streaming / continuation read
- 通用 value predicate pushdown
- 完整 residual predicate contract
- partitioned table 的完整执行实现（但 public shape 必须为其预留）
- PK merge 的完整落地实现（但 public shape 必须覆盖它）

---

## 2. 核心原则

### 2.1 Union 语义完全留在 kernel 内部

> 所有 lake+log union-read 语义都由 `fluss-lake` 处理。  
> 外部不会看到 lake branch / log branch / seam / physical lake split / merge state。

### 2.2 公共语义采用 `table -> scan -> plan`

公共入口风格贴近 `fluss-rust`：

```text
table -> new_scan()
```

planning 语义吸收 `paimon-rust`：

```text
scan -> plan -> public logical splits
```

组合起来，公共使用心智是：

```text
FlussLakeTable
  -> new_scan()
  -> FlussLakeScan
      -> plan()      -> FlussLakeReadPlan
      -> new_read()  -> FlussLakeRead
```

### 2.3 split 是唯一按-task 分发的 public object

v1 面向分布式 split 分发场景设计。

因此：
- `FlussLakeReadSplit` 是唯一按读取任务分发、checkpoint、reassignment 的 public DTO
- reader 不是 public distributed object
- planner 端分发的是 split，而不是 reader / internal task / process-local plan

### 2.4 reader 是本地执行对象

执行端应当：
- 持有与 planner 侧语义等价的 scan 配置
- 本地构造 `FlussLakeRead`
- 用它消费 public split

也就是说：
- **split 是唯一分发对象**
- **reader 是本地构造对象**
- split 的解释发生在“语义等价的 scan 配置”之下

### 2.5 append/log 与 PK 共享一套 public API

append/log 与 PK 的差异只应体现在 internal task graph 中：
- append/log：lake + log stitch
- PK：lake current state + Fluss changelog merge

对外都应表现为：

```text
table -> scan -> plan
scan -> new_read()
read.read_split(split)
```

---

## 3. 三层职责边界

## 3.1 `fluss-rust`

`fluss-rust` 只负责 Fluss 原生能力：
- 表元数据（`TableInfo`、`TableConfig`、`table.datalake.*`）
- lake snapshot / seam RPC
- log tail 从任意 offset 开始读
- Arrow 解码

不负责：
- Paimon 规划
- union 语义
- query-engine 适配

它对 `fluss-lake` 提供的最重要 lake 支持是：
- `TableConfig::get_lake_catalog_properties()`

## 3.2 `fluss-lake`

`fluss-lake` 是唯一负责 lake+log read kernel 语义的层：
- 从 Fluss 表属性打开 Paimon catalog
- 冻结读取边界
- 对 lake 做 once-per-query planning
- 生成 public logical read splits
- 在执行时 stitch / merge
- 对外暴露 scan / plan / read-split API

## 3.3 `fluss-datafusion`

`fluss-datafusion` 是 thin adapter：
- 判断表是否应由 `fluss-lake` 承接
- 构造 `FlussLakeTable -> FlussLakeScan`
- 调 `plan()`
- 拿到 `split_count()` / `splits()` / `schema()`
- 在执行端本地构造 read executor，并消费 split

它不负责：
- seam 获取与解释
- lake split regrouping
- append/log stitch
- PK merge

---

## 4. Public API 定稿

## 4.1 入口对象：`FlussLakeTable`

```rust
pub struct FlussLakeTable { ... }
impl FlussLakeTable {
    pub fn new_scan(&self) -> FlussLakeScan;
}
```

语义：
- 表示一张可由 lake kernel 读取的 Fluss 表
- 入口沿用 `fluss-rust` 的 `table -> new_scan()` 风格

## 4.2 扫描配置：`FlussLakeScan`

```rust
pub struct FlussLakeScan { ... }
impl FlussLakeScan {
    pub fn with_projection(self, indices: Vec<usize>) -> Self;
    pub fn with_filter(self, filter: FlussLakeFilter) -> Self;
    pub fn with_options(self, options: FlussLakeReadOptions) -> Self;

    pub async fn plan(&self) -> Result<FlussLakeReadPlan, FlussLakeError>;
    pub fn new_read(&self) -> Result<FlussLakeRead, FlussLakeError>;
}
```

### 语义说明
- `scan` 持有本次读取的公共配置：
  - projection
  - filter
  - read options
- `plan()` 使用这些配置进行 once-per-query planning
- `new_read()` 使用这些配置在本地构造执行器

也就是说：
- `scan` 是 planning 和 reading 的共同配置源
- 但 `plan` 与 `read` 仍然是分离的两个阶段

## 4.3 规划结果：`FlussLakeReadPlan`

```rust
pub struct FlussLakeReadPlan { /* opaque internal state */ }
impl FlussLakeReadPlan {
    pub fn schema(&self) -> SchemaRef;
    pub fn split_count(&self) -> usize;
    pub fn statistics(&self) -> &FlussLakePlanStatistics;
    pub fn splits(&self) -> &[FlussLakeReadSplit];
}
```

### 语义说明
`FlussLakeReadPlan` 负责公开：
- 输出 schema
- logical split 数量
- logical split 描述符
- split 级统计信息

它不负责：
- 创建 read executor
- 直接执行 split

## 4.4 public logical split：`FlussLakeReadSplit`

```rust
pub struct FlussLakeReadSplit {
    pub split_id: String,
    pub bucket_id: i32,
    pub partition: FlussLakePartitionIdentity,
    pub estimated_rows: Option<usize>,
    pub estimated_size: Option<usize>,
    // private opaque execution descriptor
}
```

### 语义说明
`FlussLakeReadSplit` 是：
- public
- logical
- distributed
- executable

它不是：
- Paimon `DataSplit`
- seam DTO
- internal task graph

### split 的执行 contract
`FlussLakeReadSplit` 是唯一按-task 分发的 public object，但它不是完整执行凭证。  
完整执行前提是：

> 执行端持有与 planner 侧**语义等价**的 `FlussLakeScan` 配置，并本地用它构造 `FlussLakeRead`。

也就是说：
- split 描述“读哪个逻辑单元”
- scan 配置定义“如何解释和执行这个逻辑单元”

### `split_id` 的稳定域
`split_id` 的稳定性域明确为：

> **同一张表、同一次 plan 冻结出来的 logical split 集合内部稳定且唯一。**

它用于：
- logging
- checkpoint
- failover / reassignment
- debug

它不是：
- 跨任意两次不同 plan 的全局持久引用
- 完整执行凭证

### 不应公开的内容
- `LakeSeam`
- Paimon `DataSplit`
- `log_start_offset` / `log_stop_offset`
- 当前内部 lake split index / skip count / continuation state
- append/log / PK 的 internal task struct

## 4.5 执行器：`FlussLakeRead`

```rust
pub struct FlussLakeRead { /* opaque read executor */ }
impl FlussLakeRead {
    pub async fn read_split(
        &self,
        split: &FlussLakeReadSplit,
    ) -> Result<RecordBatchStream, FlussLakeError>;

    pub async fn read_splits(
        &self,
        splits: &[FlussLakeReadSplit],
    ) -> Result<RecordBatchStream, FlussLakeError>;
}
```

### 语义说明
- `FlussLakeRead` 是执行端本地构造的 reader
- 它消费 public logical read split descriptors
- 它不被 planner 分发
- 也不应成为 public distributed object

---

## 5. Public supporting types

## 5.1 `FlussLakeReadOptions`

```rust
pub struct FlussLakeReadOptions {
    pub mode: FlussLakeReadMode,
    pub batch_size: Option<usize>,
}
```

## 5.2 `FlussLakeReadMode`

v1 只支持 bounded batch：

```rust
pub enum FlussLakeReadMode {
    Batch(FlussLakeReadBoundaryMode),
}
```

## 5.3 `FlussLakeReadBoundaryMode`

```rust
pub enum FlussLakeReadBoundaryMode {
    LatestReadable,
    SnapshotId(i64),
}
```

### 语义
- `LatestReadable`：获取一个可重复的 bounded batch snapshot
- `SnapshotId(i64)`：绑定到一个指定 snapshot

## 5.4 `FlussLakePlanStatistics`

```rust
pub struct FlussLakePlanStatistics {
    pub split_count: usize,
    pub estimated_rows: Option<usize>,
    pub estimated_size: Option<usize>,
}
```

## 5.5 `FlussLakePartitionIdentity`

```rust
pub enum FlussLakePartitionIdentity {
    Unpartitioned,
    KeyValues(Vec<(String, String)>),
}
```

这个类型同时服务于：
- public split descriptor
- filter
- future partitioned-table support

---

## 6. Read boundary contract（v1 batch）

这是第一个核心 contract。

```rust
struct ReadBoundary {
    lake_snapshot_id: i64,
    split_boundaries: Vec<SplitReadBoundary>,
}

struct SplitReadBoundary {
    split_id: String,
    partition: FlussLakePartitionIdentity,
    bucket_id: i32,
    log_start_offset: i64, // inclusive
    log_stop_offset: i64,  // exclusive
}
```

### 语义
- `ReadBoundary` 是 **filter 后真正进入执行的 logical splits** 的边界
- 它不是全表候选边界
- 它是 v1 bounded batch 的完整冻结边界

### `SnapshotBoundary` 的关系
`SnapshotBoundary` 只是内部 planning 原料：

```text
SnapshotBoundary
    + filtered split set
    + per-split frozen stop offsets
    = ReadBoundary
```

因此：
- `SnapshotBoundary` 只在 kernel 内使用
- `ReadBoundary` 才是 public contract 背后的最终 batch 语义

---

## 7. Filter contract（v1）

这是第二个核心 contract。

`FlussLakeFilter` 是 `fluss-lake` 公共 filter 的**最终类型**。命名直接奔最终目标：
它就是 filter；v1 只是**实现的子集**，不是**语义的子集**。

v1 的实现范围：
- partition 维度过滤
- bucket 维度过滤

v1 暂不实现（但未来在同一个 `FlussLakeFilter` 上扩展，不改名）：
- 通用 value predicate
- residual value predicate contract
- exact / inexact pushdown 语义

## 7.1 Public shape

```rust
pub struct FlussLakeFilter {
    pub partitions: Option<Vec<FlussLakePartitionFilter>>,
    // future: pub predicate: Option<FlussLakePredicate>,
}

pub struct FlussLakePartitionFilter {
    pub partition: FlussLakePartitionIdentity,
    pub bucket_ids: Option<Vec<i32>>,
}
```

## 7.2 组合语义
- `partitions = None` -> all partitions × all buckets
- `partitions = Some([])` -> empty target set
- 对每个 `FlussLakePartitionFilter`：
  - 先选定这个 partition
  - 再在该 partition 内按 `bucket_ids` 选 bucket
- `bucket_ids = None` -> 该 partition 的所有 bucket
- `bucket_ids = Some([])` -> 该 partition 下不选任何 bucket

这一定义明确了：
> **Fluss filter 的 public 语义是“先分区，再 bucket”。**

## 7.3 append/log vs PK
- **append/log**：filter 是主输入之一，用于生成 `(partition, bucket)` read scope
- **PK**：filter 同样用于缩小 scan scope；full-key / prefix-key 语义不属于 v1 filter contract

因此：
- append/log 与 PK 共享同一公共 filter API 形状；
- 但内部 planner 的消费方式不同。

---

## 8. split identity contract

这是第三个核心 contract。

`FlussLakeReadSplit` 必须满足：
- pure data
- serializable / versioned
- cloneable / regroupable / reassignable
- executable under a semantically-equivalent scan configuration

### 8.1 `split_id`
`split_id` 是：
- stable public identity within one plan
- printable / checkpointable / reassignable
- not a complete execution credential

### 8.2 split 与 scan 的关系
设计上必须明确：
- split 是唯一按-task 分发的 object
- 但 split 的语义依赖于一份与 planner 端语义等价的 scan configuration
- reader 在执行端本地构造，并在该 scan 语义之下消费 split

换句话说：

```text
split is distributed
reader is local
split execution is valid only under semantically equivalent scan config
```

### 8.3 opaque execution descriptor
`FlussLakeReadSplit` 需要携带一个：
- private
- opaque
- serialized/versioned
- execution descriptor

这个 descriptor：
- 不对外解释
- 但属于 public wire contract 的一部分
- 用于在执行端重建 internal task

---

## 9. Internal execution model

public API 统一，但内部 task graph 按表类型分流。

## 9.1 append/log table

```rust
struct AppendBucketReadTask {
    partition_id: Option<i64>,
    bucket: i32,
    lake_splits: Vec<DataSplit>,
    log_start_offset: i64,
    log_stop_offset: i64,   // exclusive for batch
    projection: ProjectionSpec,
    output_schema: SchemaRef,
}
```

语义：
- 一个 logical read split 对应一个 bucket task
- 该 split 的结果 = `lake(bucket b)` stitched with `log_tail(bucket b)`

## 9.2 primary-key table

```rust
struct PrimaryKeyBucketReadTask {
    partition_id: Option<i64>,
    bucket: i32,
    lake_splits: Vec<DataSplit>,
    log_start_offset: i64,
    log_stop_offset: i64,   // exclusive for batch
    merge_mode: MergeMode,
    projection: ProjectionSpec,
    output_schema: SchemaRef,
}
```

语义：
- append/log：stitch / concat
- PK：merge current state + changelog

## 9.3 Shared internal envelope

```rust
struct PlannedSplit {
    public_split: FlussLakeReadSplit,
    task: ReadTask,
}

enum ReadTask {
    AppendBucket(AppendBucketReadTask),
    PrimaryKeyBucket(PrimaryKeyBucketReadTask),
}
```

---

## 10. Planning and execution flow

## 10.1 `FlussLakeScan::plan()`
1. 读取 Fluss 元数据与 `table.datalake.*`
2. 根据 `FlussLakeReadMode` 解析并冻结全表候选 `SnapshotBoundary`
3. 打开 Paimon table at snapshot anchor
4. 做一次 lake-side planning
5. regroup 内部 `DataSplit`
6. 应用 `FlussLakeFilter` 生成真正进入执行的 logical split 集合
7. 基于该集合冻结最终 `ReadBoundary`
8. 生成 append/log 或 PK 的 internal tasks
9. 物化 public `FlussLakeReadSplit`
10. 返回 `FlussLakeReadPlan`

## 10.2 `FlussLakeScan::new_read()`
1. 在本地执行环境中，根据当前 scan 配置构造 `FlussLakeRead`
2. 该 reader 不依赖 planner 进程内 `plan` 对象身份
3. 它消费 public split descriptor，并通过 split 内 private opaque descriptor 重建 internal task

## 10.3 `FlussLakeRead::read_split(...)`
1. 读取 public `FlussLakeReadSplit`
2. 解析其对应 internal `ReadTask`
3. append/log：
   - 读 lake side
   - 读 log tail
   - stitch
4. PK：
   - 读 lake current state
   - 读 changelog continuation
   - merge
5. 返回 `RecordBatchStream`

---

## 11. What DataFusion sees

DataFusion **不会**看到：
- lake split
- log seam
- union branch
- append/log vs PK internal task shapes
- Paimon / Fluss 双源拼接

DataFusion 只知道：
- 一张表有多少 logical read split
- split descriptor 长什么样
- 如何在本地构造 read executor
- 如何读取一个 split

Concretely:

### `TableProvider::scan()`
- 构造 `FlussLakeTable`
- `new_scan().with_projection(...).with_filter(...).with_options(...).plan().await`
- 得到 `FlussLakeReadPlan`
- 同时保留与 planner 语义等价的 `FlussLakeScan`
- execution plan 持有 `plan + scan`

### `ExecutionPlan::execute(i)`
- 取 `split = &plan.splits()[i]`
- `let read = scan.new_read()?;`
- `read.read_split(split)`
- 适配成 `SendableRecordBatchStream`

---

## 12. Partitioned tables

public API 必须为 partitioned table 预留，因此：
- `FlussLakeReadSplit` 不能假定“只有 bucket”
- 必须有 `FlussLakePartitionIdentity`
- `FlussLakeFilter` 必须能表达精确 `(partition, buckets)` 目标集合

本轮正式设计应明确：
- append/log 的非分区表是首个落地点；
- partitioned table 最终仍共享同一 public API；
- 但 kernel 内部在 append/log 与 PK 上都需要为 `(partition, bucket)` 执行任务做好扩展位。

---

## 13. Naming recommendations

### Public naming (final)
- `FlussLakeTable`
- `FlussLakeScan`
- `FlussLakeReadPlan`
- `FlussLakeRead`
- `FlussLakeReadSplit`
- `FlussLakeReadOptions`
- `FlussLakeReadMode`
- `FlussLakeReadBoundaryMode`
- `FlussLakeFilter`
- `FlussLakePartitionIdentity`
- `RecordBatchStream`
- `FlussLakeError`

### Internal naming (recommended)
- `SnapshotBoundary`
- `ReadBoundary`
- `AppendBucketReadTask`
- `PrimaryKeyBucketReadTask`
- `LogTailSpec`
- `LakeBucketInput`
- `ProjectionSpec`
- `ReadTask`
- `PlannedSplit`

### Transitional names to remove from the public surface
- `LakeSeam`
- `UnionScanPlan`
- `UnionPartition`
- `plan_append_union`
- `open_append_partition`
- `union_append_partition`
- `LogTailReader`

### Connector naming follow-up
当前 `FlussUnionTableProvider` / `FlussUnionScanExec` 仍是过渡名字。正式收敛后建议改为：
- `FlussLakeTableProvider`
- `FlussLakeScanExec`

---

## 14. Migration plan

### Step 1 — introduce the new public façade
新增并导出：
- `FlussLakeTable`
- `FlussLakeScan`
- `FlussLakeReadPlan`
- `FlussLakeRead`
- `FlussLakeReadSplit`
- `FlussLakeReadOptions`
- `FlussLakeReadMode`
- `FlussLakeFilter`
- `FlussLakePartitionIdentity`

内部先桥接到当前实现，不要求一步改完。

### Step 2 — move seam acquisition into `fluss-lake`
`LakeSeam` / snapshot 获取从 `fluss-datafusion::table::union` 下沉到 `FlussLakeScan::plan()` 内部。DataFusion 不再显式构造/持有 seam。

### Step 3 — make `fluss-datafusion` depend only on the new façade
`fluss-datafusion` 只依赖：
- `FlussLakeTable`
- `FlussLakeScan`
- `FlussLakeReadPlan`
- `FlussLakeRead`
- `FlussLakeReadSplit`

### Step 4 — move lake regrouping to plan-time
从当前 execute-time 打开 lake/table 或按 bucket 过滤，重构成 query 级 once-per-query planning：
- `plan()` 一次性获取 lake-side split 计划
- 按 bucket / partition+bucket 分组
- 存入内部 `AppendBucketReadTask` / `PrimaryKeyBucketReadTask`

### Step 5 — hide old union internals
把这些从 public surface 降级：
- `LakeSeam`
- `UnionScanPlan`
- `UnionPartition`
- `plan_append_union`
- `open_append_partition`
- `union_append_partition`
- `LogTailReader`

---

## 15. Verification

### Kernel
- `cargo test -p fluss-lake`
- once-per-query regrouping regression tests
- append/log boundary trim regression tests
- PK merge tests（后续）

### DataFusion
- `cargo test -p fluss-datafusion --lib`
- `cargo check -p fluss-datafusion --features integration_tests --tests`
- gated SQL e2e 继续复用 test seam override（直到 harness 拥有真实 tiering service）

---

## 16. Final aligned decisions

1. 公共命名采用 `FlussLake*` 家族。
2. 公共语义采用 `table -> scan -> plan`，以及 `scan -> read`。
3. `ReadPlan` 公开 logical read split descriptors。
4. `FlussLakeReadSplit` 公开 bucket / partition identity，但不泄漏 seam / 物理 split / offset 细节。
5. v1 面向分布式使用场景设计 split descriptor，而不是只服务同进程 DataFusion。
6. v1 仅覆盖 bounded batch read；streaming 作为 future work。
7. v1 公共 filter 只做 partition / bucket 过滤，不承诺通用 value predicate pushdown。
8. append/log 与 PK 共享同一公共 API 形状，只在内部任务图上分叉。
9. split 是唯一按-task 分发的 public object；reader 在执行端本地构造并消费 split。
# fluss-lake 待办问题清单

本文件记录审阅 `crates/lake` 当前设计、公共接口与实现后发现的待办问题。每条问题包含现状、影响、证据、复现或测试方法、建议处理方向。

本地验证基线：

```bash
cargo test -p fluss-lake
```

该命令已通过。下面的问题主要是 `DESIGN.md` 的公共契约与当前实现之间的缺口。

---

## 问题 1：`FlussLakeReadSplit` 不是可序列化/版本化的 distributed DTO

### 现状
`DESIGN.md` 将 `FlussLakeReadSplit` 定义为唯一可分发、checkpoint、reassignment 的 public object。当前实现的 split 只能 `Clone`，没有 `Serialize` / `Deserialize`，也没有版本字段。更重要的是，split 内部直接保存私有 `ReadTask` enum，而不是一个版本化 opaque execution descriptor。

### 影响
planner 和 executor 必须在同一个 Rust 进程内共享 `FlussLakeReadSplit` 值。调度器无法把 split 序列化后发给另一个 worker，也无法持久化 checkpoint。这个问题阻断了设计中最核心的分布式 split 分发场景。

### 证据
- `DESIGN.md:221-250` 定义 split 是 public/logical/distributed/executable。
- `DESIGN.md:256-265` 说明 `split_id` 用于 checkpoint / failover / reassignment。
- `src/api.rs:47-54` 私有 `ReadTask`。
- `src/api.rs:442-449` `FlussLakeReadSplit` 只 `#[derive(Debug, Clone)]`，并持有 `task: ReadTask`。
- `Cargo.toml` 未依赖 `serde`。

### 复现/测试
增加一个下游 compile test：

```rust
let split = plan.splits()[0].clone();
let json = serde_json::to_string(&split)?;
```

当前预期：编译失败，因为 `FlussLakeReadSplit` 未实现 `Serialize`。修复后应增加 JSON/bincode round-trip 测试，验证 append 与 PK split 都能跨进程重建。

### 建议处理
引入版本化 descriptor，例如：

```rust
pub struct FlussLakeReadSplit {
    pub split_id: String,
    pub bucket_id: i32,
    pub partition: FlussLakePartitionIdentity,
    pub estimated_rows: Option<usize>,
    pub estimated_size: Option<usize>,
    pub descriptor_version: u16,
    pub descriptor: Vec<u8>,
}
```

内部 `ReadTask` 由 descriptor decode 得到，不直接存放在 public split struct 中。公共 DTO derive `serde::{Serialize, Deserialize}`。

---

## 问题 2：`plan()` 未执行 lake-side planning，Paimon planning 被推迟到 `read_split()`

### 现状
`DESIGN.md` 规定 `FlussLakeScan::plan()` 需要打开 Paimon table at snapshot anchor、执行 once-per-query lake-side planning、regroup `DataSplit`，再生成 public logical splits。当前 `plan()` 只解析 seam、冻结 log stop offset，并按 `(partition, bucket)` 生成一个 split；Paimon catalog open 和 `ReadBuilder::new_scan().plan()` 发生在 `read_split()` 执行阶段。

### 影响
`plan()` 可能成功返回，但真实 lake table、snapshot、object store 配置在执行时才失败。这削弱了 bounded batch planning 的含义，也使 split 无法携带具体 lake physical split 信息和估算统计。

### 证据
- `DESIGN.md:558-568` `plan()` 步骤 3-5 明确要求打开 Paimon table、lake-side planning、regroup `DataSplit`。
- `src/api.rs:222-299` `plan()` 只做 config validation、stop offset freeze、split construction。
- `src/reader/lake.rs:47-62` `read_lake_table` 在执行期调用 `read_builder.new_scan().plan().await`。
- `src/append.rs:67-90` 和 `src/pk.rs:68-105` 在 split 执行期打开 catalog/table。

### 复现/测试
集成测试思路：

1. 创建 lake-enabled Fluss 表，保证 Fluss 侧 `get_latest_lake_snapshot` 能返回 snapshot。
2. 将 lake catalog property 的 warehouse 指到不存在或缺少该 table 的路径。
3. 调用 `scan.plan().await`。
4. 当前预期：`plan()` 成功，`read.read_split(plan.splits()[0])` 失败。
5. 期望修复后：`plan()` 直接失败，错误指向 lake table/snapshot 不可读。

### 建议处理
将 Paimon table open、snapshot pinning、`TableScan::plan()` 移入 `FlussLakeScan::plan()`；将物理 `DataSplit` 或其可序列化描述写入问题 1 的 opaque descriptor。`read_split()` 只做 descriptor replay，不重新规划 lake splits。

---

## 问题 3：`SnapshotId(i64)` 是 public API，但历史 snapshot 未实现

### 现状
`FlussLakeReadBoundaryMode::SnapshotId(i64)` 是 public enum variant，`DESIGN.md` 说明它绑定到指定 snapshot。当前实现只调用 `admin.get_latest_lake_snapshot`。如果请求的 `snapshot_id` 不等于 latest，就返回 `"SnapshotId(...) is not yet implemented in fluss-rust"`。

### 影响
公共 API 看起来支持历史 snapshot 读取，但实际只支持 latest snapshot 的等值情况。调用方无法可靠地用该 API 做 time travel / reproducible historical read。

### 证据
- `DESIGN.md:324-335` 声明 `SnapshotId(i64)` 语义。
- `src/api.rs:63-72` 暴露 `FlussLakeReadBoundaryMode::SnapshotId(i64)`。
- `src/api.rs:340-356` `SnapshotId` 分支只取 latest，并对非 latest 返回 not implemented。

### 复现/测试
当一张表 latest lake snapshot id 为 `N > 1` 时：

```rust
let scan = lake_table.new_scan().with_options(FlussLakeReadOptions {
    mode: FlussLakeReadMode::Batch(FlussLakeReadBoundaryMode::SnapshotId(N - 1)),
    batch_size: None,
});
let err = scan.plan().await.unwrap_err();
assert!(err.to_string().contains("not yet implemented"));
```

### 建议处理
二选一：

- 隐藏/feature-gate `SnapshotId`，直到 `fluss-rust` 提供指定 snapshot RPC。
- 或实现指定 snapshot seam 获取，并补 latest / historical snapshot 的集成测试。

---

## 问题 4：partitioned lake reads 公开了 shape，但 planning/execution 均直接拒绝

### 现状
公共 API 已有 `FlussLakePartitionIdentity::KeyValues` 和 `FlussLakePartitionFilter`。但当前目标解析只支持非分区表：只要 `partition_keys` 非空就返回 `"partitioned lake reads are not yet implemented"`。append 和 PK split 执行也拒绝 `partition_id.is_some()`。

### 影响
这是设计文档中已承认的 v1 缺口，但它是用户可见的。任何 lake-enabled partitioned table 进入 `fluss-lake` 都会失败，DataFusion lake route 也会受影响。

### 证据
- `DESIGN.md:633-647` 说明 partitioned table public API 已预留，但首个落地点是非分区 append/log。
- `src/api.rs:392-428` `resolve_filter_targets` 对非空 `partition_keys` 直接报错。
- `src/append.rs:58-64` 分区 split 执行报错。
- `src/pk.rs:55-61` 分区 PK split 执行报错。
- `src/api.rs` 已有 `partitioned_table_is_rejected` 单测。

### 复现/测试
现有单测：

```bash
cargo test -p fluss-lake api::tests::partitioned_table_is_rejected
```

还应补真实集成测试：创建 lake-enabled partitioned 表，调用 `new_scan().plan()`，断言错误稳定且可读。

### 建议处理
短期：在 public docs/README 明确 partitioned lake read 暂不支持。长期：实现 `(partition, bucket)` target planning、partition-specific lake split filtering、append/PK 两条执行路径，并补 no matched partition -> empty plan 测试。

---

## 问题 5：bucket filter 未校验范围和重复值

### 现状
`FlussLakeFilter` 允许显式传 `bucket_ids`。`resolve_filter_targets` 直接把这些 id 拷贝进 target 列表，没有校验 `0 <= bucket < num_buckets`，也没有处理重复 id。

### 影响
负数或越界 bucket 会延迟到 Fluss/Paimon 调用时报错，错误信息离用户输入较远。重复 bucket 会生成重复 split，可能重复读取数据。

### 证据
- `src/api.rs:420-422` `Some(ids)` 分支直接 `ids.iter().copied()`。
- `src/api.rs:577-582` 现有测试只覆盖正常 `vec![2, 0]`。
- 无 invalid/duplicate bucket 单测。

### 复现/测试
新增单测：

```rust
#[test]
fn invalid_bucket_ids_are_rejected() {
    let filter = FlussLakeFilter {
        partitions: Some(vec![pf(Some(vec![-1, 3]))]),
    };
    assert!(resolve_filter_targets(&Some(filter), 3, &[]).is_err());
}

#[test]
fn duplicate_bucket_ids_are_rejected() {
    let filter = FlussLakeFilter {
        partitions: Some(vec![pf(Some(vec![1, 1]))]),
    };
    assert!(resolve_filter_targets(&Some(filter), 3, &[]).is_err());
}
```

### 建议处理
在 planning 目标解析时校验 bucket 范围，并选择一个明确的重复策略。建议直接 reject duplicates，避免隐式去重改变调用者意图。

---

## 问题 6：split 与 reader 的 scan 配置等价性没有校验

### 现状
设计要求执行端使用与 planner 侧语义等价的 `FlussLakeScan` 构造 `FlussLakeRead`。当前 API 允许任意 `FlussLakeRead` 消费任意 `FlussLakeReadSplit`。`read_split()` 使用 split 内部 task 的 offset/snapshot，但 schema、projection、catalog properties 来自 reader 本身，没有 fingerprint 或 schema id 校验。

### 影响
调用方可以用 projection A 规划 split，却用 projection B 的 reader 执行，得到错误 schema/列；也可能用不同 table config 的 reader 执行旧 split。错误不会早失败，而是返回不符合 planner 语义的数据。

### 证据
- `DESIGN.md:246-254` 要求 executor 持有与 planner 侧语义等价的 scan 配置。
- `src/api.rs:320-328` `new_read()` 捕获当前 scan 的 schema/projection/catalog properties。
- `src/api.rs:497-523` `read_split()` 使用 reader 的 schema/projection 执行 split。
- `src/api.rs:442-449` split 没有 table/schema/projection fingerprint。

### 复现/测试
集成测试思路：

```rust
let plan_scan = lake_table.new_scan().with_projection(vec![0]);
let plan = plan_scan.plan().await?;

let read_scan = lake_table.new_scan().with_projection(vec![1]);
let read = read_scan.new_read()?;
let stream = read.read_split(&plan.splits()[0]).await?;
```

当前预期：执行可能成功，并按 reader 的 projection 返回列。期望修复后：`read_split()` 在打开 lake/log 前拒绝 scan mismatch。

### 建议处理
在 split descriptor 和 reader 中存储/校验 scan fingerprint。至少包括 table path 或 table id、schema id、projection、filter、read mode、lake-only 标志和 descriptor version。

---

## 问题 7：`FlussLakeReadOptions::batch_size` 是 public 字段但未使用

### 现状
`FlussLakeReadOptions` 暴露 `batch_size: Option<usize>`，但源码中除了字段定义没有任何读取或传递。

### 影响
调用方设置 `batch_size` 不会改变 Paimon 或 Fluss log reader 的 batch 行为。公共配置项无效会误导调优。

### 证据
- `src/api.rs:86-90` 定义 `batch_size`。
- `rg -n "batch_size" crates/lake/src` 只有字段定义。

### 复现/测试
静态复现：

```bash
rg -n "batch_size" crates/lake/src
```

行为测试需在接入后补：设置较小 `batch_size`，断言输出 batch 行数或 reader 配置确实变化。

### 建议处理
如果短期不实现，先从 public struct 移除或标注为 reserved。若保留，需要将其传给 Paimon read builder 和 Fluss log scan/tail reader（如果底层 API 支持），并增加行为测试。

---

## 问题 8：plan statistics 与 split estimates 没有实际统计信息

### 现状
`DESIGN.md` 的 `FlussLakePlanStatistics` 包含 `estimated_rows` 和 `estimated_size`。当前实现的 `FlussLakePlanStatistics` 只有 `split_count` 与 `mode`；每个 `FlussLakeReadSplit` 的 `estimated_rows` / `estimated_size` 都固定为 `None`。

### 影响
分布式调度器无法用 plan statistics 做 split sizing、负载均衡或任务 reassignment 规划。文档里的统计接口也与实现不一致。

### 证据
- `DESIGN.md:337-345` 定义 `estimated_rows` / `estimated_size`。
- `src/api.rs:273-286` 构造 split 时估算字段均为 `None`。
- `src/api.rs:471-484` `statistics()` 返回 `split_count` 与 `mode`。

### 复现/测试
```bash
rg -n "estimated_rows|estimated_size|FlussLakePlanStatistics" crates/lake/DESIGN.md crates/lake/src/api.rs
```

建议新增测试：对非空 table plan 后，确认 statistics 与 split estimates 的 contract。若估算暂不可得，应明确文档为 `None` policy。

### 建议处理
二选一：实现估算（从 Paimon split metadata 和 log offset 差估算）；或更新 `DESIGN.md`，明确 v1 不提供 plan-level row/size estimates，仅在 split 级保留 optional 字段。

---

## 问题 9：`split_id` 稳定域符合设计，但生成方式没有显式防重

### 现状
`split_id` 由 `format!("{}:{}:{}", table_path, partition_id_or_dash, bucket)` 生成。对默认 all buckets target，该方式在一次 plan 内唯一。但如果 filter 传入重复 bucket id，`split_id` 也会重复。问题 5 修复前，`split_id` 的 "同一次 plan 内唯一" 设计无法保证。

### 影响
checkpoint / logging / reassignment 依赖 `split_id` 时可能碰到重复 split id。

### 证据
- `DESIGN.md:256-265` 要求同一次 plan 内 stable and unique。
- `src/api.rs:273-286` `split_id` 只由 table、partition、bucket 拼接。
- `src/api.rs:420-422` 允许重复 bucket id 进入 target。

### 复现/测试
新增单测：

```rust
let filter = FlussLakeFilter {
    partitions: Some(vec![pf(Some(vec![1, 1]))]),
};
let targets = resolve_filter_targets(&Some(filter), 3, &[]).unwrap();
assert_eq!(targets, vec![(None, 1), (None, 1)]);
```

修复后应改为 reject duplicate 或 dedup，并加 split_id 唯一性断言。

### 建议处理
与问题 5 一并处理：target resolver reject duplicates；`plan()` 构造完成后可用 debug assertion 检查 split_id 唯一性。

---

## 问题 10：`with_lake_only` 是 public API，但不在正式设计文档中

### 现状
`FlussLakeScan::with_lake_only(bool)` 是 public method，用于 `<table>$lake` 只读 Paimon 快照，不读 Fluss log tail。但 `DESIGN.md` 的 Public API 定稿中没有这个方法，也没有定义 lake-only read shape。

### 影响
公共 API 已扩展但正式设计未更新。下游可以直接调用 `with_lake_only`，但没有文档说明其语义、是否稳定、是否只服务 DataFusion `$lake` 系统表。

### 证据
- `src/api.rs:215-220` public `with_lake_only`。
- `src/table/union.rs:118-120` DataFusion `<table>$lake` 使用该模式。
- `DESIGN.md:172-183` `FlussLakeScan` Public API 列表没有 `with_lake_only`。

### 复现/测试
```bash
rg -n "with_lake_only|lake-only|\\$lake" crates/lake/DESIGN.md crates/lake/src crates/integrations/datafusion/src
```

### 建议处理
若 `with_lake_only` 是正式 public API，则更新 `DESIGN.md` 与 crate docs，明确它是 lake snapshot read，不包含 residual log tail。若只想给 DataFusion 内部使用，应改为 `pub(crate)` 或通过更明确的 `FlussLakeReadMode` 表达。


# Lake Union Read — 临时进度文档

## 分支
- 当前分支: `feat/lake-log-union-read`

## 当前阶段
- M0: 已完成
- M1: 已完成
- M2: 已基本打通,当前处于 **review / cleanup 阶段**
- 当前没有继续写新功能,而是在收口实现质量、整理 review 结果

## 最近关键提交
从新到旧:
- `5a618cf` feat(datafusion): add gated SQL end-to-end test for append lake union
- `3eb54a6` feat(datafusion): validate append-union execution with a gated real-backend test
- `f3b9373` feat(datafusion): route lake-enabled append tables through fluss-lake
- `ccd9064` feat(lake): append-union kernel facade (plan + open partition)
- `f6412ca` feat(lake): real FlussLogTailReader + gated log-tail integration test
- `7404e1e` feat(lake): LogTailReader seam + union orchestration (test-first, faked I/O)
- `d20ff95` feat(lake): LakeSeam — re-key lake snapshot to per-bucket seam offsets
- `f2a546b` feat(lake): lake-side reader + end-to-end seeded-warehouse test
- `d441dd8` feat(lake): kernel core — catalog config, schema alignment, append concat
- `65a9e25` feat(lake): expose lake table props + scaffold fluss-lake kernel crate
- `8afd578` chore(deps): unify arrow/datafusion/pyo3 versions for paimon interop

## 当前已经实现到哪里

### M0
- `TableConfig::get_lake_catalog_properties()` 已完成
- `FlussTableMeta` 已带 `datalake_format` / `lake_catalog_properties`

### M1 (`fluss-lake` kernel)
已完成 append/log 表的 engine-agnostic kernel:
- `config.rs`
- `schema.rs`
- `snapshot.rs` (`LakeSeam`)
- `reader::append`
- `reader::lake`
- `reader::log` (`FlussLogTailReader`)
- `union.rs`
- `plan.rs`
- `append.rs` facade:
  - `plan_append_union(...)`
  - `open_append_partition(...)`

### M2 (`fluss-datafusion` connector)
已完成:
- `FlussUnionTableProvider`
- `FlussUnionScanExec`
- `schema.rs` 路由:
  - lake-enabled append/log 表 -> union provider
  - 普通 append/log 表 -> plain log provider
- gated execution integration test
- gated SQL e2e integration test

## 已有测试 / 验证

### 普通单测/库测试
- `cargo test -p fluss-datafusion --lib` -> **49/49 通过**
- `cargo test -p fluss-lake` -> 通过(含 unit + 非 gated integration)
- `cargo check --workspace` -> 通过

### gated 测试(需要 container runtime)
已经写好并能 compile-check:
- `crates/lake/tests/log_tail_integration.rs`
- `crates/integrations/datafusion/src/execution/union_scan.rs` 内部 integration test
- `crates/integrations/datafusion/tests/integration/lake_union_sql.rs`

### 关于 tiering 的关键现实约束
**当前 Fluss test harness 没有 tiering service。**
所以真实 `get_latest_lake_snapshot()` 在测试环境里不可依赖。

为此,SQL e2e 做了一个 **仅在 `integration_tests` feature 下编译** 的 test-only seam override:
- `crates/integrations/datafusion/src/test_overrides.rs`
- 导出:
  - `set_test_lake_seam_override(...)`
  - `clear_test_lake_seam_override(...)`

生产路径仍走真实 `get_latest_lake_snapshot()`;
只有 gated SQL e2e 会注入手工 `LakeSeam`。

## 当前 review 结论(高优先级)
以下是已经整理出来、值得优先修的 findings:

### 1. lake 侧没有按 `(partition_id, bucket)` 限定读取
- 文件: `crates/lake/src/append.rs`
- 位置: 约第 109 行
- 问题:
  - 每个 union execution partition 都会打开同一张 pinned Paimon snapshot
  - `read_lake_table()` 当前只做 projection,没有把 lake 读限定到当前 `(partition,bucket)`
- 后果:
  - 多 bucket / 多 partition 表会把同一份 lake 数据重复输出多次
  - 不同 bucket/partition 的 lake 数据会混进每个输出分区
- 这是当前最严重的问题,应优先修

### 2. `fluss-lake` 的 finite log tail 边界按 batch 截,不会 trim 超过 `snapshot_end` 的行
- 文件: `crates/lake/src/reader/log.rs`
- 位置: 约第 163 行
- 问题:
  - `last_offset() >= snapshot_end - 1` 时整批直接 push
  - 不会把跨越边界的 batch slice 到 snapshot end
- 后果:
  - 会把 snapshot 边界之后新 append 的行也带进 union 结果

### 3. 旧 DataFusion plain log scan 也有同类边界 bug
- 文件: `crates/integrations/datafusion/src/backend/real.rs`
- 位置: 约第 324 行
- 问题:
  - 旧 `log_scan()` 同样是 batch 级截断,不会 trim 超界行
- 后果:
  - 非 union 的 log finite snapshot 语义也可能读到 snapshot 之后的行
- 这个是独立旧问题,但既然已经确认,最好顺手修掉

### 4. `record_batch()` 对非 append-only batch 的 Arrow payload offset 处理不对
- 文件: `crates/fluss/src/record/arrow.rs`
- 位置: 约第 1119 行
- 问题:
  - `records()` 已更新为跳过 change-type vector
  - `record_batch()` 仍从 `RECORDS_OFFSET` 开始解码
- 后果:
  - 非 append-only/changelog batch 可能 Arrow 解码错误

### 5. malformed `table.datalake.*` 元数据被静默降级成 “no lake”
- 文件: `crates/integrations/datafusion/src/backend/real.rs`
- 位置: 约第 52 行
- 问题:
  - `get_datalake_format().ok().flatten()`
  - `get_lake_catalog_properties().ok().flatten()`
  - parse error 被吞掉
- 后果:
  - misconfigured lake table 可能被当成 plain log table 暴露
  - 或用空 props 继续走 union,行为隐蔽且难排查

### 6. production 路径仍用 `get_latest_lake_snapshot()`
- 文件: `crates/integrations/datafusion/src/table/union.rs`
- 位置: 约第 176 行
- 问题:
  - 还没有切到 readable/safe snapshot 语义
- 后果:
  - 未来在真实 tiering 环境中可能出现不一致 cut
- 当前结论更接近 “值得尽快改”,但优先级低于前 1~4 条

### 7. union provider 通过 downcast 硬依赖 `RealFlussSource`
- 文件: `crates/integrations/datafusion/src/table/union.rs`
- 位置: 约第 165 行
- 问题:
  - `source.as_any().downcast_ref::<RealFlussSource>()`
- 后果:
  - 非生产 source 实现无法支持 union read
- 更多是架构问题,不是立刻会破 correctness 的第一优先级

### 8. 每个执行分区都重复 reopen paimon catalog/table
- 文件: `crates/lake/src/append.rs`
- 位置: 约第 105 行
- 问题:
  - 每个 `open_append_partition()` 都 `open_catalog()` + `get_table_at_snapshot()`
- 后果:
  - 多 partition 查询会产生重复 catalog/metastore 开销
- 明显是效率问题,但优先级低于 correctness bug

## 建议下一步(回来后优先顺序)
建议按以下顺序修:

1. **先修最严重 correctness bug**
   - lake 侧按 `(partition_id, bucket)` 限定读取
   - 这可能需要:
     - 在 kernel planner 中拿到/保留足够的 bucket/partition 约束
     - 把这些约束真正下推到 paimon 读侧(或至少在 lake splits 级过滤)

2. **再修 finite snapshot 边界 trim**
   - `crates/lake/src/reader/log.rs`
   - `crates/integrations/datafusion/src/backend/real.rs`
   - 参考现有 `RecordBatchLogReader::new_until_offsets(...)`/相关 slice 逻辑,按 row trim 到 stop offset

3. **修 `record_batch()` 非 append-only offset bug**
   - `crates/fluss/src/record/arrow.rs`

4. **决定 malformed lake config 的失败策略**
   - 更倾向显式 error,不要静默降级

5. **如果还有时间**
   - readable snapshot
   - 去掉 `RealFlussSource` downcast
   - catalog reopen 优化

## 当前工作树状态(写文档时)
- `Cargo.lock` 有未提交修改
- `.claude/` 目录未纳入 git
- `fluss-rust-community.iml` 未纳入 git

## 备注
如果后面切到别的分支再回来,建议先从这里继续:
1. 先修 `crates/lake/src/append.rs` 的 lake-side full snapshot 问题
2. 修完后马上补/调整 gated SQL e2e,确保它能抓住多 bucket / 多 partition 重复数据问题

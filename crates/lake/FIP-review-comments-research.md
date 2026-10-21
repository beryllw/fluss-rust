# FIP 评审评论调研：POC 现状与业界实现对照

> 目的：针对 `crates/lake/FIP.md` 草稿收到的 11 组评论，逐条给出
> ① 原始评论与翻译 ② 当前 POC 代码的真实行为 ③ Fluss Java 侧（fluss-community）
> 及业界项目（Paimon Java / paimon-rust / iceberg-rust / DataFusion / Flink）的对应实现
> ④ 业界解决该问题的通用模式。**本文不代拟回复**，只提供事实与弹药。

代码基线：
- POC：`fluss-rust-community/crates/lake`（依赖 `beryllw/paimon-rust` tag `v0.3.0-fluss.1`，rev `e5042e7`，该 tag 已含 `kv_file_reader.rs` / `sort_merge.rs`）
- Java：`fluss-community`（fluss-client / fluss-server / fluss-flink-common / fluss-lake-paimon）
- Paimon Rust 上游：`paimon-rust`（本地 main）

---

## 评论 1：latest readable snapshot 的语义

**原始评论**
> What makes a snapshot "readable" as opposed to simply the latest, and how is that boundary decided? Are the two ever different?

**翻译**：什么使得一个 snapshot 是"可读的"（readable），而不只是"最新的"（latest）？这个边界由谁决定？两者会不一样吗？

### POC 现状

- `api.rs` `snapshot_boundary()` 调用的是 `admin.get_latest_lake_snapshot()`，即 **latest**，不是 readable。fluss-rust 的 `Admin`（`crates/fluss/src/client/admin.rs:310`）目前**只有** `get_latest_lake_snapshot`，没有 readable 变体（RPC 层未设置 `readable=true` 标志）。
- `snapshot.rs` 的模块注释里预留了 "the future readable variant" 的说法，但 FIP 正文写的是 "latest readable"，**与实现不一致**。
- POC 目前拒绝 DV（deletion-vector）表（`pk.rs::validate_pk_merge_engine`），而非 DV 表 latest == readable，所以当前行为**碰巧正确**，但 FIP 的措辞超前于实现。

### Java 侧事实（这是回答该评论的权威依据）

- `Admin.java` 定义了两个 API（`fluss-client/.../admin/Admin.java` L462–L529）：
  - `getLatestLakeSnapshot`：绝对最新的已提交 snapshot；
  - `getReadableLakeSnapshot`：最新**可读** snapshot。Javadoc 明确说明：对 Paimon DV 表，tiered offset 对应的数据可能还在 L0（未 compact，对 DV 读不可见），直接用 latest 会**丢数据**；union read 必须用 readable。
- 边界由 **tiering committer 在 commit 时决定**（`PaimonLakeCommitter.java` L122–148）：
  - 非 DV 表：`committedIsReadable=true`，committed 即 readable，两者**永远相同**；
  - DV 表：`DvTableReadableSnapshotRetriever` 在 commit 时回溯找出"L0 已 compact 完成"的 snapshot 作为 readable snapshot，并计算其 per-bucket `readableLogEndOffsets`（与 `tieredLogEndOffsets` 区分，见 `LakeCommitResult.ReadableSnapshot`，两组 offset 可以不同）。
- 存储与查询：ZK `LakeTable` v2 元数据中每个 snapshot 记录 `tieredOffsetsFilePath` 与可空的 `readableOffsetsFilePath`；server 端 `readable=true` 请求时**从新到旧**扫描第一个 `readableOffsetsFilePath != null` 的 snapshot（`LakeTable.getOrReadLatestReadableTableSnapshot`，`RpcServiceBase.getLakeSnapshot` L495–558）。
- 另一个"latest ≠ readable"的来源：latest 位置可能是一个 compaction-only snapshot（`LakeTable.getLatestLakeSnapshotMetadata` 中的 TODO #2625）。
- Flink `LakeSplitGenerator.generateHybridLakeFlussSplits()` L90 用的正是 `getReadableLakeSnapshot`。

### 业界模式

- 本质是"物理已提交（committed/tiered）"与"读取安全（readable/visible）"分离，类似 Kafka 的 LEO vs high-watermark、Iceberg 的 snapshot vs `current-snapshot-id` 指针。边界统一由**写端（tiering commit）一次性判定并落元数据**，读端只做一次 RPC 选择，不在读路径上推断。
- 对 POC 的含义：需要在 fluss-rust RPC 的 `GetLakeSnapshotRequest` 上补 `readable=true` 标志并新增 `get_readable_lake_snapshot`；在 DV 表支持之前，可在 FIP 中明确"v1 仅支持非 DV 表，此时 latest == readable"。

---

## 评论 2：scan 配置单独传输，read 侧漂移怎么办

**原始评论**
> If the read-side projection/filter drifts from what plan() froze, what happens — silent wrong shape, or is there a check to keep them in sync?

**翻译**：如果 worker 端 read 使用的 projection/filter 与 plan() 冻结时的不一致，会发生什么——静默产出错误形状的数据，还是有一致性校验？

### POC 现状

- `FlussLakeScanConfig`（`api.rs` L77–92）是可序列化的完整 scan 配置；engine 负责把它与 split 分别传给 worker，worker 重建 `FlussLakeScan` / `FlussLakeRead`。
- `read_split()` 的输出 schema 完全由 **read 侧自己的 config** 推导（`projected_schema(&cfg.schema, ...)`）。**没有任何机制**校验 read 侧 config 与 plan 时的 config 一致：
  - projection 漂移 → 输出列集合/顺序与 `plan.schema()` 不一致，**静默错形**；
  - filter 漂移 → 结果集语义悄然改变；
  - split 里只有 `descriptor_version` + 执行边界，不含 config 指纹，无从检测。
- FIP Test Plan 里写了 "`plan.schema()` 与输出 batch schema 一致" 的不变量，但该不变量目前只在"engine 正确透传同一份 config"的前提下成立。

### 业界实现

- **Flink（Fluss Java）**：不存在"worker 重建配置"的问题面——`projectedFields` / pushed filters 在 JobGraph 构建时就固化进序列化后的 Source 对象（`FlussSourceBuilder` L365–383 把投影/filter 设置进 `LakeSource`，随 source 一起分发到 TM），JM/TM 共享同一份序列化实体，**单一事实源，物理上无漂移可能**。
- **Spark DSv2 / Iceberg**：`Scan` 在 driver 上定型后，`readSchema()`、filter 一起随 scan/task 广播；`FileScanTask` 不单独携带，靠"同一个 scan 对象派生 task 与 reader"保证一致。
- **DataFusion**：物理计划节点（ExecutionPlan）同时携带 projection/predicate 与分区（partitions ≈ splits），二者不可能分离传输。

### 业界解法归纳

1. 结构上消除漂移：让 plan 产物（或 scan 对象本身）成为唯一分发实体，worker 不"另行组装"（Flink/Spark 路线）；
2. 若架构上必须分开传（POC 现状），用**指纹校验**兜底：plan 时把 config 的 hash（如 projection+filter+lake_only 的摘要）写进每个 split，`read_split` 时比对，不一致返回显式错误（如 `PlanningFailed`/新错误变体），把"静默错形"变成"确定性报错"。

---

## 评论 3：descriptor_version 的兼容契约

**原始评论**
> If a reader is older than the planner's descriptor_version, is that mismatch detected and rejected, or undefined? What's the compatibility contract?

**翻译**：如果 reader 比 planner 的 descriptor_version 旧，这种不匹配是被检测并拒绝，还是未定义行为？兼容契约是什么？

### POC 现状

- `api.rs`：`DESCRIPTOR_VERSION: u32 = 1` 只在**写入** split 时设置；`decode_task()` 反序列化 `execution_descriptor`（serde_json）时**完全不看** `split.descriptor_version`。
- 后果：旧 reader 读新版本 descriptor，行为取决于 JSON 结构是否恰好兼容——可能报一个含糊的 `Internal("failed to decode split execution descriptor")`，也可能因字段默认值而**静默误读**。即当前是**未定义**，评论一针见血。

### 业界实现

- **Fluss Java / Flink**：`SimpleVersionedSerializer` 模式（`LakeSplitSerializer.java`）——序列化时先写 `getVersion()`，反序列化入口是 `deserialize(version, bytes)`，**版本号显式传入**，实现方对未知版本抛 `IOException("unsupported version")`。Flink 整个 checkpoint/split 体系都是这个契约：*reader 必须显式处理每个已知版本，未知版本必须拒绝*。
- **Iceberg**：manifest/metadata 带 `format-version`，读端遇到高于自身支持的 format-version 直接拒绝（"Cannot read table with format version X"）。
- **Protobuf 路线**（Fluss RPC 本体）：靠字段级前后兼容而不是整包版本号，但那要求 wire format 是 protobuf 而非 JSON。

### 业界解法归纳

契约应写成三条：
1. reader 支持的最大版本 `N`；`descriptor_version <= N` → 必须能读（向后兼容由 encoder 保证，新增字段只能是 optional/additive）；
2. `descriptor_version > N` → **必须检测并显式拒绝**，错误信息包含双方版本号（建议映射到一个明确的错误变体，而不是 `Internal`）；
3. `decode_task` 以 `split.descriptor_version` 分派解码逻辑（哪怕 v1 只有一个分支，也要先加"version > 1 即拒绝"的哨兵检查）。

---

## 评论 4：DataUnavailable 与 stop offset 永远等不到

**原始评论**
> Does the caller get a clear "re-plan and retry" here vs a generic failure? And if the stop offset is never reached, does the read give up or can it wait forever?

**翻译**：调用方能得到明确的"请重新 plan 再重试"信号，还是只是一个泛化失败？如果 stop offset 一直到不了，读取会放弃还是永远等下去？

### POC 现状（与 FIP 承诺有差距）

- `FlussLakeError::DataUnavailable` 只是**声明了但从未被抛出**。真实路径：log offset 被 truncate → fluss-rust scanner 在 `initialize_fetch`（`crates/fluss/src/client/table/scanner.rs` L1866–1873）把 `LogOffsetOutOfRangeException` 转成 `Error::UnexpectedError`，再经 `error.rs` 的 `From<fluss::error::Error>` **统一映射成 `ConnectionError`**。调用方拿到的是泛化失败，无法区分"可通过 re-plan 恢复"与"网络问题"。
- 等待行为两条路径不一致：
  - append 路径 `reader/log.rs` `read_tail`：`while !reached_end { poll... if empty continue }` —— **没有 deadline、没有空轮上限**，若既无错误又收不到数据（理论上 stop 冻结 ≤ latest，正常应能到达），会**永远等**；
  - PK 路径 `reader/changelog.rs`：有 `DEADLINE=60s`（超时报 `Internal`）和 `MAX_EMPTY_POLLS=20`；但注意 **`MAX_EMPTY_POLLS` 触发时是 `break` 而不是报错**——即连续 20 次空轮询后带着**不完整的 tail 静默返回**，这是一个正确性隐患（应报错或至少与 deadline 行为对齐）。

### 业界实现

- **Fluss Java**：`LakeSnapshotAndLogSplitScanner.pollLogRecords` 同样轮询到 `stoppingOffset-1` 为止，无 deadline；offset 越界时 `LogScanner.poll` 抛 `LogOffsetOutOfRangeException`，**没有自动 re-plan**，任务失败靠引擎重试/作业重启（重启后 enumerator 重新调用 `getReadableLakeSnapshot` 生成新 splits，等效于 re-plan）。
- **Kafka**：`OffsetOutOfRangeException` 是**专用异常** + `auto.offset.reset` 策略（earliest/latest/none），语义上就是"给调用方一个明确的、可编程的恢复路径"。
- **Iceberg**：scan 引用的 snapshot 被 expire 后读取报明确的 `ValidationException`/`NotFoundException`，引擎层的重试会重新 resolve snapshot（即天然 re-plan）。
- **DataFusion**：无内建重试，靠错误类型区分是否可重试。

### 业界解法归纳

1. 把"数据被 truncate/snapshot 失效"映射到**专用错误变体**（POC 已有 `DataUnavailable`，缺的是 fluss `LogOffsetOutOfRangeException` → `DataUnavailable` 的精确映射，而不是笼统 `ConnectionError`），并在文档里写明"收到 `DataUnavailable` ⇒ plan 已失效，请重新 `plan()` 后重试"；
2. 有界读不应无限等待：给 append 路径补 deadline（对齐 changelog 路径），deadline 到期报**错误**而非静默截断；同时修掉 changelog 路径 `MAX_EMPTY_POLLS` 静默 break 的问题。

---

## 评论 5：exact filter 对"推不下去"的谓词是否成立

**原始评论**
> For a predicate that doesn't fit/can't be pushed, does the guarantee hold or must the engine filter?

**翻译**：对于无法表达/无法下推的谓词，"无需 residual filtering"的保证还成立吗？还是引擎必须自己过滤？

### POC 现状

- `FlussLakePredicate` 是**封闭 enum**（`predicate.rs`）：只有 AlwaysTrue/False、And/Or/Not、IsNull、In、Comparison。LIKE、函数调用、列间比较等**根本无法构造**成 `FlussLakePredicate`。
- 因此保证的准确表述是：**"凡是能推进来的谓词，输出严格满足（exact）；推不进来的谓词从 API 上就进不来，天然仍是引擎的责任"**。FIP 中 "Upstream engines need not apply residual filtering" 这句话省略了"对已下推谓词"这个限定词，评论指出的正是这个表述漏洞。
- exact 的实现：`read_split` 打开（union 后的）流之后统一走 `apply_exact_filter`（arrow `filter_record_batch`），filter 引用列会并入读取投影（`read_projection_with_filter_columns`），过滤后再投影回输出列。所以对可表达谓词，保证是真实成立的。
- 注意一个已知遗留：`reader/lake.rs` 目前只把**分区等值谓词**推给 paimon，数据列谓词没有下推给 paimon 做文件级 pruning（只靠最后的 exact filter 兜底），这是性能问题而非正确性问题。

### 业界实现

- **DataFusion**：`TableProvider::supports_filters_pushdown` 逐条返回 `Exact / Inexact / Unsupported`；`Exact` 才会从计划里删掉 FilterExec。本仓库 `crates/integrations/datafusion/src/table/kv.rs` L138–202 正是这个三态模型的谨慎实现（只有匹配 exec 真正强制的谓词才标 `Exact`）。
- **Fluss Java**：`LakeSource.withFilters` 返回 `FilterPushDownResult{consumed, unconsumed}`（`PaimonLakeSource.java` L76–95：能转成 Paimon Predicate 的算 consumed，其余 unconsumed 由 Flink 保留 residual filter）。
- **iceberg-rust**：pushed predicate 经 `RowFilter`（`crates/iceberg/src/arrow/reader/row_filter.rs`）在 Parquet 解码时精确应用——FIP 引用的正是这个模式；不能表达成 Iceberg `Predicate` 的表达式由引擎保留。

### 业界解法归纳

业界从不承诺"引擎永远不用 residual filter"，而是承诺**逐谓词的显式契约**（Exact/Inexact/Unsupported 或 consumed/unconsumed）。FIP 只需把措辞收紧为："对通过 `with_filter` 传入的 `FlussLakePredicate`，SDK 保证 exact；无法表达为 `FlussLakePredicate` 的谓词不在契约范围内，由引擎负责"。POC 行为本身无需改动。

---

## 评论 6：append 表 lake 侧读取的并行粒度

**原始评论**
> For append tables, is the lake read parallel per bucket, or does it all land on one task?

**翻译**：对 append 表，lake 侧读取是按 bucket 并行，还是全部落在一个 task 上？

### POC 现状

- split 粒度统一为 `(partition, bucket)`（`api.rs::plan` 对 `0..num_buckets` 逐一生成 split）；append split 执行时 `read_lake_table(..., Some(split.bucket), ...)` 把 paimon plan 出的物理 split 过滤到本 bucket（`reader/lake.rs` L69–77）。
- 所以答案是：**按 bucket 并行，不是单 task**；但也**不比 bucket 更细**——一个 bucket 若积累了大量 lake 文件，仍由单个 split/task 承担（潜在长尾）。另外每个 split 执行时都独立做一次 paimon `plan()` 再过滤 bucket，存在重复 planning 开销。

### 业界实现

- **Fluss Java（Flink）**：append 表拆分更细——`LakeSplitGenerator.generateSplit`（isLogTable 分支）把每个 Paimon `LakeSplit`（文件组级）包成独立的 `LakeSnapshotSplit`，log tail 则是每 bucket 一个 `LogSplit`；即 **lake 侧 = 文件组级并行，log 侧 = bucket 级并行**，两类 split 独立调度到不同 task。这可行的前提是 append 语义下 lake/log 拼接不需要在同一 task 内完成（无 merge，全局不重不漏由 seam 保证）。
- **PK 表**（对照）：Java 也必须回到 bucket 粒度（`LakeSnapshotAndFlussLogSplit` 每 bucket 一个），因为 merge 必须在 bucket 内完成——和 POC 的 PK 粒度一致。
- **Paimon/Iceberg 独立批读**：都是文件/文件组级 split（`split_for_batch` bin-packing、Iceberg FileScanTask by target-split-size）。

### 业界解法归纳

append 表没有 merge 约束，业界通行做法是 lake 部分放开到**物理 split 级并行**。POC 若要对齐，需要 public split 支持"纯 lake split"与"纯 log tail split"两种形态（或在 split 内携带物理 split 句柄的不透明引用），这与 FIP "不暴露物理 Paimon split" 的原则需要权衡——Java 的做法是用 `LakeSplit` 接口 + 各 lake 格式自带 `SimpleVersionedSerializer`，做到"可分发但对引擎不透明"。

---

## 评论 7：merge 覆盖哪些 merge engine

**原始评论**
> Which merge engines are covered, and what happens for the rest?

**翻译**：PK merge 覆盖哪些 merge engine？其余的会发生什么？

### POC 现状

- `pk.rs::validate_pk_merge_engine`：v1 只接受 **`deduplicate`**（含未设置时的默认值），显式**拒绝** `partial-update`、`first-row`，以及任何开启 `deletion-vectors.enabled` 的表；拒绝时报 `Internal`（错误变体选择欠妥，语义上更接近 `NotLakeReadable`/`Unsupported`）。
- FIP 第 10 节只写了 "Merge applies upsert/delete by primary key"，**没有声明该限制**——评论要求把支持矩阵写进契约。

### 事实依据（为什么 deduplicate 是合理的 v1 范围）

- Fluss tiering 写入 Paimon 的 PK 表**不设置 merge-engine**（fluss-lake-paimon 主代码中无任何 merge-engine 配置），即默认 deduplicate；Fluss 自身的 merge 语义（如 first_row/versioned）在 Fluss KV 层生效后产出完整 changelog，湖上只需 last-write-wins 重放。因此"Fluss tiering 产出的表"范围内 deduplicate 即全集。
- 风险面在于**用户直读外部已存在的 Paimon 表**或未来 tiering 支持配置 merge-engine 时。
- paimon-rust（POC 所用 tag 即已包含）`kv_file_reader.rs` 的 `MergeFunction` 已支持 `Deduplicate` 和 `PartialUpdate`，`FirstRow` 显式 Unsupported——即底层能力略超 POC 声明，POC 是保守收窄。

### 业界实现

- **Fluss Java**：union read 链路（`LakeSnapshotAndLogSplitScanner` + `SortMergeReader`）本身也只实现 last-write-wins 覆盖语义（changelog 覆盖 snapshot、delete 删行），同样隐含 deduplicate 假设，且**没有显式校验** merge-engine——这点 POC 的显式校验反而更严谨。
- **Paimon Java** 引擎侧：不同 merge-engine 由 `MergeFunction` 工厂分派（DeduplicateMergeFunction / PartialUpdateMergeFunction / AggregateMergeFunction / FirstRowMergeFunction），不支持的组合在 planning 期显式报错——"能力矩阵 + 提前失败"是标准姿势。

### 业界解法归纳

FIP 应补一段能力矩阵：v1 支持 `deduplicate`（无 DV）；其余 merge-engine / DV 表在 `plan()` 或 `open()` 阶段**提前**返回明确错误（而不是读到一半发现）；未来按 paimon-rust `MergeFunction` 的演进逐项放开（partial-update 底层已具备）。

---

## 评论 8：对 Paimon reader 的依赖面与全部 gap

**原始评论**
> How much of this depends on the underlying Paimon reader? What are the other gaps? So it's a bit clearer what is missing, what we can deliver in paimon-rust, what will be the next steps for union-read here

**翻译**：这（bucket 级有序读缺失）多大程度上取决于底层 Paimon reader？还有哪些其他 gap？希望说清楚：缺什么、哪些可以在 paimon-rust 里补齐、union-read 这边下一步是什么。

### 依赖面拆解（paimon-rust 当前真实能力，rev e5042e7 / 本地 main 一致）

| 能力 | paimon-rust 现状 | Java Paimon 对照 |
| ---- | ---- | ---- |
| split 生成 | `table_scan.rs` L725：每 `(partition,bucket)` 的文件**按大小 bin-pack**（`split_for_batch`），不按 key 区间分组 → 同 bucket 的多个 split key 区间可重叠 | `MergeTreeSplitGenerator` 用 `IntervalPartition` 把 key 区间重叠的文件分进**同一个** split（section），跨 split key 区间不相交，并计算 `rawConvertible` |
| split 内读取 | `kv_file_reader.rs`：split 内每文件一条 sorted stream，LoserTree sort-merge + `MergeFunction`（Deduplicate/PartialUpdate）→ **split 内正确去重、key 有序** | `MergeTreeReaders` + `SortMergeReader`，同样 split 内归并 |
| 跨 split | `try_stream!` 逐 split 顺序拼接，**无跨 split 归并**；`read_pk` 还把"无 L0 的 split"走 raw 路径并与 KV 路径 `select_all` **交错**输出 | 因为 split 生成阶段已保证跨 split key 不相交，顺序拼接即安全 |
| raw 判定 | "split 无 L0 文件 ⇒ raw 读"（`table_read.rs` L109–117）——多个 >0 level 之间 key 仍可重叠，此判定**不充分** | `rawConvertible`：单 sorted run 才可 raw 读 |
| DV 表 | KV 路径显式拒绝 DV（`kv_file_reader.rs` L277–284） | 支持 |
| 统计 | `merged_row_count()` 已存在（`can_push_down_limit_hint_for_scan` 相关） | 支持 |

**结论**：FIP 所述"只保证单物理 split 内 key 有序"完全属实，且根因不在 reader 而在 **split 生成算法**（bin-pack vs IntervalPartition）。这是 paimon-rust 侧可交付、且 Java 有现成参考实现的改造。

### 可在 paimon-rust 交付的事项（按依赖排序）

1. **IntervalPartition/MergeTreeSplitGenerator 等价物**：PK 表按 key 区间 section 分组生成 split，保证"一个 key 只出现在一个 split"（直接消除评论 9 的重复 key 问题）；
2. **正确的 rawConvertible 判定**：替换"无 L0 即 raw"的近似；
3. `read_pk` 中 KV/raw 两路输出**不 select_all 交错**（section 化后自然消失）；
4. DV 表 KV 读（deletion vector 应用到 KV 路径）；
5. FirstRow / Aggregation MergeFunction（远期）。

### union-read（fluss-lake）侧的其他 gap 清单

1. **readable snapshot RPC**（评论 1）：fluss-rust Admin 增加 `readable=true`；
2. **DataUnavailable 精确映射 + append tail deadline**（评论 4）；
3. **descriptor_version 校验**（评论 3）；
4. **统计信息未接线**：FIP 承诺 `estimated_rows/size` 来自 `DataSplit.merged_row_count()`，POC 中 split 一律 `None`（`api.rs` L294–295）；
5. **数据列谓词未下推 paimon**（评论 5，性能）；
6. **append lake 侧并行粒度**（评论 6，可选增强）；
7. **每 split 重复 open catalog + plan** 的执行开销（`append.rs`/`pk.rs` 每次 `open_catalog + get_table_at_snapshot + plan`）；
8. merge 策略切换位（评论 10/11）：待 1–3 落地后 PK merge 可从 hash overlay 切换 sort-merge。

---

## 评论 9：跨 split 重复 key 与 one-row-per-key

**原始评论**
> If splits are concatenated without merging, and a key can live in more than one split of a bucket, doesn't the lake current state return that key twice (an old value next to the new one)? How does the current view stay one-row-per-key for a bucket bigger than one split?

**翻译**：既然 split 之间只拼接不归并，而一个 key 可能存在于同一 bucket 的多个 split 中，那 lake current state 不会把这个 key 返回两次（旧值挨着新值）吗？bucket 大于一个 split 时，current view 如何保持每 key 一行？

### POC 现状（评论指出的是**真实的正确性缺口**）

- 触发条件链：bucket 文件总量超过 `target_split_size`（默认 128MB）→ `split_for_batch` 把该 bucket 切成多个 key 区间重叠的 split → 同一 key 的新旧版本落在不同 split → `kv_file_reader` 只在 split 内去重 → **同 key 输出两行**。另外"无 L0 split 走 raw 读"的路径中，>0 各 level 之间同 key 也不会去重。
- POC 的 `pk_merge`（hash overlay）只处理 lake vs changelog-tail 的覆盖，**不会去除 lake 流内部的重复 key**——重复会原样进入 current view。
- 当前测试/E2E 未暴露该问题的原因：数据量小，每个 bucket 一个 split；即"one-row-per-key 目前是条件性成立（bucket ≤ 单 split），不是结构性保证"。这一点在 FIP 10.1 里承认了排序缺口，但没有点破**重复 key**这个更严重的推论，评论补上了。

### 业界解法

- **Paimon Java（根治）**：split 生成时用 `IntervalPartition` 保证跨 split key 区间不相交——"一个 key 只属于一个 split"是**计划期不变量**，读端无需跨 split 协调。这就是评论问题的标准答案，也是上面评论 8 中 paimon-rust 交付项 #1。
- **Fluss Java union read**：`SortMergeReader.ConcatRecordIterator`（名为 concat 实为 PriorityQueue k 路归并）把同 bucket 多个 sorted lake iterator 归并后再与 changelog 二路归并——但注意它**也不做跨 iterator 同 key 去重**，正确性同样依赖 Paimon Java "key 不跨 split" 的前提。
- **短期缓解（kernel 侧，如果不想等 paimon-rust）**：PK 路径对单 bucket 的所有 lake split 做一次跨 split 归并去重（利用 split 内已有序 + 文件 level/sequence 信息），或干脆把整 bucket 文件并成一个"逻辑 split"交给 `kv_file_reader`（牺牲 split 大小控制换正确性）。

---

## 评论 10：sort-merge 真的省内存吗

**原始评论**
> Would sort-merge actually lower memory here? Both approaches still buffer the whole tail - curious what the real win is.

**翻译**：切到 sort-merge 真能降内存吗？两种方案不都得把整个 tail 缓冲下来？真正的收益是什么？

### 事实核对（评论的质疑基本成立）

- POC hash overlay：tail 物化成一个 RecordBatch + `HashMap<PkKey, Option<usize>>`，内存 `O(tail)`；lake 侧流式。
- 假想的 sort-merge：changelog 按 **offset** 序到达，必须先攒齐 `[seam, stop)` 并按 key 排序才能归并——**同样 `O(tail)`**。Java 的 `LakeSnapshotAndLogSplitScanner` 就是把 tail 全量塞进 `TreeMap`（按 key 排序）再归并，内存也 `O(tail)`。
- 所以"in-memory 场景下 sort-merge 降内存"这个说法**不准确**，FIP 10.1 的措辞需要修正。

### sort-merge 的真实收益（业界视角）

1. **可溢写（spillable）**：sorted-run + 外部归并排序可以把 tail 排序降到 `O(memory budget)`（DataFusion `ExternalSorter`、Paimon 写端 sort spill 都是此路线）；hash overlay 溢写后随机探测代价极高，实际不可溢写。这才是"大 tail"时的内存论据——前提是实现外排，而非单纯换算法；
2. **输出 key 有序**：为下游保序合并、range 分片、增量物化提供性质，hash overlay 输出无序；
3. **对称的全流式结构**：当 lake 侧具备 bucket 级有序读（评论 8/9 的前置）且 tail 来源未来支持按 key 有序供给（如从 lake changelog 读）时，可退化为纯两路归并 `O(1)` 状态。
4. 客观代价：hash overlay 对小 tail（预期主流场景：tiering 滞后有限）常数更小、实现简单。

### 业界解法归纳

诚实的表述应为："两种算法在 tail 全内存时均为 `O(tail)`；sort-merge 的价值在于可外排溢写、输出有序、并在底层提供 bucket 级有序读后收敛为 O(1) 状态的全流式归并"。Java 现状（TreeMap 全缓冲）也证明大家在小 tail 假设下都接受 `O(tail)`。

---

## 评论 11：Upstream engine 是谁；先单机后分布式？

**原始评论**
> What are the potential choices for this "Upstream engine" role? And is the plan to support single-node first, then distributed later?

**翻译**："上层 engine" 的候选有哪些？计划是不是先支持单机、再支持分布式？

### 仓库内事实

- 已存在的第一个 engine：**DataFusion 集成**（`crates/integrations/datafusion`，`lake` feature 依赖 `fluss-lake`，暴露 `<table>` union read 与 `<table>$lake` lake-only 两种表名），单进程执行——即事实上的"单机优先"。
- API 设计中的分布式伏笔已经埋好：`FlussLakeReadSplit: Serialize+Deserialize`、`FlussLakeScanConfig` 可序列化、`FlussLakeRead: Send+Sync`——正是"planner 在 coordinator，read 在 worker"的分布式形态所需的三要素。
- 其他自然候选（从仓库上下文推断）：Python binding（`bindings/`，如 Ray/单机 pandas/polars 消费 Arrow 流）、未来的 Ballista/自研分布式调度、以及任何能消费 Arrow `RecordBatchStream` + 自带 split 调度的引擎。

### 业界对照

- **Java Fluss**：engine 角色由 Flink（batch/streaming）承担，Spark 连接器在建；分布式形态从第一天就是目标，所以其契约以 Flink Source API（enumerator/reader、SimpleVersionedSerializer）为形。
- **iceberg-rust / paimon-rust**：同样是"kernel + engine adapter"分层，第一个 adapter 都是 DataFusion（单机），分布式由引擎自身（如 Ballista、或 Java 引擎走各自原生实现）解决——kernel 只承诺 split 可序列化、reader 可并发，不自己做调度。POC 的边界划分与此一致。
- 建议 FIP 补一句明确的路线声明：v1 验证目标是 DataFusion 单进程（split 并行 = 进程内并发）；契约（可序列化 split/config）已为分布式引擎预留，分布式调度本身永远是 engine 职责，不进入 fluss-lake。

---

## 附录：各评论涉及的关键代码索引

| 评论 | POC | Java / 业界 |
| ---- | ---- | ---- |
| 1 | `api.rs::snapshot_boundary`, `crates/fluss/src/client/admin.rs:310` | `Admin.java` L462–529, `RpcServiceBase.java` L495–558, `LakeTable.java` L175–193, `LakeCommitResult.java`, `PaimonLakeCommitter.java` L122–148 |
| 2 | `api.rs` `FlussLakeScanConfig` / `read_split` | `FlussSourceBuilder.java` L365–383 |
| 3 | `api.rs` `DESCRIPTOR_VERSION` / `decode_task` | `LakeSplitSerializer.java` L49–130 |
| 4 | `error.rs` From impls, `reader/log.rs` L181–197, `reader/changelog.rs` L90–121, `crates/fluss/.../scanner.rs` L1866 | `LakeSnapshotAndLogSplitScanner.java` L224–241 |
| 5 | `predicate.rs`, `api.rs::read_split`, `reader/lake.rs` | `PaimonLakeSource.java` L76–95, `crates/integrations/datafusion/src/table/kv.rs` L138–202, iceberg-rust `arrow/reader/row_filter.rs` |
| 6 | `api.rs::plan`, `reader/lake.rs` L69–77 | `LakeSplitGenerator.java` L220–295 |
| 7 | `pk.rs::validate_pk_merge_engine` | `LakeSnapshotAndLogSplitScanner.java`, paimon-rust `kv_file_reader.rs` L101–116 |
| 8/9 | `pk.rs`, `reader/merge.rs` | paimon-rust `table_scan.rs` L725, `table_read.rs` L100–131, `kv_file_reader.rs` L274–350; Fluss Java `SortMergeReader.java` L209–278; Paimon Java `MergeTreeSplitGenerator`/`IntervalPartition` |
| 10 | `reader/merge.rs`, `reader/changelog.rs` | `LakeSnapshotAndLogSplitScanner.java` L67, L224–241（TreeMap 全缓冲） |
| 11 | `crates/integrations/datafusion`（`lake` feature） | Flink Source API；iceberg-rust/paimon-rust 的 DataFusion adapter 分层 |

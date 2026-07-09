# fluss-datafusion 待办问题清单

本文件记录审阅 `crates/integrations/datafusion` 当前设计、公共接口与实现后发现的待办问题。每条问题包含现状、影响、证据、复现或测试方法、建议处理方向。

本地验证基线：

```bash
cargo test -p fluss-datafusion
cargo test -p fluss-datafusion --features lake
```

以上两个命令已通过。下面的问题主要是设计/文档/接口契约漂移，而不是当前默认测试失败。

---

## 问题 1：文档声明 Log 表必须 LIMIT，但实现已支持无 LIMIT 快照扫描

### 现状
`docs/README.md` 和 `src/lib.rs` 模块文档仍声明 Log 表必须带 `LIMIT`，否则在 planning 阶段报错。实际 `FlussLogTableProvider` 已支持无 `LIMIT` 的 earliest->latest 有限快照扫描：当 `limit = None` 时 `scan()` 直接构造 `FlussLogScanExec`，`row_limit = None` 由 `RealFlussSource::log_scan` 处理为读至 query start 时刻的 latest offset。

### 影响
面向使用者和 fluss-gateway 的文档与实际行为不一致。用户读到 README 会误以为 `SELECT * FROM log_t` 会失败，实际它返回一个有限快照。文档中的 "Forms that fail conservatively" 表格因此也是错的。

### 证据
- `docs/README.md:121-137` 只列 `LIMIT n`，并写 `SELECT * FROM log_t` 会 `LIMIT required`。
- `src/lib.rs:29-34` 文档注释写 Log 表要求 `LIMIT`。
- `src/table/log.rs:130-189` `scan()` 在 `limit = None` 时不会报错。
- `docs/design.md:35-37, 299-312` 已正确描述无 `LIMIT` 快照扫描，说明 README/lib.rs 注释滞后。

### 复现/测试
静态复现：

```bash
rg -n "LIMIT required|requires.*LIMIT|without `LIMIT`" crates/integrations/datafusion/docs crates/integrations/datafusion/src
```

行为测试（需容器）：

```bash
cargo test -p fluss-datafusion --features integration_tests
```

建议新增一个集成断言：

```rust
let batches = ctx
    .sql("SELECT id, name FROM fluss.<db>.<log_table>")
    .await?
    .collect()
    .await?;
assert!(batches.iter().map(|b| b.num_rows()).sum::<usize>() > 0);
```

### 建议处理
更新 `docs/README.md`、`docs/README.zh.md` 与 `src/lib.rs`，使其与 `src/table/log.rs` 和 `docs/design.md` 一致。保留 `LimitRequired` 错误枚举即可，因为其他路径仍可能使用。

---

## 问题 2：设计文档声明非 lake KV 全表扫描，但实现会拒绝

### 现状
`docs/design.md` 声明 KV 表支持第四种读形状：无 `LIMIT` 的 full-table scan via changelog merge，并引用 `kv_full_scan` / `FlussKvFullScanExec`。当前实现中，非 lake KV 表在无完整 PK/prefix lookup 且无 `LIMIT` 时直接返回 `UnsupportedQueryPattern`。`FlussSource` trait 也没有 `kv_full_scan` 方法。

### 影响
`SELECT * FROM kv_table` 在设计文档中像是支持的，但实际非 lake KV 表会报错。维护者按设计文档查找 `FlussKvFullScanExec` / `kv_full_scan.rs` 也会被误导。

### 证据
- `docs/design.md:28-34` 列出 KV full-table scan。
- `docs/design.md:184-189, 328-333, 375-381` 引用 `kv_full_scan` 和 `FlussKvFullScanExec`。
- `src/backend/mod.rs:157-235` `FlussSource` trait 没有 `kv_full_scan`。
- `src/table/kv.rs:306-318` 无 `LIMIT` 时返回 `UnsupportedQueryPattern`。

### 复现/测试
静态复现：

```bash
rg -n "kv_full_scan|FlussKvFullScanExec" crates/integrations/datafusion/src crates/integrations/datafusion/docs
rg --files crates/integrations/datafusion/src/execution | rg kv_full_scan
```

行为测试（需容器）：

```rust
let err = ctx
    .sql("SELECT * FROM fluss.<db>.<non_lake_kv_table>")
    .await
    .unwrap_err();
assert!(err.to_string().contains("requires either a complete primary-key equality"));
```

### 建议处理
先确认真实产品契约：

- 若非 lake KV 全表扫描不在当前范围内，更新 `docs/design.md`，移除 `kv_full_scan` / `FlussKvFullScanExec` 的 currently-supported 描述。
- 若该功能应支持，补 `FlussSource::kv_full_scan`、执行计划和集成测试。

---

## 问题 3：README 缺少 prefix lookup 与 `$options` / `$lake` 系统表说明

### 现状
README 的 "Capabilities" 和 "Supported query forms" 只列出 KV point lookup 与 Log scan。实现已支持：

- KV bucket-key prefix lookup（`FlussKvPrefixLookupExec`）。
- `<table>$options` 系统表，展示表 metadata/properties。
- `<table>$lake` lake-only 读取路径（`lake` feature 下）。
- `<table>$lake$<system>` 显式报不支持。

### 影响
用户无法从 README 发现这些 SQL 可见接口，gateway 集成也可能不一致地暴露或屏蔽它们。`$options` 还有 secret-like value redaction 行为，属于安全相关接口契约。

### 证据
- `src/table/kv.rs:230-248` prefix lookup 分支。
- `src/execution/prefix_lookup.rs` 定义 `FlussKvPrefixLookupExec`。
- `src/catalog/schema.rs:197-219` 处理 `$options`、`$lake`、`$lake$<system>`。
- `docs/design.md:363-369` 已描述 prefix lookup。

### 复现/测试
静态复现：

```bash
rg -n "prefix|\\$options|\\$lake" crates/integrations/datafusion/docs/README.md
rg -n "FlussKvPrefixLookupExec|options_table|lake_only_table" crates/integrations/datafusion/src
```

建议新增集成测试：

```sql
SELECT key, value FROM fluss.<db>.<table>$options ORDER BY key;
SELECT * FROM fluss.<db>.<lake_table>$lake;
SELECT * FROM fluss.<db>.<lake_table>$lake$snapshots; -- should error clearly
```

### 建议处理
在 `docs/README.md` 和 `docs/README.zh.md` 中补充 prefix lookup、`$options`、`$lake` 与 `$lake$<system>` 的行为、feature gating 和 redaction 说明。

---

## 问题 4：`lake` feature 与默认构建行为文档不完整

### 现状
`Cargo.toml` 定义了 `lake` feature，控制是否启用 `fluss-lake` 依赖和 union read 路径。README 的 feature 表只列 `(default)` 和 `integration_tests`，没有说明 `lake` feature。更重要的是，`src/catalog/schema.rs` 注释说明不开 `lake` feature 时 lake-enabled 表会降级为 Fluss-only 读，README 未警示这个行为。

### 影响
默认构建下查询 lake-enabled 表可能只读 Fluss 侧数据，无法 union tiered lake 数据。若数据已被 tiering/compaction 移入 lake，结果可能不完整。这个行为需要被明确记录，最好改成显式报错。

### 证据
- `Cargo.toml` 中 `fluss-lake` 是 optional dependency，`lake = ["dep:fluss-lake"]`。
- `src/catalog/schema.rs:228-256` 只有 `#[cfg(feature = "lake")]` 才路由到 `FlussUnionTableProvider`。
- `src/catalog/schema.rs:235-237` 注释写明 without lake feature 会降级为 Fluss-only read。
- `docs/README.md:180-195` feature 表未列 `lake`。

### 复现/测试
静态复现：

```bash
rg -n 'feature = "lake"|lake = \\[' crates/integrations/datafusion/Cargo.toml crates/integrations/datafusion/src
rg -n "lake" crates/integrations/datafusion/docs/README.md
```

建议新增 default-feature 单测：构造 `FlussTableMeta { datalake_format: Some(Paimon), ... }`，在不启用 `lake` feature 时调用 `FlussSchemaProvider::table("t")`。当前行为会返回 KV/log provider；期望行为应改为显式错误。

### 建议处理
README 增加 `lake` feature 行，并说明默认关闭的影响。更稳妥的实现改动是在 `#[cfg(not(feature = "lake"))]` 下对 `entry.meta.is_paimon_lake()` 返回 `UnsupportedQueryPattern`，提示启用 `--features lake`。

---

## 问题 5：设计文档仍说 options 是空占位，但实现已有 `lake_storage_options`

### 现状
`docs/design.md` 的 Public API 章节仍展示 `FlussDatafusionOptions {}` 和 `RegisterCatalogOptions {}` 都是 placeholder。实际 `FlussDatafusionOptions` 已包含 `lake_storage_options: HashMap<String, String>`，用于向 lake catalog 注入 S3 凭证、endpoint、region 等 caller-side storage options。

### 影响
公共 API 文档落后于实现。使用者不知道可以通过 `FlussDatafusionOptions` 注入 lake storage options，也不知道 caller-wins 的合并语义。

### 证据
- `docs/design.md:154-171` 仍展示空 options。
- `src/config.rs:18-32` `FlussDatafusionOptions` 已有 `lake_storage_options` 字段和说明。
- `src/install.rs:53-59` `FlussDatafusion::new` 将 `options.lake_storage_options` 传给 `RealFlussSource::new`。
- `src/table/union.rs:272-277` lake read 前合并 table properties 与 caller options。

### 复现/测试
```bash
sed -n '1,80p' crates/integrations/datafusion/src/config.rs
rg -n "FlussDatafusionOptions|lake_storage_options" crates/integrations/datafusion/docs crates/integrations/datafusion/src
```

### 建议处理
更新 `docs/design.md`、`docs/README.md`、`docs/README.zh.md` 的 configuration/API 部分，记录 `lake_storage_options`、caller-wins 规则、redaction 关系以及适用 feature。

---

## 问题 6：crate layout 文档缺少实际模块，且包含不存在或过期模块

### 现状
`docs/design.md` 的 crate layout 没有列出以下实际模块：

- `table/union.rs`
- `execution/prefix_lookup.rs`
- `execution/union_scan.rs`

同时它列出了 `execution/kv_full_scan.rs`，但当前 `src/execution/` 下没有该文件。

### 影响
维护者按 layout 文档阅读代码时会遗漏 lake union provider、prefix lookup plan、union scan plan，并被 `kv_full_scan.rs` 引向不存在的实现。

### 证据
- `docs/design.md:115-138` crate layout。
- `src/table/union.rs` 存在并由 `catalog/schema.rs` 使用。
- `src/execution/prefix_lookup.rs`、`src/execution/union_scan.rs` 存在。
- `src/execution/kv_full_scan.rs` 不存在。

### 复现/测试
```bash
rg --files crates/integrations/datafusion/src/table
rg --files crates/integrations/datafusion/src/execution
rg -n "union.rs|prefix_lookup.rs|union_scan.rs|kv_full_scan.rs" crates/integrations/datafusion/docs/design.md
```

### 建议处理
更新 `docs/design.md` 的 crate layout 与 module responsibilities，和当前源码树保持一致。`kv_full_scan.rs` 的处理跟随问题 2 的产品决策。

---

## 问题 7：lake union provider 的 `scan()` 忽略 `limit` 参数，无法下推 LIMIT

### 现状
`FlussUnionTableProvider::scan` 接收 `_limit: Option<usize>`，但实现中完全不使用 `limit`。对 lake-enabled 表执行 `SELECT ... LIMIT n` 时，LIMIT 只由 DataFusion 上层截断，lake kernel 会按完整 split 读取。

### 影响
语义仍正确，但性能可能很差：大表 LIMIT 查询会读取所有 lake/log split 后再由 DataFusion 截断。`FlussLakeScan` 目前也没有 `with_limit`，所以 adapter 无法传递 SQL limit。

### 证据
- `src/table/union.rs:216-222` 参数名为 `_limit`，未使用。
- `src/table/union.rs:289-300` 构造 `FlussLakeScan` 时只传 `lake_only` 和 projection。
- `crates/lake/src/api.rs:199-220` builder 方法没有 `with_limit`。

### 复现/测试
静态复现：

```bash
rg -n "_limit|with_limit|row_limit" crates/integrations/datafusion/src/table/union.rs crates/lake/src/api.rs
```

行为测试（需容器和 lake 集成环境）：对大 lake-enabled 表执行 `EXPLAIN SELECT * ... LIMIT 1`，确认 `FlussUnionScanExec` split 数未减少，且读取仍覆盖所有 split。

### 建议处理
在 `fluss-lake` API 中增加 limit/row_limit 配置，或在 `FlussLakeReadOptions` 中增加字段；`FlussUnionTableProvider::scan` 将 SQL `limit` 传入。append/log 与 PK merge 两条路径需分别实现提前截断。

---

## 问题 8：lake-enabled PK 表的路由优先级只在代码注释里说明，设计文档未更新

### 现状
lake feature 下，`FlussSchemaProvider::table` 对 `entry.meta.is_paimon_lake()` 优先路由到 `FlussUnionTableProvider`，再由 union provider 内部决定是否走 PK point/prefix lookup fast path。这样可避免 lake-enabled PK 表直接走 `FlussKvTableProvider` 而遗漏已 tiered 的 lake 数据。当前设计文档 `catalog/` 章节仍描述按 `meta.has_primary_key()` 分 KV/log，没有说明 lake 优先级。

### 影响
维护者可能误以为 lake-enabled PK 表应直接走 KV provider，从而引入结果不完整的回归。

### 证据
- `src/catalog/schema.rs:228-256` lake-enabled 表优先 `FlussUnionTableProvider`。
- `src/table/union.rs:148-254` union provider 内部的 PK fast path。
- `docs/design.md:255-259` 仍写 `table()` branches on `meta.has_primary_key()`。
- 单测 `lake_enabled_pk_table_uses_union_provider_not_kv` 覆盖当前实现意图。

### 复现/测试
```bash
cargo test -p fluss-datafusion --features lake -- lake_enabled_pk_table_uses_union_provider_not_kv
rg -n "is_paimon_lake|has_primary_key" crates/integrations/datafusion/src/catalog/schema.rs crates/integrations/datafusion/docs/design.md
```

### 建议处理
更新 `docs/design.md` `catalog/` 章节：table routing 应先判定 lake-enabled Paimon 表，再判定非 lake KV/log。补充 lake PK 表内部仍可走 point/prefix lookup fast path 的说明。

---

## 问题 9：过渡命名 `FlussUnion*` 未按 lake 设计收敛

### 现状
`crates/lake/DESIGN.md` 建议 connector 命名收敛为 `FlussLakeTableProvider` / `FlussLakeScanExec`。当前实现仍使用 `FlussUnionTableProvider` / `FlussUnionScanExec`，`EXPLAIN` 输出和部分测试也依赖该名字。

### 影响
命名与 `fluss-lake` façade 不一致，增加维护成本。后续如果对外展示 `EXPLAIN`，`FlussUnionScanExec` 也会成为用户可见术语。

### 证据
- `src/table/union.rs:66` `FlussUnionTableProvider`。
- `src/execution/union_scan.rs:25` `FlussUnionScanExec`。
- `crates/lake/DESIGN.md:673-675` connector 命名建议。
- `tests/integration/lake_tiering_sql.rs` 中已有 `FlussUnionScanExec` 相关断言。

### 复现/测试
```bash
rg -n "FlussUnion" crates/integrations/datafusion/src crates/integrations/datafusion/tests
rg -n "FlussLakeTableProvider|FlussLakeScanExec" crates/integrations/datafusion
```

### 建议处理
低优先级重命名：`FlussUnionTableProvider` -> `FlussLakeTableProvider`，`FlussUnionScanExec` -> `FlussLakeScanExec`，同步更新 `EXPLAIN` 断言。若需要保留旧 EXPLAIN 名称，需在文档中明确它是兼容名。


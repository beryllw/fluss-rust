# fluss-datafusion Phase 1 任务状态(临时)

> 临时跟踪文档。Phase 1 全部任务完成后删除本文件。
> 任务定义的 source of truth:`docs/fluss-datafusion-phase1-design.md`。

| 任务 | 状态 |
|---|---|
| Task 1: workspace 与 crate 骨架 | 已完成 |
| Task 2: metadata cache 与注册路径 | 未开始 |
| Task 3: KV 谓词分析与 lookup 执行 | 未开始 |
| Task 4: log 有界扫描执行 | 未开始 |
| Task 5: crate 本地集成测试 harness | 未开始 |

## Task 1: workspace 与 crate 骨架

- 状态:已完成
- 新增 `crates/fluss-datafusion` 作为 workspace member,并提供可编译的公共 API 骨架(`FlussDatafusion`、`FlussDatafusionOptions`、`RegisterCatalogOptions`、`FlussDatafusionError`)。
- 固定 `datafusion = 51.0.0`(与 workspace 的 `arrow 57` 对齐),并将 workspace 的 `rust-version` 提升到 `1.88` 以满足 DataFusion 的 MSRV。
- `register_catalog()` 刻意为失败桩(failing stub),直到 Task 2 接入真实的 catalog 注册。
- 已通过 `cargo check -p fluss-datafusion` 与 `cargo check --workspace` 验证。Task 1 未新增测试;实质行为在 Task 2/5 引入。

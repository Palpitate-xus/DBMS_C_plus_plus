# Executor（执行器）

## 当前状态

执行算子（`TableScanOp`、`IndexScanOp`、`HashJoinOp` 等）统一定义于
`src/executor/ExecutionPlan.h/.cpp`，并通过 `src/interfaces/executor.h` 的
`IOperator` 生命周期接口接入 Volcano 执行模型。

## 未来迁移计划

`QueryPlanner::buildSelectPlan()` 负责生成当前结构化计划；复杂或尚未迁移
的 SQL 会经明确的 legacy/materialized-row 边界进入同一算子树。后续重点是
继续迁移复杂 DQL/DML producer、补齐并行 join/aggregate 和完整 PostgreSQL
计划语义，而不是再移动算子目录。

# Executor（执行引擎）

## 当前状态

执行引擎当前实现在 `src/optimizer/ExecutionPlan.cpp` 中，采用 Volcano Iterator Model：

- `TableScanOp` / `IndexScanOp` / `IndexOnlyScanOp`
- `FilterOp` / `ProjectOp` / `SortOp` / `LimitOp` / `DistinctOp`
- `NestedLoopJoinOp` / `HashJoinOp` / `MergeJoinOp`
- `AggregateOp`

## 未来迁移计划

Phase 5 将把这些算子迁移至此目录，统一实现 `IExecutionPlanner` / `IOperator` 接口（定义见 `src/interfaces/executor.h`）。

# Executor（执行器）

## 当前状态

执行算子（TableScanOp, IndexScanOp, HashJoinOp 等）目前仍定义于 `src/optimizer/ExecutionPlan.h`。
Phase 0 已让 `Operator` 基类继承 `IOperator` 接口（`src/interfaces/executor.h`）。

## 未来迁移计划

Phase 5 将把所有算子迁移至此目录，并统一实现完整的火山模型执行框架。

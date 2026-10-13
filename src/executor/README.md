# Executor（执行器）

Volcano 算子定义于 `ExecutionPlan.h/.cpp`，生命周期接口位于 `src/interfaces/executor.h`。查询计划和 prepared cursor 等消费者必须区分正常 EOF 与执行错误：`next()` 返回 false 后仍需检查错误状态。

通过 `QueryPlanner` 执行的路径使用 `executePlanChecked()` 并检查 `PlanExecutionResult::ok/error`。该结果保留 SQLSTATE 和原始错误消息；SQL dispatcher 使用 `throwIfFailed()` 传播错误，不应从显示文本猜错误码或把失败转换成空子查询。其他 prepared 执行入口也须遵守等价的错误、部分结果和清理契约。

清理不能覆盖首个执行错误。EXPLAIN ANALYZE 应观察实际执行图及其累计计数，PLAIN EXPLAIN 不得执行带副作用的表达式。

## 已知边界

[代码审查报告](../../docs/code-review-2026-10-13.md)包含窗口排序/peer 的类型丢失、NUMERIC 分区等价、frame 方向、NTILE、LAG/LEAD 和 SUM 中间溢出问题，以及 EXPLAIN 子图的残留缺口。
部分复杂 DQL/DML 仍经 legacy 或物化适配路径执行；现有窄范围测试通过不证明完整 PostgreSQL 执行语义。

后续应统一 typed 比较、分区/peer 等价、窗口边界和已绑定表达式消费，并分别覆盖结果值、NULL、OID、错误阶段、副作用次数和清理。完整功能状态见[总清单](../../docs/postgresql-18-gap-audit.md)。

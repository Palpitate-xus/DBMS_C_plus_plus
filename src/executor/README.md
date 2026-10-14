# Executor（执行器）

Volcano 算子定义于 `ExecutionPlan.h/.cpp`，生命周期接口位于 `src/interfaces/executor.h`。查询计划和 prepared cursor 等消费者必须区分正常 EOF 与执行错误：`next()` 返回 false 后仍需检查错误状态。

通过 `QueryPlanner` 执行的路径使用 `executePlanChecked()` 并检查 `PlanExecutionResult::ok/error`。该结果保留 SQLSTATE 和原始错误消息；SQL dispatcher 使用 `throwIfFailed()` 传播错误，不应从显示文本猜错误码或把失败转换成空子查询。其他 prepared 执行入口也须遵守等价的错误、部分结果和清理契约。

清理不能覆盖首个执行错误。EXPLAIN ANALYZE 应观察实际执行图及其累计计数，PLAIN EXPLAIN 不得执行带副作用的表达式。

## 已知边界

[代码审查报告](../../docs/code-review-2026-10-13.md)保存窗口及 EXPLAIN 的原始
问题；当前逐项修复与验收见[修复进度](../../docs/review-fixes-2026-10-13.md)。
原生与 legacy 窗口共享有方向的半开 frame 边界；EXPLAIN 的物化 JOIN/source
节点和计数来自实际执行图，不另外构造展示用计划。
窗口聚合的表达式参数通过同一 prepared value receiver 按 frame 需求求值，
保留类型/NULL、实际输出 owner 和 VOLATILE 次数；细节与尚未验收边界见
[R45](../../docs/issue-review-r45-window-aggregate-arguments.md)。
部分复杂 DQL/DML 仍经 legacy 或物化适配路径执行；现有窄范围测试通过不证明完整 PostgreSQL 执行语义。

后续应统一 typed 比较、分区/peer 等价、窗口边界和已绑定表达式消费，并分别覆盖结果值、NULL、OID、错误阶段、副作用次数和清理。完整功能状态见[总清单](../../docs/postgresql-18-gap-audit.md)。

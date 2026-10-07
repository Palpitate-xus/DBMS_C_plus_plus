2026-10-06 PL scalar/native follow-ups 两项独立source/test commits：`b6683521` 修复真实native SELECT/WHERE/ORDER/INTO表达式错误从22012/22P02/22003被吞为XX000；`8cd860e7` 用canonical AST位置绑定修复quoted "X"/x变量值/NULL/type混淆，保留BIGINT宽度、session特殊值、EXTRACT语法和显式trigger binding，并拒绝不存在的变量/qualifier/function。旧native/wire实际红、独立development新storage/stubs的3native及2protocol绿均有记录。ROOT正式O2只重编变更storage对象，其他54对象严格复核匹配，normal/repeat build、55/55签名及binary stamp通过；最终matching O2的6native及7项专项/相邻protocol通过。完整默认协议本follow-up未重跑；前一615/7bc组合在main3170 INSERT kw_joined timeout exit1，之前三次完整超时也保留，不能宣称完整protocol/suite通过。6项ledger unit/3项文档版本兼容检查通过，require-complete仍exit1。见 `docs/issue-plpgsql-scalar-binding-and-native-errors.md`。query内variable/source-column歧义、函数错误后写入不原子及statement-image写放大仍独立未完成；SQL-04/FUNC-05/06仍partial。总账273保持22 complete、166 partial、70 unverified、15 deferred_by_user；未push、Actions禁用、用户安全/TDE跳过项保持deferred。

2026-10-06 cold recovery / PL/pgSQL 两项独立修复已提交：`615f2c54` 修复真实 SIGKILL 后全新 exec 访问未初始化 database-lock map 的冷启动崩溃；`7bc76204` 保留完整 SELECT INTO 查询、structured typed first-row/NULL、STRICT/FOUND、声明/赋值/返回类型与错误码、stored CTE scope 及 native checked fallback。独立 fixture commit `acaa0bea` 用真实表达式 evaluator 替换旧 native echo host，声明 runaway counter 并精确断言未降低的步数上限；旧 O0 exit137 与 O2 RSS约53.5GiB后主动TERM的失败均保留，纠正fixture后O0 1.39秒/15420KiB通过。最终 development 7项专项/相邻协议及3项native通过；合并正式优化版55对象重编、normal/repeat build、55/55签名及binary stamp通过，10项专项/相邻协议及6项matching native通过。完整默认协议本轮在main3170 INSERT kw_joined处socket timeout exit1；此前两次JOIN完整超时与第三次instrumented CREATE TABLE超时也保留，snapshot写放大未修，不宣称全协议通过。6项ledger unit及3项文档/版本/兼容检查通过；全注册suite/TLS runtime/PG18.6 differential未跑。见 `docs/issue-cold-start-transaction-lock-registry.md` 和 `docs/issue-plpgsql-select-into-typed-query.md`。quoted scalar X/x复合绑定错值与函数P0002/22P02后写CTE仍落盘已独立复现，正在分别修复；FUNC-05/06、SQL-04、WAL-05/08、TXN-05/07仍partial。总账273保持22 complete、166 partial、70 unverified、15 deferred_by_user；未push、Actions禁用、用户跳过安全/TDE项保持deferred。

# 工作区与复查清单收尾计划

范围：开始时未提交的 `ExecutionPlan.cpp`、`main.cpp`、`grouping_sets_expr.sql`，以及 `code-review-progress.md` 中尚未验证的投影子查询解析。历史 PostgreSQL 全功能路线图不属于本次收尾；此前用户要求跳过的安全专项仍排除。不执行 git push。

| 阶段 | 工作 | 验收标准 | 状态 |
| --- | --- | --- | --- |
| 1 | 完善串行 / 并行 string_agg、array_agg | NULL、空字符串、参数表达式、FILTER、输入排序及串并行一致性有回归；单独提交 | 已完成；第 191、196 项；执行器专项及完整 SQL 入口通过 |
| 2 | 收尾分组表达式与临时调试代码 | GROUPING SETS / ROLLUP / CUBE SQL 有明确结果断言；移除临时调试输出；按问题提交 | 已完成；复查第 192 项；SQL E2E 通过 |
| 3 | 完成投影子查询解析检查 | 引号关键字、空白符、ORDER BY / LIMIT 等组合有回归；修复逐项提交 | 已完成；复查第 193–195 项 |
| 4 | 集成验证、更新清单 | 主程序构建和相关 C++ / SQL 回归通过；工作区干净；未 push | 已完成；31 个 C++ 测试和 3 套 SQL E2E 全部通过；改动全部本地提交 |

执行方式：先复现，再修复和回归，按问题单独提交。本文件记录本次明确范围的完成情况，不宣称整个数据库不存在其他缺陷。

## 完成记录（2026-09-08）

本次新增修复为第 191–196 项，分别提交为 `9d41c9b`、`2c3042d`、`06a6338`、`103c8a7`、`11c76de`、`fc6c1e0`；具体问题、影响和验证见 [复查清单](code-review-progress.md)。

收尾验证覆盖清单对应的 20 个 C++ 测试，以及以下 11 个相邻回归：`text_comparison_type_test`、`expression_evaluator_test`、`boolean_expression_test`、`greatest_least_projection_test`、`group_collection_aggregate_test`、`parser_phase1_test`、`parallel_exec_test`、`volcano_select_phase51_test`、`window_functions_test`、`aggregate_bool_test`、`aggregate_percentile_test`。

SQL 验证使用本次构建的 `build/dbms_review_main`，运行 `review_sql_e2e_test.py`（17 个查询）、`window_e2e_test.py`（13 项）和 `explain_analyze_e2e_test.py`（6 项），均在隔离目录中通过。当前环境使用 zlib 和 TLS stub；未验证 OpenSSL 分支，也未重开已排除的安全专项。Shell / Python 语法检查、`git diff --check` 和调试输出清理检查通过。

全部阶段关闭，未遗留本次范围内待办。仓库 GitHub Actions 工作流保持禁用，不执行 push；历史全功能路线图和这里明确排除的功能不计入完成范围。
## 2026-10-07 当前总清单执行计划（以上记录为历史）

Source `5ca4278e` 已逐项本地提交，618 native / 319 registered / 58 TU；完整总账仍未完成，不沿用以上旧“本次范围关闭”作为273项完成证明。提交映射/实际失败/证据见 `docs/issue-ready-source-integration.md`。

| 下一阶段 | 验收要求 | 当前证据与状态 |
| --- | --- | --- |
| 最新组合正式构建 | 全58 fresh 正常O2、repeat、全部 source/header/flags/stamp、冻结同一binary | 精确5ca快照36316已运行；未宣称通过 |
| 原完整回归 | 不改原SQL/assertions/deadlines，执行618native/319registered | 5ca runner已准备未启动；旧d2 full590/312 96468仍live |
| 存储剩余问题 | 真实rename/drop/recreate代际恢复、原vacuum_toast可见性/值断言通过 | e6 137native135pass/2fail，新BTree候选仍不能关闭这两项；分别修复 |
| 查询/类型剩余问题 | 保留whole37 DML/UNION ALL、whole domain/pattern priority、SRF完整known-gap控制 | 新ANY whole19和数组whole12专项绿；typed append、TYPE-19域链、模式、SRF两项仍进行 |
| 总账验收 | 全部原273逐项满足完整证据；用户跳过15保持deferred | 22complete/166partial/70unverified/15deferred；完成gate仍拒绝 |

不push，不启用Actions，不恢复用户跳过安全/TDE。旧e6全58正常构建实际0，但native137=135pass/2fail、wire65=64pass/1fail；旧d2 native120实际0、wire64=60pass/4fail。专项结果不替代新ROOT完整验收。

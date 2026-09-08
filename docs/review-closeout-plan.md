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

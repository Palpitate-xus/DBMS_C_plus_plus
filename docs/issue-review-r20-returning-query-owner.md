# R20：INSERT/UPDATE RETURNING 必须执行绑定后的子查询

## 原问题

旧 INSERT 和目标表 UPDATE 对 RETURNING 使用 raw AST 的另一个 `ExprEvaluator`。其中 scalar-query 的 literal wrapper 被当成普通字符串，没有调用已准备的查询、使用当前 OLD/NEW 行或检查子查询行数。例如 `RETURNING (SELECT 1/0)` 返回 SQL 文本并提交写入，`RETURNING (SELECT v FROM target)` 没有报告 `21000`。

冻结原 Main 的新协议驱动实际 exit 1，日志保留错误返回的 SQL 字符串和后续已经改变的表内容。Root R26 合入后、R20 修改前，新 native 驱动实际 abort/exit 134，记录 29 个失败检查；这证明 R26 的共同类型修复本身没有修好 RETURNING。

## 修复方法

- INSERT/普通 UPDATE 使用原完整 `PreparedQuery` 的 RETURNING AST、输出 descriptor 和同一 execution carrier；INSERT DEFAULT VALUES 的 RETURNING 也进行完整准备。
- 用原 source ordinal 设置目标表及 OLD/NEW 行像。缺失的行版本是整行 typed NULL，不是文本哨兵；保留 qualified/unqualified star、transition alias 和真实 NULL bitmap。
- scalar child 经过现有 typed 查询 host，传播除零/多行错误并读取真实相关列；不按 SQL 文本重新猜测类型。
- structural constant planning 在写入前进行，包含 scalar child 的常量；不调用 volatile 函数。这个阶段保证 DEFAULT 约束或空输入不会隐藏确定的除零错误。
- RETURNING 所有行成功后才发布完整结果。求值失败留在原 statement boundary 内，回滚本次写入并清除部分结果；显式事务早先的写入仍保留。

## 实际验证

新增 `insert_update_returning_subquery_protocol_e2e_test.py`，35 个协议控制在 PostgreSQL 18.6、最终 private 候选及最终 Root 中均实际 exit 0，核对行值、NULL、OID、列名、command tag 与 ReadyForQuery：

| 内容 | 验证 |
| --- | --- |
| INSERT/UPDATE 除零与多行 child | `22012`/`21000`；随后读取原表无变化 |
| 多行 INSERT/UPDATE 后段求值失败 | 前段变更也回滚，没有提前输出 RETURNING 行 |
| 相关新行与 OLD/NEW | 正确读取当前行和旧行，而不是 wrapper 文本 |
| NULL/零行 child/OLD star | 正确的 typed NULL 与输出 descriptor |
| 不相关 volatile child | 同一次执行只求值一次；后续 sequence 状态正确 |
| SQL PREPARE 两次 EXECUTE | 每次独立 carrier，子查询实际执行且 metadata 不退化 |
| 显式事务及 savepoint | 错误状态 E，ROLLBACK TO 后保留早先写入；正常 DELETE 邻居 |

Root 真正新编译 `DmlExecutor.cpp`；其余 57 个对象来自已完成的同 header epoch 标准构建，并在 native gate 前核对全部 58 个当前 source/header/compiler/flags receipts。最终 Main SHA256：`39388607893e6d1f80fff5f2ceb4e7021f4844e77b6f6e996b94c76a737c4ba8`。构建及测试终态均 0，最终输入签名未改变。

七个完整 native 驱动通过：新 `insert_update_returning_subquery_test`、原 `insert_statement_atomicity_test`、`prepared_query_execution_test`、`prepared_dml_plan_test`、`dml_returning_test`、`returning_transition_binding_test`、`update_returning_preparation_test`。

14 个完整协议驱动通过：新驱动、原 RETURNING array/OLD-NEW/transition/WITH/deferred、DELETE bound RETURNING、INSERT DEFAULT preparation、原五个 metadata/TEMP 失败驱动及完整 `postgres_protocol_test.py`。未删减这些原驱动的断言。

证据目录：`/tmp/dbms-review-root-r20.QCHy8AoG/`。最终日志为 `root-integrated-candidate-build.log`、`root-integrated-final-native.log`、`root-protocol-summary.log` 和各 `root-*.log`。

## 中间失败与边界

第一 private 协议候选只有 DEFAULT VALUES 错误优先级失败：返回 `23502` 而 PG 为 `22012`；补 structural planning 后同 35 项全部通过。private native 曾报告两个 `XX000` 算术 descriptor 差异，属于已由独立 R26 修好的共用 binder 类型选择，在 Root R26+R20 上保留原控制并通过。

native 首版误把 `makeIntColumn(scale=4)` 当成 INT；实际该 API 是 BIGINT，scale=2 才是 INT。类型断言纠正为真实 schema 的 BIGINT，其他原 cell/NULL/回滚检查保留。一个 Root gate 首轮最后误用了不存在的 `returning_old_new_test.cpp` 文件名，实际前四项通过但该轮退出非零；随后用真实原驱动完整重新运行七项，终态 0。所有中间日志保留。

本项不改 MERGE 的独立执行器，也不自动宣称全部未实现的 DML 形状兼容。合入后全量注册回归另记[修复总账](review-fixes-2026-10-13.md)。

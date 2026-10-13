# R31：DELETE ONLY 保留完整的绑定执行路径

## 问题和修复

R22 的合法语法执行控制发现：`DELETE FROM ONLY t AS d WHERE d.id=3 RETURNING d.id`
会删除多行并丢失 RETURNING。Main 和 native DML bridge 都曾因 AST 的 `only`
标记拒绝绑定执行器，退回旧的文本拆解路径；该路径没有保留完整别名、WHERE 和
RETURNING 语义。这是会误删数据的执行正确性问题，不是缺少 FROM 的语法问题。

现在 ONLY 使用已有的 `BoundDmlExecution`：该执行器的修改目标本来就是命名的
物理表，因此无须去掉 ONLY 后重新解析 SQL。谓词、USING、返回投影、NULL 标记和
错误回滚继续由同一个绑定执行器负责。原有游标 WHERE CURRENT OF 和 view 分支
未扩展。本项不宣称补齐不带 ONLY 的所有继承 DML 行为。

parser 同时接受 PostgreSQL 的 `ONLY (schema.table)` 形式，并要求括号内只有一个
合法表名和匹配的右括号。

## 保留的复现与验证

注册 `delete_only_qualified_returning_protocol_e2e_test.py`，使用连接内 TEMP 父表、
继承子表和 USING 来源。父子表故意包含相同 id，以验证 ONLY 不修改子表。

- 8 个成功控制：别名、NULL 谓词、NULL 返回值、括号/引用标识符、schema 限定名、
  USING、空结果的类型描述、无 RETURNING 的行数标签。
- 4 个错误控制：别名隐藏原表名（42P01）、未知列（42703）、非布尔谓词（42804）、
  后续行的 RETURNING 除零（22012）。错误控制采用 autocommit，不能用测试自己的
  ROLLBACK 掩盖产品的部分删除；每项随后核对父表和子表完整快照。
- 成功控制则显式事务回滚并再核对快照；同时核对返回 OID、NULL 和命令标签。

R29 后冻结基准 SHA-256 为
`bca85d326d1af70441ee069dc117dfedb660b0e9f565382b44b91b48b3735b5d`。
第一项原样返回 `DELETE 4`、没有行或类型描述，预期仅删除 id=2 并返回 `(2,two)`。
基准完整驱动 exit 1；12 项均不符合预期，但部分后续错误受前面错误删除的影响，
不能解释为 12 个独立根因。原输出保留在 `protocol-baseline.log`。

PostgreSQL 18.6 的相同完整驱动 12 项及快照全部通过，exit 0。
候选标准构建真实重编 Main、DmlExecutor 和 parser，冻结候选 SHA-256 为
`c86a681d65fdb01f3ccdb01a297eb6b91f8321f3f4fbff8efa9b0ac3ef267b28`。
候选同一驱动全部通过；R22 原完整驱动的 ONLY 正向断言未修改，现也 exit 0。

新增 native `delete_only_typed_owner_test.cpp` 验证直接 bridge、空描述、NULL bitmap、
限定名错误、RETURNING 错误原子性及非法括号；与原 `delete_bound_returning_test`、
`delete_exception_owner_test` 三项全部通过。原协议驱动
`delete_bound_returning_protocol_e2e_test.py`、`delete_exception_owner_protocol_e2e_test.py`
以及原 CLI `inherit_only_e2e_test.py` 均 exit 0。

原始日志和冻结候选位于 `/tmp/dbms-review-r31-evidence.bWm3P0mR/`。
未缩减的完整 `postgres_protocol_test.py` 也 exit 0，原输出保留为
`postgres_protocol_test.log`。本项没有执行整个项目的全量回归。

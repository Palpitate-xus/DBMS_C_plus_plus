# R22：INSERT INTO / DELETE FROM 的必需关键字

## 问题与修改

通用 parser 原来把 INSERT 后的 INTO、DELETE 后的 FROM 当作可选 token。
因此严格模式仍接受 `INSERT t VALUES(...)` 和 `DELETE t WHERE ...` 并写入/删行。
SQL PREPARE 又直接保存模板文本，不检查模板的 INSERT/DELETE 语法。

现在 parser 在读取表名之前要求相应关键字，缺失返回 42601；主执行器的原 DML
错误边界及扩展协议 Parse 接收该诊断，不进入名称解析或表达式执行。SQL PREPARE
在发布 statement 名称、查询参数类型或记录目标访问之前，对 INSERT/DELETE 模板
调用相同的纯 `parseForBinding` 检查，不执行模板。这不是完整 PREPARE 绑定修复；
其它模板类型和 EXECUTE 实参求值由各自审查项负责。

兼容性核对：README 和 `docs/commandsList.md` 将这两条 DML 语法明确写为
INSERT INTO / DELETE FROM；没有查到省略该关键字的显式 extended 扩展契约。
因此通用 parser 统一要求关键字，不增加新的隐式 legacy 语法。已有命名项目扩展
的路由和开关不变。

## 证据与测试边界

新增 `dml_required_keywords_test.cpp` 同时检查 parse/parseForBinding 的 36 个
正确/错误控制：大小写、注释、schema/引用标识符、DEFAULT/SELECT 来源、ONLY、
别名、USING 和 RETURNING。

新增注册的 `dml_required_keywords_protocol_e2e_test.py` 使用严格模式和隔离 TEMP
表/sequence，对 8 类缺词输入分别进行 Simple Query、wire Parse 和 SQL PREPARE，
共 24 项拒绝控制；要求唯一错误状态为 42601，无 DataRow、完成标签或 ParseComplete。
另外检查数据不变、sequence 没有求值及合法语法执行。每例通过真实 savepoint 恢复，
不是跳过后续错误；PostgreSQL 参考使用独立连接内 TEMP 对象和最终回滚。

冻结基准为 R29 后的 Main，SHA-256：
`bca85d326d1af70441ee069dc117dfedb660b0e9f565382b44b91b48b3735b5d`。
新增 native 在第一条缺词 INSERT 断言失败，exit 134；原解析错误地返回 success。
协议基准的 24 个拒绝控制中 23 个不符合预期（Simple Query 的缺 FROM + ONLY
组合原已报 42601），且 currval 已变为 1，说明本应语法拒绝的
INSERT 实际求值了 nextval。相同完整驱动在 PostgreSQL 18.6 上全部通过，exit 0。

完整协议驱动还发现独立的 **R31**：合法
`DeLeTe /* note */ FrOm ONLY keyword_rows AS d WHERE d.id=3 RETURNING d.id`
在基准返回 `DELETE 3`、没有 RETURNING，预期只删除 id=3 并返回该行。
原因是 ONLY 强制退回旧执行分支，和本项缺词语法问题不同。该原始断言保留，
没有改成只 Parse 或删除它来让本项显示全绿；R31 由后继独立提交修复。

候选真实标准构建成功，重编 Main 和 parser 后 Main SHA-256 为
`2aa9ea06985c2a37a866fd103401675d6575940900358f6fc431b56264314924`。
候选的 24 个语法拒绝项全部通过，数据保持原样、currval 正确为未定义（55000），
新增的合法 PREPARE/EXECUTE/DEALLOCATE 边界和混合大小写 INSERT 也通过。
随后原样保留的 ONLY 执行断言仍因 R31 失败，故完整新增协议驱动实际 **exit 1**，
不能声称完整 ONLY 执行已通过；其真实输出是 `protocol-candidate.log`。

三个 native 驱动 `dml_required_keywords_test`、`parser_phase1_test`、`prepare_pg_test`
全部 exit 0。三个未缩减的原协议驱动 `insert_values_syntax_protocol_e2e_test.py`、
`prepared_statement_lifecycle_e2e_test.py`、`temp_prepared_access_protocol_e2e_test.py`
也全部 exit 0。没有执行本项的完整全库回归；R31 后必须重跑原完整新增驱动。

原始日志目录：`/tmp/dbms-review-r22-evidence.2xQ9zE9B/`。首次 PG 启动使用了错误的
密码文件路径，其 fixture 错误单独保留为 `protocol-pg18-fixture-path-error.log`；
纠正路径后的完整 PG 输出为 `protocol-pg18.log`，不把 fixture 错误算产品缺陷。

# 第 958 项：TEMP 访问与 PREPARE TRANSACTION 生命周期

状态：源码 `3cfe28db` 已集成至 D source commit `70c5a116`；总账项仍为 partial。

本项修复了真实 TEMP relation、TEMP sequence 或 TEMP 对象创建访问可以绕过 `PREPARE TRANSACTION` 限制的问题。旧逻辑只检查仍留在 undo log 中的临时写入，因而空表读取、零行修改、序列访问和已 `ROLLBACK TO` 的子事务访问均可能漏检。PG18.6 对此返回 `0A000` 并结束顶层事务。

实现为事务上下文增加顶层生命周期的 sticky TEMP access 标记，不随语句／用户保存点回滚。实际 relation access、TEMP 对象创建和 `nextval`／`currval`／`lastval`／`setval` 访问设置该标记；PREPARE 在持久化 prepared state 之前拒绝并完整 rollback，不保存任何残余事务状态。下一次真实顶层 BEGIN 重置标记。

SQL `PREPARE` 和协议 Parse 使用只读 catalog／AST walker 标记被分析语句中的真实 TEMP relation，不执行表达式、不读取 heap、不消耗 sequence。覆盖 SELECT、scalar／EXISTS／IN、derived table、WITH／CTE 名称遮蔽、UNION、DML target 与 P-only Parse；普通字符串中的 SQL 外观文本不误判。补充的 derived AST／WITH 路由不等同于完整 binder。`LOCK` command tag 同时修正为 PostgreSQL 的 `LOCK TABLE`；没有扩展对 `ACCESS SHARE` 的支持。

最终独立验收均通过：

- PostgreSQL 18.6 强 oracle：`/tmp/dbms-temp-prepared-access-pg18-oracle-v11-958.log`
- 项目 Simple／Extended／P-only 强协议：`/tmp/dbms-temp-prepared-access-lock-tag-final-protocol-v5-958.log`
- 自有统一 headers/source 构建链，API 变更后的全量重编及最终签名增量 Parser／Dml／Network 编译链接
- 18 个不同 native C++：`/tmp/dbms-temp-prepared-access-lock-tag-final-native-18-958.log`
- 19 个 wire（含完整 `postgres_protocol_test.py`）：`/tmp/dbms-temp-prepared-access-lock-tag-final-wire-19-958.log`
- 19 个不同 PostgreSQL actual case：`/tmp/dbms-temp-prepared-access-lock-tag-final-actual-19-958.log`（20 次执行中有一次重复匹配，只计一次）

旧生产错误允许 TEMP PREPARE、P-only 漏检、derived／CTE 漏检和错误 LOCK tag 均保留在原失败日志中；中间失败不以最终成功覆盖。D 集成后的完整 `scripts/build_tests.sh` 另在 `/tmp/dbms-final-post-fix-build-tests.log` 完成，含该项新增协议回归。

本修复只闭合受测 PREPARE 限制，不代表 PostgreSQL 全部 2PC 语义。CAT-13／CAT-15、TXN-05／TXN-06、SQL-13、PROTO-01／PROTO-02 继续为 partial；完整 TEMP namespace／对象族、2PC 全部 resource owner／subtransaction／invalidation／crash recovery、prepared type／plan 生命周期和完整 binder 尚未完成。

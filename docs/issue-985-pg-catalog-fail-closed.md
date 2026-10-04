# 第 985 项：未实现系统 catalog 的 fail-closed 错误

## 发现

沿着 CAT-03 的协议路径继续探测后，除 `pg_index`、`pg_operator` 外，多个尚无 SQL catalog schema/执行路径的关系也会落到物理表/锁处理，产生错误的 `55P03`；缺少当前库同名 relation 时，未限定名称还可能得到 `42P01`。探测覆盖 `pg_constraint`、`pg_am`、`pg_opclass`、`pg_cast`、`pg_collation`、`pg_rewrite`、`pg_trigger`、`pg_policy`、`pg_authid`、`pg_auth_members`、`pg_default_acl`、`pg_tablespace`、`pg_statistic_ext`、`pg_subscription`、`pg_publication` 和 `pg_replication_origin`。

## 修复

上述 18 个已确认缺少 SQL catalog 执行路径的系统 relation（包括第 984 项的两项）对 `pg_catalog.name` 与缺少用户 relation 的未限定 `name` 统一返回 `0A000`。当时已存在的虚拟 `pg_database`/`pg_statistic` 路径保持可用；后续复核发现 `pg_statistic` 与 `pg_stats` 共用错误的文本 schema，现均已按第 989 项改为 fail-closed。如果用户数据库中确有同名普通 relation，未限定查找仍访问它。错误后同一连接可继续查询。

代码/测试提交：`c79c51bb fix(catalog): fail closed for unsupported relations`。

## 验证

- `bash scripts/build.sh`：最终生产源码构建通过。
- `python3 tests/pg_catalog_unavailable_sqlstate_protocol_e2e_test.py`：通过；逐一覆盖 18 个关系的限定/未限定名称、SQLSTATE、idle ReadyForQuery，且验证虚拟 catalog 与同名用户表不被误挡。
- `timeout 300s build/plpgsql_test`：单独重跑并通过（约 94 秒），包括 200,000 步 runaway guard 和 SQL 函数往返用例。
- `DBMS_PROTOCOL_TEST_TIMEOUT=120 DBMS_PROTOCOL_STARTUP_TIMEOUT=120 DBMS_PROTOCOL_SHUTDOWN_TIMEOUT=120 bash scripts/build_tests.sh`：退出码 1。完整脚本中除 `plpgsql_test` 被人工提前终止外，已运行 C++、协议和 E2E 用例均通过；catalog 定向 E2E 在完整脚本中通过。该整套结果不记作通过。
- 未运行 PostgreSQL 18.6 oracle/differential；不声称 catalog 数据或输出等价。

## 未完成部分

这是对已确认缺失 catalog 路径的错误分类修复，不会创建空 catalog 或伪造行。系统 catalog 的实际 schema、列类型、数据、OID/依赖关系和 CAT-03 其余对象仍未完成，因此 CAT-03 继续保持 `partial`。

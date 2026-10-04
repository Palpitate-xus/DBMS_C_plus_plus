# 第 988 项：`pg_database` 的 `COUNT(expr)` 忽略 NULL

## 发现与修复

第987项加入 `pg_database` 聚合投影后，复查发现聚合器把每个 `COUNT(expr)` 都按输入行数返回。因此合法查询 `COUNT(NULL)` 在目录非空时会错误返回正数。现在 `COUNT(*)` 按输入行数计数，`COUNT(expr)` 对每一行求值，只累计非 NULL 值；多个聚合投影分别计算。

代码与协议测试提交：`cbb7bd0c fix(catalog): count nonnull pg_database values`。

## 验证

- `bash scripts/build.sh`：生产构建通过。
- `python3 tests/pg_catalog_unavailable_sqlstate_protocol_e2e_test.py`：通过，断言 `COUNT(*)=1`、`COUNT(NULL)=0`，两列类型均为 PostgreSQL `bigint` OID 20，并继续覆盖目录字段类型、错误状态和连接复用。
- `python3 tests/pg_class_query_protocol_e2e_test.py`：通过。
- 本地 `pgref` 是 PostgreSQL 17.2；没有据此声称 PG18.6 oracle/differential。
- 未运行完整注册 suite。

这是 `pg_database` 受支持投影的局部聚合修正；其他目录 schema/data 和 CAT-03 仍不完整。

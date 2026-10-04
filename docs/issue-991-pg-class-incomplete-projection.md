# 第 991 项：`pg_class SELECT *` 把七列子集冒充完整目录

## 发现

`executePgClassQuery` 已提供七列 typed 查询子集，但旧 `SELECT *` 路径会把这七列展开后当作完整 `pg_class` 返回。PostgreSQL 18 的 [`pg_class` 文档](https://www.postgresql.org/docs/18/catalog-pg-class.html)列出 34 列，包括未提供的 `relallfrozen`、`relhasindex`、`relacl`、`reloptions` 和 `relpartbound` 等。因此星号结果的列数和 schema 不符合 PostgreSQL。对已知但未实现列，旧路径还误报 `42703`（列不存在）。

## 修复

保留已验证的七列 typed projection，但完整 schema 不可用时，`SELECT *`／`relation.*` 返回 `0A000`。已知 PostgreSQL 18 但未实现的列（例如 `relhasindex`）也返回 `0A000`；真正不存在的列继续返回 `42703`。`COUNT(*)` 仍是聚合参数，不会被误判为星号投影。

代码与测试提交：`4e6a78d7 fix(catalog): reject incomplete pg_class projections`。

## 验证

- `bash scripts/build.sh`：生产构建通过。
- `python3 tests/pg_class_query_protocol_e2e_test.py`：通过；Simple/Extended 模式均覆盖 `SELECT *` → `0A000`、`relhasindex` → `0A000`、unknown column → `42703`，并保留已支持列的 OID 和查询/排序/分页回归。
- `python3 tests/pg_catalog_unavailable_sqlstate_protocol_e2e_test.py`：通过。
- PG18 官方文档核对 `pg_class` 列 schema；本机 `pgref` 为 PostgreSQL 17.2，不声称 PG18.6 runtime oracle/differential。
- 未运行完整注册 suite。

这只关闭不完整星号结果和已知列错误分类；完整 `pg_class` 列、数据来源及 catalog 语义仍缺，CAT-03 继续 `partial`。

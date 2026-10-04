# 第 990 项：`pg_namespace` 返回不完整且失真的 schema 数据

## 发现

变更前 wire 检查中，`SELECT * FROM pg_catalog.pg_namespace` 成功返回三列 `oid nspname nspowner`，协议却把它们全部描述为 text/OID 25。实际行还包含固定 bootstrap placeholder `pg_temp_1`，而现有路径没有列出 `information_schema`。Catalog row 本身明确省略 `nspacl`。

PostgreSQL 18 的 [`pg_namespace` 文档](https://www.postgresql.org/docs/18/catalog-pg-namespace.html)定义四列：`oid oid`、`nspname name`、`nspowner oid` 和 `nspacl aclitem[]`。`pg_temp_N` 是临时 namespace 生命周期对象，不能把静态 placeholder 冒充当前 session 的目录行。

## 修复

在 namespace 目录具备完整 schema、typed protocol metadata、ACL 和真实 session temp namespace 生命周期前，`pg_catalog.pg_namespace` 及当前数据库中无同名普通表时的未限定查询都返回 `0A000`。移除原有三列文本 renderer；底层 `CatalogManager` namespace API 未改。

代码提交：`6daf7bf1 fix(catalog): fail closed for pg_namespace`。同名用户表路径回归：`b43445d3 test: preserve pg_namespace table lookup`。

## 验证

- 变更前 wire 复现：三列均为 text/OID 25，且暴露 `pg_temp_1` placeholder。
- `bash scripts/build.sh`：生产构建通过。
- `python3 tests/pg_catalog_unavailable_sqlstate_protocol_e2e_test.py`：通过；限定/未限定名称返回 `0A000` 且连接 idle，当前数据库同名普通 `pg_namespace` 表仍可查询。
- `python3 tests/pg_class_query_protocol_e2e_test.py`：通过。
- PG18 官方文档核实列与类型；本机 `pgref` 为 PostgreSQL 17.2，不声称 PG18.6 runtime oracle/differential。
- 未运行完整注册 suite。

CAT-03 仍未补齐 namespace ACL、temp namespace/session 生命周期、完整 typed schema 或真实目录事务语义，保持 `partial`、未勾选。

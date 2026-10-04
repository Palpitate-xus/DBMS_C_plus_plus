# 第 993 项：`pg_roles` 的四列文本输出冒充系统视图

## 发现

旧 `pg_roles` SQL renderer 固定输出 `rolname`、`rolsuper`、`rolcreatedb`、`rolcanlogin` 四列文本，忽略查询投影和过滤。PostgreSQL 18 的 [`pg_roles` 官方 schema](https://www.postgresql.org/docs/18/view-pg-roles.html) 定义 13 列，包括继承、创建角色、复制、连接数限制、脱敏密码、有效期、RLS bypass、配置和角色 OID。因此 SQL 客户端得到的列集合与类型都不可信。

## 修复

删除不完整 renderer，将显式 `pg_catalog.pg_roles` 与当前库无同名普通表/视图时的未限定查询纳入已知未实现系统视图的 `0A000` fail-closed 路径。若普通表或视图使用同名 `pg_roles`，仍交给普通 relation 解析。`SHOW USERS`、`SHOW ROLES` 和底层 `CatalogService` role API 未改。

代码和测试提交：`b63e3e93 fix(catalog): reject incomplete pg roles view`。

## 验证

- `bash scripts/build.sh`：生产构建通过。
- `python3 tests/pg_catalog_unavailable_sqlstate_protocol_e2e_test.py`：通过，覆盖限定/未限定查询、`0A000`、ReadyForQuery，以及同名普通表和视图正常访问。
- `python3 tests/pg_class_query_protocol_e2e_test.py`：通过。
- `python3 tests/div14_feature_gate_test.py`：通过，包含 `SHOW USERS` / `SHOW ROLES` 回归。
- PostgreSQL 18 官方文档核对 `pg_roles` 13 列；未运行 PG18.6 runtime oracle/differential。
- 完整注册 suite 未运行。

这里只停止伪装成 PostgreSQL view 的查询，不实现 typed `pg_roles`、`pg_authid`/`pg_auth_members` SQL schema 或完整权限语义；CAT-03 仍为 `partial`。

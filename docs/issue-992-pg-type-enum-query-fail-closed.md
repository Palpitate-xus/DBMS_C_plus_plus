# 第 992 项：`pg_type` / `pg_enum` 的旧 SQL 输出不可信

## 发现

`pg_type` 的旧 SQL 分支无论查询列表是什么都固定输出 `oid typname typnamespace typtype typlen` 五列；PostgreSQL 18 官方 [`pg_type` schema](https://www.postgresql.org/docs/18/catalog-pg-type.html) 定义 32 列。`pg_enum` 虽然内部维护 OID、所属类型、排序值和标签四项数据，但旧 SQL 分支也总是输出完整四列，忽略 `SELECT` 投影和过滤，并沿用文本输出协议，不能作为 PostgreSQL typed catalog 查询。

## 修复

移除了这两个不完整 SQL renderer。显式 `pg_catalog.pg_type` / `pg_catalog.pg_enum` 查询，以及当前库中没有同名普通表或视图时的未限定查询，明确返回 `0A000`。同名用户表和视图继续走普通 relation 查询路径。枚举类型的底层 `CatalogService` API、DDL、标签持久化和 DML 未改；只停止把不可信 SQL 结果伪装成系统目录。

代码和测试提交：`3ce4345a fix(catalog): reject untyped pg type projections`。

## 验证

- `bash scripts/build.sh`：生产构建通过。
- `python3 tests/pg_catalog_unavailable_sqlstate_protocol_e2e_test.py`：通过，覆盖两个目录的限定/未限定查询、`0A000` 与 idle `ReadyForQuery`，并验证同名用户表和同名普通视图仍可查。
- `python3 tests/pg_class_query_protocol_e2e_test.py`：通过。
- `python3 tests/postgres_protocol_test.py`：通过；枚举 DDL 和现有协议回归均正常，旧目录 SQL 查询改为明确拒绝。
- PostgreSQL 18 官方文档核对 `pg_type` 的 32 列及 `pg_enum` 的四列定义；未运行 PG18.6 runtime oracle/differential。
- 完整注册 suite 未运行。

此修复不实现 SQL 可见的 typed `pg_type` / `pg_enum` 查询、内置类型全集或其完整 catalog 语义；CAT-03 继续为 `partial`。

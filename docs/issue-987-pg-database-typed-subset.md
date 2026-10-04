# 第 987 项：`pg_database` 返回类型和 schema 不准确

## 发现

旧虚拟实现固定输出四列 `datname encoding datcollate datctype`，把 `encoding` 作为文本 `UTF8` 返回，并为所有数据库伪造 `en_US.UTF-8` locale。它还忽略投影和过滤条件，所以 `SELECT *` 会把这四列当成 PostgreSQL 的完整目录 schema 返回。PostgreSQL 18 文档列出 18 列；其中 `datname` 是 `name`，`encoding` 是 `int4`，而 `datcollate`/`datctype` 是 `text`。见[PostgreSQL 18 `pg_database` 文档](https://www.postgresql.org/docs/18/catalog-pg-database.html)。

## 修复

`pg_database` 现在只提供可准确实现的 typed 子集：`datname name` 和 `encoding integer`，其中 encoding 为 6（UTF8）。数据库创建路径只允许 UTF8；其他 encoding 在创建前 fail-closed。简单 projection/WHERE 由表达式执行器处理，协议列描述报告 PostgreSQL OID 19 和 23。

未实现的 PostgreSQL 18 已知列返回 `0A000`，不存在的列返回 `42703`；`SELECT *` 返回 `0A000`，不会把不完整 schema 冒充完整目录。显式 `pg_catalog.pg_database` 使用虚拟目录；未限定名称仅在当前库没有同名普通表或 view 时使用虚拟目录。

代码与协议测试提交：`b7fb4224 fix(catalog): expose typed pg_database subset`。

## 验证

- `bash scripts/build.sh`：生产构建通过。
- `python3 tests/pg_catalog_unavailable_sqlstate_protocol_e2e_test.py`：通过。验证限定/未限定读取、`datname`/`encoding` 值与列 OID、未实现列及 `SELECT *` 的 `0A000`、idle `ReadyForQuery`，并保留其他 catalog 的 fail-closed 覆盖。
- `python3 tests/pg_class_query_protocol_e2e_test.py`：通过。
- 当前本机 `pgref` 容器报告 `server_version_num=170002`，不是 PostgreSQL 18.6；它仅作只读 PG17.2 对照，本项不声称通过 PG18.6 oracle/differential。PG18 的列名和类型按官方文档核对。
- 本项没有重跑完整注册 suite。

## 未完成部分

这只是准确暴露 `datname` 与 UTF8 `encoding` 的查询子集，不提供其余 16 列、目录持久化/共享存储、完整权限和依赖语义，也没有完成 CAT-03。总清单仍未完成，CAT-03 保持 `partial` 且未勾选。

# 第 989 项：`pg_stats` 与 `pg_statistic` 被伪装成同一张表

## 发现

旧 SQL 虚拟目录对 `pg_stats` 和 `pg_statistic` 都返回同样 8 列：`schemaname`、`tablename`、`attname`、`null_frac`、`n_distinct`、`most_common_vals`、`most_common_freqs`、`histogram_bounds`；协议描述把每列都标成 text/OID 25。对变更前生产二进制的 wire 检查确认，`SELECT *` 在两个名字上都成功并返回这套相同描述。

PostgreSQL 18 文档中，`pg_stats` 是按调用用户可读权限过滤的视图，共 17 列；`pg_statistic` 是结构不同且默认不应公开读取的内部 catalog，其统计槽由多组类型化字段组成。详见[PostgreSQL 18 `pg_stats`](https://www.postgresql.org/docs/18/view-pg-stats.html)和[PostgreSQL 18 `pg_statistic`](https://www.postgresql.org/docs/18/catalog-pg-statistic.html)。

## 修复

在 SQL 路径补齐两种独立目录的真实 schema、typed rows 和权限过滤前，`pg_catalog.pg_stats`、`pg_catalog.pg_statistic` 以及当前库无同名普通表时的未限定名称统一返回 `0A000`。不再通过同一文本 renderer 暴露虚假字段或把敏感 catalog 当成公开视图。现有 `StorageEngine::getPgStatsRows`／`queryPgCatalog` 内部统计接口未改动。

代码与协议测试提交：`5b7df04c fix(catalog): reject untyped pg stats relations`。

## 验证

- 变更前复现：限定查询 `SELECT *` 在两个名字上都成功，列头为相同 8 列且全部 OID 25。
- `bash scripts/build.sh`：生产构建通过。
- `python3 tests/pg_catalog_unavailable_sqlstate_protocol_e2e_test.py`：通过；两个名字的限定/未限定路径均为 `0A000` 和 idle `ReadyForQuery`，原生同名表仍可访问。
- `python3 tests/pg_class_query_protocol_e2e_test.py`：通过。
- `bash scripts/build_one_test.sh pg_stats_test`：通过；ANALYZE 统计持久化、内部统计读取与现存 `queryPgCatalog` 单测接口仍正常。
- 本机 `pgref` 为 PostgreSQL 17.2；本项按 PostgreSQL 18 官方文档核 schema，不声称 PG18.6 runtime oracle/differential。
- 未运行完整注册 suite。

## 回归测试同步（2026-10-04）

之后在当前工作分支运行了完整注册入口 `bash scripts/build_tests.sh`：460
个 C++ 测试全部通过，204 个 E2E 中 202 个通过；仅
`analyze_null_statistics_protocol_e2e_test.py` 与
`pg_stats_histogram_extent_protocol_e2e_test.py` 仍试图通过 SQL 查询旧的
8 列 `pg_stats` 假视图，因此与本项的 `0A000` fail-closed 行为冲突。其
对应的 `analyze_null_statistics_test.cpp` 和
`pg_stats_histogram_extent_test.cpp` 已在同一轮全部通过，分别验证内部
NULL fraction 和 histogram 上界。

现已同步两项协议回归：继续验证 `ANALYZE` 后 SQL 查询及连接恢复，并
显式确认未实现的 `pg_stats` SQL 路径返回 `0A000`。两项修改后的 E2E
单独重跑均通过；完整注册入口尚未在这次仅测试修改后重跑。此次默认构建
使用 TLS stub，TLS E2E 按设计跳过；TLS 专项运行时证据见第 997 项。

CAT-03 仍缺 `pg_stats`／`pg_statistic` 的不同真实 schema、typed 查询、权限及统计槽语义，清单保持 `partial`、未勾选。

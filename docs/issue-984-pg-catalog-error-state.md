# 第 984 项：未实现 pg_catalog relation 的错误状态

## 问题与范围

检查 `CAT-03` 时发现，`pg_catalog.pg_index` 和 `pg_catalog.pg_operator` 尚无真实 catalog 实现，但 SQL 查询会落入不相关的 relation/锁错误路径：带 schema 名的查询可能返回 `55P03`，未限定名查询可能返回 `42P01`。这两种错误都没有准确说明这些已知系统 catalog 当前不受支持。

## 修复

对这两个尚未实现的系统 catalog，在显式 `pg_catalog` 名称及未限定名称（且当前数据库不存在同名 relation 时）返回 `0A000`。这避免把“功能未实现”伪装成锁冲突或普通对象缺失；错误仍是单语句范围，连接可继续执行后续查询。若用户数据库中存在同名 relation，仍保留普通 relation 的访问路径。

代码和回归测试提交：`ffb63d19 fix(catalog): report unsupported pg catalog state`。

## 验证

- `bash scripts/build.sh`：通过最终代码构建。
- `python3 tests/pg_catalog_unavailable_sqlstate_protocol_e2e_test.py`：通过最终代码；覆盖 `pg_index`、`pg_operator` 的限定/未限定查询、`0A000`、idle `ReadyForQuery` 及同连接后续 `SELECT 1`。
- 完整注册测试脚本此前在该修复的限定名称版本通过（`All tests passed`）；后来增加未限定名称处理和断言后，重新通过了生产构建及上述定向 E2E，但未再运行完整脚本。
- 未运行 PostgreSQL 18.6 oracle/differential；不声称与 PostgreSQL 输出等价。

## 未完成部分

这只修正错误分类，不实现 `pg_index`、`pg_operator` 的数据、列类型、OID/依赖关系或执行器读取，也不补齐 `CAT-03` 列出的其他 catalog。因此 `CAT-03` 仅推进到 `partial`，清单复选框保持未完成。

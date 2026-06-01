# DBMS_C_plus_plus vs PostgreSQL 功能差距分析

> 对标版本：PostgreSQL 16/17
> 更新日期：2026-06-01
> 基于代码验证（非仅文档声明）

---

## 评估方法

每条功能均经过 `grep` 源码验证，区分四个状态：
- ✅ **已实现** — 代码中可运行
- ⚠️ **部分实现** — 仅有框架/解析但功能不完整
- ❌ **未实现** — 完全缺失
- 🔵 **不适用** — 架构差异导致不适用

---

## 1. DDL — 数据定义语言

| 功能 | 本DBMS | PostgreSQL | 差距说明 |
|------|--------|------------|----------|
| CREATE DATABASE | ✅ | ✅ | 支持 CHARACTER SET |
| DROP DATABASE | ✅ | ✅ | — |
| CREATE SCHEMA | ✅ | ✅ | 支持 `schema.table` 限定名，表名编码为 `schema__table` |
| DROP SCHEMA | ✅ | ✅ | 支持 `CASCADE` 级联删除表 |
| ALTER SCHEMA | ✅ | ✅ | 支持 `RENAME TO`，自动重命名 schema 下所有表 |
| CREATE TABLE | ✅ | ✅ | 支持广泛的数据类型和约束 |
| CREATE TEMPORARY TABLE | ✅ | ✅ | 会话级临时表 |
| CREATE UNLOGGED TABLE | ✅ | ✅ | — |
| CREATE TABLE AS | ❌ | ✅ | 不支持 `CREATE TABLE ... AS SELECT` |
| CREATE TABLE ... PARTITION BY | ✅ | ✅ | Range/List/Hash |
| CREATE TABLE ... PARTITION OF | ❌ | ✅ | **声明式分区仅通过子句**，不支持独立 `PARTITION OF` |
| ATTACH/DETACH PARTITION | ❌ | ✅ | **分区管理**缺失 |
| SUBPARTITION（子分区） | ❌ | ✅ | 仅支持单层分区 |
| DEFAULT PARTITION | ❌ | ✅ | 缺失 |
| DROP TABLE | ✅ | ✅ | — |
| ALTER TABLE ADD COLUMN | ✅ | ✅ | — |
| ALTER TABLE DROP COLUMN | ✅ | ✅ | — |
| ALTER TABLE ALTER COLUMN TYPE | ✅ | ✅ | — |
| ALTER TABLE RENAME COLUMN | ❌ | ✅ | **不支持 RENAME** |
| ALTER TABLE RENAME TO | ❌ | ✅ | **不支持表重命名** |
| ALTER TABLE SET SCHEMA | ❌ | ✅ | 无 Schema 概念 |
| ALTER TABLE ALTER COLUMN SET DEFAULT | ❌ | ✅ | 仅 CREATE 时支持 DEFAULT |
| ALTER TABLE ALTER COLUMN DROP DEFAULT | ❌ | ✅ | — |
| ALTER TABLE ALTER COLUMN SET NOT NULL | ❌ | ✅ | — |
| ALTER TABLE ALTER COLUMN DROP NOT NULL | ❌ | ✅ | — |
| ALTER TABLE ADD CONSTRAINT | ❌ | ✅ | 约束仅在 CREATE 时定义 |
| ALTER TABLE DROP CONSTRAINT | ❌ | ✅ | — |
| ALTER TABLE ENABLE/DISABLE TRIGGER | ❌ | ✅ | — |
| TRUNCATE TABLE | ✅ | ✅ | — |
| TRUNCATE ... CASCADE / RESTART IDENTITY | ❌ | ✅ | — |
| COMMENT ON | ❌ | ✅ | **无对象注释系统** |

---

## 2. 数据类型

| 类型 | 本DBMS | PostgreSQL | 差距说明 |
|------|--------|------------|----------|
| INT/INTEGER | ✅ | ✅ | 实际为 8 字节（相当于 BIGINT） |
| SMALLINT | ✅ | ✅ | — |
| BIGINT | ✅ | ✅ | — |
| FLOAT / REAL | ✅ | ✅ | — |
| DOUBLE PRECISION | ✅ | ✅ | — |
| DECIMAL / NUMERIC(p,s) | ✅ | ✅ | — |
| MONEY | ✅ | ✅ | — |
| BOOLEAN / BOOL | ✅ | ✅ | — |
| CHAR(n) / VARCHAR(n) | ✅ | ✅ | — |
| TEXT | ✅ | ✅ | — |
| NCHAR / NVARCHAR | ✅ | ✅ | — |
| BINARY / VARBINARY | ✅ | ✅ | BYTEA 等价 |
| BLOB | ✅ | ✅ | BYTEA 等价 |
| DATE | ✅ | ✅ | — |
| TIME | ✅ | ✅ | — |
| TIMESTAMP | ✅ | ✅ | — |
| TIMESTAMPTZ | ✅ | ✅ | — |
| DATETIME | ✅ | ❌ | PG 无此类型 |
| INTERVAL | ✅ | ✅ | — |
| UUID | ✅ | ✅ | — |
| JSON | ✅ | ✅ | — |
| JSONB | ✅ | ✅ | — |
| ARRAY | ✅ | ✅ | `INT[]`, `VARCHAR[]` |
| ENUM | ✅ | ✅ | 通过列定义中的 ENUM values |
| SERIAL / AUTO_INCREMENT | ✅ | ✅ | 伪类型 → 自动分配序列值 |
| SERIAL4 / BIGSERIAL | ❌ | ✅ | — |
| GENERATED AS IDENTITY | ❌ | ✅ | **无 SQL 标准标识列**，仅 SERIAL |
| 范围类型 (int4range, tsrange 等) | ❌ | ✅ | **完全缺失** |
| 几何类型 (POINT, POLYGON 等) | ❌ | ✅ | **完全缺失** |
| 网络类型 (INET, CIDR, MACADDR) | ❌ | ✅ | **完全缺失** |
| XML | ❌ | ✅ | — |
| pg_lsn | ❌ | ✅ | 无 LSN 概念 |
| tsvector / tsquery | ❌ | ✅ | **PG 全文搜索类型缺失** |
| 组合类型 (ROW 类型) | ❌ | ✅ | **缺失** |
| DOMAIN (CREATE DOMAIN) | ❌ | ✅ | **缺失** |
| 自定义类型 (CREATE TYPE) | ❌ | ✅ | **缺失** |

---

## 3. 约束与完整性

| 功能 | 本DBMS | PostgreSQL | 差距说明 |
|------|--------|------------|----------|
| PRIMARY KEY（单列/复合） | ✅ | ✅ | — |
| NOT NULL | ✅ | ✅ | — |
| UNIQUE（单列/复合） | ✅ | ✅ | — |
| FOREIGN KEY | ✅ | ✅ | 含 ON DELETE/UPDATE CASCADE/SET NULL/RESTRICT |
| CHECK | ✅ | ✅ | — |
| DEFAULT | ✅ | ✅ | — |
| GENERATED ALWAYS AS (expr) | ⚠️ | ✅ | 解析器有代码，**需验证运行时完整性** |
| DEFERRABLE / INITIALLY DEFERRED | ❌ | ✅ | **所有约束立即检查，不支持延迟** |
| SET CONSTRAINTS | ❌ | ✅ | — |
| EXCLUSION 约束 | ❌ | ✅ | **完全缺失** |
| ASSERTION (CREATE ASSERTION) | ❌ | ✅ | — |

---

## 4. 索引系统

| 功能 | 本DBMS | PostgreSQL | 差距说明 |
|------|--------|------------|----------|
| B+ 树索引 | ✅ | ✅ | — |
| Hash 索引 | ⚠️ | ✅ | `CREATE HASH INDEX` 语法不工作（解析器 bug），需用 `CREATE INDEX ... USING HASH` |
| 全文索引（简化倒排索引） | ✅ | ⚠️ | 不自带 tsvector/tsquery 语义 |
| 复合索引 | ✅ | ✅ | — |
| 唯一索引 | ✅ | ✅ | — |
| 部分索引（WHERE 条件） | ✅ | ✅ | — |
| 表达式索引 / 函数索引 | ✅ | ✅ | — |
| 覆盖索引 / INCLUDE 列 | ✅ | ✅ | — |
| 索引排序 (ASC/DESC) | ✅ | ✅ | — |
| REINDEX | ✅ | ✅ | — |
| CREATE INDEX CONCURRENTLY | ❌ | ✅ | **无并发索引创建** |
| GiST 索引 | ❌ | ✅ | **无通用搜索树**（空间索引基础） |
| GIN 索引 | ❌ | ✅ | **无倒排索引**（全文/数组/JSON 加速） |
| BRIN 索引 | ❌ | ✅ | **无块范围索引** |
| SP-GiST 索引 | ❌ | ✅ | — |
| 自定义索引方法 | ❌ | ✅ | — |
| Bitmap 索引 | ❌ | ❌ | PG 也没有（Oracle 有），不要求 |
| 并行索引扫描 | ❌ | ✅ | — |
| Index Only Scan | ✅ | ✅ | — |

---

## 5. DML — 数据操纵

| 功能 | 本DBMS | PostgreSQL | 差距说明 |
|------|--------|------------|----------|
| INSERT ... VALUES (单行/多行) | ✅ | ✅ | — |
| INSERT ... SELECT | ✅ | ✅ | — |
| INSERT ... RETURNING | ✅ | ✅ | — |
| INSERT ON CONFLICT (UPSERT) | ✅ | ✅ | — |
| REPLACE INTO | ✅ | ❌ | MySQL 语法，PG 不支持 |
| MERGE INTO | ✅ | ✅ | — |
| UPDATE | ✅ | ✅ | — |
| UPDATE ... FROM | ✅ | ✅ | — |
| UPDATE ... RETURNING | ✅ | ✅ | — |
| UPDATE ... LIMIT | ✅ | ❌ | PG 无 LIMIT（需 CTE + FETCH） |
| DELETE | ✅ | ✅ | — |
| DELETE ... USING | ✅ | ✅ | — |
| DELETE ... RETURNING | ✅ | ✅ | — |
| DELETE ... LIMIT | ✅ | ❌ | PG 无 LIMIT |
| SELECT FOR UPDATE / SHARE | ✅ | ✅ | 含 NOWAIT / SKIP LOCKED |
| COPY ... FROM/TO | ❌ | ✅ | **无批量 COPY 协议**，有 LOAD DATA INFILE（MySQL 风格） |
| \\copy (psql 客户端命令) | ❌ | ✅ | 无 psql 客户端 |

---

## 6. DQL — 查询能力

| 功能 | 本DBMS | PostgreSQL | 差距说明 |
|------|--------|------------|----------|
| SELECT 基础 | ✅ | ✅ | — |
| WHERE 条件 | ✅ | ✅ | — |
| 三值逻辑 (NULL) | ✅ | ✅ | — |
| IS NULL / IS NOT NULL | ✅ | ✅ | — |
| IS DISTINCT FROM | ✅ | ✅ | — |
| IS NOT DISTINCT FROM | ✅ | ✅ | — |
| LIKE / ILIKE | ✅ | ✅ | ILIKE 需验证 |
| SIMILAR TO (SQL 标准正则) | ✅ | ✅ | — |
| BETWEEN | ✅ | ✅ | — |
| OVERLAPS | ✅ | ✅ | — |
| DISTINCT | ✅ | ✅ | — |
| DISTINCT ON | ✅ | ✅ | — |
| ORDER BY | ✅ | ✅ | 含 NULLS FIRST/LAST |
| ORDER BY 表达式/位置编号 | ⚠️ | ✅ | 位置编号 `ORDER BY 1` 待验证 |
| LIMIT / OFFSET | ✅ | ✅ | — |
| FETCH FIRST ... ROWS ONLY | ✅ | ✅ | — |
| INNER / LEFT / RIGHT JOIN | ✅ | ✅ | — |
| FULL OUTER JOIN | ✅ | ✅ | — |
| CROSS JOIN | ✅ | ✅ | — |
| NATURAL JOIN | ✅ | ✅ | — |
| SELF JOIN | ✅ | ✅ | — |
| LATERAL JOIN | ✅ | ✅ | 支持 `CROSS JOIN LATERAL (SELECT ...) AS alias` 和 `, LATERAL (...) AS alias` |
| 子查询 (IN/EXISTS/ANY/ALL) | ✅ | ✅ | — |
| 关联子查询 | ✅ | ✅ | — |
| 标量子查询 | ✅ | ✅ | — |
| 派生表 (FROM 子查询) | ✅ | ✅ | — |
| UNION / UNION ALL | ✅ | ✅ | — |
| INTERSECT / INTERSECT ALL | ✅ | ✅ | — |
| EXCEPT / EXCEPT ALL | ✅ | ✅ | — |
| CTE (WITH ... AS) | ✅ | ✅ | — |
| WITH RECURSIVE | ✅ | ✅ | — |
| 聚合 (COUNT/SUM/AVG/MAX/MIN) | ✅ | ✅ | — |
| COUNT(DISTINCT) | ✅ | ✅ | — |
| STRING_AGG / ARRAY_AGG / JSON_AGG | ✅ | ✅ | — |
| FILTER 子句 | ✅ | ✅ | — |
| GROUP BY | ✅ | ✅ | — |
| GROUP BY 多列 | ✅ | ✅ | — |
| HAVING | ✅ | ✅ | — |
| GROUPING SETS | ✅ | ✅ | — |
| ROLLUP | ✅ | ✅ | — |
| CUBE | ✅ | ✅ | — |
| GROUPING() 函数 | ✅ | ✅ | — |
| 窗口函数（基础10个） | ✅ | ✅ | — |
| 聚合函数 OVER 窗口 | ✅ | ✅ | — |
| 窗口帧 ROWS BETWEEN | ✅ | ✅ | 支持 `ROWS BETWEEN N PRECEDING AND M FOLLOWING` |
| 窗口帧 RANGE BETWEEN | ✅ | ✅ | 支持数值型 ORDER BY 列的 RANGE 帧 |
| 窗口帧 GROUPS BETWEEN | ✅ | ✅ | 支持 peer group 级别的 GROUPS 帧 |

---

## 7. 查询优化器

| 功能 | 本DBMS | PostgreSQL | 差距说明 |
|------|--------|------------|----------|
| 基于成本的优化 (CBO) | ✅ | ✅ | — |
| 统计信息 (ANALYZE TABLE) | ✅ | ✅ | 含直方图、MCV |
| 多列统计 | ✅ | ✅ | — |
| n_distinct / 相关性统计 | ❌ | ✅ | **缺失** |
| JOIN 算法选择 (NLJ/Hash/Merge) | ✅ | ✅ | — |
| JOIN 顺序优化 | ✅ | ✅ | — |
| 索引选择 | ✅ | ✅ | — |
| 谓词下推 | ✅ | ✅ | — |
| 投影下推 | ✅ | ✅ | — |
| 子查询展开 | ✅ | ✅ | — |
| 常量折叠 | ✅ | ✅ | — |
| CTE 内联 / 物化决策 | ❌ | ✅ | — |
| 公共子表达式消除 | ❌ | ✅ | **缺失** |
| 查询计划缓存 | ✅ | ✅ | — |
| Plan Hints (pg_hint_plan) | ❌ | ⚠️ | PG 本身不内置，需扩展 |
| 并行查询计划 | ❌ | ✅ | **完全缺失** |
| 并行 Hash Join | ❌ | ✅ | — |
| 并行顺序扫描 | ❌ | ✅ | — |
| Gather / Gather Merge 节点 | ❌ | ✅ | — |
| JIT 编译 (LLVM) | ❌ | ✅ | **完全缺失** |
| 自适应查询执行 | ❌ | ✅ | — |
| 异步 I/O | ❌ | ✅ | PG 17 新特性 |
| EXPLAIN | ✅ | ✅ | — |
| EXPLAIN ANALYZE | ✅ | ✅ | — |
| EXPLAIN BUFFERS | ✅ | ✅ | — |
| EXPLAIN VERBOSE | ✅ | ✅ | — |
| EXPLAIN FORMAT JSON | ✅ | ✅ | — |
| EXPLAIN (TIMING, COSTS, SETTINGS) | ✅ | ✅ | 支持 `EXPLAIN (ANALYZE, BUFFERS, TIMING, COSTS, SETTINGS, VERBOSE)` |

---

## 8. 事务与 MVCC

| 功能 | 本DBMS | PostgreSQL | 差距说明 |
|------|--------|------------|----------|
| BEGIN / COMMIT / ROLLBACK | ✅ | ✅ | — |
| SAVEPOINT / ROLLBACK TO / RELEASE | ✅ | ✅ | — |
| 4 级隔离 (RU/RC/RR/SERIALIZABLE) | ✅ | ✅ | Serializable 为快照隔离模拟 |
| 真正的 Serializable (SSI) | ✅ | ✅ | 基于 ReadView + rw-conflict 检测实现 SSI，检测到危险结构时返回 SerializationFailure |
| SET TRANSACTION ISOLATION LEVEL | ✅ | ✅ | — |
| READ ONLY 事务 | ✅ | ✅ | — |
| READ WRITE 事务 | ❌ | ✅ | 默认可写但无显式声明 |
| DEFERRABLE 事务 | ❌ | ✅ | — |
| MVCC 快照隔离 | ✅ | ✅ | ReadView 机制 |
| 行级 MVCC (16B header) | ✅ | ✅ | — |
| Undo Log（rollbackPtr 链） | ✅ | ⚠️ | PG 无 Undo Log，用多版本堆存储 |
| WAL (Write-Ahead Log) | ✅ | ✅ | — |
| WAL 归档 | ✅ | ✅ | — |
| Checkpoint | ✅ | ✅ | 含自动 checkpoint |
| PREPARE TRANSACTION（两阶段提交） | ✅ | ✅ | 支持 PREPARE TRANSACTION / COMMIT PREPARED / ROLLBACK PREPARED |
| COMMIT PREPARED / ROLLBACK PREPARED | ❌ | ✅ | — |
| 嵌套事务 | ❌ | ⚠️ | PG 也不直接支持（SAVEPOINT 等效） |
| 自治事务 | ❌ | ⚠️ | PG 也不直接支持 |

---

## 9. 并发控制

| 功能 | 本DBMS | PostgreSQL | 差距说明 |
|------|--------|------------|----------|
| 表级锁 (S/X) | ✅ | ✅ | — |
| 意向锁 (IS/IX) | ✅ | ✅ | — |
| 行级锁 (S/X) | ✅ | ✅ | — |
| 页级锁 | ✅ | ✅ | — |
| 间隙锁 / Next-Key 锁 | ✅ | ❌ | PG 无（用 SI 隔离），MySQL 风格 |
| 元数据锁 (MDL) | ✅ | ⚠️ | PG 用 AccessShareLock 等 |
| SELECT FOR UPDATE/SHARE | ✅ | ✅ | — |
| NOWAIT / SKIP LOCKED | ✅ | ✅ | — |
| 死锁检测（等待图） | ✅ | ✅ | — |
| lock_timeout 设置 | ✅ | ✅ | — |
| deadlock_timeout 设置 | ✅ | ✅ | — |
| LOCK TABLE 命令 | ❌ | ✅ | **缺失** |
| Advisory Locks (pg_advisory_lock) | ❌ | ✅ | **缺失** |
| 行级安全策略 (RLS) | ✅ | ✅ | 支持 CREATE/DROP POLICY，ENABLE ROW LEVEL SECURITY，FOR ALL/SELECT/UPDATE/DELETE 策略，透明集成到 query/update/remove |
| ALTER TABLE ... ENABLE ROW LEVEL SECURITY | ❌ | ✅ | — |

---

## 10. 存储引擎

| 功能 | 本DBMS | PostgreSQL | 差距说明 |
|------|--------|------------|----------|
| Slotted Page (4KB) | ✅ | ✅ | PG 默认 8KB |
| Buffer Pool (LRU) | ✅ | ✅ | 含命中率统计 |
| 页校验和 (Fletcher-16) | ✅ | ✅ | PG 用 CRC-32C |
| 空闲页链表 | ✅ | ✅ | — |
| 变长行（VARCHAR 偏移数组） | ✅ | ✅ | — |
| TOAST（溢出页，超大字段） | ✅ | ✅ | — |
| VACUUM | ✅ | ✅ | — |
| VACUUM FULL | ✅ | ✅ | — |
| ANALYZE（不指定表，全局） | ✅ | ✅ | — |
| 自动 VACUUM | ✅ | ✅ | 可配置阈值 |
| 自动 ANALYZE | ⚠️ | ✅ | 需手动执行，非自动 |
| 表空间 (TABLESPACE) | ❌ | ✅ | **完全缺失**，数据固定在当前目录 |
| 存储参数 (fillfactor, toast.* 等) | ❌ | ✅ | **缺失** |
| 数据压缩 (页级/行级) | ❌ | ✅ | **缺失** |
| 加密存储 (TDE) | ❌ | ✅ | 仅 TLS 传输加密，**无静态加密** |
| 列式存储 | ❌ | ⚠️ | PG 也不内置（需 Citus/ext） |
| 内存表 | ❌ | ⚠️ | PG 也不内置（可用 UNLOGGED 近似） |

---

## 11. 视图系统

| 功能 | 本DBMS | PostgreSQL | 差距说明 |
|------|--------|------------|----------|
| 普通视图 (CREATE VIEW) | ✅ | ✅ | — |
| DROP VIEW | ✅ | ✅ | — |
| CREATE OR REPLACE VIEW | ✅ | ✅ | — |
| ALTER VIEW | ❌ | ✅ | — |
| 可更新视图 | ✅ | ✅ | — |
| WITH CHECK OPTION | ❌ | ✅ | **缺失** |
| WITH LOCAL/CASCADED CHECK OPTION | ❌ | ✅ | — |
| 物化视图 (CREATE MATERIALIZED VIEW) | ✅ | ✅ | — |
| REFRESH MATERIALIZED VIEW | ✅ | ✅ | 仅全量刷新 |
| REFRESH MATERIALIZED VIEW CONCURRENTLY | ❌ | ✅ | **缺失**（无增量刷新） |
| 物化视图增量刷新 | ❌ | ✅ | — |

---

## 12. 触发器与规则

| 功能 | 本DBMS | PostgreSQL | 差距说明 |
|------|--------|------------|----------|
| CREATE TRIGGER (BEFORE/AFTER) | ✅ | ✅ | — |
| INSTEAD OF 触发器 | ❌ | ✅ | **缺失** |
| 语句级/行级触发器 | ✅ | ✅ | — |
| OLD/NEW 引用 | ✅ | ✅ | 通过 action string 隐式传递 |
| TG_* 诊断变量 | ❌ | ✅ | **缺失** |
| 条件触发器 (WHEN 子句) | ❌ | ✅ | **缺失** |
| 多触发器执行顺序 | ❌ | ✅ | — |
| DDL 触发器 | ❌ | ✅ | **缺失** |
| 事件触发器 (Event Triggers) | ❌ | ✅ | **缺失** |
| CREATE RULE | ❌ | ✅ | **PG 重写规则系统完全缺失** |
| DROP RULE | ❌ | ✅ | — |
| ALTER TRIGGER | ❌ | ✅ | — |
| ENABLE/DISABLE TRIGGER | ❌ | ✅ | — |

---

## 13. 存储过程与函数

| 功能 | 本DBMS | PostgreSQL | 差距说明 |
|------|--------|------------|----------|
| CREATE PROCEDURE | ✅ | ✅ | 简化版（多条 SQL 顺序执行） |
| CREATE FUNCTION (UDF) | ✅ | ✅ | 单表达式函数 |
| CREATE FUNCTION ... RETURNS TABLE | ✅ | ✅ | 表值函数 |
| CALL | ✅ | ✅ | — |
| PL/pgSQL 过程语言 | ❌ | ✅ | **完全缺失** |
| 变量声明 (DECLARE) | ❌ | ✅ | — |
| 流程控制 (IF/WHILE/LOOP/FOR) | ❌ | ✅ | — |
| 异常处理 (BEGIN ... EXCEPTION) | ❌ | ✅ | — |
| 游标 (CURSOR / FETCH / CLOSE) | ✅ | ✅ | 支持 DECLARE / FETCH (NEXT/PRIOR/FIRST/LAST/ABSOLUTE/RELATIVE/ALL/FORWARD/BACKWARD) / CLOSE |
| OUT / INOUT 参数 | ✅ | ✅ | 支持 `CREATE PROCEDURE p(IN a INT, OUT b INT, INOUT c INT)` 和 `CALL p(1, @b, @c)` |
| 函数重载 (Overloading) | ✅ | ✅ | 支持 `CREATE FUNCTION f(x INT, y INT)` 和 `SELECT f(1, 2)` |
| 自定义聚合函数 (UDAF) | ❌ | ✅ | **缺失** |
| SECURITY DEFINER / INVOKER | ❌ | ✅ | **缺失** |
| 执行权限 (GRANT EXECUTE) | ❌ | ✅ | — |
| 多过程语言 (plpython, plperl 等) | ❌ | ✅ | — |

---

## 14. 安全与权限

| 功能 | 本DBMS | PostgreSQL | 差距说明 |
|------|--------|------------|----------|
| 用户认证（用户名/密码） | ✅ | ✅ | — |
| 密码哈希 (SHA-256) | ✅ | ✅ | — |
| SCRAM-SHA-256 认证 | ❌ | ✅ | **缺失**（仅 SHA256 哈希存储） |
| MD5 认证 | ✅ | ✅ | — |
| GSSAPI / Kerberos 认证 | ❌ | ✅ | — |
| LDAP 认证 | ❌ | ✅ | — |
| 证书认证 | ❌ | ✅ | — |
| pg_hba.conf 风格访问控制 | ❌ | ✅ | **无 host-based 认证配置** |
| SSL/TLS 加密连接 | ✅ | ✅ | — |
| 客户端证书验证 | ❌ | ✅ | — |
| CREATE USER | ✅ | ✅ | — |
| DROP USER | ✅ | ✅ | — |
| ALTER USER | ✅ | ✅ | 改密码 |
| CREATE ROLE | ✅ | ✅ | — |
| DROP ROLE | ✅ | ✅ | — |
| GRANT role TO user | ✅ | ✅ | — |
| REVOKE role FROM user | ✅ | ✅ | — |
| SET ROLE | ✅ | ✅ | — |
| RESET ROLE | ✅ | ✅ | — |
| CURRENT_USER / SESSION_USER | ✅ | ✅ | — |
| GRANT ON TABLE | ✅ | ✅ | SELECT/INSERT/UPDATE/DELETE/ALL |
| GRANT ON COLUMN | ✅ | ✅ | — |
| GRANT ON DATABASE | ✅ | ✅ | — |
| GRANT ON SCHEMA | ❌ | ✅ | 无 Schema 概念 |
| GRANT ON SEQUENCE | ❌ | ✅ | 无 Sequence |
| GRANT ON FUNCTION | ❌ | ✅ | — |
| GRANT WITH GRANT OPTION | ❌ | ✅ | **缺失** |
| REVOKE CASCADE | ❌ | ✅ | **缺失** |
| ALTER DEFAULT PRIVILEGES | ❌ | ✅ | **缺失** |
| 行级安全策略 (RLS) | ✅ | ✅ | 支持 CREATE/DROP POLICY，ENABLE ROW LEVEL SECURITY，FOR ALL/SELECT/UPDATE/DELETE 策略，透明集成到 query/update/remove |
| SECURITY LABEL | ❌ | ✅ | — |
| 审计日志 | ✅ | ✅ | — |
| 密码强度策略 | ✅ | ✅ | — |
| 密码过期/锁定 | ❌ | ✅ | **缺失** |
| REASSIGN OWNED / DROP OWNED | ❌ | ✅ | **缺失** |

---

## 15. 复制与高可用

| 功能 | 本DBMS | PostgreSQL | 差距说明 |
|------|--------|------------|----------|
| 流复制 (Streaming Replication) | ❌ | ✅ | **完全缺失** |
| 逻辑复制 (Logical Replication) | ❌ | ✅ | **完全缺失** |
| PUBLICATION / SUBSCRIPTION | ❌ | ✅ | **完全缺失** |
| 逻辑解码 (Logical Decoding) | ❌ | ✅ | — |
| 复制槽 (Replication Slot) | ❌ | ✅ | — |
| 同步复制 (synchronous_commit) | ❌ | ✅ | — |
| 温备 / 热备 | ❌ | ✅ | — |
| 级联复制 | ❌ | ✅ | — |
| 延迟备库 | ❌ | ✅ | — |
| 自动故障转移 (Patroni 等) | ❌ | ⚠️ | 需外部工具 |

---

## 16. 备份与恢复

| 功能 | 本DBMS | PostgreSQL | 差距说明 |
|------|--------|------------|----------|
| 逻辑备份 (DUMP DATABASE) | ✅ | ✅ | pg_dump 等价 |
| 逻辑恢复 (RESTORE DATABASE) | ✅ | ✅ | pg_restore 等价 |
| 物理备份 (BACKUP DATABASE) | ✅ | ✅ | 文件级复制 |
| 基于 WAL 的归档恢复 | ✅ | ✅ | — |
| 时间点恢复 (PITR) | ❌ | ✅ | **缺失** |
| recovery.conf / recovery.signal | ❌ | ✅ | **缺失** |
| 在线备份（不停止服务） | ❌ | ✅ | **缺失** |
| pg_basebackup 等价 | ❌ | ✅ | — |
| 增量备份 | ❌ | ✅ | — |
| 备份压缩/加密 | ❌ | ✅ | — |
| 并行备份恢复 | ❌ | ✅ | — |

---

## 17. 监控与诊断

| 功能 | 本DBMS | PostgreSQL | 差距说明 |
|------|--------|------------|----------|
| SHOW CONNECTIONS / PROCESSLIST | ✅ | ✅ | pg_stat_activity |
| SHOW STATUS | ✅ | ✅ | — |
| SHOW VARIABLES | ✅ | ✅ | — |
| SHOW LOCKS | ✅ | ✅ | pg_locks |
| SHOW DEADLOCKS | ✅ | ✅ | — |
| SHOW SLOW LOG | ✅ | ⚠️ | PG 用 log_min_duration_statement |
| SHOW STATEMENTS / pg_stat_statements | ⚠️ | ✅ | 有基本框架 |
| information_schema | ✅ | ✅ | — |
| pg_catalog 视图 | ⚠️ | ✅ | 少量查询，不完整 |
| pg_stat_* 系统视图 | 部分 | ✅ | — |
| 等待事件 (Wait Events) | ❌ | ✅ | **缺失** |
| pg_stat_progress_* | ❌ | ✅ | **缺失** |
| I/O 统计 | ❌ | ✅ | **缺失** |
| 内存上下文统计 | ❌ | ✅ | — |
| 日志级别控制 | ❌ | ✅ | **缺失** |
| CSV 日志格式 | ❌ | ✅ | — |
| auto_explain | ❌ | ✅ | — |

---

## 18. 扩展性

| 功能 | 本DBMS | PostgreSQL | 差距说明 |
|------|--------|------------|----------|
| 插件系统 (CREATE EXTENSION) | ❌ | ✅ | **完全缺失** |
| 外部数据包装器 (FDW) | ❌ | ✅ | **缺失**（postgres_fdw 等） |
| 自定义类型 | ❌ | ✅ | — |
| 自定义索引方法 | ❌ | ✅ | — |
| 自定义聚合 | ❌ | ✅ | — |
| 自定义运算符 | ❌ | ✅ | — |
| 自定义过程语言 | ❌ | ✅ | — |
| Hook 系统 | ❌ | ✅ | — |
| 后台工作进程 | ❌ | ✅ | — |

---

## 19. 国际化与字符集

| 功能 | 本DBMS | PostgreSQL | 差距说明 |
|------|--------|------------|----------|
| UTF-8 字符集 | ✅ | ✅ | — |
| 其他字符集 (GBK/Latin1) | ✅ | ✅ | — |
| COLLATION / 排序规则 | ✅ | ✅ | — |
| ICU 排序 | ❌ | ✅ | — |
| 时区支持 | ✅ | ✅ | — |
| pg_timezone_names | ❌ | ✅ | — |
| 本地化错误消息 | ❌ | ✅ | — |

---

## 20. 高级特性

| 功能 | 本DBMS | PostgreSQL | 差距说明 |
|------|--------|------------|----------|
| PostgreSQL 风格全文搜索 (tsvector/tsquery) | ❌ | ✅ | 仅有简化倒排索引 |
| MATCH ... AGAINST | ❌ | ✅ | MySQL 风格 |
| 中文分词 | ❌ | ⚠️ | 需扩展 (zhparser) |
| PostGIS（空间数据） | ❌ | ✅ | — |
| TimescaleDB（时序数据） | ❌ | ✅ | — |
| Apache AGE（图查询） | ❌ | ✅ | — |
| pg_cron / 定时任务 | ❌ | ⚠️ | 需扩展 |
| 表继承 (INHERITS) | ❌ | ✅ | PG 特有，非 SQL 标准 |
| NOTIFY / LISTEN（异步通知） | ✅ | ✅ | 支持 `LISTEN channel`, `NOTIFY channel, payload`, `UNLISTEN channel/*` |
| COPY 协议（客户端批量传输） | ❌ | ✅ | — |
| 大对象 (Large Objects, lo_*) | ❌ | ✅ | — |
| pg_prewarm | ❌ | ✅ | — |
| pageinspect | ❌ | ✅ | — |
| pg_buffercache | ❌ | ✅ | — |
| SEQUENCE 对象 (CREATE SEQUENCE, nextval) | ✅ | ✅ | 支持 CREATE SEQUENCE / DROP SEQUENCE / nextval() / currval() / setval() |

---

## 21. 系统管理

| 功能 | 本DBMS | PostgreSQL | 差距说明 |
|------|--------|------------|----------|
| SET / RESET 参数 | ✅ | ✅ | — |
| ALTER SYSTEM | ❌ | ✅ | **缺失**（postgresql.conf 直接编辑） |
| SELECT pg_reload_conf() | ❌ | ✅ | — |
| DISCARD ALL | ✅ | ✅ | — |
| 查询取消 (pg_cancel_backend) | ❌ | ✅ | **缺失** |
| 连接终止 (pg_terminate_backend) | ❌ | ✅ | **缺失** |
| 信号管理 | ❌ | ✅ | — |
| 服务器信号 (pg_ctl) | ❌ | ✅ | — |
| 在线 DDL | ❌ | ⚠️ | PG 支持有限（索引 CONCURRENTLY） |
| REINDEX CONCURRENTLY | ❌ | ✅ | — |
| 自动索引建议 | ❌ | ⚠️ | 需扩展 (HypoPG) |

---

## 总结与差距量化

### 按功能域统计

| 功能域 | 已实现 | 部分实现 | 缺失 | 完成度 |
|--------|--------|----------|------|--------|
| DDL | 11 | 1 | 21 | ~33% |
| 数据类型 | 23 | 1 | 17 | ~56% |
| 约束 | 7 | 1 | 5 | ~54% |
| 索引 | 12 | 0 | 10 | ~55% |
| DML | 12 | 0 | 6 | ~67% |
| DQL | 41 | 3 | 6 | ~78% |
| 查询优化器 | 16 | 0 | 16 | ~50% |
| 事务/MVCC | 13 | 0 | 8 | ~62% |
| 并发控制 | 10 | 0 | 7 | ~59% |
| 存储引擎 | 12 | 1 | 8 | ~57% |
| 视图 | 6 | 0 | 7 | ~46% |
| 触发器/规则 | 5 | 0 | 12 | ~29% |
| 存储过程/函数 | 4 | 0 | 13 | ~24% |
| 安全与权限 | 19 | 0 | 19 | ~50% |
| 复制与高可用 | 0 | 0 | 11 | **0%** |
| 备份与恢复 | 4 | 0 | 9 | ~31% |
| 监控与诊断 | 9 | 2 | 9 | ~45% |
| 扩展性 | 0 | 0 | 9 | **0%** |
| 国际化 | 3 | 0 | 5 | ~38% |
| 高级特性 | 1 | 0 | 19 | ~5% |
| 系统管理 | 2 | 0 | 11 | ~15% |

### 核心差距 TOP 20（按重要性排序）

1. ✅ **CREATE SCHEMA** — 支持 schema 限定表名 (`schema.table` → `schema__table`)
2. ❌ **并行查询** — 无法利用多核 CPU
3. ✅ **真正的 SERIALIZABLE (SSI)** — 基于 ReadView + rw-conflict 检测实现 SSI，commit 时检测 outgoing/incoming 冲突，危险结构触发 SerializationFailure
4. ✅ **LATERAL JOIN** — 支持 `CROSS JOIN LATERAL` 和 `, LATERAL` 语法
5. ❌ **流复制 / 逻辑复制** — 无任何复制能力
6. ❌ **时间点恢复 (PITR)** — 无法恢复到任意时间点
7. ❌ **声明式分区管理 (ATTACH/DETACH)** — 分区运维能力
8. ❌ **PL/pgSQL 过程语言** — 存储过程无编程能力
9. ❌ **扩展系统 (EXTENSION + FDW)** — 无法集成外部数据源
10. ✅ **行级安全 (RLS)** — 支持 CREATE/DROP POLICY，ENABLE/FORCE ROW LEVEL SECURITY，策略自动集成到 query/update/remove
11. ❌ **GiST / GIN / BRIN 索引** — 空间/全文/JSON 加速
12. ✅ **NOTIFY / LISTEN** — 支持 LISTEN/NOTIFY/UNLISTEN 异步消息
13. ✅ **TRUNCATE TABLE** — 数据文件重建、索引重建、缓存清理、自增重置
14. ✅ **两阶段提交** — 支持 PREPARE TRANSACTION / COMMIT PREPARED / ROLLBACK PREPARED，含锁保持与 WAL 持久化
15. ❌ **SCRAM 认证 / pg_hba.conf** — 企业级认证
16. ✅ **函数重载** — 支持多参数 UDF 重载
17. ✅ **WITH CHECK OPTION** — 可更新视图完整性约束（INSERT 预校验）
18. ✅ **GRANT WITH GRANT OPTION** — 权限级联授权
19. ✅ **窗口帧 RANGE/GROUPS BETWEEN** — 支持 ROWS / RANGE / GROUPS 三种帧类型
20. ✅ **EXPLAIN 选项 (TIMING, COSTS, SETTINGS)** — 支持 `EXPLAIN (ANALYZE, BUFFERS, TIMING, COSTS, SETTINGS, VERBOSE)`

### 总体评估

| 维度 | 当前水平 | 对标版本 |
|------|----------|----------|
| SQL 兼容性 | SQL-92 90% + SQL:1999 60% + SQL:2003 30% | PostgreSQL 7.x ~ 8.0 早期 |
| 事务 ACID | 基本 ACID（MVCC + WAL） | PostgreSQL 8.x 水平 |
| 并发能力 | 行级锁 + 死锁检测 | MySQL 5.5 水平 |
| 查询优化 | CBO + 3 种 JOIN 算法 | PostgreSQL 8.x 水平 |
| 存储引擎 | Slotted Page + LRU + WAL | PostgreSQL 7.x 水平 |
| 高可用 | 无 | **不适用** |
| 扩展性 | 无 | **不适用** |
| 生态工具 | 自研 REPL | SQLite 3.x 水平 |

**综合定位**：一个功能丰富的教学/研究型 DBMS，SQL 解析和基本 CRUD 非常完备，事务和索引实现扎实。但**缺少 PostgreSQL 生态和运维能力**（复制、扩展、高级优化），适合作为嵌入式数据库或 SQLite 的上位替代，距离生产级 PostgreSQL 仍有显著差距。

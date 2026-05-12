# DBMS 功能差距分析（对标主流生产级数据库）

> 对比对象：PostgreSQL 16、MySQL 8.0、SQL Server 2022、Oracle 21c
> 当前实现版本：详见 [README.md](../README.md)
> 文档版本：2026-05

---

## 目录

1. [当前已实现功能概览](#1-当前已实现功能概览)
2. [SQL 语言与表达式](#2-sql-语言与表达式)
3. [数据类型系统](#3-数据类型系统)
4. [约束与完整性](#4-约束与完整性)
5. [索引系统](#5-索引系统)
6. [事务与并发控制](#6-事务与并发控制)
7. [存储引擎](#7-存储引擎)
8. [视图与物化视图](#8-视图与物化视图)
9. [触发器与存储过程](#9-触发器与存储过程)
10. [安全与权限](#10-安全与权限)
11. [复制与高可用](#11-复制与高可用)
12. [备份与恢复](#12-备份与恢复)
13. [查询优化器](#13-查询优化器)
14. [性能监控](#14-性能监控)
15. [扩展性](#15-扩展性)
16. [管理与运维](#16-管理与运维)
17. [国际化与字符集](#17-国际化与字符集)
18. [高级特性](#18-高级特性)
19. [优先级路线图](#19-优先级路线图)

---

## 1. 当前已实现功能概览

| 类别 | 已实现 |
|------|--------|
| DDL | CREATE/DROP DATABASE/TABLE/INDEX/VIEW，ALTER TABLE ADD/DROP COLUMN，CREATE TEMPORARY TABLE |
| DML | INSERT/SELECT/UPDATE/DELETE |
| 查询 | WHERE、ORDER BY、LIMIT/OFFSET、DISTINCT、GROUP BY/HAVING、JOIN (INNER/LEFT/RIGHT)、UNION/UNION ALL、子查询 (IN/EXISTS/ANY/ALL)、聚合 (COUNT/MAX/MIN/SUM/AVG)、窗口函数 (ROW_NUMBER/RANK/LAG/LEAD)、EXPLAIN |
| 数据类型 | INT、TINYINT、LONG、CHAR(n)、VARCHAR(n)、DATE、SERIAL |
| 约束 | PRIMARY KEY、NOT NULL、UNIQUE、FOREIGN KEY (含 ON DELETE)、DEFAULT、CHECK |
| 事务 | BEGIN/COMMIT/ROLLBACK、4 级隔离 (RU/RC/RR/SERIALIZABLE)、MVCC ReadView、WAL、Undo Log、Checkpoint |
| 索引 | B+ 树主键索引、B+ 树二级索引 |
| 存储 | Slotted Page (4KB)、Buffer Pool (LRU)、页校验和 (Fletcher-16)、空闲页链表、VACUUM、VARCHAR 变长行 |
| 查询优化 | 火山模型、成本估计、JOIN 算法选择 (NLJ/Hash/Merge)、索引选择、统计信息 (ANALYZE) |
| 并发 | 表级读写锁、死锁检测 |
| 网络 | TCP 服务、独立 Session、连接管理 |
| 工具 | CSV 导入/导出、预编译语句、SHOW CONNECTIONS/STATUS |
| 权限 | 用户登录、表级 GRANT/REVOKE |

---

## 2. SQL 语言与表达式

### 2.1 JOIN 类型

| 功能 | 当前 | PostgreSQL | MySQL | SQL Server | 优先级 |
|------|------|------------|-------|------------|--------|
| INNER JOIN | ✅ | ✅ | ✅ | ✅ | - |
| LEFT JOIN | ✅ | ✅ | ✅ | ✅ | - |
| RIGHT JOIN | ✅ | ✅ | ✅ | ✅ | - |
| **FULL OUTER JOIN** | ❌ | ✅ | ❌(模拟) | ✅ | **P0** |
| **CROSS JOIN** | ❌ | ✅ | ✅ | ✅ | **P0** |
| **NATURAL JOIN** | ❌ | ✅ | ✅ | - | P2 |
| **SELF JOIN** | 部分 | ✅ | ✅ | ✅ | P1 |
| **LATERAL JOIN** | ❌ | ✅ | - | ✅ APPLY | P3 |

### 2.2 集合操作

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| UNION / UNION ALL | ✅ | ✅ | - |
| **INTERSECT / INTERSECT ALL** | ❌ | ✅ | **P1** |
| **EXCEPT / MINUS** | ❌ | ✅ | **P1** |

### 2.3 CTE（公用表表达式）

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **WITH ... AS (...)** | ❌ | ✅ | **P0** |
| **WITH RECURSIVE** | ❌ | ✅ | P1 |
| **多 CTE 串联** | ❌ | ✅ | P1 |
| **CTE 内 INSERT/UPDATE/DELETE** | ❌ | ✅ | P2 |

### 2.4 子查询

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| WHERE col IN (subquery) | ✅ | ✅ | - |
| WHERE EXISTS (subquery) | ✅ | ✅ | - |
| WHERE col > ANY/ALL (subquery) | ✅ | ✅ | - |
| **标量子查询（SELECT 中）** | ❌ | ✅ | **P1** |
| **FROM 中的子查询（派生表）** | ❌ | ✅ | **P0** |
| **关联子查询** | 部分 | ✅ | P1 |

### 2.5 条件表达式

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **CASE WHEN ... THEN ... END** | ❌ | ✅ | **P0** |
| **COALESCE(a, b, ...)** | ❌ | ✅ | **P1** |
| **NULLIF(a, b)** | ❌ | ✅ | P1 |
| **GREATEST / LEAST** | ❌ | ✅ | P2 |
| **IIF / IF()** | ❌ | ✅ | P2 |

### 2.6 类型转换

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **CAST(expr AS type)** | ❌ | ✅ | **P0** |
| **CONVERT()** | ❌ | ✅ | P1 |
| **TO_NUMBER / TO_CHAR / TO_DATE** | ❌ | ✅ | P1 |
| 隐式类型转换 | 部分 | ✅ | P2 |

### 2.7 字符串函数

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **LENGTH / CHAR_LENGTH** | ❌ | ✅ | **P0** |
| **UPPER / LOWER** | ❌ | ✅ | **P0** |
| **SUBSTRING / SUBSTR** | ❌ | ✅ | **P0** |
| **TRIM / LTRIM / RTRIM** | ❌ | ✅ | **P0** |
| **CONCAT / 字符串拼接 (\|\|)** | ❌ | ✅ | **P0** |
| **REPLACE** | ❌ | ✅ | **P1** |
| **POSITION / INSTR** | ❌ | ✅ | P1 |
| **LPAD / RPAD** | ❌ | ✅ | P2 |
| **REVERSE** | ❌ | ✅ | P2 |
| **REGEXP / 正则匹配** | ❌ | ✅ | P2 |
| **SPLIT_PART / STRING_TO_ARRAY** | ❌ | ✅ | P3 |

### 2.8 数学函数

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **ABS** | ❌ | ✅ | **P0** |
| **ROUND / CEIL / FLOOR** | ❌ | ✅ | **P0** |
| **POWER / SQRT** | ❌ | ✅ | P1 |
| **MOD / %** | ❌ | ✅ | P1 |
| **LOG / EXP** | ❌ | ✅ | P2 |
| **SIN / COS / TAN** | ❌ | ✅ | P3 |
| **RANDOM / RAND** | ❌ | ✅ | P2 |

### 2.9 日期/时间函数

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **NOW / CURRENT_TIMESTAMP** | ❌ | ✅ | **P0** |
| **CURRENT_DATE / CURRENT_TIME** | ❌ | ✅ | **P0** |
| **EXTRACT(YEAR/MONTH/DAY FROM ...)** | ❌ | ✅ | **P0** |
| **DATE_TRUNC** | ❌ | ✅ | P1 |
| **DATE_ADD / DATE_SUB / INTERVAL** | ❌ | ✅ | **P1** |
| **DATEDIFF** | ❌ | ✅ | P1 |
| **DATE_FORMAT / TO_CHAR** | ❌ | ✅ | P1 |
| **AGE** | ❌ | ✅ | P3 |

### 2.10 聚合函数扩展

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| COUNT/MAX/MIN/SUM/AVG | ✅ | ✅ | - |
| **COUNT(DISTINCT col)** | ❌ | ✅ | **P0** |
| **STRING_AGG / GROUP_CONCAT** | ❌ | ✅ | P1 |
| **ARRAY_AGG** | ❌ | ✅ | P2 |
| **JSON_AGG / JSONB_AGG** | ❌ | ✅ | P3 |
| **VAR / STDDEV** | ❌ | ✅ | P3 |
| **PERCENTILE_CONT/DISC** | ❌ | ✅ | P3 |
| **FILTER 子句** | ❌ | ✅ | P3 |

### 2.11 窗口函数扩展

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| ROW_NUMBER / RANK | ✅ | ✅ | - |
| LAG / LEAD | ✅ | ✅ | - |
| **DENSE_RANK** | ❌ | ✅ | **P1** |
| **NTILE(n)** | ❌ | ✅ | P2 |
| **FIRST_VALUE / LAST_VALUE / NTH_VALUE** | ❌ | ✅ | P2 |
| **PERCENT_RANK / CUME_DIST** | ❌ | ✅ | P3 |
| **PARTITION BY 子句** | ❌ | ✅ | **P1** |
| **聚合函数 OVER 子句** | ❌ | ✅ | P1 |
| **窗口帧 (ROWS/RANGE BETWEEN)** | ❌ | ✅ | P2 |

### 2.12 INSERT 扩展

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| INSERT INTO ... VALUES | ✅ | ✅ | - |
| **INSERT 多行 (VALUES (...), (...))** | ❌ | ✅ | **P0** |
| **INSERT INTO ... SELECT** | ❌ | ✅ | **P0** |
| **INSERT ON CONFLICT / UPSERT** | ❌ | ✅ | **P1** |
| **INSERT ... RETURNING** | ❌ | ✅ | P1 |
| **REPLACE INTO** | ❌ | ✅ MySQL | P2 |
| **MERGE INTO** | ❌ | ✅ | P2 |

### 2.13 UPDATE / DELETE 扩展

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| UPDATE/DELETE 基础 | ✅ | ✅ | - |
| **UPDATE FROM (多表 UPDATE)** | ❌ | ✅ | **P1** |
| **DELETE ... USING (多表 DELETE)** | ❌ | ✅ | **P1** |
| **UPDATE/DELETE ... RETURNING** | ❌ | ✅ | P1 |
| **UPDATE/DELETE ... LIMIT** | ❌ | ✅ | P2 |

### 2.14 ORDER BY 扩展

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| ORDER BY col [ASC/DESC] | ✅ | ✅ | - |
| **ORDER BY 多列** | 部分 | ✅ | **P0** |
| **ORDER BY 表达式** | ❌ | ✅ | P1 |
| **NULLS FIRST / NULLS LAST** | ❌ | ✅ | P2 |

### 2.15 GROUP BY 扩展

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| GROUP BY 单列 | ✅ | ✅ | - |
| **GROUP BY 多列** | ❌ | ✅ | **P0** |
| **GROUPING SETS** | ❌ | ✅ | P3 |
| **ROLLUP / CUBE** | ❌ | ✅ | P3 |

---

## 3. 数据类型系统

### 3.1 数值类型

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| INT/TINYINT/LONG | ✅ | ✅ | - |
| **SMALLINT** | ❌ | ✅ | P1 |
| **BIGINT / INT8** | ✅ (LONG) | ✅ | - |
| **DECIMAL / NUMERIC** | ❌ | ✅ | **P0** |
| **FLOAT / REAL** | ❌ | ✅ | **P0** |
| **DOUBLE PRECISION** | ❌ | ✅ | **P0** |
| **MONEY** | ❌ | ✅ | P3 |
| **无符号类型 (UNSIGNED)** | ❌ | ✅ MySQL | P2 |

### 3.2 字符串类型

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| CHAR(n) / VARCHAR(n) | ✅ | ✅ | - |
| **TEXT (无限长字符串)** | ❌ | ✅ | **P0** |
| **CLOB** | ❌ | ✅ | P2 |
| **NCHAR / NVARCHAR (Unicode)** | ❌ | ✅ | P2 |

### 3.3 二进制类型

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **BINARY / VARBINARY** | ❌ | ✅ | **P1** |
| **BLOB / BYTEA** | ❌ | ✅ | **P1** |

### 3.4 日期时间类型

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| DATE | ✅ | ✅ | - |
| **TIME** | ❌ | ✅ | **P1** |
| **TIMESTAMP** | ❌ | ✅ | **P0** |
| **TIMESTAMPTZ (带时区)** | ❌ | ✅ | P1 |
| **INTERVAL** | ❌ | ✅ | P2 |
| **DATETIME** | ❌ | ✅ MySQL | P1 |
| **YEAR** | ❌ | ✅ MySQL | P3 |

### 3.5 布尔类型

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **BOOLEAN / BOOL** | ❌ | ✅ | **P1** |
| **TRUE / FALSE 字面量** | ❌ | ✅ | P1 |

### 3.6 半结构化类型

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **JSON** | ❌ | ✅ | **P1** |
| **JSONB (二进制 JSON)** | ❌ | ✅ PG | **P1** |
| **JSON 操作符 (->, ->>, @>)** | ❌ | ✅ | P2 |
| **JSON 路径查询** | ❌ | ✅ | P2 |
| **XML** | ❌ | ✅ | P3 |

### 3.7 数组类型

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **数组类型 (INT[], VARCHAR[])** | ❌ | ✅ PG | P2 |
| **数组操作 ([], ANY, ALL)** | ❌ | ✅ PG | P2 |
| **unnest()** | ❌ | ✅ PG | P2 |

### 3.8 特殊类型

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **ENUM** | ❌ | ✅ | P2 |
| **UUID** | ❌ | ✅ | P2 |
| **几何类型 (POINT/POLYGON)** | ❌ | ✅ PG | P3 |
| **网络地址 (INET/CIDR/MACADDR)** | ❌ | ✅ PG | P3 |
| **范围类型 (RANGE)** | ❌ | ✅ PG | P3 |
| **自定义类型 (CREATE TYPE)** | ❌ | ✅ PG | P3 |
| **复合类型** | ❌ | ✅ PG | P3 |

### 3.9 NULL 处理

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **NULL 字面量** | 部分 | ✅ | **P0** |
| **IS NULL / IS NOT NULL** | 部分 | ✅ | **P0** |
| **三值逻辑 (TRUE/FALSE/UNKNOWN)** | 部分 | ✅ | **P1** |
| **DISTINCT NULL 处理** | 部分 | ✅ | P1 |

---

## 4. 约束与完整性

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| PRIMARY KEY | ✅ | ✅ | - |
| NOT NULL | ✅ | ✅ | - |
| UNIQUE | ✅ | ✅ | - |
| FOREIGN KEY | ✅ | ✅ | - |
| DEFAULT | ✅ | ✅ | - |
| CHECK | ✅ | ✅ | - |
| **复合主键** | ❌ | ✅ | **P0** |
| **复合 UNIQUE** | ❌ | ✅ | **P0** |
| **多列 FOREIGN KEY** | ❌ | ✅ | P1 |
| **ON UPDATE CASCADE / RESTRICT** | 部分 | ✅ | P1 |
| **DEFERRABLE 约束** | ❌ | ✅ PG | P3 |
| **EXCLUSION 约束** | ❌ | ✅ PG | P3 |
| **域 (CREATE DOMAIN)** | ❌ | ✅ PG | P3 |
| **断言 (CREATE ASSERTION)** | ❌ | 标准 | P3 |
| **生成列 (GENERATED COLUMN)** | ❌ | ✅ | P2 |
| **标识列 (IDENTITY)** | ✅ SERIAL | ✅ | - |

---

## 5. 索引系统

### 5.1 索引类型

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **B+ 树索引** | ✅ | ✅ | - |
| **Hash 索引** | ❌ | ✅ | **P1** |
| **Bitmap 索引** | ❌ | ✅ Oracle | P3 |
| **GiST 索引（通用搜索树）** | ❌ | ✅ PG | P3 |
| **GIN 索引（倒排索引）** | ❌ | ✅ PG | P3 |
| **BRIN 索引（块范围）** | ❌ | ✅ PG | P3 |
| **全文索引** | ❌ | ✅ | P2 |
| **空间索引（R-Tree）** | ❌ | ✅ | P3 |

### 5.2 索引特性

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| 单列索引 | ✅ | ✅ | - |
| **复合索引（多列索引）** | ❌ | ✅ | **P0** |
| **索引列排序 (ASC/DESC)** | ❌ | ✅ | **P1** |
| **唯一索引** | ✅ (PK) | ✅ | P1 (非PK唯一索引) |
| **部分索引 (WHERE 条件)** | ❌ | ✅ PG | P2 |
| **表达式索引 / 函数索引** | ❌ | ✅ | P2 |
| **覆盖索引 / Index Only Scan** | ❌ | ✅ | **P0** |
| **INCLUDE 列** | ❌ | ✅ | P2 |
| **聚集索引 (Clustered Index)** | 部分 | ✅ MSSQL | P2 |
| **并发索引创建 (CONCURRENTLY)** | ❌ | ✅ | P3 |
| **索引重建 (REINDEX)** | ❌ | ✅ | P2 |

---

## 6. 事务与并发控制

### 6.1 隔离级别

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| READ UNCOMMITTED | ✅ | ✅ | - |
| READ COMMITTED | ✅ | ✅ | - |
| REPEATABLE READ | ✅ | ✅ | - |
| SERIALIZABLE | 简化 | ✅ | **P1** (真正可序列化) |

### 6.2 锁机制

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| 表级锁 | ✅ | ✅ | - |
| **行级锁** | ❌ | ✅ | **P0** |
| **页级锁** | ❌ | ✅ MSSQL | P2 |
| **共享锁 / 排他锁** | ✅ | ✅ | - |
| **意向锁 (IS/IX)** | ❌ | ✅ | P2 |
| **间隙锁 (Gap Lock)** | ❌ | ✅ MySQL | P1 |
| **Next-Key 锁** | ❌ | ✅ MySQL | P1 |
| **元数据锁 (MDL)** | ❌ | ✅ MySQL | P2 |
| **SELECT FOR UPDATE / SHARE** | ❌ | ✅ | **P1** |
| **NOWAIT / SKIP LOCKED** | ❌ | ✅ | P2 |
| 死锁检测 | ✅ | ✅ | - |
| **死锁超时** | ❌ | ✅ | P2 |

### 6.3 事务控制

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| BEGIN/COMMIT/ROLLBACK | ✅ | ✅ | - |
| **SAVEPOINT / ROLLBACK TO** | ❌ | ✅ | **P0** |
| **RELEASE SAVEPOINT** | ❌ | ✅ | P1 |
| **嵌套事务** | ❌ | ✅ | P3 |
| **只读事务 (READ ONLY)** | ❌ | ✅ | P2 |
| **自治事务** | ❌ | ✅ Oracle | P3 |

### 6.4 分布式事务

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **两阶段提交 (2PC)** | ❌ | ✅ | P3 |
| **XA 事务** | ❌ | ✅ | P3 |
| **PREPARE TRANSACTION** | ❌ | ✅ PG | P3 |

---

## 7. 存储引擎

### 7.1 存储格式

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| 行式存储 | ✅ | ✅ | - |
| **列式存储 (Columnar)** | ❌ | ✅ MSSQL/MySQL | P3 |
| **内存表 (In-Memory)** | ❌ | ✅ | P3 |
| Slotted Page | ✅ | ✅ | - |
| **可插拔存储引擎** | ❌ | ✅ MySQL | P3 |

### 7.2 表分区

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **Range 分区** | ❌ | ✅ | **P1** |
| **List 分区** | ❌ | ✅ | **P1** |
| **Hash 分区** | ❌ | ✅ | **P1** |
| **Composite 复合分区** | ❌ | ✅ | P3 |
| **分区裁剪 (Partition Pruning)** | ❌ | ✅ | P2 |
| **分区交换 (EXCHANGE PARTITION)** | ❌ | ✅ | P3 |

### 7.3 数据压缩

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **行压缩** | ❌ | ✅ | P3 |
| **页压缩** | ❌ | ✅ | P3 |
| **列压缩** | ❌ | ✅ | P3 |
| **字典压缩** | ❌ | ✅ | P3 |

### 7.4 加密

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **透明数据加密 (TDE)** | ❌ | ✅ | P3 |
| **列级加密** | ❌ | ✅ | P3 |
| **静态加密 (At-Rest)** | ❌ | ✅ | P3 |

### 7.5 表空间

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **表空间 (Tablespace)** | ❌ | ✅ | P3 |
| **多个数据文件** | ❌ | ✅ | P3 |
| **指定存储路径** | ❌ | ✅ | P3 |

### 7.6 溢出存储

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **TOAST / 大字段溢出页** | ❌ | ✅ PG | P2 |
| **行链 / 行迁移** | ❌ | ✅ | P3 |
| **LOB 存储** | ❌ | ✅ | P2 |

---

## 8. 视图与物化视图

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| 普通视图 (CREATE VIEW) | ✅ | ✅ | - |
| DROP VIEW | ✅ | ✅ | - |
| **可更新视图** | ❌ | ✅ | P2 |
| **WITH CHECK OPTION** | ❌ | ✅ | P3 |
| **物化视图 (Materialized View)** | ❌ | ✅ | **P2** |
| **物化视图刷新** | ❌ | ✅ | P2 |
| **REFRESH MATERIALIZED VIEW** | ❌ | ✅ | P2 |
| **物化视图增量刷新** | ❌ | ✅ | P3 |
| **INSTEAD OF 视图触发器** | ❌ | ✅ | P3 |

---

## 9. 触发器与存储过程

### 9.1 触发器

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **BEFORE/AFTER INSERT/UPDATE/DELETE** | ❌ | ✅ | **P1** |
| **行级触发器 (FOR EACH ROW)** | ❌ | ✅ | P1 |
| **语句级触发器 (FOR EACH STATEMENT)** | ❌ | ✅ | P2 |
| **INSTEAD OF 触发器** | ❌ | ✅ | P3 |
| **触发器引用 OLD/NEW** | ❌ | ✅ | P1 |
| **多触发器执行顺序** | ❌ | ✅ | P3 |
| **DDL 触发器** | ❌ | ✅ | P3 |
| **事件触发器** | ❌ | ✅ PG | P3 |

### 9.2 存储过程与函数

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **存储过程 (CREATE PROCEDURE)** | ❌ | ✅ | P2 |
| **用户自定义函数 (CREATE FUNCTION)** | ❌ | ✅ | P2 |
| **PL/pgSQL / PL/SQL / T-SQL** | ❌ | ✅ | P3 |
| **变量声明 (DECLARE)** | ❌ | ✅ | P3 |
| **流程控制 (IF/WHILE/LOOP)** | ❌ | ✅ | P3 |
| **异常处理 (EXCEPTION)** | ❌ | ✅ | P3 |
| **CALL 调用** | ❌ | ✅ | P2 |
| **OUT/INOUT 参数** | ❌ | ✅ | P3 |
| **表值函数** | ❌ | ✅ | P3 |
| **聚合函数 (UDAF)** | ❌ | ✅ | P3 |

### 9.3 游标

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **DECLARE CURSOR** | ❌ | ✅ | P3 |
| **OPEN/FETCH/CLOSE** | ❌ | ✅ | P3 |
| **滚动游标** | ❌ | ✅ | P3 |

---

## 10. 安全与权限

### 10.1 用户与角色

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| CREATE USER | ✅ | ✅ | - |
| **DROP USER** | ❌ | ✅ | **P1** |
| **ALTER USER (改密码)** | ❌ | ✅ | **P1** |
| **CREATE ROLE / DROP ROLE** | ❌ | ✅ | P2 |
| **角色继承 (GRANT role TO user)** | ❌ | ✅ | P2 |
| **SET ROLE** | ❌ | ✅ | P3 |

### 10.2 权限粒度

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| 表级 GRANT/REVOKE | ✅ | ✅ | - |
| **列级权限** | ❌ | ✅ | P2 |
| **数据库级权限** | ❌ | ✅ | P1 |
| **Schema 级权限** | ❌ | ✅ | P2 |
| **行级安全策略 (RLS)** | ❌ | ✅ PG | P3 |
| **GRANT WITH GRANT OPTION** | ❌ | ✅ | P3 |
| **执行权限 (EXECUTE)** | ❌ | ✅ | P3 |

### 10.3 密码与认证

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| 用户名/密码登录 | ✅ | ✅ | - |
| **密码哈希存储 (bcrypt/scrypt)** | ❌ | ✅ | **P0** |
| **密码强度策略** | ❌ | ✅ | P2 |
| **密码过期** | ❌ | ✅ | P3 |
| **账号锁定** | ❌ | ✅ | P3 |
| **MD5/SHA256 认证** | ❌ | ✅ | P2 |
| **SCRAM 认证** | ❌ | ✅ PG | P3 |
| **LDAP / Kerberos / GSSAPI** | ❌ | ✅ | P3 |
| **证书认证** | ❌ | ✅ | P3 |

### 10.4 加密传输

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **SSL/TLS 加密连接** | ❌ | ✅ | **P0** |
| **客户端证书验证** | ❌ | ✅ | P3 |

### 10.5 审计

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| 操作日志 | 部分 | ✅ | - |
| **审计日志 (Audit Log)** | ❌ | ✅ | P2 |
| **登录审计** | ❌ | ✅ | P2 |
| **DDL/DML 审计** | ❌ | ✅ | P3 |

---

## 11. 复制与高可用

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **主从复制 (Streaming Replication)** | ❌ | ✅ | P3 |
| **逻辑复制 (Logical Replication)** | ❌ | ✅ | P3 |
| **半同步复制** | ❌ | ✅ | P3 |
| **多主复制** | ❌ | ✅ | P3 |
| **读写分离** | ❌ | ✅ | P3 |
| **自动故障转移 (Failover)** | ❌ | ✅ | P3 |
| **同步副本** | ❌ | ✅ | P3 |
| **延迟从库** | ❌ | ✅ | P3 |
| **复制槽 (Replication Slot)** | ❌ | ✅ PG | P3 |
| **Binlog / WAL 传输** | 部分 | ✅ | P3 |

---

## 12. 备份与恢复

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **逻辑备份 (mysqldump/pg_dump)** | 部分 (CSV) | ✅ | **P1** |
| **物理备份 (文件系统)** | 部分 | ✅ | P2 |
| **增量备份** | ❌ | ✅ | P3 |
| **基于 WAL 的归档** | 部分 | ✅ | P2 |
| **时间点恢复 (PITR)** | ❌ | ✅ | P3 |
| **在线备份** | ❌ | ✅ | P3 |
| **热备份** | ❌ | ✅ | P3 |
| **备份压缩** | ❌ | ✅ | P3 |
| **备份加密** | ❌ | ✅ | P3 |
| **跨数据库恢复** | ❌ | ✅ | P3 |

---

## 13. 查询优化器

### 13.1 统计信息

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| 表行数 / 基数 | ✅ | ✅ | - |
| 最小/最大值 | ✅ | ✅ | - |
| **直方图统计** | ❌ | ✅ | **P1** |
| **多列统计** | ❌ | ✅ | P2 |
| **相关性统计** | ❌ | ✅ | P3 |
| **MCV (Most Common Values)** | ❌ | ✅ PG | P2 |
| **自动统计收集** | ❌ | ✅ | **P1** |

### 13.2 优化器特性

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| 基于成本的优化 (CBO) | ✅ | ✅ | - |
| JOIN 算法选择 | ✅ | ✅ | - |
| 索引选择 | ✅ | ✅ | - |
| **JOIN 顺序优化** | 部分 | ✅ | **P1** |
| **谓词下推** | 部分 | ✅ | P1 |
| **投影下推** | ❌ | ✅ | P1 |
| **子查询展开** | 部分 | ✅ | P1 |
| **常量折叠** | ❌ | ✅ | P2 |
| **公共子表达式消除** | ❌ | ✅ | P3 |
| **查询重写** | 部分 | ✅ | P2 |
| **查询计划缓存** | ❌ | ✅ | P2 |
| **Plan Hints / 优化器提示** | ❌ | ✅ | P3 |
| **并行查询** | ❌ | ✅ | P3 |
| **自适应查询优化** | ❌ | ✅ | P3 |

### 13.3 EXPLAIN

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| EXPLAIN (估算成本) | ✅ | ✅ | - |
| **EXPLAIN ANALYZE (实际执行)** | ❌ | ✅ | **P1** |
| **EXPLAIN BUFFERS** | ❌ | ✅ | P2 |
| **EXPLAIN FORMAT JSON/XML** | ❌ | ✅ | P2 |
| **EXPLAIN VERBOSE** | ❌ | ✅ | P2 |

---

## 14. 性能监控

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| SHOW CONNECTIONS | ✅ | 部分 | - |
| SHOW STATUS | ✅ | 部分 | - |
| **information_schema** | ❌ | ✅ | **P1** |
| **pg_stat_* / sys.dm_* 系统视图** | ❌ | ✅ | **P1** |
| **慢查询日志** | 部分 | ✅ | P1 |
| **慢查询分析 (pt-query-digest)** | ❌ | ✅ | P3 |
| **锁等待监控** | ❌ | ✅ | P2 |
| **死锁日志** | 部分 | ✅ | P2 |
| **活动会话监控** | 部分 | ✅ | P2 |
| **缓冲池命中率** | ❌ | ✅ | P2 |
| **I/O 统计** | ❌ | ✅ | P3 |
| **CPU/内存统计** | ❌ | ✅ | P3 |
| **等待事件 (Wait Events)** | ❌ | ✅ | P3 |
| **Performance Schema** | ❌ | ✅ MySQL | P3 |
| **pg_stat_statements** | ❌ | ✅ PG | P3 |

---

## 15. 扩展性

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **插件系统 (CREATE EXTENSION)** | ❌ | ✅ PG | P3 |
| **外部表 (FDW)** | ❌ | ✅ PG | P3 |
| **自定义类型** | ❌ | ✅ PG | P3 |
| **自定义索引方法** | ❌ | ✅ PG | P3 |
| **自定义聚合函数** | ❌ | ✅ | P3 |
| **自定义运算符** | ❌ | ✅ PG | P3 |
| **过程语言扩展** | ❌ | ✅ PG | P3 |
| **Hook 系统** | ❌ | ✅ PG | P3 |

---

## 16. 管理与运维

### 16.1 系统目录

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **information_schema.tables** | ❌ | ✅ | **P0** |
| **information_schema.columns** | ❌ | ✅ | **P0** |
| **information_schema.views** | ❌ | ✅ | P1 |
| **information_schema.routines** | ❌ | ✅ | P2 |
| **information_schema.triggers** | ❌ | ✅ | P2 |
| **information_schema.statistics** | ❌ | ✅ | P1 |
| **information_schema.key_column_usage** | ❌ | ✅ | P2 |
| **pg_catalog / sys.* 表** | ❌ | ✅ | P2 |

### 16.2 SHOW 命令

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| SHOW CONNECTIONS | ✅ | - | - |
| **SHOW DATABASES** | ❌ | ✅ | **P0** |
| **SHOW TABLES** | ❌ | ✅ | **P0** |
| **SHOW COLUMNS FROM table** | ❌ | ✅ | **P0** |
| **SHOW CREATE TABLE** | ❌ | ✅ | **P1** |
| **SHOW INDEX FROM table** | ❌ | ✅ | P1 |
| **SHOW VARIABLES** | ❌ | ✅ | P2 |
| **SHOW PROCESSLIST** | 部分 | ✅ | P2 |
| **SHOW GRANTS** | ❌ | ✅ | P2 |
| **DESC / DESCRIBE table** | ❌ | ✅ | **P0** |

### 16.3 在线 DDL

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **在线 ALTER TABLE** | ❌ | ✅ | P3 |
| **在线索引创建** | ❌ | ✅ | P3 |
| **DDL 锁等待** | ❌ | ✅ | P3 |

### 16.4 自动维护

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **自动 VACUUM (autovacuum)** | ❌ | ✅ PG | P2 |
| **自动统计收集** | ❌ | ✅ | P2 |
| **自动索引重建** | ❌ | ✅ | P3 |
| **自动 Checkpoint** | 部分 | ✅ | P1 |
| **后台任务调度** | ❌ | ✅ | P3 |

### 16.5 配置管理

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **配置文件 (postgresql.conf/my.cnf)** | ❌ | ✅ | P2 |
| **运行时参数 (SET/SHOW)** | 部分 | ✅ | P2 |
| **会话级参数** | 部分 | ✅ | P2 |
| **全局参数** | ❌ | ✅ | P2 |
| **参数持久化** | ❌ | ✅ | P3 |

### 16.6 资源限制

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| 最大连接数 | ✅ | ✅ | - |
| **查询超时 (statement_timeout)** | ❌ | ✅ | P2 |
| **空闲事务超时** | ❌ | ✅ | P2 |
| **内存限制** | ❌ | ✅ | P3 |
| **每用户连接数限制** | ❌ | ✅ | P3 |
| **资源组 / 资源管理器** | ❌ | ✅ | P3 |

---

## 17. 国际化与字符集

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **字符集 (UTF-8/GBK/Latin1)** | ❌ | ✅ | **P1** |
| **排序规则 (Collation)** | ❌ | ✅ | P2 |
| **时区支持** | ❌ | ✅ | P2 |
| **本地化错误消息** | ❌ | ✅ | P3 |
| **Unicode 字符串处理** | ❌ | ✅ | P2 |

---

## 18. 高级特性

### 18.1 全文搜索

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **全文索引** | ❌ | ✅ | P3 |
| **MATCH ... AGAINST** | ❌ | ✅ MySQL | P3 |
| **tsvector / tsquery** | ❌ | ✅ PG | P3 |
| **中文分词** | ❌ | ✅ | P3 |

### 18.2 地理空间

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **PostGIS / 空间扩展** | ❌ | ✅ | P3 |
| **空间索引 (R-Tree/GiST)** | ❌ | ✅ | P3 |
| **空间函数 (ST_Distance/ST_Contains)** | ❌ | ✅ | P3 |

### 18.3 时序数据

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **TimescaleDB 扩展** | ❌ | ✅ | P3 |
| **超表 (Hypertable)** | ❌ | ✅ | P3 |
| **连续聚合** | ❌ | ✅ | P3 |

### 18.4 图查询

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **图数据库扩展 (AGE)** | ❌ | ✅ PG | P3 |
| **Cypher / openCypher** | ❌ | ✅ | P3 |
| **递归 CTE 模拟图遍历** | ❌ | ✅ | P3 |

### 18.5 OLAP

| 功能 | 当前 | 主流 | 优先级 |
|------|------|------|--------|
| **列式存储** | ❌ | ✅ | P3 |
| **向量化执行** | ❌ | ✅ | P3 |
| **MPP (大规模并行处理)** | ❌ | ✅ | P3 |
| **物化视图自动选择** | ❌ | ✅ | P3 |

---

## 19. 优先级路线图

### P0 — 关键功能（生产可用性最低门槛）

| # | 功能 | 影响 |
|---|------|------|
| 1 | **CASE WHEN 表达式** | 条件查询基础 |
| 2 | **CAST 类型转换** | 类型互操作 |
| 3 | **字符串函数库**（LENGTH/UPPER/LOWER/SUBSTRING/TRIM/CONCAT） | SQL 完备性 |
| 4 | **数学函数**（ABS/ROUND/CEIL/FLOOR） | 数值处理 |
| 5 | **日期函数**（NOW/EXTRACT/CURRENT_TIMESTAMP） | 时间处理 |
| 6 | **数值类型**（DECIMAL/FLOAT/DOUBLE） | 精确数值与浮点 |
| 7 | **TIMESTAMP 类型** | 时间戳支持 |
| 8 | **TEXT 类型** | 无限长字符串 |
| 9 | **COUNT(DISTINCT col)** | 聚合完备性 |
| 10 | **INSERT 多行 / INSERT SELECT** | 批量数据迁移 |
| 11 | **ORDER BY 多列 / GROUP BY 多列** | 多维分组排序 |
| 12 | **FULL OUTER JOIN / CROSS JOIN** | JOIN 完备性 |
| 13 | **CTE (WITH 子句)** | 现代 SQL 必备 |
| 14 | **派生表（FROM 中的子查询）** | 嵌套查询 |
| 15 | **复合主键 / 复合 UNIQUE** | 业务建模需求 |
| 16 | **复合索引（多列索引）** | 查询性能基础 |
| 17 | **覆盖索引 / Index Only Scan** | 避免回表 |
| 18 | **行级锁** | 并发性能关键 |
| 19 | **SAVEPOINT** | 事务完整性 |
| 20 | **NULL 处理（IS NULL/IS NOT NULL）** | 三值逻辑 |
| 21 | **information_schema 系统表** | 元数据查询标准 |
| 22 | **SHOW TABLES / SHOW COLUMNS / DESC** | 元数据访问 |
| 23 | **密码哈希存储** | 安全底线 |
| 24 | **SSL/TLS 加密连接** | 安全底线 |

### P1 — 重要功能（提升生产能力）

| # | 功能 | 影响 |
|---|------|------|
| 1 | INTERSECT / EXCEPT | 集合操作完备 |
| 2 | WITH RECURSIVE | 树形/图遍历 |
| 3 | 标量子查询（SELECT 中） | 子查询完备 |
| 4 | COALESCE / NULLIF | NULL 处理 |
| 5 | TO_CHAR / TO_DATE | 类型格式化 |
| 6 | REPLACE / POSITION / INSTR | 字符串处理 |
| 7 | POWER / SQRT / MOD | 数学完备 |
| 8 | DATE_ADD / DATE_SUB / INTERVAL | 日期算术 |
| 9 | DENSE_RANK / PARTITION BY | 窗口函数完备 |
| 10 | INSERT ON CONFLICT (UPSERT) | 冲突处理 |
| 11 | UPDATE FROM / DELETE USING | 多表 DML |
| 12 | RETURNING 子句 | DML 返回数据 |
| 13 | SMALLINT / BIGINT | 数值类型完备 |
| 14 | TIME / DATETIME / TIMESTAMPTZ | 时间类型完备 |
| 15 | BOOLEAN | 布尔类型 |
| 16 | BINARY / VARBINARY / BLOB | 二进制存储 |
| 17 | JSON / JSONB | 半结构化数据 |
| 18 | Hash 索引 | 等值查询加速 |
| 19 | 索引列排序 (ASC/DESC) | 排序优化 |
| 20 | 唯一索引（非 PK） | 唯一性约束 |
| 21 | 真正的 SERIALIZABLE | 隔离级别完备 |
| 22 | 间隙锁 / Next-Key 锁 | 防止幻读 |
| 23 | SELECT FOR UPDATE / SHARE | 显式行锁 |
| 24 | 表分区（Range/List/Hash） | 大数据分区 |
| 25 | 触发器（BEFORE/AFTER） | 业务规则自动化 |
| 26 | DROP USER / ALTER USER | 用户管理完备 |
| 27 | 数据库级 / Schema 级权限 | 权限粒度 |
| 28 | 字符集（UTF-8） | 国际化基础 |
| 29 | 慢查询日志 | 性能诊断 |
| 30 | EXPLAIN ANALYZE | 实际执行成本 |
| 31 | 自动统计收集 | 优化器质量 |
| 32 | 直方图统计 | 选择率估算 |
| 33 | JOIN 顺序优化 | 多表 JOIN 性能 |
| 34 | 谓词下推 / 投影下推 | 优化器质量 |
| 35 | 自动 Checkpoint | 减少恢复时间 |
| 36 | 逻辑备份 (dump/restore) | 备份基础 |

### P2 — 增强功能（提升用户体验）

| # | 功能 |
|---|------|
| 1 | NATURAL JOIN |
| 2 | 多 CTE 串联 |
| 3 | 关联子查询 |
| 4 | GREATEST / LEAST / IIF |
| 5 | LPAD / RPAD / REVERSE / REGEXP |
| 6 | LOG / EXP / RANDOM |
| 7 | DATE_TRUNC / DATEDIFF / DATE_FORMAT |
| 8 | STRING_AGG / GROUP_CONCAT |
| 9 | NTILE / FIRST_VALUE / LAST_VALUE |
| 10 | 窗口帧 (ROWS BETWEEN) |
| 11 | MERGE INTO |
| 12 | NULLS FIRST/LAST |
| 13 | 无符号数值类型 |
| 14 | NCHAR / NVARCHAR |
| 15 | CLOB / 数组类型 / ENUM / UUID |
| 16 | JSON 操作符 |
| 17 | 部分索引 / 表达式索引 |
| 18 | INCLUDE 列 / 聚集索引 |
| 19 | 意向锁 / 元数据锁 |
| 20 | NOWAIT / SKIP LOCKED |
| 21 | 只读事务 |
| 22 | 物化视图 |
| 23 | 行级 / 语句级触发器 |
| 24 | 存储过程 / UDF / CALL |
| 25 | 表值函数 |
| 26 | 列级权限 |
| 27 | 密码强度策略 |
| 28 | MD5/SHA256 认证 |
| 29 | 审计日志 |
| 30 | 物理备份 / WAL 归档 |
| 31 | 多列统计 / MCV |
| 32 | 查询计划缓存 |
| 33 | 子查询展开 / 查询重写 |
| 34 | EXPLAIN FORMAT JSON |
| 35 | 锁等待 / 死锁日志 / 活动会话 |
| 36 | 缓冲池命中率 |
| 37 | 自动 VACUUM / 自动统计收集 |
| 38 | 配置文件 / 运行时参数 |
| 39 | statement_timeout / 空闲事务超时 |
| 40 | 排序规则 (Collation) / 时区 |
| 41 | 分区裁剪 |
| 42 | TOAST / LOB 溢出存储 |
| 43 | 可更新视图 |
| 44 | 全文索引 |

### P3 — 高级功能（生态完备性）

包括：复制与高可用、PITR、列式存储、表空间、分布式事务、扩展系统、外部表、空间扩展、时序扩展、图扩展、并行查询、向量化执行、自适应优化、Plan Hints、过程语言扩展、自定义类型/聚合/运算符、Hook 系统、LDAP/Kerberos 认证、行级安全策略 (RLS)、列级加密、TDE、跨数据库恢复、半同步复制、自动故障转移、读写分离等。

---

## 总结

| 优先级 | 总数 | 完成度 |
|--------|------|--------|
| **已实现** | ~80 | 100% |
| **P0 (关键)** | 24 | 0% |
| **P1 (重要)** | 36 | 0% |
| **P2 (增强)** | 44 | 0% |
| **P3 (高级)** | 60+ | 0% |

**当前定位**：已完成 SQL-92 大部分基础功能 + MVCC + B+ 树索引 + 基本查询优化器，对标 SQLite 早期版本水平。

**下一阶段目标**：完成 P0 全部 24 项，达到 SQL:1999 基础符合性，接近 SQLite 当前水平。

**长期目标**：完成 P0 + P1，对标 PostgreSQL 7.x 或 MySQL 5.x 早期版本。

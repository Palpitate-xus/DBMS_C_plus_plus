# DBMS SQL 命令参考手册

本文档列出本 DBMS 支持的全部 SQL 命令，按功能分类组织。每条命令包含语法说明、参数类型、返回值/输出格式及可运行示例。

---

## 目录

- [1. DDL - 数据定义](#1-ddl---数据定义)
- [2. DML - 数据操纵](#2-dml---数据操纵)
- [3. DQL - 数据查询](#3-dql---数据查询)
- [4. TCL - 事务控制](#4-tcl---事务控制)
- [5. DCL - 权限控制](#5-dcl---权限控制)
- [6. 存储程序](#6-存储程序)
- [7. 工具与诊断](#7-工具与诊断)
- [8. 数据类型](#8-数据类型)
- [9. 函数](#9-函数)

---

## 1. DDL - 数据定义

### CREATE DATABASE

**语法**
```sql
CREATE DATABASE database_name [CHARACTER SET charset_name]
```

**参数**
| 参数 | 类型 | 说明 |
|------|------|------|
| `database_name` | `STRING` | 数据库名称，唯一标识 |
| `charset_name` | `STRING` | 可选，字符集，默认 `utf8` |

**返回值** 文本提示创建结果。

**示例**
```sql
CREATE DATABASE shopdb;
CREATE DATABASE shopdb CHARACTER SET utf8;
```

---

### DROP DATABASE

**语法**
```sql
DROP DATABASE database_name
```

**参数**
| 参数 | 类型 | 说明 |
|------|------|------|
| `database_name` | `STRING` | 要删除的数据库名称 |

**示例**
```sql
DROP DATABASE shopdb;
```

---

### USE DATABASE

**语法**
```sql
USE DATABASE database_name
```

**参数**
| 参数 | 类型 | 说明 |
|------|------|------|
| `database_name` | `STRING` | 切换到的目标数据库 |

**返回值** `set Database to <database_name>`

**示例**
```sql
USE DATABASE shopdb;
```

---

### CREATE TABLE

**语法**
```sql
CREATE TABLE table_name (
    column_name data_type [column_flags] [, ...]
    [, PRIMARY KEY (column_name [, ...])]
    [, FOREIGN KEY (column_name) REFERENCES ref_table(ref_column)
       [ON DELETE {CASCADE | RESTRICT | SET NULL | SET DEFAULT | NO ACTION}]
       [ON UPDATE {CASCADE | RESTRICT | SET NULL | SET DEFAULT | NO ACTION}]]
) [PARTITION BY RANGE(column_name) (
    PARTITION partition_name VALUES LESS THAN (value),
    ...
)]
```

**参数**
| 参数 | 类型 | 说明 |
|------|------|------|
| `table_name` | `STRING` | 表名，当前数据库内唯一 |
| `column_name` | `STRING` | 列名 |
| `data_type` | `STRING` | 列数据类型，见 [数据类型](#8-数据类型) |
| `column_flags` | `FLAG...` | 可选，空格分隔：`NOT NULL`、`PRIMARY KEY`、`UNIQUE`、`AUTO_INCREMENT`、`DEFAULT 'value'` |

**返回值** 文本提示创建结果。

**示例**
```sql
CREATE TABLE users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    age INT DEFAULT 0,
    created_at TIMESTAMP
);

CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    user_id INT NOT NULL,
    amount DOUBLE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE events (
    id INT PRIMARY KEY,
    event_time TIMESTAMP
) PARTITION BY RANGE(event_time) (
    PARTITION p1 VALUES LESS THAN ('2024-01-01'),
    PARTITION p2 VALUES LESS THAN ('2025-01-01')
);
```

---

### CREATE TEMPORARY TABLE

**语法**
```sql
CREATE TEMPORARY TABLE table_name (
    column_name data_type [column_flags] [, ...]
)
```

**参数** 同 `CREATE TABLE`。

**说明** 临时表仅在当前会话可见，会话结束后自动删除。

**示例**
```sql
CREATE TEMPORARY TABLE temp_results (
    id INT,
    score DOUBLE
);
```

---

### DROP TABLE

**语法**
```sql
DROP TABLE table_name
```

**参数**
| 参数 | 类型 | 说明 |
|------|------|------|
| `table_name` | `STRING` | 要删除的表名 |

**示例**
```sql
DROP TABLE temp_results;
```

---

### ALTER TABLE ADD COLUMN

**语法**
```sql
ALTER TABLE table_name ADD COLUMN column_name data_type [column_flags]
```

**参数** 列定义同 `CREATE TABLE`。

**示例**
```sql
ALTER TABLE users ADD COLUMN phone VARCHAR(20);
ALTER TABLE users ADD COLUMN status INT NOT NULL DEFAULT 1;
```

---

### ALTER TABLE DROP COLUMN

**语法**
```sql
ALTER TABLE table_name DROP COLUMN column_name
```

**示例**
```sql
ALTER TABLE users DROP COLUMN phone;
```

---

### ALTER TABLE ALTER COLUMN TYPE

**语法**
```sql
ALTER TABLE table_name ALTER COLUMN column_name TYPE new_data_type
```

**示例**
```sql
ALTER TABLE users ALTER COLUMN age TYPE BIGINT;
```

---

### CREATE VIEW

**语法**
```sql
CREATE VIEW view_name AS SELECT select_clause
```

**参数**
| 参数 | 类型 | 说明 |
|------|------|------|
| `view_name` | `STRING` | 视图名称 |
| `select_clause` | `QUERY` | 任意 SELECT 查询 |

**示例**
```sql
CREATE VIEW active_users AS
SELECT id, name, email FROM users WHERE status = 1;
```

---

### CREATE MATERIALIZED VIEW

**语法**
```sql
CREATE MATERIALIZED VIEW view_name AS SELECT select_clause
```

**说明** 物化视图将查询结果持久化存储，需手动 `REFRESH` 更新。

**示例**
```sql
CREATE MATERIALIZED VIEW daily_sales AS
SELECT DATE(created_at) AS sale_date, SUM(amount) AS total
FROM orders GROUP BY DATE(created_at);
```

---

### DROP VIEW

**语法**
```sql
DROP VIEW view_name
```

**示例**
```sql
DROP VIEW active_users;
```

---

### CREATE INDEX

**语法**
```sql
CREATE [UNIQUE | FULLTEXT] INDEX index_name ON table_name (column_name) [USING HASH]
    [INCLUDE (column_name [, ...])]
    [WHERE condition]
```

**参数**
| 参数 | 类型 | 说明 |
|------|------|------|
| `index_name` | `STRING` | 索引名称 |
| `table_name` | `STRING` | 表名 |
| `column_name` | `STRING` | 索引列名 |
| `INCLUDE` | `LIST` | 可选，覆盖索引包含的附加列 |
| `WHERE` | `EXPR` | 可选，部分索引过滤条件 |

**示例**
```sql
CREATE INDEX idx_users_name ON users (name);
CREATE UNIQUE INDEX idx_users_email ON users (email);
CREATE INDEX idx_users_id ON users (id) USING HASH;
CREATE INDEX idx_orders_user ON orders (user_id) INCLUDE (amount);
CREATE INDEX idx_orders_amount ON orders (amount) WHERE amount > 100;
```

---

### DROP INDEX

**语法**
```sql
DROP INDEX index_name ON table_name
```

**示例**
```sql
DROP INDEX idx_users_name ON users;
```

---

### CREATE TRIGGER

**语法**
```sql
CREATE TRIGGER trigger_name {BEFORE | AFTER} {INSERT | UPDATE | DELETE} ON table_name action_string
```

**参数**
| 参数 | 类型 | 说明 |
|------|------|------|
| `trigger_name` | `STRING` | 触发器名称 |
| `timing` | `ENUM` | `BEFORE` / `AFTER` |
| `event` | `ENUM` | `INSERT` / `UPDATE` / `DELETE` |
| `table_name` | `STRING` | 目标表 |
| `action_string` | `STRING` | 触发时执行的操作描述 |

**示例**
```sql
CREATE TRIGGER trg_orders_audit AFTER INSERT ON orders audit_log;
```

---

### CREATE ROLE

**语法**
```sql
CREATE ROLE role_name
```

**示例**
```sql
CREATE ROLE readonly;
```

---

### CREATE USER

**语法**
```sql
CREATE USER username password permission_level
```

**参数**
| 参数 | 类型 | 说明 |
|------|------|------|
| `username` | `STRING` | 用户名 |
| `password` | `STRING` | 密码（明文，按配置哈希存储） |
| `permission_level` | `INT` | 权限等级：0=普通用户，1=管理员 |

**示例**
```sql
CREATE USER alice secret123 0;
CREATE USER admin admin 1;
```

---

## 2. DML - 数据操纵

### INSERT INTO

**语法**
```sql
INSERT INTO table_name (column1, column2, ...) VALUES (value1, value2, ...)
INSERT INTO table_name (column1, column2, ...) SELECT select_clause
```

**参数**
| 参数 | 类型 | 说明 |
|------|------|------|
| `table_name` | `STRING` | 目标表名 |
| `columnN` | `STRING` | 列名列表，必须显式指定 |
| `valueN` | `LITERAL` | 插入值，字符串用单引号包裹 |

**示例**
```sql
INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 25);
INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'bob@example.com', 30);
INSERT INTO temp_results (id, score) SELECT id, score FROM exams WHERE passed = 1;
```

---

### REPLACE INTO

**语法**
```sql
REPLACE INTO table_name (column1, column2, ...) VALUES (value1, value2, ...)
```

**说明** 如果存在主键冲突，先删除旧行再插入新行。

**示例**
```sql
REPLACE INTO users (id, name, email) VALUES (1, 'Alice Smith', 'alice@new.com');
```

---

### UPSERT（INSERT ... ON CONFLICT）

**语法**
```sql
INSERT INTO table_name (column1, column2, ...) VALUES (value1, value2, ...)
ON CONFLICT (conflict_column) DO UPDATE SET column = value [, ...]
```

**说明** 插入时若主键冲突，则执行 UPDATE 操作。本质是 `INSERT ... ON CONFLICT` 语法，不是独立的 `UPSERT` 命令。

**示例**
```sql
INSERT INTO users (id, name, age) VALUES (1, 'Alice', 26)
ON CONFLICT (id) DO UPDATE SET name = 'Alice', age = 26;
```

---

### UPDATE

**语法**
```sql
UPDATE table_name SET column1 = value1 [, column2 = value2, ...] [WHERE condition] [LIMIT n]
UPDATE target_table SET ... FROM source_table WHERE join_condition [LIMIT n]
```

**参数**
| 参数 | 类型 | 说明 |
|------|------|------|
| `table_name` | `STRING` | 要更新的表 |
| `column = value` | `ASSIGN` | 列赋值，值可为常量或表达式 |
| `condition` | `EXPR` | 可选，WHERE 过滤条件 |
| `LIMIT n` | `INT` | 可选，仅更新前 n 行（要求表有主键） |

**示例**
```sql
UPDATE users SET age = 26 WHERE id = 1;
UPDATE users SET status = 0 WHERE age > 60;
UPDATE orders SET amount = amount * 1.1 FROM users WHERE orders.user_id = users.id AND users.vip = 1;
UPDATE users SET age = age + 1 LIMIT 10;
```

---

### DELETE

**语法**
```sql
DELETE FROM table_name [WHERE condition] [LIMIT n]
DELETE FROM target_table USING source_table WHERE join_condition [LIMIT n]
```

**参数** 同 UPDATE。

**示例**
```sql
DELETE FROM users WHERE id = 1;
DELETE FROM orders WHERE amount < 10;
DELETE FROM logs USING users WHERE logs.user_id = users.id AND users.status = 0;
DELETE FROM events LIMIT 100;
```

---

### MERGE INTO

**语法**
```sql
MERGE INTO target_table USING source_table ON condition
UPDATE SET column = value [, ...]
INSERT (column1, ...) VALUES (value1, ...)
```

**说明** 将源表数据合并到目标表：匹配则 UPDATE，不匹配则 INSERT。

**示例**
```sql
MERGE INTO users USING temp_users ON users.id = temp_users.id
UPDATE SET name = temp_users.name, email = temp_users.email
INSERT (id, name, email) VALUES (temp_users.id, temp_users.name, temp_users.email);
```

---

### LOAD DATA INFILE

**语法**
```sql
LOAD DATA INFILE 'file_path' INTO TABLE table_name
```

**参数**
| 参数 | 类型 | 说明 |
|------|------|------|
| `file_path` | `STRING` | CSV 文件路径 |
| `table_name` | `STRING` | 目标表名 |

**说明** 从 CSV 文件批量导入数据，第一行为列名标题。

**示例**
```sql
LOAD DATA INFILE '/tmp/users.csv' INTO TABLE users;
```

---

## 3. DQL - 数据查询

### SELECT

**语法**
```sql
SELECT
    [DISTINCT]
    {* | column_name [AS alias] | expression | aggregate_function(...) | window_function(...) OVER (...)} [, ...]
FROM table_name [AS alias]
[WHERE condition]
[GROUP BY column_name [, ...]]
[HAVING condition]
[ORDER BY column_name [ASC | DESC] [, ...]]
[LIMIT n [OFFSET m]]
[FOR UPDATE | FOR SHARE [NOWAIT | SKIP LOCKED]]
```

**参数**
| 参数 | 类型 | 说明 |
|------|------|------|
| `DISTINCT` | `FLAG` | 去重 |
| `column_name` | `STRING` | 列名或表达式 |
| `table_name` | `STRING` | 表名，可为子查询 `(SELECT ...) AS alias` |
| `condition` | `EXPR` | 条件表达式，支持 `=`, `<>`, `!=`, `<`, `>`, `<=`, `>=`, `LIKE`, `BETWEEN`, `IN`, `IS NULL`, `IS NOT NULL` |
| `aggregate_function` | `FUNC` | `SUM`, `COUNT`, `AVG`, `MAX`, `MIN` |
| `window_function` | `FUNC` | 见 [窗口函数](#窗口函数) |
| `LIMIT n` | `INT` | 返回最多 n 行 |
| `OFFSET m` | `INT` | 跳过前 m 行 |
| `FOR UPDATE` | `FLAG` | 对返回行加排他锁 |
| `FOR SHARE` | `FLAG` | 对返回行加共享锁 |

**示例**
```sql
-- 基础查询
SELECT id, name, age FROM users WHERE age > 20 ORDER BY age DESC;

-- 去重
SELECT DISTINCT city FROM users;

-- 聚合 + 分组 + HAVING
SELECT status, COUNT(*) AS cnt, AVG(age) AS avg_age
FROM users
GROUP BY status
HAVING COUNT(*) > 5;

-- LIMIT + OFFSET（分页）
SELECT id, name FROM users ORDER BY id LIMIT 10 OFFSET 20;

-- 窗口函数
SELECT id, name, age,
       RANK() OVER (ORDER BY age DESC) AS age_rank,
       ROW_NUMBER() OVER (PARTITION BY status ORDER BY age) AS row_num
FROM users;

-- FOR UPDATE（悲观锁）
SELECT * FROM orders WHERE status = 0 FOR UPDATE;
```

---

### JOIN

**语法**
```sql
SELECT ... FROM table1
[INNER] JOIN table2 ON condition
| LEFT JOIN table2 ON condition
| RIGHT JOIN table2 ON condition
| CROSS JOIN table2
```

**说明** 支持 Nested Loop Join、Hash Join、Merge Join，优化器根据统计信息自动选择。

**示例**
```sql
SELECT users.name, orders.amount
FROM users
JOIN orders ON users.id = orders.user_id;

SELECT users.name, orders.order_id
FROM users
LEFT JOIN orders ON users.id = orders.user_id;

SELECT a.name, b.name AS referrer
FROM users a
LEFT JOIN users b ON a.referral_id = b.id;
```

---

### UNION

**语法**
```sql
SELECT ... UNION [ALL] SELECT ...
```

**说明** `UNION` 去重，`UNION ALL` 保留全部行。

**示例**
```sql
SELECT name, email FROM users WHERE status = 1
UNION
SELECT name, email FROM archived_users WHERE status = 1;
```

---

### SELECT INTO OUTFILE

**语法**
```sql
SELECT ... INTO OUTFILE 'file_path'
```

**说明** 将查询结果导出为 CSV 文件。

**示例**
```sql
SELECT id, name, email FROM users INTO OUTFILE '/tmp/users_export.csv';
```

---

### WITH (CTE)

**语法**
```sql
WITH cte_name AS (SELECT ...)
[, cte_name2 AS (SELECT ...)]
SELECT ... FROM cte_name ...
```

**示例**
```sql
WITH vip_users AS (
    SELECT id, name FROM users WHERE vip = 1
)
SELECT vip_users.name, orders.amount
FROM vip_users
JOIN orders ON vip_users.id = orders.user_id;
```

---

## 4. TCL - 事务控制

### BEGIN

**语法**
```sql
BEGIN [TRANSACTION] [ISOLATION LEVEL {READ UNCOMMITTED | READ COMMITTED | REPEATABLE READ | SERIALIZABLE}]
```

**参数**
| 参数 | 类型 | 说明 |
|------|------|------|
| `ISOLATION LEVEL` | `ENUM` | 事务隔离级别，默认 `READ COMMITTED` |

**说明** 开启一个新事务，分配唯一的事务 ID，注册到活跃事务集合。基于 MVCC + ReadView 实现快照读。

**示例**
```sql
BEGIN;
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
```

---

### COMMIT

**语法**
```sql
COMMIT
```

**说明** 提交当前事务，所有修改对后续事务可见。

**示例**
```sql
BEGIN;
INSERT INTO users (id, name) VALUES (10, 'Tom');
COMMIT;
```

---

### ROLLBACK

**语法**
```sql
ROLLBACK
ROLLBACK TO SAVEPOINT savepoint_name
```

**说明** 回滚当前事务或回滚到指定保存点。

**示例**
```sql
BEGIN;
INSERT INTO users (id, name) VALUES (11, 'Jerry');
ROLLBACK;  -- Jerry 不会插入

BEGIN;
SAVEPOINT sp1;
DELETE FROM users WHERE id = 1;
ROLLBACK TO SAVEPOINT sp1;  -- 撤销 DELETE
COMMIT;
```

---

### SAVEPOINT

**语法**
```sql
SAVEPOINT savepoint_name
```

**示例**
```sql
BEGIN;
SAVEPOINT before_update;
UPDATE users SET age = 99 WHERE id = 1;
ROLLBACK TO SAVEPOINT before_update;
COMMIT;
```

---

### RELEASE SAVEPOINT

**语法**
```sql
RELEASE SAVEPOINT savepoint_name
```

**说明** 释放保存点，释放后不能再回滚到该点。

**示例**
```sql
BEGIN;
SAVEPOINT sp1;
INSERT INTO users (id, name) VALUES (12, 'Kate');
RELEASE SAVEPOINT sp1;
COMMIT;
```

---

### CHECKPOINT

**语法**
```sql
CHECKPOINT
```

**说明** 强制将所有脏页刷盘，截断 WAL 日志，加速崩溃恢复。

**示例**
```sql
CHECKPOINT;
```

---

### VACUUM

**语法**
```sql
VACUUM [table_name]
```

**说明** 回收已删除行占用的页空间。若指定表名则只清理该表。

**示例**
```sql
VACUUM;
VACUUM users;
```

---

### SET TRANSACTION ISOLATION LEVEL

**语法**
```sql
SET TRANSACTION ISOLATION LEVEL {READ UNCOMMITTED | READ COMMITTED | REPEATABLE READ | SERIALIZABLE}
```

**示例**
```sql
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
```

---

## 5. DCL - 权限控制

### GRANT

**语法**
```sql
GRANT privilege [(column1, column2, ...)] ON table_name TO user_name
GRANT role_name TO user_name
```

**参数**
| 参数 | 类型 | 说明 |
|------|------|------|
| `privilege` | `ENUM` | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `ALL` |
| `columnN` | `STRING` | 可选，列级权限限制 |
| `table_name` | `STRING` | 表名 |
| `user_name` | `STRING` | 目标用户或角色 |

**示例**
```sql
GRANT SELECT ON users TO readonly_user;
GRANT SELECT, INSERT ON orders TO alice;
GRANT SELECT (name, email) ON users TO reporter;
GRANT readonly TO alice;
```

---

### REVOKE

**语法**
```sql
REVOKE privilege [(column1, column2, ...)] ON table_name FROM user_name
REVOKE role_name FROM user_name
```

**示例**
```sql
REVOKE INSERT ON orders FROM alice;
REVOKE ALL ON users FROM readonly_user;
REVOKE readonly FROM alice;
```

---

## 6. 存储程序

### CREATE PROCEDURE

**语法**
```sql
CREATE PROCEDURE procedure_name AS sql_statement1; sql_statement2; ...
```

**说明** 将多条 SQL 语句封装为存储过程，通过 `CALL` 调用。

**示例**
```sql
CREATE PROCEDURE reset_user_status AS
UPDATE users SET status = 0 WHERE status IS NULL;
```

---

### CREATE FUNCTION

**语法**
```sql
CREATE FUNCTION function_name(parameter_name) RETURNS return_type AS expression
```

**参数**
| 参数 | 类型 | 说明 |
|------|------|------|
| `parameter_name` | `STRING` | 函数参数名 |
| `return_type` | `STRING` | 返回类型 |
| `expression` | `EXPR` | 函数体表达式 |

**示例**
```sql
CREATE FUNCTION double_value(x) RETURNS INT AS x * 2;
```

---

### CALL

**语法**
```sql
CALL procedure_name()
```

**示例**
```sql
CALL reset_user_status();
```

---

## 7. 工具与诊断

### SHOW CONNECTIONS

**说明** 显示当前活跃连接数、总连接数、最大连接数、拒绝连接数。

**输出格式**
```
active_connections: N
total_connections: N
max_connections: N
rejected_connections: N
```

**示例**
```sql
SHOW CONNECTIONS;
```

---

### SHOW PROCESSLIST

**说明** 显示当前所有连接的进程信息（类似 MySQL `SHOW PROCESSLIST`）。

**输出格式**
```
id user host db command time state info
```

**示例**
```sql
SHOW PROCESSLIST;
```

---

### SHOW STATEMENTS

**说明** 显示当前数据库的 SQL 统计信息（调用次数、总耗时、最小/最大/平均耗时）。

**输出格式**
```
query calls total_time min_time max_time mean_time dbname
```

**示例**
```sql
SHOW STATEMENTS;
```

---

### SHOW STATUS

**说明** 显示服务器综合状态：连接数、BufferPool 命中率、查询计划缓存命中率。

**输出格式**
```
active_connections N
total_connections N
max_connections N
rejected_connections N
buffer_pool_hits N
buffer_pool_misses N
buffer_pool_hit_rate X%
plan_cache_size N
plan_cache_hits N
plan_cache_misses N
plan_cache_hit_rate X%
```

**示例**
```sql
SHOW STATUS;
```

---

### SHOW VARIABLES

**说明** 显示当前运行时的配置变量值。

**示例**
```sql
SHOW VARIABLES;
```

---

### SHOW TABLES

**说明** 列出当前数据库的所有表。

**示例**
```sql
SHOW TABLES;
```

---

### SHOW VIEWS

**说明** 列出当前数据库的所有视图，可更新视图会标注基表。

**示例**
```sql
SHOW VIEWS;
```

---

### SHOW MATERIALIZED VIEWS

**说明** 列出当前数据库的所有物化视图。

**示例**
```sql
SHOW MATERIALIZED VIEWS;
```

---

### SHOW COLUMNS

**语法**
```sql
SHOW COLUMNS FROM table_name
SHOW COLUMNS IN table_name
```

**输出格式**
```
field type null key default extra
```

**示例**
```sql
SHOW COLUMNS FROM users;
```

---

### SHOW INDEXES

**说明** 列出当前数据库所有表的索引信息。

**输出格式**
```
table column type include where
```

**示例**
```sql
SHOW INDEXES;
```

---

### SHOW DATABASES

**说明** 列出所有数据库。

**示例**
```sql
SHOW DATABASES;
```

---

### SHOW GRANTS

**语法**
```sql
SHOW GRANTS [FOR user_name]
```

**说明** 显示当前用户或指定用户的权限列表。

**示例**
```sql
SHOW GRANTS;
SHOW GRANTS FOR alice;
```

---

### SHOW TRIGGERS

**说明** 列出当前数据库的所有触发器。

**输出格式**
```
name timing event table action
```

**示例**
```sql
SHOW TRIGGERS;
```

---

### SHOW PROCEDURES

**说明** 列出当前数据库的所有存储过程及其语句数。

**示例**
```sql
SHOW PROCEDURES;
```

---

### SHOW FUNCTIONS

**说明** 列出当前数据库的所有用户定义函数（UDF）和表值函数（TVF）。

**示例**
```sql
SHOW FUNCTIONS;
```

---

### SHOW SLOW LOG

**说明** 显示超过慢查询阈值的查询记录。

**输出格式**
```
timestamp user db ms sql
```

**示例**
```sql
SHOW SLOW LOG;
```

---

### SHOW STATS

**语法**
```sql
SHOW STATS [FOR table_name]
```

**说明** 显示表及列的统计信息：行数、列基数、最小/最大值、最常出现值（MCV）。

**示例**
```sql
SHOW STATS FOR users;
```

---

### SHOW CREATE TABLE

**语法**
```sql
SHOW CREATE TABLE table_name
```

**说明** 显示重建该表的 `CREATE TABLE` DDL 语句。

**示例**
```sql
SHOW CREATE TABLE users;
```

---

### SHOW DEAD TUPLES

**语法**
```sql
SHOW DEAD TUPLES FROM table_name
```

**说明** 显示表中已删除但未清理的行数。

**示例**
```sql
SHOW DEAD TUPLES FROM users;
```

---

### SHOW DEADLOCKS

**说明** 显示历史死锁记录。

**示例**
```sql
SHOW DEADLOCKS;
```

---

### SHOW LOCKS

**说明** 显示当前锁持有和等待情况。

**输出格式**
```
resource holder mode
--- waits ---
waiter holders
```

**示例**
```sql
SHOW LOCKS;
```

---

### SHOW PLAN CACHE

**说明** 显示查询计划缓存中的 SQL 列表。

**示例**
```sql
SHOW PLAN CACHE;
```

---

### SHOW USERS

**语法**
```sql
SHOW USERS;
```

**说明** 显示所有用户及其权限级别，从 `user.dat` 读取。需要 admin 权限。

**示例**
```sql
SHOW USERS;
-- 输出: username permission
-- admin 1
-- user1 0
```

---

### SHOW ROLES

**语法**
```sql
SHOW ROLES;
```

**说明** 显示所有角色名称，从 `role.dat` 读取（仅显示 `__ROLE__` 标记的条目）。需要 admin 权限。

**示例**
```sql
CREATE ROLE auditor;
SHOW ROLES;
-- 输出: role_name
-- auditor
```

---

### DESC / DESCRIBE

**语法**
```sql
DESC table_name
DESCRIBE table_name
```

**说明** 显示表结构，同 `SHOW COLUMNS`。

**示例**
```sql
DESC users;
DESCRIBE orders;
```

---

### VIEW TABLE

**语法**
```sql
VIEW TABLE table_name
```

**说明** 打印表的完整结构定义（列信息、主键、外键、索引等）。

**示例**
```sql
VIEW TABLE users;
```

---

### VIEW DATABASE

**说明** 列出当前数据库的所有表名。

**示例**
```sql
VIEW DATABASE;
```

---

### ANALYZE TABLE

**语法**
```sql
ANALYZE TABLE table_name
ANALYZE TABLE table_name COLUMNS (column1, column2, ...)
```

**说明** 收集表和列的统计信息，供查询优化器使用。

**示例**
```sql
ANALYZE TABLE users;
ANALYZE TABLE users COLUMNS (age, status);
```

---

### DUMP DATABASE

**语法**
```sql
DUMP DATABASE database_name TO 'file_path'
```

**说明** 将数据库导出为 SQL 脚本文件。

**示例**
```sql
DUMP DATABASE shopdb TO '/tmp/shopdb_backup.sql';
```

---

### RESTORE DATABASE

**语法**
```sql
RESTORE DATABASE database_name FROM 'file_path'
```

**说明** 从 SQL 脚本文件恢复数据库。

**示例**
```sql
RESTORE DATABASE shopdb FROM '/tmp/shopdb_backup.sql';
```

---

### BACKUP DATABASE

**语法**
```sql
BACKUP DATABASE database_name TO 'file_path'
```

**说明** 将数据库文件直接复制到指定路径。

**示例**
```sql
BACKUP DATABASE shopdb TO '/backup/shopdb.bak';
```

---

### CLEAR PLAN CACHE

**说明** 清空查询计划缓存。

**示例**
```sql
CLEAR PLAN CACHE;
```

---

### SET (变量)

**语法**
```sql
SET variable_name = value
```

**支持的变量**
| 变量名 | 类型 | 说明 |
|--------|------|------|
| `slow_query_threshold` | `DOUBLE` | 慢查询阈值（毫秒） |
| `checkpoint_interval` | `INT` | 自动 checkpoint 间隔（SQL 条数） |
| `statement_timeout` | `INT` | 语句超时（毫秒） |
| `password_policy_level` | `INT` | 密码强度策略（0-3） |
| `password_hash_algorithm` | `STRING` | 密码哈希算法 |
| `audit_level` | `INT` | 审计日志级别（0-3） |

**示例**
```sql
SET slow_query_threshold = 100.0;
SET checkpoint_interval = 100;
SET statement_timeout = 5000;
SET audit_level = 2;
```

---

### SET TIMEZONE

**语法**
```sql
SET TIMEZONE = 'timezone_string'
SET TIME ZONE 'timezone_string'
```

**示例**
```sql
SET TIMEZONE = '+08:00';
```

---

### SET AUTO_VACUUM

**语法**
```sql
SET AUTO_VACUUM = value
```

**说明** 设置自动 VACUUM 阈值（死行数达到该值时触发）。

**示例**
```sql
SET AUTO_VACUUM = 1000;
```

---

### PREPARE

**语法**
```sql
PREPARE statement_name FROM 'sql_template'
```

**参数**
| 参数 | 类型 | 说明 |
|------|------|------|
| `statement_name` | `STRING` | 预编译语句名称 |
| `sql_template` | `STRING` | SQL 模板，用 `?` 作为占位符 |

**示例**
```sql
PREPARE stmt1 FROM 'SELECT * FROM users WHERE age > ?';
```

---

### EXECUTE

**语法**
```sql
EXECUTE statement_name [USING (value1, value2, ...)]
```

**参数**
| 参数 | 类型 | 说明 |
|------|------|------|
| `statement_name` | `STRING` | 预编译语句名称 |
| `valueN` | `LITERAL` | 替换占位符的实际值 |

**示例**
```sql
EXECUTE stmt1 USING (25);
```

---

### DEALLOCATE PREPARE

**语法**
```sql
DEALLOCATE PREPARE statement_name
```

**说明** 删除预编译语句，释放资源。

**示例**
```sql
DEALLOCATE PREPARE stmt1;
```

---

### EXPLAIN

**语法**
```sql
EXPLAIN SELECT ...
EXPLAIN ANALYZE SELECT ...
EXPLAIN FORMAT JSON SELECT ...
```

**说明** 显示查询执行计划，包括扫描方式、JOIN 算法、成本估计等。`ANALYZE` 会实际执行并显示真实耗时。

**示例**
```sql
EXPLAIN SELECT * FROM users WHERE age > 20;
EXPLAIN ANALYZE SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id;
```

---

## 8. 数据类型

| 类型 | 说明 | 存储大小 | 示例值 |
|------|------|----------|--------|
| `INT` / `INTEGER` | 有符号整数 | 8 字节 | `42`, `-100` |
| `SMALLINT` | 短整数 | 8 字节 | `10` |
| `BIGINT` | 长整数 | 8 字节 | `9223372036854775807` |
| `FLOAT` | 单精度浮点 | 4 字节 | `3.14` |
| `DOUBLE` | 双精度浮点 | 8 字节 | `3.1415926535` |
| `MONEY` | 货币类型（双精度） | 8 字节 | `99.99` |
| `BOOL` / `BIT` | 布尔值 | 1 字节 | `true`, `false` |
| `DATE` | 日期 | 4 字节 | `'2024-01-15'` |
| `TIME` | 时间 | 4 字节 | `'14:30:00'` |
| `DATETIME` | 日期时间 | 8 字节 | `'2024-01-15 14:30:00'` |
| `TIMESTAMP` | 时间戳 | 8 字节 | `'2024-01-15 14:30:00'` |
| `TIMESTAMPTZ` | 带时区时间戳 | 8 字节 | `'2024-01-15 14:30:00+08'` |
| `CHAR(n)` | 定长字符串 | n 字节 | `'hello'` |
| `VARCHAR(n)` | 变长字符串 | 实际长度 + 开销 | `'hello world'` |
| `NCHAR(n)` | 定长 Unicode 字符串 | n 字节 | `'中文'` |
| `NVARCHAR(n)` | 变长 Unicode 字符串 | 实际长度 + 开销 | `'中文测试'` |
| `BLOB` | 二进制大对象 | 变长 | 存储二进制数据 |
| `TEXT` | 文本大对象 | 变长 | 存储长文本 |
| `JSON` | JSON 文本 | 变长，最大 64KB | `'{"a":1}'` |
| `JSONB` | 二进制 JSON（带验证） | 变长，最大 64KB | `'{"a":1}'` |
| `ARRAY <type>` | 数组 | 变长 | `ARRAY INT` |

**说明**
- 每行前 16 字节为 MVCC 头部（`creatorTxnId` + `rollbackPtr`）。
- `VARCHAR`、`TEXT`、`BLOB`、`JSON`、`JSONB` 为变长类型，行内存储偏移数组 + 实际数据。
- 单行数据超过页可用空间时，大字段自动存放到溢出页。

---

## 9. 函数

### 9.1 标量函数

| 函数 | 参数 | 返回值 | 说明 | 示例 |
|------|------|--------|------|------|
| `LENGTH(s)` | `STRING` | `INT` | 字符串长度 | `LENGTH('hello')` → `5` |
| `CHAR_LENGTH(s)` | `STRING` | `INT` | 字符数 | `CHAR_LENGTH('中文')` → `2` |
| `UPPER(s)` | `STRING` | `STRING` | 转大写 | `UPPER('hello')` → `'HELLO'` |
| `LOWER(s)` | `STRING` | `STRING` | 转小写 | `LOWER('HELLO')` → `'hello'` |
| `TRIM(s)` | `STRING` | `STRING` | 去除首尾空格 | `TRIM('  hello  ')` → `'hello'` |
| `SUBSTRING(s, start, len)` | `STRING, INT, INT` | `STRING` | 子串 | `SUBSTRING('hello', 1, 3)` → `'ell'` |
| `CONCAT(s1, s2, ...)` | `STRING...` | `STRING` | 拼接字符串 | `CONCAT('a', 'b')` → `'ab'` |
| `ABS(n)` | `NUMERIC` | `NUMERIC` | 绝对值 | `ABS(-5)` → `5` |
| `ROUND(n, d)` | `NUMERIC, INT` | `NUMERIC` | 四舍五入 | `ROUND(3.1415, 2)` → `3.14` |
| `CEIL(n)` | `NUMERIC` | `NUMERIC` | 向上取整 | `CEIL(3.2)` → `4` |
| `FLOOR(n)` | `NUMERIC` | `NUMERIC` | 向下取整 | `FLOOR(3.8)` → `3` |
| `NOW()` | 无 | `TIMESTAMP` | 当前时间戳 | `NOW()` |
| `CURRENT_TIMESTAMP` | 无 | `TIMESTAMP` | 同 `NOW()` | `CURRENT_TIMESTAMP` |
| `EXTRACT(part FROM t)` | `ENUM, TIMESTAMP` | `INT` | 提取日期部分 | `EXTRACT(YEAR FROM NOW())` |
| `CAST(expr AS type)` | `ANY, STRING` | `type` | 类型转换 | `CAST('123' AS INT)` |
| `CONVERT(expr, type)` | `ANY, STRING` | `type` | 同 `CAST` | `CONVERT('123', INT)` |
| `TO_NUMBER(s)` | `STRING` | `NUMERIC` | 字符串转数字 | `TO_NUMBER('42')` → `42` |
| `TO_CHAR(n, fmt)` | `NUMERIC, STRING` | `STRING` | 数字转字符串 | `TO_CHAR(42, '999')` |
| `TO_DATE(s, fmt)` | `STRING, STRING` | `DATE` | 字符串转日期 | `TO_DATE('2024-01-15', 'YYYY-MM-DD')` |
| `COALESCE(v1, v2, ...)` | `ANY...` | `ANY` | 返回第一个非 NULL | `COALESCE(NULL, 'a')` → `'a'` |
| `NULLIF(a, b)` | `ANY, ANY` | `ANY` | a=b 时返回 NULL | `NULLIF(1, 1)` → `NULL` |
| `REPLACE(s, from, to)` | `STRING, STRING, STRING` | `STRING` | 替换子串 | `REPLACE('abc', 'b', 'X')` → `'aXc'` |
| `POSITION(sub IN s)` | `STRING, STRING` | `INT` | 子串位置 | `POSITION('b' IN 'abc')` → `2` |
| `INSTR(s, sub)` | `STRING, STRING` | `INT` | 同 `POSITION` | `INSTR('abc', 'b')` → `2` |
| `CASE WHEN(cond1, val1, cond2, val2, ..., elseVal)` | `ANY...` | `ANY` | 条件分支 | `CASE WHEN(age>18, 'adult', 'minor')` |

### 9.2 JSONB 函数

| 函数 | 参数 | 返回值 | 说明 | 示例 |
|------|------|--------|------|------|
| `JSONB_EXTRACT(json, path)` | `JSONB, STRING` | `STRING` | 按路径提取 JSON 片段 | `JSONB_EXTRACT(data, '$.name')` |
| `JSONB_EXTRACT_TEXT(json, path)` | `JSONB, STRING` | `STRING` | 按路径提取标量文本 | `JSONB_EXTRACT_TEXT(data, '$.name')` |
| `JSONB_CONTAINS(json, key)` | `JSONB, STRING` | `BOOL` | 是否包含指定键 | `JSONB_CONTAINS(data, 'name')` |
| `JSONB_EXISTS(json, path)` | `JSONB, STRING` | `BOOL` | 路径是否存在 | `JSONB_EXISTS(data, '$.items[0]')` |
| `JSONB_PRETTY(json)` | `JSONB` | `STRING` | 格式化 JSON | `JSONB_PRETTY(data)` |

### 9.3 聚合函数

| 函数 | 参数 | 返回值 | 说明 | 示例 |
|------|------|--------|------|------|
| `SUM(expr)` | `NUMERIC` | `NUMERIC` | 求和 | `SUM(amount)` |
| `COUNT(*)` | 无 | `INT` | 计数（含 NULL） | `COUNT(*)` |
| `COUNT(expr)` | `ANY` | `INT` | 计数（排除 NULL） | `COUNT(name)` |
| `COUNT(DISTINCT expr)` | `ANY` | `INT` | 去重计数 | `COUNT(DISTINCT city)` |
| `AVG(expr)` | `NUMERIC` | `NUMERIC` | 平均值 | `AVG(age)` |
| `MAX(expr)` | `ANY` | `ANY` | 最大值 | `MAX(age)` |
| `MIN(expr)` | `ANY` | `ANY` | 最小值 | `MIN(age)` |

### 9.4 窗口函数

| 函数 | 说明 | 示例 |
|------|------|------|
| `ROW_NUMBER() OVER (...)` | 行号，从 1 开始 | `ROW_NUMBER() OVER (ORDER BY age DESC)` |
| `RANK() OVER (...)` | 排名，同值同排名，跳号 | `RANK() OVER (ORDER BY score DESC)` |
| `DENSE_RANK() OVER (...)` | 密集排名，同值同排名，不跳号 | `DENSE_RANK() OVER (ORDER BY score DESC)` |
| `LAG(expr, offset, default) OVER (...)` | 前 offset 行值 | `LAG(amount, 1, 0) OVER (ORDER BY date)` |
| `LEAD(expr, offset, default) OVER (...)` | 后 offset 行值 | `LEAD(amount, 1, 0) OVER (ORDER BY date)` |
| `FIRST_VALUE(expr) OVER (...)` | 分区第一行值 | `FIRST_VALUE(name) OVER (PARTITION BY dept)` |
| `LAST_VALUE(expr) OVER (...)` | 分区最后一行值 | `LAST_VALUE(name) OVER (PARTITION BY dept)` |
| `NTILE(n) OVER (...)` | 分 n 桶 | `NTILE(4) OVER (ORDER BY score)` |
| `PERCENT_RANK() OVER (...)` | 百分比排名 | `PERCENT_RANK() OVER (ORDER BY score)` |
| `SUM/AVG/COUNT/MAX/MIN OVER (...)` | 聚合窗口函数 | `SUM(amount) OVER (PARTITION BY user_id)` |

---

## 附录：条件表达式语法

WHERE、HAVING、ON 子句支持的条件运算符：

| 运算符 | 说明 | 示例 |
|--------|------|------|
| `=` | 等于 | `age = 25` |
| `<>`, `!=` | 不等于 | `status <> 0` |
| `<`, `>`, `<=`, `>=` | 比较 | `age > 18` |
| `LIKE` | 模糊匹配（`%` 通配符） | `name LIKE 'A%'` |
| `BETWEEN ... AND ...` | 范围 | `age BETWEEN 18 AND 60` |
| `IN (...)` | 在列表中 | `status IN (1, 2, 3)` |
| `IS NULL` | 是 NULL | `email IS NULL` |
| `IS NOT NULL` | 不是 NULL | `email IS NOT NULL` |
| `AND` | 逻辑与 | `age > 18 AND status = 1` |
| `OR` | 逻辑或 | `status = 0 OR status = 2` |

**三值逻辑说明**：比较涉及 NULL 时结果为 UNKNOWN，WHERE 子句中 UNKNOWN 被当作 FALSE 处理（即 NULL 不满足任何等值比较条件）。

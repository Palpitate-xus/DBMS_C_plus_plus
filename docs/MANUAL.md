# DBMS 完整使用手册

> 最后更新: 2026-08-14
> 版本: v2 存储格式 / 生产化重构阶段
> 回归基线: PASS=139 FAIL=0（137 个 C++ 测试 + PostgreSQL 协议 E2E + 窗口函数 E2E）

> 数据目录说明：当前版本只接受 v2、8 KiB heap page 和当前 schema 格式。heap 文件头、页布局、line pointer 和 checksum 必须通过严格校验；损坏或截断文件 fail-closed。WAL 记录的长度、CRC、8 字节对齐和前向链也必须通过校验；恢复遇到损坏 WAL 或非法 heap image 会 fail-closed。旧数据目录不会自动迁移；升级前请导出 SQL 或删除并重建数据目录。WAL 的 LSN 0 是合法首位置，事务提交会先刷盘 COMMIT WAL，再刷盘 CLOG 可见性；WAL/CLOG/fsync 失败时提交失败、追加 ABORT WAL 并回滚。

> 路由说明：基础 `ALTER TABLE`、RLS enable/disable/force、分区 ATTACH/DETACH、trigger enable/disable、CLUSTER、REPLICA IDENTITY、约束验证/延迟属性、`CREATE TABLE` 分区、RLS 可见性扫描和简单单表视图 `INSTEAD OF` DML 路径由统一执行链处理；ALTER 多子命令失败会恢复整句当前格式快照，DROP/REPLACE 在变更前建立快照，外层 `ROLLBACK` 先恢复对象再撤销行级变更；规范和 legacy DDL 入口在隐式提交失败时都会报告 SQLSTATE 并停止执行；快照已被 DDL 修改后继续执行另一条快照型 DDL、创建或回滚 SAVEPOINT 会 fail-closed；普通事务提交/回滚会清理物理事务备份，DDL CREATE undo 会跨语句外层事务和 SAVEPOINT 回放；触发器函数运行时和完整对象 owner/ACL 组合语义仍未完成。含内存 DDL undo 的事务暂不支持 PREPARE TRANSACTION。RLS 已支持默认 `WITH CHECK`、`PUBLIC` 角色、基础 `PERMISSIVE/RESTRICTIVE` 组合、表 owner 绕过、基于 `pg_authid` 的 `SUPERUSER/BYPASSRLS` 绕过和 `FORCE ROW LEVEL SECURITY`。

---

## 目录

1. [快速开始](#1-快速开始)
2. [数据类型](#2-数据类型)
3. [数据库操作](#3-数据库操作)
4. [表操作](#4-表操作)
5. [数据操作 (DML)](#5-数据操作-dml)
6. [查询 (DQL)](#6-查询-dql)
7. [事务控制](#7-事务控制)
8. [索引](#8-索引)
9. [视图与物化视图](#9-视图与物化视图)
10. [约束](#10-约束)
11. [用户与权限](#11-用户与权限)
12. [角色管理](#12-角色管理)
13. [存储过程与函数](#13-存储过程与函数)
14. [触发器](#14-触发器)
15. [分区表](#15-分区表)
16. [全文搜索](#16-全文搜索)
17. [JSON/JSONB](#17-jsonjsonb)
18. [网络服务](#18-网络服务)
19. [预编译语句](#19-预编译语句)
20. [导入导出](#20-导入导出)
    - [WAL 归档与时间点恢复 (PITR)](#201-wal-归档与时间点恢复-pitr)
21. [pg_hba 访问控制](#21-pg_hba-访问控制)
22. [复制与高可用](#22-复制与高可用)
23. [大对象](#23-大对象)
24. [多进程管理](#24-多进程管理)
25. [GUC 参数配置](#25-guc-参数配置)

---

## 1. 快速开始

### 编译

```bash
# 方式一: 自动构建 (推荐)
./scripts/build.sh

# 方式二: CMake
cmake -S . -B build && cmake --build build -j$(nproc)

# CMake 标准完整回归（等价于 scripts/build_tests.sh）
cmake --build build --target check
# 或
ctest --test-dir build --output-on-failure
```

### 运行

```bash
# 交互模式
./dbms_main

# 网络服务模式
./dbms_main --server 9999
```

### 首次登录

默认情况下，系统启动后需要创建数据库和用户。首次使用时在交互模式下执行：

```sql
CREATE DATABASE mydb;
USE DATABASE mydb;
--- 现在可以开始建表和操作数据
```

---

## 2. 数据类型

### 数值类型
| 类型 | 说明 | 示例 |
|------|------|------|
| `INT` / `INTEGER` | 32位整数 | `42` |
| `BIGINT` | 64位整数 | `9999999999` |
| `SMALLINT` | 16位整数 | `100` |
| `FLOAT` / `REAL` | 单精度浮点 | `3.14` |
| `DOUBLE PRECISION` | 双精度浮点 | `3.1415926535` |
| `NUMERIC(p,s)` / `DECIMAL(p,s)` | 精确数值 | `NUMERIC(10,2)` |
| `MONEY` | 货币类型 | `$100.50` |
| `SERIAL` | 自增整数 (同 IDENTITY) | |

### 字符串类型
| 类型 | 说明 |
|------|------|
| `VARCHAR(n)` | 变长字符串，最大 n 字符 |
| `CHAR(n)` | 定长字符串 |
| `TEXT` | 长文本 |

### 日期时间类型
| 类型 | 说明 | 特殊值 |
|------|------|--------|
| `DATE` | 日期 | `'2024-01-01'`, `infinity`, `-infinity` |
| `TIME` | 时间 | `'14:30:00'` |
| `TIMESTAMP` | 日期时间 | `'2024-01-01 14:30:00'` |
| `TIMESTAMPTZ` | 带时区时间戳 | `'2024-01-01 14:30:00+08'` |
| `INTERVAL` | 时间间隔 | `'1 year 2 months'`, `'90 minutes'` |

### 二进制类型
| 类型 | 说明 |
|------|------|
| `BYTEA` | 二进制数据，支持 hex (`\xDEADBEEF`) 和 escape 格式 |
| `BLOB` | 大字节对象 (MySQL 兼容) |

### 其他类型
| 类型 | 说明 |
|------|------|
| `BOOLEAN` | true/false |
| `UUID` | 全局唯一标识符，如 `'550e8400-e29b-41d4-a716-446655440000'` |
| `JSON` / `JSONB` | JSON 数据，插入时自动验证 |
| `INET` | IPv4/IPv6 地址，如 `'192.168.1.1'` |
| `CIDR` | 网络地址，如 `'192.168.0.0/24'` |
| `MACADDR` | MAC 地址，如 `'08:00:2b:01:02:03'` |
| `ARRAY` | 数组，如 `INT[]` |
| `POINT` | 几何点 |
| `LINE` / `LSEG` / `BOX` / `PATH` / `POLYGON` / `CIRCLE` | 几何类型 |
| `TSVECTOR` / `TSQUERY` | 全文搜索 |
| `COMPOSITE` | 复合类型 (CREATE TYPE) |
| `ENUM` | 枚举类型 (CREATE TYPE ... AS ENUM) |
| `RANGE` | 范围类型，如 `INT4RANGE` |
| `DOMAIN` | 域类型 |

---

## 3. 数据库操作

```sql
-- 创建数据库
CREATE DATABASE dbname;

-- 使用数据库
USE DATABASE dbname;

-- 删除数据库
DROP DATABASE dbname;

-- 删除数据库 (级联)
DROP DATABASE dbname CASCADE;

-- 查看所有数据库
SHOW DATABASES;
```

---

## 4. 表操作

### 建表

```sql
-- 基本建表
CREATE TABLE users (
    ID INT PRIMARY KEY,
    NAME VARCHAR(50) NOT NULL,
    EMAIL VARCHAR(100) UNIQUE,
    AGE INT DEFAULT 0,
    ACTIVE BOOLEAN DEFAULT TRUE
);

-- 自增列
CREATE TABLE items (
    ID INT GENERATED ALWAYS AS IDENTITY,
    LABEL TEXT
);

-- 或使用 SERIAL (兼容语法)
CREATE TABLE items (
    ID SERIAL PRIMARY KEY,
    LABEL TEXT
);

-- 临时表
CREATE TEMPORARY TABLE tmp_data (X INT, Y INT);
-- 临时表仅在当前连接可见，断开连接后自动清理；支持 ON COMMIT
-- PRESERVE ROWS/DELETE ROWS/DROP，完整 pg_temp schema/catalog/search_path
-- 语义尚未实现。服务器重启时会清理异常退出遗留的临时物理文件。

-- Unlogged 表 (不写 WAL)
CREATE UNLOGGED TABLE cache (KEY TEXT, VALUE TEXT);

-- 分区表
CREATE TABLE events (
    ID INT,
    EVENT_TIME TIMESTAMP
) PARTITION BY RANGE (EVENT_TIME);

-- List 分区
CREATE TABLE logs (
    ID INT,
    REGION TEXT
) PARTITION BY LIST (REGION);

-- Hash 分区
CREATE TABLE hash_tbl (
    ID INT,
    HASH_KEY TEXT
) PARTITION BY HASH (HASH_KEY);

-- OF type (typed table)
CREATE TABLE typed_tbl OF some_composite_type;

-- LIKE 复制表结构
CREATE TABLE users_copy (LIKE src INCLUDING ALL);
CREATE TABLE users_defaults (LIKE src INCLUDING DEFAULTS);
CREATE INDEX users_indexes (LIKE src INCLUDING INDEXES);
CREATE TABLE users_constraints (LIKE src INCLUDING CONSTRAINTS);
CREATE TABLE users_identity (LIKE src INCLUDING IDENTITY);

-- 带 storage 参数
CREATE TABLE t (ID INT) WITH (FILLFACTOR = 90);
```

### 修改表

```sql
-- 添加列
ALTER TABLE users ADD COLUMN PHONE VARCHAR(20);
ALTER TABLE users ADD COLUMN SCORE INT DEFAULT 0;

-- 添加列 (IF NOT EXISTS)
ALTER TABLE users ADD IF NOT EXISTS PHONE VARCHAR(20);

-- 删除列
ALTER TABLE users DROP COLUMN PHONE;
ALTER TABLE users DROP IF EXISTS PHONE;

-- 修改列类型
ALTER TABLE users ALTER COLUMN AGE TYPE BIGINT;
ALTER TABLE users ALTER COLUMN AGE SET DATA TYPE VARCHAR(10);

-- 设置/删除默认值
ALTER TABLE users ALTER COLUMN AGE SET DEFAULT 18;
ALTER TABLE users ALTER COLUMN AGE DROP DEFAULT;

-- 设置/删除 NOT NULL
ALTER TABLE users ALTER COLUMN AGE SET NOT NULL;
ALTER TABLE users ALTER COLUMN AGE DROP NOT NULL;

-- 重命名列
ALTER TABLE users RENAME COLUMN AGE TO USER_AGE;
ALTER TABLE users RENAME COLUMN AGE TO USER_AGE; -- IF EXISTS

-- 重命名表
ALTER TABLE users RENAME TO app_users;

-- 添加约束
ALTER TABLE users ADD CONSTRAINT CHK_AGE CHECK (AGE >= 0);
ALTER TABLE users ADD PRIMARY KEY (ID);
ALTER TABLE users ADD UNIQUE (EMAIL);
ALTER TABLE users ADD FOREIGN KEY (DEPT_ID) REFERENCES depts(ID);
ALTER TABLE users ADD CONSTRAINT EXCL_EMAIL EXCLUDE USING btree (EMAIL WITH =);

-- 删除约束
ALTER TABLE users DROP CONSTRAINT CHK_AGE;
ALTER TABLE users DROP CONSTRAINT IF EXISTS CHK_AGE;
ALTER TABLE users DROP CONSTRAINT EXCL_EMAIL;

-- 重命名约束
ALTER TABLE users RENAME CONSTRAINT CHK_AGE TO CHK_USER_AGE;

-- SET / RESET storage 参数
ALTER TABLE users SET (FILLFACTOR = 80);
ALTER TABLE users RESET (FILLFACTOR);

-- 修改 replica identity
ALTER TABLE users REPLICA IDENTITY DEFAULT;
ALTER TABLE users REPLICA IDENTITY FULL;
ALTER TABLE users REPLICA IDENTITY USING INDEX idx_name;

-- CLUSTER ON / SET WITHOUT CLUSTER
ALTER TABLE users CLUSTER ON idx_name;
ALTER TABLE users SET WITHOUT CLUSTER;

-- ENABLE/DISABLE TRIGGER
ALTER TABLE users DISABLE TRIGGER trigger_name;
ALTER TABLE users ENABLE TRIGGER trigger_name;

-- ROW LEVEL SECURITY
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE users FORCE ROW LEVEL SECURITY;

-- SET SCHEMA
ALTER TABLE users SET SCHEMA new_schema;

-- SET TABLESPACE
-- 先执行 CREATE TABLESPACE name LOCATION 'absolute/path'
ALTER TABLE users SET TABLESPACE my_space;

-- SET LOGGED / UNLOGGED
ALTER TABLE users SET LOGGED;
ALTER TABLE users SET UNLOGGED;

-- ALTER COLUMN SET STATISTICS
ALTER TABLE users ALTER COLUMN AGE SET STATISTICS 500;

-- INHERIT / NO INHERIT (表继承)
ALTER TABLE child INHERIT parent;
ALTER TABLE child NO INHERIT parent;

-- ATTACH / DETACH PARTITION
ALTER TABLE events ATTACH PARTITION events_p2024 FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
ALTER TABLE events DETACH PARTITION events_p2024;

-- SET DEFAULT VALUES
ALTER TABLE users ALTER COLUMN ID SET DEFAULT 42;
```

### 删除表

```sql
DROP TABLE users;
DROP TABLE IF EXISTS users;
DROP TABLE users CASCADE;
```

### 截断表

```sql
TRUNCATE TABLE users;
TRUNCATE TABLE users, logs, cache;
TRUNCATE ONLY users;
TRUNCATE TABLE parent RESTRICT;
TRUNCATE TABLE parent RESTART IDENTITY CASCADE;
```

`RESTRICT` 是默认行为：如果有外键依赖，语句会在任何表修改前失败；`CASCADE` 会递归截断外键依赖表。`RESTART IDENTITY` 重置被截断表的 identity 序列，`ONLY` 不扩展到继承子表。

---

## 5. 数据操作 (DML)

### 插入

```sql
INSERT INTO users (ID, NAME, AGE) VALUES (1, 'Alice', 25);
INSERT INTO users (ID, NAME) VALUES (2, 'Bob');
INSERT INTO users VALUES (3, 'Charlie', 30);

-- 多行插入
INSERT INTO users (ID, NAME) VALUES
    (4, 'Dave'),
    (5, 'Eve'),
    (6, 'Frank');

-- INSERT INTO ... SELECT
INSERT INTO users_backup SELECT * FROM users WHERE AGE > 20;

-- ON CONFLICT (UPSERT；当前 AST 路径为显式匹配单列/复合主键或 UNIQUE 约束 target + 常量或只引用 excluded 的受限标量表达式 SET/WHERE)
INSERT INTO users (ID, NAME, AGE) VALUES (1, 'Alice Updated', 26)
    ON CONFLICT (ID) DO UPDATE SET NAME = 'Alice Updated', AGE = 26;

-- DEFAULT VALUES
INSERT INTO users DEFAULT VALUES;

-- OVERRIDING
INSERT INTO users (ID, NAME) OVERRIDING SYSTEM VALUE VALUES (10, 'Test');

-- RETURNING
INSERT INTO users (ID, NAME) VALUES (100, 'New') RETURNING ID, NAME;
```

### 更新

```sql
UPDATE users SET AGE = 26 WHERE ID = 1;
UPDATE users SET AGE = AGE + 1 WHERE NAME LIKE 'A%';
UPDATE users SET NAME = 'Bob Updated', AGE = 31 WHERE ID = 2;

-- UPDATE FROM (多表更新)
UPDATE users SET AGE = orders.derived_age
FROM orders WHERE users.ID = orders.user_id;

-- RETURNING
UPDATE users SET AGE = 18 WHERE AGE < 18 RETURNING *;
```

### 删除

```sql
DELETE FROM users WHERE ID = 1;
DELETE FROM users WHERE AGE < 18;

-- DELETE USING
DELETE FROM users USING orders WHERE users.ID = orders.user_id;

-- RETURNING
DELETE FROM users WHERE ACTIVE = FALSE RETURNING ID;
```

### MERGE

```sql
MERGE INTO target USING source ON target.ID = source.ID
    WHEN MATCHED THEN UPDATE SET NAME = source.NAME
    WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (source.ID, source.NAME);
```

当前 typed executor 仅支持单源表、一个 `MATCHED` 分支和一个 `NOT MATCHED` 分支；支持 `UPDATE`/`INSERT`/`DO NOTHING`。多 `WHEN`、`BY SOURCE`/`BY TARGET`、`DELETE`、复杂 source query 和 `RETURNING` 会明确拒绝。

### REPLACE INTO (MySQL 兼容)

```sql
REPLACE INTO users (ID, NAME) VALUES (1, 'New Alice');
-- 等价于: DELETE + INSERT (冲突时)
```

---

## 6. 查询 (DQL)

### 基础查询

```sql
SELECT * FROM users;
SELECT NAME, AGE FROM users;
SELECT * FROM users WHERE AGE > 20;
SELECT * FROM users WHERE AGE > 20 AND NAME LIKE 'A%';
SELECT * FROM users WHERE ID IN (1, 2, 3);
SELECT * FROM users WHERE AGE BETWEEN 18 AND 65;
SELECT * FROM users WHERE NAME IS NULL;
SELECT * FROM users WHERE ACTIVE IS NOT NULL;

-- DISTINCT
SELECT DISTINCT DEPT FROM users;
SELECT DISTINCT ON (DEPT) NAME, SALARY FROM employees;

-- LIMIT/OFFSET
SELECT * FROM users LIMIT 10;
SELECT * FROM users LIMIT 10 OFFSET 20;
SELECT * FROM users FETCH FIRST 5 ROWS ONLY;
SELECT * FROM users FETCH FIRST 5 ROWS WITH TIES;

-- ORDER BY
SELECT * FROM users ORDER BY AGE DESC;
SELECT * FROM users ORDER BY AGE ASC, NAME DESC;
SELECT * FROM users ORDER BY AGE NULLS FIRST;
```

### 聚合

```sql
SELECT COUNT(*) FROM users;
SELECT COUNT(DISTINCT DEPT) FROM users;
SELECT MAX(SALARY), MIN(SALARY), AVG(SALARY) FROM employees;
SELECT SUM(AMOUNT) FROM orders;

-- GROUP BY
SELECT DEPT, COUNT(*) FROM employees GROUP BY DEPT;
SELECT DEPT, AVG(SALARY) FROM employees GROUP BY DEPT HAVING AVG(SALARY) > 50000;
SELECT DEPT, COUNT(*) FROM employees GROUP BY ROLLUP (DEPT);
SELECT DEPT, TEAM, COUNT(*) FROM employees GROUP BY CUBE (DEPT, TEAM);
SELECT DEPT, COUNT(*) FROM employees GROUP BY GROUPING SETS ((DEPT), ());
```

### JOIN

```sql
SELECT * FROM users INNER JOIN orders ON users.ID = orders.USER_ID;
SELECT * FROM users LEFT JOIN orders ON users.ID = orders.USER_ID;
SELECT * FROM users RIGHT JOIN orders ON users.ID = orders.USER_ID;
SELECT * FROM users FULL OUTER JOIN orders ON users.ID = orders.USER_ID;
SELECT * FROM users CROSS JOIN orders;

-- NATURAL JOIN
SELECT * FROM users NATURAL JOIN user_profiles;

-- JOIN USING
SELECT * FROM users JOIN orders USING (ID);

-- 多表 JOIN
SELECT u.NAME, o.AMOUNT, p.PRODUCT_NAME
FROM users u
JOIN orders o ON u.ID = o.USER_ID
JOIN products p ON o.PRODUCT_ID = p.ID;

-- FOR UPDATE
SELECT * FROM users WHERE ID = 1 FOR UPDATE;
SELECT * FROM users WHERE ID = 1 FOR SHARE;
SELECT * FROM users WHERE ID = 1 FOR NO KEY UPDATE;
SELECT * FROM users WHERE ID = 1 FOR KEY SHARE;
SELECT * FROM users WHERE ID = 1 FOR UPDATE NOWAIT;
SELECT * FROM users WHERE ID = 1 FOR UPDATE SKIP LOCKED;
```

### 子查询

```sql
-- IN 子查询
SELECT * FROM users WHERE ID IN (SELECT USER_ID FROM orders);

-- EXISTS 子查询
SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.USER_ID = u.ID);

-- ANY/ALL
SELECT * FROM employees WHERE SALARY > ANY (SELECT SALARY FROM managers);
SELECT * FROM employees WHERE SALARY > ALL (SELECT SALARY FROM interns);

-- 标量子查询
SELECT NAME, (SELECT COUNT(*) FROM orders o WHERE o.USER_ID = u.ID) AS order_count FROM users u;

-- 派生表
SELECT * FROM (SELECT ID, NAME FROM users WHERE AGE > 20) AS adults;
```

### UNION / INTERSECT / EXCEPT

```sql
SELECT NAME FROM active_users
UNION
SELECT NAME FROM archived_users;

SELECT NAME FROM active_users
UNION ALL
SELECT NAME FROM archived_users;

SELECT A FROM t1 INTERSECT SELECT A FROM t2;
SELECT A FROM t1 EXCEPT SELECT A FROM t2;
```

### CTE (公用表表达式)

```sql
WITH vip_users AS (SELECT ID, NAME FROM users WHERE VIP = 1)
SELECT vip_users.NAME, orders.AMOUNT
FROM vip_users JOIN orders ON vip_users.ID = orders.USER_ID;

-- RECURSIVE CTE
WITH RECURSIVE cte AS (
    SELECT 1 AS n UNION ALL SELECT n + 1 FROM cte WHERE n < 100
)
SELECT * FROM cte;
```

### 窗口函数

```sql
SELECT NAME, ROW_NUMBER() OVER (ORDER BY SCORE DESC) FROM users;
SELECT NAME, RANK() OVER (ORDER BY SCORE DESC) FROM users;
SELECT NAME, DENSE_RANK() OVER (ORDER BY SCORE DESC) FROM users;
SELECT NAME, LAG(NAME, 1) OVER (ORDER BY AGE) FROM users;
SELECT NAME, LEAD(NAME, 1) OVER (ORDER BY AGE) FROM users;
SELECT NAME, FIRST_VALUE(NAME) OVER (ORDER BY SCORE) FROM users;
SELECT NAME, LAST_VALUE(NAME) OVER (ORDER BY SCORE) FROM users;
SELECT NAME, NTILE(4) OVER (ORDER BY SCORE) FROM users;
SELECT NAME, PERCENT_RANK() OVER (ORDER BY SCORE) FROM users;
SELECT NAME, CUME_DIST() OVER (ORDER BY SCORE) FROM users;

-- PARTITION BY + ORDER BY
SELECT NAME, DEPT, ROW_NUMBER() OVER (PARTITION BY DEPT ORDER BY SALARY DESC)
FROM employees;

-- EXCLUDE CURRENT ROW / GROUP / TIES / NO OTHERS
SELECT NAME, AVG(SALARY) OVER (ORDER BY AGE ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW)
FROM employees;
```

### EXPLAIN

```sql
EXPLAIN SELECT * FROM users WHERE ID = 1;
EXPLAIN (ANALYZE) SELECT * FROM users WHERE AGE > 20;
EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM users;
EXPLAIN (FORMAT JSON) SELECT * FROM users;
```

---

## 7. 事务控制

```sql
BEGIN;
BEGIN TRANSACTION;
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
BEGIN READ ONLY;
BEGIN TRANSACTION READ WRITE;

-- DML 操作...
COMMIT;
ROLLBACK;

-- AND CHAIN / AND NO CHAIN
COMMIT AND CHAIN;
COMMIT AND NO CHAIN;
ROLLBACK AND CHAIN;
ROLLBACK AND NO CHAIN;

-- SAVEPOINT
SAVEPOINT sp1;
ROLLBACK TO SAVEPOINT sp1;
RELEASE SAVEPOINT sp1;

-- PREPARE TRANSACTION / COMMIT PREPARED
PREPARE TRANSACTION 'txn_001';
COMMIT PREPARED 'txn_001';
ROLLBACK PREPARED 'txn_001';
```

包含跨语句 DDL CREATE undo 的事务暂不支持 `PREPARE TRANSACTION`；请先完成 `COMMIT` 或 `ROLLBACK`。

---

## 8. 索引

```sql
-- B+ 树索引
CREATE INDEX idx_name ON users (NAME);
CREATE UNIQUE INDEX idx_email ON users (EMAIL);

-- 复合索引
CREATE INDEX idx_name_age ON users (NAME, AGE);

-- Hash 索引
CREATE INDEX idx_email_hash ON users USING HASH (EMAIL);
CREATE INDEX idx_body_gin ON users USING GIN (BODY);
CREATE INDEX idx_id_gist ON users USING GiST (ID);
CREATE INDEX idx_id_brin ON users USING BRIN (ID);
CREATE INDEX idx_id_spgist ON users USING SPGIST (ID);

-- 覆盖索引 (INCLUDE)
CREATE INDEX idx_name_include ON users (NAME) INCLUDE (AGE, EMAIL);

-- 部分索引
CREATE INDEX idx_active ON users (NAME) WHERE ACTIVE = TRUE;

-- 表达式索引
CREATE INDEX idx_upper_name ON users ((UPPER(NAME)));

-- 带排序方向
CREATE INDEX idx_age_desc ON users (AGE DESC NULLS LAST);

-- 并发创建
CREATE INDEX CONCURRENTLY idx_name ON users (NAME);

-- 删除索引
DROP INDEX idx_name;
DROP INDEX IF EXISTS idx_a, idx_b CASCADE;

-- 重索引
REINDEX TABLE users;
REINDEX INDEX idx_name;

-- 查看索引信息
SHOW INDEX FROM users;
```

---

## 9. 视图与物化视图

```sql
-- 视图
CREATE VIEW vip_users AS SELECT * FROM users WHERE VIP = 1;
CREATE OR REPLACE VIEW active_users AS SELECT * FROM users WHERE ACTIVE = TRUE;

-- WITH CHECK OPTION
CREATE VIEW young_users AS SELECT * FROM users WHERE AGE < 30 WITH CHECK OPTION;

-- 删除视图
DROP VIEW vip_users;

-- 物化视图
CREATE MATERIALIZED VIEW mv_dept_stats AS
    SELECT DEPT, COUNT(*), AVG(SALARY) FROM employees GROUP BY DEPT;

CREATE MATERIALIZED VIEW mv_empty WITH NO DATA;

-- 刷新
REFRESH MATERIALIZED VIEW mv_dept_stats;
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_dept_stats;
REFRESH MATERIALIZED VIEW mv_dept_stats WITH NO DATA;
```

---

## 10. 约束

```sql
-- CHECK 约束 (支持多个命名)
CREATE TABLE products (
    ID INT PRIMARY KEY,
    PRICE INT CONSTRAINT POSITIVE CHECK (PRICE > 0),
    CONSTRAINT MAX_PRICE CHECK (PRICE < 10000)
);

-- 表级约束全名约束名
ALTER TABLE products ADD CONSTRAINT CHK_PRICE_RANGE CHECK (PRICE > 0 AND PRICE < 10000);

-- 外键级联
CREATE TABLE orders (
    ID INT PRIMARY KEY,
    USER_ID INT REFERENCES users(ID) ON DELETE CASCADE ON UPDATE SET NULL
);
```

---

## 11. 用户与权限

```sql
-- 创建用户
CREATE USER alice WITH PASSWORD 'secret';

-- 创建超级用户
CREATE USER admin WITH PASSWORD 'change-this-password' SUPERUSER;

-- 删除用户
DROP USER alice;

-- 权限管理
GRANT SELECT ON users TO alice;
GRANT SELECT, INSERT, UPDATE ON users TO alice;
GRANT ALL PRIVILEGES ON DATABASE mydb TO admin;
GRANT SELECT (ID, NAME) ON users TO alice;  -- 列级权限
GRANT SELECT ON ALL TABLES IN SCHEMA public TO alice;
GRANT ALL ON users TO alice WITH GRANT OPTION;

-- 撤销权限
REVOKE SELECT ON users FROM alice;
REVOKE ALL PRIVILEGES ON DATABASE mydb FROM admin;
REVOKE GRANT OPTION FOR INSERT ON users FROM alice CASCADE;

-- ALTER DEFAULT PRIVILEGES
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO alice;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT ON TABLES TO alice, bob;
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT ON TABLES FROM alice;
```

当前仅支持表级 `TABLE`/`TABLES` 默认权限。规则只影响之后创建的表，重复 GRANT 不会重复写入；`WITH GRANT OPTION`、`GRANT OPTION FOR` 以及 sequence/function 等对象类型会明确报错。

---

## 12. 角色管理

```sql
-- 创建角色
CREATE ROLE readonly_role;
CREATE ROLE dev WITH CREATEDB LOGIN CONNECTION LIMIT 100;

-- 创建用户 (用户的特殊形式)
CREATE USER bob WITH PASSWORD 'secret' LOGIN IN ROLE readonly_role;

-- 授予/撤销角色
GRANT readonly_role TO bob;
GRANT readonly_role TO bob WITH ADMIN OPTION;
REVOKE ADMIN OPTION FOR readonly_role FROM bob;
REVOKE readonly_role FROM bob;

-- 修改角色属性
ALTER ROLE bob WITH SUPERUSER CREATEROLE REPLICATION BYPASSRLS;
ALTER ROLE bob WITH PASSWORD 'new_secret' VALID UNTIL '2026-12-31';
ALTER ROLE bob CONNECTION LIMIT 50;
ALTER ROLE bob NOLOGIN;
ALTER ROLE bob RENAME TO robert;
```

---

## 13. 存储过程与函数

```sql
-- 创建函数
CREATE FUNCTION add_one(INT) RETURNS INT AS 'return $1 + 1;' LANGUAGE sql;

CREATE FUNCTION greet(NAME VARCHAR) RETURNS VARCHAR AS
'BEGIN RETURN ''Hello, '' || NAME || ''!''; END;' LANGUAGE plpgsql;

-- 多参数函数
CREATE FUNCTION add(INT, INT) RETURNS INT AS 'return $1 + $2' LANGUAGE sql;

-- 聚合函数
CREATE AGGREGATE my_sum(INT) (SFUNC = INT4PLUS, STYPE = INT4, INITCOND = '0');

-- 存储过程
CREATE PROCEDURE transfer(INT, INT, NUMERIC) AS $$
BEGIN
    UPDATE accounts SET balance = balance - $3 WHERE id = $1;
    UPDATE accounts SET balance = balance + $3 WHERE id = $2;
END;
$$ LANGUAGE plpgsql;

-- 调用
SELECT add_one(5);
CALL transfer(1, 2, 100);

-- 删除
DROP FUNCTION add_one(INT);
DROP PROCEDURE transfer;
```

---

## 14. 触发器

```sql
-- 创建触发器
CREATE TRIGGER update_modified
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER check_age
    BEFORE INSERT ON users
    FOR EACH ROW
    WHEN (NEW.age < 0)
    EXECUTE FUNCTION reject_negative_age();

-- 删除触发器
DROP TRIGGER update_modified ON users;
```

---

## 15. 分区表

```sql
-- RANGE 分区
CREATE TABLE events (
    ID INT, EVENT_TIME TIMESTAMP
) PARTITION BY RANGE (EVENT_TIME);

-- ATTACH 已有表作为分区
CREATE TABLE events_2024 (LIKE events);
ALTER TABLE events ATTACH PARTITION events_2024
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

-- LIST 分区
CREATE TABLE logs (
    ID INT, REGION TEXT
) PARTITION BY LIST (REGION);

-- HASH 分区
CREATE TABLE h (
    ID INT, K TEXT
) PARTITION BY HASH (K);
```

---

## 16. 全文搜索

```sql
-- TSVECTOR 列
CREATE TABLE docs (ID INT PRIMARY KEY, CONTENT TEXT, TSV TSVECTOR);

-- 生成 TSVECTOR
SELECT TO_TSVECTOR('english', 'The quick brown fox');

-- TSQUERY
SELECT * FROM docs WHERE TSV @@ TO_TSQUERY('english', 'fox & quick');

-- RANK
SELECT TS_RANK(TSV, TO_TSQUERY('english', 'fox')) FROM docs;
```

---

## 17. JSON/JSONB

```sql
CREATE TABLE configs (ID INT PRIMARY KEY, DATA JSONB);

INSERT INTO configs VALUES (1, '{"name":"Alice","age":25,"tags":["admin","user"]}');

-- 提取
SELECT DATA->>'name' FROM configs;
SELECT JSONB_EXTRACT_TEXT(DATA, '$.name') FROM configs;
SELECT JSONB_PRETTY(DATA) FROM configs;

-- 包含
SELECT * FROM configs WHERE DATA @> '{"age":25}';
SELECT JSONB_CONTAINS(DATA, '{"admin"}') FROM configs;
```

---

## 17b. 运行时性能参数（环境变量）

```bash
# 堆表缓冲池帧数（每帧 8 KiB，默认 256 = 2 MiB/关系）
export DBMS_BUFFER_FRAMES=1024

# 索引缓冲池帧数（默认 128）
export DBMS_INDEX_BUFFER_FRAMES=256
```

两个变量均接受 16..4096 并夹取到该区间；未设置时使用默认值。缓冲池为每关系独立，
总内存占用 ≈ 帧数 × 8 KiB × 打开的关系数，调大时请评估进程驻留内存。
页完整性校验（Fletcher-16 + 结构校验）只在页面从磁盘加载时执行一次，缓冲命中不重复校验。

---

## 18. 网络服务

```bash
# 生产服务端：证书和私钥必须预先准备好
export DBMS_TLS_CERT=/etc/dbms/tls/server.crt
export DBMS_TLS_KEY=/etc/dbms/tls/server.key
./dbms_main --server 9999

# 仅限本地开发的明文模式（生产环境禁止）
./dbms_main --server 9999 --insecure

# 客户端连接（libpq/psql；当前支持 SCRAM-SHA-256 与基础协议流程）
psql "host=localhost port=9999 dbname=info user=admin sslmode=require"

# 连接监控
SHOW CONNECTIONS;
SHOW PROCESSLIST;
SHOW STATUS;
SHOW LOCKS;
SHOW DEADLOCKS;
```

服务端默认 fail-closed：OpenSSL 不可用、证书/私钥缺失或 TLS 初始化失败时不会启动明文监听。证书路径也可以保留默认值 `server.crt` / `server.key`。

服务进程收到 `SIGINT` 或 `SIGTERM` 后会停止接收新连接，关闭活动连接并等待客户端 worker 退出；监听端口失败会以非零退出码结束。`--insecure` 仅用于本地开发。

---

## 19. 预编译语句

```sql
PREPARE stmt AS SELECT * FROM users WHERE ID = $1;
EXECUTE stmt USING (1);
DEALLOCATE PREPARE stmt;
```

---

## 20. 导入导出

```sql
-- CSV 导入
LOAD DATA INFILE 'data.csv' INTO TABLE users;

-- CSV 导出
SELECT * FROM users INTO OUTFILE 'output.csv';

-- COPY
COPY users FROM 'data.csv';
COPY users TO 'output.csv';

-- DUMP/RESTORE (数据库级)
DUMP DATABASE mydb TO 'mydb.sql';
RESTORE DATABASE mydb FROM 'mydb.sql';

-- BACKUP (物理备份: 整库目录快照)
BACKUP DATABASE mydb TO 'mydb.bak';
```

---

## 20.1 WAL 归档与时间点恢复 (PITR)

### 配置归档

`dbms.conf` 中设置 `archive_command`（单引号值,可含空格和 `=`）:

```
# 内建安全模式: 段被原子复制到目录 (无 shell)
archive_command='dir:/var/dbms/archive'

# 外部命令形式已保留 (dir: 前缀以外的值暂不执行)
```

归档触发在 checkpoint 边界: 完整写满的段先标记 `archive_status/<seg>.ready`,
复制成功后翻转为 `.done`; 失败保留 `.ready`,后台归档线程每轮重试。

### 强制切换段 (pg_switch_wal)

小负载不会自然填满 16 MiB 段。要在关键时刻确保 WAL 已归档:

```sql
PG_SWITCH_WAL;   -- 当前段零填充关闭, 后续记录写入新段
CHECKPOINT;      -- 关闭的段此刻被归档
```

### 时间点恢复

```sql
-- 1. 基础备份
BACKUP DATABASE mydb TO '/backup/mydb_base';

-- 2. (持续运行中) 事务不断写入并被归档

-- 3. 灾难发生: 恢复到过去某一时刻
RESTORE DATABASE mydb FROM '/backup/mydb_base'
    PITR '2026-08-23 12:34:56' ARCHIVE '/var/dbms/archive';

-- 4. 重启进程: 恢复重放归档 WAL, 目标时刻之后的事务被回滚
```

语义:

- 恢复目标时刻**含**该时刻: 恰在目标时刻提交的事务保留。
- 目标之后提交的事务按未提交处理,其 before-image 被恢复(等于这些写入从未发生)。
- 恢复目标是单次的: 重放成功后 `recovery_target` 标记被消费,后续重启是普通崩溃恢复。
- 提交时间戳以 v2 格式写在 WAL 提交记录里; 旧格式记录按"无限制"重放。
- 时间线固定为 1 (多时间线 `PITR` 分支仍为差距, 见 feature-gaps.md)。

---

## 21. pg_hba 访问控制

系统支持类 PostgreSQL 的 `pg_hba.conf` 访问控制：

```
# TYPE  DATABASE  USER      ADDRESS        METHOD
local   all       all                      trust
host    all       all       127.0.0.1/32    md5
host    all       all       192.168.0.0/16  scram-sha-256
hostssl all       admin     10.0.0.0/8      cert
host    all       all       0.0.0.0/0       reject
```

解析器识别 `trust`, `md5`, `scram-sha-256`, `password`, `ident`, `peer`, `cert`, `pam`, `ldap`, `radius`, `reject`。
网络运行时当前实际执行 `trust`、`password`、`scram-sha-256`、使用 SCRAM verifier 的 `md5`、以及 `reject`；其他方法 fail-closed 返回未实现错误。支持首条匹配、`hostssl`/`hostnossl`、IPv4/IPv6 CIDR、`sameuser`、`samerole`/`samegroup` 和 `+role`。

---

## 22. 复制与高可用

```sql
-- 创建复制槽 (支持物理和逻辑)
SELECT * FROM pg_create_physical_replication_slot('standby_1');
SELECT * FROM pg_create_logical_replication_slot('logical_slot', 'test_decoding');

-- 查看复制槽
SELECT * FROM pg_replication_slots;

-- 删除复制槽
SELECT * FROM pg_drop_replication_slot('standby_1');

-- 备用节点管理
-- standby_mode / promote 通过 ReplicationManager API 操作

-- 同步/异步复制配置
SET synchronous_commit = on;

-- 发布/订阅
CREATE PUBLICATION mypub FOR TABLE users, orders;
CREATE SUBSCRIPTION mysub CONNECTION 'host=primary' PUBLICATION mypub;
```

---

## 23. 大对象

```sql
-- 大对象通过 LargeObjectManager API 管理
-- 创建/读取/写入/截断/删除/导入/导出
-- 存储在 {db}/.lobjects/ 目录中
```

---

## 24. 多进程管理

系统内置 `ProcessManager`，管理后端进程池：

```cpp
// BackendType: ClientBackend, WalWriter, BgWriter, Checkpointer,
//             AutoVacuumLauncher/Worker, ReplicationSender, Archiver,
//             StatsCollector, LogicalLauncher/Worker
```

---

## 25. GUC 参数配置

```sql
-- 当前连接参数
SET timezone = 'Asia/Shanghai';
SET statement_timeout = 30000;
SET lock_timeout = 1000;
SET deadlock_timeout = 1000;

-- 重置
RESET search_path;
RESET ALL;

-- 查看运行时参数
SHOW VARIABLES;

-- 配置文件位于工作目录 dbms.conf；修改后由管理员请求重新加载
SELECT pg_reload_conf();

-- 事务隔离级别
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

-- ALTER SYSTEM (持久化)
ALTER SYSTEM SET work_mem = '64MB';
-- 当前版本只支持 ALTER SYSTEM SET；如需恢复默认值请删除/修改 dbms.conf

-- AUTO_VACUUM
SET GLOBAL AUTO_VACUUM = ON;
SET GLOBAL AUTO_VACUUM_THRESHOLD = 1000;

-- 兼容模式（DIV 框架；会话开始处受限，事务中不可更改）
SHOW compatibility_mode;                     -- 默认 postgresql18
SET compatibility_mode = 'extended';         -- 显式启用项目扩展层
SET compatibility_mode = 'postgresql18';     -- 回到 PostgreSQL 18 语义
SHOW dbms.extensions;                        -- 审计当前会话启用的偏移
```

### 25.1 兼容模式与能力门控（0A000）

默认 `postgresql18` 模式下，以下命令**没有真实运行时**，一律返回
`feature_not_supported`（SQLSTATE `0A000`），不再写入兼容对象记录后假报成功：

- `CREATE/ALTER/DROP EXTENSION、OPERATOR [CLASS/FAMILY]、RULE、LANGUAGE、
  AGGREGATE、TRANSFORM、ASSERTION、ACCESS METHOD、EVENT TRIGGER、
  SERVER、FOREIGN DATA WRAPPER、FOREIGN TABLE、USER MAPPING、
  SUBSCRIPTION、TEXT SEARCH CONFIGURATION/DICTIONARY/PARSER/TEMPLATE`
- `IMPORT FOREIGN SCHEMA`（无 FDW 运行时）
- `LOAD 'library'`（无动态加载运行时）

其中 `ASSERTION` 在两种模式下都返回 `0A000`（PostgreSQL 18 本身也未实现
SQL assertion）。显式 `SET compatibility_mode = 'extended'` 后，其余门控命令
恢复旧的兼容对象记录行为，便于项目工具链过渡；该模式是会话级设置，
进入事务后不可切换。

同一批门控的项目语法（`postgresql18` 模式下的行为）：

- `USE [DATABASE]`（DIV-01）→ `0A000`，提示重连切换数据库
- `REPLACE INTO`（DIV-02）→ `42601`，提示 `INSERT ... ON CONFLICT`
- `LOAD DATA INFILE`（DIV-03）→ `42601`，提示 `COPY ... FROM`
- `SELECT ... INTO OUTFILE`（DIV-04）→ `42601`，提示 `COPY ... TO`
- `DESC/DESCRIBE`、`VIEW TABLE/DATABASE`、`SHOW USERS/ROLES/POOLS`（DIV-05）→
  `0A000`，提示查询 `information_schema`/`pg_catalog`/`pg_roles`
- `CREATE/DROP REPLICATION SLOT` SQL 形式与 `SHOW LOGICAL/REPLICATION SLOTS`
  （DIV-09）→ `0A000`，提示使用 replication protocol 或
  `pg_*_replication_slot()` 函数
- `SET GLOBAL`（DIV-11）→ `42601`，提示 `ALTER SYSTEM`
- MySQL/SQL Server 类型别名 `TINYINT`、`LONG`、`DATETIME`、`BLOB`、
  `BINARY/VARBINARY`、`NCHAR/NVARCHAR`（DIV-06）→ `42704`/`42601` 类型错误，
  错误信息给出等价的 PostgreSQL 类型（`smallint`/`bigint`/`timestamp`/
  `bytea`/`char`/`varchar`）
- `CREATE/DROP FULLTEXT INDEX`（DIV-07）→ `42601`，提示
  `CREATE INDEX ... USING gin (to_tsvector(col))` 与 `DROP INDEX`
- `DUMP`（DIV-10）→ `0A000`，提示 `pg_dump`；`BACKUP DATABASE` →
  `0A000`，提示 `pg_basebackup`；`RESTORE DATABASE` → `0A000`，提示
  `pg_restore` 或 recovery.signal + restore_command；
  `CLEAR PLAN CACHE` → `0A000`（项目扩展）
- PgBouncer 风格连接池与 TDE（DIV-12）：`SHOW POOLS`、`SHOW TDE STATUS`
  → `0A000`；`pool_mode`/`pool_size`/`max_client_conn` 不再出现在
  `SHOW ALL`，单独 `SHOW` 返回 `42704` 未识别参数；扩展能力只能通过
  服务器配置文件启用，不在 SQL 层暴露
- `CREATE ASSERTION`（DIV-08）→ 两种模式都返回 `0A000`（PostgreSQL 18
  同样未实现 SQL assertion）
- 启动标识（DIV-13）：服务端 banner 标明自身版本与兼容模式，并明确
  提示“这不是 PostgreSQL server cluster”；`server_version` 参数上报
  `DBMS-C++ protocol/3.0`，不冒充 PostgreSQL 版本号；无默认端口，
  必须显式 `--server PORT`

会话默认模式可由环境变量 `DBMS_COMPATIBILITY_MODE=extended|postgresql18`
设定（默认 `postgresql18`），CLI 与网络会话一致生效。

### 25.2 差分兼容测试（P0-16）

`tests/compat/pg_diff_runner.py` 将 `tests/compat/cases/*.sql` 中的同一组
语句分别发往参考 PostgreSQL（docker 容器 `pgref`，psql `-A -t` + NULL
标记）和本 DBMS（wire protocol），规范化不稳定字段后逐条比对行数据与
SQLSTATE。任何差异必须显式加入 allowlist 并注明原因与过期版本。

当前覆盖（67 个用例文件）：算术、字符串函数、布尔/NULL 三值逻辑、
整数/numeric 除法（含 PG `select_div_scale` 的 16/20 位小数规则）、
CASE 表达式、聚合（sum/count/avg/min/max、GROUP BY、FILTER 子句）、
显式/隐式类型转换（CAST(x AS t) 前缀语法、`::` 后缀语法、舍入与
布尔规则）、有状态 DDL/DML 往返、JOIN（inner/left/right/cross 及
join 上的纯聚合）、子查询（WHERE 标量子查询、IN/NOT IN、HAVING、
无 FROM 投影中的标量子查询、相关 EXISTS/NOT EXISTS —— 单等值
相关条件经半连接（SemiJoinOp）下推执行、相关标量聚合
子查询 v > (SELECT min(v) ... WHERE g = s.g) —— 按相关值分组
一次性物化内表聚合，再逐行比较）、错误面（除零 22012、非法整型输入
22P02、缺失表/函数）、窗口函数（row_number/rank/dense_rank/lag/lead/first_value/
last_value/sum/avg/min/max/count、PARTITION BY 与 ORDER BY 组合、
命名窗口 WINDOW 子句、ntile/lag(v,n) 带偏移量、ROWS/RANGE/
GROUPS 帧含无 ORDER BY 的 PARTITION 帧、EXCLUDE CURRENT ROW/
GROUP/TIES 排除子句、表头别名或裸函数名、avg 的 PG
select_div_scale 精确数值语义）、ROLLUP 分组集（含关键字后带
空格的 ROLLUP (g) 写法与总计行的 NULL 键）、CUBE 多列与 GROUPING SETS
（空键单元格在结果行中保持其位置，排序时空键居末）、NULL 排序（ASC 时
NULLS LAST、DESC 时 NULLS FIRST，PG 默认语义）与 NULL 单元格
的协议空值显示、投影列序（SELECT 列表顺序而非表定义顺序，
含别名投影，重复列（SELECT a, b, a）按列表重复输出）、聚合输出上的 ORDER BY（按别名、聚合表达式或
分组列排序，含 DESC 与 NULL 语义）、INTERSECT/EXCEPT/UNION 集合运算
（尾部 ORDER BY/LIMIT 作用于整个集合结果，含 UNION ALL）、CAST 列头
按目标类型命名（float8/text/bpchar 等）与一元正负号投影
（SELECT -v，头为 ?column?），数值转整数按 PG 四舍五入，裸小数
字面量按 NUMERIC 精确运算、除法精确性判定与 select_div_scale 的
20 位宽结果规则（1.5/1、2.2/2 与 PG 逐位一致）、GROUP BY 选择
列表中的相关标量子查询（按分组值相关求值，列头取子查询自身的
输出列名）、EXISTS/NOT EXISTS 的多列相关（复合键半连接/反连
接，含相关列与内表同名的歧义消解）、混合等值/范围相关谓词的
EXISTS/NOT EXISTS 逐行求值、GROUP BY 选择列表中
"sum(id) + (select ...)" 聚合与相关标量子查询的算术组合
（逐组求值聚合与子查询后按 Numeric 精确运算，列头 ?column?）、SELECT 列表中普通列与算术表达式混合
（修复公共列序重排误删表达式列：重排仅在纯普通列查询执行）、字符串连接 ||（列/字面量/内联 ::text 转换/函数调用操作数，顶层切分保留嵌套括号）、函数调用与算术组合（coalesce(a,b)+1 等，顶层运算符切分、NULL 传播、数值字面量与一元符号处理、全 NULL 行经协议保留）、日期函数（EXTRACT year/month/day/dow/doy 的 FROM 参数形式与 NULL、日期 ± 整数天返回日期、日期 − 日期返回天数、DATE '...' 类型字面量）、区间运算与 age（date ± interval 'N day/week/month/year' 返回时间戳、age(t, s) 按 PG 格式 "N years M mons K days" 借位渲染、类型字面量参数）、LIKE/ILIKE 谓词（LIKE 区分大小写、ILIKE 折叠大小写、NOT 变体、下划线通配符）、LIKE ESCAPE 子句（normalize 阶段将 esc+X 编码为 0x01+字面量，匹配器解编码）、负整数（parseInt 支持符号、定长整数解码符号扩展、% 与 int/int 截断除法向零取整）、numeric 标度渲染（+− 取最大标度、* 标度相加、/ 精确十进制长除法 16 位小数、整数无小数点）、round/trunc（round 负数位、trunc(x)/trunc(x,n) 向零截断含负 n 位）、div(x,y) 整数商向零截断（mod 已对齐）、gcd/lcm（绝对值运算、lcm=|ab|/gcd）、width_bucket（trunc((op-lb)*cnt/(ub-lb))+1，越界 0/cnt+1）、类型化 numeric 转换（v::numeric(4,2) 重写为 cast、十进制半进位、按标度渲染）、left/right/repeat（负 n 分别去尾/去头、n=0 空串）、btrim/ltrim/rtrim（trim 字符集合、单参 btrim 去空白；lpad/rpad 已对齐）、strpos（1 基找不到 0）与 overlay（placing/from/for 语法重写为位置参数）、translate（from→to 逐字符映射，to 较短删除、to 较长忽略多余；字面量路径原本已通，列路径补路由+处理器）、to_char 数值模式（未用 9 位渲染空格、全零整数无 0 模式渲染空、0 位补零、G 千分位逗号、D 小数点、MI/PL/SG/PR/L 符号与货币模式）、to_number（符号/数字/单小数点提取，分组与模板字符跳过；求值器与引擎双路径）、to_date（YYYY/MM/DD 模式解析双路径，字面量分隔符逐字符对齐）、to_timestamp（YYYY/MM/DD+HH24/MI/SS 模式双路径，渲染 +00 时区）、日期 to_char（Mon/Day/Dy 模式；typed literal `date '...'` 在表达式解析前解包）、组合算术中的 typed cast（`v::numeric(4,2) + 1`；`::` 修饰符在求值器中规范化、嵌套 cast AS 形式在算术操作数中拆分，+/- 结果 scale 取操作数文本 scale 的最大值）、关键字 CAST（解析器 CAST(expr AS type) 类型名循环停在 `(`，修饰符正确落入 typeMods → numeric(p,s)/varchar(n) 精确求值）、to_char MI 符号位（前缀/后缀位置感知：负数减号、正数空格、FM 抑制空格；引擎列路径新增数字模板渲染；另修复 find_first_of 多字符常量 '90' 的老 bug → 改为字符串 "90"）、to_char S 符号位（`S` 与数字相邻、在填充区内：`S9999` -42 → `  -42`；`9999S` 后缀；FM 保留符号只去空格；`SG` 前缀仍走默认分支渲染 `- 482`）、to_char TH/V/RN 模板（TH 序数后缀随模板大小写：`9999TH`→`   42ND`、`9999th`→`    1st`；V 小数点移位 `9V99` 4.2→` 420`，溢出渲染 `###`；RN 罗马数字右对齐宽 15：`RN` 42→`           XLII`；求值器与引擎列路径双实现，列路径数字模板识别扩展到 RN）、to_char EEEE 科学计数法（有效位数 = 模板 9 数；PG 风格 `4.20e-03`，负号占符号槽、正数空槽、指数至少 2 位；双路径实现）、to_char 时间戳模板扩展（HH12/MI AM、HH24:MI:SS、Dy 组合已覆盖，新增 J 儒略日：`2026-08-15` → `2461268`，公式 jd = d + (153m+2)/5 + 365y + y/4 − y/100 + y/400 + 1721119，m 为从三月起的月序）、justify_hours/justify_days/justify_interval（双路径：求值器 lambda + 引擎处理器；`25 hours`→`1 day 01:00:00`、`70 days`→`2 mons 10 days`、混合符号 `-2 mons -3 days 04:05:06`→`-2 mons -2 days -19:54:54`；顺带修复 parseIntervalText 不支持负号分量 token、负数分量用复数 `days` 的 PG 规则）、trunc 数值渲染（单参整数值输出 `3` 而非 `3.000000`；双参裁剪尾零 `trunc(3.14159,2)`→`3.14`）、make_interval（命名参数 years/months/weeks/days/hours/mins/secs — 求值器读取 FunctionCallExpr::namedArgs，此前解析器产物从未被求值；`make_interval(days=>3, hours=>2)`→`3 days 02:00:00`）、overlay 关键字语法（`overlay(s placing r from st for n)`：列路径在 main.cpp 投影层重写为位置参数；无 FROM 标量路径在求值器 lambda 内过滤空关键字参数 — 该路径中 placing/from 产生空值参数、for 被丢弃，过滤后 [s,r,st,n] 正确计算；字面量与列双路径 PG 精确一致）、trim 关键字形式（`trim(both|leading|trailing [chars] from s)`：解析器在 parsePrimaryExpr 内把 both/leading/trailing 重写为带引号的方向字面量、from 重写为逗号（通过 const_cast 别名修改调用方 token 缓冲）；overlay 的 placing/from/for 与 extract 的 from 同机制归一为位置参数；求值器 trim lambda 识别方向字执行 both/leading/trailing 三种裁剪；顺带修复此前 overlay 解析器补丁因 const tokens 无法编译被静默丢弃的问题）、杂项算子（`position(x in y)`、`x SIMILAR TO y` 已有覆盖；新增正则匹配算子 `~`、`~*`、`!~`、`!~*`：tokenizer 支持 `!~*` 三字符折叠、parseComparisonExpr 接受这四个符号、求值器以 std::regex 判定并渲染 t/f；`timestamp AT TIME ZONE 'zone'`：naive 输入按该时区读取墙钟换算为 UTC 并追加 `+00` 后缀（shift 方向取负），parseTimeZoneOffset 增加常见命名时区固定偏移表如 Asia/Tokyo=+9、America/New_York=-5）、集合与数组算子（`substring(x from n [for m])` 关键字形式已有覆盖；新增 `(a,b) OVERLAPS (c,d)`：tokenizer 后处理将行值形式重写为 `overlaps(a,b,c,d)` 函数调用（修正后向括号扫描把行闭括号误计为嵌套的缺陷），求值器实现 PG 半开区间点/区间边界规则；数组 `@>`/`<@` 沿用现有包含判定，`&&` 重叠算子补全解析层（JSON 算子级接受）与求值（元素集合相交判定）；CLI 标量与列两路选择列表切分器及 splitTopLevelComma 均跟踪方括号深度，`array[1,2,3]` 不再被逗号拆裂——期间误改 VALUES 行解析器的括号分支已回滚）、数组函数与正则类（POSIX 字符类 `[[:alpha:]]`、`w` 转义在 `~` 算子下与 PG 一致；`array || array` 现按数组语义合并外层花括号（原为文本直连渲染 `{1,2}{3,4}`）；新增 `array_dims` 渲染 `[1:n]`，`array_position`/`array_length`/`array_upper`/`array_lower`/`string_to_array`/`array_to_string`/`cardinality` 一并纳入差分覆盖与 isScalarFunc 路由）、format 与引号折叠（发现并修复求值器 `unquote()` 不折叠 SQL 双写单引号的通用缺陷：`'it''s'` 此前求值为 `it''s`，现按 SQL 标准折叠为 `it's`，连带修正 `format('%L', ...)` 的双重转义；`regexp_replace` g 标志、`format` 的 %s/%I/%L/%% 均与 PG 逐字符一致）、文本函数与 regexp_matches（`initcap`/`ascii`/`chr`/`to_hex`/`octet_length`/`bit_length`/`md5`/`split_part` 越界空串/`substr` 负起点/`reverse`/`repeat`/`concat_ws` 已有覆盖；新增 `regexp_matches(text, pattern[, flags])`：std::regex 首个匹配按捕获组渲染 `{g1,g2}` 文本数组（无组则整匹配），flags 支持 i 忽略大小写并按被匹配文本原大小写输出，差分含 `{a,b}`、icase `{B}` 两类探针）、字符串边界与多字节（新增 utf8CharCount/utf8ByteAt 辅助：`left`/`right` 支持负偏移（丢弃对应端字符）且按字符而非字节计数，`position`/`strpos` 命中位置由字节偏移换算为字符序号，`héllo` 类多字节探针与 PG 一致；`regexp_split_to_array`、`starts_with` 已有覆盖）、数值舍入语义（PG numeric 为半上舍入、float8 为半偶舍入：修复 `divideByPowerOf10` 在全部数位被丢弃时直接归零不做进位的缺陷（`round(0.5::numeric)`→0）；`isNumericTypeName` 收窄为精确十进制类型、未定型小数字面量按 `numeric` 求值，float 走双精度路径 `std::nearbyint`（`round(2.5::float8)`→2）；ceil/floor 等一元数学函数整值结果渲染为裸整数而非 `-2.000000`）、power 的 numeric 呈现（PG `power(numeric,numeric)` 语义：两参均整且结果精确时渲染裸整数（`power(2,10)`→1024），否则按 16 位小数呈现并以 long double `powl` 复刻 PG 扩展精度 exp/ln 的第 16 位舍入（`power(2,0.5)`→1.4142135623730950、`power(2.5,2)`→6.2500000000000000，逐字符与 PG 一致）；`trunc` 保留位数、`mod`/`%` 负数取模语义已有覆盖）、exp/ln/log/sqrt 的 numeric 呈现（按 PG 各函数的显示标度逐一对齐：`exp` 整数入参 15 位小数、小数入参 16 位；`ln` 恒 16 位；`log`/`log10` 精确整值且整数入参时渲染裸整数（`log(100)`→2），否则 16 位（`log(2,64)`→6.0000000000000000）；`sqrt` 整数入参精确值渲染裸整数（`sqrt(4)`→2），其余 15 位（`sqrt(9.0)`→3.000000000000000）；数值一律以 long double 计算再按位取整，第 15/16 位与 PG 逐字符一致）、三角/双曲/浮点函数的 float8 最短表示（新增 `float8Text`：按 15→17 位有效数字尝试并回解析校验的最短往返渲染，接入 `sin/cos/tan/asin/acos/atan/atan2/cbrt/sinh/cosh/tanh/asinh/acosh/atanh/degrees/radians/cot/pi/pow`，与 PG 的 Ryu 风格 dtoa 逐字符一致，如 `sin(1)`→0.8414709848078965、`degrees(1)`→57.29577951308232、`pi()`→3.141592653589793）））））。运行：`python3 tests/compat/pg_diff_runner.py
[--only NAME]`（需要 docker 参考库）。

差分驱动已修复的语义：`CASE` 生成真正的 `CaseExpr`；`NULL AND/OR x`
三值逻辑；`IS [NOT] DISTINCT FROM`；一元负号保留整数类型（`-7/2 = -3`）；
`SUBSTRING(s FROM n FOR m)`/`TRIM(... FROM s)` 关键字调用语法；float8
字面量算术输出最短可回环十进制表示；`avg()` 用 numeric 精确除法（不再
丢失小数位，`avg(10.5)` 常量参数可求值）；numeric/decimal 列在聚合器中
按数值而非词法聚合；`CAST(expr AS type)` 前缀表达式解析（此前仅支持
`::`）；无 FROM 投影的 `AS` 别名只在顶层括号深度切分（`cast(1 as text)`
不再被截断）；int 转换按 PG 舍入（`3.7→4`，`-3.7→-4`，`true→1`）；
date 输出 ISO 零填充格式（`2020-01-02`）；聚合/函数投影列头按 PG 规则
命名（函数名或 `?column?`，防止多词表头撑爆协议列数）。

---

## 测试验证

完整测试套件运行方式：

```bash
./scripts/build.sh              # 编译生产二进制
./scripts/run_all_tests_fast.sh # 自包含地构建并安静运行统一回归：137 个 C++ 测试 + 2 个 E2E
./scripts/build_tests.sh        # 自包含地构建并运行完整输出的规范测试入口
```

`build_tests.sh` 是唯一负责生产二进制、生产对象编译、测试链接、桩对象选择和 E2E 调度的测试实现；即使项目根目录没有 `dbms_main`，它也会先通过共享构建逻辑生成当前二进制。`run_all_tests_fast.sh` 只是它的安静输出外壳，成功时输出计数，失败时保留完整诊断。上述 shell 入口共享 `scripts/build_common.sh` 的编译配置；`scripts/build_one_test.sh <test_name>` 可用于单测试增量编译，编译配置变化会自动使对象缓存失效。

每个 C++ 测试都在独立的临时工作目录中执行，测试结束后自动删除；因此 `.txnid`、WAL、catalog、日志和 `__t_*` 数据库不会跨测试共享。窗口函数 E2E 测试使用临时工作目录和临时管理员账号，结束后自动删除，不依赖或污染项目根目录。

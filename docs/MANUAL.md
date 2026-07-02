# DBMS 完整使用手册

> 最后更新: 2026-07-02
> 版本: Wave 4.x 全量完成
> 测试: PASS=109 FAIL=0 (`scripts/run_all_tests_fast.sh`)

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
mkdir -p build && cd build && cmake .. && make -j$(nproc)
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

-- 删除约束
ALTER TABLE users DROP CONSTRAINT CHK_AGE;
ALTER TABLE users DROP CONSTRAINT IF EXISTS CHK_AGE;

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
```

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

-- ON CONFLICT (UPSERT)
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

-- UPDATE LIMIT (MySQL 兼容)
UPDATE users SET AGE = 0 LIMIT 10;

-- RETURNING
UPDATE users SET AGE = 18 WHERE AGE < 18 RETURNING *;
```

### 删除

```sql
DELETE FROM users WHERE ID = 1;
DELETE FROM users WHERE AGE < 18;

-- DELETE USING
DELETE FROM users USING orders WHERE users.ID = orders.user_id;

-- DELETE LIMIT
DELETE FROM users WHERE AGE > 100 LIMIT 10;

-- RETURNING
DELETE FROM users WHERE ACTIVE = FALSE RETURNING ID;
```

### MERGE

```sql
MERGE INTO target USING source ON target.ID = source.ID
    WHEN MATCHED THEN UPDATE SET NAME = source.NAME
    WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (source.ID, source.NAME);
```

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
CREATE USER admin WITH PASSWORD 'admin' SUPERUSER;

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
```

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

## 18. 网络服务

```bash
# 启动服务端 (支持 TLS)
./dbms_main --server 9999

# 客户端连接
nc localhost 9999

# 连接监控
SHOW CONNECTIONS;
SHOW PROCESSLIST;
SHOW STATUS;
SHOW LOCKS;
SHOW DEADLOCKS;
```

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

-- BACKUP
BACKUP DATABASE mydb TO 'mydb.bak';
```

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

支持的认证方法: `trust`, `md5`, `scram-sha-256`, `password`, `ident`, `peer`, `cert`, `pam`, `ldap`, `radius`, `reject`。

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
-- 设置参数
SET search_path TO public, myschema;
SET timezone = 'Asia/Shanghai';
SET statement_timeout = 30000;

-- 重置
RESET search_path;
RESET ALL;

-- 查看
SHOW search_path;
SHOW ALL;

-- 事务隔离级别
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

-- ALTER SYSTEM (持久化)
ALTER SYSTEM SET work_mem = '64MB';
ALTER SYSTEM RESET work_mem;

-- AUTO_VACUUM
SET AUTO_VACUUM = ON;
SET AUTO_VACUUM_THRESHOLD = 1000;
```

---

## 测试验证

完整测试套件运行方式：

```bash
./scripts/build.sh              # 编译
./scripts/run_all_tests_fast.sh # 运行全部 109 个测试
```

每个测试函数执行完毕后自动清理数据库目录（`__t_*` 前缀），不影响项目根目录。

# 关系型数据库管理系统 (DBMS)

基于 C++17 实现的关系型数据库管理系统，支持标准 SQL 交互，具备页式存储、B+ 树索引、MVCC 事务、查询优化器、网络服务等生产级数据库核心功能。对标 PostgreSQL 级功能完整度。

## 功能特性

### 数据定义 (DDL)
- **数据库管理**：`CREATE DATABASE`, `DROP DATABASE`, `USE DATABASE`
- **表管理**：`CREATE TABLE`, `CREATE TEMPORARY TABLE`, `DROP TABLE`, `ALTER TABLE ADD/DROP/ALTER COLUMN`
- **视图**：`CREATE VIEW`, `CREATE MATERIALIZED VIEW`, `DROP VIEW`, `REFRESH MATERIALIZED VIEW`
- **索引**：`CREATE INDEX`, `CREATE UNIQUE INDEX`, `CREATE INDEX ... USING HASH`, `CREATE FULLTEXT INDEX`, `DROP INDEX`
  - 覆盖索引（`INCLUDE` 列）、部分索引（`WHERE` 条件）、复合索引
- **触发器**：`CREATE TRIGGER`（BEFORE/AFTER + INSERT/UPDATE/DELETE）
- **存储程序**：`CREATE PROCEDURE`, `CREATE FUNCTION`（UDF）, `CALL`
- **角色**：`CREATE ROLE`, `GRANT role TO user`, `REVOKE role FROM user`
- **分区表**：`CREATE TABLE ... PARTITION BY RANGE`
- **查看结构**：`VIEW TABLE`, `VIEW DATABASE`, `DESC`, `SHOW COLUMNS`, `SHOW CREATE TABLE`
- **数据类型**：`INT`, `SMALLINT`, `BIGINT`, `FLOAT`, `DOUBLE`, `MONEY`, `BOOL`, `CHAR(n)`, `VARCHAR(n)`, `NCHAR(n)`, `NVARCHAR(n)`, `DATE`, `TIME`, `DATETIME`, `TIMESTAMP`, `TIMESTAMPTZ`, `BLOB`, `TEXT`, `JSON`, `JSONB`, `ARRAY type`, `SERIAL`
- **约束支持**：主键 (Primary Key)、非空 (NOT NULL)、唯一 (UNIQUE)、外键 (Foreign Key) 含 `ON DELETE/UPDATE CASCADE/SET NULL/RESTRICT/DEFAULT/NO ACTION`、CHECK 约束、DEFAULT 默认值、AUTO_INCREMENT

### 数据操纵 (DML)
- **插入**：`INSERT INTO ... VALUES (...)`, `INSERT INTO ... SELECT ...`
- **替换**：`REPLACE INTO`（冲突时先删后插）
- **合并**：`MERGE INTO ... USING ... ON ... UPDATE SET ... INSERT ...`
- **Upsert**：`INSERT INTO ... VALUES ... ON CONFLICT (col) DO UPDATE SET ...`
- **查询**：`SELECT` 支持 `*`、指定列、`WHERE`、`ORDER BY`、`LIMIT`、`OFFSET`、`DISTINCT`
- **更新**：`UPDATE ... SET ... WHERE ...`, `UPDATE ... FROM ... WHERE ...`, `UPDATE ... LIMIT n`
- **删除**：`DELETE FROM ... WHERE ...`, `DELETE ... USING ... WHERE ...`, `DELETE ... LIMIT n`
- **多表更新/删除**：支持 `FROM` / `USING` 子句的跨表 UPDATE/DELETE

### 高级查询 (DQL)
- **条件过滤**：支持 `=`, `<>`, `!=`, `>`, `<`, `>=`, `<=`, `LIKE`, `BETWEEN`, `IN`, `EXISTS`, `ANY`, `ALL`, `IS NULL`, `IS NOT NULL` 以及 `AND`/`OR` 组合
- **三值逻辑**：`TRUE` / `FALSE` / `UNKNOWN`，WHERE 子句中 `UNKNOWN` 被当作 `FALSE`
- **排序**：`ORDER BY column [ASC|DESC]`，支持字符串、数值、日期类型
- **聚合函数**：`COUNT(*)`, `COUNT(DISTINCT ...)`, `MAX`, `MIN`, `SUM`, `AVG`
- **分组**：`GROUP BY ... HAVING ...`
- **表连接**：`INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `CROSS JOIN`
- **JOIN 算法选择**：NestedLoopJoin / HashJoin / MergeJoin，查询优化器根据统计信息自动选择
- **子查询**：支持 `IN`, `EXISTS`, `ANY`, `ALL` 子查询；支持标量子查询 `(SELECT ...)` 作为列表达式
- **联合**：`UNION`, `UNION ALL`
- **CTE**：`WITH cte_name AS (SELECT ...)` 公用表表达式
- **导出**：`SELECT ... INTO OUTFILE 'file.csv'`
- **执行计划**：`EXPLAIN SELECT ...`, `EXPLAIN ANALYZE SELECT ...`, `EXPLAIN FORMAT JSON SELECT ...`
- **窗口函数**：`ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LAG()`, `LEAD()`, `FIRST_VALUE()`, `LAST_VALUE()`, `NTILE()`, `PERCENT_RANK()`, `CUME_DIST()` 支持 `OVER (PARTITION BY ... ORDER BY ...)`
- **派生表**：`(SELECT ...) AS alias`
- **锁查询**：`FOR UPDATE`, `FOR SHARE`, `NOWAIT`, `SKIP LOCKED`

### 事务控制 (TCL) / MVCC
- `BEGIN [TRANSACTION] [ISOLATION LEVEL level]` — 开启事务，分配全局唯一事务 ID，创建 ReadView
- `COMMIT` — 提交事务，持久化到 WAL
- `ROLLBACK` — 基于 Undo Log 的增量回滚
- `SAVEPOINT spname` / `ROLLBACK TO SAVEPOINT spname` / `RELEASE SAVEPOINT spname`
- **MVCC 快照隔离**：每行带 16 字节 MVCC 头部（creatorTxnId + rollbackPtr），事务内读取基于 ReadView 的可见性规则，实现读写不阻塞
- **隔离级别**：支持 `READ UNCOMMITTED` / `READ COMMITTED` / `REPEATABLE READ` / `SERIALIZABLE`
- **全局事务 ID 生成器**：单调递增 64 位 txId，持久化到 `.txnid` 文件

### 索引
- **B+ 树主键索引**：磁盘页式存储（4096 字节/页），O(log n) 精确查找
- **B+ 树二级索引**：单列/多列二级索引，支持覆盖索引和部分索引
- **Hash 索引**：等值查询优化
- **Fulltext 索引**：文本全文检索
- **复合索引**：多列联合索引
- **唯一性约束**：通过 B+ 树自动检测重复主键/唯一键

### 查询优化器
- **执行计划树**：火山模型（TableScan, IndexScan, Filter, Project, Sort, Limit, Distinct, NestedLoopJoin, HashJoin, MergeJoin, Aggregate, GroupAggregate）
- **成本估计**：基于表统计信息估算各算子成本
- **索引选择**：自动选择 IndexScan 替代 TableScan
- **条件下推**：索引条件从 Filter 中移除避免重复过滤
- **JOIN 算法选择**：根据表大小和排序状态自动选择最优 JOIN 算法
- **查询计划缓存**：重复 SQL 自动复用执行计划

### 存储引擎
- **Slotted Page**：4096 字节页式存储，slot 数组管理记录位置
- **页分配器**：空闲页链表管理，支持页复用
- **Buffer Pool**：LRU 缓存，减少磁盘 I/O
- **页校验和**：Fletcher-16 校验，检测页损坏
- **WAL 日志**：Write-Ahead Logging 支持崩溃恢复
- **Checkpoint**：`CHECKPOINT` 命令刷盘所有脏页并截断 WAL，加速重启恢复
- **fsync 持久化**：WAL 写入、事务提交、Checkpoint 均调用 `fsync()` 保证数据落盘
- **VARCHAR 变长行**：`[定长数据 | 变长偏移数组 | 变长数据]` 格式，减少存储浪费
- **溢出页**：单行数据超过页空间时，大字段（TEXT/BLOB/JSON）自动存放到溢出页
- **MVCC 行格式**：每行开头 16 字节头部（8B creatorTxnId + 8B rollbackPtr）
- **统计信息**：`ANALYZE TABLE` 收集行数、列基数、最小/最大值、MCV（最常出现值）、多列统计
- **VACUUM**：`VACUUM [tablename]` 回收已删除行占用的空间，页压缩并归还空页
- **自动 VACUUM**：可配置阈值，死行数达到阈值时自动触发

### 权限管理
- 用户登录系统（用户名/密码存储于 `user.dat`）
- 管理员与普通用户权限区分
- `CREATE USER` 创建新用户，支持密码强度策略
- **表级权限**：`GRANT` / `REVOKE` SELECT/INSERT/UPDATE/DELETE/ALL
- **列级权限**：`GRANT SELECT (col1, col2) ON table TO user`
- **角色**：`CREATE ROLE`, `GRANT role TO user`, `REVOKE role FROM user`
- **审计日志**：可配置审计级别，记录 DDL/DML/全部操作

### JSONB 支持
- `JSON` / `JSONB` 数据类型，插入时自动验证 JSON 格式合法性
- **标量函数**：`JSONB_EXTRACT`, `JSONB_EXTRACT_TEXT`, `JSONB_CONTAINS`, `JSONB_EXISTS`, `JSONB_PRETTY`

### 临时表
- **CREATE TEMPORARY TABLE**：会话级临时表，自动覆盖同名永久表
- 临时表存储在会话中，断开连接后自动清理

### 网络服务
- **TCP 服务器**：`./dbms_main --server PORT` 启动服务端
- **TLS 加密**：自动检测/生成 TLS 证书，支持加密连接
- **多客户端**：每个连接独立线程，支持并发访问
- **会话隔离**：每个客户端连接拥有独立的 Session（用户名、权限、当前数据库、预编译语句、临时表），多客户端互不干扰
- **连接管理**：最大连接数限制（默认 64）
- **连接监控**：`SHOW CONNECTIONS`, `SHOW PROCESSLIST`, `SHOW STATUS`

### 数据导入导出
- **CSV 导入**：`LOAD DATA INFILE 'file.csv' INTO TABLE tname`
- **CSV 导出**：`SELECT ... INTO OUTFILE 'file.csv'`
- **数据库导出**：`DUMP DATABASE dbname TO 'file.sql'`
- **数据库恢复**：`RESTORE DATABASE dbname FROM 'file.sql'`
- **数据库备份**：`BACKUP DATABASE dbname TO 'file.bak'`

### 预编译语句
- `PREPARE stmt_name FROM 'SQL template'`
- `EXECUTE stmt_name USING (val1, val2, ...)` — `?` 占位符替换
- `DEALLOCATE PREPARE stmt_name`

### 锁与并发
- **多级锁**：表级共享锁/排他锁、行级锁、Gap 锁、页级锁
- **死锁检测**：等待图（Wait-for Graph）检测并打破死锁
- **死锁日志**：`SHOW DEADLOCKS` 查看历史死锁记录
- **锁监控**：`SHOW LOCKS` 查看当前锁持有和等待情况

## 编译与运行

### 环境要求
- GCC / Clang 支持 C++17
- Linux 环境
- POSIX 线程支持

### 编译

#### 方式一：CMake（推荐）
```bash
mkdir build && cd build
cmake ..
make -j$(nproc)
```

#### 方式二：自动构建脚本（自动检测 OpenSSL）
```bash
./scripts/build.sh
```

#### 方式三：手动 g++
```bash
g++ -std=c++17 -O2 -pthread \
    src/main.cpp \
    src/commands/TableManage.cpp \
    src/optimizer/ExecutionPlan.cpp \
    src/storage/BufferPool.cpp \
    src/storage/PageAllocator.cpp \
    src/storage/Page.cpp \
    src/storage/PgPage.cpp \
    src/storage/PageWrapper.cpp \
    src/storage/FreeSpaceMap.cpp \
    src/storage/VisibilityMap.cpp \
    src/access/BPTree.cpp \
    src/access/HashIndex.cpp \
    src/access/SPGiSTIndex.cpp \
    src/transaction/LockManager.cpp \
    src/transaction/TxnIdGenerator.cpp \
    src/network/NetworkServer.cpp \
    src/network/TLSWrapper_stub.cpp \
    src/common/Config.cpp \
    -I src -I src/common -I src/storage -I src/access \
    -I src/transaction -I src/network -I src/utils \
    -I src/optimizer -I src/commands -I src/interfaces \
    -o dbms_main
```

> **TLS 说明**：若系统已安装 OpenSSL 开发库（`libssl-dev`），CMake 和 `build.sh` 会自动启用真实 TLS 支持；否则回退到明文 TCP（stub 实现）。

### 交互式运行
```bash
./dbms_main
```
启动后输入用户名和密码登录（默认管理员账户在 `user.dat` 中配置）。

### 网络服务模式
```bash
# 服务端
./dbms_main --server 9999

# 客户端（使用 netcat 或 telnet）
nc localhost 9999
```

## SQL 语法示例

### 数据库操作
```sql
create database shopdb
use database shopdb
drop database shopdb
```

### 建表
```sql
create table users (
    id int not null primary key auto_increment,
    name varchar(50) not null,
    email varchar(100) unique,
    age int default 0,
    score int check (score >= 0 and score <= 100),
    created_at timestamp
);

-- 花括号格式（旧版兼容）
create table users {id:int 0 PK, name:char20 0, age:int 1, score:int 0}
```

### 外键约束
```sql
create table orders (
    order_id int primary key,
    user_id int not null,
    amount double,
    foreign key (user_id) references users(id)
        on delete cascade on update cascade
);
```

### 分区表
```sql
create table events (
    id int primary key,
    event_time timestamp
) partition by range(event_time) (
    partition p1 values less than ('2024-01-01'),
    partition p2 values less than ('2025-01-01')
);
```

### 插入数据
```sql
insert into users (id, name, age, score) values (1, 'Alice', 25, 85);
insert into users (id, name, score) values (2, 'Bob', 72);
insert into orders (order_id, user_id, amount) values (101, 1, 199.99);
```

### JSONB
```sql
create table configs (id int primary key, settings jsonb);
insert into configs (id, settings) values (1, '{"theme":"dark","notifications":true}');
select jsonb_extract_text(settings, '$.theme') from configs;
select jsonb_pretty(settings) from configs;
```

### 查询
```sql
select * from users;
select name, score from users where score >= 80;
select * from users where score > 70 and age > 20;
select * from users where name like 'A%';
select * from users order by score desc;
select * from users limit 10 offset 5;
select distinct age from users;
select count(*), max(score), min(score), sum(score), avg(score) from users;
```

### JOIN 查询
```sql
select users.name, orders.amount from users join orders on users.id = orders.user_id;
select * from users left join orders on users.id = orders.user_id;
select a.name, b.name as referrer from users a left join users b on a.referral_id = b.id;
```

### GROUP BY
```sql
select age, count(*) from users group by age;
select age, count(*) from users group by age having count(*) > 1;
```

### UNION
```sql
select name from users where status = 1
union
select name from archived_users where status = 1;
```

### CTE
```sql
with vip_users as (select id, name from users where vip = 1)
select vip_users.name, orders.amount from vip_users join orders on vip_users.id = orders.user_id;
```

### 窗口函数
```sql
select name, row_number() over (order by score) from users;
select name, rank() over (order by score desc) from users;
select name, lag(score, 1, 0) over (order by id) from users;
select dept, name, sum(salary) over (partition by dept) from employees;
```

### 视图
```sql
create view adult_users as select * from users where age >= 18;
select * from adult_users;
drop view adult_users;
```

### 物化视图
```sql
create materialized view daily_sales as
select date(created_at) as sale_date, sum(amount) as total from orders group by date(created_at);
refresh materialized view daily_sales;
```

### 索引
```sql
create index idx_name on users(name);
create unique index idx_email on users(email);
create index idx_hash on users(id) using hash;
create index idx_cover on users(name) include (email, age);
create index idx_partial on orders(amount) where amount > 100;
drop index idx_name on users;
```

### 执行计划
```sql
analyze table users;
explain select * from users where id = 1;
explain analyze select * from users join orders on users.id = orders.user_id;
```

### 更新与删除
```sql
update users set score = 95 where id = 1;
update users set status = 0 where age > 60;
update orders set amount = amount * 1.1 from users where orders.user_id = users.id and users.vip = 1;
delete from users where id = 1;
delete from orders where amount < 10;
```

### MERGE INTO
```sql
merge into users using temp_users on users.id = temp_users.id
update set name = temp_users.name, email = temp_users.email
insert (id, name, email) values (temp_users.id, temp_users.name, temp_users.email);
```

### 事务
```sql
begin;
insert into users (id, name, age, score) values (5, 'frank', 35, 55);
savepoint sp1;
update users set age = 99 where id = 1;
rollback to savepoint sp1;
commit;
```

### 多事务隔离测试
```sql
-- 连接 A
begin;
insert into users (id, name) values (10, 'Tom');
-- 不提交

-- 连接 B
select * from users where id = 10;  -- 看不到（未提交隔离）

-- 连接 A
commit;

-- 连接 B
select * from users where id = 10;  -- 现在能看到
```

### Checkpoint
```sql
checkpoint;
```

### VACUUM
```sql
vacuum users;
vacuum;
```

### 用户管理
```sql
create user eve secret123 0;
create user admin2 pass123 1;
create role readonly;
```

### 权限管理
```sql
grant select on users to eve;
grant select, insert on orders to eve;
grant select (name, email) on users to reporter;
grant all on orders to eve;
revoke insert on users from eve;
grant readonly to eve;
revoke readonly from eve;
```

### CSV 导入导出
```sql
load data infile '/tmp/users.csv' into table users;
select * from users into outfile '/tmp/export.csv';
```

### 预编译语句
```sql
prepare get_user from 'select * from users where id = ?';
execute get_user using (1);
deallocate prepare get_user;
```

### 连接监控
```sql
show connections;
show processlist;
show status;
show slow log;
show locks;
show deadlocks;
```

## 项目结构

```
.
├── main.cpp                 # SQL 交互入口、命令解析、网络服务启动
├── TableManage.h            # 存储引擎头文件
├── TableManage.cpp          # 存储引擎实现（DDL/DML/索引/事务/权限/统计/MVCC/Checkpoint）
├── ExecutionPlan.h          # 查询执行计划框架（火山模型算子）
├── ExecutionPlan.cpp        # 查询优化器与 EXPLAIN，JOIN 算法选择
├── Page.h                   # Slotted Page 数据结构
├── Page.cpp                 # 页操作（插入/删除/更新/压缩/校验和）
├── PageAllocator.h          # 页分配管理器
├── PageAllocator.cpp        # 页分配与空闲链表
├── BufferPool.h             # LRU 缓冲池
├── BufferPool.cpp           # 缓冲池实现（含 fsync）
├── BPTree.h                 # B+ 树索引头文件
├── BPTree.cpp               # B+ 树索引实现
├── HashIndex.h              # Hash 索引头文件
├── HashIndex.cpp            # Hash 索引实现
├── LockManager.h            # 锁管理器头文件
├── LockManager.cpp          # 读写锁、行锁、死锁检测
├── NetworkServer.h          # TCP 网络服务头文件
├── NetworkServer.cpp        # 网络服务实现（TLS + 独立 Session）
├── TLSWrapper.h             # TLS 加密包装器
├── TLSWrapper.cpp           # TLS 上下文与握手实现
├── TxnIdGenerator.h         # 全局事务 ID 生成器头文件
├── TxnIdGenerator.cpp       # 全局事务 ID 生成器（持久化、线程安全）
├── Config.h                 # 运行时配置头文件
├── Config.cpp               # 配置加载与保存
├── Session.h                # 会话上下文结构体
├── DateType.h               # 日期类型定义与运算
├── logs.h                   # 操作日志记录
├── permissions.h            # 用户认证与权限查询
├── sha256.h                 # SHA-256 哈希实现
├── set.h                    # 集合数据结构
├── user.dat                 # 用户账号数据
├── dbms.log                 # 运行日志
├── slow_query.log           # 慢查询日志
└── docs/
    ├── commandsList.md      # SQL 命令完整参考手册（含示例和参数说明）
    └── feature-gap-analysis.md  # 功能差距分析
```

## 存储格式

每个数据库为一个独立目录：

```
dbname/
├── tlist.lst               # 表名列表
├── .stats                  # 表统计信息（行数、列基数、最小/最大值、MCV）
├── .permissions            # 表级权限数据
├── .views/                 # 视图定义目录
│   └── viewname.view
├── .triggers/              # 触发器定义目录
│   └── triggername.trg
├── .procedures/            # 存储过程目录
│   └── procname.proc
├── .udfs/                  # 用户定义函数目录
│   └── funcname.udf
├── .roles                  # 角色数据
├── .materialized_views/    # 物化视图数据
│   └── mvname/
├── checkpoint              # Checkpoint 记录
├── wal                     # Write-Ahead Log（事务日志）
├── .txnid                  # 全局事务 ID 持久化文件
├── tablename.stc           # 表结构（二进制）
├── tablename.dt            # 表数据（Slotted Page，4096 字节/页）
├── tablename.idx           # B+ 树主键索引（4096 字节/页）
├── tablename.colname.idx   # 二级索引
├── tablename.secidx        # 二级索引元数据
└── tablename.hashidx       # Hash 索引
```

## 页格式

数据页（4096 字节）：

```
+--------------+------------------+-------------+---------------+--------+
| Header(20B)  | Slot Array       | Free Space  | Record Data   |        |
|              | (grows down)     |             | (grows up)    |        |
+--------------+------------------+-------------+---------------+--------+
0              sizeof(Header)     freeOffset    dataOffset      PAGE_SIZE

Header:
  - pageId (4B): 页号
  - numSlots (2B): slot 数量
  - freeOffset (2B): 空闲空间起始偏移
  - dataOffset (2B): 数据区起始偏移
  - checksum (2B): Fletcher-16 页校验和
  - nextPage (4B): 空闲页链表指针
```

## 行格式

### 定长行

```
+-------------+---------------------------+
| MVCC Header | Fixed Data                |
| (16 bytes)  |                           |
+-------------+---------------------------+

MVCC Header:
  - creatorTxnId (8B): 创建此行的事务 ID
  - rollbackPtr  (8B): 指向旧版本（Undo Log），0 表示无旧版本
```

### 变长行（VARCHAR / TEXT / BLOB / JSON）

```
+-------------+-------------+------------------+----------------+
| MVCC Header | Fixed Data  | Var Offset Array | Var Data       |
| (16 bytes)  |             | (4 bytes/var col)|                |
+-------------+-------------+------------------+----------------+

Var Offset Array 每项 (4 bytes):
  - dataOffset (2B): 变长数据起始偏移（相对行数据起始）
  - dataLen    (2B): 变长数据长度
```

## 已知限制

- **数据存储为小写**：所有字符串值在存储时会被转换为小写（通过 `toLower()` 预处理），`SELECT 'Hello' → 'hello'`
- **标量函数不支持独立 SELECT**：`SELECT upper('hello')` 会报语法错误，需用于表列：`SELECT upper(name) FROM users`
- **`SAVEPOINT` 需要在事务内**：`SAVEPOINT` 命令必须在 `BEGIN` 之后执行，否则返回 "Not in transaction"
- **`CREATE HASH INDEX`** ✅ 已实现：`CREATE HASH INDEX name ON table(col)` 创建哈希索引，支持 O(1) 等值查询
- **`SHOW USERS` / `SHOW ROLES`** ✅ 已实现：`SHOW USERS` 读取 `user.dat` 展示所有用户及权限级别，`SHOW ROLES` 读取 `role.dat` 展示所有角色名称（需 admin 权限）

## 参考项目

- [hyrise/sql-parser](https://github.com/hyrise/sql-parser) — SQL Parser for C++
- [zcbenz/BPlusTree](https://github.com/zcbenz/BPlusTree) — B+ tree implementation which stores data in file
- [Jefung/simple_DBMS](https://github.com/Jefung/simple_DBMS) — C++ 实现简单数据库引擎
- [niteshkumartiwari/B-Plus-Tree](https://github.com/niteshkumartiwari/B-Plus-Tree) — Mini Database System using B+ Tree in C++

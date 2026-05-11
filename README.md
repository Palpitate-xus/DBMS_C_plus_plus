# 简易数据库管理系统 (Simple DBMS)

基于 C++17 实现的关系型数据库管理系统，支持标准 SQL 交互，具备页式存储、B+ 树索引、MVCC 事务、查询优化器、网络服务等生产级数据库核心功能。

## 功能特性

### 数据定义 (DDL)
- **数据库管理**：`CREATE DATABASE`, `DROP DATABASE`, `USE DATABASE`
- **表管理**：`CREATE TABLE`, `DROP TABLE`, `ALTER TABLE ADD/DROP COLUMN`
- **视图**：`CREATE VIEW`, `DROP VIEW`
- **索引**：`CREATE INDEX`, `DROP INDEX`（二级索引）
- **查看结构**：`VIEW TABLE`, `VIEW DATABASE`
- **数据类型**：`INT`, `TINYINT`, `LONG`, `CHAR(n)`, `VARCHAR(n)`, `DATE`
- **约束支持**：主键 (Primary Key)、非空 (NOT NULL)、唯一 (UNIQUE)、外键 (Foreign Key)、CHECK 约束、DEFAULT 默认值、SERIAL 自增

### 数据操纵 (DML)
- **插入**：`INSERT INTO ... VALUES (...)`
- **查询**：`SELECT` 支持 `*`、指定列、WHERE、ORDER BY、LIMIT、OFFSET、DISTINCT
- **更新**：`UPDATE ... SET ... WHERE ...`
- **删除**：`DELETE FROM ... WHERE ...`

### 高级查询
- **条件过滤**：支持 `=`, `>`, `<`, `>=`, `<=`, `!=`, `LIKE` 以及 `AND`/`OR` 组合
- **排序**：`ORDER BY column [ASC|DESC]`，支持字符串、整数、日期类型
- **聚合函数**：`COUNT(*)`, `MAX`, `MIN`, `SUM`, `AVG`
- **分组**：`GROUP BY ... HAVING ...`
- **表连接**：`INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`
- **JOIN 算法选择**：NestedLoopJoin / HashJoin / MergeJoin，查询优化器根据统计信息自动选择
- **子查询**：支持 `IN`, `EXISTS`, `ANY`, `ALL` 子查询
- **联合**：`UNION`, `UNION ALL`
- **执行计划**：`EXPLAIN SELECT ...` 带成本估计
- **窗口函数**：`ROW_NUMBER()`, `RANK()`, `LAG()`, `LEAD()` 支持 `OVER (ORDER BY ...)`

### 事务控制 (TCL) / MVCC
- `BEGIN` — 开启事务，分配全局唯一事务 ID，创建 ReadView
- `COMMIT` — 提交事务，持久化到 WAL
- `ROLLBACK` — 基于 Undo Log 的增量回滚
- **MVCC 快照隔离**：每行带 16 字节 MVCC 头部（creatorTxnId + rollbackPtr），事务内读取基于 ReadView 的可见性规则，实现读写不阻塞
- **隔离级别**：支持 READ UNCOMMITTED / READ COMMITTED / REPEATABLE READ / SERIALIZABLE
- **全局事务 ID 生成器**：单调递增 64 位 txId，持久化到 `.txnid` 文件

### 索引
- **B+ 树主键索引**：磁盘页式存储（4096 字节/页），O(log n) 精确查找
- **二级索引**：B+ 树非主键索引，支持多值索引
- **唯一性约束**：通过 B+ 树自动检测重复主键

### 查询优化器
- **执行计划树**：火山模型（TableScan, IndexScan, Filter, Project, Sort, Limit, Distinct, NestedLoopJoin, Aggregate）
- **成本估计**：基于表统计信息估算各算子成本
- **索引选择**：自动选择 IndexScan 替代 TableScan
- **条件下推**：索引条件从 Filter 中移除避免重复过滤

### 存储引擎
- **Slotted Page**：4096 字节页式存储，slot 数组管理记录位置
- **页分配器**：空闲页链表管理，支持页复用
- **Buffer Pool**：LRU 缓存，减少磁盘 I/O
- **页校验和**：Fletcher-16 校验，检测页损坏
- **WAL 日志**：Write-Ahead Logging 支持崩溃恢复
- **Checkpoint**：`CHECKPOINT` 命令刷盘所有脏页并截断 WAL，加速重启恢复
- **fsync 持久化**：WAL 写入、事务提交、Checkpoint 均调用 `fsync()` 保证数据落盘
- **VARCHAR 变长行**：`[定长数据 | 变长偏移数组 | 变长数据]` 格式，减少存储浪费
- **MVCC 行格式**：每行开头 16 字节头部（8B creatorTxnId + 8B rollbackPtr）
- **统计信息**：`ANALYZE TABLE` 收集行数、列基数、最小/最大值
- **VACUUM**：`VACUUM [tablename]` 回收已删除行占用的空间，页压缩并归还空页

### 权限管理
- 用户登录系统（用户名/密码存储于 `user.dat`）
- 管理员与普通用户权限区分
- `CREATE USER` 创建新用户
- **表级权限**：`GRANT` / `REVOKE` SELECT/INSERT/UPDATE/DELETE/ALL

### 临时表
- **CREATE TEMPORARY TABLE**：会话级临时表，自动覆盖同名永久表
- **DROP TEMPORARY TABLE**：删除会话临时表，恢复访问永久表
- 临时表存储在会话中，断开连接后自动清理

### 网络服务
- **TCP 服务器**：`./dbms_main --server PORT` 启动服务端
- **多客户端**：每个连接独立线程，支持并发访问
- **会话隔离**：每个客户端连接拥有独立的 Session（用户名、权限、当前数据库、预编译语句、临时表），多客户端互不干扰
- **连接管理**：最大连接数限制（默认 64）
- **连接监控**：`SHOW CONNECTIONS`, `SHOW STATUS`

### 数据导入导出
- **CSV 导入**：`LOAD DATA INFILE 'file.csv' INTO TABLE tname`
- **CSV 导出**：`SELECT ... INTO OUTFILE 'file.csv'`

### 测试脚本
- **SQL 集成测试**：`./test_sql.sh` — 12 项 SQL 语句级测试（CREATE/INSERT/SELECT/UPDATE/DELETE/JOIN/事务/Checkpoint/索引等）
- **API 功能测试**：`./test_all.sh` — 6 项 C++ API 级测试（VARCHAR/JOIN/MVCC/Checkpoint/聚合/索引）

### 窗口函数
```sql
select name, row_number() over (order by score) from users
select name, rank() over (order by score desc) from users
select name, lag(score) over (order by id) from users
select name, lead(score) over (order by id) from users
```

### 临时表
```sql
create temporary table temp_users (id int not null primary key, name varchar 50)
insert into temp_users (id, name) values (1, 'Temp')
select * from temp_users
drop temporary table temp_users
```

### SERIAL 自增
```sql
create table users (id serial primary key, name varchar 50)
insert into users (name) values ('Alice')   -- id 自动分配为 1
insert into users (name) values ('Bob')     -- id 自动分配为 2
```

### CHECK 约束
```sql
create table users (id int primary key, score int check (score >= 0 and score <= 100))
insert into users (id, score) values (1, 120)  -- 拒绝：违反 CHECK 约束
```

### VACUUM
```sql
vacuum users    -- 回收 users 表的已删除行空间
vacuum          -- 回收当前数据库所有表的空间
```

### 预编译语句
- `PREPARE stmt_name FROM 'SQL template'`
- `EXECUTE stmt_name USING (val1, val2, ...)` — `?` 占位符替换
- `DEALLOCATE PREPARE stmt_name`

### 锁与并发
- **读写锁**：表级共享锁/排他锁
- **死锁检测**：等待图（Wait-for Graph）检测并打破死锁

## 编译与运行

### 环境要求
- GCC / Clang 支持 C++17
- Linux 环境
- POSIX 线程支持

### 编译
```bash
g++ -std=c++17 -O2 -pthread main.cpp TableManage.cpp ExecutionPlan.cpp BufferPool.cpp \
    PageAllocator.cpp Page.cpp BPTree.cpp LockManager.cpp NetworkServer.cpp \
    TxnIdGenerator.cpp -o dbms_main
```

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
create database testdb
use database testdb
drop database testdb
```

### 建表
```sql
create table users {id:int 0 PK, name:char20 0, age:int 1, score:int 0}
-- 语法：列名:类型 是否可空(0=NOT NULL, 1=Nullable) [PK]

-- 也支持括号格式
create table users ( id int not null primary key, name varchar 20 not null, age int )
```

### 插入数据
```sql
insert into users (id, name, age, score) values (1, 'alice', 25, 85)
insert into users (id, name, score) values (2, 'bob', 72)
```

### 查询
```sql
select * from users
select name, score from users where score >= 80
select * from users where score > 70 and age > 20
select * from users where name like 'a%'
select * from users order by score desc
select * from users limit 10 offset 5
select distinct age from users
select count(*), max(score), min(score), sum(score), avg(score) from users
```

### JOIN 查询
```sql
create table orders {oid:int 0 PK, uid:int 0, amount:int 0}
select * from users join orders on users.id = orders.uid
select * from users left join orders on users.id = orders.uid
select users.name, orders.amount from users join orders on users.id = orders.uid where orders.amount > 40
```

### GROUP BY
```sql
select age, count(*) from users group by age
select age, count(*) from users group by age having count(*) > 1
```

### UNION
```sql
select name from users union select name from admins
```

### 视图
```sql
create view adult_users as select * from users where age >= 18
select * from adult_users
drop view adult_users
```

### 索引
```sql
create index idx_name on users(name)
drop index idx_name on users
```

### 执行计划
```sql
analyze table users
explain select * from users where id = 1
```

### 更新与删除
```sql
update users set score=95 where id=1
delete from users where score < 80
```

### 事务
```sql
begin
insert into users (id, name, age, score) values (5, 'frank', 35, 55)
rollback        -- 基于 Undo Log 的增量回滚
-- 或
commit          -- 提交
```

### Checkpoint
```sql
checkpoint      -- 刷盘所有脏页，截断 WAL
```

### 用户管理
```sql
create user eve eve 0     -- 用户名 密码 权限(0=普通, 1=admin)
```

### 表级权限
```sql
grant select on users to eve
grant all on orders to eve
revoke insert on users from eve
```

### CSV 导入导出
```sql
load data infile '/tmp/users.csv' into table users
select * from users into outfile '/tmp/export.csv'
```

### 预编译语句
```sql
prepare get_user from 'select * from users where id = ?'
execute get_user using (1)
deallocate prepare get_user
```

### 连接监控
```sql
show connections
show status
```

## 项目结构

```
.
├── main.cpp              # SQL 交互入口、命令解析、网络服务启动
├── TableManage.h         # 存储引擎头文件
├── TableManage.cpp       # 存储引擎实现（DDL/DML/索引/事务/权限/统计/MVCC/Checkpoint）
├── ExecutionPlan.h       # 查询执行计划框架（火山模型算子）
├── ExecutionPlan.cpp     # 查询优化器与 EXPLAIN，JOIN 算法选择
├── Page.h                # Slotted Page 数据结构
├── Page.cpp              # 页操作（插入/删除/更新/压缩/校验和）
├── PageAllocator.h       # 页分配管理器
├── PageAllocator.cpp     # 页分配与空闲链表
├── BufferPool.h          # LRU 缓冲池
├── BufferPool.cpp        # 缓冲池实现（含 fsync）
├── BPTree.h              # B+ 树索引头文件
├── BPTree.cpp            # B+ 树索引实现
├── LockManager.h         # 锁管理器头文件
├── LockManager.cpp       # 读写锁与死锁检测
├── NetworkServer.h       # TCP 网络服务头文件
├── NetworkServer.cpp     # 网络服务实现（独立 Session）
├── TxnIdGenerator.h      # 全局事务 ID 生成器头文件
├── TxnIdGenerator.cpp    # 全局事务 ID 生成器（持久化、线程安全）
├── Session.h             # 会话上下文结构体
├── DateType.h            # 日期类型定义与运算
├── logs.h                # 操作日志记录
├── permissions.h         # 用户认证与权限查询
├── test_sql.sh           # SQL 集成测试脚本
├── test_all.sh           # API 功能测试脚本
├── user.dat              # 用户账号数据
├── dbms.log              # 运行日志
└── slow_query.log        # 慢查询日志
```

## 存储格式

每个数据库为一个独立目录：

```
dbname/
├── tlist.lst            # 表名列表
├── .stats               # 表统计信息（行数、列基数、最小/最大值）
├── .permissions         # 表级权限数据
├── .views/              # 视图定义目录
│   └── viewname.view
├── checkpoint           # Checkpoint 记录（timestamp + maxCommittedTxId）
├── wal                  # Write-Ahead Log（事务日志）
├── tablename.stc        # 表结构（二进制）
├── tablename.dt         # 表数据（Slotted Page，4096 字节/页）
├── tablename.idx        # B+ 树主键索引（4096 字节/页）
├── tablename.colname.idx   # 二级索引
└── tablename.secidx     # 二级索引元数据
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

### 变长行（VARCHAR）

```
+-------------+-------------+------------------+----------------+
| MVCC Header | Fixed Data  | Var Offset Array | Var Data       |
| (16 bytes)  |             | (4 bytes/var col)|                |
+-------------+-------------+------------------+----------------+

Var Offset Array 每项 (4 bytes):
  - dataOffset (2B): 变长数据起始偏移（相对行数据起始）
  - dataLen    (2B): 变长数据长度
```

## 参考项目

- [hyrise/sql-parser](https://github.com/hyrise/sql-parser) — SQL Parser for C++
- [zcbenz/BPlusTree](https://github.com/zcbenz/BPlusTree) — B+ tree implementation which stores data in file
- [Jefung/simple_DBMS](https://github.com/Jefung/simple_DBMS) — C++ 实现简单数据库引擎
- [niteshkumartiwari/B-Plus-Tree](https://github.com/niteshkumartiwari/B-Plus-Tree) — Mini Database System using B+ Tree in C++

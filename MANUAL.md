# DBMS 完整使用手册

## 目录

1. [快速开始](#1-快速开始)
2. [SQL 命令参考](#2-sql-命令参考)
3. [数据类型](#3-数据类型)
4. [DDL — 数据定义](#4-ddl--数据定义)
5. [DML — 数据操纵](#5-dml--数据操纵)
6. [DQL — 高级查询](#6-dql--高级查询)
7. [TCL — 事务控制](#7-tcl--事务控制)
8. [DCL — 权限控制](#8-dcl--权限控制)
9. [系统命令](#9-系统命令)
10. [存储与文件格式](#10-存储与文件格式)
11. [架构说明](#11-架构说明)

---

## 1. 快速开始

### 1.1 编译

```bash
g++ -std=c++17 -O2 -pthread main.cpp TableManage.cpp ExecutionPlan.cpp BufferPool.cpp \
    PageAllocator.cpp Page.cpp BPTree.cpp LockManager.cpp NetworkServer.cpp \
    TxnIdGenerator.cpp -o dbms_main
```

### 1.2 启动交互模式

```bash
./dbms_main
```

输入用户名密码登录（默认在 `user.dat` 中配置）。

### 1.3 启动网络服务

```bash
./dbms_main --server 9999
```

客户端用 `nc localhost 9999` 连接。

### 1.4 初始化管理员账号

创建 `user.dat`：

```
admin admin admin
```

格式：`username password permission`
- `permission = admin` — 管理员（所有权限）
- `permission = 0` — 普通用户

---

## 2. SQL 命令参考

### 命令总览

| 类别 | 命令 |
|------|------|
| DDL | `CREATE DATABASE`, `DROP DATABASE`, `USE DATABASE`, `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE`, `CREATE VIEW`, `DROP VIEW`, `CREATE INDEX`, `DROP INDEX` |
| DML | `INSERT INTO`, `SELECT`, `UPDATE`, `DELETE FROM` |
| DQL | `JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `UNION`, `UNION ALL`, `GROUP BY`, `HAVING`, `LIMIT`, `OFFSET`, `DISTINCT`, `EXPLAIN` |
| TCL | `BEGIN`, `COMMIT`, `ROLLBACK`, `CHECKPOINT`, `VACUUM` |
| DCL | `CREATE USER`, `GRANT`, `REVOKE` |
| 系统 | `ANALYZE TABLE`, `SHOW CONNECTIONS`, `SHOW STATUS`, `PREPARE`, `EXECUTE`, `DEALLOCATE PREPARE`, `LOAD DATA INFILE`, `SELECT ... INTO OUTFILE` |

---

## 3. 数据类型

| 类型 | 说明 | 存储大小 | 示例 |
|------|------|---------|------|
| `INT` | 32位有符号整数 | 4 字节 | `123`, `-456` |
| `TINYINT` | 8位有符号整数 | 1 字节 | `1`, `0` |
| `LONG` | 64位有符号整数 | 8 字节 | `9999999999` |
| `CHAR(n)` | 定长字符串，空格填充 | n 字节 | `'hello'` |
| `VARCHAR(n)` | 变长字符串，最大 n 字节 | 实际长度 + 4 字节偏移 | `'hello'` |
| `DATE` | 日期（YYYY-MM-DD） | 12 字节 | `'2024-01-15'` |

---

## 4. DDL — 数据定义

### 4.1 CREATE DATABASE

```sql
create database dbname
```

创建新数据库。数据库以目录形式存储在当前工作目录下。

**示例：**
```sql
create database testdb
```

---

### 4.2 DROP DATABASE

```sql
drop database dbname
```

删除数据库及其所有表、索引、视图。

**示例：**
```sql
drop database testdb
```

---

### 4.3 USE DATABASE

```sql
use database dbname
```

切换当前会话的数据库。

**示例：**
```sql
use database testdb
```

---

### 4.4 CREATE TABLE

支持两种语法格式。

#### 括号格式（推荐）

```sql
create table tablename ( colname type [constraints], ... )
```

**参数：**
- `colname` — 列名
- `type` — `int`, `tinyint`, `long`, `char n`, `varchar n`, `date`
- `constraints` — 可选：`not null`, `primary key`, `unique`, `default 'value'`

**示例：**
```sql
create table users (
    id int not null primary key,
    name varchar 50 not null,
    email varchar 100 unique,
    age int default 18,
    score int
)
```

#### 花括号格式（旧版兼容）

```sql
create table tablename {col:type flags [PK], ...}
```

**参数：**
- `flags` — `0` = NOT NULL, `1` = nullable
- `PK` — 可选，标记为主键

**示例：**
```sql
create table users {id:int 0 PK, name:char20 0, age:int 1, score:int 0}
```

---

### 4.5 DROP TABLE

```sql
drop table tablename
```

**示例：**
```sql
drop table users
```

---

### 4.6 ALTER TABLE

#### ADD COLUMN

```sql
alter table tablename add column colname type [constraints]
```

**示例：**
```sql
alter table users add column phone char 20
```

#### DROP COLUMN

```sql
alter table tablename drop column colname
```

**示例：**
```sql
alter table users drop column phone
```

---

### 4.7 CREATE VIEW

```sql
create view viewname as select ...
```

**示例：**
```sql
create view adult_users as select * from users where age >= 18
```

---

### 4.8 DROP VIEW

```sql
drop view viewname
```

**示例：**
```sql
drop view adult_users
```

---

### 4.9 CREATE INDEX

```sql
create index indexname on tablename(colname)
```

创建 B+ 树二级索引。

**示例：**
```sql
create index idx_name on users(name)
```

---

### 4.10 DROP INDEX

```sql
drop index indexname on tablename
```

**示例：**
```sql
drop index idx_name on users
```

---

## 5. DML — 数据操纵

### 5.1 INSERT INTO

```sql
insert into tablename (col1, col2, ...) values (val1, val2, ...)
```

**参数：**
- `col1, col2, ...` — 列名列表
- `val1, val2, ...` — 值列表（字符串用单引号）

**示例：**
```sql
insert into users (id, name, age) values (1, 'Alice', 25)
insert into users (id, name) values (2, 'Bob')
```

---

### 5.2 SELECT

```sql
select [distinct] columns from tablename
[where conditions]
[group by col [having conditions]]
[order by col [asc|desc]]
[limit n] [offset m]
```

**参数：**
- `columns` — `*` 或 `col1, col2, ...` 或聚合函数
- `conditions` — 见 [5.5 条件表达式](#55-条件表达式)

**示例：**
```sql
select * from users
select name, age from users where age > 20
select distinct age from users
select * from users where name like 'A%'
select * from users order by age desc
select * from users limit 10 offset 5
```

---

### 5.3 UPDATE

```sql
update tablename set col1 = val1, col2 = val2, ... [where conditions]
```

**示例：**
```sql
update users set age = 26 where id = 1
update users set score = 100 where name = 'Alice'
```

---

### 5.4 DELETE FROM

```sql
delete from tablename [where conditions]
```

**示例：**
```sql
delete from users where id = 1
delete from users
```

---

### 5.5 条件表达式

| 运算符 | 说明 | 示例 |
|--------|------|------|
| `=` | 等于 | `id = 1` |
| `>`, `<`, `>=`, `<=` | 比较 | `age > 20` |
| `!=`, `<>` | 不等于 | `name != 'Alice'` |
| `like` | 模式匹配（`%` = 任意串, `_` = 单字符） | `name like 'A%'` |

**逻辑组合：**
```sql
where age > 20 and name like 'A%'
where score >= 80 or age < 18
```

**子查询：**
```sql
where id in (select uid from orders)
where exists (select * from orders where uid = 1)
where age > any (select age from admins)
where age > all (select age from admins)
```

---

## 6. DQL — 高级查询

### 6.1 JOIN

```sql
select ... from table1 join table2 on table1.col = table2.col [where ...]
```

**JOIN 类型：**
- `JOIN` / `INNER JOIN` — 内连接
- `LEFT JOIN` — 左外连接
- `RIGHT JOIN` — 右外连接

**示例：**
```sql
select * from users join orders on users.id = orders.uid
select * from users left join orders on users.id = orders.uid
select users.name, orders.amount from users join orders on users.id = orders.uid where orders.amount > 40
```

---

### 6.2 UNION / UNION ALL

```sql
select ... union select ...
select ... union all select ...
```

**示例：**
```sql
select name from users union select name from admins
```

---

### 6.3 GROUP BY / HAVING

```sql
select col, aggregate from tablename group by col [having conditions]
```

**聚合函数：**
| 函数 | 说明 |
|------|------|
| `count(*)` | 计数 |
| `max(col)` | 最大值 |
| `min(col)` | 最小值 |
| `sum(col)` | 求和 |
| `avg(col)` | 平均值 |

**示例：**
```sql
select age, count(*) from users group by age
select age, count(*) from users group by age having count(*) > 1
select count(*), max(score), min(score), sum(score), avg(score) from users
```

---

### 6.4 EXPLAIN

```sql
explain select ...
```

显示查询执行计划，包括各算子成本估计。

**示例：**
```sql
explain select * from users where id = 1
explain select * from users join orders on users.id = orders.uid
```

---

## 7. TCL — 事务控制

### 7.1 BEGIN

```sql
begin
```

开启事务，分配事务 ID，创建 ReadView。

---

### 7.2 COMMIT

```sql
commit
```

提交事务，持久化到 WAL。

---

### 7.3 ROLLBACK

```sql
rollback
```

回滚事务，基于 Undo Log 恢复数据。

---

### 7.4 CHECKPOINT

```sql
checkpoint
```

刷盘所有脏页，写入 checkpoint 记录，截断 WAL。

---

### 7.5 VACUUM

```sql
vacuum [tablename]
```

回收已删除行占用的空间。不带表名则清理当前数据库所有表。

**示例：**
```sql
vacuum users
vacuum
```

---

## 8. DCL — 权限控制

### 8.1 CREATE USER

```sql
create user username password permission
```

**参数：**
- `username` — 用户名
- `password` — 密码
- `permission` — `1` = 管理员, `0` = 普通用户

**示例：**
```sql
create user eve eve 0
create user admin2 pass123 1
```

---

### 8.2 GRANT

```sql
grant privilege on tablename to username
```

**privilege：** `select`, `insert`, `update`, `delete`, `all`

**示例：**
```sql
grant select on users to eve
grant all on orders to eve
```

---

### 8.3 REVOKE

```sql
revoke privilege on tablename from username
```

**示例：**
```sql
revoke insert on users from eve
```

---

## 9. 系统命令

### 9.1 ANALYZE TABLE

```sql
analyze table tablename
```

收集表统计信息（行数、列基数、最小/最大值），供查询优化器使用。

---

### 9.2 SHOW CONNECTIONS

```sql
show connections
```

显示当前连接列表（网络服务模式）。

---

### 9.3 SHOW STATUS

```sql
show status
```

显示服务器状态统计。

---

### 9.4 PREPARE / EXECUTE / DEALLOCATE

```sql
prepare stmt_name from 'sql template with ?'
execute stmt_name using (val1, val2, ...)
deallocate prepare stmt_name
```

**示例：**
```sql
prepare get_user from 'select * from users where id = ?'
execute get_user using (1)
deallocate prepare get_user
```

---

### 9.5 CSV 导入导出

**导入：**
```sql
load data infile '/path/to/file.csv' into table tablename
```

**导出：**
```sql
select * from tablename into outfile '/path/to/file.csv'
```

---

## 10. 存储与文件格式

### 10.1 数据库目录结构

```
dbname/
├── tlist.lst            # 表名列表
├── .stats               # 统计信息
├── .permissions         # 权限数据
├── .views/              # 视图定义
│   └── viewname.view
├── checkpoint           # Checkpoint 记录
├── wal                  # WAL 日志
├── tablename.stc        # 表结构（二进制）
├── tablename.dt         # 表数据（Slotted Page）
├── tablename.idx        # B+ 树主键索引
├── tablename.colname.idx   # 二级索引
└── tablename.secidx     # 二级索引元数据
```

### 10.2 行格式

**定长行：**
```
+-------------+---------------------------+
| MVCC Header | Fixed Data                |
| (16 bytes)  |                           |
+-------------+---------------------------+
```

**变长行（VARCHAR）：**
```
+-------------+-------------+------------------+----------------+
| MVCC Header | Fixed Data  | Var Offset Array | Var Data       |
| (16 bytes)  |             | (4 bytes/var col)|                |
+-------------+-------------+------------------+----------------+
```

### 10.3 MVCC 头部

| 字段 | 大小 | 说明 |
|------|------|------|
| `creatorTxnId` | 8 bytes | 创建此行的事务 ID |
| `rollbackPtr` | 8 bytes | 指向旧版本（Undo Log） |

---

## 11. 架构说明

### 11.1 核心组件

| 文件 | 职责 |
|------|------|
| `main.cpp` | SQL 解析、命令分发、网络服务入口 |
| `TableManage.h/cpp` | 存储引擎（DDL/DML/索引/事务/MVCC/Checkpoint/VACUUM） |
| `ExecutionPlan.h/cpp` | 查询优化器、火山模型执行计划、JOIN 算法选择 |
| `Page.h/cpp` | Slotted Page（插入/删除/更新/压缩/校验和） |
| `PageAllocator.h/cpp` | 页分配器、空闲页链表 |
| `BufferPool.h/cpp` | LRU 缓冲池、fsync 持久化 |
| `BPTree.h/cpp` | B+ 树索引（磁盘页式存储） |
| `LockManager.h/cpp` | 表级读写锁、死锁检测 |
| `NetworkServer.h/cpp` | TCP 网络服务、独立 Session |
| `TxnIdGenerator.h/cpp` | 全局事务 ID 生成器 |
| `Session.h` | 会话上下文结构体 |

### 11.2 事务隔离

当前实现 **快照隔离（Snapshot Isolation）**：

- 每行带 16 字节 MVCC 头部
- `BEGIN` 时创建 ReadView，记录活跃事务集合
- `SELECT` 只读取 ReadView 创建前已提交的数据
- 同一事务内的修改对自己可见
- 未提交的数据对其他事务不可见

### 11.3 JOIN 算法选择

查询优化器根据统计信息自动选择：

| 场景 | 算法 |
|------|------|
| 小表（< 100 行） | NestedLoopJoin |
| 大表 + 无序 | HashJoin |
| 大表 + join key 有索引 | MergeJoin |

### 11.4 崩溃恢复

1. 启动时读取最新 checkpoint
2. 从 checkpoint 位置开始重放 WAL
3. 遇到未完成的 `BEGIN`（无对应 `COMMIT`）：回滚该事务

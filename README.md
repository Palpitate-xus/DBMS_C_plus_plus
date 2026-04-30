# 简易数据库管理系统 (Simple DBMS)

基于 C++17 实现的简易关系型数据库管理系统，支持标准的 SQL 语句交互，具备磁盘持久化、B+ 树索引、事务等核心数据库功能。

## 功能特性

### 数据定义 (DDL)
- **数据库管理**：`CREATE DATABASE`, `DROP DATABASE`
- **表管理**：`CREATE TABLE`, `DROP TABLE`, `ALTER TABLE ADD/DROP COLUMN`
- **查看结构**：`VIEW TABLE`, `VIEW DATABASE`
- **数据类型**：`INT`, `TINYINT`, `LONG`, `CHAR(n)`, `DATE`
- **约束支持**：主键 (Primary Key)、非空 (NOT NULL)

### 数据操纵 (DML)
- **插入**：`INSERT INTO ... VALUES (...)`
- **查询**：`SELECT` 支持 `*`、指定列、WHERE 条件、ORDER BY
- **更新**：`UPDATE ... SET ... WHERE ...`
- **删除**：`DELETE FROM ... WHERE ...`

### 高级查询
- **条件过滤**：支持 `=`, `>`, `<`, `>=`, `<=`, `!=` 以及 `AND`/`OR` 组合
- **排序**：`ORDER BY column [ASC|DESC]`，支持字符串、整数、日期类型感知排序
- **聚合函数**：`COUNT(*)`, `MAX`, `MIN`, `SUM`, `AVG`
- **表连接**：`INNER JOIN`，支持 `ON` 条件和 `WHERE` 过滤

### 事务控制 (TCL)
- `BEGIN` — 开启事务（数据库级快照）
- `COMMIT` — 提交事务
- `ROLLBACK` — 回滚事务

### 索引
- **B+ 树主键索引**：磁盘页式存储（4096 字节/页），支持 O(log n) 的精确查找和范围扫描
- **唯一性约束**：通过 B+ 树索引自动检测重复主键

### 权限管理
- 用户登录系统（用户名/密码存储于 `user.dat`）
- 管理员与普通用户权限区分
- `CREATE USER` 创建新用户

## 编译与运行

### 环境要求
- GCC / Clang 支持 C++17
- Linux 环境

### 编译
```bash
g++ -std=c++17 -I. main.cpp TableManage.cpp BPTree.cpp -o dbms
```

### 运行
```bash
./dbms
```
启动后输入用户名和密码登录（默认管理员账户在 `user.dat` 中配置）。

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
select * from users where score < 80 or age < 26
select * from users order by score desc
select count(*), max(score), min(score), sum(score), avg(score) from users
```

### 更新与删除
```sql
update users set score=95 where id=1
delete from users where score < 80
```

### JOIN
```sql
create table orders {oid:int 0 PK, uid:int 0, amount:int 0}
select * from users join orders on users.id = orders.uid
select users.name, orders.amount from users join orders on users.id = orders.uid where orders.amount > 40
```

### 修改表结构
```sql
alter table users add column email:char30 1
alter table users drop column email
```

### 事务
```sql
begin
insert into users (id, name, age, score) values (5, 'frank', 35, 55)
rollback        -- 撤销
-- 或
commit          -- 提交
```

### 用户管理
```sql
create user eve eve 0     -- 用户名 密码 权限(0=普通, 1=admin)
```

## 项目结构

```
.
├── main.cpp              # SQL 交互入口与命令解析
├── TableManage.h         # 存储引擎头文件
├── TableManage.cpp       # 存储引擎实现（DDL/DML/索引/事务）
├── BPTree.h              # B+ 树索引头文件
├── BPTree.cpp            # B+ 树索引实现（磁盘页式存储）
├── DateType.h            # 日期类型定义与运算
├── logs.h                # 操作日志记录
├── permissions.h         # 用户认证与权限查询
├── user.dat              # 用户账号数据
└── dbms.log              # 运行日志
```

## 存储格式

每个数据库为一个独立目录：

```
dbname/
├── tlist.lst          # 表名列表
├── tablename.stc      # 表结构（二进制）
├── tablename.dt       # 表数据（定长行，二进制）
└── tablename.idx      # B+ 树主键索引（4096 字节/页）
```

## 参考项目

- [hyrise/sql-parser](https://github.com/hyrise/sql-parser) — SQL Parser for C++
- [zcbenz/BPlusTree](https://github.com/zcbenz/BPlusTree) — B+ tree implementation which stores data in file
- [Jefung/simple_DBMS](https://github.com/Jefung/simple_DBMS) — C++ 实现简单数据库引擎
- [niteshkumartiwari/B-Plus-Tree](https://github.com/niteshkumartiwari/B-Plus-Tree) — Mini Database System using B+ Tree in C++

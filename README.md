# 关系型数据库管理系统 (DBMS)

基于 C++17 实现的关系型数据库管理系统，支持标准 SQL 交互，具备页式存储、B+ 树索引、MVCC 事务、查询优化器、网络服务等数据库核心功能。

> **完整使用手册**: [docs/MANUAL.md](docs/MANUAL.md)
> **生产化状态与边界**: [docs/production-status.md](docs/production-status.md)
> **PostgreSQL 18 差距分析**: [docs/postgresql-comparison.md](docs/postgresql-comparison.md)
> **当前状态（2026-08-09）**: 生产化重构进行中；统一回归基线 PASS=124 FAIL=0（122 个 C++ 测试 + PostgreSQL 协议 E2E + 窗口函数 E2E），主构建 `-Wall -Wextra` 无警告。当前发行格式为单一的 v2/8 KiB 存储格式，不提供旧数据迁移；这不代表已达到 PostgreSQL 生产级等价。

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
- **Upsert（窄版 AST 路径）**：单列主键/唯一列 target 配合常量 `SET` 的 `INSERT INTO ... VALUES ... ON CONFLICT (col) DO UPDATE SET ...`
- **查询**：`SELECT` 支持 `*`、指定列、`WHERE`、`ORDER BY`、`LIMIT`、`OFFSET`、`DISTINCT`
- **更新**：`UPDATE ... SET ... WHERE ...`, `UPDATE ... FROM ... WHERE ...`, `UPDATE ... LIMIT n`
- **删除**：`DELETE FROM ... WHERE ...`, `DELETE ... USING ... WHERE ...`, `DELETE ... LIMIT n`
- **多表更新/删除**：支持 `FROM` / `USING` 子句的跨表 UPDATE/DELETE

普通单表 `INSERT ... VALUES` / `DEFAULT VALUES`、无 JOIN/聚合/排序的单表 `INSERT ... SELECT`、无 target 的 `ON CONFLICT DO NOTHING`，以及单列主键/唯一列 target 配合常量 `SET` 的窄版 `ON CONFLICT DO UPDATE`，当前由 `src/commands/DmlExecutor` 消费 AST；支持结构化常量/列表达式、简单 `AND` 谓词、`IS NULL`/`IS NOT NULL` 及 StorageEngine 统一约束路径。普通单表 INSERT/UPDATE/DELETE 的列投影 `RETURNING` 已在存储修改边界收集并通过 PostgreSQL 协议结果集发送。复杂 `INSERT ... SELECT`、复合/部分/索引推断 conflict target、引用 `excluded` 或列的 `DO UPDATE` 表达式、`DO UPDATE WHERE`、`RETURNING` 表达式、多表/复杂表达式、视图写入和 `MERGE` 明确回退到 legacy 执行路径。该回退边界会逐步缩小，不能视为 PostgreSQL 完整语义。

### 高级查询 (DQL)
- **条件过滤**：支持 `=`, `<>`, `!=`, `>`, `<`, `>=`, `<=`, `LIKE`, `BETWEEN`, `IN`, `EXISTS`, `ANY`, `ALL`, `IS NULL`, `IS NOT NULL` 以及 `AND`/`OR` 组合；未关联单列 `IN`/`NOT IN`、未关联单表 `EXISTS`/`NOT EXISTS` 和单列 `ANY/ALL` 已进入结构化 Volcano 计划，复杂/关联子查询仍受生产边界限制
- **三值逻辑**：`TRUE` / `FALSE` / `UNKNOWN`，WHERE 子句中 `UNKNOWN` 被当作 `FALSE`
- **排序**：`ORDER BY column [ASC|DESC]`，支持字符串、数值、日期类型
- **聚合函数**：`COUNT(*)`, `COUNT(DISTINCT ...)`, `MAX`, `MIN`, `SUM`, `AVG`
- **分组**：`GROUP BY ... HAVING ...`
- **表连接**：`INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `CROSS JOIN`
- **JOIN 算法选择**：NestedLoopJoin / HashJoin / MergeJoin，查询优化器根据统计信息自动选择
- **子查询**：解析层支持 `IN`, `EXISTS`, `ANY`, `ALL` 及标量子查询；执行层已结构化未关联单列 `IN`/`NOT IN`、未关联单表 `EXISTS`/`NOT EXISTS`、单个未关联标量目标（严格 0/1 行）以及单列 `ANY/ALL` 量化过滤（含 NULL/空集三值逻辑）；关联、复杂标量、row comparison 和复杂组合仍走兼容 fallback
- **联合**：`UNION`, `UNION ALL`
- **CTE**：`WITH cte_name AS (SELECT ...)` 公用表表达式
- **导出**：`SELECT ... INTO OUTFILE 'file.csv'`
- **执行计划**：`EXPLAIN SELECT ...`, `EXPLAIN ANALYZE SELECT ...`, `EXPLAIN FORMAT JSON SELECT ...`
- **窗口函数**：`ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LAG()`, `LEAD()`, `FIRST_VALUE()`, `LAST_VALUE()`, `NTILE()`, `PERCENT_RANK()`, `CUME_DIST()` 支持 `OVER (PARTITION BY ... ORDER BY ...)`
- **派生表**：`(SELECT ...) AS alias`
- **锁查询**：`FOR UPDATE`, `FOR SHARE`, `NOWAIT`, `SKIP LOCKED`

### 事务控制 (TCL) / MVCC
- `BEGIN` / `START TRANSACTION` 支持 `ISOLATION LEVEL`、`READ ONLY/WRITE` 和 `NOT DEFERRABLE` 选项；当前 `DEFERRABLE` 会明确拒绝（尚无安全快照实现）
- `COMMIT` — 提交事务，持久化到 WAL
- `ROLLBACK` — 基于 Undo Log 的增量回滚
- `SAVEPOINT spname` / `ROLLBACK TO [SAVEPOINT] spname` / `RELEASE [SAVEPOINT] spname` 通过事务 AST 统一解析
- **MVCC 快照隔离**：每行使用 PostgreSQL 风格 HeapTupleHeader，事务内读取基于 ReadView 的可见性规则
- **隔离级别**：支持 `READ UNCOMMITTED` / `READ COMMITTED` / `REPEATABLE READ` / `SERIALIZABLE`
- **全局事务 ID 生成器**：单调递增 64 位 txId，持久化到 `.txnid` 文件
- **HOT 更新**：堆内元组直接更新（不更新索引指针），减少 WAL 写入与索引维护开销
- **CLOG (Commit Log)**：事务提交状态位图，加速可见性判断与故障恢复

### 索引
- **B+ 树主键索引**：磁盘页式存储，O(log n) 精确查找
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
- **Slotted Page**：8192 字节页式存储，line pointer 数组管理记录位置
- **页分配器**：空闲页链表管理，支持页复用
- **Buffer Pool**：LRU 缓存，减少磁盘 I/O
- **页校验和**：Fletcher-16 校验，检测页损坏
- **WAL 日志**：Write-Ahead Logging 支持崩溃恢复
- **Checkpoint**：`CHECKPOINT` 命令刷盘所有脏页并截断 WAL，加速重启恢复
- **fsync 持久化**：WAL 写入、事务提交、Checkpoint 均调用 `fsync()` 保证数据落盘
- **VARCHAR 变长行**：`[定长数据 | 变长偏移数组 | 变长数据]` 格式，减少存储浪费
- **溢出页**：单行数据超过页空间时，大字段（TEXT/BLOB/JSON）自动存放到溢出页
- **MVCC 行格式**：每行开头为 PostgreSQL 风格 HeapTupleHeader（含 xmin/xmax/ctid、null bitmap 和对齐信息）
- **统计信息**：`ANALYZE TABLE` 收集行数、列基数、最小/最大值、MCV（最常出现值）、多列统计
- **SQL 可观测性**：`SHOW STATEMENTS` 与 `pg_stat_statements` 风格虚拟表提供线程安全的调用次数及耗时聚合；统计当前为进程内生命周期
- **运行时统计**：共享 `RuntimeStats` 在 SQL、StorageEngine 和 Volcano 扫描算子边界记录数据库查询/失败/事务、顺序扫描、索引扫描及 DML 计数，供 `SHOW STATUS`、`pg_stat_database` 和 `pg_stat_tables` 使用；当前为进程内统计
- **VACUUM**：`VACUUM [tablename]` 回收已删除行占用的空间，页压缩并归还空页
- **自动 VACUUM**：可配置阈值，死行数达到阈值时自动触发

### 权限管理
- 用户登录系统（角色与 SCRAM 凭据存储于 `info/pg_catalog/pg_authid.cat`）
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
- **PostgreSQL TCP 服务器**：`./dbms_main --server PORT` 启动 PostgreSQL protocol 3.0 服务端
- **扩展查询协议**：支持 Parse/Bind/Execute/Describe/Close/Sync、SCRAM、常用标量及 date/time/timestamp/timestamptz/uuid binary 参数与结果，以及基础 portal `maxRows` 分页；完整 libpq/cursor 语义仍在建设中
- **TLS 加密**：默认必须提供证书和私钥；证书缺失或 TLS 初始化失败时拒绝启动
- **开发明文模式**：仅可通过显式 `./dbms_main --server PORT --insecure` 开启，不得用于生产环境
- **多客户端**：每个连接独立线程，支持并发访问；legacy 文本执行器的结果捕获采用线程局部输出路由，不再用全局锁串行化协议会话
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

### 并发测试结果
以下指标均在并发测试套件（`concurrency_test`、`hot_update_test`、`clog_test`、`clog_integration_test`、`subxip_visibility_test`、`checkpoint_test`、`wal_basic_test`）中实测验证，全部 PASS：

| 测试项 | 结果 | 关键指标 |
|--------|------|----------|
| **事务原子性** | ✅ PASS | BEGIN/COMMIT/ROLLBACK 正确提交与回滚，Savepoint 部分回滚正常 |
| **WAL 顺序写入吞吐** | ✅ PASS | ~1767–1781 行/秒（200 行写入约 113ms） |
| **B+ 树索引插入吞吐** | ✅ PASS | ~1729–1741 行/秒（100 行插入约 57ms） |
| **索引等值查找** | ✅ PASS | 34 条匹配记录在 3–4ms 内返回 |
| **聚合性能** | ✅ PASS | 500 行 COUNT/SUM/MAX/GROUP BY 约 78ms；批量插入 500 行约 254ms |
| **MVCC 快照隔离** | ✅ PASS | 事务内 ReadView 可见性规则正确，未提交数据不可见 |
| **HOT 堆内更新** | ✅ PASS | 更新不修改索引指针，减少 WAL 日志写入 |
| **CLOG 提交日志** | ✅ PASS | 事务提交状态位图读写、提交顺序追踪、子事务可见性正确 |
| **Checkpoint 持久化** | ✅ PASS | 脏页刷盘 + WAL 截断，重启后数据完整恢复 |
| **WAL 崩溃恢复** | ✅ PASS | 未提交事务回滚，已提交事务持久化 |

**并发隔离验证示例**（基于 MVCC 快照隔离 + 锁机制）：
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

### 新增功能 (Phase 4 完整化)
- **pg_hba.conf 访问控制**: 首条匹配、CIDR/IPv4/IPv6、角色组和传输类型约束；运行时支持 trust/password/md5→SCRAM/scram/reject
- **表继承**: `ALTER TABLE ... INHERIT / NO INHERIT`
- **ALTER TABLE SET TABLESPACE / SET STATISTICS**
- **COMMIT/ROLLBACK AND [NO] CHAIN**
- **复制管理**: ReplicationManager (slots, standby, sync, promote)
- **大对象**: LargeObjectManager (CRUD + import/export)
- **多进程后端**: ProcessManager (10 种后端类型)
- **扩展生态框架**: EXTENSION, FDW, PL, custom types/operators

> 详细语法和示例请查阅 [docs/MANUAL.md](docs/MANUAL.md)

## 编译与运行

### 环境要求
- GCC / Clang 支持 C++17
- Linux 环境
- POSIX 线程支持

### 编译

#### 方式一：标准脚本构建（推荐）
```bash
./scripts/build.sh
```

#### 方式二：CMake
```bash
cmake -S . -B build
cmake --build build -j$(nproc)
```

> CMake 与全部 shell 构建/测试入口共同使用 [`cmake/dbms_sources.txt`](cmake/dbms_sources.txt) 和 [`scripts/build_common.sh`](scripts/build_common.sh)；新增或删除生产模块、编译选项或 TLS/压缩依赖不再分别维护多份配置。对象缓存带有编译配置指纹，参数变化时会自动失效。

> **依赖说明**：zlib 开发库是当前 TOAST 压缩存储格式的必需依赖（Ubuntu/Debian 安装 `zlib1g-dev`）；CMake 与全部 shell 构建入口会统一链接它。若系统已安装 OpenSSL 开发库（`libssl-dev`），CMake 和 `build.sh` 会编译真实 TLS；否则只保留离线构建所需的 stub，网络服务默认 fail-closed。生产部署必须使用真实 OpenSSL、证书和私钥。

### 交互式运行
```bash
./dbms_main
```
启动后输入用户名和密码登录（角色必须先存在于 `pg_authid`，密码使用 SCRAM-SHA-256）。

### 网络服务模式
```bash
# 服务端
export DBMS_TLS_CERT=/etc/dbms/tls/server.crt
export DBMS_TLS_KEY=/etc/dbms/tls/server.key
./dbms_main --server 9999

# 仅限本地开发：显式允许明文
./dbms_main --server 9999 --insecure

# 客户端（libpq/psql；当前支持 SCRAM-SHA-256 与基础协议流程）
psql "host=localhost port=9999 dbname=info user=admin sslmode=require"
```

### Docker 部署

项目支持 Docker 多阶段构建与 Docker Compose 一键部署。

```bash
# 构建镜像
docker build -t dbms-c-plus-plus:latest .

# 交互式运行（开发调试）
docker run -it --rm -v dbms_data:/data dbms-c-plus-plus:latest

# 服务器模式
docker run -d --name dbms_server -p 9999:9999 -v dbms_data:/data \
    dbms-c-plus-plus:latest ./dbms_main --server 9999

# Docker Compose 一键启动
docker compose up -d

# 自定义端口（默认 9999）
DBMS_PORT=8888 docker compose up -d

# 停止
docker compose down
```

> **镜像**：基于 `ubuntu:26.10`，多阶段构建（g++15 + OpenSSL），最终镜像约 114MB。
> **数据持久化**：数据库文件存储在 `/data` 目录，通过 Docker 卷（`dbms_data`）持久化，容器重启数据不丢失。
> **TLS**：容器不再自动生成证书，也不会在证书缺失时回退到明文；请挂载证书/私钥并通过 `DBMS_TLS_CERT`、`DBMS_TLS_KEY` 指定路径。仅本地调试可显式追加 `--insecure`。

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
├── PgPage.h                 # 8 KiB PostgreSQL 风格 heap page
├── PgPage.cpp               # 页操作（插入/删除/更新/压缩/校验和）
├── PageWrapper.h            # 引擎 RowId 与 line pointer 的适配层
├── PageWrapper.cpp          # 单一 v2 页格式实现
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
├── src/process/OutputCapture.*  # legacy 执行器的线程局部文本输出捕获
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
├── info/pg_catalog/pg_authid.cat # 角色与 SCRAM 凭据 catalog
├── dbms.log                 # 运行日志
├── slow_query.log           # 慢查询日志
├── Dockerfile               # Docker 多阶段构建定义
├── docker-compose.yml       # Docker Compose 一键部署配置
├── .dockerignore            # Docker 构建排除规则
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
├── tablename.dt            # 表数据（Slotted Page，8192 字节/页）
├── tablename.idx           # B+ 树主键索引
├── tablename.colname.idx   # 二级索引
├── tablename.secidx        # 二级索引元数据
└── tablename.hashidx       # Hash 索引
```

## 页格式

数据页（8192 字节）：

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
- **`SHOW USERS` / `SHOW ROLES`** ✅ 已实现：从 `pg_authid` 展示用户与角色属性（需 admin 权限）

## 参考项目

## 文档

| 文件 | 说明 |
|------|------|
| [README.md](README.md) | 项目总览与功能特性 |
| [MANUAL.md](docs/MANUAL.md) | 完整使用手册 (25 章, 覆盖全部 SQL 语法) |
| [implementation-plan.md](docs/implementation-plan.md) | 实施计划与历史 Wave 记录（当前状态以 Gap 表为准） |
| [all-gaps-todo.md](docs/all-gaps-todo.md) | Gap 追踪与进度备注 |
| [postgresql-comparison.md](docs/postgresql-comparison.md) | PostgreSQL 18 功能对比与差距分析 |
| [test-report.md](docs/test-report.md) | 自动测试报告（当前回归基线 PASS=124 FAIL=0） |
| [commandsList.md](docs/commandsList.md) | SQL 命令参考手册 |
| [archive/](docs/archive/) | 历史过程文档 (Phase 4 专项计划、PG 差距分析) |

## 致谢

- [hyrise/sql-parser](https://github.com/hyrise/sql-parser) — SQL Parser for C++
- [zcbenz/BPlusTree](https://github.com/zcbenz/BPlusTree) — B+ tree implementation which stores data in file
- [Jefung/simple_DBMS](https://github.com/Jefung/simple_DBMS) — C++ 实现简单数据库引擎
- [niteshkumartiwari/B-Plus-Tree](https://github.com/niteshkumartiwari/B-Plus-Tree) — Mini Database System using B+ Tree in C++

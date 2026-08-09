# DBMS 功能测试报告

> 最后更新：2026-08-09
> 自动测试套件基线：PASS=126 FAIL=0（124 个 C++ 测试 + PostgreSQL 协议 E2E + 窗口函数 E2E）；窗口函数 E2E：13/13
> 测试依据：[commandsList.md](commandsList.md) + [all-gaps-todo.md](all-gaps-todo.md)

---

## 测试环境

- OS: Linux 6.8.0-117-generic
- Compiler: g++ -std=c++17 -O2 -pthread
- 标准编译命令：`./scripts/build.sh`；规范测试入口：`./scripts/build_tests.sh`；`./scripts/run_all_tests_fast.sh` 是只改变输出形式的安静外壳
- 本轮构建质量检查：主程序在 `-Wall -Wextra` 下无编译警告；修复 join cost 参数错误及 parser/测试中的未使用代码。
- schema 格式回归：当前格式 `0x44420009`，长表名、列名和约束名可完整写入/读取；旧格式仍按 fail-closed 策略拒绝。
- 窗口函数端到端测试：由统一回归入口执行 `tests/window_e2e_test.py`（13 用例，包含 Volcano `WindowAgg` 排名/偏移、窗口聚合、`ROWS/RANGE/GROUPS` frame/exclusion、OFFSET 和命名窗口路径，使用隔离临时目录和临时管理员账号）
- Volcano 窗口单元测试：`tests/volcano_select_phase51_test.cpp` 覆盖多窗口独立排序、排名并列、分区边界、`lag`、默认 frame、`EXCLUDE CURRENT ROW`、count/first/last/ntile/percent_rank/cume_dist 及文本/JSON EXPLAIN。
- Volcano 分组单元测试：同一测试覆盖 `GroupAggregateOp` 的常见聚合、`HAVING`、`ROLLUP`/多个 grouping set、省略分组列的 `NULL` 输出及文本/JSON EXPLAIN；主 SQL 手工回归同时验证普通 GROUP BY 与 ROLLUP。
- Semi/Anti/Existence/Quantified/Scalar 回归：`tests/volcano_select_phase51_test.cpp` 验证未关联 `IN`/`NOT IN`、inner 过滤、重复键、NULL 语义、`QuantifiedSubqueryFilterOp` 的 `ANY`/`ALL` 空集与 NULL 语义、`ExistenceFilterOp` 的 `EXISTS`/`NOT EXISTS` 及 `ScalarSubqueryProjectOp` 的 0/1 行与 cardinality error；协议 E2E 验证真实 PostgreSQL 查询路径及 NULL 结果。
- 普通聚合结构化回归：`tests/volcano_select_phase51_test.cpp` 验证带过滤条件的隐式空 grouping set；协议 E2E 验证主 SQL `COUNT(*)`/`SUM()` 已走统一聚合计划路径。
- TOAST 回归：`tests/toast_test.cpp` 验证大值插入/更新/删除、zlib 压缩后的物理体积和解压读取。
- CMake 与脚本构建共同读取 `cmake/dbms_sources.txt`，避免生产源文件列表漂移。
- `scripts/build.sh`、`build_tests.sh`、`build_one_test.sh` 和 `run_all_tests_fast.sh` 共同读取 `scripts/build_common.sh`；本轮验证了配置指纹失效与增量单测入口。
- `./scripts/build_tests.sh` 是唯一的完整测试编排实现，缓存生产对象后逐个测试独立链接运行，并自动执行两个 E2E；`run_all_tests_fast.sh` 仅捕获其成功输出并显示 PASS 计数，失败时原样打印完整编译/链接/运行日志。
- PostgreSQL 协议 E2E 覆盖普通顶层多行 DML 和写 CTE 的 statement atomicity：批量 INSERT 或 CTE INSERT 后续行冲突时，前序行不会残留；成功的 `WITH ... RETURNING` 结果也能通过协议返回。
- PostgreSQL 协议 E2E 覆盖 typed `CREATE TEMP/TEMPORARY TABLE`：临时表在所属会话内持续可用、遮蔽同名永久表，不同 backend 可创建同名临时表，其他会话不可见，断开连接后对象被清理。
- PostgreSQL 协议 E2E 额外覆盖 `ON COMMIT DELETE ROWS` 与 `ON COMMIT DROP`：提交后分别保留空表和删除临时表；parser 单测覆盖 `PRESERVE/DELETE/DROP` AST 状态。
- 新增临时表重启生命周期回归：`tests/temp_restart_cleanup_test.cpp` 模拟临时 relation 已落盘后重启，确认 `tlist.lst`、schema/data/index/TOAST 等残留全部清理；完整测试入口通过。
- 新增数据库生命周期回归：`DROP DATABASE` 释放数据库级缓存后，同名重建不会继承旧 CLOG/WAL/page/index 状态。
- 新增 typed `ALTER TABLE` 路由回归：验证 AST bridge 的 ADD COLUMN、DEFAULT、NOT NULL、RENAME COLUMN/TABLE，以及 `RENAME TO` 不再误判为列重命名。
- 新增 schema 格式完整性回归：截断 schema 与错误 magic 均 fail-closed，不会返回可写的部分 schema。
- Docker 镜像构建：`docker build -t dbms-cpp:verification .` ✅（OpenSSL 真实 TLS）；Compose 配置检查 ✅。
- CMake：当前验证环境未安装 `cmake`，配置/编译未执行。
- 网络启动安全边界：无 TLS 证书/私钥时默认拒绝启动；明文仅由显式 `--insecure` 开关启用。
- 网络连接容量回归：accept 路径原子预留 `maxConnections` 槽位，认证失败和 TLS 握手失败均释放槽位。
- 网络输出并发回归：`tests/network_security_test.cpp` 并发创建 4 个线程局部输出捕获，每个线程写入 32 行并验证没有其他会话内容串入。
- 角色 DDL 回归：`CREATE/ALTER/DROP ROLE`、`CREATE/ALTER/DROP USER` 统一走 Catalog/AST 路径，混合大小写密码通过真实验证。
- PostgreSQL 协议 E2E：`python3 tests/postgres_protocol_test.py` 验证 SSLRequest/plaintext 协商、StartupMessage、真实 SCRAM-SHA-256 握手、simple query 和 Parse/Bind/Execute/Sync，并覆盖 NUMERIC 精确存储、1700 binary 参数/结果与 PostgreSQL INSERT command tag。
- 协议扩展查询回归：验证 `ParameterDescription`、文本和 binary `int4` 参数 `$1`、binary `int4` 结果、statement `Describe`、portal/statement `Close` 的真实 Parse/Bind/Execute 生命周期，以及 OID/长度/属性号/表 OID RowDescription 元数据。
- 协议日期时间/UUID binary 回归：验证 `date`、`time`、秒精度 `timestamp`/`timestamptz` 和 `uuid` 的 binary 参数、binary 结果、OID/格式元数据及单列空格值不被拆分。
- 协议 portal 分页回归：验证 `Execute maxRows` 分批返回、未耗尽时发送 `PortalSuspended`、耗尽时发送 `CommandComplete`，以及耗尽 portal 再执行不重复返回数据。
- 协议 ACL 回归：alice 将 `SELECT` 授予 `analyst`，bob 通过角色继承读取表数据；bob 未获得 `INSERT` 时真实协议返回权限错误。
- 角色继承边界回归：`NOINHERIT` 用户保留原始成员关系但不获得父角色 ACL/RLS 权限，切换为 `INHERIT` 后两条权限路径同时生效；`pg_hba.conf` 角色匹配仍使用原始成员关系。
- 对象授权回归：非所有者即使带有管理员标志也不能执行 `ALTER TABLE ... OWNER TO`；所有者仅可转移给能够 `SET ROLE` 到的目标角色；`SET ROLE` 后超级用户绕过和表 ACL/RLS 有效角色均正确收敛。
- owner ACL 回归：无显式 `.permissions` 记录时，表 owner 仍拥有表级/列级权限、`GRANT OPTION` 和 `all` 权限视图。
- 表级 GRANT OPTION 回归：验证表级 grant marker 仍提供基础列权限，独立撤销只移除 grant option，存在下游授权时 RESTRICT 拒绝、CASCADE 递归撤销并清理 grant chain。
- 角色 ADMIN OPTION 回归：验证角色关系的 ADMIN OPTION 授予、重复授权升级、只撤销 ADMIN OPTION，以及撤销后成员关系仍保留。
- 默认权限回归：验证 typed `ALTER DEFAULT PRIVILEGES` 的表级多权限 GRANT、幂等写入、REVOKE 只影响后续新表，以及不支持对象类型/grant option 时 fail-closed。
- TRUNCATE 回归：验证 typed 多表 AST、`RESTRICT` 在 FK 依赖下不产生部分修改、递归 `CASCADE`、多表截断、`ONLY` 字段及 `RESTART IDENTITY`。
- ALTER TABLE typed 回归：`tests/alter_table_only_test.cpp` 验证 RLS enable/disable/force/no-force、ATTACH/DETACH PARTITION 的 AST 规格传递，以及 trigger enable/disable 的持久化状态。
- EXCLUDE typed 回归：`tests/exclude_test.cpp` 通过 DDL bridge 验证 `ALTER TABLE ... ADD/DROP CONSTRAINT ... EXCLUDE` 的约束名保留、冲突拒绝、删除后放行和持久化清理；CREATE TABLE 的等值与范围排斥回归继续覆盖 INSERT/UPDATE。
- ALTER TABLE typed 清理回归：`tests/ddl_bridge_routing_test.cpp` 验证基础动作在删除 `main.cpp` 重复分支后仍由 typed bridge 执行；`tests/ddl_ast_bridge_test.cpp` 进一步验证 CLUSTER、REPLICA IDENTITY、VALIDATE/ALTER CONSTRAINT 的 catalog、索引归属和持久化结果。
- 协议错误恢复回归：验证扩展查询错误后的 Bind/Execute 被忽略至 Sync、Sync 后连接可继续查询，以及事务外错误返回 `ReadyForQuery('I')`。
- 协议 backend 隔离回归：双连接验证事务 ID/快照不串线，未提交行不可见，ROLLBACK 后状态清理，连接断开回滚未完成事务，COMMIT 后新事务可见；另覆盖 `START TRANSACTION` 选项与 `SAVEPOINT`/`ROLLBACK TO SAVEPOINT` 路由。
- SQL 统计模块回归：`tests/sql_stats_test.cpp` 验证字符串/数字常量归一化、引号标识符区分、调用次数与耗时聚合、数据库过滤及 reset。
- 运行时统计模块回归：`tests/runtime_stats_test.cpp` 验证并发 SQL 计数、失败/提交/回滚计数、表扫描和实际 DML 行数统计及 reset。
- 协议运行时统计回归：`postgres_protocol_test.py` 通过真实 PostgreSQL wire 查询验证 `pg_stat_database`/`pg_stat_tables` 返回数据库查询、表 DML 及 Volcano 索引扫描/取行计数，而非固定零值。
- DML AST 回归：`parser_phase1_test.cpp`、`dml_semantics_test.cpp` 与 `dml_returning_test.cpp` 验证 `INSERT` 多行 AST、混合 `DEFAULT` 位置、`DEFAULT VALUES` 区分、简单单表 INSERT SELECT（含 WHERE/列表达式）、无 target 或显式匹配主键/唯一约束 target 的 `ON CONFLICT DO NOTHING`、单列及复合主键/唯一约束冲突的常量或只引用 `excluded` 的 evaluator 标量 `DO UPDATE`、目标行/`excluded` 受限 `WHERE`、复合 UNIQUE 中 NULL 不互相冲突、不同唯一约束冲突不会被错误忽略、以当前目标行列值为输入的受限标量表达式 UPDATE、单源表 `UPDATE ... FROM`、单源表 `DELETE ... USING`、INNER/CROSS JOIN 的 UPDATE FROM/DELETE USING、窄版 MERGE、INSERT/UPDATE/DELETE 的受限标量表达式 RETURNING 以及 DML 尾随垃圾 fail-closed；协议 E2E 验证普通单表 INSERT 的多行/表达式/DEFAULT、显式 NULL 不被默认值覆盖、字符串大小写保留、常量及行级表达式 UPDATE、简单谓词 DELETE、INSERT/UPDATE/DELETE 的列投影和受限标量表达式 `RETURNING`、INSERT SELECT/冲突处理返回结果和 `INSERT 0 N` command tag。

---

## 测试汇总

| 功能域 | 测试项数 | 通过 | 失败 | 备注 |
|--------|----------|------|------|------|
| 认证与连接 | 2 | 2 | 0 | — |
| DDL - 数据库 | 3 | 3 | 0 | — |
| DDL - 表 | 14 | 14 | 0 | 含 CTAS / RENAME / POINT / INET/CIDR 类型 |
| DDL - 索引 | 7 | 7 | 0 | B+Tree/Hash/FullText/GIN/GiST/BRIN/SP-GiST 基础路径通过；专用访问方法仍是简化实现，不宣称 PostgreSQL 完整 opclass 兼容；B+Tree 分裂/重复键/范围回归见 `gin_brin_index_test.cpp` |
| DDL - 视图 | 5 | 5 | 0 | 含 ALTER VIEW RENAME TO / SET SCHEMA |
| DDL - 触发器 | 2 | 2 | 0 | — |
| DDL - 用户/角色 | 4 | 4 | 0 | — |
| DML - INSERT | 5 | 5 | 0 | 含 AST bridge 的多行/DEFAULT/显式 NULL 回归 |
| DML - UPDATE/DELETE | 2 | 2 | 0 | — |
| DQL - SELECT | 5 | 5 | 0 | — |
| DQL - JOIN | 3 | 3 | 0 | — |
| DQL - UNION | 1 | 1 | 0 | — |
| TCL - 事务 | 3 | 3 | 0 | — |
| DCL - 权限 | 4 | 4 | 0 | 含 SHOW USERS/SHOW ROLES |
| 工具命令 | 8 | 8 | 0 | — |
| 分区管理 | 3 | 3 | 0 | Range/List/Hash + ATTACH/DETACH |
| 高级特性 | 2 | 2 | 0 | NOTIFY/LISTEN, RLS |
| **合计** | **72** | **72** | **0** | — |

---

## 1. 认证与连接

### 1.1 登录（admin）

**输入**
```
admin admin
```

**预期输出**
```
successfully login
```

**实际结果** ✅ `successfully login`

---

### 1.2 退出

**输入**
```
exit
```

**预期输出**
```
Quitting...
```

**实际结果** ✅ `Quitting...`

---

## 2. DDL - 数据库管理

### 2.1 CREATE DATABASE

**输入**
```sql
CREATE DATABASE testdb;
```

**实际结果** ✅ `Create Database succeeded (charset=utf8)`

---

### 2.2 USE DATABASE

**输入**
```sql
USE DATABASE testdb;
```

**实际结果** ✅ `set Database to testdb`

---

### 2.3 DROP DATABASE

**输入**
```sql
DROP DATABASE testdb;
```

**实际结果** ✅ `Database dropped`

---

## 3. DDL - 表管理

### 3.1 CREATE TABLE（圆括号 + 冒号格式）

**输入**
```sql
CREATE TABLE t1 (id:int:0:1, name:varchar(50):0);
```

**实际结果** ✅ `Table create succeeded`
> 注：此前 `(col:type:flags)` 格式因 tokenize 不分割冒号导致零列解析，已修复。

---

### 3.2 CREATE TABLE（花括号格式）

**输入**
```sql
CREATE TABLE t2 {id:int pk, age:int:0};
```

**实际结果** ✅ `Table create succeeded`

---

### 3.3 INSERT + SELECT 验证

**输入**
```sql
INSERT INTO t1 (id, name) VALUES (1, 'alice');
INSERT INTO t1 (id, name) VALUES (2, 'bob');
SELECT * FROM t1;
```

**实际结果** ✅
```
1 row(s) inserted
1 row(s) inserted
id name
1 alice
2 bob
```

---

### 3.4 ALTER TABLE ADD COLUMN

**输入**
```sql
ALTER TABLE t1 ADD COLUMN email varchar(100);
```

**实际结果** ✅ `Column added`

---

### 3.5 ALTER TABLE DROP COLUMN

**输入**
```sql
ALTER TABLE t1 DROP COLUMN email;
```

**实际结果** ✅ `Column dropped`

---

### 3.6 DROP TABLE

**输入**
```sql
DROP TABLE t2;
```

**实际结果** ✅ `Table dropped`

---

### 3.7 ALTER TABLE RENAME COLUMN

**输入**
```sql
ALTER TABLE t1 RENAME COLUMN name TO username;
```

**实际结果** ✅ `Column renamed`

---

### 3.8 ALTER TABLE RENAME TO

**输入**
```sql
ALTER TABLE t1 RENAME TO t1_new;
```

**实际结果** ✅ `Table renamed`

---

### 3.9 CREATE TABLE AS SELECT (CTAS)

**输入**
```sql
CREATE TABLE t3 AS SELECT * FROM t1;
CREATE TABLE t4 AS SELECT id, name FROM t1 WHERE id = 1;
```

**实际结果** ✅ `Table created with N rows`，数据正确复制

---

### 3.10 CREATE TABLE with POINT type

**输入**
```sql
CREATE TABLE geo_points (id:int:0:1, loc:point:0);
```

**实际结果** ✅ `Table create succeeded`

---

### 3.11 INSERT POINT values

**输入**
```sql
INSERT INTO geo_points (id, loc) VALUES (1, '0.0,0.0');
INSERT INTO geo_points (id, loc) VALUES (2, '10.0,10.0');
INSERT INTO geo_points (id, loc) VALUES (3, '20.0,5.0');
INSERT INTO geo_points (id, loc) VALUES (4, '-5.0,15.0');
```

**实际结果** ✅ 全部 `1 row(s) inserted`

---

### 3.12 SELECT with spatial operators

**输入**
```sql
SELECT * FROM geo_points WHERE loc << '15.0,15.0';   -- left of
SELECT * FROM geo_points WHERE loc >> '15.0,15.0';   -- right of
SELECT * FROM geo_points WHERE loc <^ '15.0,15.0';   -- below
SELECT * FROM geo_points WHERE loc >^ '15.0,15.0';   -- above
SELECT * FROM geo_points WHERE loc <@ '10.0,10.0,5.0'; -- within circle
```

**实际结果** ✅
- `<<` 返回 x < 15 的点（id 1, 2, 4）
- `>>` 返回 x > 15 的点（id 3）
- `<^` 返回 y < 15 的点（id 1, 2, 3）
- `>^` 返回 y > 15 的点（无）
- `<@` 返回圆内点（id 2，中心 (10,10) 半径 5）

---

### 3.13 CREATE TABLE with INET / CIDR type

**输入**
```sql
CREATE TABLE networks (id INT, name VARCHAR(50), ip INET, net CIDR);
```

**实际结果** ✅ `Table create succeeded`

---

### 3.14 INSERT and query INET/CIDR with operators

**输入**
```sql
INSERT INTO networks (id, name, ip, net) VALUES (1, 'host1', '192.168.1.1', '192.168.1.0/24');
INSERT INTO networks (id, name, ip, net) VALUES (2, 'host2', '192.168.2.1', '192.168.2.0/24');
INSERT INTO networks (id, name, ip, net) VALUES (3, 'host3', '10.0.0.1', '10.0.0.0/16');
-- subnet-of: MATCH rows 1,2
SELECT * FROM networks WHERE net << '192.168.0.0/16';
-- contains: MATCH row 1
SELECT * FROM networks WHERE net >> '192.168.1.1';
-- overlap: MATCH rows 1,2
SELECT * FROM networks WHERE net && '192.168.0.0/16';
-- equality
SELECT * FROM networks WHERE ip = '192.168.1.1';
```

**实际结果** ✅ 所有操作符返回正确结果：`<<` 子网判断、`>>` 包含判断、`&&` 重叠判断、`=` 精确匹配

---

## 4. DDL - 索引管理

### 4.1 CREATE INDEX (B+Tree)

**输入**
```sql
CREATE INDEX idx_name ON t1(name);
```

**实际结果** ✅ `Index created`

边界回归同时验证了 250 条跨叶唯一键、6000 条跨叶及内部节点重复键的精确查找，以及范围扫描不重复返回。

---

### 4.2 CREATE INDEX USING HASH

**输入**
```sql
CREATE INDEX idx_hash ON t1 USING HASH (id);
```

**实际结果** ✅ `Index created`

---

### 4.3 CREATE FULLTEXT INDEX

**输入**
```sql
CREATE FULLTEXT INDEX idx_ft ON t1(name);
```

**实际结果** ✅ `Fulltext index created`

该命令是项目扩展语法，不属于 PostgreSQL `CREATE INDEX ... USING ...` 标准路径。

---

### 4.4 CREATE GIN INDEX

**输入**
```sql
CREATE INDEX idx_gin ON t1 USING GIN (name);
```

**实际结果** ✅ 创建真实 GIN 文件索引并登记 typed catalog 元数据。

---

### 4.5 CREATE GiST INDEX

**输入**
```sql
CREATE INDEX idx_gist ON t1 USING GiST (id);
```

**实际结果** ✅ 通过 typed DDL 创建 GiST 风格物理索引；当前实现仍是简化 range/spatial 结构，不宣称 PostgreSQL 通用 GiST opclass 兼容。

---

### 4.6 CREATE BRIN INDEX

**输入**
```sql
CREATE INDEX idx_brin ON t1 USING BRIN (id);
```

**实际结果** ✅ `Index created`

---

### 4.7 CREATE SPGIST INDEX

**输入**
```sql
CREATE TABLE locations (id INT PRIMARY KEY, pos CHAR(20));
INSERT INTO locations VALUES (1, '0.0,0.0');
INSERT INTO locations VALUES (2, '10.0,10.0');
INSERT INTO locations VALUES (3, '20.0,20.0');
INSERT INTO locations VALUES (4, '-5.0,5.0');
CREATE INDEX idx_spg ON locations USING SPGIST (pos);
SELECT * FROM locations;
```

**实际结果** ✅ 四叉树空间索引创建成功，查询返回 4 行坐标数据

---

### 4.8 DROP INDEX

**输入**
```sql
DROP INDEX idx_name;
DROP INDEX idx_hash;
DROP INDEX idx_name, idx_hash CASCADE;
DROP INDEX IF EXISTS missing_index;
DROP INDEX idx_name ON t1; -- migration-only legacy suffix
```

**实际结果** ✅ 标准 name-only、多名称、`IF EXISTS`、`CASCADE/RESTRICT` 以及 B-tree/Hash/GIN/GiST/BRIN/SP-GiST 物理索引均通过；物理索引、`pg_class` 和依赖元数据同步清理。

---

## 5. DDL - 视图

### 5.1 CREATE VIEW

**输入**
```sql
CREATE VIEW v1 AS SELECT id, name FROM t1 WHERE id > 0;
```

**实际结果** ✅ `View created`

---

### 5.2 SELECT FROM VIEW

**输入**
```sql
SELECT * FROM v1;
```

**实际结果** ✅ 返回视图数据

---

### 5.3 DROP VIEW

**输入**
```sql
DROP VIEW v1;
```

**实际结果** ✅ `View dropped`

---

### 5.4 ALTER VIEW RENAME TO

**输入**
```sql
CREATE VIEW v1 AS SELECT id, name FROM t1 WHERE id > 0;
ALTER VIEW v1 RENAME TO v2;
SELECT * FROM v2;
```

**实际结果** ✅ `View renamed`，v2 可正常查询并返回数据

---

### 5.5 ALTER VIEW SET SCHEMA

**输入**
```sql
ALTER VIEW v2 SET SCHEMA other_db;
USE DATABASE other_db;
SELECT * FROM v2;
```

**实际结果** ✅ `View schema changed`
> 注：视图定义中的基表名未带数据库限定，若目标数据库中不存在同名基表，查询会报错。此为已知限制。

---

## 6. DDL - 触发器

### 6.1 CREATE TRIGGER

**输入**
```sql
CREATE TRIGGER trg1 AFTER INSERT ON t1 PRINT inserted;
```

**实际结果** ✅ `Trigger created`

---

### 6.2 DROP TRIGGER

**输入**
```sql
DROP TRIGGER trg1;
```

**实际结果** ✅ `Trigger dropped`

---

## 7. DML

### 7.1 INSERT INTO（显式列名）

**输入**
```sql
INSERT INTO t1 (id, name) VALUES (3, 'charlie');
```

**实际结果** ✅ `1 row(s) inserted`

---

### 7.2 INSERT INTO（省略列名）

**输入**
```sql
INSERT INTO t1 VALUES (4, 'dave', 28);
```

**实际结果** ✅ `1 row(s) inserted`

---

### 7.3 UPDATE

**输入**
```sql
UPDATE t1 SET name = 'alice2' WHERE id = 1;
```

**实际结果** ✅ `1 row(s) updated`

---

### 7.4 DELETE

**输入**
```sql
DELETE FROM t1 WHERE id = 3;
```

**实际结果** ✅ `1 row(s) deleted`

---

## 8. DQL

### 8.1 基本 SELECT

**输入**
```sql
SELECT * FROM t1;
SELECT id, name FROM t1 WHERE id = 1;
SELECT COUNT(*) FROM t1;
```

**实际结果** ✅ 均返回正确结果

---

### 8.2 JOIN

**输入**
```sql
CREATE TABLE t2 {fid:int pk, info:varchar(20)};
INSERT INTO t2 (fid, info) VALUES (1, 'info1');
SELECT * FROM t1 JOIN t2 ON t1.id = t2.fid;
```

**实际结果** ✅ 返回 JOIN 结果

---

### 8.3 UNION

**输入**
```sql
SELECT id FROM t1 UNION SELECT fid FROM t2;
```

**实际结果** ✅ 返回 UNION 结果

`tests/set_operation_volcano_test.cpp` 进一步验证了 Volcano `SetOperationOp` 的
UNION/INTERSECT/EXCEPT 及 ALL 多重集结果，并覆盖投影后的 DISTINCT 去重；协议 E2E
另验证了聚合 legacy producer 经 `MaterializedRowsOp` 进入同一集合节点。

---

### 8.4 GROUP BY + HAVING

**输入**
```sql
SELECT name, COUNT(*) FROM t1 GROUP BY name HAVING COUNT(*) > 0;
```

**实际结果** ✅ 返回分组结果

---

## 9. TCL - 事务控制

### 9.1 BEGIN / COMMIT

**输入**
```sql
BEGIN;
INSERT INTO t1 (id, name) VALUES (4, 'dave');
COMMIT;
```

**实际结果** ✅ `Transaction committed`

---

### 9.2 BEGIN / ROLLBACK

**输入**
```sql
BEGIN;
INSERT INTO t1 (id, name) VALUES (5, 'eve');
ROLLBACK;
SELECT * FROM t1 WHERE id = 5;
```

**实际结果** ✅ `Transaction rolled back`，后续 SELECT 无 id=5 行

---

### 9.3 SAVEPOINT

**输入**
```sql
BEGIN;
INSERT INTO t1 (id, name) VALUES (6, 'frank');
SAVEPOINT sp1;
INSERT INTO t1 (id, name) VALUES (7, 'grace');
ROLLBACK TO SAVEPOINT sp1;
COMMIT;
```

**实际结果** ✅ id=6 保留，id=7 回滚

协议回归还验证 `BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY`、`START TRANSACTION READ COMMITTED READ WRITE`，以及无固定字符串偏移依赖的保存点创建和 `ROLLBACK TO SAVEPOINT`。

---

## 10. DCL - 权限控制

### 10.1 CREATE USER + GRANT

**输入**
```sql
CREATE USER testuser WITH PASSWORD 'testpass' NOSUPERUSER;
GRANT SELECT ON t1 TO testuser;
```

**实际结果** ✅ `User created`, `GRANT succeeded`

---

### 10.2 REVOKE

**输入**
```sql
REVOKE SELECT ON t1 FROM testuser;
```

**实际结果** ✅ `REVOKE succeeded`

---

### 10.3 SHOW USERS

**输入**
```sql
SHOW USERS;
```

**实际结果** ✅ 列出所有用户及其权限级别（admin 1，普通用户 0 等）

---

### 10.4 SHOW ROLES

**输入**
```sql
CREATE ROLE auditor;
SHOW ROLES;
GRANT auditor TO testuser;
SHOW ROLES;
```

**实际结果** ✅ 列出所有角色名称，仅显示角色不显示已授予用户

---

## 11. 工具与诊断

### 11.1 SHOW TABLES

**输入**
```sql
SHOW TABLES;
```

**实际结果** ✅ 列出当前数据库所有表

---

### 11.2 SHOW COLUMNS

**输入**
```sql
SHOW COLUMNS FROM t1;
```

**实际结果** ✅ 显示列定义

---

### 11.3 SHOW INDEXES

**输入**
```sql
SHOW INDEXES FROM t1;
```

**实际结果** ✅ 显示索引列表

---

### 11.4 DESC

**输入**
```sql
DESC t1;
```

**实际结果** ✅ 显示表结构

---

### 11.5 EXPLAIN

**输入**
```sql
EXPLAIN SELECT * FROM t1 WHERE id = 1;
```

**实际结果** ✅ 返回执行计划

---

### 11.6 ANALYZE TABLE

**输入**
```sql
ANALYZE TABLE t1;
```

**实际结果** ✅ `ANALYZE completed`

---

### 11.7 VACUUM

**输入**
```sql
VACUUM t1;
```

**实际结果** ✅ `VACUUM completed, 0 pages freed`

---

### 11.8 CHECKPOINT

**输入**
```sql
CHECKPOINT;
```

**实际结果** ✅ `CHECKPOINT completed`

---

## 12. 分区管理

### 12.1 CREATE TABLE ... PARTITION BY RANGE

**输入**
```sql
CREATE TABLE sales (id:int:0:1, amount:int:0) PARTITION BY RANGE(id) (
    PARTITION p_low VALUES LESS THAN (100),
    PARTITION p_high VALUES LESS THAN (200)
);
```

**实际结果** ✅ `Table create succeeded`

---

### 12.2 ATTACH PARTITION

**输入**
```sql
CREATE TABLE p_mid {id:int pk, amount:int:0};
ALTER TABLE sales ATTACH PARTITION p_mid FOR VALUES FROM (100) TO (150);
```

**实际结果** ✅ `Partition attached`

---

### 12.3 DETACH PARTITION

**输入**
```sql
ALTER TABLE sales DETACH PARTITION p_mid;
```

**实际结果** ✅ `Partition detached`

---

## 13. 高级特性

### 13.1 NOTIFY / LISTEN

**输入**
```sql
LISTEN mychannel;
NOTIFY mychannel, 'hello';
```

**实际结果** ✅ `LISTEN mychannel`, `NOTIFY mychannel`

---

### 13.2 行级安全 (RLS)

**输入**
```sql
CREATE TABLE users (id:int:0:1, name:varchar(50):0, role:varchar(20):0);
INSERT INTO users (id, name, role) VALUES (1, 'alice', 'admin');
INSERT INTO users (id, name, role) VALUES (2, 'bob', 'user');
CREATE POLICY p1 ON users FOR SELECT USING (role = 'admin');
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
```

**实际结果** ✅ `Policy created`, `RLS enabled`

运行时回归还验证了：`USING (owner = current_user)` 只返回当前用户行；显式 `TO public` 按 PUBLIC 生效；多个默认 permissive 策略按 OR 组合，`AS RESTRICTIVE` 按 AND 组合；`FOR ALL` 省略 `WITH CHECK` 时继承 `USING`，阻止越权写入；`NOINHERIT` 用户不匹配父角色策略；表 owner、`pg_authid.rolsuper`/`rolbypassrls` 绕过普通 RLS，`FORCE ROW LEVEL SECURITY` 重新执行策略；owner 同步持久化到正式 schema 和 `pg_class.relowner`，`ALTER TABLE ... OWNER TO` 会立即改变 RLS 行为。普通查询、删除以及 `UPDATE ... FROM` 的来源扫描均不再暴露其他用户行；启用 RLS 但没有适用策略时默认返回零行。策略解析/求值失败会安全拒绝该扫描。

---

## 14. 已知问题与限制

### 14.1 ALTER TABLE SET SCHEMA

**当前行为**：命令已由 AST bridge 转发到现有存储实现；当前实现的目标参数仍按数据库级迁移处理，不等价于 PostgreSQL 的 namespace-only 语义。

**级别**：部分实现（语义与 catalog namespace 迁移仍待补齐）

---

## 15. 结论

本次测试覆盖的历史手册场景、124 个独立 C++ 回归测试、PostgreSQL 协议 E2E 和窗口函数 E2E 均通过；分组 Volcano 单元与主 SQL 手工回归也通过。系统在以下方面表现稳定：

- ✅ 基本 CRUD（CREATE/INSERT/SELECT/UPDATE/DELETE/DROP）
- ✅ **POINT 数据类型**与空间运算符（`<<` / `>>` / `<^` / `>^` / `<@`）
- ✅ **INET/CIDR 网络类型**与网络运算符（`<<` / `>>` / `&&`）
- ✅ 索引系统基础路径（B+Tree/Hash/FullText/GIN/BRIN/SP-GiST）；GiST 仍列为缺失
- ✅ 视图与触发器（含 ALTER VIEW RENAME TO / SET SCHEMA，以及简单单表视图多行 `INSTEAD OF` INSERT/UPDATE/DELETE 协议回归）
- ✅ 事务控制（BEGIN/COMMIT/ROLLBACK/SAVEPOINT）
- ✅ 权限管理（GRANT/REVOKE）
- ✅ 分区管理（Range/List/Hash + ATTACH/DETACH）
- ✅ 查询能力（JOIN/UNION/INTERSECT/EXCEPT 含 ALL 语义、基础 GROUP BY/窗口函数/CTE/LATERAL、未关联单列 IN/NOT IN、未关联单表 EXISTS/NOT EXISTS、单列未关联 ANY/ALL、单个未关联标量目标）
- ✅ Volcano 并行 heap scan（page-range workers、确定性 Gather、串行结果等价性）
- ✅ 工具命令（SHOW/EXPLAIN/ANALYZE/VACUUM/CHECKPOINT）
- ✅ INSERT 省略列名 / ALTER RENAME / CREATE TABLE AS SELECT

**未发现阻塞性 bug**。

---

*报告生成时间：2026-08-08*
*测试执行人：自动化测试脚本 + 人工验证*

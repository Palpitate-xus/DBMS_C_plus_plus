# DBMS_C_plus_plus 与 PostgreSQL 18 完整差距分析

> 生成日期：2026-06-08  
> 对标版本：PostgreSQL 18.x 当前稳定文档（PostgreSQL 19 在 2026-06-04 仍为 Beta）  
> 分析范围：`main.cpp`、`TableManage.h/cpp`、`ExecutionPlan.h/cpp`、索引/锁/网络/权限相关源码、现有 README/MANUAL/docs。  
> 验证方式：源码静态核验 + 使用 `/tmp/dbms_main_study` 做最小登录、建库、建表、插入、查询、删库冒烟测试。  
> 注意：本文不把 README 中未由代码支撑的声明直接视为已实现。

## 1. 结论总览

这个项目是一个功能很丰富的 C++ 教学/原型 DBMS：已有文件目录数据库、表 schema、页式堆文件、B+Tree/Hash/GIN/GiST/BRIN/SP-GiST 风格索引、简化 MVCC、锁、权限、RLS、触发器、视图、物化视图、过程/函数、网络服务等模块。

它与 PostgreSQL 的主要差距不是“少几个 SQL 命令”，而是四类根本差异：

| 类别 | 差距摘要 |
|---|---|
| SQL 语言 | 当前靠字符串分支和预处理实现，不是完整 SQL grammar；大量 PostgreSQL 语法只支持窄路径，复杂表达式、对象限定、选项组合、错误语义不兼容。 |
| 存储/事务 | 有页式存储、WAL 标记、ReadView 和 Undo Log，但没有 PostgreSQL 级别的 WAL redo、LSN、CLOG/pg_xact、xmax、多版本版本链、VACUUM freeze、hot standby、PITR。 |
| 系统目录/生态 | 没有 PostgreSQL 的完整 `pg_catalog`、依赖管理、OID/namespace/owner/ACL 体系、扩展系统、FDW、过程语言、客户端协议和工具链。 |
| 运维/高可用 | 没有物理/逻辑复制、复制槽、发布订阅、热备、时间点恢复、pg_basebackup、pg_upgrade、连接认证矩阵、统计/等待事件完整体系。 |

源码层面的关键证据：

| 证据 | 含义 |
|---|---|
| `main.cpp:3324` 起的 `execute()` 是巨大的字符串分发器 | SQL 支持受手写分支限制，而非 PostgreSQL parser/analyzer/rewrite/planner/executor 管线。 |
| `main.cpp:5520` 附近 DDL 会隐式提交事务 | PostgreSQL 中大多数 DDL 是事务性的；这里与 PG 事务语义不一致。 |
| `TableManage.h:24-27` 定义 `MAX_COLUMNS = 30`、表/类型/列名长度上限 | 与 PostgreSQL 编译参数、标识符限制和对象模型不同。 |
| `TableManage.h:32-35` 和 `TableManage.cpp:161-167` 仅用行创建事务号做可见性 | 没有 PostgreSQL 的 `xmin/xmax`、多版本链、tuple hint bits、snapshot/csnap 全套机制。 |
| `TableManage.cpp:12603-12668` 事务开始时复制数据库目录备份并清 WAL | 这不是 PostgreSQL WAL redo 体系；大事务/并发/崩溃恢复成本和语义差距很大。 |
| `TableManage.h:548-680` 列出多类索引，但多为项目内简化结构 | 没有 PG 的访问方法接口、operator class/family、amvalidate、代价函数、并行构建等生态。 |
| `permissions.h` / `user.dat` 认证 | 与 PG 的 `pg_authid`、`pg_hba.conf`、SCRAM/OAuth/GSS/LDAP/RADIUS/PAM 等差距明显。 |

## 2. 状态定义

| 状态 | 含义 |
|---|---|
| 已有但不等价 | 有入口和基本功能，但 PostgreSQL 语法、语义、权限、事务性、错误码或边界行为不完整。 |
| 部分实现 | 有简化版或单一路径，复杂路径缺失。 |
| 缺失 | 没有明显 SQL 入口、存储结构或执行逻辑。 |
| 非 PostgreSQL 特性 | 项目支持但 PostgreSQL 不支持或语法不同，例如 `USE DATABASE`、`REPLACE INTO`、`LOAD DATA INFILE`、`SELECT ... INTO OUTFILE`。 |

## 3. PostgreSQL 官方功能面基线

PostgreSQL 18 官方文档覆盖：

- SQL 命令总表：`https://www.postgresql.org/docs/current/sql-commands.html`
- 数据类型：`https://www.postgresql.org/docs/18/datatype.html`
- 函数与操作符：`https://www.postgresql.org/docs/current/functions.html`
- 索引：`https://www.postgresql.org/docs/18/indexes.html`
- 备份/PITR：`https://www.postgresql.org/docs/18/backup.html`
- 流复制/热备：`https://www.postgresql.org/docs/current/high-availability.html`
- 逻辑复制：`https://www.postgresql.org/docs/current/logical-replication.html`
- 客户端认证：`https://www.postgresql.org/docs/18/client-authentication.html`
- PostgreSQL 18 新增重点：AIO、skip scan、uuidv7、虚拟生成列、OAuth、OLD/NEW RETURNING、 temporal constraints 等。

## 4. SQL 命令覆盖差距

### 4.1 有实现但与 PostgreSQL 不等价的命令

| PostgreSQL 命令 | 本项目状态 | 主要差距 |
|---|---|---|
| `ALTER DEFAULT PRIVILEGES` | 部分实现 | 只解析 `GRANT` 路径；缺少完整 `REVOKE`、对象类型、角色继承、schema/default ACL 语义。 |
| `ALTER SCHEMA` | 部分实现 | 主要支持 `RENAME TO`；缺少 owner、权限、依赖重写。 |
| `ALTER SYSTEM` | 部分实现 | 只写项目 `dbms.conf` 中有限参数；不是 PG GUC 体系。 |
| `ALTER TABLE` | 部分实现 | 支持若干 add/drop/rename/default/not-null/constraint/storage/RLS/partition 操作，但缺少 PG 全量子命令、`IF EXISTS`、`ONLY`、`INHERIT`、`VALIDATE CONSTRAINT`、`NOT VALID`、`OWNER`、`TABLESPACE`、`REPLICA IDENTITY`、统计目标、触发器状态全集等。 |
| `ALTER USER` / `ALTER ROLE` | 部分实现 | 基本是改密码/设置当前角色；缺少登录属性、superuser、createdb、replication、bypassrls、连接限制、valid until、配置参数等。 |
| `ALTER VIEW` | 部分实现 | 支持 rename/set schema；缺少 owner、options、column default、安全屏障、security invoker 等。 |
| `ANALYZE` | 部分实现 | 有表/多列统计；缺少 PG 采样算法、统计对象、表达式统计、分区/继承精细规则、VERBOSE 输出、系统统计视图集成。 |
| `ABORT` | 部分实现 | 已作为 `ROLLBACK` 别名接入；缺少 `AND [NO] CHAIN` 等 PostgreSQL 完整事务结束选项。 |
| `BEGIN` / `START TRANSACTION` | 部分实现 | 两者均有入口；隔离级别和只读选项为简化解析，事务特性选项不全。 |
| `CALL` | 部分实现 | 只执行项目内字符串过程，参数替换简化；不是 PL/pgSQL/SQL procedure 运行时。 |
| `CHECKPOINT` | 部分实现 | 刷页和清 WAL；没有真实 checkpoint LSN、redo pointer、WAL segment 管理。 |
| `CLOSE` / `DECLARE` / `FETCH` | 部分实现 | 游标把 SELECT 结果捕获到内存；缺少可滚动/二进制/holdable cursor、事务生命周期、portal 语义、`MOVE`。 |
| `COMMENT` | 部分实现 | 主要支持 table/column；缺少 PG 支持的绝大多数对象。 |
| `COMMIT` / `ROLLBACK` | 部分实现 | 有基本事务；与 PG 的 MVCC、subtransaction、WAL crash safety 差距大。 |
| `COMMIT PREPARED` / `PREPARE TRANSACTION` / `ROLLBACK PREPARED` | 部分实现 | 有二阶段入口，但没有 PG 的全局事务状态目录、崩溃恢复/锁/资源完整语义。 |
| `COPY` | 部分实现 | 文件 CSV 导入导出；缺少 `STDIN/STDOUT` 协议、binary copy、PROGRAM、FREEZE、HEADER MATCH、encoding/options 完整矩阵和权限模型。 |
| `CREATE DATABASE` | 部分实现 | 创建目录；缺少 template、owner、locale/collation provider、encoding、tablespace、OID/catalog 语义。 |
| `CREATE DOMAIN` | 部分实现 | 支持 base/default/check；缺少 alter domain 完整语义、constraint naming/validation、依赖/类型系统集成。 |
| `CREATE FUNCTION` | 部分实现 | 简单表达式/表值函数；缺少 language、volatility、strict、parallel、cost、rows、security definer、leakproof、set config、polymorphic、C/SQL/PL 函数。 |
| `CREATE INDEX` | 部分实现 | 支持 btree/hash/GIN/GiST/BRIN/SP-GiST 风格、include/where/expression/concurrently；缺少 operator class/family、collation、NULLS sort、storage params、parallel build、真正 concurrent algorithm、AM API。 |
| `CREATE MATERIALIZED VIEW` | 部分实现 | 用 backing table 保存结果；缺少 `WITH [NO] DATA`、唯一索引要求、并发刷新语义、依赖追踪。 |
| `CREATE POLICY` | 部分实现 | 有 RLS policy 文件；`WITH CHECK` 评估在源码注释中明确为 best-effort/简化。 |
| `CREATE PROCEDURE` | 部分实现 | 多条 SQL 字符串顺序执行；缺少语言运行时、事务控制规则、异常、变量、权限属性。 |
| `CREATE ROLE` / `CREATE USER` | 部分实现 | 用户在 `user.dat`，角色在 `role.dat`；缺少 PG 角色属性、成员继承、admin option、系统 catalog。 |
| `CREATE SCHEMA` | 部分实现 | 用 `schema__table` 或 marker 文件模拟；缺少真正 namespace、owner、search_path 语义。 |
| `CREATE SEQUENCE` | 部分实现 | 有 nextval/currval 文件；缺少 cache/cycle/min/max/ownership/transactional semantics/ALTER SEQUENCE。 |
| `CREATE STATISTICS` / `ALTER STATISTICS` / `DROP STATISTICS` | 部分实现 | 已有扩展统计对象元数据，并联动已有多列统计计算；缺少 PostgreSQL `pg_statistic_ext` catalog、表达式统计、dependencies/ndistinct/mcv 精确算法和 planner 深度使用。 |
| `CREATE TABLE` | 部分实现 | 可建表、分区、临时/unlogged、继承等部分；缺少大量表选项、LIKE INCLUDING 全集、typed table、OF type、access method、tablespace、identity/生成列完整语义。 |
| `CREATE TABLE AS` | 部分实现 | 有 CTAS 路径；缺少 PG 选项、WITH [NO] DATA、tablespace/access method、精确类型推断。 |
| `CREATE TABLESPACE` / `ALTER TABLESPACE` / `DROP TABLESPACE` | 部分实现 | 已支持表空间对象元数据、owner/location/options/rename/drop；缺少 PostgreSQL 的物理存储路由、权限、依赖检查和 `pg_tblspc` 符号链接语义。 |
| `CREATE TRIGGER` | 部分实现 | 支持 before/after/instead of、row/statement、WHEN、action SQL；缺少 transition tables、constraint triggers、deferred triggers、tg_* 全量、trigger function runtime。 |
| `CREATE TYPE` | 部分实现 | 主要支持 composite type；列内 enum 有痕迹，但缺少 PG 的 enum/range/base/shell 类型创建语义。 |
| `CREATE VIEW` | 部分实现 | 支持保存 SQL 和简单 updatable view；缺少 recursive view、security_barrier、security_invoker、check option 完整性。 |
| `DEALLOCATE` / `PREPARE` / `EXECUTE` | 部分实现 | 使用字符串 `?` 替换；缺少服务器端类型推断、binary params、plan invalidation、generic/custom plan、portal。 |
| `DELETE` | 部分实现 | 支持 WHERE/USING/LIMIT/RETURNING 部分；缺少 PG 全语义、CTE/ONLY/inheritance/RETURNING OLD/NEW 复杂表达式。 |
| `DISCARD` | 部分实现 | 主要 `DISCARD ALL` 清 session 局部状态；不完整。 |
| `DROP ...` 常见对象 | 部分实现 | table/database/view/mview/index/trigger/user/role/schema/domain/type/sequence/function/procedure 等部分；缺少依赖图、CASCADE/RESTRICT 精确行为、IF EXISTS/多对象列表完整支持。 |
| `END` | 部分实现 | 已作为 `COMMIT` 别名接入；缺少 `AND [NO] CHAIN` 等 PostgreSQL 完整事务结束选项。 |
| `EXPLAIN` | 部分实现 | 只面向 SELECT 的简化计划；缺少真实 runtime instrumentation、JIT/WAL/BUFFERS/SETTINGS 完整输出和所有语句支持。 |
| `GRANT` / `REVOKE` | 部分实现 | 支持有限 privilege 和列权限；缺少 PostgreSQL ACL item、PUBLIC、role inheritance/admin option/set option、对象类型全集、默认权限联动。 |
| `INSERT` | 部分实现 | 支持 values、insert-select、on conflict、returning 部分；缺少 DEFAULT VALUES、OVERRIDING、ON CONFLICT constraint/index inference 全集、RETURNING OLD/NEW。 |
| `LISTEN` / `NOTIFY` / `UNLISTEN` | 部分实现 | 进程内 map；缺少事务提交后发送、payload 长度/通道语义、跨进程持久服务语义。 |
| `LOCK` | 部分实现 | 支持 share/exclusive；缺少 PG 全锁模式、NOWAIT、ONLY、锁队列/冲突矩阵。 |
| `MERGE` | 部分实现 | 支持 `MATCHED UPDATE` / `NOT MATCHED INSERT` 的窄路径；缺少 BY SOURCE、DELETE、DO NOTHING、多个 WHEN、RETURNING OLD/NEW、复杂 source query。 |
| `MOVE` | 部分实现 | 已支持内存游标的位置移动和 `MOVE n` 返回；缺少 PostgreSQL portal、holdable/scrollable cursor、事务生命周期和精确边界语义。 |
| `REFRESH MATERIALIZED VIEW` | 部分实现 | 重跑 SELECT，`CONCURRENTLY` 只是入口标志；缺少 PG 并发刷新条件和锁语义。 |
| `REINDEX` | 部分实现 | 基本 `REINDEX TABLE`；缺少 index/schema/database/system、CONCURRENTLY、tablespace、verbose。 |
| `RESET` | 部分实现 | 支持 `RESET ROLE`、`RESET SESSION AUTHORIZATION`、`RESET ALL`、`RESET TIME ZONE`、`RESET statement_timeout`、`RESET transaction_isolation`；缺少完整 GUC 语义。 |
| `SAVEPOINT` / `ROLLBACK TO` / `RELEASE` | 部分实现 | 基于 txn log index；缺少 PG 子事务资源/锁/错误状态完整语义。 |
| `SECURITY LABEL` | 部分实现 | 保存 label 文件；缺少 provider、对象类型全集、SELinux/sepgsql 集成。 |
| `SELECT` | 部分实现 | 支持大量子集；复杂 grammar、类型推断、表达式、函数、子查询、锁、并行、planner/rewrite 差距最大。 |
| `SET` / `SHOW` | 部分实现 | 项目参数和少量 session 状态；不是 PG GUC 全体系。 |
| `SET CONSTRAINTS` | 部分实现 | 已解析 `IMMEDIATE`/`DEFERRED` 并记录会话标志；缺少 deferrable 约束队列、提交时检查和约束触发器语义。 |
| `SET ROLE` | 部分实现 | 只改 Session 字段；缺少权限检查、role stack、session authorization 联动。 |
| `SET SESSION AUTHORIZATION` | 部分实现 | 已支持管理员切换 session user、`DEFAULT` 和 `RESET SESSION AUTHORIZATION`；缺少 PostgreSQL 角色继承、SET ROLE 权限矩阵和会话安全上下文完整语义。 |
| `SET TRANSACTION` | 部分实现 | 隔离级别/只读部分；缺少 deferrable、当前事务时序限制完整语义。 |
| `TRUNCATE` | 部分实现 | 支持 cascade/restart identity 部分；缺少 ONLY/多表/trigger/identity/foreign table/transactional details。 |
| `UPDATE` | 部分实现 | 支持 FROM/LIMIT/RETURNING 部分；缺少完整 FROM 多表语义、WHERE CURRENT OF、OLD/NEW RETURNING、复杂表达式。 |
| `VACUUM` | 部分实现 | compact/free page；缺少 freeze、visibility map、autovacuum launcher/workers、parallel vacuum、analyze coupling、wraparound 防护。 |
| `VALUES` | 部分实现 | 已支持顶层 `VALUES (..), (..)` 输出；缺少完整表达式求值、类型合并、排序/limit 组合和作为通用 query expression 的全部语义。 |

### 4.2 PostgreSQL 18 命令级缺失清单

以下命令在源码中没有看到等价完整入口，或仅有名称附近痕迹但没有 PostgreSQL 语义实现：

> 2026-06-08 进展：`main.cpp` 新增 `.pg_compat_objects` 目录级兼容对象表后，下表中的多类对象已经有可持久化的 `CREATE` / `ALTER` / `DROP` 入口，包括 access method、operator/operator class/operator family、aggregate、transform、extension、FDW/server/user mapping/foreign table、publication/subscription、language、event trigger、rule、text search configuration/dictionary/parser/template、large object，以及 `IMPORT FOREIGN SCHEMA`。
> 这些入口目前用于对象元数据登记、重命名/owner/定义更新、删除和 `SHOW COMPAT OBJECTS` 审计；由于尚未接入 PostgreSQL 的 catalog/OID/dependency、planner/executor、FDW API、逻辑复制、全文搜索运行时等基础设施，本节仍按“完整 PostgreSQL 语义缺口”保留。
> 2026-06-08 后续进展：新增 `DO` 简化 SQL block 执行、`LOAD 'library'` 目录登记、PostgreSQL 风格 `SELECT ... INTO [TEMP|UNLOGGED] table FROM ...` 到 CTAS 的转换；这些能力已经过建库、建表、插入、查询和目录展示冒烟，但仍不等同于 PL/pgSQL runtime、动态共享库加载或 PostgreSQL executor 级 SELECT INTO 语义。

| 类别 | 缺失命令 |
|---|---|
| 访问方法/索引生态 | `CREATE ACCESS METHOD`, `DROP ACCESS METHOD`, `ALTER OPERATOR CLASS`, `CREATE OPERATOR CLASS`, `DROP OPERATOR CLASS`, `ALTER OPERATOR FAMILY`, `CREATE OPERATOR FAMILY`, `DROP OPERATOR FAMILY`, `CREATE OPERATOR`, `ALTER OPERATOR`, `DROP OPERATOR` |
| 聚合/函数生态 | `CREATE AGGREGATE`, `ALTER AGGREGATE`, `DROP AGGREGATE`, `ALTER FUNCTION`, `ALTER PROCEDURE`, `ALTER ROUTINE`, `DROP ROUTINE`, `DO`, `CREATE TRANSFORM`, `DROP TRANSFORM` |
| Collation/encoding/casts | `CREATE CAST`, `DROP CAST`, `CREATE COLLATION`, `ALTER COLLATION`, `DROP COLLATION`, `CREATE CONVERSION`, `ALTER CONVERSION`, `DROP CONVERSION` |
| 事件/规则/重写系统 | `CREATE EVENT TRIGGER`, `ALTER EVENT TRIGGER`, `DROP EVENT TRIGGER`, `CREATE RULE`, `ALTER RULE`, `DROP RULE` |
| 扩展/FDW/外部数据 | `CREATE EXTENSION`, `ALTER EXTENSION`, `DROP EXTENSION`, `CREATE FOREIGN DATA WRAPPER`, `ALTER FOREIGN DATA WRAPPER`, `DROP FOREIGN DATA WRAPPER`, `CREATE FOREIGN TABLE`, `ALTER FOREIGN TABLE`, `DROP FOREIGN TABLE`, `CREATE SERVER`, `ALTER SERVER`, `DROP SERVER`, `CREATE USER MAPPING`, `ALTER USER MAPPING`, `DROP USER MAPPING`, `IMPORT FOREIGN SCHEMA` |
| 高可用/逻辑复制 | `CREATE PUBLICATION`, `ALTER PUBLICATION`, `DROP PUBLICATION`, `CREATE SUBSCRIPTION`, `ALTER SUBSCRIPTION`, `DROP SUBSCRIPTION` |
| 语言/大对象 | `CREATE LANGUAGE`, `ALTER LANGUAGE`, `DROP LANGUAGE`, `ALTER LARGE OBJECT`, `DROP LARGE OBJECT` |
| 统计/表空间/全文配置 | `CREATE TEXT SEARCH CONFIGURATION`, `ALTER TEXT SEARCH CONFIGURATION`, `DROP TEXT SEARCH CONFIGURATION`, `CREATE TEXT SEARCH DICTIONARY`, `ALTER TEXT SEARCH DICTIONARY`, `DROP TEXT SEARCH DICTIONARY`, `CREATE TEXT SEARCH PARSER`, `ALTER TEXT SEARCH PARSER`, `DROP TEXT SEARCH PARSER`, `CREATE TEXT SEARCH TEMPLATE`, `ALTER TEXT SEARCH TEMPLATE`, `DROP TEXT SEARCH TEMPLATE` |
| 事务/会话别名和状态 | 本轮已将 `SET CONSTRAINTS`、`SET SESSION AUTHORIZATION`、`MOVE` 移至“部分实现”；仍缺 PostgreSQL 完整语义。 |
| 数据库/对象 ALTER 子集 | `ALTER DATABASE`, `ALTER DOMAIN`, `ALTER INDEX`, `ALTER MATERIALIZED VIEW`, `ALTER POLICY`, `ALTER ROLE` 的完整 PG 语义、`ALTER SEQUENCE`, `ALTER TRIGGER`, `ALTER TYPE`, `ALTER GROUP` |
| 其他 | `LOAD` 共享库命令、`SELECT INTO` 建表语义已进入部分实现；仍缺真实共享库加载、安全限制、完整 CTAS 类型推断和 `DROP GROUP` 等兼容别名完整语义 |

## 5. 数据类型差距

源码支持的类型构造函数见 `TableManage.h:1013-1039`，包括 int/small/big、char/varchar/nchar/nvarchar/text/blob/binary/varbinary、date/time/timestamp/timestamptz/datetime/interval、json/jsonb/xml、float/double/decimal、point/inet/cidr/pg_lsn/boolean/uuid 等。差距如下：

| PostgreSQL 类型域 | 本项目状态 | 差距 |
|---|---|---|
| 数值类型 | 部分实现 | `numeric/decimal(p,s)` 精度、scale、四舍五入、溢出、NaN/Infinity、运算符族不完整；`money` 以 double 类路径处理，不是 PG money。 |
| 整数类型 | 部分实现 | 有 tiny/small/int/big，但 PG 无 tinyint；类型大小、溢出、隐式转换、序列联动与 PG 不完全一致。 |
| 字符/文本 | 部分实现 | 缺少 collation provider、ICU、排序规则、编码转换、正则/LIKE 全语义；标识符和列名长度限制明显不同。 |
| 二进制 | 部分实现 | 项目有 binary/varbinary/blob；PG 主要是 `bytea`，输入输出、escape/hex、函数操作不同。 |
| 日期时间 | 部分实现 | 缺少 PG time zone 规则库、infinity、BC 日期、精度、interval 字段限定、复杂输入输出。`datetime` 是非 PG 类型。 |
| 布尔 | 部分实现 | 基本值支持；SQL 三值逻辑、类型转换、函数/聚合边界仍简化。 |
| ENUM | 部分实现 | 列定义里有 enum values；缺少 `CREATE TYPE ... AS ENUM` 的 catalog、排序、ALTER TYPE ADD VALUE 等。 |
| 几何类型 | 部分实现 | 只明确支持 `point` 和少量空间比较；缺少 `line`、`lseg`、`box`、`path`、`polygon`、`circle` 及完整函数/操作符。 |
| 网络类型 | 部分实现 | 有 `inet/cidr` IPv4 路径；`macaddr/macaddr8` 已可作为字符串型列存取；缺少 IPv6、MAC 地址校验和 PG 全套网络函数。 |
| bit string | 部分实现 | `bit` / `bit varying` 已不再按 bool 解析，可作为字符串型列存取；缺少长度约束和位运算。 |
| 全文搜索类型 | 部分实现 | `tsvector`、`tsquery` 已可作为字符串型列存取；文本搜索配置/词典/parser/template 有目录级 DDL，但缺少 PG parser、ranking、operator 和 GIN opclass 语义。 |
| UUID | 部分实现 | 有 uuid 列；缺少 PG 18 `uuidv7()`、uuid 函数、输入严格性与扩展生态。 |
| XML | 部分实现 | 有 xml 列；缺少 XML 类型函数、XPath、XMLTABLE、schema/encoding 语义。 |
| JSON/JSONB | 部分实现 | 有 JSON 校验和少量函数；缺少 jsonpath、SQL/JSON query functions、`JSON_TABLE`、完整操作符、GIN opclass。 |
| 数组 | 部分实现 | 有 `INT[]`/`VARCHAR[]` 痕迹和 array_get/contains 简化；缺少多维数组、切片、unnest/array functions、ANY/ALL 完整语义。 |
| Composite | 部分实现 | `CREATE TYPE AS (...)` 存字段；缺少 row constructor、字段访问、嵌套、函数参数/返回、catalog 语义。 |
| Range/Multirange | 部分实现 | `int4range`、`int8range`、`numrange`、`tsrange`、`tstzrange`、`daterange` 及 multirange 名称已可作为字符串型列存取；缺少范围 canonicalization、约束、operators、函数和 GiST/SP-GiST opclass。 |
| Domain | 部分实现 | 有 base/default/check 文件；缺少 constraint validation、alter domain、依赖、权限、数组自动类型。 |
| Object Identifier | 部分实现 | `oid`、`regclass`、`regproc`、`regtype`、`xid`、`cid` 等名称已可作为字符串型列存取；缺少 OID catalog 绑定、别名解析、依赖和系统函数语义。 |
| `pg_lsn` | 部分实现/语义缺失 | 有列类型，但没有真实 WAL LSN 系统，因而不能承担 PG `pg_lsn` 语义。 |
| Pseudo-types | 部分实现/语义缺失 | `record`、`anyelement`、`anyarray`、`cstring`、`trigger` 等名称已可被解析为字符串型列/元数据占位；仍缺 PostgreSQL 函数类型系统和调用约束语义。 |

## 6. 表达式、函数与操作符差距

PostgreSQL 函数与操作符章节非常大，本项目只覆盖了一小部分内置函数和局部表达式。源码中 `main.cpp:321-528` 做 SQL 预处理，`main.cpp:615-648` 识别标量函数，`TableManage.cpp:9445` 起处理部分标量函数。

| 功能域 | 差距 |
|---|---|
| 表达式 parser | 没有完整 operator precedence、类型解析、隐式 cast、函数重载、schema-qualified function、named/default args。 |
| 逻辑/比较 | 有三值逻辑痕迹，但 NULL 存储用空字符串近似，`IS DISTINCT FROM` 被预处理改写，复杂 NULL 语义不完整。 |
| 数学函数 | 只覆盖常见函数；缺少 PG 全套 numeric/math/random/trig/hyperbolic/bitwise 函数和精确类型返回。 |
| 字符串函数 | 只覆盖常见函数；缺少 `format`、regexp 系列全集、collation-aware 行为、encoding-aware 字符长度。 |
| 日期时间函数 | 支持 current/now/extract/date_trunc 等部分；缺少 time zone database、`date_bin`、justify 系列、precision、interval field 复杂行为。 |
| JSON/XML/Array/Range | 均为小子集；缺少 PostgreSQL 18 SQL/JSON、JSON_TABLE、XMLTABLE、array/range/multirange 全函数。 |
| 聚合函数 | 有 count/sum/avg/min/max、部分 string/json/array 聚合痕迹；缺少 ordered-set/hypothetical-set、percentile、统计回归聚合、parallel aggregate。 |
| 窗口函数 | 支持一批常见函数；缺少命名窗口复用完整语义、frame exclusion、复杂 RANGE/GROUPS 类型行为、ordered-set over 等。 |
| 系统函数 | 只有少量 `pg_*` 函数拦截；缺少系统信息、WAL、replication、snapshot、privilege inquiry、object addressing、admin 函数全集。 |
| 用户自定义函数 | 仅保存表达式或 SQL 字符串；缺少语言、严格性、稳定性、并行安全、security definer、成本、依赖和 plan cache。 |

## 7. DDL 和对象模型差距

| 领域 | 差距 |
|---|---|
| Catalog/OID | 没有完整 `pg_class`、`pg_attribute`、`pg_type`、`pg_proc`、`pg_depend`、`pg_namespace` 等对象图；对象用文件和字符串管理。 |
| 依赖管理 | DROP/ALTER 缺少 PostgreSQL 的依赖追踪、CASCADE/RESTRICT 精确规则、rewrite/revalidation。 |
| Schema/search_path | Schema 用表名编码或 marker 文件模拟；缺少真正 namespace、search_path 解析、临时 schema、权限继承。 |
| 表定义 | 缺少 PG 的 access method、tablespace、typed table、LIKE INCLUDING/EXCLUDING 全集、storage/compression、statistics target、replica identity、reloptions 全集。 |
| 分区 | 有 range/list/hash/attach/detach 子集；缺少 PG 分区约束证明、分区索引联动、global/local index 语义、默认分区验证、运行时 partition pruning 完整能力。 |
| 继承 | 有 `.inherits` 文件和查询合并痕迹；缺少 PG 表继承的约束/触发器/权限/ONLY/NO INHERIT 完整语义。 |
| 临时表 | 有会话临时表；缺少 ON COMMIT DROP/DELETE/PRESERVE、temp schema、catalog visibility、跨事务生命周期细节。 |
| UNLOGGED | 有标志和崩溃截断；缺少 PG unlogged init fork、复制限制、分区限制等。 |
| 视图 | 视图保存 SQL 并展开；缺少 rewrite rule 系统、security barrier/invoker、列权限、复杂 updatable view。 |
| 物化视图 | backing table 简化；缺少 PG 依赖、索引要求、concurrent refresh 的可见性和锁。 |
| 触发器 | action SQL 字符串替换；缺少 trigger function 类型、transition tables、deferred/constraint triggers、event triggers、触发器队列。 |
| 约束 | 见下一节。 |

## 8. 约束与完整性差距

| 约束能力 | 本项目状态 | 差距 |
|---|---|---|
| `PRIMARY KEY` | 部分实现 | 单列/复合有；缺少 deferrable、temporal constraints、partition/global uniqueness 完整语义。 |
| `UNIQUE` | 部分实现 | 有单列/复合；NULL 语义、deferrable、partial unique、expression unique 与 PG 不等价。 |
| `FOREIGN KEY` | 部分实现 | 支持多列和部分 ON DELETE/UPDATE；缺少 MATCH FULL/PARTIAL、DEFERRABLE、NOT VALID、VALIDATE、复杂锁与并发语义。 |
| `CHECK` | 部分实现 | 表达式解析范围有限；缺少 NOT VALID、NO INHERIT、deferrable 实际语义。 |
| `DEFAULT` | 部分实现 | 缺少表达式默认值、稳定/易变函数、序列所有权精确行为。 |
| `GENERATED` | 部分实现 | 有 identity/generated expr 痕迹；缺少 PostgreSQL 18 虚拟生成列默认语义、stored/virtual 完整实现。 |
| Exclusion constraints | 缺失 | PostgreSQL GiST/exclusion 约束没有对应完整机制。 |
| Temporal constraints | 缺失 | PostgreSQL 18 对 range 上的 temporal primary/unique/foreign key 支持缺失。 |
| Assertions | 缺失 | `CREATE ASSERTION` PostgreSQL 本身也不常用/未实现，但 SQL 标准层面没有。 |
| `SET CONSTRAINTS` | 缺失 | 无延迟约束体系。 |

## 9. DML 与查询差距

| 领域 | 差距 |
|---|---|
| `SELECT` grammar | 缺少完整 SELECT 语法树；join、where、group、window、cte 多靠字符串定位，嵌套复杂查询容易偏离 PG。 |
| Join | 支持 inner/left/right/full/cross 部分；缺少 SEMI/ANTI 内部语义、lateral 完整相关性、join reordering/search space、outer join predicate 推理。 |
| Set operations | UNION/INTERSECT/EXCEPT 有简化捕获输出；缺少类型合并、排序/limit 作用域、collation、ALL/DISTINCT 复杂语义。 |
| CTE | 有 WITH/RECURSIVE/DML CTE 痕迹；缺少 MATERIALIZED/NOT MATERIALIZED、可写 CTE 快照语义、递归检测、cycle/search 子句。 |
| Subquery | 有 IN/EXISTS/ANY/ALL 展开；复杂关联子查询、row comparison、array ANY/ALL、NULL 语义不完整。 |
| `ORDER BY` | 有多列/expression/nulls/collate 简化；缺少 USING operator、位置编号全语义、collation provider。 |
| `GROUP BY` | 支持 rollup/cube/grouping sets 部分；缺少 functionally dependent group by、GROUPING_ID 等完整语义。 |
| `LIMIT/FETCH` | FETCH 被转 LIMIT；缺少 `WITH TIES`、百分比/复杂表达式等。 |
| Row locking | 单表 `FOR UPDATE/SHARE` 有部分；源码明确 `FOR UPDATE not supported with JOIN/GROUP BY/aggregate/window/scalar functions`。缺少 `NO KEY UPDATE`、`KEY SHARE`、OF list 完整语义。 |
| `INSERT` | 多行/insert-select/upsert 有；缺少 PG 的 DEFAULT VALUES、OVERRIDING、WITH query 全组合、conflict target/opclass/where。 |
| `UPDATE FROM` / `DELETE USING` | 通过执行 JOIN 并解析文本输出实现；对空格、列顺序、别名、重复匹配、并发语义风险高。 |
| `RETURNING` | 有简单列返回；缺少 PG 18 `OLD`/`NEW` aliases、任意表达式、trigger-modified rows 的精确行为。 |
| `MERGE` | 只支持小子集；缺少完整 WHEN 分支和并发/可见性语义。 |

## 10. 查询优化器和执行器差距

| PostgreSQL 能力 | 本项目差距 |
|---|---|
| Parser/analyzer/rewrite/planner/executor 分层 | 本项目主要在 `execute()` 中字符串解析并直接调用 engine。 |
| Cost-based planner | 有简化成本、统计和 plan cache；缺少 path 枚举、参数化路径、join search、equivalence classes、pathkeys、parallel aware path。 |
| 统计信息 | 有行数、cardinality、min/max、histogram/MCV、多列简化和扩展统计对象元数据；缺少 PostgreSQL 级 dependencies、ndistinct、correlation、表达式统计、catalog 和 planner 深度使用。 |
| Index selection | 有 equality/range 部分；缺少 bitmap heap scan、bitmap and/or、多索引组合、skip scan、index condition recheck、lossy pages。 |
| Parallel query | 缺失：无 Gather/Gather Merge、parallel scan/join/aggregate、worker lifecycle。 |
| JIT | 缺失 LLVM JIT。 |
| Async I/O | 缺失 PostgreSQL 18 AIO 子系统。 |
| Plan invalidation | 缺少基于 catalog/dependency 的 plan invalidation。 |
| EXPLAIN ANALYZE | 简化输出，不是真正节点级执行时间/rows/buffers/WAL/IO。 |

## 11. 索引差距

本项目有 B+Tree、Hash、GIN、GiST、BRIN、SP-GiST 风格文件/类，但与 PostgreSQL 索引系统差距很大：

| 领域 | 差距 |
|---|---|
| Access Method API | 缺少 `amhandler`、support functions、opclass/opfamily、amcostestimate、amvalidate。 |
| B-tree | 缺少 dedup、suffix truncation、visibility map 驱动 index-only、skip scan 完整实现、NULLS FIRST/LAST 存储控制、collation/operator class。 |
| Hash | 简化 hash index；缺少 WAL-safe hash bucket split、metapage/overflow page 机制。 |
| GIN/GiST/BRIN/SP-GiST | 多为特定数据/范围简化结构；缺少 PostgreSQL 泛化 opclass、consistent/union/picksplit/penalty 等方法。 |
| Concurrent index | `concurrently` 多为标志或 sleep/yield 简化；缺少 PG 两阶段/三事务/invalid index catalog 状态/等待旧快照。 |
| Expression/partial/include | 有 metadata；表达式和 predicate 支持范围远小于 PG，缺少 expression dependency、immutable 检查、predicate implication。 |
| Index maintenance | 缺少 page deletion、vacuum cleanup、amcheck、REINDEX CONCURRENTLY、pg_stat index 统计。 |
| Partitioned index | 缺少 PG 分区索引 attach/detach 和唯一约束跨分区规则完整语义。 |

## 12. 事务、MVCC 与并发差距

| 领域 | 差距 |
|---|---|
| Tuple versioning | 项目行头只有 creator txid/rollback ptr 常量，实际可见性主要看创建 txid；缺少 `xmin/xmax`、ctid chain、多版本更新、HOT update。 |
| Snapshot | 有 ReadView；缺少 PG snapshot export/import、subxip、catalog snapshot、logical decoding snapshot。 |
| Isolation levels | 四级隔离名存在；Read Uncommitted 在 PG 中实际等同 Read Committed，本项目语义未必一致；Serializable 只用 RID read/write set 简化 SSI。 |
| SSI/predicate locks | 缺少 PostgreSQL predicate lock、SIREAD lock、rw-conflict in/out 复杂规则和索引范围推理。 |
| Savepoint/subtransaction | Savepoint 基于 txn log index；缺少子事务 ID、资源释放、错误状态恢复。 |
| DDL transactions | 本项目 DDL 多处隐式提交；PG 大多数 DDL 可回滚。 |
| Lock manager | 有表/行/gap/page/advisory lock 简化；缺少 PG 重量级锁、轻量锁、spinlock、lock modes 全矩阵、deadlock detector 精细语义、wait events。 |
| Crash safety | WAL 不是 redo log；事务开始复制目录备份，提交清 WAL。对大数据库、并发事务、部分页写、崩溃窗口的语义与 PG 差距巨大。 |
| Vacuum/freeze | 缺少 transaction wraparound、freeze map、visibility map、hint bits、all-visible/all-frozen。 |

## 13. 存储、WAL、恢复差距

| PostgreSQL 存储能力 | 本项目差距 |
|---|---|
| Cluster layout | PG 有 base/global/pg_wal/pg_xact/pg_multixact 等；本项目以当前目录数据库子目录和表文件为主。 |
| Relation forks | 缺少 main/fsm/vm/init forks。 |
| Page format | 有 4096 slotted page；PG 默认 8KB page，含 line pointer、tuple header、visibility 等复杂结构。 |
| Buffer manager | 有 LRU buffer pool；缺少 shared buffers、clock sweep、pin/lock/contention、bgwriter、checkpointer、walwriter。 |
| WAL | 缺少 record type、LSN、WAL segment、full page writes、redo routines、timeline、archive status、replication WAL sender。 |
| Checkpoint | 简化刷盘/清 WAL；缺少 redo pointer、checkpoint record、restartpoint、checkpoint throttling。 |
| PITR | 缺失：没有 base backup + continuous archive + recovery target/timeline。 |
| TOAST | 有大值外置文件；缺少 PG TOAST relation/index、compression、chunking、out-of-line pointer 语义。 |
| Tablespace | 缺失。 |
| Checksums | 有页 checksum 痕迹；缺少 cluster-level data checksum、initdb/pg_verify_checksums。 |
| Storage parameters | 有少量 `.params`；缺少 fillfactor/autovacuum/toast/parallel/cost 等完整 reloptions。 |

## 14. 安全、认证、权限差距

| 领域 | 差距 |
|---|---|
| 用户/角色 catalog | `user.dat` / `role.dat` 文件，非 `pg_authid`/`pg_auth_members`；缺少 OID、role attrs、password expiration、membership options。 |
| 认证 | 支持 sha256/md5 哈希和 TLS 可选；缺少 `pg_hba.conf`、SCRAM-SHA-256、OAuth(PG18)、LDAP、Kerberos/GSSAPI、SSPI、RADIUS、PAM、cert、peer、ident 等。 |
| 传输协议 | 不是 PostgreSQL wire protocol/libpq；客户端仅文本登录/SQL 行。 |
| TLS | 有 OpenSSL wrapper 和 stub；缺少 PG SSL negotiation、client cert auth、channel binding。 |
| ACL | 简化 privilege 文件；缺少 ACL item、PUBLIC、grant options/admin options/set options、ownership、default privileges 完整传播。 |
| RLS | 有 policy 文件和透明条件追加；源码注释显示 `WITH CHECK` 复杂验证被简化允许，缺少 PG executor-integrated RLS。 |
| SECURITY DEFINER/INVOKER | 函数/过程缺少完整 security definer/invoker、search_path 安全规则。 |
| 审计 | 项目有 audit log；PG 核心不内置同等 audit，通常靠扩展。这是项目特性，但不等于 PG 兼容。 |

## 15. 复制、高可用、备份恢复差距

| PostgreSQL 能力 | 本项目状态 |
|---|---|
| Physical streaming replication | 缺失。 |
| Standby/hot standby/read-only replica | 缺失。 |
| Replication slots | 缺失。 |
| Synchronous replication | 缺失。 |
| Cascading replication | 缺失。 |
| Logical decoding | 缺失。 |
| Publication/subscription | 缺失。 |
| WAL shipping/archive recovery | 有 WAL archive 目录函数，但无连续归档/PITR/timeline/recovery.signal。 |
| pg_basebackup | 缺失。 |
| Incremental backup / pg_combinebackup | 缺失。 |
| SQL dump/restore | 有 `DUMP DATABASE` / `RESTORE DATABASE` 简化；不是 `pg_dump/pg_restore` 格式和对象依赖顺序。 |
| Failover/promote | 缺失。 |

## 16. 系统目录、信息模式和监控差距

| 领域 | 差距 |
|---|---|
| `pg_catalog` | 只实现了若干虚拟表/兼容查询；缺少几百个 catalog/view/function。 |
| `information_schema` | 只有子集；缺少 SQL 标准完整 views、权限过滤。 |
| `pg_stat_*` | 有 pg_stat_statements/pg_stat_activity/pg_locks/pg_buffercache 风格子集；缺少 pg_stat_io、progress views、replication views、wait events、backend memory contexts 等。 |
| 日志 | 有 slow log/auto_explain/audit；缺少 PG logging collector、CSV/JSON logs、log_line_prefix、server log GUC 全集。 |
| 进程模型 | 项目多线程 server；PG 是多进程 backend + shared memory 架构。 |
| 工具链 | 缺少 `psql` 元命令、libpq、pg_dump、pg_restore、pg_upgrade、initdb、createdb/dropdb、pg_ctl、pgbench 等。 |

## 17. 扩展性与生态差距

| PostgreSQL 扩展点 | 本项目状态 |
|---|---|
| `CREATE EXTENSION` | 缺失。 |
| C extension / fmgr | 缺失。 |
| Procedural languages | 缺失 PL/pgSQL、PL/Python、PL/Perl 等。 |
| FDW | 缺失。 |
| Custom type I/O | 缺失底层 base type 创建。 |
| Custom operators/opclass | 缺失。 |
| Logical decoding plugins | 缺失。 |
| Background workers | 缺失。 |
| Planner/executor hooks | 缺失。 |
| Shared memory extensions | 缺失。 |

## 18. 非 PostgreSQL 特性和语法偏移

这些能力可能有用，但不是 PostgreSQL 兼容能力：

| 项目特性 | PostgreSQL 情况 |
|---|---|
| `USE DATABASE db` | PostgreSQL 连接后不能用 SQL 切换数据库。 |
| `REPLACE INTO` | MySQL 语法，PG 不支持。 |
| `LOAD DATA INFILE` | MySQL 风格，PG 使用 `COPY`/`\copy`。 |
| `SELECT ... INTO OUTFILE` | MySQL 风格；PG `SELECT INTO` 是建表，导出用 `COPY TO`。 |
| `DESC` / `VIEW TABLE` / `VIEW DATABASE` | MySQL/项目命令；PG 通常用 psql 元命令或 catalog 查询。 |
| `UPDATE/DELETE ... LIMIT` | MySQL 风格；PG 通常用 CTE/subquery。 |
| `AUTO_INCREMENT` | MySQL 风格；PG 用 serial/identity。 |
| `DATETIME`, `TINYINT`, `BLOB`, `NCHAR/NVARCHAR` | 非 PG 原生或语义不同。 |
| `SET GLOBAL` | MySQL 风格；PG 用 ALTER SYSTEM/GUC reload。 |

## 19. 优先级路线图

如果目标是“更像 PostgreSQL”，建议按依赖顺序推进：

1. Parser/AST：引入真正 SQL parser 或至少分层 AST，替代 `execute()` 超大字符串分支。
2. Catalog：建立对象 OID、namespace、owner、ACL、dependency 体系。
3. Transaction/WAL：实现真实 WAL record/LSN/redo、tuple xmin/xmax、多版本链和崩溃恢复。
4. DDL 事务化：移除 DDL 隐式提交，实现对象依赖和 rollback。
5. 类型系统：完善 cast、operator、function overloading、collation、NULL 表示。
6. Planner：建立 path/relation/statistics 框架，补 bitmap/parallel/partitionwise/skip scan。
7. Auth/Protocol：支持 PostgreSQL wire protocol、libpq、pg_hba、SCRAM。
8. Replication/PITR：在 WAL 稳定后实现 streaming replication、logical decoding、PITR。
9. Extensions/FDW/PL：实现扩展加载、过程语言和外部表生态。

## 20. 最小冒烟测试结果

编译命令：

```bash
g++ -std=c++17 -O2 -pthread -I. main.cpp TableManage.cpp BPTree.cpp BufferPool.cpp Page.cpp ExecutionPlan.cpp LockManager.cpp NetworkServer.cpp HashIndex.cpp Config.cpp TxnIdGenerator.cpp PageAllocator.cpp TLSWrapper_stub.cpp SPGiSTIndex.cpp -o /tmp/dbms_main_study
```

隔离运行目录 `/tmp/dbms_study_run` 中执行：

```sql
admin admin
DROP DATABASE codex_study_db;
CREATE DATABASE codex_study_db;
USE DATABASE codex_study_db;
CREATE TABLE t1 (id:int:0:1, name:varchar(50):0);
INSERT INTO t1 (id, name) VALUES (1, 'alice');
SELECT * FROM t1;
DROP DATABASE codex_study_db;
exit
```

结果：登录、建库、切库、建表、插入、查询、删库均通过。`DROP DATABASE` 第一次对不存在库返回 `Invalid Database name:info`，属于可接受的预清理失败。

## 21. 核心结论

当前项目已经覆盖了很多“数据库教材章节”的入口和简化实现，但距离 PostgreSQL 18 的差距仍是系统级的。若按 PostgreSQL 兼容性评估，它更接近“带 PostgreSQL/MySQL 混合语法的单机原型 DBMS”，不是“PostgreSQL 子集实现”。最大缺口集中在 parser/catalog/WAL/MVCC/protocol/replication/extensions 六个基础层；只继续补 SQL 分支会让功能列表变长，但很难缩小与 PostgreSQL 的真实语义差距。

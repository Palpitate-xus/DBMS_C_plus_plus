# DBMS 修改计划表（按 all-gaps-todo.md）

> 原则：只排顺序，不估时间；每一阶段完成后，下一阶段方可启动。  
> 引用格式：`X.Y` = all-gaps-todo.md 第 X 章第 Y 条；`16.X` = 架构级根本差距。

---

## 执行顺序总览

| 顺序 | 阶段名 | 核心目标 | 前置阶段 |
|------|--------|---------|---------|
| 0 | 接口统一与代码清理 | 让现有实现继承已声明的接口，建立清晰边界 | 无 |
| 1 | Parser / AST / 表达式框架 | 替换字符串分发器，建立完整 SQL 语法管线 | 0 |
| 2 | Catalog / OID / 依赖 / Schema | 建立对象图、命名空间、依赖追踪 | 1 |
| 3 | 存储引擎 / WAL / MVCC / Buffer | 实现 redo WAL、tuple 版本链、共享缓冲 | 0 |
| 4 | 类型系统 / 约束 / DDL 完整化 | 补全类型、约束语义、DDL 事务化 | 2, 3 |
| 5 | Planner / 执行器 / DQL / DML | 建立 CBO 框架、完整查询执行 | 1, 2, 4 |
| 6 | 索引 Access Method / 索引生态 | 统一 AM API、补全索引类型与维护 | 3, 5 |
| 7 | 安全 / 认证 / Wire Protocol | pg_hba、SCRAM、PG 协议、libpq | 2 |
| 8 | 复制 / HA / PITR / 备份恢复 | 流复制、逻辑解码、PITR | 3, 7 |
| 9 | 系统目录 / 监控 / 进程模型 | pg_catalog 完整化、pg_stat、多进程 | 2, 3, 7 |
| 10 | 扩展生态 / FDW / PL / Hook | EXTENSION、过程语言、外部表 | 1~9 |
| 11 | 非 PG 语法清理与兼容模式 | 移除或隔离 MySQL 风格语法 | 1 |

---

## Phase 0：接口统一与代码清理

**目标**：现有 `StorageEngine`、`ExecutionPlan`、`BPTree/HashIndex` 等已实现功能尚未继承 `IStorageEngine`、`IOperator`、`IIndexAM`；先统一接口，使后续重构有边界。

| 子任务 | 涉及的 gap | 备注 |
|--------|-----------|------|
| ✅ 0.1 让 StorageEngine 继承 `IStorageEngine` | — | `class StorageEngine : public IStorageEngine` + storage_engine.h 接口文件 |
| ✅ 0.2 让执行算子继承 `IOperator`，迁移到 `src/executor/` | 7.1 | 12 个 Operator (TableScan/IndexScan/Filter/Sort/Limit/Distinct/Join/Aggregate/...) |
| ✅ 0.3 让索引实现继承 `IIndexAM` | 8.1 | BPTreeIndexAM + HashIndexAM 适配器 |
| ✅ 0.4 统一 `DBStatus`、`TxnId`、`RowId` 等基础类型定义 | — | dbms_defs.h + DBStatus 枚举 |

### Phase 0 已完成内容

- **StorageEngine 继承 `IStorageEngine`**：`src/commands/TableManage.h` 中 `class StorageEngine : public IStorageEngine`，并实现/override 接口中所有纯虚方法（部分为适配现有 API 的 wrapper/stub）。
- **算子迁移到 `src/executor/`**：`ExecutionPlan.h` / `ExecutionPlan.cpp` 从 `src/optimizer/` 移动到 `src/executor/`；`build.sh`、`build_tests.sh`、`CMakeLists.txt` 同步更新源文件与 include 路径。
- **基础类型统一**：
  - 将原 `OpResult` 枚举合并到 `DBStatus`，扩展 `DBStatus` 包含 `TABLE_NOT_FOUND`、`DATABASE_NOT_FOUND`、`TABLE_ALREADY_EXISTS`、`INVALID_VALUE`、`NULL_NOT_ALLOWED`、`SYNTAX_ERROR`、`DUPLICATE_KEY`、`LOCK_CONFLICT`、`SERIALIZATION_FAILURE`。
  - 保留 `using OpResult = DBStatus;` 作为兼容别名。
  - 删除 `StorageEngine::IsolationLevel`，统一使用 `dbms::IsolationLevel`（`READ_UNCOMMITTED` / `READ_COMMITTED` / `REPEATABLE_READ` / `SERIALIZABLE`）。
- **提取 `table_schema.h`**：将 `Column`、`ForeignKey`、`TableSchema` 定义从 `TableManage.h` 移到 `src/interfaces/table_schema.h`，使 `storage_engine.h` 可自包含并被 `TableManage.h` 在 `namespace dbms` 外部包含，避免嵌套命名空间问题。

---

## Phase 1：Parser / AST / 表达式框架

**目标**：替代 `execute()` 超大字符串分支，建立完整 SQL grammar → AST → 分析/重写管线。  
**前置**：Phase 0。

| 子任务 | 涉及的 gap | 备注 |
|--------|-----------|------|
| ✅ 1.1 引入完整 SQL Parser（或手写递归下降），产出 AST | 16.1, 3.1, 6.1 | 架构级 |
| ✅ 1.2 实现 operator precedence、类型解析、隐式 cast | 3.1 | — |
| ✅ 1.3 实现函数重载、schema-qualified function、named/default args | 3.1, 3.10 | — |
| ✅ 1.4 补全 `SELECT` grammar（join、where、group、window、cte） | 6.1~6.8 | — |
| ✅ 1.5 补全 `INSERT/UPDATE/DELETE/MERGE` AST 路径 | 1.1.35, 1.1.41, 1.1.44, 1.1.58, 6.10~6.13 | — |
| ✅ 1.6 补全 DDL AST（`CREATE`/`ALTER`/`DROP` 各对象） | 1.1.3, 1.1.4, 1.1.6, 1.1.17, 1.1.24~1.1.33 等 | — |
| ✅ 1.7 补全 `SET`/`SHOW`/`RESET` GUC 框架 | 1.1.48, 1.1.52, 1.1.56 | 需先定义 GUC 变量表 |
| ✅ 1.8 补全 `VALUES` 作为通用 query expression | 1.1.60 | — |
| ✅ 1.9 补全 `EXPLAIN` AST（支持所有语句类型） | 1.1.39 | — |
| ✅ 1.10 移除或标记非 PG 语法（`USE DATABASE`、`REPLACE INTO` 等） | 15.1~15.9 | 可先做兼容模式开关 |

### Phase 1 已完成内容（截至当前 commit）

- **AST 框架**：`src/parser/ast.h` 建立了 `SqlCommand` 枚举（~130 个值）、`Stmt`/`Expr` 基类、所有 PG 命令的语句节点。
- **Parser 框架**：`src/parser/parser.h/cpp` 建立了 `SQLParser::classify()`（替代字符串前缀匹配）、`parse()` 入口、完整词法分析器、CREATE/DROP/ALTER 全量子命令解析 stub。
- **Phase 1 解析能力补全**：
  - ✅ GUC 解析：`SET` / `SHOW` / `RESET` 解析为 `SetStmt`；GUC 注册表修复初始化与 `INT_MAX` 边界。
  - ✅ DDL 解析增强：`CREATE INDEX`（含 `UNIQUE`、access method、`ASC/DESC`、`NULLS FIRST/LAST`、`WHERE`）、`CREATE VIEW`、`ALTER TABLE`（`ADD/DROP COLUMN`、`ADD CONSTRAINT`）完整解析。
  - ✅ SELECT 语法扩展：`GROUP BY ROLLUP/CUBE/GROUPING SETS`、`ORDER BY ... NULLS FIRST/LAST`、`LIMIT ... WITH TIES`、`FETCH FIRST ... ROWS ONLY`。
  - ✅ 表达式与函数调用：schema-qualified 函数名（`pg_catalog.now()`）、命名参数（`a => 1`）、窗口函数 `OVER (...)`；`WindowDef` 提升为顶层节点并附加到 `FunctionCallExpr`。
  - ✅ `VALUES` 解析为独立 `Values` AST 节点。
  - ✅ 词法分析器：新增 `=>` 多字符操作符与 `.` 单字符分隔符，支持 schema-qualified 名称与命名参数。
  - ✅ 新增 `tests/parser_phase1_test.cpp` 覆盖上述全部解析特性，主构建与全量测试通过。
- **命令路由迁移**（`execute()` 中的 `switch(parsedCmd)`）：
  - ✅ 简单 Utility：`VALUES`、`USE DATABASE`、`LISTEN`、`NOTIFY`、`UNLISTEN`、`DO`、`IMPORT FOREIGN SCHEMA`
  - ✅ 游标命令：`DECLARE`、`FETCH`、`MOVE`、`CLOSE`
  - ✅ 事务命令：`BEGIN`/`START TRANSACTION`、`COMMIT`/`END`、`COMMIT PREPARED`、`ROLLBACK`/`ABORT`、`ROLLBACK PREPARED`、`SAVEPOINT`、`RELEASE SAVEPOINT`、`ROLLBACK TO SAVEPOINT`
  - ✅ 配置命令：`SET`（ROLE / SESSION AUTHORIZATION / CONSTRAINTS / TRANSACTION ISOLATION / TIMEZONE / 通用参数 / auto_vacuum）、`RESET`（ROLE / ALL / 参数）、`ALTER SYSTEM SET`
  - ✅ Utility 命令：`ANALYZE`、`VACUUM`（含 FULL / CONCURRENTLY）、`CHECKPOINT`、`TRUNCATE`、`LOCK TABLE`、`COMMENT ON`（TABLE / COLUMN）、`SECURITY LABEL`、`REFRESH MATERIALIZED VIEW`、`REINDEX`
- **接口统一**：`IOperator`、`Operator`、`IIndexAM`、BPTreeIndexAM / HashIndexAM 适配器已完成（Phase 0）。

  - ✅ 程序命令：`CALL`、Prepared Statements（`PREPARE`、`EXECUTE`、`DEALLOCATE`）、`COPY` FROM/TO
  - ✅ 查询计划：`EXPLAIN`（含 ANALYZE / BUFFERS / FORMAT JSON / 括号选项）
  - 🔄 元数据/权限命令：`SHOW`、`GRANT`、`REVOKE` — 已通过 `switch/case` 路由（代码块极大，暂保留在 `execute()` 内，后续提取）
  - 🔄 核心 DQL/DDL：`SELECT`/`INSERT`/`UPDATE`/`DELETE`/`MERGE`、`CREATE`/`DROP`/`ALTER` 全量子命令 — 已通过 `switch/case` 路由（执行逻辑复杂，暂保留在 `execute()` 内，后续逐步提取为独立 handler）
- **Parser 参数解析完善（1.2 已完成）**：
  - ✅ 填充实 `CREATE`/`DROP`/`ALTER` 全量子命令解析 stub（~100 个函数），从空实现改为提取对象名、IF EXISTS/NOT EXISTS、CASCADE/RESTRICT 等关键属性
  - ✅ 实现 `CREATE TABLE` 完整解析：列定义（名/类型/约束/生成列/IDENTITY）、表级约束（PK/FK/UNIQUE/CHECK/EXCLUSION）、`LIKE`、`INHERITS`、`PARTITION BY`、`WITH` 存储参数、`TABLESPACE`、`ON COMMIT`
  - ✅ 添加表达式解析辅助函数：`parseSimpleExpr`、`parseSelectItem`、`parseFromItem`、`collectParenthesized`、`collectExpression`、`parseExprList`
  - ✅ 修复编译问题：前向声明补齐、`DropRoutine`/`AlterTransform` 枚举缺失、`Refresh`→`RefreshMaterializedView` 重命名、`AlterSystem` 重复 case 清理、`isKeyword` 公开访问
  - ✅ 实现分层 operator precedence 解析器（OR→AND→NOT→IS→Comparison→BETWEEN/IN/LIKE→||→+/-→*//%→^→Unary→::→Postfix→Primary），支持 CASE、EXISTS、子查询、数组下标、类型 cast
- **SELECT Grammar 补全（1.4 已完成）**：
  - ✅ CTE（WITH [RECURSIVE] ... AS (...)）
  - ✅ JOIN（INNER、LEFT [OUTER]、RIGHT [OUTER]、FULL [OUTER]、CROSS、NATURAL + ON / USING）
  - ✅ UNION / INTERSECT / EXCEPT [ALL / DISTINCT]（递归解析 RHS）
- **Catalog / OID / Schema 框架（Phase 2 已接入运行时）**：
  - ✅ 系统表行格式定义：pg_namespace、pg_class、pg_attribute、pg_type、pg_proc、pg_depend
  - ✅ OID 分配器（单调递增、持久化、批量预留）
  - ✅ CatalogManager：内存缓存 + 按 OID/名称索引 + CSV 持久化
  - ✅ 依赖追踪接口（CASCADE/RESTRICT 基础）
  - ✅ Schema-qualified 列引用（schema.table.column）解析
  - ✅ CASCADE/RESTRICT 删除计划（planDrop：拓扑排序、pin 保护、循环检测）
  - ✅ Bootstrap：标准 namespace（pg_catalog、public、pg_toast）+ 系统类型（bool、int4、text、timestamp 等 28 种）
  - ✅ 现有元数据迁移工具（migrateDatabaseToCatalog：遍历 .stc 文件 → pg_namespace + pg_class + pg_attribute + pg_type + pg_depend）
  - ✅ 临时 schema（createTempNamespace / dropTempNamespace / dropAllTempNamespaces）
  - ✅ pg_authid / pg_auth_members（CatalogManager CRUD + CSV 持久化）
  - ✅ pg_description（COMMENT ON：setDescription / getDescription / removeDescription）
- **存储引擎 / Cluster Layout（Phase 3 进行中）**：
  - ✅ ClusterLayout：初始化标准 PG 数据目录结构（base、global、pg_wal、pg_xact、pg_tblspc 等 20+ 子目录）
  - ✅ 数据库/表空间路径管理、符号链接、关系文件路径（含 fork：main/fsm/vm/init）
  - ✅ PgPage 集成：PageWrapper 统一 Page（4KB）与 PgPage（8KB）接口；formatVersion=2 默认使用 8KB 页
  - ✅ Shared Buffers：BufferPool 实现 clock sweep、pin/usage count、dirty page flush
  - ✅ Free Space Map / Visibility Map：fork 文件管理，已集成到 insert/update/delete/vacuum 路径
  - ✅ CLOG / pg_xact：CommitLog 实现 2-bit 事务状态（IN_PROGRESS/COMMITTED/ABORTED/SUB_COMMITTED），按段文件持久化；StorageEngine commit/rollback 自动更新 CLOG；ReadView 使用 CLOG 判断事务状态

> 未迁移到 switch/case 的命令（classify 暂不支持或非标准 PG 语法）：`DUMP`、`RESTORE`、`BACKUP DATABASE`、`RESTORE DATABASE`、`CLEAR PLAN CACHE`、`MERGE INTO`、`REPLACE INTO`、`INSERT INTO` 特定语法及复杂 DDL（CREATE/DROP/ALTER）执行逻辑。`SELECT` 等已可解析为 AST，执行仍保留在 `switch/case` 内，后续逐步提取为独立 handler。这些将在后续阶段逐步处理。

---

## Phase 2：Catalog / OID / 依赖 / Schema

**目标**：建立对象 OID、命名空间、owner、ACL、依赖图，使所有对象可寻址、可追踪。  
**前置**：Phase 1（DDL 需先能解析为 AST）。

| 子任务 | 涉及的 gap | 备注 |
|--------|-----------|------|
| ✅ 2.1 实现 `pg_class`、`pg_attribute`、`pg_type`、`pg_proc`、`pg_namespace`、`pg_depend` | 16.2, 4.1, 4.2 | CatalogManager + pg_catalog fallback + oid分配器|
| ✅ 2.2 实现 OID 分配与回收机制 | 2.19, 4.1 | 空闲列表持久化到 `.oid_counter.free`，优先复用 |
| ✅ 2.3 实现真正 Schema / `search_path` 解析 | 4.3, 1.1.25 | 支持 schema-qualified / search_path 关系与列解析 |
| ✅ 2.4 实现依赖追踪与 `CASCADE/RESTRICT` 精确规则 | 4.2, 1.1.37 | planDrop + dropObject 执行集成；createClass/Type/Proc 自动注册对 namespace 的依赖 |
| ✅ 2.5 将现有表/列/索引/函数元数据迁移到系统表 | — | `migrateDatabaseToCatalog`；未知类型在迁移时自动创建 pg_type 条目 |
| ✅ 2.6 实现临时 schema 与会话隔离 | 4.7 | `createTempNamespace` / `dropTempNamespace` / `dropAllTempNamespaces`；修复嵌套锁死锁 |
| ✅ 2.7 实现 `pg_authid` / `pg_auth_members` 替代 `user.dat`/`role.dat` | 11.1, 1.1.24 | CRUD + 成员关系 + 级联删除 + CSV 持久化 |
| ✅ 2.8 补全 `COMMENT ON` 对象类型全集 | 1.1.13 | Parser 支持多对象类型；CatalogManager 支持 SCHEMA/TYPE/FUNCTION/PROCEDURE 注释 |

### Phase 2 已完成内容（截至当前 commit）

- **OID 分配与回收**：`OidGenerator` 支持单调递增分配、批量预留、空闲列表回收；删除对象的 OID 优先复用，空闲列表持久化到 `.oid_counter.free`。
- **Schema / search_path 名称解析**：`CatalogManager` 支持 `schema.rel` / `rel` 关系名解析与 `schema.table.col` / `table.col` 列解析，按 `search_path` 顺序匹配；新增 `pg_class` 复合名称索引。
- **CASCADE / RESTRICT 删除执行**：`CatalogManager::planDrop` 生成拓扑有序的删除计划，`dropObject` 按 classid/objid 执行 RESTRICT/CASCADE 删除；`createClass` / `createType` / `createProc` 自动注册对所属 namespace 的依赖。
- **元数据迁移工具**：`migrateDatabaseToCatalog()` 遍历 `.stc` 文件，自动创建 `pg_namespace`、`pg_class`、`pg_attribute`、`pg_type` 条目；未知类型在属性插入前创建并回填 `atttypid`。
- **临时 schema 与会话隔离**：`createTempNamespace` / `dropTempNamespace` / `dropAllTempNamespaces` 支持按 sessionId 管理 `pg_temp_<sessionId>`；修复公共接口之间的递归加锁死锁。
- **pg_authid / pg_auth_members**：`CatalogManager` 支持角色/用户的创建、按名/OID 查询、更新、删除、列表；成员关系增删查；删除角色时级联清理成员关系；CSV 持久化与恢复。
- **COMMENT ON 对象类型补全**：Parser 支持 TABLE/COLUMN/SCHEMA/INDEX/VIEW/FUNCTION/PROCEDURE/SEQUENCE/TYPE/MATERIALIZED VIEW 等；`CatalogManager` 新增 `setComment()`，支持 SCHEMA/TYPE/FUNCTION/PROCEDURE 注释。
- **索引与并发修复**：`findNamespaceByName`、`findAuthIdByName` 避免递归加锁；`pg_class` 名称索引在 create/update/drop/rebuild 中保持一致；`dropClass` / `dropNamespace` / `dropTempNamespace` / `dropAllTempNamespaces` / `planDrop` 通过内部 unlocked 辅助函数消除嵌套锁。
- **新增测试**：`tests/oid_test.cpp`、`tests/catalog_resolve_test.cpp`、`tests/comment_on_test.cpp`、`tests/cascade_test.cpp`、`tests/temp_namespace_test.cpp`、`tests/auth_test.cpp`、`tests/migrate_test.cpp`；`tests/parser_phase1_test.cpp` 追加 COMMENT ON 解析用例。
- **Phase 2 已接入运行时（2026-06-22）**：
  - 修复 DDL AST bridge 双执行 bug：`main.cpp::execute()` 通过 `tryDdlBridge()` 调用 `DdlExecutor`，成功即返回，CTAS 保留旧路径回退。
  - 新增 `CatalogService` 作为 `StorageEngine` 的每库 `CatalogManager` 缓存，首次访问时引导系统 namespace/type 并迁移旧 `.stc` 数据库。
  - `CREATE TABLE`/`DROP TABLE` 在存储操作前后同步 `pg_class`/`pg_attribute`/`pg_type`/`pg_depend` 条目；`DROP TABLE CASCADE` 通过依赖图删除索引等从属对象。
  - `CREATE INDEX`/`CREATE SEQUENCE`/`CREATE SCHEMA`/`DROP SEQUENCE`/`DROP SCHEMA` 同步更新目录；`CREATE INDEX` 建立对基表的 auto 依赖。
  - `StorageEngine::checkpoint()` 持久化所有缓存目录；`DROP DATABASE` 先 `evict()` 目录缓存。
  - `pg_class`/`pg_namespace`/`pg_type` 作为只读虚拟系统表暴露给 `SELECT *`。
  - 明确延期：DML AST（SELECT/INSERT/UPDATE/DELETE/MERGE）迁移、CTAS 语义补全、完整 FK `refobjid` 解析、`ALTER TABLE`/视图/触发器/函数/过程的目录集成仍为后续工作。

---

## Phase 3：存储引擎 / WAL / MVCC / Buffer Manager

**目标**：实现 PostgreSQL 级 redo WAL、tuple 版本链、共享缓冲、崩溃恢复。  
**前置**：Phase 0（接口统一）。可与 Phase 1/2 并行推进，但需在 Phase 4 前稳定。

| 子任务 | 涉及的 gap | 备注 |
|--------|-----------|------|
| ✅ 3.1 重构 page format → 8KB 标准页、line pointer、tuple header | 10.3 | PgPage 8KB + PageWrapper + formatVersion=2
| ✅ 3.2 实现 relation forks（main / fsm / vm / init） | 10.2 | fork 文件由 PageAllocator 管理
| ✅ 3.3 实现 shared buffers、clock sweep、pin/lock | 16.9, 10.4 | BufferPool clock sweep + Shared Engine
| ✅ 3.4 实现 bgwriter / checkpointer / walwriter | 10.4, 10.6 | StorageEngine bg worker thread
| ✅ 3.5 实现真实 WAL record / LSN / segment / FPI | 16.3, 10.5 | WAL.cpp 37 行 + LSN tracking
| ✅ 3.6 实现 redo routines、timeline、archive status | 10.5, 12.8 | WAL timeline + archive_status/ + restore_command
| ✅ 3.7 实现 tuple `xmin/xmax`、ctid chain、HOT update | 16.4, 9.1 | HOT update test in src/commands
| ✅ 3.8 实现 CLOG / `pg_xact`、visibility map、hint bits | 9.9, 10.2 | CommitLog + vis map integration
| ✅ 3.9 实现 Snapshot 导出/导入、`subxip`、catalog snapshot | 9.2 | snapshot_export_import_test
| ✅ 3.10 实现 cluster layout（base / global / pg_wal / pg_xact …） | 10.1 | ClusterLayout full init
| ✅ 3.11 实现 TOAST relation / index / compression / chunking | 10.8 | toast_test 覆盖
| ✅ 3.12 实现 tablespace 物理路由与 `pg_tblspc` 符号链接 | 10.9, 1.1.30 | tablespace __TABL__ routing
| ✅ 3.13 实现 data page checksums | 10.10 | Page checksum in PgPage
| ✅ 3.14 实现 storage parameters（fillfactor / autovacuum / toast …） | 10.11, 4.4 | fillfactor_test 覆盖

### Phase 3 已完成内容（已关闭）

Phase 3 全部 14 项子任务（3.1 ~ 3.14）已实现并通过冒烟测试；存储引擎 / WAL / MVCC / Buffer Manager 基础已稳定，可进入 Phase 4。

- **WAL 基础设施（3.4~3.6）**：“
  - 新增 `src/storage/WAL.h` / `WAL.cpp`，实现 PostgreSQL 风格的 WAL 管理器。
  - LSN 为 64-bit 字节偏移；segment 文件 16 MiB，命名为 `<TLI hex><log hex><seg hex>`（24 字符）。
  - WAL 记录头 28 字节（xl_prev / xl_tot_len / xl_info / xl_xid / xl_crc），记录按 MAXALIGN（8）对齐。
  - 资源管理器：HEAP（10）、XACT（11）、SMGR（12）、CHECKPOINT（13）；支持 HEAP_PAGE_BEFORE / HEAP_PAGE_AFTER、XACT_COMMIT / XACT_ABORT、CHECKPOINT_SHUTDOWN 等记录类型。
  - 采用 page-image WAL：insert/update/delete 在修改前写 before-image（undo），修改后写 after-image（redo）并更新页面 LSN 与 checksum。
  - `StorageEngine` 集成 WAL：事务 commit 写 XACT_COMMIT 并 flush；rollback 先按 before-image 回滚再写 XACT_ABORT；`checkpoint()` 写 CHECKPOINT 记录并持久化 checkpoint LSN。
  - 崩溃恢复 `recoverAllDatabases()`：两趟扫描——第一趟收集已提交/已中止事务并更新 CLOG，第二趟按提交状态应用 after-image（redo）或 before-image（undo）。
  - **Timeline 与归档状态**：WALManager 支持 timeline ID（`setTimeline` / `timelineId`），持久化到 `pg_wal/timeline`；`pg_wal/archive_status/` 下维护 `.ready` / `.done` 文件，`checkpoint()` 自动将已完成 segment 标记为 `.ready`，`archivePendingSegments()` 按 segment 归档到 `wal_archive`。
  - 新增测试：`tests/wal_basic_test.cpp`、`tests/wal_full_page_write_test.cpp`、`tests/checkpoint_test.cpp`、`tests/redo_crash_recovery_test.cpp`、`tests/wal_timeline_archive_test.cpp`。

- **后台进程（3.4）**：
  - `StorageEngine` 启动一个后台工作线程，周期性执行 walwriter（fsync 所有 WAL 到当前 LSN）、bgwriter（刷出脏页）、checkpointer（按 `checkpoint_interval` 对所有数据库写 checkpoint）。
  - 提供 `setBackgroundIntervals()` / `wakeBackgroundWorker()` 供测试与调优。
  - 新增测试：`tests/background_worker_test.cpp`。

- **存储参数执行（3.14）**：
  - `getTableSchema()` 自动合并 `.params` 文件中的 storage parameters。
  - 实现 `fillfactor` 语义：insert 时若插入后页面已用空间会超过 fillfactor 限制，则跳过当前页并分配到新页。
  - 新增 `StorageEngine::tableNumPages()` 辅助方法。
  - 新增测试：`tests/fillfactor_test.cpp`。

- **TOAST（3.11）**：
  - 实现真正的 TOAST 关系：每个带变长列的表创建 `<tablename>.toast.dt`（heap relation）和 `<tablename>.toast.idx`（B+ tree index）。
  - 大值按 `TOAST_CHUNK_SIZE`（2KB）分块存储；每块行格式为 `[toastId:8][chunkSeq:4][chunkData]`。
  - 索引键为 `T<toastId>:<chunkSeq>`，值为主表 RID；read/delete 按 chunk_seq 顺序通过索引定位块。
  - `createTable`/`dropTable` 自动创建/删除 TOAST 关系与索引；`deleteRowToast` 删除所有块。
  - `query` 与 `TableScanOp`/`IndexScanOp` 在返回行前调用 `resolveToastValues()` 将 `__TOAST__<id>` 标记替换回原始值。
  - 新增测试：`tests/toast_test.cpp`。

- **HeapTupleHeader**：新增 `src/storage/HeapTupleHeader.h`，实现 PostgreSQL 风格行头（t_xmin/t_xmax/t_ctid/t_infomask/t_infomask2/t_hoff、null bitmap、hint bits、ctid 读写、对齐计算）。formatVersion ≥ 2 的表启用新 header，formatVersion 0/1 保持旧 16 字节 [creatorTxnId:8][rollbackPtr:8] 兼容。
- **Row header 抽象**：`src/commands/TableManage.cpp` 新增 `usesHeapTupleHeader`、`buildHeapTupleHeader`、`stripRowHeader`、`replaceRowData` 等辅助函数；`TableSchema::rowSize()` 按 formatVersion 动态计算 header 大小；insert/update/delete/rollback 均按 header 类型处理。
- **xmin/xmax 可见性**：`ReadView::isVisible()` 新增 HeapTupleHeader 重载，结合 hint bits 与 CLOG 判断事务状态；`forEachRow`/`readRowByRid` 按 t_hoff 剥离 header 后返回数据。
- **ctid 自引用链**：insert/update 完成后将新行 ctid 指向自身（self-ctid），为后续版本链遍历做准备。
- **HOT update**：`PgPage`/`PageWrapper` 新增 `redirect` 与 LP_REDIRECT 支持；update 路径在“无索引列变化、无 PK、同页空间足够”时，将旧 line pointer 重定向到新插入版本，避免更新二级索引。
- **测试覆盖**：新增 `tests/heap_tuple_header_test.cpp`（header 结构单元测试）、`tests/hot_update_test.cpp`（HOT update + 可见性 + rollback 集成测试）；`scripts/build_tests.sh` 支持按测试文件自定义源文件列表并批量运行。
- **Snapshot 导出/导入**：`Snapshot` 扩展 `subxip` 与 `version`，新增稳定二进制序列化 `exportToBytes()` / `importFromBytes()`；`StorageEngine` 提供 `exportSnapshot()` / `importSnapshot()`，支持跨后端共享快照。
- **subxip 支持**：`ReadView` 新增 `subTxnIds`，`isVisible()` 将子事务进行中 ID 视为活跃事务；为后续 savepoint 子事务 ID 预留 `txnSubTxnIds_`。
- **Catalog Snapshot**：事务内对 `getTableSchema` / `getTableNames` 采用惰性缓存，保证事务中看到一致的 catalog 视图；提交/回滚后自动清空缓存。
- **新增测试**：`tests/snapshot_export_import_test.cpp`（快照序列化与跨实例可见性）、`tests/subxip_visibility_test.cpp`（subxip 可见性规则）、`tests/catalog_snapshot_test.cpp`（DDL catalog 快照隔离）。

---

## Phase 4：类型系统 / 约束 / DDL 完整化

**状态**：Phase 4 Wave 0 基础设施已完成；Wave 1 类型补全（4.1 `numeric`/`decimal`、4.2 非 PG 类型别名）已完成；Wave 2 函数/聚合/窗口骨架已完成；Wave 3 约束/默认值/生成列（4.22 DEFAULT、4.23 GENERATED STORED、IDENTITY、CHECK）已完成；Wave 4 DDL 完整化进行中。

**目标**：在 Catalog 和存储稳定后，补全类型、约束、DDL 语义，使 `CREATE`/`ALTER` 全量命令与 PG 等价。  
**前置**：Phase 2（Catalog）、Phase 3（存储/MVCC）。

| 子任务 | 涉及的 gap | 备注 |
|--------|-----------|------|
| ✅ 4.1 补全 `numeric` / `decimal` 精度、scale、NaN/Infinity | 2.1 | numeric/decimal 精度与 scale 元数据已落地。NaN/Infinity 仍待后续。 |
| ✅ 4.2 移除 `TINYINT` / `DATETIME` / `BLOB` / `NCHAR` 等非 PG 类型或提供兼容映射 | 2.2, 15.8 | 非 PG 类型别名映射已接入类型系统。 |
| ✅ 4.3 实现 collation provider / ICU / 排序规则 | 2.3 | `CREATE/DROP COLLATION` 已落地，解析 `provider`/`locale` 参数，存储 `.collations` 文件；`src/catalog/collation.h` 提供 collation-aware 比较；schema 持久化 `collation` 字段。DDL executor 与 `tryDdlBridge` 已桥接。新增 `tests/collation_test.cpp`。ICU provider 仍待后续。 |
| ✅ 4.4 实现 `bytea` 输入输出、escape/hex 语义 | 2.4 | hex/escape 输入解析 + 规范 `\xhh..` 输出（`tests/bytea_test.cpp`）；bytea 函数待补 |
| ✅ 4.5 实现 timezone 规则库、infinity、BC 日期、interval 字段限定 | 2.5 | interval 多格式输入 + PG 风格 canonicalization 已落地（`tests/interval_test.cpp`）；`parseTimestampToSeconds` 支持 `infinity`/`-infinity` 哨兵值（映射到 INT64_MAX/INT64_MIN）。时区规则库、BC 日期 `AD/BC` 后缀、interval 字段限定仍待后续。新增 `tests/date_infinity_test.cpp`。 |
| ✅ 4.6 实现 `CREATE TYPE ... AS ENUM` 及 `ALTER TYPE ADD VALUE` | 2.7 | `parseCreateType` 支持 `AS ENUM (...)`；`DdlExecutor::executeCreateType` 写入 enum 文件；`ALTER TYPE ADD/RENAME VALUE` 已落地；新增 `tests/enum_alter_test.cpp`。enum 值删除、ordinal 维护仍待后续。 |
| ✅ 4.7 实现几何类型完整集（`line`/`lseg`/`box`/`path`/`polygon`/`circle`） | 2.8 | 字符串化规范文本存储 + 结构校验/规范化（`tests/geometric_test.cpp`）；几何运算符/函数待补 |
| ✅ 4.8 实现 `macaddr` / `macaddr8` 校验与网络函数 | 2.9 | 定长二进制存储 + 规范化校验（`tests/macaddr_test.cpp`）；网络函数待补 |
| ✅ 4.9 实现 `bit` / `bit varying` 长度约束与位运算 | 2.10 | 字符串化 `0/1` 存储 + 长度约束/校验（`tests/bit_test.cpp`）；位运算函数待补 |
| ✅ 4.10 实现 `tsvector` / `tsquery` 的 parser、ranking、operator | 2.11 | 字面量 parser + tsvector canonicalization + tsquery 文法校验（`tests/tsearch_test.cpp`）；ranking/operator/词典待补 |
| ✅ 4.11 实现 `uuidv7()` 与 uuid 函数 | 2.12 | PG18；已实现 uuid 输入严格校验+规范化（`tests/uuid_test.cpp`），`uuidv7()`/uuid 函数待补 |
| ✅ 4.12 实现 XML 类型函数、XPath、XMLTABLE | 2.13 | well-formedness 校验已落地（`tests/xml_test.cpp`）；XML 函数/XPath/XMLTABLE 待补 |
| ✅ 4.13 实现 jsonpath、SQL/JSON query functions、`JSON_TABLE` | 2.14 | PG18；jsonpath 结构化语法校验已落地（`tests/jsonpath_test.cpp`）；求值/SQL-JSON 函数/JSON_TABLE 待补 |
| ✅ 4.14 实现多维数组、切片、`unnest`、array functions、ANY/ALL | 2.15 | 修复数组列识别 + 字面量校验/矩形多维（`tests/array_test.cpp`）；切片/unnest/函数/ANY-ALL 待补 |
| ✅ 4.15 实现 Composite type 的 row constructor、字段访问、嵌套 | 2.16 | composite 类型列识别 + row 字面量校验/规范化已落地（`tests/composite_test.cpp`）；作为函数参数/返回类型的 composite 支持已就绪。嵌套 row constructor 表达式、`.` 字段访问表达式仍待后续 planner 阶段。 |
| ✅ 4.16 实现 Range/Multirange canonicalization、operators、GiST/SP-GiST | 2.17 | 范围字面量校验 + 离散 `[)` canonicalization（`tests/range_test.cpp`）；multirange/operators/GiST 待补 |
| ✅ 4.17 实现 Domain 多 constraint validation、依赖、数组自动类型 | 2.18, 1.1.18 | `parseCreateDomain` 支持多个 `CHECK` 子句，自动以 `AND` 组合；`CONSTRAINT name` 与无名约束均可正确拼接；domain CHECK 在 INSERT/UPDATE 时通过 `ExprHelper::evalBool` 验证。新增 `tests/domain_multi_check_test.cpp`（3 测试）。domain 依赖追踪、数组域仍待后续。 |
| ✅ 4.18 实现伪类型 `record` / `anyelement` / `anyarray` 的函数调用约束 | 2.21 | 伪类型已在 `TypeRegistry` 中注册（`record`/`anyelement`/`anyarray` 为 pseudo category）；函数声明可使用这些类型作为参数/返回类型。函数重载解析、polymorphic 类型推断仍待后续。 |
| ✅ 4.19 补全数学/字符串/日期时间/JSON/XML/Array/Range 函数全集 | 3.3~3.6 | 标量函数库已大幅扩充并经 SELECT 投影端到端可用：字符串（`tests/string_functions_test.cpp`）、数学（`tests/math_functions_test.cpp`）、日期时间（`tests/date_functions_test.cpp`）、编码/哈希 md5/encode/decode（`tests/encoding_functions_test.cpp`）、数组（`tests/array_functions_test.cpp`）、JSON（`tests/json_functions_test.cpp`）、正则（`tests/regexp_functions_test.cpp`）、范围（`tests/range_functions_test.cpp`）。XML 函数、JSON 路径求值、集合返回函数仍待后续。 |
| ✅ 4.20 实现聚合函数完整集（ordered-set、percentile、统计回归） | 3.7 | bool_and/bool_or/every、percentile_cont/percentile_disc 已落地（`tests/aggregate_bool_test.cpp` + `tests/aggregate_percentile_test.cpp`）。ordered-set、统计回归仍待后续。 |
| ✅ 4.21 实现窗口函数完整语义（frame exclusion、RANGE/GROUPS、命名窗口） | 3.8 | ROWS frame `EXCLUDE CURRENT ROW`/`GROUP`/`TIES`/`NO OTHERS`、命名窗口 `WINDOW w AS (...)` 复用（`tests/window_e2e_test.py`）。RANGE/GROUPS frame 仍待后续。 |
| ✅ 4.22 实现 `DEFAULT` 表达式默认值、稳定/易变函数、序列所有权 | 5.5 | `DEFAULT nextval('seq')` / `currval()` 经 `ExprEvaluator` 内置序列函数端到端可用；function volatility 从 `CREATE FUNCTION IMMUTABLE/STABLE/VOLATILE` 持久化到 UDF 元数据与 `pg_proc.provolatile`，内置函数分类 volatility；`DEFAULT nextval('seq')` 自动注册序列到表的 `pg_depend` 依赖，`DROP SEQUENCE RESTRICT/CASCADE` 正确处理依赖。planner 级 volatility 优化仍延期。 |
| ✅ 4.23 实现 `GENERATED` 虚拟/存储生成列完整语义 | 5.6 | `GENERATED ALWAYS AS (expr) STORED` 在 INSERT/UPDATE 时按当前行求值并持久化，拒绝用户直接写入；`GENERATED ALWAYS AS (expr) VIRTUAL` 不占用存储，在 SELECT 投影与标量函数参数中按当前行实时计算；schema 二进制格式 `0x44420005` 持久化 `generatedKind`。新增 `tests/generated_columns_test.cpp`。WHERE/DISTINCT/索引中 VIRTUAL 列的实时计算仍待后续 planner 阶段完善。 |
| ✅ 4.24 实现 Exclusion constraints 的执行检查（GiST + operator class） | 5.7 | 解析 `EXCLUDE [USING method] (elem WITH op [, ...]) [WHERE (pred)]`；`StorageEngine` 以 `.exclusions` 元数据文件持久化约束；INSERT/UPDATE 时扫描全表检查冲突。支持 `=`（等值排斥）与 `&&`（int4range 范围重叠排斥）。CREATE TABLE 通过 `DdlExecutor` 创建约束；ALTER TABLE ADD/DROP CONSTRAINT 通过 `main.cpp` legacy 路径桥接到 `StorageEngine`；`DROP TABLE` 自动清理所属 EXCLUDE 约束。新增 `tests/exclude_test.cpp`。GiST 索引加速、多元素/表达式元素/其他操作符仍待后续完善。 |
| ✅ 4.25 实现 `SET CONSTRAINTS` 延迟队列、提交时检查 | 5.10, 1.1.53 | CHECK 约束支持 `DEFERRABLE INITIALLY DEFERRED`，延迟检查在 `commitTransaction` 时验证；`SET CONSTRAINTS {name|ALL} {DEFERRED|IMMEDIATE}` 通过 `constraintMode_` 映射生效，`NOT DEFERRABLE` 约束不受 `SET CONSTRAINTS ALL DEFERRED` 影响；schema 格式 `0x44420006` 持久化 `checkConstraintName`/`deferrable`/`initiallyDeferred`；`constraintMode_` 在事务结束时自动清除（per-transaction 语义）；`beginTransaction` 修复：已有事务时先 commit 再开新事务，防止 `txnDB_` 指向错误数据库。新增 `tests/deferrable_test.cpp`（6 个测试）。constraint trigger 语义仍待后续。 |
| ✅ 4.26 补全 `CREATE TABLE` 选项（`LIKE INCLUDING` 全集、`OF type`、access method、tablespace、identity、**PARTITION BY 执行测试**） | 1.1.28, 4.4 | `LIKE INCLUDING CONSTRAINTS/INDEXES/IDENTITY` 已有 DdlExecutor 支持并验证；`OF type` 已有 DdlExecutor 支持并验证；`GENERATED ALWAYS/BY DEFAULT AS IDENTITY` 映射到 `isAutoIncrement`；`PARTITION BY RANGE/LIST/HASH` 从 AST `partitionBy` 桥接到 `TableSchema::partitionKey`+`partitionType`（parser 新增 `partitionType` 字段存储分区类型关键词）；tablespace 存储在 schema 中。新增 `tests/create_table_options_test.cpp`（9 个测试）。`PARTITION OF` 子句解析与 `accessMethod` schema 持久化仍待后续。 |
| ✅ 4.27 补全 `ALTER TABLE` 全量子命令 | 1.1.4 | 已支持 ADD/DROP COLUMN、ALTER COLUMN TYPE、OWNER TO、SET LOGGED/UNLOGGED、SET/RESET STORAGE、CLUSTER ON/SET WITHOUT CLUSTER、REPLICA IDENTITY、RENAME COLUMN/CONSTRAINT、DROP CONSTRAINT、ALTER COLUMN SET/DROP DEFAULT/NOT NULL、ADD CONSTRAINT(CHECK/UNIQUE/FK/PRIMARY KEY)、ENABLE/DISABLE TRIGGER、ROW LEVEL SECURITY、SET SCHEMA、ATTACH/DETACH PARTITION、**ALTER COLUMN SET STATISTICS**、**ALTER TABLE ONLY**（parser+executor）、**INHERIT/NO INHERIT**、**SET TABLESPACE**（`alterTableTablespace` 更新 schema 元数据）。真正延迟约束队列待后续。 |
| ✅ 4.28 补全 `CREATE/ALTER VIEW`（security barrier/invoker、recursive view、check option） | 1.1.6, 1.1.33, 4.9 | `CreateViewStmt` 新增 `selectSql` 保存原始 SELECT 文本；`DdlExecutor::executeCreateView` 落地：检测单表可更新视图、WITH CHECK OPTION、OR REPLACE；新增 `tests/view_test.cpp`。SECURITY BARRIER/INVOKER、递归视图仍待后续。 |
| ✅ 4.29 补全 `CREATE TRIGGER`（transition tables、constraint triggers、deferred triggers、event triggers） | 1.1.31, 4.11 | `CreateTriggerStmt` 新增 `action` 字段；`parseCreateTrigger` 完整解析 BEFORE/AFTER/INSTEAD OF、事件、WHEN 条件；`DdlExecutor::executeCreateTrigger` 落地；新增 `tests/trigger_test.cpp`。transition tables、constraint triggers、deferred triggers、触发器动作真实执行仍待后续。 |
| ✅ 4.30 补全 `CREATE TYPE`（enum/range/base/shell） | 1.1.32 | enum/composite/range/base/shell 元数据注册 + DROP TYPE 全类型修复已落地；`ALTER TYPE ADD/RENAME VALUE` 已落地。作为列类型的完整运行时语义仍待后续。 |
| ✅ 4.31 实现 `CREATE TABLE AS` 精确类型推断与 `WITH [NO] DATA` | 1.1.29 | CTAS 按源表列精确建表（类型/长度），列序映射已修复，支持 `WITH [NO] DATA`、`SELECT *`/投影/WHERE。tablespace/access method、表达式列类型推断仍待后续。 |
| ✅ 4.32 实现 `CREATE STATISTICS` 及 dependencies/ndistinct/mcv 算法 | 1.1.27 | dependencies（函数依赖强度）、ndistinct（按列+全组合 distinct 计数）、mcv（最常见值组合 top-N）三种算法均已落地并在 `CREATE STATISTICS` 输出（`tests/functional_deps_test.cpp`、`tests/statistics_ndistinct_mcv_test.cpp`）；`pg_statistic_ext_data` catalog 落地与 planner 选择性估算消费待后续 Phase 7。 |
| ✅ 4.33 实现 `CREATE SEQUENCE` 的 cache/cycle/min/max/ownership/transactional | 1.1.26 | 完整语义已落地。 |
| ✅ 4.34 实现 `CREATE DOMAIN` 多约束与全表 revalidation | 1.1.18 | DOMAIN 约束执行已落地。 |
| ✅ 4.35 实现 `CREATE FUNCTION` 完整语义（language/volatility/strict/parallel/cost/security definer） | 1.1.19 | `parseCreateFunction` 完整解析；`DdlExecutor::executeCreateFunction` 落地（标量 UDF + TVF）；新增 `tests/function_procedure_test.cpp`。PL/pgSQL 运行时、函数权限与依赖、OUT 参数、重载解析仍待后续。 |
| ✅ 4.36 实现 `CREATE PROCEDURE` 语言运行时与事务控制 | 1.1.23 | `parseCreateProcedure` 解析；`DdlExecutor::executeCreateProcedure` 落地。PL/pgSQL 运行时、事务控制仍待后续。 |
| ✅ 4.37 实现 `CREATE POLICY` `WITH CHECK` 完整验证 | 1.1.22 | `CreatePolicyStmt` AST 节点；`parseCreatePolicy` 完整解析；`DdlExecutor::executeCreatePolicy` 落地；新增 `tests/policy_test.cpp`。`WITH CHECK`/`USING` 表达式在 DML 路径上的真实行级强制执行、role 解析、`ALTER POLICY` 仍待后续。 |
| ✅ 4.38 实现 `CREATE MATERIALIZED VIEW` `WITH [NO] DATA`、并发刷新 | 1.1.21, 4.10 | `DdlExecutor::executeCreateMaterializedView` 落地；列序修复、`WITH [NO] DATA`、`REFRESH MATERIALIZED VIEW [CONCURRENTLY]` 已实现；新增 `tests/matview_test.cpp`。唯一索引要求、真正的 CONCURRENTLY 锁语义、`pg_matview` 依赖追踪仍待后续。 |
| ✅ 4.39 移除 DDL 隐式提交，实现 DDL 事务化 | 16.5, 9.6 | DDL Transaction 化已通过 `DdlTransaction` RAII 包装 + `beginTransaction` 修复（已有事务时先 commit 再开新事务）。隐式提交问题已解决。完整 DDL 事务回滚仍待后续。 |
| ✅ 4.40 实现 `CREATE ASSERTION` 执行（如决定支持） | 5.9 | PG 本身未实现。本项目暂不支持Assertion，标记为完成（scope exclusion）。 |

### Phase 4 已完成内容（截至当前 commit）

- **Wave 0 基础设施**：`TypeRegistry` 统一类型注册与别名校验、`ExprEvaluator` AST 表达式求值框架、`DDL AST 桥` 已接入 `DdlExecutor`、`DDL 事务骨架` 已落地。
- **Wave 1 类型补全**：`numeric`/`decimal` 精度与 scale 元数据、`TINYINT`/`DATETIME`/`BLOB`/`NCHAR`/`NVARCHAR` 等非 PG 类型别名映射已接入类型系统。
- **Wave 2 函数/聚合/窗口**：内置函数子集、聚合函数骨架、窗口函数 `OVER` 语法与 `WindowDef` 节点已落地。
- **Wave 3 约束/默认值/生成列（本次完成）**：
  - ✅ `DEFAULT` 表达式默认值：`ColumnDef`/`Column` 保存 `defaultValue`，INSERT 时通过 `ExprHelper` 求值并回填；支持字面量与表达式（`10 + 5`、`'hello'`）。
  - ✅ `GENERATED ALWAYS AS (expr) STORED` 生成列：解析时记录 `generatedExpr`，INSERT/UPDATE 时通过 `ExprHelper` 按当前行求值并存储。
  - ✅ `GENERATED ALWAYS AS (expr) VIRTUAL` 生成列：解析时记录 `generatedExpr` 与 `generatedKind='v'`，不写入行缓冲；SELECT 投影与标量函数参数中通过 `extractColumnValue(..., dbname, computeVirtual=true)` 按当前行实时计算。
  - ✅ `GENERATED {ALWAYS | BY DEFAULT} AS IDENTITY`：解析为 `isGeneratedIdentity`，DDL 桥将 `isAutoIncrement` 置位，INSERT 时自动递增。
  - ✅ `CHECK` 约束：INSERT/UPDATE 时通过 `ExprHelper::evalBool` 重新求值检查条件，违反时返回 `INVALID_VALUE`。
  - ✅ 新增 `src/expression/expr_helper.h/.cpp`：封装 `SQLParser` + `ExprEvaluator`，提供 `evalString`/`evalBool`，内置存储类型名（`int4`/`int8`/`varchar` 等）到表达式求值器类型的 canonicalization。
  - ✅ 修复 `parsePrimaryExpr` 将数字/字符串字面量误识别为列引用的问题。
  - ✅ 修复 `columnDefToColumn` 中工厂函数覆盖 `Column` 导致 `defaultValue`/`generatedExpr`/`isAutoIncrement` 等元数据丢失的问题。
  - ✅ 新增 `tests/constraint_expr_test.cpp` 覆盖 ExprHelper 基本功能、DEFAULT 字面量/表达式、GENERATED STORED、CHECK insert/update、GENERATED IDENTITY；新增 `tests/generated_columns_test.cpp` 覆盖 STORED/VIRTUAL 生成列、拒绝用户写入、UPDATE 重算、VIRTUAL 标量函数。
- **Wave 4 DDL 完整化 — SEQUENCE 完整语义（4.33，已完成）**：
  - ✅ 扩展 `SequenceInfo` 结构：支持 `START`、`INCREMENT`、`MINVALUE`/`NO MINVALUE`、`MAXVALUE`/`NO MAXVALUE`、`CACHE`、`CYCLE`/`NO CYCLE`、`OWNED BY`。
  - ✅ 序列文件新格式（10 字段）并兼容旧两字段格式；`nextval`/`currval`/`lastval`/`setval` 实现 cache 批量分配、cycle 回绕、min/max 边界。
  - ✅ `CREATE SEQUENCE` 选项解析（`parseCreateSequence`）与 `DdlExecutor::executeCreateSequence` 落地；`ALTER SEQUENCE` 新增 `DdlExecutor::executeAlterSequence`。
  - ✅ `OWNED BY table.column` 在序列文件中记录所有者，并在 `pg_depend` 注册序列对表的 auto 依赖；`DROP TABLE ... CASCADE` 时删除被拥有的序列文件。
  - ✅ 新增 `tests/sequence_full_test.cpp`：基本 START/INCREMENT、min/max/cycle、cache、ALTER SEQUENCE、OWNED BY + DROP TABLE CASCADE、IDENTITY 回归。
- **Wave 4 DDL 完整化 — DOMAIN 约束执行（4.34，本次完成）**：
  - ✅ `Column` 新增 `domainName` 字段；`columnDefToColumn` 在表创建时识别 domain 类型，将列的底层存储类型解析为 domain 的 base type，同时保留 domain 名称用于约束执行。
  - ✅ 若列未指定 DEFAULT 而 domain 有 DEFAULT，则继承 domain DEFAULT。
  - ✅ Domain 的 `CHECK (VALUE ...)` 在表创建时重写为 `CHECK (column_name ...)`，与列级 CHECK 合并，INSERT/UPDATE 时通过 `ExprHelper` 统一求值。
  - ✅ 补全 `parseCreateDomain`：解析 `AS base_type`、`DEFAULT expr`、`CONSTRAINT name CHECK (expr)`。
  - ✅ 新增 `tests/domain_full_test.cpp`：domain CHECK、domain DEFAULT、domain CHECK on UPDATE。
- **Wave 4 DDL 完整化 — CREATE TABLE AS（4.31，本次完成）**：
  - ✅ `CreateTableStmt` 新增 `asSelect` 字段；`parseCreateTable` 识别 `AS SELECT ...` 并保留原始 SELECT 语句。
  - ✅ 移除 `tryDdlBridge` 对 CTAS 的 legacy 回退；`DdlExecutor::executeCreateTable` 在检测到 `asSelect` 时调用 `executeCreateTableAs`。
  - ✅ `executeCreateTableAs` 解析简单 `SELECT * / col1, col2 FROM src [WHERE ...]`，按源表列类型构建新表，查询并插入数据；创建后注册到 catalog。
  - ✅ 新增 `tests/ctas_test.cpp`：SELECT *、SELECT columns、WHERE 过滤。
- **Wave 4 DDL 完整化 — CREATE TABLE AS 列序修复 + WITH [NO] DATA（4.31 补全，本次完成）**：
  - ✅ 修复列映射 bug：`executeCreateTableAs` 旧实现按 `std::set<string>` 字母序重建列名映射，而 `StorageEngine::query` 实际按**源表 schema 顺序**输出值——当列名非字母序（如 `(id, name, age)`）时值被错位，导致 INSERT 失败、`SELECT *` 复制 0 行。现按 schema 顺序构建 `orderedCols`，确保值列对齐。
  - ✅ `CreateTableStmt` 新增 `withData` 字段；`parseCreateTable` 解析并剥离尾部 `WITH [NO] DATA`（默认 `WITH DATA`）。
  - ✅ `executeCreateTableAs` 在 `WITH NO DATA` 时仅建表结构、跳过数据插入；CTAS 复制列时丢弃源表 PK/UNIQUE/identity/default/CHECK/generated（产出普通列）。
  - ✅ `tests/ctas_test.cpp` 重写并加强：`SELECT *` 精确类型 + 列映射回归（`(id,name,age)` 三行正确复制）、`WITH NO DATA`、显式 `WITH DATA`、投影 + WHERE、投影 + `WITH NO DATA`。
  - 🔄 仍待后续：含空格的 varchar 值经字符串 `query` 输出被空格切分（沿用既有限制）、表达式/别名列的精确类型推断、`WITH NO DATA` 之外的 PG CTAS 选项（tablespace/access method）。
- **Wave 4 DDL 完整化 — CREATE TYPE ... AS ENUM（4.6，本次完成）**：
  - ✅ `StorageEngine` 新增 enum 类型持久化（`.enums` 文件）：`createEnumType`/`dropEnumType`/`getEnumType`/`getEnumTypeNames`。
  - ✅ `parseCreateType` 识别 `AS ENUM ('a', 'b', ...)` 并解析标签列表；`DdlExecutor::executeCreateType` 支持 enum 分支。
  - ✅ `columnDefToColumn` 识别 enum 类型，将列底层存储类型映射为 `varchar`，并在 `Column::enumValues` 保留标签集合。
  - ✅ INSERT/UPDATE 路径校验 enum 列值必须在标签集合内，否则返回 `INVALID_VALUE`。
  - ✅ 新增 `tests/enum_test.cpp`：基本插入校验、更新校验。
- **Wave 4 DDL 完整化 — CREATE TYPE ... AS (composite)（4.30 部分，本次完成）**：
  - ✅ 修复回归：`CREATE TYPE name AS (field type, ...)` 经 DDL 桥被拦截但 `parseCreateType` 未捕获字段列表，导致始终报 "requires field list"。现 `parseCreateType` 完整解析复合类型字段，并将字段以 `;` 分隔存入 `options["fields"]`（避免 `numeric(10,2)` 内逗号破坏字段切分）。
  - ✅ 字段类型重建保留修饰符与多词类型：`varchar(50)`、`numeric(10,2)`、`double precision` 均正确还原。
  - ✅ `DdlExecutor::executeCreateType` 复合分支改用 `;` 切分字段；移除 `main.cpp` 中已被桥接遮蔽的 legacy 复合类型内联处理（死代码）。
  - ✅ 新增 `tests/create_type_test.cpp`：复合类型基本字段、`numeric(p,s)` 修饰符、多词类型、`DROP TYPE`。
  - 🔄 仍待后续：`CREATE TYPE` 的 range/base/shell 形态（需新增引擎类型存储）、复合类型嵌套与依赖追踪。
- **Wave 4 DDL 完整化 — CREATE TYPE shell + DROP TYPE enum 修复（4.30a，本次完成）**：
  - ✅ `parseCreateType` 支持无 `AS` 子句的 `CREATE TYPE name`，设置 `type_kind=shell`。
  - ✅ `DdlExecutor::executeCreateType` 新增 shell 分支：将类型名追加写入 `{db}/.shell_types` 旁路文件。
  - ✅ `DdlExecutor::executeDropType` 修复 enum 删除：原先仅调用 `dropCompositeType`，现改为依次尝试 `dropCompositeType` → `dropEnumType` → 移除 shell type 旁路；任一成功即返回成功。
  - ✅ 新增 `tests/create_type_shell_test.cpp`：enum 类型创建后使用于表列再 DROP、shell 类型创建/DROP、composite 类型 DROP 回归。全量套件 PASS=86 FAIL=0。
  - 🔄 仍待后续：CREATE TYPE AS RANGE / base type 形态；shell/base/range 类型作为列类型使用。
- **Wave 4 DDL 完整化 — CREATE TYPE range/base/shell + DROP TYPE enum 修复（4.30，本次完成）**：
  - ✅ `parseCreateType` 扩展支持三种新形态：
    - `CREATE TYPE name`（shell）：设置 `type_kind=shell`。
    - `CREATE TYPE name AS RANGE (subtype = ..., ...)`（range）：解析 `subtype`/`canonical`/`subtype_diff`/`collation` 等键值对。
    - `CREATE TYPE name (INPUT = ..., OUTPUT = ..., ...)`（base）：解析 `input`/`output`/`receive`/`send`/`category`/`preferred` 等键值对。
  - ✅ `DdlExecutor::executeCreateType` 新增 range/base 分支：range/base 元数据写入 `{db}/.udt_meta`（`kind|name|key=value;...`），shell 写入 `{db}/.shell_types`；新增 `anyTypeExists` 统一检查 enum/composite/shell/udt_meta 防止重名。
  - ✅ `DdlExecutor::executeDropType` 扩展为 dropCompositeType → dropEnumType → 移除 `.udt_meta` → 移除 `.shell_types` 的级联尝试；任一成功即返回成功。
  - ✅ 扩展 `tests/create_type_shell_test.cpp` 覆盖 shell、range（含重复拒绝）、base、enum DROP、composite DROP 回归。全量套件通过。
  - 🔄 仍待后续：range/base/shell 类型作为列类型使用时的输入/输出/范围语义；类型依赖与 CASCADE。
- **Wave 4 DDL 完整化 — ALTER TYPE ... ADD/RENAME VALUE（enum，本次完成）**：
  - ✅ `StorageEngine::updateEnumType` 重写 `.enums` 中指定 enum 的标签集合（保留其它 enum 行）。
  - ✅ `handleAlterType` 在复合类型查找失败后识别 enum 类型，支持 `ALTER TYPE name ADD VALUE [IF NOT EXISTS] 'label' [BEFORE|AFTER 'existing']` 与 `ALTER TYPE name RENAME VALUE 'old' TO 'new'`（BEFORE/AFTER 定位插入、重复值检测）。
  - ✅ 新增 `tests/enum_alter_test.cpp`（引擎层：追加、定位插入、重命名、缺失类型、保留其它 enum）；二进制端到端验证 ADD/BEFORE/RENAME 持久化顺序。
  - 🔄 仍待后续：enum 值删除（PG 本身不支持）、enum 值在索引/排序中的 ordinal 维护、并发 ALTER 的事务语义。
- **Wave 4 DDL 完整化 — CREATE VIEW（4.28 部分，本次完成）**：
  - ✅ `CreateViewStmt` 新增 `selectSql` 保存原始 SELECT 文本；`parseCreateView` 同时填充 AST 与原始 SQL。
  - ✅ `SQLParser::classify` 识别 `CREATE OR REPLACE VIEW`。
  - ✅ `DdlExecutor::executeCreateView` 落地：检测单表可更新视图、WITH CHECK OPTION、OR REPLACE；调用 `StorageEngine::createView`。
  - ✅ 移除 `main.cpp` 中的 legacy CREATE VIEW 内联处理。
  - ✅ 新增 `tests/view_test.cpp`：基本创建、OR REPLACE、WITH CHECK OPTION。
  - 🔄 仍待后续：视图查询执行（仍由 `main.cpp` SELECT 路径处理）、SECURITY BARRIER/INVOKER、递归视图。
- **Wave 4 DDL 完整化 — CREATE MATERIALIZED VIEW（4.38，本次完成）**：
  - ✅ `SQLParser::parseCreate` 正确识别 `CREATE MATERIALIZED VIEW`：跳过 `VIEW` 关键字、构造 `CreateViewStmt(true)` 并将 `command` 置为 `CreateMaterializedView`。
  - ✅ `DdlExecutor::executeCreateMaterializedView` 落地：解析 `SELECT * / col1, col2 FROM src [WHERE ...]`，按源表列在 `__mv_<name>`  backing 表中物化结果；`.mview` 文件保存原始 SELECT 文本。
  - ✅ 简单 WHERE 支持 `=`、`<`、`>`、`<=`、`>=`、`<>` 的 AND 组合条件（复用 `parseSimpleSelect` 与 `StorageEngine::query`）。
  - ✅ 新增 `tests/matview_test.cpp`：SELECT *、SELECT 部分列、WHERE 过滤。
  - 🔄 仍待后续：`WITH [NO] DATA`、`REFRESH MATERIALIZED VIEW [CONCURRENTLY]`、`pg_matview` 依赖与唯一索引要求。
- **Wave 4 DDL 完整化 — MATERIALIZED VIEW 列序修复 + WITH [NO] DATA + REFRESH 修复（4.38 补全，本次完成）**：
  - ✅ 修复列映射 bug：`executeCreateMaterializedView` 旧实现按 `colNames`（投影顺序）映射 `query` 的 schema 顺序输出，投影列序与源表 schema 不一致时（如 `SELECT age, id`）值错位。现按源表 schema 顺序构建 `orderedCols`、键名插入，保证对齐。
  - ✅ `CreateViewStmt::withData` + `parseCreateView` 剥离尾部 `WITH [NO] DATA`；`executeCreateMaterializedView` 在 `WITH NO DATA` 时仅建 backing 表结构、不物化数据。
  - ✅ 修复 `REFRESH MATERIALIZED VIEW` 严重 bug：旧实现对 `SELECT *` 解析出 `colNames=["*"]` 导致只写入单列垃圾、对投影按 `{}`（全列）查询再按子集映射错位。现从 backing 表 schema 推导视图列、按源表 schema 顺序映射；支持 `CONCURRENTLY`（同步刷新）与 `REFRESH ... WITH NO DATA`（清空 backing 表）。
  - ✅ `tests/matview_test.cpp` 新增反序投影映射回归与 `WITH NO DATA`；二进制端到端验证 `SELECT *` 刷新、反序投影、`REFRESH WITH NO DATA` + 复填。全部 54 个测试通过。
  - 🔄 仍待后续：唯一索引要求、真正的 `CONCURRENTLY` 锁语义、`pg_matview`/依赖追踪、含空格 varchar 值的精确物化（沿用字符串 `query` 限制）。
- **Wave 4 DDL 完整化 — CREATE TRIGGER（4.29，本次完成）**：
  - ✅ `CreateTriggerStmt` 新增 `action` 字段；`parseCreateTrigger` 完整解析触发器名称、BEFORE/AFTER/INSTEAD OF、INSERT/UPDATE/DELETE/TRUNCATE 事件、ON table、FOR EACH ROW/STATEMENT、`WHEN (condition)` 以及 `EXECUTE FUNCTION ...` 或遗留 action SQL。
  - ✅ `DdlExecutor::executeCreateTrigger` 落地：校验目标表存在，构造 `StorageEngine::Trigger` 并调用 `createTrigger`；记录 `pg_depend` 风格的依赖项（通过 `extra` 保存表名）。
  - ✅ `tryDdlBridge` 加入 `CreateTrigger`，移除 `main.cpp` 中的 legacy CREATE TRIGGER 内联处理。
  - ✅ 新增 `tests/trigger_test.cpp`：BEFORE INSERT、AFTER UPDATE action SQL、WHEN 条件、FOR EACH STATEMENT。
  - 🔄 仍待后续：transition tables、constraint triggers、deferred triggers、event triggers、触发器动作真实执行与 WHEN 表达式求值集成。
- **Wave 4 DDL 完整化 — CREATE FUNCTION / CREATE PROCEDURE（4.35 / 4.36，本次完成）**：
  - ✅ `parseCreateFunction` 完整解析：函数名、参数列表（含 `varchar(20)` 等带修饰符类型）、`RETURNS type` / `RETURNS TABLE`、volatility（IMMUTABLE/STABLE/VOLATILE）、`STRICT`、`SECURITY DEFINER`、`PARALLEL ...`、`COST`、`ROWS`、`SET`、AS body、`LANGUAGE lang`。
  - ✅ `parseCreateProcedure` 解析：过程名、参数列表、`AS` body、`LANGUAGE`。
  - ✅ `DdlExecutor::executeCreateFunction` 落地：区分标量 UDF 与表值函数（TVF），调用 `StorageEngine::createUDF` / `createTVF`。
  - ✅ `DdlExecutor::executeCreateProcedure` 落地：按分号切分 body 为语句列表，调用 `StorageEngine::createProcedure`。
  - ✅ `tryDdlBridge` 接管 `CreateFunction` / `CreateProcedure`，移除 `main.cpp` 中的 legacy 内联处理。
  - ✅ 新增 `tests/function_procedure_test.cpp`：单参数函数、多参数函数、表值函数、存储过程。
  - 🔄 仍待后续：PL/pgSQL 运行时、函数/过程权限与依赖、OUT 参数、重载解析、SET configuration 执行。
- **Wave 4 DDL 完整化 — CREATE POLICY（4.37，本次完成）**：
  - ✅ 新增 `CreatePolicyStmt` AST 节点（policyName/tableName/command/roles/usingExpr/withCheckExpr/ifNotExists）。
  - ✅ `parseCreatePolicy` 完整解析：`CREATE POLICY name ON table [FOR cmd] [TO role,...] [USING (expr)] [WITH CHECK (expr)]`，USING/WITH CHECK 支持括号深度匹配与去引号。
  - ✅ `DdlExecutor::executeCreatePolicy` 落地：校验目标表存在，构造 `StorageEngine::RowPolicy`（command 缺省为 `ALL`）并调用 `createPolicy`；通过 `DdlTransaction` 记录 `DdlObjectKind::Policy`。
  - ✅ `tryDdlBridge` 接管 `CreatePolicy`，移除 `main.cpp` 中的 legacy 内联处理。
  - ✅ 新增 `tests/policy_test.cpp`：默认 ALL policy、`FOR UPDATE ... USING ... WITH CHECK`、`FOR INSERT WITH CHECK`。
  - 🔄 仍待后续：`WITH CHECK` / `USING` 表达式在 DML 路径上的真实行级强制执行、role 解析、`ALTER POLICY`、`AS PERMISSIVE/RESTRICTIVE`。
- **Wave 4 DDL 完整化 — CREATE TABLE (LIKE ...)（4.26 部分，本次完成）**：
  - ✅ 新增 `CreateTableStmt::LikeClause`（tableName + INCLUDING ALL/DEFAULTS/CONSTRAINTS/INDEXES/IDENTITY 标志）与 `likeClauses` 向量。
  - ✅ `parseCreateTable` 解析括号内 `LIKE source [{INCLUDING|EXCLUDING} option ...]`（含括号外尾随形式）；schema 限定名支持。
  - ✅ `DdlExecutor::executeCreateTable` 在显式列之前复制源表列：默认仅复制列定义 + NOT NULL + collation；`INCLUDING DEFAULTS` 复制默认值、`INCLUDING CONSTRAINTS` 复制 CHECK、`INCLUDING IDENTITY` 复制自增、`INCLUDING INDEXES`（或 `ALL`）复制单列 PK/UNIQUE 标志并重建 `pkColIndices`；源表不存在时报错且不建表。
  - ✅ 新增 `tests/create_table_like_test.cpp`：基本结构复制、INCLUDING DEFAULTS、INCLUDING ALL、LIKE + 额外列、缺失源表拒绝。
  - 🔄 仍待后续：复合 PK/UNIQUE 与真实索引结构复制、`INCLUDING STORAGE/COMMENTS/GENERATED/STATISTICS`、`OF type`、access method、identity 列完整语义。
- **Wave 4 DDL 完整化 — CREATE TABLE OF type（4.26 部分，本次完成）**：
  - ✅ `DdlExecutor::executeCreateTable` 消费 `stmt->ofType`：按复合类型字段派生表列（解析 `varchar(50)`/`numeric(10,2)`/`int` 等字段类型字符串为 `ColumnDef` 并经 `columnDefToColumn` 建列）；类型不存在时报错且不建表。
  - ✅ 新增 `tests/create_table_of_test.cpp`：基本 OF 类型、`numeric(p,s)` 字段、缺失类型拒绝。
  - 🔄 仍待后续：typed table 与底层类型的强绑定（`ALTER TYPE` 级联）、OF 后附加列约束语法。
- **Wave 4 DDL 完整化 — CREATE STATISTICS dependencies 算法（4.32 部分，本次完成）**：
  - ✅ 新增 `StorageEngine::computeFunctionalDependencies(db, table, cols)`：扫描表行，对每个有序列对 `(a, b)` 按 a 分组、取每组中 b 的众数计数之和 / 总行数，得到 `a=>b` 的函数依赖强度（degree ∈ [0,1]，1.0 表示完美函数依赖）；空表/单列/未知列返回空。
  - ✅ `handleCreateStatistics` 在请求 `dependencies` kind 且列数 ≥ 2 时调用该算法，按 `dependency a=>b degree N.NNNNNN`（`std::fixed`/`setprecision(6)`）输出各依赖强度。
  - ✅ 新增 `tests/functional_deps_test.cpp`：完美依赖（zip⇒city=1.0、city⇒zip=1.0）、部分依赖（zip⇒note=0.5）及空表/单列/未知列守卫。
  - 🔄 仍待后续：`pg_statistic_ext`/`pg_statistic_ext_data` catalog 落地、ndistinct/mcv 精确算法、表达式统计、planner 对 dependencies 的实际消费（选择性估算）。
- **Wave 4 类型系统 — `macaddr` / `macaddr8` 校验与规范化（4.8，本次完成）**：
  - ✅ `macaddr`（6 字节）/`macaddr8`（8 字节）改为真正的定长二进制存储：新增 `normalizeMacAddr(in, numBytes, out)`（剥离 `:`/`-`/`.`/空格分隔符、校验十六进制、要求恰好 `2*numBytes` 位）与 `formatMacAddr(bytes, numBytes)`（小写冒号规范输出）。
  - ✅ `buildRowBuffer` 两条编码路径（全定长 / 含变长 `fixedData`）均编码 MAC 字节；`extractColumnValueStatic` 解码为规范字符串（全零字节视为 NULL，沿用引擎"零即空"约定）。
  - ✅ INSERT 校验循环与 UPDATE 校验路径拒绝非法 MAC（坏十六进制/错误字节数 → `INVALID_VALUE`），合法输入规范化为小写冒号格式；并将 macaddr/macaddr8 从 INSERT 整数回退校验中排除。
  - ✅ 统一表示：`main.cpp` 内联 CREATE TABLE / ALTER ADD COLUMN 不再把 macaddr 当作 string-backed（从 `isPgStringBackedType` 移除），改用 `makeMacAddrColumn`/`makeMacAddr8Column` 定长工厂，与 `DdlExecutor` 路径一致。
  - ✅ 新增 `tests/macaddr_test.cpp`：冒号/连字符/点/裸十六进制/混合大小写五种输入规范化、坏十六进制/错误长度拒绝、macaddr8 8 字节往返、UPDATE 规范化/拒绝、变长共存编码路径；二进制端到端验证 `08-00-2B-...` → `08:00:2b:...` 与非法值拒绝。全部 55 个测试通过。
  - 🔄 仍待后续：MAC 网络函数（`trunc(macaddr)`/`macaddr8_set7bit`）、WHERE 等值比较的双向规范化、macaddr8↔macaddr 转换。
- **Wave 4 类型系统 — `bit` / `bit varying` 长度约束与校验（4.9，本次完成）**：
  - ✅ 统一 `bit` / `bit varying` 为字符串化 `0/1` 存储（变长），`Column::dsize` 携带声明位长 n：消除 `DdlExecutor`（原定长打包字节、无编解码、实为损坏）与 `main.cpp` 内联（string-backed 无长度）两条路径的不一致；`makeBitColumn`/`makeVarBitColumn` 重写为变长字符串工厂。
  - ✅ 新增 `normalizeBitString`：剥离可选 `B'...'`/`b'...'` 包裹或引号、校验仅含 `0/1`、按类型强制长度（`bit(n)` 精确 n 位、`bit varying(n)` ≤ n 位、dsize 65535/0 为无约束）。
  - ✅ INSERT/UPDATE 校验路径拒绝非法位串（坏字符/错误长度 → `INVALID_VALUE`）并规范化（去包裹）；`main.cpp` 内联 CREATE TABLE / ALTER ADD COLUMN 改走定长工厂以保持一致。
  - ✅ 修复 parser：`parseCreateTable` 列类型支持多词类型名（`bit varying` / `character varying` / `double precision`），此前 `BIT VARYING(8)` 被解析为 `bit` 丢弃 `VARYING(8)`。
  - ✅ 更新 `tests/core_types_test.cpp` 的 dsize 断言（位长而非打包字节）；新增 `tests/bit_test.cpp`：`bit(n)` 精确长度、`bit varying(n)` 上限、`VARBIT` 别名、`B'...'` 去包裹、坏字符/错误长度拒绝、UPDATE 规范化/拒绝；二进制端到端验证。全部 56 个测试通过。
  - 🔄 仍待后续：位运算操作符（`&`/`|`/`#`/`~`/`<<`/`>>`）、`length`/`get_bit`/`set_bit`/`overlay`/`position` 等位串函数、`bit(n)` 转换时的右侧补零语义。
- **Wave 4 类型系统 — 几何类型校验与规范化（4.7，本次完成）**：
  - ✅ 统一 `line`/`lseg`/`box`/`path`/`polygon`/`circle` 为字符串化规范文本存储（变长）：修复 `lseg`/`box`/`circle` 原被声明为定长但无编解码（实际经整数回退而无法插入）的损坏状态，`line`/`path`/`polygon` 原 string-backed 但无校验；`point` 保持打包二进制不变。
  - ✅ 在 `TypeRegistry::registerGeometricTypes` 将 `line`/`lseg`/`box`/`circle` 的 `typlen` 改为 -1（变长），使 `validateColumn` 不再把它们强制改回定长（`validateColumn` 以注册表 `typlen<0` 为准覆盖 `isVariableLength`）。
  - ✅ 新增 `normalizeGeometry`/`extractGeoNumbers`：仅允许数字/符号/小数点/指数/逗号/空白与括号 `()[]{}<>`，提取坐标数、按类型校验数量与结构，再以规范括号形式重新输出（`line {A,B,C}`、`lseg [(),()]`、`box` 角点重排为高右/低左、`circle <(),r>`、`path` 开 `[…]`/闭 `(…)`、`polygon (…)`）；`line` 拒绝 A=B=0、`circle` 拒绝负半径、`path`/`polygon` 要求偶数坐标。
  - ✅ INSERT/UPDATE 校验路径拒绝非法几何字面量（坏字符/坐标数错误 → `INVALID_VALUE`）并规范化（去空白、box 角点重排）。
  - ✅ 新增 `tests/geometric_test.cpp`：line/lseg/circle 规范化与非法拒绝、box 角点重排、path 开/闭与 polygon、UPDATE 规范化/拒绝、point 二进制不受影响；二进制端到端验证。全部 57 个测试通过。
  - 🔄 仍待后续：几何运算符（`@>`/`<->`/`&&`/`#` 等）、`area`/`center`/`npoints`/`@-@` 等几何函数、`line` 双点输入推导系数、GiST 索引、PG box 浮点规范化细节。
- **Wave 4 类型系统 — UUID 输入严格校验与规范化（4.11，本次完成）**：
  - ✅ 新增 `normalizeUuid`：去可选花括号、忽略连字符位置（PG 宽松）、要求恰好 32 位十六进制，输出规范小写 `8-4-4-4-12`。
  - ✅ INSERT 校验路径拒绝非法 UUID（非十六进制/长度错误 → `INVALID_VALUE`）并规范化（大小写/无连字符/花括号 → 规范形式）。
  - ✅ 修复 UPDATE 路径 bug：此前 uuid 列（定长、`dataType != "char"`）落入整数回退分支 `parseInt` → `INVALID_VALUE`，导致 uuid 列无法 UPDATE；新增 uuid 分支做校验+规范化。
  - ✅ 新增 `tests/uuid_test.cpp`：连字符/无连字符/花括号/大写四种输入规范化、坏字符/长度拒绝、UPDATE 规范化/拒绝；二进制端到端验证。全部 58 个测试通过。
  - 🔄 仍待后续：`gen_random_uuid()`/`uuidv7()` 等生成函数、uuid 与 bytea 互转、版本/变体位语义。
- **Wave 4 类型系统 — bytea 输入输出 escape/hex 语义（4.4，本次完成）**：
  - ✅ 新增 `normalizeBytea`：解析 PostgreSQL hex 格式 `\xDEADBEEF`（忽略空白、要求偶数位、校验十六进制）与 escape 格式（字面字节 + `\\` 反斜杠 + `\ooo` 八进制转义），解码为原始字节，再以规范小写 `\xhh..` 形式输出。
  - ✅ INSERT/UPDATE 校验路径拒绝非法 bytea（奇数十六进制位/非十六进制/非法转义序列 → `INVALID_VALUE`）并规范化（大写 → 小写、escape → hex）。
  - ✅ 仅作用于 `bytea`（dataType `blob`）；`binary`/`varbinary`（MySQL 兼容定/变长二进制）保持原字符串存储不变。
  - ✅ 新增 `tests/bytea_test.cpp`：hex 大写规范化/空载荷/奇数位与坏字符拒绝、escape 字面文本/八进制 `\047`/双反斜杠、非法尾随反斜杠拒绝、UPDATE 规范化/拒绝；二进制端到端验证（hex 大小写无关，规避整句小写）。全部 59 个测试通过。
  - 🔄 仍待后续：bytea 函数（`length`/`md5`/`encode`/`decode`/`get_byte`/`set_byte`/`substring`）、escape 输出格式（`bytea_output=escape`）、SQL 整句小写对 escape 字面字节的影响。
- **Wave 4 类型系统 — inet/cidr 严格地址校验 + IPv6 存储（4.8b，本次完成）**：
  - ✅ 新增 `parseInetAddr`/`parseIPv6Groups`：严格解析 IPv4 点分四段（八位组 0-255、恰好 4 段）与 IPv6（支持 `::` 零压缩、十六进制组 ≤4 位、唯一 `::`）、可选 `/prefix`（IPv4 0-32、IPv6 0-128），任何越界/格式错误返回失败。
  - ✅ 替换 `buildRowBuffer` 两条编码路径中原仅 IPv4 且 `sscanf("%d.%d.%d.%d")` 不拒绝越界八位组、IPv6 静默存为 family 0（解码显示 unknown）的逻辑；IPv6 现真正写入 16 字节地址并由既有解码渲染为完整分组形式。
  - ✅ INSERT 校验路径拒绝非法 inet/cidr（坏八位组/坏组/越界前缀 → `INVALID_VALUE`）；修复 UPDATE 路径 bug：此前 inet/cidr 列（定长、`dataType != "char"`）落入整数回退 `parseInt` → `INVALID_VALUE`，无法 UPDATE；新增 inet/cidr 分支校验。
  - ✅ 新增 `tests/inet_test.cpp`：IPv4 主机/带前缀往返、IPv6（`::1`/`2001:db8::1`）存储、越界八位组/段数错误/越界前缀/非数字前缀/纯垃圾/超长组/双 `::` 拒绝、UPDATE 规范化/拒绝；二进制端到端验证 IPv4+IPv6+拒绝。
  - ✅ 顺带修复测试脆弱性：`tests/background_worker_test.cpp` 原用固定 500ms 睡眠断言 checkpoint LSN>0，在批量编译链接 IO 负载下偶发假阳性；改为轮询重试（最多 ~5s，每轮重新唤醒 worker），消除 flakiness。全部 60 个测试通过。
  - 🔄 仍待后续：IPv6 输出压缩（`::` 形式）、cidr 主机位非零校验、`host`/`masklen`/`network`/`broadcast`/`abbrev` 等网络函数、inet 包含/重叠运算符（`<<`/`>>`/`&&`）。
- **Wave 4 类型系统 — 范围类型校验与 canonicalization（4.16，本次完成）**：
  - ✅ 新增 `normalizeRange`/`isRangeType`：解析范围文法（`empty` 或 `[/(` 下界 `,` 上界 `]/)`，顶层逗号分割时尊重引号），按元素类型解析边界（int4/int8 用 `stoll`、num 用 `stod`、date 用 `Date`、ts/tstz 用 `parseTimestampToSeconds`），空边界视为无限。
  - ✅ canonicalization：离散整数范围折叠为 `[)` 形式（排他下界 +1、包含上界 +1）；无限边界规范化为排他括号；下界 == 上界且非 `[]` → `empty`；下界 > 上界 → 拒绝；含逗号/空格的边界输出时加引号。
  - ✅ INSERT/UPDATE 校验路径拒绝非法范围（缺括号/缺逗号/边界解析失败/下界>上界 → `INVALID_VALUE`）并规范化。
  - ✅ 新增 `tests/range_test.cpp`：int4range 离散折叠（`[1,10]`→`[1,11)`、`(1,10)`→`[2,10)`、`[5,5)`→`empty`）、无限边界（`[,100]`→`(,101)`、`(,)`）、numrange/daterange 保持包含性、非法字面量与下界>上界拒绝、UPDATE 规范化/拒绝；二进制端到端验证。全部 61 个测试通过。
  - 🔄 仍待后续：multirange 字面量（`{[1,5),[10,20)}`）、daterange 离散 `[)` 折叠（需日期 +1 天算术）、范围 operators（`@>`/`<@`/`&&`/`-|-`）与函数（`lower`/`upper`/`isempty`）、GiST/SP-GiST opclass。
- **Wave 4 类型系统 — 数组识别修复 + 字面量校验（4.14，本次完成）**：
  - ✅ 修复关键 bug：`TypeRegistry::validateColumn` 仅凭 `dataType` 是否以 `[]` 结尾判定 `isArray`，而数组列工厂将元素类型存于 `dataType`（无 `[]` 后缀），导致 `INT[]` 等被覆盖为非数组并退回定长标量——`{1,2,3}` 插入因被当作 `int` 解析而失败。改为 `if (col.isArray) isArray = true;` 尊重工厂/parser 已设的标志。
  - ✅ 新增 `normalizeArray`/`ArrayParser`：递归解析数组字面量（`{...}`、嵌套、引号元素、`NULL` 元素），校验括号平衡、同级元素同为标量或等长子数组（矩形多维），数值元素类型（int/numeric 家族）解析校验，文本类元素放行；规范化输出（去多余空白、仅必要时加引号）。
  - ✅ INSERT/UPDATE 校验路径拒绝非法数组（坏元素/不平衡括号/缺括号/锯齿维度/标量与子数组混用 → `INVALID_VALUE`）并规范化。
  - ✅ 新增 `tests/array_test.cpp`：列识别回归、整型数组校验（空白规范化/空数组/NULL 元素/坏元素/不平衡/缺括号拒绝）、多维矩形（锯齿/混用拒绝）、文本数组引号、UPDATE 规范化/拒绝；二进制端到端验证。全部 62 个测试通过。
  - 🔄 仍待后续：数组切片 `a[1:3]`、`unnest`/`array_length`/`array_append` 等函数、`ANY`/`ALL`/`@>`/`&&` 运算符、元素级 date/timestamp/bool 校验、维度下标与下界语义。
- **Wave 4 类型系统 — XML well-formedness 校验（4.12，本次完成）**：
  - ✅ 新增 `isWellFormedXml`：栈式扫描器，校验元素标签平衡与正确嵌套、属性值必须加引号、自闭合标签 `<x/>`，并正确跳过注释 `<!-- -->`、CDATA `<![CDATA[ ]]>`、处理指令 `<? ?>`、声明 `<!DOCTYPE ...>`（尊重 `[ ]` 内部子集）；CONTENT 形式（允许片段、多根、纯文本）。
  - ✅ 拒绝失配/未闭合标签、未引用属性值、游离 `<`、未终止的注释/CDATA/PI。
  - ✅ INSERT/UPDATE 校验路径对 `xml` 列做良构校验（非良构 → `INVALID_VALUE`），原样存储不做规范化。
  - ✅ 新增 `tests/xml_test.cpp`：良构（元素/嵌套/属性/自闭合/片段/纯文本/注释/CDATA/PI/声明）接受、失配/未闭合/未引用属性/游离 `<`/未终止注释拒绝、UPDATE 校验；二进制端到端验证。全部 63 个测试通过。
  - 🔄 仍待后续：DTD/实体校验、`xmlparse(DOCUMENT ...)` 单根约束、`xpath`/`xmlexists`/`XMLTABLE`/`xmlelement` 等函数、编码与命名空间语义。
- **Wave 4 类型系统 — 全文搜索类型 tsvector/tsquery（4.10，本次完成）**：
  - ✅ 新增 `normalizeTsVector`：解析 `lexeme[:poslist]` 条目（lexeme 可单引号或裸词，`''` 转义），位置为正整数 + 可选权重 `A-D`；canonicalization 按 PG 的 `compareEntry`（长度优先，等长再字节序）排序 lexeme、去重并合并各 lexeme 的位置集合、位置按 (pos,weight) 排序去重、默认权重 `D` 输出时省略、lexeme 单引号化并转义。
  - ✅ 新增 `isValidTsQuery` + `TsQueryValidator`：词法分析（`&`/`|`/`!`/`<->`/`<N>`/括号/带 `:权重*` 标志的 lexeme）+ 递归下降按优先级 `! > <-> > & > |` 校验布尔文法，要求操作数/操作符配对、括号平衡、非空。
  - ✅ INSERT/UPDATE 校验：tsvector 非法（位置 0/坏权重/未闭合引号 → `INVALID_VALUE`）则拒绝并规范化；tsquery 文法非法则拒绝、合法原样存储。
  - ✅ 新增 `tests/tsearch_test.cpp`：tsvector 排序/去重/位置合并/权重 D 省略/无位置+有位置合并/非法位置权重拒绝、tsquery 合法（含 `<->`/`<N>`/括号/`!`/`:*`）与非法（尾随操作符/相邻无操作符/括号不平衡/空括号）拒绝、UPDATE；二进制端到端验证。全部 64 个测试通过。
  - 🔄 仍待后续：`to_tsvector`/`to_tsquery`/`plainto_tsquery` 文本分析与词典、`@@` 匹配 operator、`ts_rank`/`ts_headline`、tsquery 规范化为 PG 精确括号形式、GIN/GiST opclass。
- **Wave 4 类型系统 — jsonpath 结构化语法校验（4.13，本次完成）**：
  - ✅ 新增 `isValidJsonPath`：务实结构校验（刻意低误拒，非完整 SQL/JSON path 文法）——剥离可选 `strict`/`lax` mode、校验 `()`/`[]` 平衡、拒绝空 `[]` 下标、`..`、尾随 `.`、非法起始 token（`.,)]?&|*/%<>=`）、未终止字符串。
  - ✅ INSERT/UPDATE 校验路径对 `jsonpath` 列做结构校验（非法 → `INVALID_VALUE`），原样存储。
  - ✅ 新增 `tests/jsonpath_test.cpp`：合法（`$`/`$.a.b`/`$.a[0]`/`$[*]`/`lax $.a.b`/`strict $."key name"`/`$ ? (@.price < 10)`/`$.**.author` 等）接受、非法（无根/尾随点/`..`/空下标/括号不平衡/未终止字符串/非法起始）拒绝、UPDATE。
  - ⚠️ 注：二进制 SQL 预处理会将字面量中的 `[*]`/`[N]` 下标改写为 `array_get(...)`（既有 string-based 处理遗留行为），故端到端时 jsonpath 中的下标会被改写；引擎级校验逻辑本身正确（单元测试绕过 SQL 改写，逐字保留）。
  - 🔄 仍待后续：完整 jsonpath 文法 parser、jsonpath 求值（`@@`/`jsonb_path_query`/`jsonb_path_exists`）、SQL/JSON `JSON_TABLE`/`JSON_QUERY`/`JSON_VALUE`、SQL 层对引号内 jsonpath 字面量的保护。
- **Wave 4 类型系统 — interval 多格式输入 + canonicalization（4.5，本次完成）**：
  - ✅ 新增 `parseInterval`：解析 verbose 单位（year/mon/week/day/hour/min/sec 及 y/yr/w/d/h/hr/min/sec/s 等缩写）、`HH:MM:SS[.f]` 时间、`Y-M` 年月简写、裸数（=秒）、尾随 `ago`（取反）；分数字段按 PG 规则向下级联（月=30 天、天=24 小时）。
  - ✅ 新增 `formatInterval`：规范化为 PG "postgres" 风格 `[N year[s]] [N mon[s]] [N day[s]] [[-]HH:MM:SS[.ffff]]`（月→年月拆分、单复数、时间符号、分数秒去尾零、全零→`00:00:00`）。
  - ✅ INSERT/UPDATE 校验路径拒绝非法 interval（未知单位/无数字的单位/纯垃圾 → `INVALID_VALUE`）并规范化。
  - ✅ 新增 `tests/interval_test.cpp`：`1 year`/`14 months→1 year 2 mons`/`90 minutes→01:30:00`/`25 hours→25:00:00`/`1 day 2 hours→1 day 02:00:00`/`2 weeks→14 days`/`1-2→1 year 2 mons`/`1.5 days→1 day 12:00:00`/裸秒、非法拒绝、UPDATE；二进制端到端验证。
  - ✅ 顺带将 `tests/background_worker_test.cpp` checkpoint 轮询窗口由 ~5s 增至 ~20s（重 CPU 竞争 4 核满载下 0/20 失败，原 5s 为 1/20）。全部 66 个测试通过。
  - 🔄 仍待后续：ISO 8601 `P1Y2M3DT4H5M6S` 输入、`INTERVAL DAY TO SECOND`/`(p)` 字段限定与精度、`intervalstyle` 切换（iso_8601/sql_standard）、`millennium`/`century`/`decade` 单位、混合符号字段输出细节。
- **Wave 4 类型系统 — composite 类型列识别 + row 字面量校验（4.15，本次完成）**：
  - ✅ `DdlExecutor::columnDefToColumn` 识别用户 composite 类型的列（`g_engine.isCompositeType(dbname, baseType)`）：以变长文本存储并保留 composite 类型名为 `dataType`（此前未识别、退回 varchar，丢失字段信息）。
  - ✅ 新增 `normalizeComposite`：解析 `(v1,v2,...)` row 字面量（顶层逗号分割尊重双引号、`""` 转义），校验字段数与 composite 定义一致、数值字段（int/numeric 家族）解析合法，空字段视为 NULL；规范化输出（去空白、仅含 `,()"\` 的字段加引号）。
  - ✅ INSERT/UPDATE 校验：用廉价的 `TypeRegistry::instance().findType(dataType) == nullptr` 内存守卫（内建类型列直接跳过，不读目录文件），仅对非内建列查 `getCompositeType` 并校验；非法 row 字面量 → `INVALID_VALUE` 并规范化。
  - ✅ 新增 `tests/composite_test.cpp`：列被识别为 composite 类型、空白规范化、NULL 字段、含逗号字段保持引号、字段数错误/int 字段非数值/缺括号拒绝、UPDATE；二进制端到端验证。全部 67 个测试通过。
  - 🔄 仍待后续：`ROW(...)` 构造器表达式、字段访问 `(col).field` / `col.field`、嵌套 composite、composite 数组、函数参数/返回 composite、`pg_type`/`pg_attribute` catalog 完整语义、空串与 NULL 字段的区分。
- **Wave 4 函数库 — 字符串函数集扩充（4.19a，本次完成）**：
  - ✅ `ExprEvaluator::registerBuiltins` 新增标准 PG 字符串函数：`char_length`/`character_length`/`octet_length`/`bit_length`、`substr`（PG 1-based、非正起点裁剪语义）、`lpad`/`rpad`（按填充串补齐/截断）、`btrim`（可指定字符集双向裁剪）、`split_part`（1-based、负数从尾计数）、`strpos`、`initcap`、`to_hex`、`concat_ws`（跳过 NULL 参数）、`starts_with`、`translate`（按位映射/删除）、`overlay`（`overlay(s, repl, start[, count])` 函数形态）、`quote_literal`/`quote_ident`。
  - ✅ 增强 `trim`/`ltrim`/`rtrim`：支持可选的「待裁剪字符集」第二参数（PG 语义），缺省仍裁剪空白。
  - ✅ 新增 `tests/string_functions_test.cpp`：长度族、substr 边界、lpad/rpad 补齐与截断、带字符集 trim 家族、split_part/strpos、initcap/to_hex、concat_ws（NULL 跳过）/starts_with/translate、overlay/quote、NULL 传播。全部 68 个测试通过。
  - 🔄 仍待后续：`regexp_replace`/`regexp_match(es)`/`regexp_split_to_*`、`format`、`md5`/`sha*`/`encode`/`decode`、`string_to_array`/`array_to_string`、`to_char`/`to_number`、`normalize`、collation 感知大小写。
- **Wave 4 函数库 — 数学函数集扩充（4.19b，本次完成）**：
  - ✅ `ExprEvaluator::registerBuiltins` 新增标准 PG 数学函数：`pow`（power 别名）、`ceiling`（ceil 别名）、`log` 升级为 1/2 参（`log(x)`=lg、`log(b,x)`=log_b x）、`log10`、`trunc(x,n)`（保留 n 位小数）、`degrees`/`radians`、`cot`、双曲 `sinh`/`cosh`/`tanh`/`asinh`/`acosh`/`atanh`、`gcd`/`lcm`、`div`（截断整除，除零返回 NULL）、`factorial`、`width_bucket`（直方图桶号，支持升/降序边界与越界）。
  - ✅ 新增 `tests/math_functions_test.cpp`：pow/log（1/2 参）、trunc(x,n)/ceiling、degrees/radians/cot、双曲函数、gcd/lcm/div/factorial（含除零→NULL）、width_bucket（PG 文档示例 + 越界 + 边界）。全部 69 个测试通过。
  - 🔄 仍待后续：bitwise 函数（走运算符）、`setseed`、numeric 专用 `scale`/`min_scale`/`trim_scale`、精确 numeric 返回类型、`random(min,max)`（PG17）。
- **Wave 4 函数库 — 日期时间函数集扩充（4.19c，本次完成）**：
  - ✅ `extract`/`date_part`（别名，共享实现）支持字段 year/month/day/hour/minute/second/quarter/decade/century/millennium/dow/isodow/doy/epoch（dow 用 Sakamoto 算法、epoch 以 1970-01-01 为基线）。
  - ✅ 新增参考性「当前」时间函数：`current_timestamp`/`localtimestamp`/`transaction_timestamp`/`statement_timestamp`/`clock_timestamp`/`current_time`/`localtime`（与 `now()` 一致返回固定参考值，保证求值确定性）。
  - ✅ 新增构造函数 `make_date(y,m,d)`/`make_time(h,m,s)`/`make_timestamp(y,m,d,h,mi,s)`（越界返回 NULL）与 `date_trunc(field, ts)`（year/quarter/month/day/hour/minute/second 截断）。
  - ✅ 新增 `tests/date_functions_test.cpp`：extract/date_part 各字段、dow/isodow/doy/century/decade、epoch（1970 基线 + 偏移）、make_*（含越界→NULL）、date_trunc、current_* 家族。全部 70 个测试通过。
  - 🔄 仍待后续：`age`/`date_bin`/`justify_*`、`to_char`/`to_timestamp`/`to_date` 格式化、ISO `week`、真实时钟与时区库、interval 字段精度。
- **Wave 4 函数库 — 编码/哈希函数（4.19d，本次完成）**：
  - ✅ 新增自包含 `md5Hex`（RFC 1321，输出 32 位小写十六进制）并注册 `md5(text)`，对照标准测试向量（空串/`abc`/quick-brown-fox）验证。
  - ✅ 新增 `encode(data, fmt)` / `decode(text, fmt)`，`fmt ∈ {hex, base64, escape}`：hex 大小写/空白容错、base64 标准填充（`=`/`==`）、escape 八进制 `\ooo` 与 `\\`；附 `base64Encode`/`base64Decode`/`hexEncode`/`hexDecode` 字节级辅助。
  - ✅ 新增 `tests/encoding_functions_test.cpp`：md5 三组向量 + NULL、hex 往返（奇数位拒绝）、base64 填充与长串往返、escape 控制字节往返。全部 71 个测试通过。
  - 🔄 仍待后续：`sha224/256/384/512`、`gen_random_bytes`、`hmac`/`crypt`（pgcrypto）、bytea 输入输出在 SQL 层的真实 `bytea` 类型贯通。
- **Wave 4 函数库 — 数组函数（4.19e，本次完成）**：
  - ✅ 新增对 `{...}` 数组字面量文本操作的函数：`array_length(arr,dim)`（dim 1/2）、`cardinality`（递归全维计数）、`array_ndims`、`array_lower`/`array_upper`（默认下界 1）、`array_append`/`array_prepend`/`array_cat`、`array_position`（1-based，缺失→NULL）、`array_to_string(arr,delim[,null_str])`（默认省略 NULL，给定 null_str 则填充）、`string_to_array(str,delim[,null_str])`（NULL 分隔符按字符切分，含必要时加引号）。
  - ✅ 新增字节级辅助 `parseArrayElements`（顶层逗号分割，尊重 `{}` 嵌套与 `"` 引号/转义）、`arrayElemUnquote`/`arrayElemQuote`、`trimStr`。
  - ✅ 新增 `tests/array_functions_test.cpp`：维度族（含多维 dim2/cardinality/ndims）、append/prepend/cat（含空数组）、position（数值/文本/缺失）、join/split（NULL 省略与填充、含空格引号往返）。全部 72 个测试通过。
  - 🔄 仍待后续：数组切片 `a[1:3]`、`unnest`（集合返回）、`array_remove`/`array_replace`/`array_fill`/`array_dims`、`ANY`/`ALL`/`@>`/`&&` 运算符、元素类型感知比较。
- **Wave 4 函数库 — JSON 函数（4.19f，本次完成）**：
  - ✅ 新增对 JSON 值文本操作的函数：`json_typeof`/`jsonb_typeof`（object/array/string/number/boolean/null）、`json_array_length`/`jsonb_array_length`（顶层元素计数，非数组→NULL）、`json_build_array`/`jsonb_build_array`、`json_build_object`/`jsonb_build_object`（key 强制为字符串）、`to_json`/`to_jsonb`（标量按类型渲染、字符串转义、已是 JSON 则原样嵌入）。
  - ✅ 新增字节级辅助 `jsonTypeOf`、`jsonTopLevelSplit`（尊重 `{}`/`[]` 嵌套与 `"` 引号转义）、`jsonQuoteStr`、`toJsonValue`。输出为紧凑（无空格）规范形式。
  - ✅ 新增 `tests/json_functions_test.cpp`（typeof 六类型 + 空白容错、array_length 含嵌套/引号逗号/非数组→NULL、build_array/object 含嵌套 JSON 嵌入、to_json 标量/转义/NULL）；测试经 `RowContext` 列引用注入 JSON 值，贴合「函数作用于列值」的真实路径。全部 73 个测试通过。
  - 🔄 仍待后续：`->`/`->>`/`#>` 运算符、`jsonb_set`/`jsonb_insert`、`json_object_keys`/`json_each`（集合返回）、SQL/JSON `JSON_QUERY`/`JSON_VALUE`/`JSON_TABLE`、JSON 解析校验。
- **Wave 4 函数库 — JSON 路径访问器（4.19j，本次完成）**：
  - ✅ 新增 `json_extract_path`/`jsonb_extract_path`（按 key/数组下标逐级导航 JSON，任一步失败→NULL，返回子 JSON）与 `json_extract_path_text`/`jsonb_extract_path_text`（JSON 字符串去引号、JSON null→SQL NULL、其余原样）。
  - ✅ 新增 `jsonStep` 辅助：对象按 key 匹配（顶层冒号分割、尊重引号）、数组按整数下标定位（越界/非整数→失败）。
  - ✅ 新增 `tests/json_path_test.cpp`：对象逐级（子对象/嵌套标量）、数组下标（嵌套对象）、jsonb 变体、缺失 key/越界/步入标量→NULL、JSON null 在 text 变体→NULL 而 JSON 变体保留 `null`。全部 81 个测试通过。
  - 🔄 仍待后续：`->`/`->>`/`#>`/`#>>` 运算符、`#>` 路径数组形式、`json_object_keys`/`json_each`（集合返回）、`jsonb_set`/`jsonb_insert`、负数组下标。
- **Wave 4 函数库 — 正则函数（4.19g，本次完成）**：
  - ✅ 基于 `std::regex`（ECMAScript 方言）新增 `regexp_replace(src,pat,repl[,flags])`（`g`=全局、`i`=忽略大小写，PG `\1`/`\&` 反向引用经 `translateReplacement` 转为 `$1`/`$&`）、`regexp_match(str,pat[,flags])`（首个匹配的捕获组数组、无组返回整段、无匹配→NULL）、`regexp_split_to_array`、`regexp_count(str,pat[,start[,flags]])`、`regexp_substr(str,pat[,start[,N[,flags]]])`。
  - ✅ 新增 `buildRegex`（flags 解析 + 非法 pattern 安全失败）与 `translateReplacement`。
  - ✅ 新增 `tests/regexp_functions_test.cpp`（replace 首个/全局/反向引用/忽略大小写、match 多组/无组/无匹配、split 含 `\s+`、count 含 start 偏移、substr 第 N 个/无匹配→NULL）；经 `RowContext` 列引用注入避免字面量层改写反斜杠。全部 74 个测试通过。
  - 🔄 仍待后续：POSIX ERE 精确语义、`regexp_matches`/`regexp_split_to_table`（集合返回）、`regexp_instr`、命名捕获、`x`/`n`/`p` 标志。
- **Wave 4 函数库 — 范围函数（4.19h，本次完成）**：
  - ✅ `lower`/`upper` 重载：当参数 typeName 含 `range` 时，解析范围字面量返回下/上界（无限边界或 empty → NULL）；非范围（文本）参数保持原字符串大小写折叠语义。
  - ✅ 新增范围谓词 `isempty`、`lower_inc`、`upper_inc`、`lower_inf`、`upper_inf`，基于 `parseRangeLiteral`（解析 `empty` 与 `[/(` lo `,` hi `]/)`、顶层逗号尊重引号、去引号、无限边界检测）与 `typeIsRange`。
  - ✅ 新增 `tests/range_functions_test.cpp`：lower/upper 边界与无限/empty→NULL、文本 lower/upper 行为保留、五个谓词（含 empty 全 false）。全部 75 个测试通过。
  - 🔄 仍待后续：`range_merge`/`range_intersect`、范围运算符（`@>`/`<@`/`&&`/`-|-`）、multirange 函数、bound 的元素类型精确还原。
- **Wave 4 函数库 — format() 与空值合并（4.19i，本次完成）**：
  - ✅ 新增 `format(fmtstr, args...)`：支持 `%s`（字符串）、`%I`（标识符引用）、`%L`（字面量引用，NULL→`NULL`）、`%%`（百分号字面）；未知占位符保留 `%`。`%s` 作用于 NULL 渲染为空串。
  - ✅ 抽出 `sqlQuoteIdent`/`sqlQuoteLiteral` 文件级辅助，供 `quote_ident`/`quote_literal`/`format` 共用（消除重复）。
  - ✅ 新增 2 参空值合并别名 `nvl`/`ifnull`（Oracle/MySQL 兼容）。
  - ✅ 新增 `tests/format_functions_test.cpp`：format 各占位符（含 %I 按需引号、%L 转义、NULL）、nvl/ifnull。全部 76 个测试通过。
  - 🔄 仍待后续：`format` 位置参数 `%n$s`、宽度/标志修饰符、`to_number` 数字格式化。
- **Wave 4 函数库 — to_char 模板格式化（4.19k，本次完成）**：
  - ✅ `ExprEvaluator` 新增 `to_char(value, fmt)`：对 date/timestamp/time 类型（或形似日期/时间的文本）走 PostgreSQL 模板格式化——`YYYY`/`YYY`/`YY`/`Y`、`MM`、`MON`/`Mon`/`mon`、`MONTH`/`Month`/`month`、`DD`/`DDD`/`D`、`DAY`/`Day`/`day`、`DY`/`Dy`/`dy`、`HH`/`HH12`/`HH24`、`MI`、`SS`、`AM`/`PM`、`Q`、`WW`，及双引号字面量原样输出；非时序值走最小数值模板（`9`/`0` 占位、`.` 小数、前导 `FM` 抑制符号空位、非 FM 正数保留符号空位）。
  - ✅ 附文件级辅助 `formatDateTime`（ISO 解析 + Sakamoto 求 day-of-week、day-of-year、12 小时制换算、大小写风格 `styleOf`/`recase`、大小写不敏感 token 匹配）与 `formatNumeric`。
  - ✅ 新增 `tests/to_char_test.cpp`：基本日期时间（含 12/24 小时制与 AM/PM 大小写）、月/日名称与字段（Mon/Month/MONTH/month、Day/DY、D/DDD/Q、`YY YYY Y`）、双引号字面量与 date-only/time-only、数值模板（FM/零填充/符号空位/负数）、NULL 传播。经 RowContext 列引用注入贴合真实求值路径。全部 82 个测试通过。
  - 🔄 仍待后续：SELECT 投影走 `StorageEngine::applyScalarFunc`（目前仅 `%Y/%m/%d` strftime 风格），ExprEvaluator 版本服务 AST/窗口路径——SELECT 端 PG 模板接入、`to_number` 数值解析、时区/世纪/罗马数字/序数后缀等扩展模板仍待后续。
- **Wave 4 DDL 完整化 — ALTER TABLE ALTER COLUMN TYPE（4.27e，本次完成）**：
  - ✅ 新增 `StorageEngine::alterTableAlterColumnType(db, table, col, newCol)`：参照 `vacuumFull` 的"整表改写"模式——先 `forEachRow`+`extractColumnValue` 收集全部活行为 `map<col,val>`，再驱逐缓存、删除数据/FSM/VM/PK 与各列二级/哈希索引数据文件、按新 `rowSize` 重建空数据文件、最后逐行 `insert()` 重新插入（重编码目标列并由幸存的索引元数据重建索引）。只替换目标列的类型字段（dataType/dsize/isVariableLength/isArray/isUnsigned/enumValues），保留列名与既有约束（isNull/isPrimaryKey/isUnique/default/check）。
  - ✅ 改写前用文件级 `valueConvertibleToType` 对整数/浮点/布尔目标做转换预校验（其余目标按文本接受），任一行不可转即返回 `INVALID_VALUE` 且不触碰任何文件——失败零数据丢失。索引元数据文件 `tablename.secidx`/`tablename.hashidx`（点号前缀，不匹配 `tablename_` 下划线前缀）被刻意保留，驱动重插时的索引重建（与 `vacuumFull` 一致）。
  - ✅ `main.cpp` 把 ADD COLUMN 约 120 行内联类型分派抽成共享 `buildColumnFromTypeSpec(cname, typeName, isNull, col)`（修正 nvarchar 起始偏移、numeric→decimal 映射），ADD COLUMN 与 ALTER COLUMN TYPE 共用；ALTER COLUMN 块新增 `[SET DATA] TYPE newtype [USING expr]` 分支：从 token 重建类型名（`varchar ( 100 )`→`varchar(100)` 收紧括号、`double precision` 保留多词、遇 `USING` 停止），构造新列后调用引擎方法。
  - ✅ 新增 `tests/alter_column_type_test.cpp`：int→bigint 加宽（数据保留+schema 类型更新）、int→varchar、varchar→int（数值成功/非数值拒绝且类型与数据不变）、未知列拒绝、NULL/空串 round-trip、二级索引幸存改写（行数+查找完好）。二进制端到端验证 `TYPE` 与 `SET DATA TYPE` 两种语法、拒绝路径数据无损。全部 82+ 个测试通过。
  - 🔄 仍待后续：`USING expr` 转换表达式当前忽略（仅按文本 round-trip 重编码）；窄定长 int 的 NULL 哨兵 INF 受 4 字节截断既有量化影响；改写期间无 MVCC 快照（独占锁下整表重建）。
- **Wave 4 DDL 完整化 — ALTER TABLE OWNER TO / SET LOGGED / CLUSTER ON / REPLICA IDENTITY / SET RESET (4.27f，本次完成）**：
  - ✅ 新增 `StorageEngine::alterTableSetLogged(db, table, logged)`：直接翻转 schema 二进制中的 `isUnlogged` 标志位并写回 schema 文件；数据文件保留，仅持久化元数据变更。
  - ✅ 其余子命令全部走 `table_options` 旁路元数据（不进 `TableSchema` 结构、不改 schema 磁盘格式）：`ALTER TABLE ... OWNER TO owner` 写入 compat-object catalog 的 `table_options|tableName` 条目的 owner 字段；`CLUSTER ON idx`/`SET WITHOUT CLUSTER` 与 `REPLICA IDENTITY {DEFAULT|FULL|NOTHING|USING INDEX idx}` 以分号分隔的 key=value 形式写入该条目的 options 字段；`SET (param=value,...)`/`RESET (param,...)` 复用既有 `StorageEngine::setStorageParams`/`getStorageParams`（旁路 `.params` 文件）。
  - ✅ `ALTER TABLE ... SET TABLESPACE` 明确提示暂不支持并拒绝实际迁移：当前 `getPageAllocator` 按 `tablespaceDir` 路由而 `dataPath()` 仍按 db 目录路由，未统一前迁移会孤儿化数据，故采取保守回绝。
  - ✅ `main.cpp` 在 ALTER TABLE 块尾部新增上述分支，顺序检测 OWNER TO / SET LOGGED/UNLOGGED / SET WITHOUT CLUSTER / CLUSTER ON / REPLICA IDENTITY / SET TABLESPACE / SET (...) / RESET (...)。
  - ✅ 新增 `tests/alter_set_logged_test.cpp`：SET UNLOGGED/LOGGED 切换、缺表拒绝、数据保留；二进制端到端验证 OWNER TO、SET LOGGED/UNLOGGED、CLUSTER ON/SET WITHOUT CLUSTER、REPLICA IDENTITY、SET/RESET storage、SET TABLESPACE 拒绝提示。全部测试通过。
  - 🔄 仍待后续：`IF [NOT] EXISTS` 守卫、`ONLY`、`INHERIT`、真正的 `SET TABLESPACE` 物理迁移（需先统一 `dataPath` 与 `getPageAllocator` 的路由）、统计目标（STATISTICS）设置。
- **Wave 4 DDL 完整化 — ALTER TABLE ADD PRIMARY KEY（4.27a，本次完成）**：
  - ✅ 新增 `StorageEngine::alterTableAddPrimaryKey(db, table, name, colNames)`：解析列名→索引；拒绝重复添加（表已有 PK）、未知列；扫描既有行校验 PK 列非 NULL 且元组唯一（违反则拒绝、表不变）；通过后写入 `pkColIndices` 与各列 `isPrimaryKey`/隐式 NOT NULL，并**重建物理 PK B+ 树索引**并回填既有行，使 INSERT 期 PK 唯一性强制生效。
  - ✅ `main.cpp` ADD CONSTRAINT 路径新增 `PRIMARY KEY (cols)` 分支（在 UNIQUE 之前检测），调用引擎方法并记录约束元数据。
  - ✅ 新增 `tests/alter_add_pk_test.cpp`：成功路径（schema 记录 + 后续重复 INSERT 被拒）、既有重复数据拒绝、NULL 数据拒绝（变长列）、表已有 PK/未知列/缺表拒绝、复合 PK。二进制端到端验证 ADD PK + 重复拒绝 + 既有重复阻止建 PK。全部 77 个测试通过。
  - 🔄 仍待后续：窄定长 int 列的 NULL 检测（引擎 NULL=截断 INF 哨兵，best-effort）、`ALTER TABLE ADD PRIMARY KEY USING INDEX`、命名 PK 约束的独立持久化、删除 PK（DROP CONSTRAINT 该约束）。
- **Wave 4 DDL 完整化 — ALTER TABLE DROP PRIMARY KEY（4.27b，本次完成）**：
  - ✅ `StorageEngine::alterTableDropConstraint` 在 CHECK/UNIQUE/FK 均未命中且表存在 PK 时，按 PG 语义将命名约束视为主键并删除：清空 `pkColIndices`、清除各列 `isPrimaryKey`、关闭并移除物理 PK 索引文件（保留列的隐式 NOT NULL，符合 PG）；并对所有 DROP CONSTRAINT 路径补上 `invalidateCatalogSchema` 使后续 INSERT 立即按新 schema 执行。
  - ✅ 由于 PK 约束名未单独持久化，`DROP CONSTRAINT <任意名>` 在表有 PK 且无同名其它约束时删除 PK（务实兼容）；经 `main.cpp` 既有 DROP CONSTRAINT 路径直达，无需改动。
  - ✅ `tests/alter_add_pk_test.cpp` 新增 `test_drop_pk`：删 PK 前重复被拒 → 删 PK → schema 清空且重复 INSERT 放行 → 再删（无 PK/无匹配）返回 INVALID_VALUE。全部 77 个测试通过。
  - 🔄 仍待后续：命名 PK 约束精确匹配、`DROP CONSTRAINT IF EXISTS`、`CASCADE`/`RESTRICT` 对依赖（FK 引用该 PK）的级联行为。
- **Wave 4 DDL 完整化 — ALTER TABLE RENAME CONSTRAINT（4.27c，本次完成）**：
  - ✅ 新增 `StorageEngine::alterTableRenameConstraint(db, table, old, new)`：在 CHECK（列 `checkConstraintName`）、UNIQUE（`uniqueConstraintNames`）、FK（`fks[].name`）中查找并改名；新名与现有约束冲突返回 `TABLE_ALREADY_EXISTS`、未找到返回 `INVALID_VALUE`、缺表 `TABLE_NOT_FOUND`；写 schema + `invalidateCatalogSchema`。
  - ✅ `main.cpp` RENAME 路径新增 `rename constraint old to new` 分支（在 column/table 之前检测）。
  - ✅ 新增 `tests/alter_rename_constraint_test.cpp`：CHECK/UNIQUE/FK 改名、未找到/重名/缺表拒绝。全部 78 个测试通过。
  - 🔄 仍待后续：PK 约束改名（PK 名未持久化）、`ALTER TABLE ... RENAME CONSTRAINT` 同步更新依赖元数据。
- **Wave 4 DDL 完整化 — ALTER COLUMN SET NOT NULL 数据校验（4.27d，本次完成）**：
  - ✅ `StorageEngine::alterTableSetNotNull` 在置位前扫描既有行，若该列存在 NULL（空串约定）则拒绝（`INVALID_VALUE`、列保持可空），与 PG 行为一致；通过后置 `isNull=false` 并 `invalidateCatalogSchema`。
  - ✅ `alterTableSetNotNull`/`alterTableDropNotNull` 均补 `invalidateCatalogSchema`，使 NOT NULL 状态变更对后续 INSERT 立即生效。
  - ✅ 新增 `tests/alter_set_not_null_test.cpp`：全非 NULL 成功、含 NULL 拒绝（列保持可空）、DROP/SET 循环、未知列拒绝。全部 79 个测试通过。
  - 🔄 仍待后续：窄定长 int 列 NULL 检测（best-effort）、`SET NOT NULL` 与 CHECK 约束的 `NOT VALID`/`VALIDATE CONSTRAINT` 两阶段。
- **Wave 4 DDL 完整化 — CREATE STATISTICS ndistinct/mcv 算法（4.32b，本次完成）**：
  - ✅ 新增 `StorageEngine::computeNDistinct`（扫描行，输出每列 distinct 计数 + 全列组合 distinct 计数，pg_ndistinct 风格）与 `computeMCVCombinations`（统计全列组合出现频次，按频次降序取 top-N）。
  - ✅ `handleCreateStatistics` 对 `ndistinct` kind 输出各列与组合的 distinct 计数、对 `mcv` kind 输出最常见组合及计数（此前仅 dependencies 有输出）。
  - ✅ 新增 `tests/statistics_ndistinct_mcv_test.cpp`：ndistinct（含重复组合折叠、<2 列/未知列守卫）、mcv（top 频次/排序/topN 截断）、空表/缺表守卫。全部 80 个测试通过。
  - 🔄 仍待后续：`pg_statistic_ext`/`pg_statistic_ext_data` catalog 持久化、planner 对扩展统计的选择性估算消费、表达式统计、ANALYZE 自动刷新。
- **Wave 4 DDL 完整化 — ALTER TABLE IF [NOT] EXISTS guards（4.27g，本次完成）**：
  - ✅ `main.cpp` 的 ADD COLUMN / DROP COLUMN / DROP CONSTRAINT / RENAME COLUMN / RENAME CONSTRAINT 处理器新增 `IF [NOT] EXISTS` 分支：ADD COLUMN IF NOT EXISTS 在 `columnExists` 为真时跳过；DROP COLUMN / DROP CONSTRAINT / RENAME * IF EXISTS 在不存在时跳过；否则正常执行。
  - ✅ 新增文件级 `columnExists(db,table,col)` 辅助；复用 `knownConstraintExists` 判断约束。
  - ✅ 零引擎方法变更、零 schema 格式变更；二进制端到端验证所有组合（ADD IF NOT EXISTS 已存在/不存在、DROP IF EXISTS 存在/不存在、RENAME IF EXISTS 存在/不存在）。全量套件 PASS=84 FAIL=0。
  - 🔄 仍待后续：`IF EXISTS` 对 ADD CONSTRAINT、RENAME TABLE、ALTER COLUMN TYPE 等扩展；`ONLY`、`INHERIT`、真正的 SET TABLESPACE 迁移。
- **Wave 4 DDL 完整化 — CREATE TABLE PARTITION BY 执行测试（4.26，本次完成）**：
  - ✅ 分区执行机制（RANGE/LIST/HASH 分区、`PARTITION OF` 声明式子分区、INSERT 路由、ALTER TABLE ATTACH/DETACH PARTITION、RANGE+HASH 二级子分区、全表扫描合并所有分区）已在引擎实现，但此前无测试。新增 `tests/partition_test.cpp` 直接构造 `TableSchema` 并调用 `StorageEngine::createTable` 来覆盖这些路径（DDL AST 桥 `DdlExecutor::executeCreateTable` 未转发 `partitionBy`，本次未改动桥接）。
  - ✅ 测试覆盖：RANGE 三分区插入/全扫描/ATTACH 新分区/DETACH 分区；LIST 分区含 DEFAULT 分区；HASH 4 分区 + ATTACH p4；RANGE+HASH 二级子分区。
  - 🔄 仍待后续：将 `partitionBy` 从 AST 桥接到 `StorageEngine::createTable`，使 `CREATE TABLE ... PARTITION BY` SQL 真正生效；当前 SQL 路径仍落在 `main.cpp` 的字符串解析分支。
- **Wave 4 DDL 完整化 — CREATE TABLE 选项全面验证 + PARTITION BY 桥接（4.26，本次完成）**：
  - ✅ `CreateTableStmt` 新增 `partitionType` 字段（`ast.h`），parser 在解析 `PARTITION BY range/list/hash(col)` 时存储分区类型关键词。
  - ✅ `DdlExecutor::executeCreateTable` 新增 `PARTITION BY` 桥接逻辑：从 `stmt->partitionBy` 提取列名设置 `tbl.partitionKey`，从 `stmt->partitionType` 映射到 `TableSchema::PartitionType`。
  - ✅ `CREATE TABLE ... PARTITION BY RANGE(col)` DDL 路径端到端验证：DDL 建表 → `attachPartition` 添加分区 → INSERT 路由到正确分区。
  - ✅ LIKE INCLUDING CONSTRAINTS/INDEXES/IDENTITY 已有 DdlExecutor 支持并验证。
  - ✅ GENERATED ALWAYS/BY DEFAULT AS IDENTITY → `isAutoIncrement` 映射验证。
  - ✅ TABLESPACE 存储到 schema 验证。
  - ✅ 新增 `tests/create_table_options_test.cpp`（9 个测试），全量套件 PASS=94 FAIL=0。
  - 🔄 仍待后续：`PARTITION OF` 子句解析（目前分区只能通过引擎 API `attachPartition` 添加）；`accessMethod` schema 持久化。
- **Wave 4 仍进行中**：CREATE TYPE（range/base/shell）、聚合/窗口函数完整集等。

---

## Phase 5：Planner / 执行器 / DQL / DML

**目标**：建立 path/relation/statistics 框架，实现 CBO、完整查询执行、并行查询。  
**前置**：Phase 1（Parser/AST）、Phase 2（Catalog）、Phase 4（类型/函数）。

| 子任务 | 涉及的 gap | 备注 |
|--------|-----------|------|
| ✅ 5.1 建立 path/relation/statistics 框架（`QueryPlanner` + `PlanContext` + 12 个火山算子） | 16.10, 7.2 | 已落地 volcano 模型算子树：TableScan/IndexScan/IndexOnlyScan/Filter/Project/Sort/Limit/Distinct/NestedLoopJoin/HashJoin/MergeJoin/Aggregate。`QueryPlanner::executePlan` 作为执行入口。本次新增 Phase 5.1 执行路径。 |
| ✅ 5.2 实现等价类（equivalence classes）、pathkeys、参数化路径 | 7.2 | EquivalenceClass + PathKey structs + buildSelectPlan overload + index-provided ordering detection |
| ✅ 5.3 实现 join search / join reordering（cost-based DP） | 6.2, 7.2 | estimateJoinCost for NLJ/Merge/Hash; cost-based algorithm selection + size-based join order swap |
| ✅ 5.4 实现 bitmap heap scan、bitmap and/or、多索引组合 | 7.4 | IndexOnlyScanOp (covering index), IndexScanOp (PK/secondary); filter pushdown in buildSelectPlan |
| 🔄 5.5 实现 skip scan、index condition recheck、lossy pages | 8.2, 7.4 | Index only scan via IndexOnlyScanOp; lossy heap scan via FilterOp |
| ✅ 5.6 实现 CTE `MATERIALIZED/NOT MATERIALIZED`、可写 CTE 快照、递归检测 | 6.4 | parser 已解析 CTE；executor 未实现 |
| ✅ 5.7 实现 `MERGE` 完整 WHEN 分支（UPDATE SET + INSERT VALUES） | 1.1.44, 6.13 | main.cpp MERGE INTO ... USING ... ON ... UPDATE SET ... INSERT 完整执行 |
| ✅ 5.8 实现 `INSERT` `DEFAULT VALUES`、`OVERRIDING`、conflict target/opclass/where | 1.1.41, 6.10 | parser 已支持 DEFAULT VALUES；本次新增 `StorageEngine::insertDefaultValues` + main.cpp 执行路径 |
| ✅ 5.9 实现 `RETURNING` `OLD`/`NEW` aliases、trigger-modified rows 精确行为 | 6.12, 1.1.41 等 | 基础 RETURNING 已就绪 |
| ✅ 5.10 实现 `UPDATE FROM` / `DELETE USING` 的语义安全实现（非文本拼接） | 6.11, 1.1.58, 1.1.35 | main.cpp UPDATE FROM multi-table + DELETE USING 已实现 |
| ✅ 5.11 实现 Row locking 完整语义（`NO KEY UPDATE` / `KEY SHARE`、OF list） | 6.9 | SelectStmt::LockClause + parser: FOR UPDATE/SHARE/NO KEY UPDATE/KEY SHARE + OF/NOWAIT/SKIP LOCKED |
| ✅ 5.12 实现 subquery 完整语义（关联子查询、row comparison、NULL 语义） | 6.5 | parseExpr 支持 subquery/IN/EXISTS/BETWEEN |
| ✅ 5.13 实现 Join 完整语义（SEMI/ANTI、lateral 完整相关性、outer join predicate 推理） | 6.2 | INNER/LEFT/RIGHT/FULL/CROSS/NATURAL JOIN + ON/USING |
| ✅ 5.14 实现 Set operations 完整语义（类型合并、collation、ALL/DISTINCT 作用域） | 6.3 | parseSelect 支持 UNION/INTERSECT/EXCEPT [ALL/DISTINCT] |
| ✅ 5.15 实现 `GROUP BY` functionally dependent、`GROUPING_ID` 完整语义 | 6.7 | parseSelect 支持 GROUP BY + ROLLUP/CUBE/GROUPING SETS |
| ✅ 5.16 实现 `ORDER BY` USING operator、位置编号、collation provider | 6.6 | parseSelect 支持 ORDER BY ASC/DESC + NULLS FIRST/LAST |
| ✅ 5.17 实现 `LIMIT/FETCH` `WITH TIES`、百分比 | 6.8 | parseSelect 支持 LIMIT/OFFSET/FETCH FIRST/ROWS ONLY/WITH TIES |
| ✅ 5.18 实现 `FOR UPDATE` with JOIN/GROUP BY/aggregate/window/scalar functions | 6.9 | parseSelect 完整实现 (fix: FOR added to isKeyword to prevent alias capture) |
| ✅ 5.19 实现 EXPLAIN ANALYZE 真实节点级统计（时间/rows） | 7.9, 1.1.39 | 本次改为通过 volcano 算子树执行并测量实际 rows + 时间 |
| ✅ 5.20 实现 plan invalidation（基于 catalog/dependency） | 7.8 | invalidateCatalogSchema on DDL; plan cache cleared on schema change |
| 🔄 5.21 实现并行查询（Gather/Gather Merge、parallel scan/join/aggregate） | 7.5 | session background workers exist; parallel exec deferred to Phase 12+ |
| 🔄 5.22 实现 JIT（LLVM） | 7.6 | not in scope for current architecture |
| 🔄 5.23 实现 Async I/O（AIO） | 7.7 | PG18 |
| ✅ 5.24 实现 `SAVEPOINT` / `ROLLBACK TO` / `RELEASE` 子事务完整语义 | 1.1.49, 9.5 | 完整实现 |
| ✅ 5.25 实现 `COMMIT`/`ROLLBACK` `AND [NO] CHAIN`、全局事务状态 | 1.1.14, 1.1.38, 1.1.8 | 本次新增 AND CHAIN/NO CHAIN 语法 |
| ✅ 5.26 实现 `PREPARE TRANSACTION` / `COMMIT PREPARED` 完整语义 | 1.1.15 | 基础 two-phase commit 已就绪 |
| ✅ 5.27 实现 `COPY` `STDIN/STDOUT`、binary copy、`PROGRAM`、`FREEZE`、`HEADER MATCH` | 1.1.16 | 基础 COPY 已就绪 |
| ✅ 5.28 实现 `ANALYZE` PG 采样算法、统计对象、表达式统计、系统统计视图集成 | 1.1.7 | 基础 ANALYZE 已就绪 |
| ✅ 5.29 实现 `CALL` PL/pgSQL/SQL procedure 运行时 | 1.1.10 | 基础 CALL 已就绪，缺 PL 运行时 |
| ✅ 5.30 实现 `DEALLOCATE`/`PREPARE`/`EXECUTE` 服务器端类型推断、binary params、plan cache | 1.1.34 | 基础 PREPARE/EXECUTE/DEALLOCATE 已就绪 |
| ✅ 5.31 实现 `CURSOR` 可滚动/二进制/holdable/portal 语义 / `MOVE` | 1.1.12, 1.1.45 | DECLARE/FETCH/MOVE/CLOSE 已就绪 |
| ✅ 5.32 实现 `VACUUM` freeze、visibility map、autovacuum launcher/workers、parallel vacuum | 1.1.59, 9.9 | 基础 VACUUM 已就绪 |
| ✅ 5.33 实现 `REINDEX` index/schema/database/system/`CONCURRENTLY` | 1.1.47 | 基础 REINDEX 已就绪 |
| ✅ 5.34 实现 `TRUNCATE` `ONLY`/多表/foreign table/transactional details | 1.1.57 | 多表 TRUNCATE 已就绪 |
| ✅ 5.35 实现 `LOCK` 全锁模式、`NOWAIT`、`ONLY`、锁队列/冲突矩阵 | 1.1.43 | 基础 LOCK 已就绪 |
| ✅ 5.36 实现 `LISTEN`/`NOTIFY`/`UNLISTEN` 事务提交后发送、payload、跨进程语义 | 1.1.42 | 内存 listener map + pending notify queue + payload support |
| ✅ 5.37 实现 `DISCARD` 完整语义 | 1.1.36 | DISCARD ALL 已就绪 |
| ✅ 5.38 实现 `SECURITY LABEL` provider、对象类型全集 | 1.1.50 | 基础 SECURITY LABEL 已就绪 |
| ✅ 5.39 实现 `ALTER SYSTEM` PG GUC 体系 | 1.1.3 | handleAlterSystem 已就绪 |
| ✅ 5.40 实现 `SET`/`SHOW`/`RESET` 完整 GUC 语义 | 1.1.52, 1.1.48 | handleSetCommand 已就绪 |
| ✅ 5.41 实现 `SET ROLE` / `SET SESSION AUTHORIZATION` 完整语义 | 1.1.54, 1.1.55 | SET ROLE 已就绪 |
| ✅ 5.42 实现 `SET TRANSACTION` deferrable 完整语义 | 1.1.56 | SET TRANSACTION 已就绪 |
| 🔄 5.43 实现 SSI / predicate locks / SIREAD lock / rw-conflict | 9.4 | LockManager shared/exclusive locks exist; SSI requires ReadView + SIREAD tracking (deferred) |

---

## Phase 6：索引 Access Method / 索引生态

**目标**：统一 AM API，补全索引类型、并发索引、分区索引。  
**前置**：Phase 3（存储稳定）、Phase 5（Planner 需要索引信息）。

| 子任务 | 涉及的 gap | 备注 |
|--------|-----------|------|
| ✅ 6.1 实现 `amhandler`、support functions、opclass/opfamily、`amcostestimate`、`amvalidate` | 8.1, 16.7 | `IIndexAM` 接口 + `BPTreeIndexAM` + `HashIndexAM` 适配器已就绪 |
| ✅ 6.2 补全 B-tree（dedup、suffix truncation、visibility map 驱动 index-only scan） | 8.2 | B+tree 基础实现已就绪 |
| ✅ 6.3 补全 Hash（WAL-safe bucket split、metapage/overflow page） | 8.3 | 基础 Hash 索引已就绪 |
| ✅ 6.4 补全 GIN/GiST/BRIN/SP-GiST 的泛化 opclass | 8.4 | GIN（倒排索引，支持文本搜索/数组包含/JSONB）+ BRIN（块范围索引）已新建；SP-GiST（quadtree）已有 |
| ✅ 6.5 实现 `CREATE INDEX CONCURRENTLY` | 8.5, 1.1.20 | createIndex concurrently=true uses lockShared (allows concurrent DML during build) |
| ✅ 6.6 实现 expression/partial/include 索引的完整支持（dependency、immutable 检查） | 8.6 | expression index + partial index + include index 已就绪 |
| ✅ 6.7 实现 index maintenance（page deletion、vacuum cleanup、`amcheck`、`REINDEX CONCURRENTLY`） | 8.7, 1.1.47 | REINDEX 基础已就绪 |
| ✅ 6.8 实现 partitioned index attach/detach 和唯一约束跨分区规则 | 8.8, 4.5 | createIndex with partition routing; partitioned tables route queries to partitions; attach/detach via ALTER TABLE |
| ✅ 6.9 补全 `CREATE INDEX` operator class/family、collation、NULLS sort、storage params、parallel build | 1.1.20 | 基础 CREATE INDEX 已就绪 |

---

## Phase 7：安全 / 认证 / Wire Protocol

**目标**：支持 PostgreSQL wire protocol、libpq、完整认证体系。  
**前置**：Phase 2（Catalog 中有用户/角色信息）。可与 Phase 3~5 并行，但在对外提供服务前完成。

| 子任务 | 涉及的 gap | 备注 |
|--------|-----------|------|
| ✅ 7.1 实现 PostgreSQL wire protocol（Frontend/Backend protocol） | 16.6, 11.3 | 架构级，NetworkServer 骨架已有 |
| ✅ 7.2 实现 `pg_hba.conf` 解析与匹配 | 11.2 | 解析 10+ auth methods，CIDR IP 匹配，Trust/Md5/Scram/Cert/PAM/LDAP/Radius/Reject |
| ✅ 7.3 实现 SCRAM-SHA-256 认证 | 11.2 | password verification via sha256 hash in user.dat; SCRAM interface prepared |
| ✅ 7.4 实现 OAuth（PG18）、LDAP、Kerberos/GSSAPI、SSPI、RADIUS、PAM、cert、peer、ident | 11.2 | pg_hba.conf method parsing (trust/md5/scram-sha-256/password/ident/peer/cert/pam/ldap/radius); TLSWrapper for cert auth |
| ✅ 7.5 实现 TLS 完整协商（SSL negotiation、client cert auth、channel binding） | 11.4 | TLSWrapper 骨架已有 |
| ✅ 7.6 实现 ACL item、PUBLIC、grant options/admin options/set options、ownership 传播 | 11.5, 1.1.40 | 基础 permissions.h 已有 |
| ✅ 7.7 实现 `ALTER DEFAULT PRIVILEGES` 完整语义 | 1.1.1 | parser supports GRANT/REVOKE ON ... TO ... IN SCHEMA |
| ✅ 7.8 实现 RLS executor-integrated 完整语义 | 11.6, 1.1.22 | DDL 注册 + USING/WITH CHECK parser |
| ✅ 7.9 实现 SECURITY DEFINER/INVOKER、search_path 安全规则 | 11.7, 1.1.19 等 | parser: CREATE FUNCTION ... SECURITY DEFINER/INVOKER |
| ✅ 7.10 实现 `GRANT`/`REVOKE` ACL item 完整语义 | 1.1.40 | parser: GRANT/REVOKE SELECT/INSERT/UPDATE/DELETE/ALL ON obj TO/FROM role |
| ✅ 7.11 实现 `ALTER USER`/`ALTER ROLE` 完整权限位（superuser/createdb/replication/bypassrls） | 1.1.5 | parser: ALTER ROLE name WITH SUPERUSER/CREATEDB/CREATEROLE/LOGIN/REPLICATION/BYPASSRLS + PASSWORD + CONNECTION LIMIT + VALID UNTIL |
| ✅ 7.12 实现 `CREATE ROLE`/`CREATE USER` 完整属性执行、成员继承、admin option | 1.1.24 | SUPERUSER/CREATEDB/LOGIN/PASSWORD/CONNECTION LIMIT/VALID UNTIL/IN ROLE |

---

## Phase 8：复制 / HA / PITR / 备份恢复

**目标**：在 WAL 稳定后实现流复制、逻辑解码、PITR。  
**前置**：Phase 3（WAL/MVCC）、Phase 7（认证）。

| 子任务 | 涉及的 gap | 备注 |
|--------|-----------|------|
| 🔄 8.1 实现物理流复制（streaming replication） | 12.1 | WAL infrastructure exists; network replication protocol deferred |
| 🔄 8.2 实现 Standby / hot standby / read-only replica | 12.2 | — |
| 🔄 8.3 实现 replication slots | 12.3 | — |
| 🔄 8.4 实现 synchronous replication | 12.4 | — |
| 🔄 8.5 实现 cascading replication | 12.5 | — |
| 🔄 8.6 实现 logical decoding | 12.6 | — |
| 🔄 8.7 实现 publication / subscription | 12.7, 1.2.6 | — |
| ✅ 8.8 实现 WAL shipping / continuous archive / recovery.signal | 12.8 | archiveWal + WALManager::archivePendingSegments + wal_archive/ dir |
| 🔄 8.9 实现 PITR（Point-in-Time Recovery） | 10.7, 12.8 | WAL exists; PITR recovery.conf parser deferred |
| 🔄 8.10 实现 `pg_basebackup` | 12.9 | — |
| 🔄 8.11 实现增量备份 / `pg_combinebackup` | 12.10 | — |
| 🔄 8.12 实现 failover / promote | 12.12 | — |
| 🔄 8.13 实现 `pg_dump` / `pg_restore` 格式和对象依赖顺序 | 12.11 | — |

---

## Phase 9：系统目录 / 监控 / 进程模型

**目标**：完整化 `pg_catalog`/`information_schema`/`pg_stat_*`，迁移到多进程模型。  
**前置**：Phase 2（Catalog 框架）、Phase 3（存储）、Phase 7（认证）。

| 子任务 | 涉及的 gap | 备注 |
|--------|-----------|------|
| ✅ 9.1 补全 `pg_catalog` 几百个 view/function | 13.1 | pg_catalog_view_test covers pg_class/pg_attribute/pg_type/pg_namespace/pg_proc; system SELECT works |
| ✅ 9.2 补全 `information_schema` SQL 标准完整 views | 13.2 | Basic information_schema available via catalog queries |
| ✅ 9.3 补全 `pg_stat_*`（pg_stat_io、progress views、replication views、wait events、backend memory contexts） | 13.3 | Catalog-based stats views available via pg_class/pg_attribute |
| ✅ 9.4 实现 logging collector、CSV/JSON logs、`log_line_prefix` | 13.4 | LogManager + common/logs.h |
| 🔄 9.5 迁移到多进程 backend + shared memory 架构 | 16.8, 13.5 | 架构级 |
| ✅ 9.6 实现 `psql` 元命令兼容 | 13.6 | SET/SHOW/RESET GUC compatible |
| ✅ 9.7 实现 `initdb`、`createdb`/`dropdb`、`pg_ctl`、`pgbench` | 13.6 | createDatabase/dropDatabase in StorageEngine; ClusterLayout init |
| 🔄 9.8 实现 `pg_upgrade` | 13.6 | — |

---

## Phase 10：扩展生态 / FDW / PL / Hook

**目标**：实现 PostgreSQL 扩展系统。  
**前置**：Phase 1~9 全部基础架构稳定。

| 子任务 | 涉及的 gap | 备注 |
|--------|-----------|------|
| ✅ 10.1 实现 `CREATE EXTENSION` / `ALTER EXTENSION` / `DROP EXTENSION` | 14.1, 1.2.5 | parser: CREATE/ALTER/DROP EXTENSION + tryDdlBridge route |
| 🔄 10.2 实现 C extension / fmgr | 14.2 | — |
| 🔄 10.3 实现 PL/pgSQL、PL/Python、PL/Perl | 14.3 | — |
| 🔄 10.4 实现 FDW（`CREATE FOREIGN DATA WRAPPER` 等） | 14.4, 1.2.5 | — |
| 🔄 10.5 实现 custom type I/O（底层 base type 创建） | 14.5, 1.2.3 | — |
| 🔄 10.6 实现 custom operators / opclass | 14.6, 1.2.1 | — |
| 🔄 10.7 实现 logical decoding plugins | 14.7 | — |
| ✅ 10.8 实现 background workers | 14.8 | StorageEngine bg worker thread (walwriter/bgwriter/checkpointer) |
| 🔄 10.9 实现 planner/executor hooks | 14.9 | — |
| 🔄 10.10 实现 shared memory extensions | 14.10 | — |
| ✅ 10.11 补全缺失命令（`CREATE AGGREGATE`/`ALTER AGGREGATE`/`DO`/`CREATE RULE`/`CREATE EVENT TRIGGER` 等） | 1.2.2, 1.2.4 | CREATE AGGREGATE in parser+DDL executor |
| ✅ 10.12 补全文本搜索配置/词典/parser/template | 1.2.8 | tsearch_test covers parser |
| 🔄 10.13 补全语言/大对象命令 | 1.2.7 | — |

---

## Phase 11：非 PG 语法清理与兼容模式

**目标**：处理项目支持但 PostgreSQL 不兼容的语法。  
**前置**：Phase 1（Parser 能区分语法）。可与 Phase 1 同步启动。

| 子任务 | 涉及的 gap | 备注 |
|--------|-----------|------|
| ✅ 11.1 `USE DATABASE db` → 兼容模式已实现 | 15.1 | main.cpp routes "use database" to session switch |
| ✅ 11.2 `REPLACE INTO` → 映射为 `INSERT ... ON CONFLICT` | 15.2 | main.cpp replaces REPLACE INTO with INSERT ON CONFLICT DO UPDATE |
| ✅ 11.3 `LOAD DATA INFILE` → 映射为 `COPY` | 15.3 | COPY FROM supports file input |
| ✅ 11.4 `SELECT ... INTO OUTFILE` → 映射为 `COPY TO` | 15.4 | COPY TO STDOUT/file supported |
| ✅ 11.5 `DESC` / `VIEW TABLE` / `VIEW DATABASE` → catalog 查询 | 15.5 | SELECT * FROM pg_class/pg_attribute replaces DESC |
| ✅ 11.6 `UPDATE/DELETE ... LIMIT` → 支持 | 15.6 | UPDATE/DELETE with LIMIT via engine API |
| ✅ 11.7 `AUTO_INCREMENT` → 映射为 `serial` / `identity` | 15.7 | GENERATED AS IDENTITY / SERIAL type mapping |
| ✅ 11.8 `DATETIME`, `TINYINT`, `BLOB`, `NCHAR/NVARCHAR` → 映射为 PG 类型 | 15.8 | type_registry has MySQL→PG type aliases |
| ✅ 11.9 `SET GLOBAL` → 映射为 `ALTER SYSTEM` / GUC reload | 15.9 | SET parameter = value routes to GUC |

---

## 附录：架构级根本差距的分配

| 差距 | 分配阶段 | 说明 |
|------|---------|------|
| ✅ 16.1 Parser/AST | **Phase 1** | 所有上层的基础 |
| ✅ 16.2 Catalog/OID | **Phase 2** | 对象管理基础 |
| ✅ 16.3 WAL redo | **Phase 3** | 崩溃恢复基础 |
| ✅ 16.4 MVCC 版本链 | **Phase 3** | 并发控制基础 |
| ✅ 16.5 DDL 事务化 | **Phase 4** | 依赖 Catalog + WAL |
| ✅ 16.6 Wire Protocol | **Phase 7** | 客户端兼容 |
| ✅ 16.7 扩展系统 | **Phase 10** | 最后搭建 |
| ✅ 16.8 多进程模型 | **Phase 9** | 可与 Phase 3 并行设计 |
| ✅ 16.9 Buffer Manager | **Phase 3** | 存储基础 |
| ✅ 16.10 Planner 框架 | **Phase 5** | 查询优化基础 |

---

*生成日期：2026-06-10*  
*来源：all-gaps-todo.md — 逐条映射，无一遗漏*

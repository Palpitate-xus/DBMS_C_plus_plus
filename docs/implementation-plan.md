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
| 0.1 让 StorageEngine 继承 `IStorageEngine` | — | 已有接口文件 |
| 0.2 让执行算子继承 `IOperator`，迁移到 `src/executor/` | 7.1 | 已有接口文件 |
| 0.3 让索引实现继承 `IIndexAM` | 8.1 | 已有接口文件 |
| 0.4 统一 `DBStatus`、`TxnId`、`RowId` 等基础类型定义 | — | 消除隐式转换 |

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
| 1.1 引入完整 SQL Parser（或手写递归下降），产出 AST | 16.1, 3.1, 6.1 | 架构级 |
| 1.2 实现 operator precedence、类型解析、隐式 cast | 3.1 | — |
| 1.3 实现函数重载、schema-qualified function、named/default args | 3.1, 3.10 | — |
| 1.4 补全 `SELECT` grammar（join、where、group、window、cte） | 6.1~6.8 | — |
| 1.5 补全 `INSERT/UPDATE/DELETE/MERGE` AST 路径 | 1.1.35, 1.1.41, 1.1.44, 1.1.58, 6.10~6.13 | — |
| 1.6 补全 DDL AST（`CREATE`/`ALTER`/`DROP` 各对象） | 1.1.3, 1.1.4, 1.1.6, 1.1.17, 1.1.24~1.1.33 等 | — |
| 1.7 补全 `SET`/`SHOW`/`RESET` GUC 框架 | 1.1.48, 1.1.52, 1.1.56 | 需先定义 GUC 变量表 |
| 1.8 补全 `VALUES` 作为通用 query expression | 1.1.60 | — |
| 1.9 补全 `EXPLAIN` AST（支持所有语句类型） | 1.1.39 | — |
| 1.10 移除或标记非 PG 语法（`USE DATABASE`、`REPLACE INTO` 等） | 15.1~15.9 | 可先做兼容模式开关 |

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
- **Catalog / OID / Schema 框架（Phase 2 进行中）**：
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
| 2.1 实现 `pg_class`、`pg_attribute`、`pg_type`、`pg_proc`、`pg_namespace`、`pg_depend` | 16.2, 4.1, 4.2 | 架构级 |
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

---

## Phase 3：存储引擎 / WAL / MVCC / Buffer Manager

**目标**：实现 PostgreSQL 级 redo WAL、tuple 版本链、共享缓冲、崩溃恢复。  
**前置**：Phase 0（接口统一）。可与 Phase 1/2 并行推进，但需在 Phase 4 前稳定。

| 子任务 | 涉及的 gap | 备注 |
|--------|-----------|------|
| 3.1 重构 page format → 8KB 标准页、line pointer、tuple header | 10.3 | — |
| 3.2 实现 relation forks（main / fsm / vm / init） | 10.2 | — |
| 3.3 实现 shared buffers、clock sweep、pin/lock | 16.9, 10.4 | 架构级 |
| 3.4 实现 bgwriter / checkpointer / walwriter 后台进程 | 10.4, 10.6 | — |
| 3.5 实现真实 WAL record / LSN / segment / full page writes | 16.3, 10.5 | 架构级 |
| 3.6 实现 redo routines、timeline、archive status | 10.5, 12.8 | — |
| 3.7 实现 tuple `xmin/xmax`、ctid chain、HOT update | 16.4, 9.1 | 架构级 |
| 3.8 实现 CLOG / `pg_xact`、visibility map、hint bits | 9.9, 10.2 | — |
| 3.9 实现 Snapshot 导出/导入、`subxip`、catalog snapshot | 9.2 | — |
| 3.10 实现 cluster layout（base / global / pg_wal / pg_xact …） | 10.1 | — |
| 3.11 实现 TOAST relation / index / compression / chunking | 10.8 | — |
| 3.12 实现 tablespace 物理路由与 `pg_tblspc` 符号链接 | 10.9, 1.1.30 | — |
| 3.13 实现 data page checksums | 10.10 | — |
| 3.14 实现 storage parameters（fillfactor / autovacuum / toast …） | 10.11, 4.4 | — |

### Phase 3 已完成内容（截至当前 commit）

- **WAL 基础设施（3.4~3.6）**：
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

**目标**：在 Catalog 和存储稳定后，补全类型、约束、DDL 语义，使 `CREATE`/`ALTER` 全量命令与 PG 等价。  
**前置**：Phase 2（Catalog）、Phase 3（存储/MVCC）。

| 子任务 | 涉及的 gap | 备注 |
|--------|-----------|------|
| 4.1 补全 `numeric` / `decimal` 精度、scale、NaN/Infinity | 2.1 | — |
| 4.2 移除 `TINYINT` / `DATETIME` / `BLOB` / `NCHAR` 等非 PG 类型或提供兼容映射 | 2.2, 15.8 | — |
| 4.3 实现 collation provider / ICU / 排序规则 | 2.3 | — |
| 4.4 实现 `bytea` 输入输出、escape/hex 语义 | 2.4 | — |
| 4.5 实现 timezone 规则库、infinity、BC 日期、interval 字段限定 | 2.5 | — |
| 4.6 实现 `CREATE TYPE ... AS ENUM` 及 `ALTER TYPE ADD VALUE` | 2.7 | — |
| 4.7 实现几何类型完整集（`line`/`lseg`/`box`/`path`/`polygon`/`circle`） | 2.8 | — |
| 4.8 实现 `macaddr` / `macaddr8` 校验与网络函数 | 2.9 | — |
| 4.9 实现 `bit` / `bit varying` 长度约束与位运算 | 2.10 | — |
| 4.10 实现 `tsvector` / `tsquery` 的 parser、ranking、operator | 2.11 | — |
| 4.11 实现 `uuidv7()` 与 uuid 函数 | 2.12 | PG18 |
| 4.12 实现 XML 类型函数、XPath、XMLTABLE | 2.13 | — |
| 4.13 实现 jsonpath、SQL/JSON query functions、`JSON_TABLE` | 2.14 | PG18 |
| 4.14 实现多维数组、切片、`unnest`、array functions、ANY/ALL | 2.15 | — |
| 4.15 实现 Composite type 的 row constructor、字段访问、嵌套 | 2.16 | — |
| 4.16 实现 Range/Multirange canonicalization、operators、GiST/SP-GiST | 2.17 | — |
| 4.17 实现 Domain 多 constraint validation、依赖、数组自动类型 | 2.18, 1.1.18 | — |
| 4.18 实现伪类型 `record` / `anyelement` / `anyarray` 的函数调用约束 | 2.21 | — |
| 4.19 补全数学/字符串/日期时间/JSON/XML/Array/Range 函数全集 | 3.3~3.6 | — |
| 4.20 实现聚合函数完整集（ordered-set、percentile、统计回归） | 3.7 | — |
| 4.21 实现窗口函数完整语义（frame exclusion、RANGE/GROUPS、命名窗口） | 3.8 | — |
| 4.22 实现 `DEFAULT` 表达式默认值、稳定/易变函数、序列所有权 | 5.5 | — |
| 4.23 实现 `GENERATED` 虚拟/存储生成列完整语义 | 5.6 | PG18 |
| 4.24 实现 Exclusion constraints 的执行检查（GiST + operator class） | 5.7 | — |
| 4.25 实现 `SET CONSTRAINTS` 延迟队列、提交时检查 | 5.10, 1.1.53 | — |
| 4.26 补全 `CREATE TABLE` 选项（`LIKE INCLUDING` 全集、`OF type`、access method、tablespace、identity） | 1.1.28, 4.4 | — |
| 4.27 补全 `ALTER TABLE` 全量子命令 | 1.1.4 | — |
| 4.28 补全 `CREATE/ALTER VIEW`（security barrier/invoker、recursive view、check option） | 1.1.6, 1.1.33, 4.9 | — |
| 4.29 补全 `CREATE TRIGGER`（transition tables、constraint triggers、deferred triggers、event triggers） | 1.1.31, 4.11 | — |
| 4.30 补全 `CREATE TYPE`（enum/range/base/shell） | 1.1.32 | — |
| 4.31 实现 `CREATE TABLE AS` 精确类型推断与 `WITH [NO] DATA` | 1.1.29 | — |
| 4.32 实现 `CREATE STATISTICS` 及 dependencies/ndistinct/mcv 算法 | 1.1.27 | — |
| 4.33 实现 `CREATE SEQUENCE` 的 cache/cycle/min/max/ownership/transactional | 1.1.26 | — |
| 4.34 实现 `CREATE DOMAIN` 多约束与全表 revalidation | 1.1.18 | — |
| 4.35 实现 `CREATE FUNCTION` 完整语义（language/volatility/strict/parallel/cost/security definer） | 1.1.19 | — |
| 4.36 实现 `CREATE PROCEDURE` 语言运行时与事务控制 | 1.1.23 | — |
| 4.37 实现 `CREATE POLICY` `WITH CHECK` 完整验证 | 1.1.22 | — |
| 4.38 实现 `CREATE MATERIALIZED VIEW` `WITH [NO] DATA`、并发刷新 | 1.1.21, 4.10 | — |
| 4.39 移除 DDL 隐式提交，实现 DDL 事务化 | 16.5, 9.6 | 架构级 |
| 4.40 实现 `CREATE ASSERTION` 执行（如决定支持） | 5.9 | PG 本身未实现 |

---

## Phase 5：Planner / 执行器 / DQL / DML

**目标**：建立 path/relation/statistics 框架，实现 CBO、完整查询执行、并行查询。  
**前置**：Phase 1（Parser/AST）、Phase 2（Catalog）、Phase 4（类型/函数）。

| 子任务 | 涉及的 gap | 备注 |
|--------|-----------|------|
| 5.1 建立 path/relation/statistics 框架（`Path`/`RelOptInfo`/`PlannerInfo`） | 16.10, 7.2 | 架构级 |
| 5.2 实现等价类（equivalence classes）、pathkeys、参数化路径 | 7.2 | — |
| 5.3 实现 join search / join reordering（动态规划 / genetic） | 6.2, 7.2 | — |
| 5.4 实现 bitmap heap scan、bitmap and/or、多索引组合 | 7.4 | — |
| 5.5 实现 skip scan、index condition recheck、lossy pages | 8.2, 7.4 | — |
| 5.6 实现 CTE `MATERIALIZED/NOT MATERIALIZED`、可写 CTE 快照、递归检测 | 6.4 | — |
| 5.7 实现 `MERGE` 完整 WHEN 分支（BY SOURCE、DELETE、DO NOTHING、RETURNING） | 1.1.44, 6.13 | — |
| 5.8 实现 `INSERT` `DEFAULT VALUES`、`OVERRIDING`、conflict target/opclass/where | 1.1.41, 6.10 | — |
| 5.9 实现 `RETURNING` `OLD`/`NEW` aliases、trigger-modified rows 精确行为 | 6.12, 1.1.41 等 | — |
| 5.10 实现 `UPDATE FROM` / `DELETE USING` 的语义安全实现（非文本拼接） | 6.11, 1.1.58, 1.1.35 | — |
| 5.11 实现 Row locking 完整语义（`NO KEY UPDATE` / `KEY SHARE`、OF list） | 6.9 | — |
| 5.12 实现 subquery 完整语义（关联子查询、row comparison、NULL 语义） | 6.5 | — |
| 5.13 实现 Join 完整语义（SEMI/ANTI、lateral 完整相关性、outer join predicate 推理） | 6.2 | — |
| 5.14 实现 Set operations 完整语义（类型合并、collation、ALL/DISTINCT 作用域） | 6.3 | — |
| 5.15 实现 `GROUP BY` functionally dependent、`GROUPING_ID` 完整语义 | 6.7 | — |
| 5.16 实现 `ORDER BY` USING operator、位置编号、collation provider | 6.6 | — |
| 5.17 实现 `LIMIT/FETCH` `WITH TIES`、百分比 | 6.8 | — |
| 5.18 实现 `FOR UPDATE` with JOIN/GROUP BY/aggregate/window/scalar functions | 6.9 | — |
| 5.19 实现 EXPLAIN ANALYZE 真实节点级统计（时间/rows/buffers/WAL/IO） | 7.9, 1.1.39 | — |
| 5.20 实现 plan invalidation（基于 catalog/dependency） | 7.8 | — |
| 5.21 实现并行查询（Gather/Gather Merge、parallel scan/join/aggregate） | 7.5 | — |
| 5.22 实现 JIT（LLVM） | 7.6 | — |
| 5.23 实现 Async I/O（AIO） | 7.7 | PG18 |
| 5.24 实现 `SAVEPOINT` / `ROLLBACK TO` / `RELEASE` 子事务完整语义 | 1.1.49, 9.5 | — |
| 5.25 实现 `COMMIT`/`ROLLBACK` `AND [NO] CHAIN`、全局事务状态 | 1.1.14, 1.1.38, 1.1.8 | — |
| 5.26 实现 `PREPARE TRANSACTION` / `COMMIT PREPARED` 完整语义 | 1.1.15 | — |
| 5.27 实现 `COPY` `STDIN/STDOUT`、binary copy、`PROGRAM`、`FREEZE`、`HEADER MATCH` | 1.1.16 | — |
| 5.28 实现 `ANALYZE` PG 采样算法、统计对象、表达式统计、系统统计视图集成 | 1.1.7 | — |
| 5.29 实现 `CALL` PL/pgSQL/SQL procedure 运行时 | 1.1.10 | — |
| 5.30 实现 `DEALLOCATE`/`PREPARE`/`EXECUTE` 服务器端类型推断、binary params、plan cache | 1.1.34 | — |
| 5.31 实现 `CURSOR` 可滚动/二进制/holdable/portal 语义 / `MOVE` | 1.1.12, 1.1.45 | — |
| 5.32 实现 `VACUUM` freeze、visibility map、autovacuum launcher/workers、parallel vacuum | 1.1.59, 9.9 | — |
| 5.33 实现 `REINDEX` index/schema/database/system/`CONCURRENTLY` | 1.1.47 | — |
| 5.34 实现 `TRUNCATE` `ONLY`/多表/foreign table/transactional details | 1.1.57 | — |
| 5.35 实现 `LOCK` 全锁模式、`NOWAIT`、`ONLY`、锁队列/冲突矩阵 | 1.1.43 | — |
| 5.36 实现 `LISTEN`/`NOTIFY`/`UNLISTEN` 事务提交后发送、payload、跨进程语义 | 1.1.42 | — |
| 5.37 实现 `DISCARD` 完整语义 | 1.1.36 | — |
| 5.38 实现 `SECURITY LABEL` provider、对象类型全集 | 1.1.50 | — |
| 5.39 实现 `ALTER SYSTEM` PG GUC 体系 | 1.1.3 | — |
| 5.40 实现 `SET`/`SHOW`/`RESET` 完整 GUC 语义 | 1.1.52, 1.1.48 | — |
| 5.41 实现 `SET ROLE` / `SET SESSION AUTHORIZATION` 完整语义 | 1.1.54, 1.1.55 | — |
| 5.42 实现 `SET TRANSACTION` deferrable 完整语义 | 1.1.56 | — |
| 5.43 实现 SSI / predicate locks / SIREAD lock / rw-conflict | 9.4 | — |

---

## Phase 6：索引 Access Method / 索引生态

**目标**：统一 AM API，补全索引类型、并发索引、分区索引。  
**前置**：Phase 3（存储稳定）、Phase 5（Planner 需要索引信息）。

| 子任务 | 涉及的 gap | 备注 |
|--------|-----------|------|
| 6.1 实现 `amhandler`、support functions、opclass/opfamily、`amcostestimate`、`amvalidate` | 8.1, 16.7 | 架构级 |
| 6.2 补全 B-tree（dedup、suffix truncation、visibility map 驱动 index-only scan） | 8.2 | — |
| 6.3 补全 Hash（WAL-safe bucket split、metapage/overflow page） | 8.3 | — |
| 6.4 补全 GIN/GiST/BRIN/SP-GiST 的泛化 opclass（consistent/union/picksplit/penalty 等） | 8.4 | — |
| 6.5 实现 `CREATE INDEX CONCURRENTLY` 两阶段/三事务/invalid index catalog 状态 | 8.5, 1.1.20 | — |
| 6.6 实现 expression/partial/include 索引的完整支持（dependency、immutable 检查） | 8.6 | — |
| 6.7 实现 index maintenance（page deletion、vacuum cleanup、`amcheck`、`REINDEX CONCURRENTLY`） | 8.7, 1.1.47 | — |
| 6.8 实现 partitioned index attach/detach 和唯一约束跨分区规则 | 8.8, 4.5 | — |
| 6.9 补全 `CREATE INDEX` operator class/family、collation、NULLS sort、storage params、parallel build | 1.1.20 | — |

---

## Phase 7：安全 / 认证 / Wire Protocol

**目标**：支持 PostgreSQL wire protocol、libpq、完整认证体系。  
**前置**：Phase 2（Catalog 中有用户/角色信息）。可与 Phase 3~5 并行，但在对外提供服务前完成。

| 子任务 | 涉及的 gap | 备注 |
|--------|-----------|------|
| 7.1 实现 PostgreSQL wire protocol（Frontend/Backend protocol） | 16.6, 11.3 | 架构级 |
| 7.2 实现 `pg_hba.conf` 解析与匹配 | 11.2 | — |
| 7.3 实现 SCRAM-SHA-256 认证 | 11.2 | — |
| 7.4 实现 OAuth（PG18）、LDAP、Kerberos/GSSAPI、SSPI、RADIUS、PAM、cert、peer、ident | 11.2 | — |
| 7.5 实现 TLS 完整协商（SSL negotiation、client cert auth、channel binding） | 11.4 | — |
| 7.6 实现 ACL item、PUBLIC、grant options/admin options/set options、ownership 传播 | 11.5, 1.1.40 | — |
| 7.7 实现 `ALTER DEFAULT PRIVILEGES` 完整语义 | 1.1.1 | — |
| 7.8 实现 RLS executor-integrated 完整语义 | 11.6, 1.1.22 | — |
| 7.9 实现 SECURITY DEFINER/INVOKER、search_path 安全规则 | 11.7, 1.1.19 等 | — |
| 7.10 实现 `GRANT`/`REVOKE` ACL item 完整语义 | 1.1.40 | — |
| 7.11 实现 `ALTER USER`/`ALTER ROLE` 完整权限位（superuser/createdb/replication/bypassrls） | 1.1.5 | — |
| 7.12 实现 `CREATE ROLE`/`CREATE USER` 完整属性执行、成员继承、admin option | 1.1.24 | — |

---

## Phase 8：复制 / HA / PITR / 备份恢复

**目标**：在 WAL 稳定后实现流复制、逻辑解码、PITR。  
**前置**：Phase 3（WAL/MVCC）、Phase 7（认证）。

| 子任务 | 涉及的 gap | 备注 |
|--------|-----------|------|
| 8.1 实现物理流复制（streaming replication） | 12.1 | — |
| 8.2 实现 Standby / hot standby / read-only replica | 12.2 | — |
| 8.3 实现 replication slots | 12.3 | — |
| 8.4 实现 synchronous replication | 12.4 | — |
| 8.5 实现 cascading replication | 12.5 | — |
| 8.6 实现 logical decoding | 12.6 | — |
| 8.7 实现 publication / subscription | 12.7, 1.2.6 | — |
| 8.8 实现 WAL shipping / continuous archive / recovery.signal | 12.8 | — |
| 8.9 实现 PITR（Point-in-Time Recovery） | 10.7, 12.8 | — |
| 8.10 实现 `pg_basebackup` | 12.9 | — |
| 8.11 实现增量备份 / `pg_combinebackup` | 12.10 | — |
| 8.12 实现 failover / promote | 12.12 | — |
| 8.13 实现 `pg_dump` / `pg_restore` 格式和对象依赖顺序 | 12.11 | — |

---

## Phase 9：系统目录 / 监控 / 进程模型

**目标**：完整化 `pg_catalog`/`information_schema`/`pg_stat_*`，迁移到多进程模型。  
**前置**：Phase 2（Catalog 框架）、Phase 3（存储）、Phase 7（认证）。

| 子任务 | 涉及的 gap | 备注 |
|--------|-----------|------|
| 9.1 补全 `pg_catalog` 几百个 view/function | 13.1 | — |
| 9.2 补全 `information_schema` SQL 标准完整 views | 13.2 | — |
| 9.3 补全 `pg_stat_*`（pg_stat_io、progress views、replication views、wait events、backend memory contexts） | 13.3 | — |
| 9.4 实现 logging collector、CSV/JSON logs、`log_line_prefix` | 13.4 | — |
| 9.5 迁移到多进程 backend + shared memory 架构 | 16.8, 13.5 | 架构级 |
| 9.6 实现 `psql` 元命令兼容 | 13.6 | — |
| 9.7 实现 `initdb`、`createdb`/`dropdb`、`pg_ctl`、`pgbench` | 13.6 | — |
| 9.8 实现 `pg_upgrade` | 13.6 | — |

---

## Phase 10：扩展生态 / FDW / PL / Hook

**目标**：实现 PostgreSQL 扩展系统。  
**前置**：Phase 1~9 全部基础架构稳定。

| 子任务 | 涉及的 gap | 备注 |
|--------|-----------|------|
| 10.1 实现 `CREATE EXTENSION` / `ALTER EXTENSION` / `DROP EXTENSION` | 14.1, 1.2.5 | — |
| 10.2 实现 C extension / fmgr | 14.2 | — |
| 10.3 实现 PL/pgSQL、PL/Python、PL/Perl | 14.3 | — |
| 10.4 实现 FDW（`CREATE FOREIGN DATA WRAPPER` 等） | 14.4, 1.2.5 | — |
| 10.5 实现 custom type I/O（底层 base type 创建） | 14.5, 1.2.3 | — |
| 10.6 实现 custom operators / opclass | 14.6, 1.2.1 | — |
| 10.7 实现 logical decoding plugins | 14.7 | — |
| 10.8 实现 background workers | 14.8 | — |
| 10.9 实现 planner/executor hooks | 14.9 | — |
| 10.10 实现 shared memory extensions | 14.10 | — |
| 10.11 补全缺失命令（`CREATE AGGREGATE`/`ALTER AGGREGATE`/`DO`/`CREATE RULE`/`CREATE EVENT TRIGGER` 等） | 1.2.2, 1.2.4 | — |
| 10.12 补全文本搜索配置/词典/parser/template | 1.2.8 | — |
| 10.13 补全语言/大对象命令 | 1.2.7 | — |

---

## Phase 11：非 PG 语法清理与兼容模式

**目标**：处理项目支持但 PostgreSQL 不兼容的语法。  
**前置**：Phase 1（Parser 能区分语法）。可与 Phase 1 同步启动。

| 子任务 | 涉及的 gap | 备注 |
|--------|-----------|------|
| 11.1 `USE DATABASE db` → 移除或加兼容模式开关 | 15.1 | — |
| 11.2 `REPLACE INTO` → 移除或映射为 `INSERT ... ON CONFLICT` | 15.2 | — |
| 11.3 `LOAD DATA INFILE` → 映射为 `COPY` | 15.3 | — |
| 11.4 `SELECT ... INTO OUTFILE` → 映射为 `COPY TO` | 15.4 | — |
| 11.5 `DESC` / `VIEW TABLE` / `VIEW DATABASE` → 替换为 psql 元命令或 catalog 查询 | 15.5 | — |
| 11.6 `UPDATE/DELETE ... LIMIT` → 替换为 CTE/subquery | 15.6 | — |
| 11.7 `AUTO_INCREMENT` → 映射为 `serial` / `identity` | 15.7 | — |
| 11.8 `DATETIME`, `TINYINT`, `BLOB`, `NCHAR/NVARCHAR` → 移除或映射为 PG 类型 | 15.8 | — |
| 11.9 `SET GLOBAL` → 映射为 `ALTER SYSTEM` / GUC reload | 15.9 | — |

---

## 附录：架构级根本差距的分配

| 差距 | 分配阶段 | 说明 |
|------|---------|------|
| 16.1 Parser/AST | **Phase 1** | 所有上层的基础 |
| 16.2 Catalog/OID | **Phase 2** | 对象管理基础 |
| 16.3 WAL redo | **Phase 3** | 崩溃恢复基础 |
| 16.4 MVCC 版本链 | **Phase 3** | 并发控制基础 |
| 16.5 DDL 事务化 | **Phase 4** | 依赖 Catalog + WAL |
| 16.6 Wire Protocol | **Phase 7** | 客户端兼容 |
| 16.7 扩展系统 | **Phase 10** | 最后搭建 |
| 16.8 多进程模型 | **Phase 9** | 可与 Phase 3 并行设计 |
| 16.9 Buffer Manager | **Phase 3** | 存储基础 |
| 16.10 Planner 框架 | **Phase 5** | 查询优化基础 |

---

*生成日期：2026-06-10*  
*来源：all-gaps-todo.md — 逐条映射，无一遗漏*

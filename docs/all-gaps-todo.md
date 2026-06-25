# DBMS 全部 Gap TODO（唯一来源）

> 生成日期：2026-06-10
> 来源：基于 `postgresql-complete-gap-analysis.md` 逐条提取，**无一遗漏**
> 原则：本文件为唯一 TODO 来源，所有 gap 状态以此为准
> 规则：每条 gap 均需**原汁原味实现**，对标 PostgreSQL 18 语义与行为，禁止投机取巧

> **状态符号**：❌ 缺失　|　⚠️ 部分实现　|　✅ 已完成（语义对齐 PG）　|　🔄 有骨架/在途（已落地框架但未达 PG 完整语义）

---

## 更新记录

| 日期 | 摘要 |
|------|------|
| 2026-06-10 | 初始生成，基于 gap-analysis 逐条提取全部 gap |
| 2026-06-21 | 大范围同步实现现状：Phase 1（Parser/AST）、Phase 2（Catalog/OID）、Phase 3（WAL/MVCC/Buffer/Cluster）、Phase 4 Wave 0~2（TypeRegistry/ExprEvaluator/DDL AST 桥/DDL 事务骨架/函数/聚合/窗口骨架）已落地，逐章更新状态标记与"已完成进展"小节。详见各章末尾与下文总览。 |
| 2026-06-21（修正） | 经运行时核对，**Phase 1/2 为"模块+测试已写、未接入主程序 `execute()` 运行时"**：DML 全部走字符串分发（AST 未被消费）；`CatalogManager` 在 main.cpp 从未构造、`migrateDatabaseToCatalog` 无调用者、`planDrop` 无 DROP 处理器接入、核心系统表不可查、`resolveTableName` 仍用 `schema__table` 编码。据此下调 §1/§4/§16/§17 相关表述，16.1 由 ✅ 改 🔄，4.1/4.2 维持 🔄 但注明未接入运行时。Phase 3（WAL/MVCC/Buffer）为真实接入运行时。 |
| 2026-06-25 | Phase 4 Wave 3 约束/默认值/生成列落地：DEFAULT 表达式（含字面量与复杂表达式）、GENERATED ALWAYS AS (expr) STORED、GENERATED/BY DEFAULT AS IDENTITY、CHECK 约束在 INSERT/UPDATE 路径通过 `ExprHelper` + `ExprEvaluator` 真正执行；新增 `src/expression/expr_helper.h/.cpp` 与 `tests/constraint_expr_test.cpp`；修复 parser 将数字/字符串字面量误当列引用、DDL 桥工厂函数覆盖 `Column` 导致元数据丢失、IDENTITY 大小写解析等问题。`docs/implementation-plan.md` 同步更新 Phase 4 Wave 3 完成状态。 |
| 2026-06-25 | Phase 4 Wave 4.33 SEQUENCE 完整语义落地：扩展 `SequenceInfo`/`StorageEngine` 序列文件格式，支持 START/INCREMENT/MINVALUE/MAXVALUE/CACHE/CYCLE/OWNED BY；`parseCreateSequence` 解析全部选项并新增 `parseAlterSequence`/`DdlExecutor::executeAlterSequence`；`DROP TABLE CASCADE` 删除被拥有序列；新增 `tests/sequence_full_test.cpp`。 |

> 2026-06-21 更新方法：核对 `src/`（parser/catalog/storage/expression/commands）、`tests/` 与 `docs/implementation-plan.md`、`docs/phase4-plan.md` 的实际代码与提交历史，将仍标 ❌/⚠️ 但代码中已有真实实现的条目上调；仍处于骨架或未开始的条目保留并标注 🔄/❌。未对齐 PG 完整语义的条目即便有实现仍标 ⚠️。

---

## 总览

> 以下为 **2026-06-21** 重新核对后的维度汇总。完成度较 2026-06-10 版本显著上升：Parser/AST、Catalog/OID/依赖、WAL redo/LSN、MVCC 版本链、Buffer/Cluster/forks、TypeRegistry、表达式求值器、DDL AST 桥接已落地为真实代码（含测试），但仍多数未达 PG **完整语义**，故多为 ⚠️ 而非 ✅。

| 维度 | ✅ 已完整 | ⚠️ 部分实现 | ❌/🔄 缺失或骨架 | 备注 |
|------|-----------|-------------|--------|------|
| SQL 命令 | 约 65 条 | 约 55 条 | 约 45 条 ❌ | DDL 已可解析为 AST 并经 DdlExecutor 执行（TABLE/INDEX/VIEW/SEQUENCE/DOMAIN/TYPE/DB/SCHEMA/COMMENT） |
| 数据类型 | 约 10 种 | 约 35 种 | 约 3 种 ❌ | TypeRegistry 统一注册 40+ 类型与别名/修饰符校验；多数类型仍"字符串存储+注册校验"，未达 PG I/O 语义 |
| 表达式/函数 | 基础子集 | 中量 | 大量 | ExprEvaluator 支持 cast/case/coalesce/between/in/like/funcall/子查询；内置函数子集 |
| DDL 对象模型 | 基础 | 简化→中量（模块+测试） | 架构级未接入运行时 | pg_class/attribute/type/proc/depend/namespace + OID/依赖 CASCADE/RESTRICT 模块已写并有测试，但 **CatalogManager 未在 main.cpp 构造、DROP 未接 planDrop**，运行时仍走文件+`schema__table` |
| 约束 | 6 类 | 5 类 | 1 类 ❌ | DEFAULT/GENERATED STORED/IDENTITY/CHECK 已接入 INSERT/UPDATE 执行路径（ExprHelper + ExprEvaluator）；DEFERRABLE 延迟队列、EXCLUDE 执行检查仍 ⚠️ |
| DQL/查询 | 大量 | 中量 | 大量 | SELECT 已解析为 AST（CTE/JOIN/SET OPS）；执行仍走旧 switch/case |
| 优化器/执行器 | 简化 | 中量 | 大量 ❌ | `src/optimizer/` 仍为空；Path/RelOptInfo 框架未建（Phase 5 未启动） |
| 索引 | 6 种 | 简化 | 大量 ❌ | IIndexAM 适配器已统一；AM API/opclass/concurrent/维护仍 ❌ |
| 事务/MVCC | 基础→中量 | 中量 | 部分 ❌ | xmin/xmax/ctid/HOT、CLOG(pg_xact)、snapshot export/import+subxip 已实现；SSI/完整子事务仍 ❌ |
| 存储/WAL | 基础 ✅ | 中量 | 部分 ❌ | redo WAL(LSN/segment/full-page/redo/timeline/archive)、forks(main/fsm/vm/init)、BufferPool(clock sweep/pin)、ClusterLayout、TOAST、checksum 已实现；PITR/真实 freeze 仍 ❌ |
| 安全/权限 | 基础 | 简化 | 大量 ❌ | pg_authid/auth_members 已建；pg_hba/SCRAM/wire protocol 仍 ❌ |
| 复制/HA | 0 | WAL archive 1 项 | 全部 ❌ | `src/replication/` 仅有 README；流复制/逻辑复制/PITR 全缺 |
| 监控/诊断 | 子集 | 子集 | 大量 ❌ | pg_stat_activity/locks/statements 风格子集；pg_stat_io/wait events 缺 |
| 扩展/生态 | 0 | 0 | 全部 ❌ | EXTENSION/FDW/PL 全缺；event trigger/rule 仅 parser classify stub |

---

## 1. SQL 命令覆盖差距

### 1.1 有实现但与 PostgreSQL 不等价的命令

> **已完成进展（2026-06-22）**：Phase 1 Parser/AST 与 Phase 2 Catalog/OID 已接入运行时。`main.cpp::execute()` 通过 `tryDdlBridge()` 调用 `DdlExecutor` 处理 DDL，修复双执行 bug；CTAS 保留旧路径回退。`StorageEngine` 新增 `CatalogService` 缓存，首次访问时引导系统 namespace/type 并迁移旧 `.stc` 数据库；`CREATE TABLE`/`DROP TABLE`/`CREATE INDEX`/`CREATE SEQUENCE`/`CREATE SCHEMA`/`DROP SEQUENCE`/`DROP SCHEMA` 同步维护 `pg_class`/`pg_attribute`/`pg_type`/`pg_namespace`/`pg_depend`；`DROP TABLE CASCADE` 通过依赖图删除从属索引。`pg_class`/`pg_namespace`/`pg_type` 作为只读虚拟系统表支持 `SELECT *`。`StorageEngine::checkpoint()` 持久化目录，`DROP DATABASE` 先 `evict()` 缓存。**DML（SELECT/INSERT/UPDATE/DELETE/MERGE）仍走旧字符串分发，AST 未被消费**——已明确延期为独立后续任务。DDL 中仍缺 `ALTER TABLE`/`DROP INDEX`/视图/物化视图/触发器/策略/函数/过程的目录集成。

| # | 命令 | 差距描述 | 状态 |
|---|------|---------|------|
| 1.1.1 | `ALTER DEFAULT PRIVILEGES` | 只解析 `GRANT` 路径；缺少完整 `REVOKE`、对象类型、角色继承、schema/default ACL 语义 | ⚠️ |
| 1.1.2 | `ALTER SCHEMA` | 主要支持 `RENAME TO`；缺少 owner、权限、依赖重写 | ⚠️ |
| 1.1.3 | `ALTER SYSTEM` | 只写项目 `dbms.conf` 中有限参数；不是 PG GUC 体系 | ⚠️ |
| 1.1.4 | `ALTER TABLE` | 仍缺 PG 全量子命令、`IF EXISTS`、`ONLY`、`INHERIT`、`OWNER`、`TABLESPACE`、`REPLICA IDENTITY`、统计目标、触发器状态全集和真正延迟约束队列 | ⚠️ |
| 1.1.5 | `ALTER USER` / `ALTER ROLE` | 缺少真实 superuser/createdb/replication/bypassrls 权限位、连接限制、valid until、配置参数执行语义 | ⚠️ |
| 1.1.6 | `ALTER VIEW` | 缺少 owner、options、column default、安全屏障、security invoker | ⚠️ |
| 1.1.7 | `ANALYZE` | 有表/多列统计；缺少 PG 采样算法、统计对象、表达式统计、分区/继承精细规则、`VERBOSE` 输出、系统统计视图集成 | ⚠️ |
| 1.1.8 | `ABORT` | 已作为 `ROLLBACK` 别名接入；缺少 `AND [NO] CHAIN` 等完整事务结束选项 | ⚠️ |
| 1.1.9 | `BEGIN` / `START TRANSACTION` | 隔离级别和只读选项为简化解析，事务特性选项不全 | ⚠️ |
| 1.1.10 | `CALL` | 只执行项目内字符串过程，参数替换简化；不是 PL/pgSQL/SQL procedure 运行时 | ⚠️ |
| 1.1.11 | `CHECKPOINT` | 刷页和清 WAL；没有真实 checkpoint LSN、redo pointer、WAL segment 管理 | ⚠️ |
| 1.1.12 | `CLOSE` / `DECLARE` / `FETCH` | 游标把 SELECT 结果捕获到内存；缺少可滚动/二进制/holdable cursor、事务生命周期、portal 语义、`MOVE` | ⚠️ |
| 1.1.13 | `COMMENT` | 主要支持 table/column；缺少 PG 支持的绝大多数对象 | ⚠️ |
| 1.1.14 | `COMMIT` / `ROLLBACK` | 有基本事务；与 PG 的 MVCC、subtransaction、WAL crash safety 差距大 | ⚠️ |
| 1.1.15 | `COMMIT PREPARED` / `PREPARE TRANSACTION` / `ROLLBACK PREPARED` | 有二阶段入口，但没有 PG 的全局事务状态目录、崩溃恢复/锁/资源完整语义 | ⚠️ |
| 1.1.16 | `COPY` | 文件 CSV 导入导出；缺少 `STDIN/STDOUT` 协议、binary copy、`PROGRAM`、`FREEZE`、`HEADER MATCH`、encoding/options 完整矩阵和权限模型 | ⚠️ |
| 1.1.17 | `CREATE DATABASE` | 创建目录；缺少 template、owner、locale/collation provider、encoding、tablespace、OID/catalog 语义 | ⚠️ |
| 1.1.18 | `CREATE DOMAIN` | 支持 base/default/check；缺少多约束、全表 revalidation、依赖/权限/类型系统深度集成 | ⚠️ |
| 1.1.19 | `CREATE FUNCTION` | 简单表达式/表值函数；缺少 language、volatility、strict、parallel、cost、rows、security definer、leakproof、set config、polymorphic、C/SQL/PL 函数 | ⚠️ |
| 1.1.20 | `CREATE INDEX` | 支持 btree/hash/GIN/GiST/BRIN/SP-GiST 风格、include/where/expression/concurrently；缺少 operator class/family、collation、NULLS sort、storage params、parallel build、真正 concurrent algorithm、AM API | ⚠️ |
| 1.1.21 | `CREATE MATERIALIZED VIEW` | 用 backing table 保存结果；缺少 `WITH [NO] DATA`、唯一索引要求、并发刷新语义、依赖追踪 | ⚠️ |
| 1.1.22 | `CREATE POLICY` | 有 RLS policy 文件；`WITH CHECK` 评估在源码注释中明确为 best-effort/简化 | ⚠️ |
| 1.1.23 | `CREATE PROCEDURE` | 多条 SQL 字符串顺序执行；缺少语言运行时、事务控制规则、异常、变量、权限属性 | ⚠️ |
| 1.1.24 | `CREATE ROLE` / `CREATE USER` | 用户在 `user.dat`，角色在 `role.dat`；缺少 PG 角色属性执行、成员继承、admin option、系统 catalog | ⚠️ |
| 1.1.25 | `CREATE SCHEMA` | 用 `schema__table` 或 marker 文件模拟；缺少真正 namespace、owner、search_path 语义 | ⚠️ |
| 1.1.26 | `CREATE SEQUENCE` | 有 nextval 文件；缺少 cache/cycle/min/max/ownership/transactional semantics | ⚠️ |
| 1.1.27 | `CREATE STATISTICS` / `ALTER STATISTICS` / `DROP STATISTICS` | 有扩展统计对象元数据；缺少 `pg_statistic_ext` catalog、表达式统计、dependencies/ndistinct/mcv 精确算法和 planner 深度使用 | ⚠️ |
| 1.1.28 | `CREATE TABLE` | 可建表、分区、临时/unlogged、继承等部分；缺少大量表选项、`LIKE INCLUDING` 全集、typed table、`OF type`、access method、tablespace、identity/生成列完整语义 | ⚠️ |
| 1.1.29 | `CREATE TABLE AS` | 有 CTAS 路径；缺少 PG 选项、`WITH [NO] DATA`、tablespace/access method、精确类型推断 | ⚠️ |
| 1.1.30 | `CREATE TABLESPACE` / `ALTER TABLESPACE` / `DROP TABLESPACE` | 已支持表空间对象元数据；缺少物理存储路由、权限、依赖检查和 `pg_tblspc` 符号链接语义 | ⚠️ |
| 1.1.31 | `CREATE TRIGGER` | 支持 before/after/instead of、row/statement、`WHEN`、action SQL；缺少 transition tables、constraint triggers、deferred triggers、tg_* 全量、trigger function runtime | ⚠️ |
| 1.1.32 | `CREATE TYPE` | 主要支持 composite type；缺少 PG 的 enum/range/base/shell 类型创建语义 | ⚠️ |
| 1.1.33 | `CREATE VIEW` | 支持保存 SQL 和简单 updatable view；缺少 recursive view、security_barrier、security_invoker、check option 完整性 | ⚠️ |
| 1.1.34 | `DEALLOCATE` / `PREPARE` / `EXECUTE` | 使用字符串 `?` 替换；缺少服务器端类型推断、binary params、plan invalidation、generic/custom plan、portal | ⚠️ |
| 1.1.35 | `DELETE` | 支持 WHERE/USING/LIMIT/RETURNING 部分；缺少 PG 全语义、CTE/`ONLY`/inheritance/RETURNING OLD/NEW 复杂表达式 | ⚠️ |
| 1.1.36 | `DISCARD` | 主要 `DISCARD ALL` 清 session 局部状态；不完整 | ⚠️ |
| 1.1.37 | `DROP ...` 常见对象 | table/database/view/mview/index/trigger/user/role/group/schema/domain/type/sequence/function/procedure 等部分；缺少依赖图、`CASCADE/RESTRICT` 精确行为、`IF EXISTS`/多对象列表完整支持 | ⚠️ |
| 1.1.38 | `END` | 已作为 `COMMIT` 别名接入；缺少 `AND [NO] CHAIN` 等完整事务结束选项 | ⚠️ |
| 1.1.39 | `EXPLAIN` | 只面向 SELECT 的简化计划；缺少真实 runtime instrumentation、JIT/WAL/BUFFERS/SETTINGS 完整输出和所有语句支持 | ⚠️ |
| 1.1.40 | `GRANT` / `REVOKE` | 支持有限 privilege 和列权限；缺少 PostgreSQL ACL item、`PUBLIC`、role inheritance/admin option/set option、对象类型全集、默认权限联动 | ⚠️ |
| 1.1.41 | `INSERT` | 支持 values、insert-select、on conflict、returning 部分；缺少 `DEFAULT VALUES`、`OVERRIDING`、ON CONFLICT constraint/index inference 全集、RETURNING OLD/NEW | ⚠️ |
| 1.1.42 | `LISTEN` / `NOTIFY` / `UNLISTEN` | 进程内 map；缺少事务提交后发送、payload 长度/通道语义、跨进程持久服务语义 | ⚠️ |
| 1.1.43 | `LOCK` | 支持 share/exclusive；缺少 PG 全锁模式、`NOWAIT`、`ONLY`、锁队列/冲突矩阵 | ⚠️ |
| 1.1.44 | `MERGE` | 支持 `MATCHED UPDATE` / `NOT MATCHED INSERT` 的窄路径；缺少 BY SOURCE、DELETE、DO NOTHING、多个 WHEN、RETURNING OLD/NEW、复杂 source query | ⚠️ |
| 1.1.45 | `MOVE` | 已支持内存游标的位置移动；缺少 PostgreSQL portal、holdable/scrollable cursor、事务生命周期和精确边界语义 | ⚠️ |
| 1.1.46 | `REFRESH MATERIALIZED VIEW` | 重跑 SELECT，`CONCURRENTLY` 只是入口标志；缺少 PG 并发刷新条件和锁语义 | ⚠️ |
| 1.1.47 | `REINDEX` | 基本 `REINDEX TABLE`；缺少 index/schema/database/system、`CONCURRENTLY`、tablespace、verbose | ⚠️ |
| 1.1.48 | `RESET` | 支持 `RESET ROLE`/`RESET ALL` 等；缺少完整 GUC 语义 | ⚠️ |
| 1.1.49 | `SAVEPOINT` / `ROLLBACK TO` / `RELEASE` | 基于 txn log index；缺少 PG 子事务资源/锁/错误状态完整语义 | ⚠️ |
| 1.1.50 | `SECURITY LABEL` | 保存 label 文件；缺少 provider、对象类型全集、SELinux/sepgsql 集成 | ⚠️ |
| 1.1.51 | `SELECT` | 支持大量子集；复杂 grammar、类型推断、表达式、函数、子查询、锁、并行、planner/rewrite 差距最大 | ⚠️ |
| 1.1.52 | `SET` / `SHOW` | 项目参数和少量 session 状态；不是 PG GUC 全体系 | ⚠️ |
| 1.1.53 | `SET CONSTRAINTS` | 已解析 `IMMEDIATE`/`DEFERRED` 并记录会话标志；缺少 deferrable 约束队列、提交时检查和约束触发器语义 | ⚠️ |
| 1.1.54 | `SET ROLE` | 只改 Session 字段；缺少权限检查、role stack、session authorization 联动 | ⚠️ |
| 1.1.55 | `SET SESSION AUTHORIZATION` | 已支持管理员切换 session user；缺少 PostgreSQL 角色继承、SET ROLE 权限矩阵和会话安全上下文完整语义 | ⚠️ |
| 1.1.56 | `SET TRANSACTION` | 隔离级别/只读部分；缺少 deferrable、当前事务时序限制完整语义 | ⚠️ |
| 1.1.57 | `TRUNCATE` | 支持 cascade/restart identity 部分；缺少 `ONLY`/多表/trigger/identity/foreign table/transactional details | ⚠️ |
| 1.1.58 | `UPDATE` | 支持 FROM/LIMIT/RETURNING 部分；缺少完整 FROM 多表语义、`WHERE CURRENT OF`、OLD/NEW RETURNING、复杂表达式 | ⚠️ |
| 1.1.59 | `VACUUM` | compact/free page；缺少 freeze、visibility map、autovacuum launcher/workers、parallel vacuum、analyze coupling、wraparound 防护 | ⚠️ |
| 1.1.60 | `VALUES` | 已支持顶层 `VALUES (..), (..)` 输出；缺少完整表达式求值、类型合并、排序/limit 组合和作为通用 query expression 的全部语义 | ⚠️ |

### 1.2 PostgreSQL 18 命令级缺失清单

| # | 类别 | 缺失命令 | 状态 |
|---|------|---------|------|
| 1.2.1 | 访问方法/索引生态 | `CREATE ACCESS METHOD`, `DROP ACCESS METHOD`, `ALTER OPERATOR CLASS`, `CREATE OPERATOR CLASS`, `DROP OPERATOR CLASS`, `ALTER OPERATOR FAMILY`, `CREATE OPERATOR FAMILY`, `DROP OPERATOR FAMILY`, `CREATE OPERATOR`, `ALTER OPERATOR`, `DROP OPERATOR` | ❌ |
| 1.2.2 | 聚合/函数生态 | `CREATE AGGREGATE`, `ALTER AGGREGATE`, `DROP AGGREGATE`, `ALTER FUNCTION`, `ALTER PROCEDURE`, `ALTER ROUTINE`, `DROP ROUTINE`, `DO`, `CREATE TRANSFORM`, `DROP TRANSFORM` | ❌ |
| 1.2.3 | Collation/encoding/casts | `CREATE CAST`, `DROP CAST`, `CREATE COLLATION`, `ALTER COLLATION`, `DROP COLLATION`, `CREATE CONVERSION`, `ALTER CONVERSION`, `DROP CONVERSION` | ❌ |
| 1.2.4 | 事件/规则/重写系统 | `CREATE EVENT TRIGGER`, `ALTER EVENT TRIGGER`, `DROP EVENT TRIGGER`, `CREATE RULE`, `ALTER RULE`, `DROP RULE` | ❌ |
| 1.2.5 | 扩展/FDW/外部数据 | `CREATE EXTENSION`, `ALTER EXTENSION`, `DROP EXTENSION`, `CREATE FOREIGN DATA WRAPPER`, `ALTER FOREIGN DATA WRAPPER`, `DROP FOREIGN DATA WRAPPER`, `CREATE FOREIGN TABLE`, `ALTER FOREIGN TABLE`, `DROP FOREIGN TABLE`, `CREATE SERVER`, `ALTER SERVER`, `DROP SERVER`, `CREATE USER MAPPING`, `ALTER USER MAPPING`, `DROP USER MAPPING`, `IMPORT FOREIGN SCHEMA` | ❌ |
| 1.2.6 | 高可用/逻辑复制 | `CREATE PUBLICATION`, `ALTER PUBLICATION`, `DROP PUBLICATION`, `CREATE SUBSCRIPTION`, `ALTER SUBSCRIPTION`, `DROP SUBSCRIPTION` | ❌ |
| 1.2.7 | 语言/大对象 | `CREATE LANGUAGE`, `ALTER LANGUAGE`, `DROP LANGUAGE`, `ALTER LARGE OBJECT`, `DROP LARGE OBJECT` | ❌ |
| 1.2.8 | 统计/表空间/全文配置 | `CREATE TEXT SEARCH CONFIGURATION`, `ALTER TEXT SEARCH CONFIGURATION`, `DROP TEXT SEARCH CONFIGURATION`, `CREATE TEXT SEARCH DICTIONARY`, `ALTER TEXT SEARCH DICTIONARY`, `DROP TEXT SEARCH DICTIONARY`, `CREATE TEXT SEARCH PARSER`, `ALTER TEXT SEARCH PARSER`, `DROP TEXT SEARCH PARSER`, `CREATE TEXT SEARCH TEMPLATE`, `ALTER TEXT SEARCH TEMPLATE`, `DROP TEXT SEARCH TEMPLATE` | ❌ |

---

## 2. 数据类型差距

> **已完成进展（2026-06-21）**：Phase 4 Wave 0 已建立 `TypeRegistry`/`TypeInfo`（`src/catalog/type_registry.{h,cpp}`），bootstrap 注册 40+ 内置类型，含类型别名规范化（`int`→`integer`、`varchar`→`character varying`、`bool`→`boolean`、`datetime`→`timestamp` 等）与类型修饰符校验（`varchar(n)`/`numeric(p,s)`/`timestamp(p)`/`bit(n)`/`interval(p)`）。`TINYINT`/`BLOB`/`NCHAR/NVARCHAR` 已作为兼容别名内部映射。下表多数 ⚠️ 类型现在**有注册与校验**，但仍以"字符串存储 + 注册校验"承载，未达 PG 的 I/O、运算、函数语义（对应 phase4-plan Wave 1，4.1~4.18 多数尚未拆出 `src/types/` 独立实现）。逐条细化：2.7 enum 已注册并存储标签（catalog 化/`ALTER TYPE ADD VALUE` 待 Phase 4.6）；2.10 bit 已校验长度修饰符（位运算待 Phase 4.9）；2.17 range/multirange 已注册（canonicalization/operators/functions 待 Phase 4.16）；2.21 伪类型已注册占位（调用约束待 Phase 4.18）。Wave 1/2 已落地的类型 I/O 与函数以 🔄 标注。

| # | PostgreSQL 类型域 | 差距描述 | 状态 |
|---|------------------|---------|------|
| 2.1 | 数值类型 | `numeric/decimal(p,s)` 精度、scale、四舍五入、溢出、NaN/Infinity、运算符族不完整；`money` 以 double 类路径处理，不是 PG money | ⚠️ |
| 2.2 | 整数类型 | PG 无 tinyint；类型大小、溢出、隐式转换、序列联动与 PG 不完全一致 | ⚠️ |
| 2.3 | 字符/文本 | 缺少 collation provider、ICU、排序规则、编码转换、正则/LIKE 全语义；标识符和列名长度限制明显不同 | ⚠️ |
| 2.4 | 二进制 | PG 主要是 `bytea`，输入输出、escape/hex、函数操作不同 | ⚠️ |
| 2.5 | 日期时间 | 缺少 PG time zone 规则库、infinity、BC 日期、精度、interval 字段限定、复杂输入输出。`datetime` 是非 PG 类型 | ⚠️ |
| 2.6 | 布尔 | SQL 三值逻辑、类型转换、函数/聚合边界仍简化 | ⚠️ |
| 2.7 | ENUM | 列定义里有 enum values；缺少 `CREATE TYPE ... AS ENUM` 的 catalog、排序、`ALTER TYPE ADD VALUE` | ⚠️ |
| 2.8 | 几何类型 | 只明确支持 `point` 和少量空间比较；缺少 `line`、`lseg`、`box`、`path`、`polygon`、`circle` 及完整函数/操作符 | ⚠️ |
| 2.9 | 网络类型 | 有 `inet/cidr` IPv4/IPv6 路径；`macaddr/macaddr8` 已可作为字符串型列存取；缺少 MAC 地址校验和 PG 全套网络函数 | ⚠️ |
| 2.10 | bit string | `bit` / `bit varying` 已不再按 bool 解析，可作为字符串型列存取；缺少长度约束和位运算 | ⚠️ |
| 2.11 | 全文搜索类型 | `tsvector`、`tsquery` 已可作为字符串型列存取；文本搜索配置/词典/parser/template 有目录级 DDL，但缺少 PG parser、ranking、operator 和 GIN opclass 语义 | ⚠️ |
| 2.12 | UUID | 有 uuid 列；缺少 PG 18 `uuidv7()`、uuid 函数、输入严格性与扩展生态 | ⚠️ |
| 2.13 | XML | 有 xml 列；缺少 XML 类型函数、XPath、XMLTABLE、schema/encoding 语义 | ⚠️ |
| 2.14 | JSON/JSONB | 有 JSON 校验和少量函数；缺少 jsonpath、SQL/JSON query functions、`JSON_TABLE`、完整操作符、GIN opclass | ⚠️ |
| 2.15 | 数组 | 有 `INT[]`/`VARCHAR[]` 痕迹和 array_get/contains 简化；缺少多维数组、切片、unnest/array functions、ANY/ALL 完整语义 | ⚠️ |
| 2.16 | Composite | `CREATE TYPE AS (...)` 存字段，并可通过 `ALTER TYPE` 更新 composite 名称和属性；缺少 row constructor、字段访问、嵌套、函数参数/返回、catalog 语义 | ⚠️ |
| 2.17 | Range/Multirange | `int4range`、`int8range`、`numrange`、`tsrange`、`tstzrange`、`daterange` 及 multirange 名称已可作为字符串型列存取；缺少范围 canonicalization、约束、operators、函数和 GiST/SP-GiST opclass | ⚠️ |
| 2.18 | Domain | 有 base/default/check 文件并可通过 `ALTER DOMAIN` 更新 rename/default/单 check constraint；缺少多 constraint validation、依赖、权限、数组自动类型 | ⚠️ |
| 2.19 | Object Identifier | `oid`、`regclass`、`regproc`、`regtype`、`xid`、`cid` 等名称已可作为字符串型列存取；缺少 OID catalog 绑定、别名解析、依赖和系统函数语义 | ⚠️ |
| 2.20 | `pg_lsn` | 有列类型，但没有真实 WAL LSN 系统，因而不能承担 PG `pg_lsn` 语义 | ⚠️ |
| 2.21 | Pseudo-types | `record`、`anyelement`、`anyarray`、`cstring`、`trigger` 等名称已可被解析为字符串型列/元数据占位；仍缺 PostgreSQL 函数类型系统和调用约束语义 | ⚠️ |

---

## 3. 表达式、函数与操作符差距

> **已完成进展（2026-06-21）**：Phase 1 已实现分层 operator precedence 解析器（OR→AND→NOT→IS→比较→BETWEEN/IN/LIKE→`||`→`+/-`→`*//%`→`^`→一元→`::`→后缀→primary），支持 CASE/EXISTS/子查询/数组下标/cast。Phase 4 Wave 0.2 已建立 `ExprEvaluator`（`src/expression/ExprEvaluator.{h,cpp}`），支持常量/列引用/NULL/CAST/二元一元/CASE/COALESCE/NULLIF/GREATEST/LEAST/比较/BETWEEN/IN/LIKE/ILIKE/逻辑/函数调用/简单子查询；Wave 2 已新增内置函数子集与聚合/窗口骨架（`tests/functions_test.cpp`、`tests/window_functions_test.cpp`、`tests/core_types_test.cpp`）。3.1 由 ❌ 上调为 🔄（解析层具备 precedence/类型解析/schema-qualified/named args；**执行期**完整隐式 cast 与函数重载解析仍 ⚠️）。

| # | 功能域 | 差距描述 | 状态 |
|---|--------|---------|------|
| 3.1 | 表达式 parser | 没有完整 operator precedence、类型解析、隐式 cast、函数重载、schema-qualified function、named/default args | 🔄 |
| 3.2 | 逻辑/比较 | NULL 存储用空字符串近似，`IS DISTINCT FROM` 被预处理改写，复杂 NULL 语义不完整 | ⚠️ |
| 3.3 | 数学函数 | 只覆盖常见函数；缺少 PG 全套 numeric/math/random/trig/hyperbolic/bitwise 函数和精确类型返回 | ⚠️ |
| 3.4 | 字符串函数 | 只覆盖常见函数；缺少 `format`、regexp 系列全集、collation-aware 行为、encoding-aware 字符长度 | ⚠️ |
| 3.5 | 日期时间函数 | 支持 current/now/extract/date_trunc 等部分；缺少 time zone database、`date_bin`、justify 系列、precision、interval field 复杂行为 | ⚠️ |
| 3.6 | JSON/XML/Array/Range | 均为小子集；缺少 PostgreSQL 18 SQL/JSON、JSON_TABLE、XMLTABLE、array/range/multirange 全函数 | ⚠️ |
| 3.7 | 聚合函数 | 有 count/sum/avg/min/max、部分 string/json/array 聚合痕迹；缺少 ordered-set/hypothetical-set、percentile、统计回归聚合、parallel aggregate | ⚠️ |
| 3.8 | 窗口函数 | 支持一批常见函数；缺少命名窗口复用完整语义、frame exclusion、复杂 RANGE/GROUPS 类型行为、ordered-set over | ⚠️ |
| 3.9 | 系统函数 | 只有少量 `pg_*` 函数拦截；缺少系统信息、WAL、replication、snapshot、privilege inquiry、object addressing、admin 函数全集 | ⚠️ |
| 3.10 | 用户自定义函数 | 仅保存表达式或 SQL 字符串；缺少语言、严格性、稳定性、并行安全、security definer、成本、依赖和 plan cache | ⚠️ |

---

## 4. DDL 和对象模型差距

> **已完成进展（2026-06-21，含运行时核对）**：⚠️ **Phase 2 模块与单元测试已写，但尚未接入主程序运行时**。`CatalogManager`（`src/catalog/catalog.{h,cpp}`）以内存缓存 + 按 OID/名称索引 + CSV 持久化承载 `pg_namespace`/`pg_class`/`pg_attribute`/`pg_type`/`pg_proc`/`pg_depend`/`pg_authid`/`pg_auth_members`/`pg_description` 系统表行格式；OID 分配器（单调递增/批量预留/空闲回收，持久化 `.oid_counter.free`）；`planDrop` 拓扑排序 + pin 保护 + 循环检测 + RESTRICT/CASCADE；`migrateDatabaseToCatalog` 迁移工具；临时 schema + 嵌套锁死锁修复——**以上均有测试覆盖，但运行时未生效**：`CatalogManager` 在 `main.cpp` 中从未被构造（仅测试/README 使用），`migrateDatabaseToCatalog` 无任何调用者，`planDrop` 仅被 catalog 内部 `dropObject` 调用而**无 DROP 命令处理器接入**。`pg_class`/`pg_attribute`/`pg_type`/`pg_proc`/`pg_depend` **不可查询**（只有 pg_database/pg_tables/pg_indexes/pg_settings/pg_roles/pg_namespace 是 main.cpp ~14208 的硬编码虚拟表）。`resolveTableName`（main.cpp:2504）仍用 `schema__table` 字符串拼接，未走 CatalogManager 的 search_path。`TypeRegistry::instance()` 全程序仅被调用 1 次（TableManage.cpp:6057 建表列校验）。故 4.1/4.2 仍标 🔄（**框架已写+有测试，未接入运行时**），4.3 标 ⚠️（运行时表名解析仍是 `schema__table` 编码，非真正 search_path）。后续真正"完成 Phase 2"的关键是把 CatalogManager 接入建表/删表/表名解析执行路径。

| # | 领域 | 差距描述 | 状态 |
|---|------|---------|------|
| 4.1 | Catalog/OID | 没有完整 `pg_class`、`pg_attribute`、`pg_type`、`pg_proc`、`pg_depend`、`pg_namespace` 等对象图；对象用文件和字符串管理 | 🔄 |
| 4.2 | 依赖管理 | DROP/ALTER 缺少 PostgreSQL 的依赖追踪、`CASCADE/RESTRICT` 精确规则、rewrite/revalidation | 🔄 |
| 4.3 | Schema/search_path | Schema 用表名编码或 marker 文件模拟；缺少真正 namespace、search_path 解析、临时 schema、权限继承 | ⚠️ |
| 4.4 | 表定义 | 缺少 PG 的 access method、tablespace、typed table、`LIKE INCLUDING/EXCLUDING` 全集、storage/compression、statistics target、replica identity、reloptions 全集 | ⚠️ |
| 4.5 | 分区 | 有 range/list/hash/attach/detach 子集；缺少 PG 分区约束证明、分区索引联动、global/local index 语义、默认分区验证、运行时 partition pruning 完整能力 | ⚠️ |
| 4.6 | 继承 | 有 `.inherits` 文件和查询合并痕迹；缺少 PG 表继承的约束/触发器/权限/`ONLY`/`NO INHERIT` 完整语义 | ⚠️ |
| 4.7 | 临时表 | 有会话临时表；缺少 `ON COMMIT DROP/DELETE/PRESERVE`、temp schema、catalog visibility、跨事务生命周期细节 | ⚠️ |
| 4.8 | UNLOGGED | 有标志和崩溃截断；缺少 PG unlogged init fork、复制限制、分区限制等 | ⚠️ |
| 4.9 | 视图 | 视图保存 SQL 并展开；缺少 rewrite rule 系统、security barrier/invoker、列权限、复杂 updatable view | ⚠️ |
| 4.10 | 物化视图 | backing table 简化；缺少 PG 依赖、索引要求、concurrent refresh 的可见性和锁 | ⚠️ |
| 4.11 | 触发器 | action SQL 字符串替换；缺少 trigger function 类型、transition tables、deferred/constraint triggers、event triggers、触发器队列 | ⚠️ |

---

## 5. 约束与完整性差距

> **已完成进展（2026-06-21）**：约束 DDL 已登记 `DEFERRABLE`/`INITIALLY DEFERRED`/`NOT VALID`/`VALIDATE` 元数据（见 5.2/5.3/5.4 原文）。Phase 4 Wave 3 进行中：`DdlExecutor::columnDefToColumn` 已解析 `GENERATED ALWAYS AS (expr) STORED` 与 `IDENTITY`；`ForeignKey` 增加 `deferrable`/`initiallyDeferred` 字段；`tests/constraints_test.cpp`（未提交）覆盖 GENERATED STORED 与 DEFERRABLE INITIALLY DEFERRED FK。5.6 上调为 🔄。延迟约束队列/提交时检查（5.10/SET CONSTRAINTS）仍 ⚠️。

| # | 约束能力 | 差距描述 | 状态 |
|---|---------|---------|------|
| 5.1 | `PRIMARY KEY` | 单列/复合有；缺少 deferrable、temporal constraints、partition/global uniqueness 完整语义 | ⚠️ |
| 5.2 | `UNIQUE` | 有单列/复合，并登记 `DEFERRABLE`、`INITIALLY DEFERRED`、`NOT VALID`、`VALIDATE` 元数据；NULL 语义、实际延迟检查、partial unique、expression unique 与 PG 不等价 | ⚠️ |
| 5.3 | `FOREIGN KEY` | 支持多列和部分 ON DELETE/UPDATE，并登记 `DEFERRABLE`、`INITIALLY DEFERRED`、`NOT VALID`、`VALIDATE` 元数据；缺少 `MATCH FULL/PARTIAL`、实际延迟检查、提交时 recheck、复杂锁与并发语义 | ⚠️ |
| 5.4 | `CHECK` | 表达式解析范围有限，并登记 `DEFERRABLE`、`INITIALLY DEFERRED`、`NOT VALID`、`VALIDATE` 元数据；缺少 `NO INHERIT`、实际延迟检查和 PostgreSQL 级表达式语义 | ⚠️ |
| 5.5 | `DEFAULT` | 缺少表达式默认值、稳定/易变函数、序列所有权精确行为 | ⚠️ |
| 5.6 | `GENERATED` | 有 identity/generated expr 痕迹；缺少 PostgreSQL 18 虚拟生成列默认语义、stored/virtual 完整实现 | 🔄 |
| 5.7 | Exclusion constraints | 支持 `ALTER TABLE ... ADD CONSTRAINT ... EXCLUDE USING ...` 的目录级登记和 drop/validate/options 元数据；缺少 GiST/operator class 执行检查、并发冲突检测和 executor enforcement | ⚠️ |
| 5.8 | Temporal constraints | 缺失。PostgreSQL 18 对 range 上的 temporal primary/unique/foreign key 支持缺失 | ❌ |
| 5.9 | Assertions | `CREATE/DROP ASSERTION` 已有目录级入口；PostgreSQL 本身未实现 SQL 标准 ASSERTION，本项目也缺少全库断言执行、依赖和重验证机制 | ⚠️ |
| 5.10 | `SET CONSTRAINTS` | 已记录会话 immediate/deferred 标志，约束 DDL 也登记 deferrable/not-valid/validated 元数据；仍无延迟约束队列、提交时检查和 constraint trigger 语义 | ⚠️ |

---

## 6. DML 与查询差距

| # | 领域 | 差距描述 | 状态 |
|---|------|---------|------|
| 6.1 | `SELECT` grammar | 缺少完整 SELECT 语法树；join、where、group、window、cte 多靠字符串定位，嵌套复杂查询容易偏离 PG | ⚠️ |
| 6.2 | Join | 支持 inner/left/right/full/cross 部分；缺少 SEMI/ANTI 内部语义、lateral 完整相关性、join reordering/search space、outer join predicate 推理 | ⚠️ |
| 6.3 | Set operations | UNION/INTERSECT/EXCEPT 有简化捕获输出；缺少类型合并、排序/limit 作用域、collation、ALL/DISTINCT 复杂语义 | ⚠️ |
| 6.4 | CTE | 有 WITH/RECURSIVE/DML CTE 痕迹；缺少 MATERIALIZED/NOT MATERIALIZED、可写 CTE 快照语义、递归检测、cycle/search 子句 | ⚠️ |
| 6.5 | Subquery | 有 IN/EXISTS/ANY/ALL 展开；复杂关联子查询、row comparison、array ANY/ALL、NULL 语义不完整 | ⚠️ |
| 6.6 | `ORDER BY` | 有多列/expression/nulls/collate 简化；缺少 USING operator、位置编号全语义、collation provider | ⚠️ |
| 6.7 | `GROUP BY` | 支持 rollup/cube/grouping sets 部分；缺少 functionally dependent group by、GROUPING_ID 等完整语义 | ⚠️ |
| 6.8 | `LIMIT/FETCH` | FETCH 被转 LIMIT；缺少 `WITH TIES`、百分比/复杂表达式等 | ⚠️ |
| 6.9 | Row locking | 单表 `FOR UPDATE/SHARE` 有部分；源码明确 `FOR UPDATE not supported with JOIN/GROUP BY/aggregate/window/scalar functions`。缺少 `NO KEY UPDATE`、`KEY SHARE`、OF list 完整语义 | ⚠️ |
| 6.10 | `INSERT` | 多行/insert-select/upsert 有；缺少 PG 的 `DEFAULT VALUES`、`OVERRIDING`、WITH query 全组合、conflict target/opclass/where | ⚠️ |
| 6.11 | `UPDATE FROM` / `DELETE USING` | 通过执行 JOIN 并解析文本输出实现；对空格、列顺序、别名、重复匹配、并发语义风险高 | ⚠️ |
| 6.12 | `RETURNING` | 有简单列返回；缺少 PG 18 `OLD`/`NEW` aliases、任意表达式、trigger-modified rows 的精确行为 | ⚠️ |
| 6.13 | `MERGE` | 只支持小子集；缺少完整 WHEN 分支和并发/可见性语义 | ⚠️ |

---

## 7. 查询优化器和执行器差距

> **已完成进展（2026-06-21）**：解析/分析层已分层（Parser → AST，`src/parser/`），`execute()` 不再是纯字符串分发器，DDL 经 `DdlExecutor` 驱动；7.1 由 ❌ 上调为 🔄。但**优化器框架尚未建立**：`src/optimizer/` 目录为空，无 `Path`/`RelOptInfo`/`PlannerInfo` 框架（Phase 5 未启动），故 7.2~7.4 仍为 ⚠️/❌，7.5/7.6/7.7（并行/JIT/AIO）仍 ❌。

| # | PostgreSQL 能力 | 差距描述 | 状态 |
|---|----------------|---------|------|
| 7.1 | Parser/analyzer/rewrite/planner/executor 分层 | 主要在 `execute()` 中字符串解析并直接调用 engine | 🔄 |
| 7.2 | Cost-based planner | 有简化成本、统计和 plan cache；缺少 path 枚举、参数化路径、join search、equivalence classes、pathkeys、parallel aware path | ⚠️ |
| 7.3 | 统计信息 | 有行数、cardinality、min/max、histogram/MCV、多列简化和扩展统计对象元数据；缺少 PostgreSQL 级 dependencies、ndistinct、correlation、表达式统计、catalog 和 planner 深度使用 | ⚠️ |
| 7.4 | Index selection | 有 equality/range 部分；缺少 bitmap heap scan、bitmap and/or、多索引组合、skip scan、index condition recheck、lossy pages | ⚠️ |
| 7.5 | Parallel query | 缺失：无 Gather/Gather Merge、parallel scan/join/aggregate、worker lifecycle | ❌ |
| 7.6 | JIT | 缺失 LLVM JIT | ❌ |
| 7.7 | Async I/O | 缺失 PostgreSQL 18 AIO 子系统 | ❌ |
| 7.8 | Plan invalidation | 缺少基于 catalog/dependency 的 plan invalidation | ❌ |
| 7.9 | EXPLAIN ANALYZE | 简化输出，不是真正节点级执行时间/rows/buffers/WAL/IO | ⚠️ |

---

## 8. 索引差距

> **已完成进展（2026-06-21）**：Phase 0 已统一 `IIndexAM` 接口并提供 `BPTreeIndexAM`/`HashIndexAM` 适配器（`src/access/`）。8.1 由 ❌ 上调为 🔄（有接口层与适配器，但缺 `amhandler`/support functions/opclass/opfamily/amcostestimate/amvalidate 的 PG 完整 AM API）。其余索引能力仍在 Phase 6 待办。

| # | 领域 | 差距描述 | 状态 |
|---|------|---------|------|
| 8.1 | Access Method API | 缺少 `amhandler`、support functions、opclass/opfamily、amcostestimate、amvalidate | 🔄 |
| 8.2 | B-tree | 缺少 dedup、suffix truncation、visibility map 驱动 index-only、skip scan 完整实现、NULLS FIRST/LAST 存储控制、collation/operator class | ⚠️ |
| 8.3 | Hash | 简化 hash index；缺少 WAL-safe hash bucket split、metapage/overflow page 机制 | ⚠️ |
| 8.4 | GIN/GiST/BRIN/SP-GiST | 多为特定数据/范围简化结构；缺少 PostgreSQL 泛化 opclass、consistent/union/picksplit/penalty 等方法 | ⚠️ |
| 8.5 | Concurrent index | `concurrently` 多为标志或 sleep/yield 简化；缺少 PG 两阶段/三事务/invalid index catalog 状态/等待旧快照 | ⚠️ |
| 8.6 | Expression/partial/include | 有 metadata；表达式和 predicate 支持范围远小于 PG，缺少 expression dependency、immutable 检查、predicate implication | ⚠️ |
| 8.7 | Index maintenance | 缺少 page deletion、vacuum cleanup、amcheck、`REINDEX CONCURRENTLY`、pg_stat index 统计 | ⚠️ |
| 8.8 | Partitioned index | 缺少 PG 分区索引 attach/detach 和唯一约束跨分区规则完整语义 | ⚠️ |

---

## 9. 事务、MVCC 与并发差距

> **已完成进展（2026-06-21）**：Phase 3.7~3.9 已落地 PostgreSQL 风格 `HeapTupleHeader`（`t_xmin/t_xmax/t_cid/t_ctid/t_infomask/t_infomask2/t_hoff`、null bitmap、hint bits、ctid 自引用链、HOT update 经 `LP_REDIRECT`）；`CLOG`/`pg_xact`（2-bit 事务状态、按段持久化，commit/rollback 自动更新）；`Snapshot` 导出/导入（`exportToBytes/importFromBytes`）+ `subxip` 可见性 + catalog snapshot 惰性缓存。9.1/9.2/9.8 上调为 🔄（9.8：WAL 已是 redo log，LSN/segment/full-page/redo/timeline/archive + 两趟扫描恢复）。9.4 SSI 仍 ❌（Phase 5.43）。9.6 DDL 事务化仍 🔄（Wave 0.4 骨架：`DdlTransaction` + `XLOG_CATALOG_*` WAL 记录，Wave 5 全量移除隐式提交未做）。9.7 已修复嵌套锁死锁（Phase 2.6），轻量锁/spinlock/wait events 仍 ❌。9.9 `VisibilityMap`(AllVisible) + hint bits 已实现（Phase 3.8），freeze map/all-frozen/wraparound/autovacuum 仍 ❌（Phase 5.32）。

| # | 领域 | 差距描述 | 状态 |
|---|------|---------|------|
| 9.1 | Tuple versioning | 项目行头只有 creator txid/rollback ptr 常量，实际可见性主要看创建 txid；缺少 `xmin/xmax`、ctid chain、多版本更新、HOT update | 🔄 |
| 9.2 | Snapshot | 有 ReadView；缺少 PG snapshot export/import、subxip、catalog snapshot、logical decoding snapshot | 🔄 |
| 9.3 | Isolation levels | 四级隔离名存在；Read Uncommitted 在 PG 中实际等同 Read Committed，本项目语义未必一致；Serializable 只用 RID read/write set 简化 SSI | ⚠️ |
| 9.4 | SSI/predicate locks | 缺少 PostgreSQL predicate lock、SIREAD lock、rw-conflict in/out 复杂规则和索引范围推理 | ❌ |
| 9.5 | Savepoint/subtransaction | Savepoint 基于 txn log index；缺少子事务 ID、资源释放、错误状态恢复 | ⚠️ |
| 9.6 | DDL transactions | 本项目 DDL 多处隐式提交；PG 大多数 DDL 可回滚 | 🔄 |
| 9.7 | Lock manager | 有表/行/gap/page/advisory lock 简化；缺少 PG 重量级锁、轻量锁、spinlock、lock modes 全矩阵、deadlock detector 精细语义、wait events | ⚠️ |
| 9.8 | Crash safety | WAL 不是 redo log；事务开始复制目录备份，提交清 WAL。对大数据库、并发事务、部分页写、崩溃窗口的语义与 PG 差距巨大 | 🔄 |
| 9.9 | Vacuum/freeze | 缺少 transaction wraparound、freeze map、visibility map、hint bits、all-visible/all-frozen | ⚠️ |

---

## 10. 存储、WAL、恢复差距

> **已完成进展（2026-06-21）**：Phase 3 全部 14 子任务（3.1~3.14）已关闭。`ClusterLayout`（base/global/pg_wal/pg_xact/pg_tblspc 等 20+ 子目录 + 关系 forks main/fsm/vm/init + tablespace 符号链接）；`PgPage`/`PageWrapper` 统一 4KB/8KB 页接口（formatVersion≥2 默认 8KB + line pointer + tuple header）；`BufferPool`（clock sweep + pin/usage count + dirty flush）；`FreeSpaceMap`/`VisibilityMap`（fork 文件，集成到 insert/update/delete/vacuum）；WAL（LSN/segment/full-page/redo/timeline/archive_status，见 §9）；checkpoint（redo pointer + checkpoint record + `archivePendingSegments`）；TOAST（真实 chunked 关系 + B+tree 索引，2KB 分块）；data page checksums + 测试；storage parameters（`fillfactor` 执行 + `.params` 合并）。10.1/10.2/10.4/10.5/10.9 由 ❌ 上调为 ✅；10.3/10.6/10.8/10.10/10.11 上调为 🔄。10.7 PITR 仍 ❌（Phase 8）。

| # | PostgreSQL 存储能力 | 差距描述 | 状态 |
|---|--------------------|---------|------|
| 10.1 | Cluster layout | PG 有 base/global/pg_wal/pg_xact/pg_multixact 等；本项目以当前目录数据库子目录和表文件为主 | ✅ |
| 10.2 | Relation forks | 缺少 main/fsm/vm/init forks | ✅ |
| 10.3 | Page format | 有 4096 slotted page；PG 默认 8KB page，含 line pointer、tuple header、visibility 等复杂结构 | 🔄 |
| 10.4 | Buffer manager | 有 LRU buffer pool；缺少 shared buffers、clock sweep、pin/lock/contention、bgwriter、checkpointer、walwriter | ✅ |
| 10.5 | WAL | 缺少 record type、LSN、WAL segment、full page writes、redo routines、timeline、archive status、replication WAL sender | 🔄 |
| 10.6 | Checkpoint | 简化刷盘/清 WAL；缺少 redo pointer、checkpoint record、restartpoint、checkpoint throttling | 🔄 |
| 10.7 | PITR | 缺失 | ❌ |
| 10.8 | TOAST | 有大值外置文件；缺少 PG TOAST relation/index、compression、chunking、out-of-line pointer 语义 | 🔄 |
| 10.9 | Tablespace | 缺失 | ✅ |
| 10.10 | Checksums | 有页 checksum 痕迹；缺少 cluster-level data checksum、initdb/pg_verify_checksums | 🔄 |
| 10.11 | Storage parameters | 有少量 `.params`；缺少 fillfactor/autovacuum/toast/parallel/cost 等完整 reloptions | 🔄 |

---

## 11. 安全、认证、权限差距

> **已完成进展（2026-06-21）**：Phase 2.7 已用 `pg_authid`/`pg_auth_members` 替代 `user.dat`/`role.dat`（CRUD + 成员关系 + 级联删除 + CSV 持久化），11.1 差距收窄但仍 ⚠️（属性执行/membership options/password expiration 未做）。TLS 有 OpenSSL wrapper + stub（11.4 ⚠️）。pg_hba/SCRAM/wire protocol（11.2/11.3）仍 ❌，待 Phase 7。

| # | 领域 | 差距描述 | 状态 |
|---|------|---------|------|
| 11.1 | 用户/角色 catalog | `user.dat` / `role.dat` / `.pg_role_attrs` 文件，非 `pg_authid`/`pg_auth_members`；角色属性目前主要可记录和审计，缺少 OID、属性执行语义、password expiration、membership options | ⚠️ |
| 11.2 | 认证 | 支持 sha256/md5 哈希和 TLS 可选；缺少 `pg_hba.conf`、SCRAM-SHA-256、OAuth(PG18)、LDAP、Kerberos/GSSAPI、SSPI、RADIUS、PAM、cert、peer、ident | ❌ |
| 11.3 | 传输协议 | 不是 PostgreSQL wire protocol/libpq；客户端仅文本登录/SQL 行 | ❌ |
| 11.4 | TLS | 有 OpenSSL wrapper 和 stub；缺少 PG SSL negotiation、client cert auth、channel binding | ⚠️ |
| 11.5 | ACL | 简化 privilege 文件；缺少 ACL item、PUBLIC、grant options/admin options/set options、ownership、default privileges 完整传播 | ⚠️ |
| 11.6 | RLS | 有 policy 文件和透明条件追加；源码注释显示 `WITH CHECK` 复杂验证被简化允许，缺少 PG executor-integrated RLS | ⚠️ |
| 11.7 | SECURITY DEFINER/INVOKER | 函数/过程缺少完整 security definer/invoker、search_path 安全规则 | ❌ |
| 11.8 | 审计 | 项目有 audit log；PG 核心不内置同等 audit，通常靠扩展 | — |

---

## 12. 复制、高可用、备份恢复差距

> **已完成进展（2026-06-21）**：`src/replication/` 仅有 README，流复制/逻辑复制/PITR/复制槽/发布订阅全 ❌（Phase 8）。唯一有进展的是 12.8：WAL 已支持 timeline + `archive_status`(.ready/.done) + `archivePendingSegments()`（Phase 3.6），但连续归档/recovery.signal/PITR 仍 ❌。

| # | PostgreSQL 能力 | 差距描述 | 状态 |
|---|----------------|---------|------|
| 12.1 | Physical streaming replication | 缺失 | ❌ |
| 12.2 | Standby/hot standby/read-only replica | 缺失 | ❌ |
| 12.3 | Replication slots | 缺失 | ❌ |
| 12.4 | Synchronous replication | 缺失 | ❌ |
| 12.5 | Cascading replication | 缺失 | ❌ |
| 12.6 | Logical decoding | 缺失 | ❌ |
| 12.7 | Publication/subscription | 缺失 | ❌ |
| 12.8 | WAL shipping/archive recovery | 有 WAL archive 目录函数，但无连续归档/PITR/timeline/recovery.signal | ⚠️ |
| 12.9 | pg_basebackup | 缺失 | ❌ |
| 12.10 | Incremental backup / pg_combinebackup | 缺失 | ❌ |
| 12.11 | SQL dump/restore | 有 `DUMP DATABASE` / `RESTORE DATABASE` 简化；不是 `pg_dump/pg_restore` 格式和对象依赖顺序 | ⚠️ |
| 12.12 | Failover/promote | 缺失 | ❌ |

---

## 13. 系统目录、信息模式和监控差距

> **已完成进展（2026-06-21）**：13.1 由 ❌ 上调为 🔄——核心系统表（pg_class/attribute/type/proc/depend/namespace/authid/auth_members/description）已建为真实对象图（Phase 2），虚拟表 pg_database/pg_tables/pg_indexes/pg_roles/pg_namespace 已有；但距 PG 几百个 view/function 仍有量级差距，故保留 🔄。其余（information_schema 完整 views、pg_stat_io/wait events、logging collector、多进程、psql/工具链）仍 ❌/⚠️。

| # | 领域 | 差距描述 | 状态 |
|---|------|---------|------|
| 13.1 | `pg_catalog` | 只实现了若干虚拟表/兼容查询；缺少几百个 catalog/view/function | 🔄 |
| 13.2 | `information_schema` | 只有子集；缺少 SQL 标准完整 views、权限过滤 | ⚠️ |
| 13.3 | `pg_stat_*` | 有 pg_stat_statements/pg_stat_activity/pg_locks/pg_buffercache 风格子集；缺少 pg_stat_io、progress views、replication views、wait events、backend memory contexts | ⚠️ |
| 13.4 | 日志 | 有 slow log/auto_explain/audit；缺少 PG logging collector、CSV/JSON logs、log_line_prefix、server log GUC 全集 | ❌ |
| 13.5 | 进程模型 | 项目多线程 server；PG 是多进程 backend + shared memory 架构 | ❌ |
| 13.6 | 工具链 | 缺少 `psql` 元命令、libpq、pg_dump、pg_restore、pg_upgrade、initdb、createdb/dropdb、pg_ctl、pgbench | ❌ |

---

## 14. 扩展性与生态差距

| # | PostgreSQL 扩展点 | 差距描述 | 状态 |
|---|------------------|---------|------|
| 14.1 | `CREATE EXTENSION` | 缺失 | ❌ |
| 14.2 | C extension / fmgr | 缺失 | ❌ |
| 14.3 | Procedural languages | 缺失 PL/pgSQL、PL/Python、PL/Perl 等 | ❌ |
| 14.4 | FDW | 缺失 | ❌ |
| 14.5 | Custom type I/O | 缺失底层 base type 创建 | ❌ |
| 14.6 | Custom operators/opclass | 缺失 | ❌ |
| 14.7 | Logical decoding plugins | 缺失 | ❌ |
| 14.8 | Background workers | 缺失 | ❌ |
| 14.9 | Planner/executor hooks | 缺失 | ❌ |
| 14.10 | Shared memory extensions | 缺失 | ❌ |

---

## 15. 非 PostgreSQL 特性和语法偏移

以下能力项目支持但不是 PostgreSQL 兼容能力，**如需 PostgreSQL 兼容，需考虑移除或提供兼容模式**：

| # | 项目特性 | PostgreSQL 情况 |
|---|---------|----------------|
| 15.1 | `USE DATABASE db` | PG 连接后不能用 SQL 切换数据库 |
| 15.2 | `REPLACE INTO` | MySQL 语法，PG 不支持 |
| 15.3 | `LOAD DATA INFILE` | MySQL 风格，PG 使用 `COPY`/\`copy` |
| 15.4 | `SELECT ... INTO OUTFILE` | MySQL 风格；PG `SELECT INTO` 是建表，导出用 `COPY TO` |
| 15.5 | `DESC` / `VIEW TABLE` / `VIEW DATABASE` | MySQL/项目命令；PG 通常用 psql 元命令或 catalog 查询 |
| 15.6 | `UPDATE/DELETE ... LIMIT` | MySQL 风格；PG 通常用 CTE/subquery |
| 15.7 | `AUTO_INCREMENT` | MySQL 风格；PG 用 serial/identity |
| 15.8 | `DATETIME`, `TINYINT`, `BLOB`, `NCHAR/NVARCHAR` | 非 PG 原生或语义不同 |
| 15.9 | `SET GLOBAL` | MySQL 风格；PG 用 ALTER SYSTEM/GUC reload |

---

## 16. 架构级根本差距（系统级，非单点功能）

以下差距不是单个功能点，而是**系统级架构差异**，需要整体重构，无法通过补丁修复。**状态**列反映 2026-06-21 核对结果——其中 16.1/16.2/16.3/16.4/16.9 的框架已落地（Phase 1~3），但"完整对齐 PG"仍需持续深化。

| # | 差距 | 影响范围 | 难度 | 状态 |
|---|------|---------|------|------|
| 16.1 | **Parser/AST** — 当前 `execute()` 是巨大字符串分发器，不是完整 SQL grammar 管线 | SQL 解析、类型推断、函数重载、语法错误信息 | 极高 | 🔄 框架已建：`src/parser/` 递归下降 Parser + AST + `classify()`，DDL 子集经 `DdlExecutor`；但 DML 全部仍走字符串分发，AST 未被执行器消费 |
| 16.2 | **Catalog/OID** — 没有完整 `pg_class`、`pg_attribute`、`pg_type`、`pg_proc`、`pg_depend`、`pg_namespace` 等对象图 | DDL、权限、依赖、对象寻址 | 极高 | 🔄 模块+测试已写（系统表对象图/OID/依赖 CASCADE/RESTRICT），但 **CatalogManager 未在 main.cpp 构造、DROP 未接 planDrop、系统表不可查**，运行时仍用文件+`schema__table` |
| 16.3 | **WAL redo** — 当前 WAL 不是 redo log，缺少 LSN、segment、full page writes、redo routines | 崩溃恢复、PITR、复制 | 极高 | ✅ 已是 redo log：LSN/segment/full-page/redo/timeline/archive + 两趟扫描崩溃恢复（Phase 3.4~3.6）；PITR/复制仍 ❌ |
| 16.4 | **MVCC 版本链** — 只有 creator txid，缺少 `xmin/xmax`、ctid chain、HOT update | 并发控制、VACUUM、存储格式 | 极高 | 🔄 `HeapTupleHeader`(xmin/xmax/ctid) + HOT + CLOG 可见性已实现（Phase 3.7/3.8）；多版本边界/vacuum 回收仍需深化 |
| 16.5 | **DDL 事务化** — 多处 DDL 隐式提交，与 PG 事务语义不一致 | 数据一致性、回滚、并发 | 高 | 🔄 Wave 0.4 骨架（`DdlTransaction` + `XLOG_CATALOG_*` WAL）；全量移除隐式提交待 Phase 4.39 Wave 5 |
| 16.6 | **PostgreSQL Wire Protocol** — 不是 PG 协议，客户端仅文本登录 | libpq 兼容性、生态工具 | 高 | ❌ Phase 7 待办 |
| 16.7 | **扩展系统** — 没有插件加载框架、Hook 系统、共享内存扩展 | EXTENSION、FDW、PL、自定义类型 | 极高 | ❌ Phase 10 待办 |
| 16.8 | **多进程模型** — 项目是多线程 server，PG 是多进程 backend + shared memory | 连接隔离、崩溃恢复、共享内存 | 高 | ❌ Phase 9 待办 |
| 16.9 | **Buffer Manager** — 缺少 shared buffers、clock sweep、pin/lock、bgwriter、walwriter | I/O 性能、并发、恢复 | 高 | ✅ `BufferPool`(clock sweep + pin/usage) + bgwriter/checkpointer/walwriter 后台线程已实现（Phase 3.3/3.4） |
| 16.10 | **Cost-based Planner 框架** — 缺少 path/relation/statistics 框架 | 查询优化质量、并行查询、自适应优化 | 极高 | ❌ `src/optimizer/` 为空，Phase 5 未启动 |

---

## 17. 实施路线图（建议顺序）

若目标是"更像 PostgreSQL"，建议按以下依赖顺序推进。每条均需**原汁原味实现 PG 语义**，禁止折中。

> **进度（2026-06-21）**：Phase 0~3 已完成；Phase 4 进行中（Wave 0~2 已落地，Wave 3 约束进行中，Wave 4~6 待办）；Phase 5~10 未启动。详见 `docs/implementation-plan.md` 与 `docs/phase4-plan.md`。

### Phase 1：Parser 与 AST 🔄 框架完成，DML 未接入执行
- 引入真正 SQL parser 或至少分层 AST，替代 `execute()` 超大字符串分支 — **框架已完成**（`src/parser/`，~6400 行）
- 实现完整 operator precedence、类型解析、隐式 cast、函数重载、schema-qualified function — **解析层完成，执行期 cast/重载仍 ⚠️**
- 所有 SQL 命令通过 AST 表示，而非字符串解析 — **DDL 子集经 DdlExecutor 走 AST；DML（SELECT/INSERT/UPDATE/DELETE/MERGE）仍全部走字符串分发，AST 未被消费**

### Phase 2：Catalog 体系 🔄 模块+测试完成，未接入运行时
- 建立对象 OID、namespace、owner、ACL、dependency 体系 — **模块+测试完成，但 CatalogManager 未在 main.cpp 构造，运行时未生效；ACL/owner 待深化**
- 实现 `pg_class`、`pg_attribute`、`pg_type`、`pg_proc`、`pg_depend`、`pg_namespace` 等系统表 — **对象图已写并有测试，但不可查询（仅 pg_database/pg_tables 等硬编码虚拟表可查）**
- 所有 DROP/ALTER 行为改为基于 catalog 的依赖追踪 — **planDrop 框架已写，但无 DROP 命令处理器接入；ALTER rewrite 路径待补**

### Phase 3：事务/WAL/MVCC ✅ 完成
- 实现真实 WAL record/LSN/redo、tuple xmin/xmax、多版本链和崩溃恢复 — **已完成**
- 移除 DDL 隐式提交，实现对象依赖和 rollback — **DDL 事务骨架已建，全量移除隐式提交在 Phase 4.39**
- 实现 CLOG/pg_xact、visibility map、hint bits — **已完成**（freeze map 仍待 Phase 5.32）

### Phase 4：类型系统与函数 🔄 进行中（Wave 0~2 完成）
- 完善 cast、operator、function overloading、collation、NULL 表示 — **TypeRegistry + ExprEvaluator 已建，逐类型 I/O 待 Wave 1**
- 实现完整的表达式求值框架 — **ExprEvaluator 已建（Wave 0.2）**
- 补齐 JSON/XML/Array/Range 全函数 — **内置函数子集已加（Wave 2），全集待补**

### Phase 5：Planner 与执行器 ❌ 未启动
- 建立 path/relation/statistics 框架 — **未启动**（`src/optimizer/` 为空）
- 补 bitmap/parallel/partitionwise/skip scan — **未启动**
- 实现 EXPLAIN ANALYZE 真实节点级统计 — **未启动**

### Phase 6：协议与认证 ❌ 未启动
- 支持 PostgreSQL wire protocol、libpq — **未启动**
- 实现 pg_hba.conf、SCRAM-SHA-256 — **未启动**

### Phase 7：复制/PITR ❌ 未启动
- 在 WAL 稳定后实现 streaming replication、logical decoding、PITR — **未启动**
- 实现复制槽、发布订阅、级联复制 — **未启动**

### Phase 8：扩展/FDW/PL ❌ 未启动
- 实现扩展加载框架、过程语言和外部表生态 — **未启动**
- 实现 Hook 系统、后台工作进程、共享内存扩展 — **未启动**

---

*最后更新：2026-06-21*
*关联文档：*
- [postgresql-complete-gap-analysis.md](postgresql-complete-gap-analysis.md) — 最详细的差距分析原文（不可删除）
- [implementation-plan.md](implementation-plan.md) — 按 phase 排列的实施计划与"已完成内容"记录（Phase 0~3 已完成，Phase 4 进行中）
- [phase4-plan.md](phase4-plan.md) — Phase 4 逐子任务展开与 Wave 0~6 进度
- [test-report.md](test-report.md) — 功能测试报告
- [commandsList.md](commandsList.md) — 支持的 SQL 命令列表

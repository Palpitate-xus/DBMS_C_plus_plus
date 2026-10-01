# DBMS_C_plus_plus 对标 PostgreSQL 18.6：完整差距清单

> 审计日期：2026-08-23
> 项目基线：`5a31aa3` 加审计时的动态工作区（`src/main.cpp`、`src/parser/parser.cpp`、`src/commands/DmlExecutor.{h,cpp}` 有尚未提交的修改）
> 对标基线：PostgreSQL 18.6
> 文档定位：当前差距的权威清单；每项的代码落点、I/O 保真、性能和验收方案见 [postgresql-18-implementation-blueprint.md](postgresql-18-implementation-blueprint.md)；旧的 `all-gaps-todo.md`、`feature-gaps.md` 和 `postgresql-comparison.md` 保留作历史记录。

> 2026-09-08 续做：总清单尚未完成。最新逐项状态见 [gap-progress.json](gap-progress.json)，执行顺序见 [总清单执行计划](full-gap-execution-plan.md)。原审计条目须按当前代码重新核实；局部 bug 修复不等于整个功能族完成。

2026-10-01 第921项 source `a858f437`，D合并 `0c9908bb`：已有query后同级SET TRANSACTION／重复BEGIN isolation被误拒25001，不同level在没有query的USER-SP内反而误接受；旧完整19强wire、actual failed=1及全同source／header真实production对象重链专项native断言134保留。setter对live transaction相同enum提前成功且不清snapshotAcquired；真正修改同时检查USER-SP与已有读／写／DDL／snapshot状态，内部statement marker不误算user subtransaction。真实PG18.6与本项目最终四种isolation×Simple／Extended、same-level after-query／USER-SP、different-level before-query child拒绝、RELEASE后Top允许修改、双连接REPEATABLE READ在SET／BEGIN后仍看旧snapshot及结束后见新行，全部exit0。Header inline实现变化，无新增字段或disk改变，但本worktree全source production真实自有统一headers冷编译／link exit0；11 C++、专项、12wire邻居／完整协议及9不同actual亦全部exit0，无旧ABIs或假cache。组合应434 C++／164E2E／433actual。冻结根／A2d2db1d0本轮真正全闭合：429 C++／158E2E脚本exit0、根同源正式427 PG18.6差分cases=427 failed=0／427不同[OK]／exit0；根已保留 `build/dbms_bigint_minimum_signed_dml_formal_2d2db1d0`。之后方可快进并冻结新同源组合，再跑其真正全套和433全差分，旧427不能替代新source验证。922已read-only wire核实BEGIN READ COMMITTED、缺LEVEL等非法PG语法被接受，而合法comma transaction modes尚未支持／syntax误XX000；独立grammar核实继续，不在本项完成范围。完整snapshot／command-counter／SSI／GUC／lexer及907temp SERIAL等其余总清单仍未完成。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user，不push，Actions仅本地disabled workflow，不声明远端已收到未push的更改。

2026-10-01 第920项 source `3e3e144e`，D合并 `071bbfea`：保存点内SET READ ONLY后，ROLLBACK TO及RELEASE没有恢复parent mode；旧完整18强wire被错误25006拒绝write、actual failed=1，另外child READ ONLY可非法改READ WRITE，真实旧差分亦保留。SavepointState保存入栈readOnly，在成功rollback／release named subtree恢复对应parent值；READ ONLY向READ WRITE切换时USER-SP guard返回失败，内部statement marker不冒充用户block。真实PG18.6及本项目最终Simple／Extended、正常和failed ROLLBACK TO、failed COMMIT CHAIN、subcommit、nested／duplicate-SP parent差异、RO child禁止放宽与正常Top控制全exit0。初始RELEASE仍保持child mode的错误PG预期failed=1保留，源码xact.c的prevXactReadOnly明确在subcommit／subabort均恢复后，按真实oracle修正夹具，未把误假设当完成。新增private in-memory字段，因此本worktree所有source统一自有headers真实冷production build exit0，11 C++、专项、11wire邻居／完整协议及9不同actual全部exit0，heap／WAL故障native所报rollback incomplete为原断言覆盖的预期诊断，不冒充失败或全库sanitizer。无磁盘格式变化。组合应433 C++／163E2E／432actual；根/A仍冻结2d正式429／158已exit0，427同源差分仍运行，已输出部分[OK]但未声明整轮成功、不混新source。921已真实PG核实相同isolation level在snapshot后和USER-SP内合法且必须保留原read view，不同level在USER-SP中即使未query也须25001；旧完整19 strong wire／actual failed=1及其全同source／header真实production对象重链的新native assert134保留。921隔离修改setter早返回不清snapshot并约束USER-SP，正基于920新layout统一自有全重编；其PG同一oracle含四种level、Simple／Extended、双连接repeatable-read可见性控制已exit0，新生产尚待验证，不声明完成。完整isolation／GUC／resource owner／SubXID／readonly族、907temp SERIAL及其余总清单未完成。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user，不push，Actions禁用。

2026-10-01 第919项 source `35d4c7c4`，D合并 `1f440223`：重复BEGIN没有显式mode却清READ ONLY，且AdvisoryLockManager::beginTransaction释放旧transaction locks并重置USER-SP边界；真实旧13 mode误成功INSERT及旧完整18双连接try-lock=t／exit1保留。AST记录isolation／read-only／deferrable是否显式指定，BEGIN／START toString保留options；live block只应用显式mode且检查SET TRANSACTION setter结果，不重做engine begin、advisory／notification资源初始化和CHAIN origin。warning25001从完整有效AST＋显式block状态结构化产生，先于显式选项25001错误，不由stdout推断；Extended implicit block被用户BEGIN提升时不误发warning。START TRANSACTION tag按真实PG独立输出，不再冒充BEGIN。最终真实PG强oracle／本项目覆盖Simple／Extended、双连接pre／post-SP锁与ROLLBACK TO、无显式选项模式、显式模式生效、snapshot后修改错误、warning code及隐式提升全部exit0；11 C++、专项、10wire邻居／完整协议及9不同actual也全部exit0。AST新增字段，本worktree所有生产source真实自有统一headers冷编译／link exit0，无旧ABI借用、无disk改变。最初PG夹具错误假定重复BEGIN显式选项也忽略，随后tag错误均保留为夹具失败，修正为PG实际规则后完整强oracle通过，未把夹具错误当产品成功。组合应432 C++／162E2E／431actual；根/A仍冻结2d正式429／158已exit0，427同源差分仍运行，不混新source。920已真实PG核实read-only在subabort和subcommit均恢复prevXactReadOnly及RO child禁止READ WRITE；旧18 wire和actual failed=1保留。920保存／恢复字段与setter USER-SP guard、嵌套native／wire测试已隔离实现，新增SavepointState字段需全自有统一header重编，正式build进行中，未声明完成；初始RELEASE应保持child mode的错误oracle预期保留并按真实PG修正。完整GUC／subtransaction／read-only／SQL／协议、907temp SERIAL及其余总清单未完成。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user，不push，Actions禁用。

2026-10-01 第918项 source `8df2e3e0`，D合并 `48c737d4`：真实PG18.6确认CHAIN-origin top-level abort恢复链开始时的READ ONLY baseline，而普通BEGIN-origin abort仍恢复默认read-write；正常ending继承当前SET后的mode，failed user-SP仍保留live parent mode。新Session分别记录origin与baseline，ending在engine已物理abort但logical E时仅取CHAIN baseline，新的普通BEGIN与plain／NO CHAIN结束清理；不把后来SET的mode当abort baseline。最终强Simple／Extended×COMMIT／ROLLBACK、SET READ WRITE／READ ONLY两方向、成功ending、plain重置和普通BEGIN控制oracle／本项目均exit0；原915及完整913真实production误成功INSERT／exit1保留。Session新增字段，因此918 worktree真实全source production冷构建（main／Table／Network／parser等全为自有统一headers）exit0；10 C++、专项、9wire邻居／完整协议及9不同actual全部exit0，无ABI旧对象混用、无disk格式改变。组合应431 C++／161E2E／430actual，完整read-only／default GUC／isolation／DEFERRABLE族仍partial。冻结根／A2d2db1d0正式全套429 C++／158E2E已真正exit0（`/tmp/dbms-tests-bigint-minimum-signed-dml-915-916.log`），根同源427全差分正在运行（`/tmp/dbms-pgdiff-bigint-minimum-signed-dml-915-916.log`），尚未声明其成功、不混入917／913／918。919重复BEGIN无显式read mode却清掉READ ONLY、重置transaction advisory ownership已按PG建立独立强schedule；PG实际会应用显式BEGIN选项而非全部忽略，保留最初错误fixture预期失败并校正为真正规则，另START TRANSACTION tag须独立，不冒充BEGIN。919修复及AST option-presence新增layout全自有重编进行中。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user；907temp SERIAL及其余总清单仍未完成，不push，Actions禁用。

2026-10-01 第913项 source `8e985f7b`，D合并 `aba1ab67`：没有显式block的AND CHAIN及Extended Execute／Flush隐式block均须25P01；正常和logical failed block分别保留合法CHAIN语义。AST解析完整chain／no-chain选项、WORK／TRANSACTION／END／ABORT及注释分割，ending handler拒绝非ending AST；ROLLBACK WORK／TRANSACTION TO用结构性token分派到USER savepoint，Simple／Extended失败保存点恢复后保留之前write和T状态，不再误作整段rollback。failed COMMIT／END改写前解析完整命令，非法suffix保持42601／E／25P02，不丢尾部误启动新chain。旧915 idle CHAIN误成功及13中间版failed-invalid-ending误成功真实exit1保留，真实PG18.6最终同一强oracle全部exit0。AST／Session layout有新增字段，本worktree独立真实全source production构建后按实际修改main／parser／Network增量builder重编，最终统一自有headers／对象，无借用旧ABI缓存；最终10 C++、专项＋8wire邻居／完整协议及9不同actual全部exit0，原失败未放宽。应431 C++／160E2E／429actual；根／A仍冻结2d2db1d0，根正式production exit0，A429 C++已通过、158E2E继续，427全差分尚未启动，未混入917／913。918 CHAIN-origin top-abort READ ONLY baseline已真实PG核实Simple／Extended及SET两方向、正常结束、plain结束重置；旧13仍误接受INSERT，918隔离修复与全自有重编进行中，未宣告完成。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user；完整事务／协议、907temp SERIAL及其余总清单仍未完成，不push，Actions禁用。

## 1. 结论

这个项目已经不是“玩具 SQL 解析器”：它有约 9.2 万行 C/C++ 核心代码、8 KiB 页式堆、Buffer Pool、FSM/VM、TOAST、WAL、CLOG、MVCC、锁管理器、B+Tree/Hash/GIN/BRIN/GiST/SP-GiST/Bloom 风格索引、Volcano 执行器、PostgreSQL v3 协议子集、SCRAM、PITR 子集，以及 158 个 C++ 测试文件和 7 个 Python 协议/E2E 测试文件。当前源码可成功编译。

但它目前更适合定位为：**功能面很宽、实现深度不均的单机关系数据库原型/教学型内核**，还不能定位为 PostgreSQL 的兼容替代品，更不能承诺 PostgreSQL 级生产可靠性。

真正的差距不是再补几十条 SQL，而是以下系统性问题：

1. SQL 仍存在 typed AST/Volcano 与 `main.cpp` 字符串执行器两套路径，复杂查询和 DML 会回退 legacy 逻辑。
2. catalog 不是所有对象与执行语义的唯一事实源；大量能力由 schema 文件、sidecar 文件、兼容对象文件和虚拟系统表拼接。
3. 很多 `CREATE/ALTER/DROP` 只是在 `.pg_compat_objects` 中保存定义，执行时并没有 PostgreSQL 对应能力。
4. DDL 会在若干路径隐式提交，和 PostgreSQL 的事务化 DDL 不兼容。
5. SERIALIZABLE 只是 SSI 子集；索引范围 predicate lock、完整 rw-conflict 图和 safe snapshot 不完整。
6. 除 B+Tree/Hash 的部分路径外，索引结构、并发算法、WAL、vacuum/opclass 语义远未达到 PostgreSQL 访问方法水平。
7. 没有物理流复制、hot standby、同步复制、真正 WAL 驱动且 wire-compatible 的逻辑复制。
8. 缺少 PostgreSQL 扩展体系、FDW 运行时、fmgr、hooks、动态后台 worker 和工具生态。
9. 协议、catalog、错误码和工具链不足以让普通 PostgreSQL 客户端/ORM/运维工具无差别工作。
10. 没有跨版本升级承诺和存储格式兼容路径；当前文档明确要求旧格式导出后重建。

因此，不建议给出一个“完成了 PostgreSQL 的百分之多少”的数字。按语法关键词计数会严重高估成熟度：AST 中出现命令名、能够保存一条兼容对象记录、能够通过单线程 happy-path 测试，都不等于实现了 PostgreSQL 语义。

## 2. 审计口径和证据

### 2.1 状态定义

| 状态 | 含义 |
|---|---|
| `P` | 有真实执行路径，但仅为 PostgreSQL 语义子集；仍是差距项 |
| `S` | 只有解析、分类、定义保存或兼容对象骨架；不能视为对应功能已实现 |
| `X` | 当前生产入口没有可用实现 |

本清单中的“完整”是指：覆盖 PostgreSQL 18 的全部 183 条 SQL 命令，并覆盖类型、表达式、catalog、优化器、事务、存储、复制、协议、安全、运维和扩展等能力域。PostgreSQL 内置函数、GUC、catalog 列和错误码有数千项，本文件按完整的**功能族**列出，不逐个复制官方手册中的每个函数重载或每一列定义。

### 2.2 本次实际检查

- 审阅了 `README.md`、发布说明、构建脚本、现有差距文档、`src/` 全部模块目录和测试清单。
- 重点核对 `parser/ast`、DDL/DML executor、Volcano planner/executor、catalog/type registry、存储/WAL/锁、网络协议、复制和 PL/pgSQL 实现。
- `./scripts/build.sh` 编译通过；本机未检测到 OpenSSL，因此构建的是 TLS stub。此次没有重新执行完整回归套件，不能把历史文档中的 PASS 数当作本次验证结果。
- 审计期间工作区源码仍在变化；本次只写入本文、README 文档索引和旧清单的历史标记，没有修改这些在途源码或 `v0.2.0/` 未跟踪目录。

### 2.3 主要源码证据

| 证据 | 说明 |
|---|---|
| `src/main.cpp` | 仍包含巨型 SQL 预处理/字符串分发、legacy 查询与 DDL/DML 兼容路径、输出文本捕获 |
| `src/parser/ast.h`, `src/parser/parser.cpp` | 递归下降 parser 和宽命令枚举；“可分类/可生成 AST”不代表可执行 |
| `src/commands/DdlExecutor.cpp` | typed DDL 只接管明确子集；其他命令回退 legacy；多个 DDL 路径先隐式提交 |
| `src/commands/DmlExecutor.cpp` | typed DML 的受支持边界和大量显式 fallback/unsupported 分支 |
| `src/executor/ExecutionPlan.cpp` | Volcano 算子、简化成本模型；明确缺 index-only scan 等路径 |
| `src/catalog/` | 核心 catalog/OID 框架，但对象全集和 catalog 驱动执行未完成 |
| `src/commands/TableManage.cpp` | 主存储引擎以及大量 sidecar、虚拟 catalog、DDL/DML/事务实现集中点 |
| `src/storage/`, `src/transaction/` | 页、Buffer Pool、WAL、CLOG、FSM/VM、TDE、锁和事务基础 |
| `src/replication/LogicalDecoder.cpp` | 写路径采集式逻辑变更，不是 PostgreSQL WAL 解码/复制协议 |
| `src/network/NetworkServer.cpp` | PostgreSQL protocol 3.0 子集、SCRAM、扩展查询和受限 binary I/O |
| `src/utils/plpgsql.cpp` | 最小 PL/pgSQL 解释器，不是 PostgreSQL PL/pgSQL 完整运行时 |

## 3. 当前能力画像

| 维度 | 当前可确认能力 | 对 PostgreSQL 的判断 |
|---|---|---|
| SQL 前端 | 递归下降 parser、typed AST、较宽语法面 | 复杂语句仍依赖文本改写和 legacy 分支，缺统一 parse/analyze/rewrite 管线 |
| DDL/DML | 常见表、索引、视图、角色、基础 INSERT/UPDATE/DELETE/MERGE | 常用子集可用；对象全集、复杂语义、事务性和依赖一致性不完整 |
| 查询执行 | Volcano 算子、三类 join、聚合、集合操作、窗口、部分并行执行 | 有真实执行器；planner、spill、参数化路径、复杂子查询等差距很大 |
| 存储 | 8 KiB slotted page、heap tuple header、FSM/VM、TOAST、Buffer Pool | 是真实内核基础；仍缺 PG 的成熟格式、vacuum/freeze、全资源 WAL 和长期兼容性 |
| 事务 | WAL/CLOG/MVCC/savepoint/2PC 子集、死锁检测、SSI 子集 | 不能声称 PostgreSQL 等价的 ACID/Serializable/DDL transaction |
| 索引 | 多种访问方法名称和若干真实候选扫描 | 除基础 B+Tree 外，多数是专用 sidecar/简化结构，不是 PG AM/opclass 实现 |
| 协议 | Startup、SCRAM、Simple/Extended Query 子集、部分 binary I/O | 可连接部分客户端；不满足完整 libpq、COPY、portal、replication protocol |
| 复制恢复 | crash recovery、WAL 归档、单时间线 PITR 子集、逻辑变更流 | 无可用 physical standby；逻辑流不兼容 pgoutput 且不能 WAL replay |
| 安全 | 角色、基础 ACL/RLS、pg_hba 子集、SCRAM、TLS wrapper | 认证方法、TLS/channel binding、对象 ACL、安全上下文均不完整 |
| 扩展生态 | 兼容对象记录、少量内置 hook-like C++ 接口 | 无 PostgreSQL extension/fmgr/FDW/background worker 生态 |

## 4. P0：在宣称“可替代 PostgreSQL”前必须关闭

- [ ] **P0-01：统一 SQL 执行管线。** 所有语句必须经过 lexer/parser → parse analysis/binder → rewrite → planner → executor，删除 `main.cpp` 中会改变语义的字符串重写和复杂 SQL legacy fallback。
- [ ] **P0-02：结构化结果与错误。** 执行器返回 typed rows、command tag、warning/error、SQLSTATE 和 diagnostics，禁止以捕获 `std::cout` 文本作为协议结果来源。
- [ ] **P0-03：catalog 成为唯一事实源。** 表、列、索引、类型、函数、约束、权限、依赖、统计、复制对象全部以事务化 catalog 为准，清理重复 sidecar/虚拟表拼装状态。
- [ ] **P0-04：完整事务化 DDL。** 去掉 DDL 隐式提交；支持语句原子性、事务回滚、savepoint、并发 DDL 锁、catalog WAL 和 crash recovery。
- [ ] **P0-05：完整持久性证明。** heap、所有索引、catalog、FSM/VM/TOAST、序列、统计、复制槽和配置状态均遵守 write-ahead/fsync/原子发布规则，并通过系统化 kill-point 测试。
- [ ] **P0-06：索引 WAL 重构。** 停止依赖整文件 before/after image 或未记录增量的 sidecar；为每种生产访问方法实现 page/logical WAL、redo、split recovery 和 vacuum cleanup。
- [ ] **P0-07：完整 MVCC/SSI。** 补齐物理索引范围 predicate lock、完整 rw-dependency 图、safe snapshot、DEFERRABLE read-only transaction 和精确 serialization failure。
- [ ] **P0-08：XID 生命周期。** 实现 wraparound 防护、freeze/all-frozen、MultiXact、oldest xmin horizon、长事务/复制槽对 vacuum horizon 的影响。
- [ ] **P0-09：单实例并发与故障隔离。** 当前明确不支持两个进程共享数据目录；需要可靠 postmaster/backend 隔离或明确定义并证明另一套共享状态架构。
- [ ] **P0-10：真实备份和恢复链。** 在线一致 base backup、backup manifest、增量备份、归档恢复、多 timeline、恢复目标和校验工具必须闭环。
- [ ] **P0-11：物理复制和 hot standby。** WAL sender/receiver、startup/recovery process、standby query、复制槽保留、同步/quorum、promotion/timeline 和故障切换。
- [ ] **P0-12：wire protocol 合规。** 完整错误/notice、portal、COPY、cancel、SSL negotiation、binary type I/O、pipeline 和 replication protocol，建立 libpq/psql/JDBC/ORM 兼容矩阵。
- [ ] **P0-13：权限边界统一。** 每条 SQL、函数、视图、触发器、COPY、大对象、schema/search_path 和维护命令都经过同一 owner/ACL/RLS/security context 检查。
- [ ] **P0-14：资源治理。** work_mem/temp spill、连接/worker/内存/I/O 限额、取消与超时传播、磁盘满/文件描述符耗尽/OOM 的 fail-safe 行为。
- [ ] **P0-15：可升级性。** 定义 catalog/storage version、升级工具和兼容窗口；不能继续以“旧目录导出后重建”作为生产升级策略。
- [ ] **P0-16：差分兼容测试。** 对同一 SQL/并发 schedule/崩溃点在 PostgreSQL 18 与本项目上做结果、SQLSTATE、catalog 和持久化差分。

## 5. PostgreSQL 18 全部 183 条 SQL 命令审计

这里的 `P` 也仍然代表“有差距”。当前没有任何一条命令可以仅凭现有测试证据宣称覆盖 PostgreSQL 的全部语法、权限、并发、错误和 catalog 语义。

### 5.1 ALTER 命令（42/42 已列）

| 状态 | 命令 | 主要差距 |
|---|---|---|
| `P` | `ALTER COLLATION`, `ALTER DATABASE`, `ALTER DEFAULT PRIVILEGES`, `ALTER DOMAIN`, `ALTER GROUP`, `ALTER POLICY`, `ALTER ROLE`, `ALTER SEQUENCE`, `ALTER STATISTICS`, `ALTER SYSTEM`, `ALTER TABLE`, `ALTER TABLESPACE`, `ALTER TYPE`, `ALTER USER` | 有具体 handler，但只覆盖选项子集；owner/ACL、依赖、并发锁、事务回滚、catalog 一致性和 PostgreSQL 错误语义不完整 |
| `S` | `ALTER AGGREGATE`, `ALTER CONVERSION`, `ALTER EVENT TRIGGER`, `ALTER EXTENSION`, `ALTER FOREIGN DATA WRAPPER`, `ALTER FOREIGN TABLE`, `ALTER FUNCTION`, `ALTER INDEX`, `ALTER LANGUAGE`, `ALTER LARGE OBJECT`, `ALTER MATERIALIZED VIEW`, `ALTER OPERATOR`, `ALTER OPERATOR CLASS`, `ALTER OPERATOR FAMILY`, `ALTER PROCEDURE`, `ALTER PUBLICATION`, `ALTER ROUTINE`, `ALTER RULE`, `ALTER SERVER`, `ALTER SUBSCRIPTION`, `ALTER TEXT SEARCH CONFIGURATION`, `ALTER TEXT SEARCH DICTIONARY`, `ALTER TEXT SEARCH PARSER`, `ALTER TEXT SEARCH TEMPLATE`, `ALTER TRIGGER`, `ALTER USER MAPPING` | 多数只修改 `.pg_compat_objects` 定义/owner/name，未改变对应运行时能力 |
| `X` | `ALTER SCHEMA`, `ALTER VIEW` | parser 枚举存在，但当前主执行入口没有可确认的 PostgreSQL 语义实现 |

### 5.2 CREATE 命令（42/42 已列）

| 状态 | 命令 | 主要差距 |
|---|---|---|
| `P` | `CREATE COLLATION`, `CREATE DATABASE`, `CREATE DOMAIN`, `CREATE FUNCTION`, `CREATE GROUP`, `CREATE INDEX`, `CREATE MATERIALIZED VIEW`, `CREATE POLICY`, `CREATE PROCEDURE`, `CREATE PUBLICATION`, `CREATE ROLE`, `CREATE SCHEMA`, `CREATE SEQUENCE`, `CREATE STATISTICS`, `CREATE TABLE`, `CREATE TABLE AS`, `CREATE TABLESPACE`, `CREATE TRIGGER`, `CREATE TYPE`, `CREATE USER`, `CREATE VIEW` | 常用子集有真实对象/数据路径；仍缺完整对象模型、依赖、权限、并发、事务和 option 语义 |
| `S` | `CREATE ACCESS METHOD`, `CREATE AGGREGATE`, `CREATE CAST`, `CREATE CONVERSION`, `CREATE EVENT TRIGGER`, `CREATE EXTENSION`, `CREATE FOREIGN DATA WRAPPER`, `CREATE FOREIGN TABLE`, `CREATE LANGUAGE`, `CREATE OPERATOR`, `CREATE OPERATOR CLASS`, `CREATE OPERATOR FAMILY`, `CREATE RULE`, `CREATE SERVER`, `CREATE SUBSCRIPTION`, `CREATE TEXT SEARCH CONFIGURATION`, `CREATE TEXT SEARCH DICTIONARY`, `CREATE TEXT SEARCH PARSER`, `CREATE TEXT SEARCH TEMPLATE`, `CREATE TRANSFORM`, `CREATE USER MAPPING` | 主要是解析或兼容对象/独立元数据记录；没有相应 executor、AM、FDW、extension、rewrite 或 subscriber 运行时 |

### 5.3 DROP 命令（43/43 已列）

| 状态 | 命令 | 主要差距 |
|---|---|---|
| `P` | `DROP COLLATION`, `DROP DATABASE`, `DROP DOMAIN`, `DROP FUNCTION`, `DROP GROUP`, `DROP INDEX`, `DROP MATERIALIZED VIEW`, `DROP OWNED`, `DROP POLICY`, `DROP PROCEDURE`, `DROP PUBLICATION`, `DROP ROLE`, `DROP SCHEMA`, `DROP SEQUENCE`, `DROP STATISTICS`, `DROP TABLE`, `DROP TABLESPACE`, `DROP TRIGGER`, `DROP TYPE`, `DROP USER`, `DROP VIEW` | 实际删除范围不一；`CASCADE/RESTRICT`、跨对象依赖、owner、并发和事务回滚不完整；`DROP OWNED` 主要清权限记录，不是 PG 对象所有权语义 |
| `S` | `DROP ACCESS METHOD`, `DROP AGGREGATE`, `DROP CAST`, `DROP CONVERSION`, `DROP EVENT TRIGGER`, `DROP EXTENSION`, `DROP FOREIGN DATA WRAPPER`, `DROP FOREIGN TABLE`, `DROP LANGUAGE`, `DROP OPERATOR`, `DROP OPERATOR CLASS`, `DROP OPERATOR FAMILY`, `DROP ROUTINE`, `DROP RULE`, `DROP SERVER`, `DROP SUBSCRIPTION`, `DROP TEXT SEARCH CONFIGURATION`, `DROP TEXT SEARCH DICTIONARY`, `DROP TEXT SEARCH PARSER`, `DROP TEXT SEARCH TEMPLATE`, `DROP TRANSFORM`, `DROP USER MAPPING` | 多数只删除兼容对象记录；没有可删除的真实运行时对象 |

### 5.4 其余命令（56/56 已列）

| 状态 | 命令 | 主要差距 |
|---|---|---|
| `P` | `ABORT`, `ANALYZE`, `BEGIN`, `CALL`, `CHECKPOINT`, `CLOSE`, `CLUSTER`, `COMMENT`, `COMMIT`, `COMMIT PREPARED`, `COPY`, `DEALLOCATE`, `DECLARE`, `DELETE`, `DISCARD`, `DO`, `END`, `EXECUTE`, `EXPLAIN`, `FETCH`, `GRANT`, `INSERT`, `LISTEN`, `LOCK`, `MERGE`, `MOVE`, `NOTIFY`, `PREPARE`, `PREPARE TRANSACTION`, `REASSIGN OWNED`, `REFRESH MATERIALIZED VIEW`, `REINDEX`, `RELEASE SAVEPOINT`, `RESET`, `REVOKE`, `ROLLBACK`, `ROLLBACK PREPARED`, `ROLLBACK TO SAVEPOINT`, `SAVEPOINT`, `SECURITY LABEL`, `SELECT`, `SELECT INTO`, `SET`, `SET CONSTRAINTS`, `SET ROLE`, `SET SESSION AUTHORIZATION`, `SET TRANSACTION`, `SHOW`, `START TRANSACTION`, `TRUNCATE`, `UNLISTEN`, `UPDATE`, `VACUUM`, `VALUES` | 均只有子集；详细差距见后续各能力域 |
| `S` | `IMPORT FOREIGN SCHEMA`, `LOAD` | FDW 没有运行时；`LOAD` 只记录“loaded_library”，并未 `dlopen`/加载 PostgreSQL shared library |

汇总：`P=110`、`S=71`、`X=2`，合计 183。这个统计只说明审计覆盖完整，不代表实现完成度。

## 6. SQL 前端、名称解析和语义分析

- [ ] **SQL-01** 用正式 grammar/lexer 覆盖 PostgreSQL 词法规则，去掉基于 `substr/find/tokenize` 的二次解析。
- [ ] **SQL-02** 支持完整 quoted identifier、Unicode escape identifier/string、escape string、bit/hex literal、标准 conforming strings 和嵌套注释。
- [ ] **SQL-03** 将当前 SQL 预处理中的 boolean、array、CASE、ANY/ALL 等文本 rewrite 移到 AST/analyze 阶段。
- [ ] **SQL-04** 实现 binder：relation/column/function/operator/type 的 namespace lookup、歧义检测和逐层 scope。
- [ ] **SQL-05** 完整实现 schema-qualified 名称、`search_path`、临时 schema、`pg_catalog` 隐式搜索和跨 schema 同名对象。
- [ ] **SQL-06** 实现 PostgreSQL identifier folding 和最大长度规则，所有 catalog/物理文件名使用同一规范。
- [ ] **SQL-07** 实现 unknown literal、参数类型推断、preferred type、category 和 common supertype 选择。
- [ ] **SQL-08** 实现 assignment/implicit/explicit cast graph；当前 `CREATE CAST` 记录不能驱动 analyzer/executor。
- [ ] **SQL-09** 实现函数/过程/聚合/operator 重载解析、默认参数、variadic、多态伪类型和 schema 可见性。
- [ ] **SQL-10** 实现 collatable expression 推导、collation conflict 和 provider/version 处理。
- [ ] **SQL-11** 对所有语句做尾随 token 检查、准确 error position、hint/detail/context 和 PostgreSQL SQLSTATE 映射。
- [ ] **SQL-12** 移除以空格分割行/列的内部结果格式；它不能正确承载含空格、NULL、转义和复合值的数据。
- [ ] **SQL-13** 统一 SQL 级 `PREPARE ... AS ... $n` 与协议 prepared statement 的类型、计划、失效和 portal 生命周期。
- [ ] **SQL-14** 实现 rewrite system 所需的 query tree 复制、权限标记和依赖失效。

## 7. Catalog、OID、对象和 DDL

- [ ] **CAT-01** 将 catalog 从 CSV/sidecar 缓存提升为 WAL/MVCC 管理的普通系统关系。
- [ ] **CAT-02** `pg_class`、`pg_attribute`、`pg_type`、`pg_proc`、`pg_depend`、`pg_namespace` 等必须是内部执行的真实来源，而不是另一套虚拟输出。
- [ ] **CAT-03** 补齐 `pg_constraint`、`pg_index`、`pg_am`、`pg_opclass`、`pg_operator`、`pg_cast`、`pg_collation`、`pg_rewrite`、`pg_trigger`、`pg_policy`、`pg_auth*`、`pg_default_acl`、`pg_database`、`pg_tablespace`、`pg_statistic*`、复制 catalog 等。
- [ ] **CAT-04** 实现所有对象的稳定 OID、reg* 查找、OID 引用和 dump/restore 保真。
- [ ] **CAT-05** 统一 owner、ACL、comment、security label、extension membership 和 dependency graph。
- [ ] **CAT-06** 完整 `CASCADE/RESTRICT`、internal/auto/normal/pin/extension dependency 行为。
- [ ] **CAT-07** DDL 对 catalog 和物理文件的修改必须同一事务提交/回滚/恢复。
- [ ] **CAT-08** `CREATE DATABASE` 补 template、owner、encoding、locale/ICU provider、collation version、tablespace、strategy 和 connection limit。
- [ ] **CAT-09** schema 不应通过 `schema__table` 或 marker 模拟；补真正 namespace、rename/owner、权限和 search_path。
- [ ] **CAT-10** 表/列 DDL 补完整 rewrite、`USING`、dependency invalidation、锁等级、递归/ONLY 和多 action 原子性。
- [ ] **CAT-11** 分区表补默认分区、约束证明、attach validation、detach concurrently/finalize、分区索引和跨分区唯一性。
- [ ] **CAT-12** 继承补约束/default/generated/identity/统计/权限传播和多父表冲突规则。
- [ ] **CAT-13** 临时对象补 `pg_temp_N` catalog、search_path、ON COMMIT、两阶段事务限制和 session/backend 清理语义。
- [ ] **CAT-14** unlogged relation 补 init fork、crash truncate、复制和备份行为。
- [ ] **CAT-15** sequence 补 relation/catalog 语义、cache、cycle、min/max、owned-by、并发、WAL、session currval/lastval 和非事务行为。
- [ ] **CAT-16** view 补完整 rewrite rule、自动可更新判断、CHECK OPTION、security barrier/invoker、recursive view 和依赖。
- [ ] **CAT-17** materialized view 补列类型推断、依赖、populate state、真实 concurrent refresh 和唯一索引要求。
- [ ] **CAT-18** trigger 补 trigger function、transition table、constraint/deferred trigger、列列表、`WHEN` scope、触发顺序和递归规则。
- [ ] **CAT-19** event trigger 和 rule 当前只是骨架，需要真实 DDL/rewrite 事件执行。
- [ ] **CAT-20** tablespace 补 cluster 级 catalog、OID/symlink 布局、owner/ACL、并发、跨设备持久化和备份恢复。
- [ ] **CAT-21** `COMMENT`/`SECURITY LABEL` 覆盖 PostgreSQL 对象全集，并使用 catalog dependency。
- [x] **CAT-22** 删除 `.pg_compat_objects` 的“成功但无运行时效果”语义；未实现命令应返回 `0A000 feature_not_supported`。

## 8. 数据类型、I/O、函数和操作符

当前 `TypeRegistry` 注册了较宽的类型名，表达式执行器也有约 140 个内置标量函数名，但多数类型仍以规范化文本或 string-backed 形式承载。这与 PostgreSQL 的 binary datum、typmod、cast、operator class 和函数生态不是一个完成度。

- [ ] **TYPE-01** numeric 补 PostgreSQL 精度/scale、NaN/±Infinity、舍入、溢出、比较、hash、sort support 和 binary protocol。
- [ ] **TYPE-02** integer/float 补完整溢出、NaN ordering、implicit cast、平台无关 binary storage 和错误语义。
- [x] **TYPE-03** `money` 补 locale-aware I/O 和精确内部表示；不能走 double/字符串近似。
- [ ] **TYPE-04** character/text 补无限 text/bytea 语义、typmod、blank padding、encoding、collation 和 Unicode 行为；当前 65535 等项目限制不是 PG 限制。
- [x] **TYPE-05** bytea 补全部 operators/functions、binary I/O 和大值 TOAST 行为。
- [ ] **TYPE-06** date/time/timestamp/timestamptz/interval 补微秒精度、BC/infinity、完整 timezone database、DST、timezone abbreviation、typmod 和所有边界值。
- [ ] **TYPE-07** boolean 保持真正三值类型，避免预处理阶段把 `TRUE/FALSE` 文本改成 `1/0` 引起类型偏移。
- [ ] **TYPE-08** enum 补 catalog ordering、rename/add value 的并发可见性、比较/hash 和 dump/restore。
- [ ] **TYPE-09** geometric types 补 PostgreSQL binary representation、全部 operator/function、NaN/边界和 GiST/SP-GiST opclass。
- [ ] **TYPE-10** inet/cidr/macaddr 补完整网络运算、排序、包含、hash 和 binary protocol。
- [ ] **TYPE-11** bit/varbit 补完整位运算、移位、比较、substring 和 binary I/O。
- [ ] **TYPE-12** tsvector/tsquery 补配置/字典/parser、词干/停用词、完整 query tree、headline/rank 和 GIN/GiST opclass。
- [x] **TYPE-13** UUID 使用 16-byte RFC datum 与原生索引键，保留 PostgreSQL 宽松输入/规范输出，并实现 `gen_random_uuid()`、`uuidv4()`、`uuidv7([shift])`、版本/时间提取及 binary protocol。
- [ ] **TYPE-14** XML 补 libxml 语义、well-formed document/content、XMLTABLE/XMLNAMESPACES 和相关函数。
- [ ] **TYPE-15** JSON/JSONB 补真正 binary JSONB、完整 operators/functions、SQL/JSON、jsonpath evaluator、GIN opclass、duplicate key/numeric/collation 细节。
- [ ] **TYPE-16** array 补任意元素类型、多维 lower bounds、rectangularity、comparison/hash、array assignment、record/array binary I/O 和完整函数集。
- [ ] **TYPE-17** composite/record 补 row descriptor、anonymous record、field selection/update、comparison、I/O 和函数返回 record 推导。
- [ ] **TYPE-18** range 补 canonical/subtype diff、empty/infinite bounds、operators、aggregate 和 GiST/SP-GiST；multirange 当前只是 string-backed 名称，需要真实实现。
- [ ] **TYPE-19** domain 补任意层嵌套、multiple CHECK、NOT NULL/default、cast、全表 revalidation 和 domain-over-composite/array。
- [ ] **TYPE-20** OID 家族补 `oid`, `regclass`, `regtype`, `regproc`, `regprocedure`, `regoperator`, `regnamespace`, `regrole` 等真实 catalog resolution。
- [ ] **TYPE-21** 内部类型补 `xid/xid8/cid/tid`, `pg_lsn`, `pg_snapshot`, `aclitem`, `name`, `char`, `cstring` 等 PostgreSQL I/O/比较语义。
- [ ] **TYPE-22** pseudo/polymorphic 类型补 `anycompatible*`, `anymultirange`, `internal`, `trigger`, `event_trigger`, `table_am_handler`, `index_am_handler` 等调用约束。
- [ ] **FUNC-01** 补 PostgreSQL 数学、字符串、binary、格式化、日期时间、枚举、几何、网络、全文、XML、JSON、array/range、系统信息、管理、统计和 replication 函数族。
- [ ] **FUNC-02** 实现 aggregate catalog/executor：ordered-set、hypothetical-set、partial/combine/serialize、moving aggregate、FILTER/ORDER BY/DISTINCT 完整组合。
- [ ] **FUNC-03** 实现 user-defined operator、commutator/negator、selectivity function、hash/merge 标记和 dependency。
- [ ] **FUNC-04** volatility/strict/leakproof/parallel safety/security definer/cost/rows/SET 属性必须真正影响 planner 和 executor。
- [ ] **FUNC-05** 实现 SQL function inlining、support functions、polymorphism、variadic/default/named arguments 和重载。
- [ ] **FUNC-06** PL/pgSQL 补 records/rowtype、exceptions、diagnostics、dynamic SQL、cursors、trigger variables、subtransactions、packages of statements、plan cache 和 dependency invalidation。

## 9. 约束和数据完整性

- [ ] **CONS-01** primary key/unique 支持完整多列、NULLS NOT DISTINCT、deferrable、partitioned table 和 concurrent conflict 语义。
- [ ] **CONS-02** foreign key 支持多列、MATCH FULL/PARTIAL、deferrable、循环 FK、partitioned table、各种 action 与并发 snapshot/locking 规则。
- [ ] **CONS-03** CHECK 支持 immutable dependency、NOT VALID/VALIDATE、domain/table 语义和 partition constraint 证明。
- [ ] **CONS-04** exclusion constraint 使用真实 GiST/opclass、任意表达式/操作符、多列、deferrable 和并发冲突检查。
- [ ] **CONS-05** generated column 补 PostgreSQL 18 virtual/stored 默认与限制、依赖和复制行为。
- [ ] **CONS-06** identity 补 ALWAYS/BY DEFAULT、OVERRIDING、sequence options、ALTER 和 partition/inheritance 行为。
- [ ] **CONS-07** constraint trigger 接入统一 deferred event queue，并保证 savepoint/rollback/crash 语义。
- [ ] **CONS-08** DML 的所有入口，包括 legacy、view、trigger、COPY、partition route、MERGE、ON CONFLICT，都必须经过同一约束路径。

## 10. 查询、DML 和执行语义

- [ ] **DML-01** 删除 DML 双入口；复杂 `INSERT ... SELECT`、view/CTE/partition target 全部使用结构化 ModifyTable。
- [ ] **DML-02** `ON CONFLICT` 补 index inference、partial/expression index、constraint target、复杂 expression/subquery、并发 speculative insertion。
- [ ] **DML-03** `UPDATE ... FROM`/`DELETE ... USING` 补任意 join tree、outer/lateral/subquery、重复 source row 和 `WHERE CURRENT OF`。
- [ ] **DML-04** `MERGE` 补多个 WHEN、MATCHED DELETE、NOT MATCHED BY SOURCE/TARGET、DO NOTHING、复杂 source、RETURNING 和并发规则。
- [ ] **DML-05** PostgreSQL 18 `RETURNING WITH (OLD/NEW AS ...)`、任意表达式、subquery/window、trigger 后值和协议 metadata。
- [ ] **DML-06** `COPY` 补 protocol STDIN/STDOUT、binary、PROGRAM、FREEZE、ON_ERROR、REJECT_LIMIT、HEADER MATCH、encoding 和权限。
- [ ] **QRY-01** SELECT target list 补完整 expression、SRF、row expansion、star qualification、alias visibility 和 resjunk column。
- [ ] **QRY-02** FROM 补完整 LATERAL、table function、ROWS FROM、WITH ORDINALITY、TABLESAMPLE、XMLTABLE/JSON_TABLE。
- [ ] **QRY-03** join 补 USING/NATURAL 的输出列合并、FULL/outer null extension、lateral/parameterized join 和任意嵌套语义。
- [ ] **QRY-04** subquery 补 correlated scalar/EXISTS/IN/ANY/ALL、row comparison、decorrelation、parameter passing 和 NULL 三值逻辑。
- [ ] **QRY-05** CTE 补 recursive evaluation、SEARCH/CYCLE、materialized/not materialized、data-modifying CTE snapshot 和 visibility。
- [ ] **QRY-06** set operations 补任意 query expression、对应列类型/collation、嵌套 precedence、ALL duplicate count 和 ORDER/LIMIT scope。
- [ ] **QRY-07** aggregate/grouping 补 grouping sets 的任意组合、GROUPING/GROUPING_ID、ordered/distinct aggregate 和 functional dependency。
- [ ] **QRY-08** window 补多个 window specs、named/inherited window、所有 frame/exclusion/peer 边界、ordered-set interaction 和 spill。
- [ ] **QRY-09** DISTINCT/DISTINCT ON 补 PostgreSQL ordering 约束、NULL/collation 和可 spill 实现。
- [ ] **QRY-10** ORDER BY 补任意 expression、operator USING、NULLS、collation、stable tie handling 和 top-N/external sort。
- [ ] **QRY-11** row locking 补 `FOR UPDATE/NO KEY UPDATE/SHARE/KEY SHARE OF ... NOWAIT/SKIP LOCKED` 的完整冲突和 EPQ recheck。
- [ ] **QRY-12** inheritance/partition scan、ONLY、view rewrite 和 RLS 必须在 planner 中统一展开，不能靠文本标志或旁路扫描。
- [ ] **QRY-13** snapshot、trigger、rule、RLS、generated column 和 RETURNING 的执行顺序需与 PostgreSQL 一致。

## 11. 优化器和执行器

- [ ] **OPT-01** 建立 PostgreSQL 式 relation/path/parameterization/equivalence class/pathkey 框架，而不是直接拼单棵计划树。
- [ ] **OPT-02** 实现 exhaustive/DP join search、GEQO 阈值、outer/semi/anti join constraints 和 bushy plan。
- [ ] **OPT-03** 完整 predicate implication、constant propagation、equivalence class、join removal、outer join reduction 和 partition pruning。
- [ ] **OPT-04** 完整统计：采样、null fraction、ndistinct、MCV、histogram、correlation、extended ndistinct/dependencies/MCV、表达式统计。
- [ ] **OPT-05** 实现 selectivity/cost support function、数据类型/operator/collation-aware 估算和统计失效。
- [ ] **OPT-06** parameterized path、nested-loop inner index scan、subplan/initplan、memoize 和 correlated execution。
- [ ] **OPT-07** 补 Seq/TID/TID Range/Sample/Function/Values/CTE/WorkTable/Foreign/Custom/Append/MergeAppend 扫描节点。
- [ ] **OPT-08** 补真正 index-only scan、visibility map 条件、heap fetch fallback 和 INCLUDE column。
- [ ] **OPT-09** bitmap 补 block/lossy bitmap、range predicate、parallel bitmap、recheck 和 work_mem spill。
- [ ] **OPT-10** sort/hash/aggregate/window/join 必须使用 work_mem 并支持临时文件 spill、批处理和 skew handling。
- [ ] **OPT-11** 补 Incremental Sort、Memoize、Materialize、Unique、LockRows、ModifyTable、ProjectSet、RecursiveUnion 等节点完整语义。
- [ ] **OPT-12** parallel query 补 worker pool/lifecycle、parallel-aware append/bitmap/hash、partial/final aggregate、leader participation、parallel safety。
- [ ] **OPT-13** PostgreSQL 18 AIO：异步 read queue、prefetch、io_method/io_combine_limit、统计和取消；不只等于“使用 io_uring”。
- [ ] **OPT-14** LLVM JIT：expression、tuple deform、cost threshold、EXPLAIN JIT 信息和平台构建。
- [ ] **OPT-15** generic/custom prepared plans、catalog/GUC/statistics invalidation、search_path/role/RLS 安全的 plan cache。
- [ ] **OPT-16** EXPLAIN 覆盖所有 statement/node，补 VERBOSE、COSTS、SETTINGS、WAL、MEMORY、SERIALIZE、SUMMARY、FORMAT JSON/XML/YAML 完整结构。
- [ ] **OPT-17** executor cancellation、interrupt、statement timeout、error cleanup 和 resource owner 必须贯穿全部算子/worker/I/O。

## 12. 索引和访问方法

- [ ] **IDX-01** 实现 catalog-driven Table AM/Index AM API、handler、support routine、validator、cost estimate、build/insert/scan/vacuum callbacks。
- [ ] **IDX-02** operator class/family、strategy/support number、cross-type operator、collation 和 sort support。
- [ ] **IDX-03** B-tree 补 PostgreSQL key ordering、NULL、dedup、suffix truncation、page split/delete/recycle、fast root、high key、concurrent scan 和 corruption checks。
- [ ] **IDX-04** PostgreSQL 18 B-tree skip scan 和多列统计驱动 path 选择。
- [ ] **IDX-05** Hash 补 metapage/bucket/overflow/split、并发锁、WAL、vacuum 和 hash support function。
- [ ] **IDX-06** GIN 补 entry/posting tree、pending list/fastupdate、extract/consistent/triConsistent、vacuum、WAL 和 jsonb/array/tsvector opclass。
- [ ] **IDX-07** GiST 当前只是 flat interval sidecar；需真实 tree、union/penalty/picksplit/same/consistent/distance、KNN、WAL 和 opclass。
- [ ] **IDX-08** SP-GiST 补 radix/quad/k-d 等节点模型、choose/picksplit/inner-consistent/leaf-consistent、WAL/vacuum/opclass。
- [ ] **IDX-09** BRIN 补 page range summary tuple、revmap、autosummarize、desummarize、minmax/minmax-multi/bloom/inclusion opclass。
- [ ] **IDX-10** expression/partial index 补 immutable 检查、dependency、predicate implication、planner matching 和 HOT safety。
- [ ] **IDX-11** covering/index-only scan 补 VM、included payload、non-key attribute 和 heap visibility fallback。
- [ ] **IDX-12** partitioned index、attach/detach、parent validity 和唯一约束跨分区规则。
- [ ] **IDX-13** 真正 `CREATE INDEX CONCURRENTLY`/`REINDEX CONCURRENTLY`：invalid state、多事务阶段、等待旧 snapshot、失败恢复。
- [ ] **IDX-14** vacuum cleanup、page deletion、bulk delete、amcheck、pg_stat index/progress 和 corruption recovery。
- [ ] **IDX-15** 所有索引 DML、DDL、reindex、crash recovery、TDE、backup/restore 路径使用统一且可证明的持久化协议。

## 13. 事务、MVCC、锁和 VACUUM

- [ ] **TXN-01** 明确实现 PG 的 Read Uncommitted=Read Committed；逐语句 snapshot 和 command counter 行为一致。
- [ ] **TXN-02** Repeatable Read、Serializable、read-only/deferrable 的 snapshot 时机和错误条件一致。
- [ ] **TXN-03** tuple xmin/xmax/cmin/cmax/ctid/infomask、combo CID、HOT chain、redirect/dead line pointer 的全部可见性规则。
- [ ] **TXN-04** subtransaction 使用真实 SubXID/parent、overflow、CLOG/pg_subtrans、resource owner 和错误状态。
- [ ] **TXN-05** savepoint 回滚 catalog、locks、files、deferred events、portals、LISTEN/NOTIFY 和 sequence 相关状态。
- [ ] **TXN-06** 2PC 使用全局 durable prepared transaction state，恢复 locks/subxacts/invalidation/notify，并支持跨 backend 管理和清理。
- [ ] **TXN-07** 完整 heavyweight lock modes/conflict matrix、fast-path locks、lock queue fairness、deadlock soft edge/reorder 和 wait events。
- [ ] **TXN-08** tuple locks、MultiXact、key-share/no-key-update、EPQ 和 FK/unique 冲突的锁规则。
- [ ] **TXN-09** predicate lock 在 relation/page/tuple/index range 间升级，覆盖所有访问方法和空范围。
- [x] **TXN-10** advisory lock 的 session/transaction 两类、two-int key、shared/exclusive、try-lock 和 cleanup。
- [ ] **TXN-11** commit/abort/group commit 顺序、synchronous_commit 级别、commit timestamp 和 WAL flush wait。
- [x] **TXN-12** LISTEN/NOTIFY 在事务提交后投递、rollback 丢弃、payload/channel 规则、跨 session/backend 队列和协议异步通知。
- [ ] **VAC-01** VACUUM 的 prune/freeze/index cleanup/truncate、visibility/freeze map、failsafe 和 wraparound 防护。
- [ ] **VAC-02** autovacuum launcher/worker、per-table thresholds/cost delay、worker slots、anti-wraparound 优先级和冲突取消。
- [ ] **VAC-03** VACUUM FULL 使用 transactional table rewrite/swap；ANALYZE/VACUUM option 与 progress view 对齐。
- [ ] **VAC-04** HOT eligibility 必须考虑所有索引（含 expression/partial）和 page space；chain pruning 与 concurrent snapshot 安全。

## 14. 存储、WAL、checkpoint 和恢复

- [ ] **STO-01** 定义稳定 on-disk format、control file、system identifier、catalog version、block size、endianness 和 feature flags。
- [ ] **STO-02** relation locator/fork/segment 布局、database/tablespace OID 和临时 relation 命名对齐内部模型。
- [ ] **STO-03** page header、item identifier、tuple/varlena/toast pointer、special space、LSN/checksum 的兼容且自描述格式。
- [ ] **STO-04** Buffer Manager 补 shared hash/partition locks、buffer content locks、I/O-in-progress、prefetch、bulk strategy、ring buffer 和 resource owner pin cleanup。
- [ ] **STO-05** FSM/VM 持久化、crash rebuild、all-visible/all-frozen 和 index-only/VACUUM 交互。
- [ ] **STO-06** TOAST 补 varlena short/compressed/external datum、storage strategy、toast_tuple_target、pglz/lz4、dedup/delete/vacuum 和索引一致性。
- [ ] **WAL-01** WAL resource manager 记录 heap/index/catalog/fsm/vm/toast/multixact/sequence/standby 等所有资源变化。
- [ ] **WAL-02** WAL insertion、page LSN、full-page image、compression、continuation、segment switch、recycling 和 concurrent writer 规则。
- [ ] **WAL-03** checkpoint 补 redo horizon、dirty buffer scheduling、checkpoint completion、control file、WAL retention 和节流。
- [ ] **WAL-04** restartpoint、recovery consistency、recovery conflict、hot standby snapshot、timeline history 和 promotion。
- [ ] **WAL-05** recovery 必须是 redo-based 状态机；当前额外 before-image undo 模型需证明与 steal/no-force、并发 checkpoint 的所有 crash window 一致。
- [ ] **WAL-06** 数据页、所有索引和元数据 checksum；补离线/在线启停与 `pg_checksums`/verify 工具等价物。
- [ ] **WAL-07** 目录/fsync/rename/link/unlink 顺序覆盖 ext4/XFS、跨设备表空间、磁盘满、partial write、torn write 和 power-loss。
- [ ] **WAL-08** unlogged/temp relation、2PC、sequence、DDL、logical slot 在 crash 后的专门恢复规则。
- [ ] **STO-07** 大对象补 catalog、ACL、事务、64-bit offset、lo_* API、protocol/libpq 和 vacuum。
- [ ] **STO-08** TDE 使用成熟密码库和审计过的 AEAD/KMS 方案；补索引/WAL/temp/backup 全覆盖、密钥轮换、per-database key 和灾难恢复。自研 SHA-256-CTR+EtM 不应直接作为生产加密承诺。

## 15. 复制、高可用和备份

- [ ] **REPL-01** physical replication connection、`IDENTIFY_SYSTEM`、`START_REPLICATION`、CopyBoth、keepalive/feedback 和 WAL sender/receiver。
- [ ] **REPL-02** standby startup/replay、read-only query、recovery snapshot、冲突处理、`hot_standby_feedback`。
- [ ] **REPL-03** physical/logical replication slot 持久化、restart/confirmed LSN、WAL retention、xmin/catalog_xmin、drop/advance/copy/sync/failover。
- [ ] **REPL-04** synchronous replication、remote_write/flush/apply、priority/quorum、sync standby reconfiguration 和 commit wait。
- [ ] **REPL-05** cascading replication、timeline follow、promotion、rewind 和 split-brain 运维边界。
- [ ] **REPL-06** logical decoding 必须从 WAL 解码 committed transaction，而不是 DML 写路径旁路采集。
- [ ] **REPL-07** 实现 wire-compatible `pgoutput`、protocol v1-v4、large transaction streaming、2PC、origin、binary、schema/type messages。
- [ ] **REPL-08** publication 补 column list、row filter、partition root、publish_via_partition_root 和 ALTER 行为。
- [ ] **REPL-09** subscription/apply worker、initial table sync、replication origin、conflict、disabled slot、two-phase、failover slot。
- [ ] **REPL-10** logical decoding plugin API、snapshot export、reorder buffer、spill 和 output plugin lifecycle。
- [ ] **BACKUP-01** online base backup protocol/API、start/stop backup、backup_label、tablespace map、WAL inclusion 和 throttling。
- [ ] **BACKUP-02** backup manifest、checksums、验证工具和损坏/缺文件诊断。
- [ ] **BACKUP-03** PostgreSQL incremental backup/backup summary 和 `pg_combinebackup` 等价链路。
- [ ] **BACKUP-04** PITR 补 `restore_command`、recovery.signal/standby.signal、name/time/xid/LSN/immediate target、inclusive/action/pause。
- [ ] **BACKUP-05** 多 timeline archive、history file、archive cleanup、archive module/command retry 和安全 shell substitution。
- [ ] **BACKUP-06** `pg_dump`/`pg_restore` 兼容的 schema/data/archive 格式、依赖排序、parallel dump/restore、large object、ACL/owner。
- [ ] **BACKUP-07** `pg_rewind`、promote 后重新加入、备份恢复演练和 RPO/RTO 证据。

## 16. PostgreSQL 协议和客户端兼容

- [x] **PROTO-01** 协议 3.0/3.2 negotiation、startup parameters、ParameterStatus、BackendKeyData、ReadyForQuery 状态完整性。
- [x] **PROTO-02** Simple Query 多 statement、implicit transaction、empty query、command tag 和错误后跳过规则。
- [ ] **PROTO-03** Extended Query Parse/Bind/Describe/Execute/Close/Flush/Sync、unnamed replacement、portal suspension、error recovery 和 pipelining。
- [ ] **PROTO-04** RowDescription/DataRow 使用准确 type OID/typmod/table OID/attnum/format；不能由文本输出猜列。
- [ ] **PROTO-05** 所有内置/用户类型的 text/binary input/output，尤其 numeric、array、range/composite、jsonb、inet、interval、bytea。
- [ ] **PROTO-06** COPY IN/OUT/BOTH、CopyData/Done/Fail 和流式 backpressure。
- [ ] **PROTO-07** CancelRequest 使用 backend PID/secret key，在阻塞锁、I/O、并行 worker 和长算子中及时生效。
- [ ] **PROTO-08** ErrorResponse/NoticeResponse/NotificationResponse 全字段和 SQLSTATE；异步消息可以在查询之间/期间发送。
- [ ] **PROTO-09** SSLRequest/GSSENCRequest、TLS negotiation、证书、SNI、channel binding 和 secure renegotiation policy。
- [ ] **PROTO-10** replication mode 与 physical/logical streaming protocol。
- [ ] **PROTO-11** Unix-domain socket、IPv4/IPv6、keepalive、TCP user timeout、client_encoding 和 locale。
- [ ] **PROTO-12** 建立并持续运行 libpq、psql、JDBC、Npgsql、psycopg、pgx、SQLAlchemy、Django、Hibernate 兼容套件。
- [ ] **CLIENT-01** 提供或兼容 `psql` 元命令、COPY、describe、变量、脚本错误控制和密码处理。
- [ ] **CLIENT-02** 工具链：initdb、pg_ctl、createdb/dropdb、createuser/dropuser、vacuumdb、reindexdb、clusterdb、pg_isready、pgbench。

## 17. 安全、认证和权限

- [ ] **SEC-01** `pg_hba.conf` 完整 record type、samehost/samenet、replication、database/role list、include、map、reload 和错误诊断。
- [ ] **SEC-02** 认证方法：peer/ident、cert、LDAP、PAM、RADIUS、GSSAPI/Kerberos、SSPI、BSD、OAuth；不支持时明确拒绝而非近似。
- [ ] **SEC-03** SCRAM-SHA-256-PLUS/channel binding、iteration policy、verifier lifecycle、password encryption GUC 和 credential rotation。
- [ ] **SEC-04** TLS 使用真实 OpenSSL 构建作为发布门槛，补协议 negotiation、client cert、CRL/OCSP、cipher/min protocol、reload 和统计。
- [ ] **SEC-05** role membership 补 INHERIT/SET/ADMIN option 的 PostgreSQL 18 语义、grantor/dependency、循环和 DROP/REASSIGN OWNED。
- [ ] **SEC-06** ACL item 和所有对象类型：database/schema/table/column/sequence/function/procedure/language/type/domain/FDW/server/tablespace/large object/parameter。
- [ ] **SEC-07** default privileges 使用 catalog，并正确作用于 owner/schema/object type/large object。
- [ ] **SEC-08** RLS 补 planner/rewrite integration、policy dependency、partition/inheritance、prepared plan、leakproof ordering 和完整 owner/bypass/force 规则。
- [ ] **SEC-09** SECURITY DEFINER/INVOKER 的 user identity、search_path、防对象劫持、SET 配置和异常恢复。
- [ ] **SEC-10** view security_barrier/security_invoker、function leakproof、row security 与 optimizer pushdown 的安全证明。
- [ ] **SEC-11** `SECURITY LABEL` provider、sepgsql/MAC hook；当前 label 文件不是强制访问控制。
- [ ] **SEC-12** COPY/PROGRAM、file read/write、large object、extension/library load 的超级用户/预定义角色权限和路径防护。
- [ ] **SEC-13** 审计日志需防篡改、结构化、敏感参数脱敏、rotation/retention；同时明确它不是 PostgreSQL 核心兼容能力。

## 18. 系统目录、information_schema、监控和运维

- [ ] **MON-01** `pg_catalog` 补 PostgreSQL 18 system catalogs/views/functions 的结构、OID、类型和权限过滤。
- [ ] **MON-02** 当前 `information_schema` 只有 tables/columns/statistics/routines/views/triggers/key_column_usage 等少量虚拟表；补标准全集和角色可见性。
- [ ] **MON-03** 统计子系统补 shared/persistent counters、snapshot semantics、reset、track_* GUC、function/SLRU/WAL/checkpointer/bgwriter/I/O 统计。
- [ ] **MON-04** `pg_stat_activity` 补 state、query_id、xact/query start、wait_event_type/event、backend type、client、leader pid 和权限脱敏。
- [ ] **MON-05** `pg_locks` 补全部 locktag/mode/granted/fastpath/waitstart 和 predicate locks。
- [ ] **MON-06** 复制/归档/SSL/GSS/subscription/slot/recovery 统计视图。
- [ ] **MON-07** PostgreSQL 18 `pg_stat_io`、`pg_aios`、WAL/checkpointer/slru 和 backend memory context 视图。
- [ ] **MON-08** ANALYZE/VACUUM/CREATE INDEX/CLUSTER/COPY/base backup progress views。
- [ ] **MON-09** `pg_stat_statements` 补 queryid/jumble、plans、rows、block/WAL/JIT/parallel、reset/minmax、容量淘汰和权限。
- [ ] **MON-10** logging collector、stderr/csvlog/jsonlog/syslog、rotation、log_line_prefix、statement/duration/error verbosity 和采样。
- [ ] **MON-11** auto_explain、pg_buffercache 等应通过真实 extension 或清楚标注为内置兼容视图，字段必须对齐。
- [ ] **OPS-01** 配置采用 data directory/control file/GUC context/source/reload/restart 语义；当前大量状态依赖启动 CWD。
- [ ] **OPS-02** `SHOW ALL`/`pg_settings` 补完整 name/setting/unit/category/context/source/min/max/enum/pending_restart。
- [ ] **OPS-03** SIGHUP/reload、smart/fast/immediate shutdown、startup lock/PID file、crash restart 和 child supervision。
- [ ] **OPS-04** 日志、WAL、数据、temp、archive 的目录/权限/umask/ownership 和服务管理约定。
- [ ] **OPS-05** 磁盘空间、inode、FD、内存、CPU、I/O、连接风暴和慢客户端的监控及保护。

## 19. 扩展、FDW、过程语言和生态

- [ ] **EXT-01** `CREATE EXTENSION` 控制文件、SQL install/update、version graph、relocatable/schema、membership、dependency、dump/restore。
- [ ] **EXT-02** 动态库加载、fmgr ABI、Datum/NullableDatum、memory context、PG_FUNCTION_INFO、error/interrupt 安全。
- [ ] **EXT-03** 自定义 base/composite/range/multirange type I/O、typmod、analyze、subscript handler。
- [ ] **EXT-04** 自定义 function/procedure/aggregate/operator/cast/collation/conversion 的真实 catalog 驱动执行。
- [ ] **EXT-05** index/table access method API、operator class/family 和 WAL for extensions。
- [ ] **EXT-06** planner/executor/utility/object access/emit log 等 hooks 和 CustomScan API。
- [ ] **EXT-07** shared memory/LWLock tranche/dynamic shared memory、shared preload/session preload。
- [ ] **EXT-08** background worker 注册、启动时机、restart、signal、DB connection 和 shared memory。
- [ ] **EXT-09** procedural language handler/validator/inline handler；PL/Python、PL/Perl、PL/Tcl 等生态接口。
- [ ] **FDW-01** FDW handler/validator、foreign server/user mapping、GetForeignRelSize/Paths/Plan、scan/modify/direct modify、transaction callbacks。
- [ ] **FDW-02** IMPORT FOREIGN SCHEMA、parameterized pushdown、join/aggregate pushdown、async foreign scan 和 EXPLAIN。
- [ ] **EXT-10** logical decoding output plugin、archive module、OAuth validator module 和 injection point 等 PG18 扩展点。
- [ ] **EXT-11** 兼容常用扩展的现实前提：pg_stat_statements、auto_explain、pg_trgm、btree_gin/gist、hstore、citext、uuid-ossp、postgres_fdw 等。

## 20. 工程质量、发布和生产化

- [x] **ENG-01** 修正文档事实漂移：README、RELEASE-NOTES、CHANGELOG、feature-gaps 和 production-status 中存在互相冲突的 PASS 数、版本和“已完成”描述。
- [x] **ENG-02** 修正版本单一事实源：`version.h` 的 `MAJOR/MINOR/PATCH` 当前仍是 0/1/0，而字符串与 CMake 是 0.2.0。
- [ ] **ENG-03** 每个 release 必须在 clean worktree、固定 compiler/dependency、真实 TLS 构建上完成全量测试并保存机器可读报告。
- [ ] **ENG-04** 单元测试之外增加 SQLLogicTest、PostgreSQL regression/isolation test 移植、ORM suites 和随机 differential SQL。
- [ ] **ENG-05** parser/expression/protocol/WAL/page/catalog/backup 输入 fuzzing，以及 corpus/minimization。
- [ ] **ENG-06** fault injection 覆盖每个 write/fsync/rename/unlink/alloc/thread/lock/WAL point，配合 power-cut 模拟和恢复不变量检查。
- [ ] **ENG-07** TSAN/ASAN/UBSAN/LSAN、debug assertions、不同优化级、GCC/Clang 和 32/64-bit/endianness 策略持续运行。
- [ ] **ENG-08** 并发 history checking：事务隔离用 Elle/Jepsen 风格验证，而不是只看测试返回 PASS。
- [ ] **ENG-09** 长时间 soak、连接风暴、锁风暴、checkpoint/vacuum/backup/DDL/DML 混合、磁盘慢/满/错误测试。
- [ ] **ENG-10** pgbench/TPC-C/TPC-H/TPC-DS 与 PostgreSQL 同硬件基准，记录吞吐、p50/p95/p99、WAL 放大、CPU、内存、I/O。
- [ ] **ENG-11** crash-compatible upgrade/downgrade、catalog migration、rollback plan 和长期 on-disk compatibility 测试。
- [ ] **ENG-12** 发布二进制、包管理、容器、SBOM、依赖/CVE、签名、reproducible build 和支持平台矩阵。
- [ ] **ENG-13** 安全评审：密码学、协议、SQL 权限、文件路径、extension/load、backup/restore、DoS 和敏感日志。
- [ ] **ENG-14** 明确 SLA、支持范围、已知限制、数据恢复手册、备份恢复演练和 incident response。
- [x] **ENG-15** 建立兼容版本策略：是“接受 psql 的自有 DBMS”，还是“PostgreSQL 18 行为兼容”；两种目标的验收标准完全不同。

## 21. 非 PostgreSQL 语法和行为偏移

这些能力可以作为项目扩展保留，但必须放入显式 compatibility mode，不能混入 PostgreSQL 模式：

- [x] **DIV-01** `USE DATABASE`：PostgreSQL 连接建立后不能用 SQL 切换 database。
- [x] **DIV-02** `REPLACE INTO`：MySQL 语法；PostgreSQL 使用 `INSERT ... ON CONFLICT`。
- [x] **DIV-03** `LOAD DATA INFILE`：MySQL 风格；PostgreSQL 使用 `COPY`/psql `\copy`。
- [x] **DIV-04** `SELECT ... INTO OUTFILE`：MySQL 风格；PostgreSQL `SELECT INTO` 是建表。
- [x] **DIV-05** `DESC`/`DESCRIBE`、`VIEW TABLE`、`VIEW DATABASE`、`SHOW USERS/ROLES/POOLS` 等是项目命令或客户端元命令风格。
- [x] **DIV-06** `AUTO_INCREMENT`、unsigned integers、`TINYINT`、`DATETIME`、`BLOB`、`NCHAR/NVARCHAR`、`BINARY/VARBINARY` 是兼容别名或非 PG 类型。
- [x] **DIV-07** `CREATE FULLTEXT INDEX`、`CREATE HASH INDEX` 等快捷语法不是 PostgreSQL 的标准写法；PG 使用 `CREATE INDEX ... USING ...` 和 operator class。
- [x] **DIV-08** `CREATE ASSERTION` 被当作 compatibility object 接受，但 PostgreSQL 18 自身也没有实现 SQL assertion；项目当前更没有约束运行时。
- [x] **DIV-09** 普通 SQL 形式的 `CREATE/DROP REPLICATION SLOT` 和 `SHOW LOGICAL ...` 是项目接口；PostgreSQL 通过 replication protocol 或系统函数管理/消费槽。
- [x] **DIV-10** `DUMP`、`BACKUP DATABASE`、`RESTORE DATABASE`、`RESTORE ... PITR`、`CLEAR PLAN CACHE` 是项目命令，不是 PostgreSQL SQL reference 命令。
- [x] **DIV-11** `SET GLOBAL` 和部分 `SHOW` 命令是 MySQL 风格，不是 PostgreSQL GUC 语法。
- [ ] **DIV-12** 内置 PgBouncer 风格连接池、TDE、`BACKUP/RESTORE DATABASE` 是项目扩展，不等于 PostgreSQL 核心同名能力。
- [x] **DIV-13** 默认端口、数据目录和启动 CWD 状态布局与 PostgreSQL 不同，应避免给工具造成“这是 PostgreSQL cluster”的假象。
- [x] **DIV-14** 对仅保存兼容记录的命令返回“created/loaded/altered”会误导用户；应改为 feature-not-supported，直到运行时真正存在。

## 22. 建议实施顺序和验收门

### Gate 0：先定义目标，不再按关键词追功能

1. 冻结一份 PostgreSQL 18 compatibility contract。
2. 建立 183 命令和各功能族的自动化 matrix。
3. 建立 PostgreSQL differential、isolation、crash 和 protocol 测试框架。
4. 所有未实现骨架统一返回 `0A000`，禁止“保存记录后报告成功”。

### Gate 1：统一内核执行边界

1. 完成 binder/type inference/rewrite。
2. DQL/DML/utility 全部 typed AST 执行。
3. 结构化 result/error/SQLSTATE。
4. 删除 legacy 文本结果和语义 rewrite。

验收：复杂 SQL 不进入字符串 fallback；协议层不解析 `std::cout`；同一语句 CLI/协议结果完全一致。

### Gate 2：catalog 和事务化 DDL

1. catalog 关系化、MVCC/WAL 化。
2. object/owner/ACL/dependency 全集。
3. DDL 无隐式提交并支持 savepoint/crash recovery。
4. schema/search_path/temp/partition/trigger/view 统一 catalog 驱动。

验收：随机 DDL transaction 在任意 kill point 后与 PostgreSQL 的可见对象集合一致。

### Gate 3：存储、索引和 MVCC 正确性

1. XID/MultiXact/freeze/autovacuum。
2. 完整 SSI/predicate lock。
3. 所有索引使用真实 AM、WAL、vacuum、concurrent build。
4. work_mem spill、resource owner、故障注入。

验收：隔离测试、长稳、故障注入、索引一致性和恢复矩阵全部通过。

### Gate 4：协议、安全和工具兼容

1. protocol/COPY/binary/SSL/cancel/pipeline。
2. ACL/RLS/SECURITY DEFINER/认证。
3. libpq/psql/JDBC/ORM matrix。
4. catalog/information_schema/monitoring/tooling。

验收：主流 PG client 和 ORM 无项目专用分支即可完成迁移、事务、COPY、DDL 和 introspection。

### Gate 5：备份、复制和 HA

1. base backup/manifest/incremental/PITR 多 timeline。
2. physical streaming/hot standby/sync replication/slots。
3. WAL-based logical decoding/pgoutput/subscription。
4. promote/rewind/failover 演练。

验收：持续写入下完成备份恢复、主备切换、故障重加入，满足明确 RPO/RTO 且无数据分叉。

### Gate 6：扩展生态和高级性能

1. extension/fmgr/hooks/background worker/FDW/PL。
2. 完整统计/path planner、parallel、AIO、JIT。
3. pg_upgrade、发布包、长期兼容和安全审计。

真正做到 PostgreSQL 等级是一个多年、多人、持续验证的数据库工程，不应继续沿用旧文档中“单人几十周即可完整对齐”的估算。

## 23. PostgreSQL 官方对标来源

2026-10-01 第917项 source `f469eed0`，D合并 `46e5969e`：BIGINT／NUMERIC SUM经serial／parallel GroupAggregate的15位double输出丢精度，旧915强wire及actual failed=1真实保留；legacy三聚合路径另仍执行int64累加，旧Table定向signed-overflow sanitizer在MAX＋MAX真实报runtime error／exit1。按真实scalar column type选exact Numeric accumulator，BIGINT超int64、NUMERIC全部小数位与NULL维持；float输入保留原floating路径。legacy删除未使用的有符号累加，补有效数值count及NULL跳过，让flat／group／grouping sets都用精确结果。新native覆盖三legacy API、serial／parallel两种group shape、真实300输入worker分组、最小值和NULL；最终10 C++、专项、8wire邻居／完整协议及9不同actual均exit0，独立Table signed-overflow instrumentation重跑专项亦exit0（不是全库UBSan／ASan）。真实PG18.6相同强oracle全部通过，无numeric tolerance放宽。初版去掉legacy累加后没有同步SUM count，native及完整协议SUM=0真实失败，已修正并全部重跑闭合，原日志保留。两个CPP-only真实重编＋916Dml／914Network／908正式其他对象冻结组合，逐个cmp source，无header／layout／disk变更，不称独立冷构建。组合应430 C++／159E2E／428actual；根／A仍冻结2d2db1d0（到915／916，429／158／427），根正式production exit0，A完整脚本运行，之后再以同源正式binary跑427全差分，不混入917。完整numeric／float语义、expression aggregate、overflow／binary、907temp SERIAL及其余总清单未完成。913真实PG已核实CHAIN须显式block且END／ABORT／WORK／TRANSACTION／注释分割选项合法；新的chained-origin abort READ ONLY恢复差异单独留918继续。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user，不push，Actions禁用。

2026-10-01 第915项 source `5f19850a`，D合并 `c3882b66`（含独立916前置 `3eb708e0`）：合法BIGINT INT64_MIN与INF失败／NULL sentinel冲突，原INSERT／UPDATE22023、原native insert abort134保留。storage内部改用optional checked decimal；v2 heap依真实NULL bitmap决定NULL，保留旧nullable无bitmap格式的sentinel限制且不做disk迁移。materialized OLD／NEW、typed matcher、FK referenced image、RETURNING／logical image及UPDATE／INSERT／DELETE undo index提取绑定各自NULL bitmap；真实强wire曾依次复现WHERE漏行、UPDATE RETURNING空值、负谓词legacy回退（独立916）及SP restore后secondary index丢键致DELETE58030，失败日志全部保留。最终真实PG18.6相同完整wire oracle、专项、15 C++（12邻居＋3 rollback heap／WAL failure）、10wire邻居、完整协议和9个不同actual case均exit0；MIN／MAX／NULL、BETWEEN／IS NULL、PK duplicate、secondary IndexScan、NULL↔MIN UPDATE、精确RETURNING、SP恢复后删除与COPY验证通过，故障注入的rollback incomplete诊断为既有测试预期且全exit0。Table CPP-only重编，逐个cmp源文件后重链916Dml／914Network／908正式真实对象，无既有header／layout／disk改变，不称独立冷构建或整库sanitizer证明。首次native夹具误用API／未清空追加输出vector的问题已独立纠正，原失败记录不冒充产品复现；强wire断言未放宽。组合应429 C++／158E2E／427actual，正式生产／整套／全差分需新冻结验收，旧9f的426／153／424全exit0不代替它。SUM／AVG精确大值、legacynullable格式、其他codec／numeric overflow与完整TYPE-02及其他族仍未完成；907temp SERIAL和913无block CHAIN继续。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user，不push，Actions禁用。

2026-10-01 第916项 source `3eb708e0`，D合并 `660f10d1`：parser把负整数／显式正整数条件表示为UnaryOp，DML bridge原只接受LiteralExpr而退回旧路径，UPDATE／DELETE漏RETURNING且可误改／误删不匹配行；旧914强wire及actual failed=1均保留。新增严格的单层signed integer literal绑定，只接受未加引号十进制数字token并保留字符串大小／signed spelling（INT64_MIN不先转换positive int64），不把cast／text／其他表达式误折叠。真实PG18.6 oracle、专项wire、10 C++、9邻居／完整协议、9个不同actual case全部exit0；负条件、显式+条件、AND、quoted column、NULL不匹配、exact affected tag及全部未匹配行保持均校验。DmlExecutor CPP-only重编＋914Network／908正式其他对象冻结组合，source逐个cmp，无header／layout／disk变更，不称独立冷构建。最初邻居选择使用不存在case而exit1已保留，最终实存9case重跑无allowlist通过。与915存储修复的组合仍在验：915新wire又发现savepoint恢复时BIGINT最小值secondary index丢键（DELETE58030），已保留原失败并继续修复；915尚未提交。根9f完整426／153／424全exit0仅属到909／908冻结基线，不含916。其他numeric predicate、legacy fallback、SUM溢出、907temp SERIAL、913无block CHAIN及其余总清单未完成，不push，Actions禁用。

2026-10-01 冻结 `9f86fa62`（source `7baa97e9`，仅到909／908）的正式验收已全部闭合：根正式production真实全header重编exit0，A完整脚本426/426 C++及153/153协议/E2E exit0（`/tmp/dbms-tests-abort-cleanup-908-909.log`），根同源正式二进制424/424真实PG18.6差分exit0（`/tmp/dbms-pgdiff-abort-cleanup-908-909.log`，cases=424 failed=0，424个不同[OK]）。这份全量结果不包含后来911／912／914的source；三项已各自定向验收，组合正式全量待下一冻结轮次。915仍在隔离修复BIGINT最小值：原INSERT／UPDATE拒绝、UPDATE RETURNING丢值及DELETE typed matcher漏行均保留失败记录，尚未提交或计通过。907临时SERIAL及其他未完成族继续。总账仍273：24 complete／138 partial／96 unverified／15 deferred_by_user，不push，Actions禁用。

2026-10-01 第914项 source `462ceffe`，D合并 `892664fa`：COPY scalar SMALLINT／INT／BIGINT非法输入原22023而非22P02，旧911强wire exit1保留。新增独立IntegerTextCodec，在COPY field boundary按真实signed width解析并canonical decimal；非法syntax22P02、超范围22003，保留PG decimal／0x／0o／0b、前导sign、单underscores（base prefix后可单underscore）、周围whitespace和small-overflow/trailing-invalid与huge-overflow的错误优先次序，NULL optional与array field不误转。真实PG18.6完整codec wire oracle、项目专项、8 C++（含新unit）／单独ASan＋UBSan、9邻居／完整协议及9不同actual全exit0；late-row失败清掉前行但原表已提交数据保持，CopyDone后I／无COPY CommandComplete，valid边界与NULL bitmap保持。Network CPP-only＋新增纯header helper与冻结组合真实objects重链，无既有类layout或disk变更，不称独立冷构建。该codec native可解析INT64_MIN，但storage仍以INF sentinel拒绝该值；独立915 originalactual已cases=1 failed=1，INSERT／UPDATE22023及missing row均保留，不把codec测试冒充完整BIGINT runtime。普通Bind／CLI／其他COPY类型、binary与codec族仍未完成。组合应427 C++／156E2E／425actual；根/A9f仍冻结426／153／424，本轮C++已426/426通过，正式E2E正在运行，随后才跑424全量差分；907临时SERIAL未修，其他族继续。

908隔离正式组合已快进95b20267并真实增量重编main／Network，五wire（911／912／908／903／COPY）、912actual和完整协议均exit0；保留原194正式binary于build/dbms_subtransaction_release_formal_19461193，组合95正式binary另存build/dbms_savepoint_copy_chain_formal_95b20267。这是独立正式脚本真实对象，不是手工cache移植；尚不含914。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user，未push，Actions禁用。

2026-10-01 第911项 source `257beec6`，D合并 `dc67d9d2`：COPY行错误已发ErrorResponse但尚等CopyDone／Sync时仍持有旧tuple及transaction advisory locks，原912强wire在该精确阶段55P03／exit1保留。receiveCopyIn在发送错误前调用与普通SQL共享的USER SP／top-level abort；COPY guard改用结构性internal statement SP（碰撞后用实际返回名），同步notification／advisory快照。已被USER rollback或top abort移除的guard不再发ROLLBACK TO／RELEASE；显式failed block继续E／25P02且再COPY不进入G，SESSION ownership保留。提前COPY校验／gate错误也走abort，direct SP异常转结构化错误，snapshot恢复绑定连接当前live Session而非短租已归还对象。最终真实PG18.6四组合（Simple／Extended×TOP／USER）在CopyDone／Sync发送前的锁／advisory、earlier write、已copy前行原子undo及恢复全部通过；10 C++、专项、8邻居／完整协议及9不同实际case全exit0。Network CPP-only＋冻结908真实其他对象及912真实main重链，source逐个cmp，header不变，无disk改变，不称独立冷构建。最初invalid integer fixture发现独立22023代替22P02，留待914；换duplicate-PK fixture的PG在批量flush前不发送该错误，原reference timeout保留，不能套用错误的即时E预期。最终NOT NULL fixture在PG与本项目均是23502并真实复现未释放锁；未放宽SQLSTATE／锁断言。Parse／Bind等一般protocol-error abort、完整portal／SubXID／resource-owner以及codec／binary／backpressure族仍未完成。组合应426 C++／155E2E／425actual，根/A仍冻结9f验收426／153／424，不含911／912；907temp SERIAL、914 COPY整数、915 BIGINT最小值sentinel冲突继续，不push、Actions禁用。

2026-10-01 第912项 source `f7ff4ac3`，D合并 `146aea0b`：正常COMMIT／ROLLBACK AND CHAIN会在engine结束事务时清掉READ ONLY，随后新事务错误接受INSERT（旧强wire exit1、actual cases=1 failed=1保留）。main在结束前捕获mode，并仅为新chain恢复；SET TRANSACTION READ ONLY同样继承。真实PG强oracle还证明顶层abort后chain恢复read-write，而仅user subtransaction失败时parent READ ONLY保持，两例均纳入最终断言，不能笼统继承failed block最初的mode。专项wire、8邻居（含908即时锁释放／COPY／deferred）、完整协议、专项＋8不同actual全部exit0。main CPP-only重编，逐个cmp其余source／header并重链冻结908正式真实production对象；无header／layout／disk改变，不称独立冷构建。最初自有夹具patch拼接SyntaxError和缺失case选择失败保留，不冒充旧产品复现；修正夹具后真实旧wire／actual独立失败，未放宽oracle。新组合应426 C++／154E2E／425actual；根/A仍冻结9f的426／153／424本轮正式验收，不含912。没有active block的AND CHAIN 25P01、链选项lexer／别名及unsupported DEFERRABLE仍未完成，不勾整个TXN族；907temp SERIAL仍未修，911 COPY资源abort路径继续。

2026-10-01 冻结494正式全量闭合：424/424 C++、151/151E2E和同源423/423 PG18.6实际差分（`/tmp/dbms-pgdiff-savepoint-gap-serial-null-903-906.log`，cases=423 failed=0）均exit0。随后根与A快进并冻结 `9f86fa62`（source `7baa97e9`，新增909／908），正式production全重编及426 C++／153E2E正在真实运行，之后才启动该正式binary的424全量差分；旧423不能替代新source验收。912正常COMMIT／ROLLBACK AND CHAIN的READ ONLY丢失已保留原wire误成功INSERT和actual failed=1，PG完整强oracle通过；此前夹具拼接SyntaxError和不存在选择项失败不计旧产品证据，原日志保留。912隔离修复及测试继续，不在9f本轮全量中；907temp SERIAL仍未修。

2026-10-01 第908项 source `19461193`，D合并 `7baa97e9`：三连接真实PG18.6 schedule确认错误必须在ErrorResponse之前释放active user savepoint之后的tuple／transaction advisory locks；没有user SP时必须立即释放全部transaction资源，而SESSION advisory lock不随abort释放。旧84强wire在未发送ROLLBACK TO时保留row2锁而exit1；初版只清理subtransaction的Top-level advisory断言亦exit1，原日志保留。新增latestUserSavepoint结构性查找，Network在普通SQL错误边界恢复最近USER SP或物理abort无SP事务，并同步notification／advisory transaction资源；协议仍E／25P02直至客户端恢复，先前用户SP工作和锁保持。最终独立真实production全重编＋Network增量、12 C++、最终三连接PG oracle／本项目强wire、8邻居／COPY／完整协议、9不同actual case均exit0。v1本地Table括号编译错误已修正且原失败日志保留；两个邻居选择脚本的不存在文件名错误亦保留，不算产品失败或成功用例，最终使用实际文件名重跑通过。非virtual方法无新增对象字段或磁盘格式更改。COPY wire内部SP仍是legacy USER分类，Parse/Bind等非SQL错误边界、真正SubXID/resource owner及其余savepoint族未完成，不勾整个TXN-05。

正式冻结494基线：根production全header真实重编exit0；A完整脚本424/424 C++、151/151 E2E、exit0已闭合（`/tmp/dbms-tests-savepoint-gap-serial-null-903-906.log`）。随后根494同正式binary的423全量PG18.6差分已启动，尚在运行，不能声称全量结果；根/A仍冻结494，不含909／908。含两项下一轮应426 C++／153E2E／424actual。907temp SERIAL原failed=1尚未修；912正常AND CHAIN丢失READ ONLY已真实PG确认，接续独立修复。总账273：24 complete、138 partial、96 unverified、15 deferred_by_user；未push，Actions禁用。

- [PostgreSQL 18.6 Documentation](https://www.postgresql.org/docs/18/)
- [PostgreSQL 18 SQL Commands：183 条命令目录](https://www.postgresql.org/docs/18/sql-commands.html)
- [The SQL Language](https://www.postgresql.org/docs/18/sql.html)
- [Concurrency Control](https://www.postgresql.org/docs/18/mvcc.html)
- [Indexes](https://www.postgresql.org/docs/18/indexes.html)
- [Server Administration](https://www.postgresql.org/docs/18/admin.html)
- [Monitoring Database Activity](https://www.postgresql.org/docs/18/monitoring.html)
- [Frontend/Backend Protocol](https://www.postgresql.org/docs/18/protocol.html)
- [High Availability, Load Balancing, and Replication](https://www.postgresql.org/docs/18/high-availability.html)
- [Logical Replication Architecture](https://www.postgresql.org/docs/18/logical-replication-architecture.html)
- [Extending SQL](https://www.postgresql.org/docs/18/extend.html)
- [PostgreSQL 18 release notes](https://www.postgresql.org/docs/18/release-18.html)

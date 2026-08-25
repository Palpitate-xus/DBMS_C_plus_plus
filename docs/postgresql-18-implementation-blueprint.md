# DBMS_C_plus_plus 对标 PostgreSQL 18.6：逐项完整实施蓝图

> 编制日期：2026-08-23
> 源码基线：`5a31aa3` 加当前动态工作区
> 对应审计：[postgresql-18-gap-audit.md](postgresql-18-gap-audit.md)
> 覆盖口径：审计中的 16 个 P0、243 个能力差距和 14 个非 PostgreSQL 偏移，共 273 项；另给出 183 条 SQL 命令的闭环办法。

## 1. 目标和不可打折的完成定义

目标是 **PostgreSQL 18 行为兼容**，不是复用若干 PostgreSQL 关键字。除项目显式扩展模式外，同一输入在 PostgreSQL 18.6 与本项目上必须得到相同的：

1. 解析结果和对象解析，包括 identifier folding、`search_path`、类型推导、collation 和依赖。
2. 可见行、列顺序、NULL、类型 OID/typmod、文本/二进制值、command tag 和异步消息。
3. 成功/失败边界、SQLSTATE、severity、position、detail、hint、context 和事务状态。
4. 并发可见性、锁等待/冲突、触发顺序、权限/RLS 和崩溃恢复后的持久状态。
5. catalog 可观察结果，以及 dump/restore、客户端、复制和运维接口行为。

“语法可解析”“把定义写入 `.pg_compat_objects`”“单线程 happy path 成功”都不能关闭差距。每项只有在 PostgreSQL differential、并发 isolation、协议字节级和必要的 crash/fault-injection 测试全部通过后才能勾选。

本蓝图不要求数据页或 WAL 与 PostgreSQL **二进制同格式**；它要求 SQL、协议、catalog 和运维行为兼容。磁盘格式采用本项目可演进的版本化格式，通过导入/升级工具保真。若未来要求物理文件级兼容，必须改为 PostgreSQL fork，这不应和当前独立内核路线混在一起。

## 2. 输入输出无损契约

### 2.1 输入必须原样保留

- 新增 `src/sql/SourceText.{h,cpp}`：保存客户端原始字节、解码后的 UTF-8、`client_encoding` 和 byte/character offset 映射。parser 只引用 token span，不改写源串。
- 新增 `src/sql/Lexer`、`GrammarParser`、`ParseAnalyze`、`Binder`、`Rewriter`。`rawSql` 只用于错误位置、审计和 query jumble；执行不得再对它做 `substr/find/tokenize` 二次解释。
- protocol Bind 参数以 `{type_oid, format, is_null, bytes}` 保存；不得先转成带引号 SQL 再重新 parse。SQL 字面量和 Bind 参数走同一类型的 `typinput/typreceive`，但保留各自错误位置规则。
- 所有名称保存原始拼写和规范化名称；quoted identifier 不折叠，未 quoted identifier 按 PostgreSQL 规则折叠和截断，并发出相同 notice。

### 2.2 内部值不能再是“空格分隔字符串”

新增 `src/types/Datum.h`、`TypeDescriptor.h`、`TupleDesc.h` 和 `src/executor/TupleSlot.h`：

```text
Datum = fixed-width scalar | varlena reference | composite reference
Value = {Datum datum, bool is_null, Oid type_oid, int32 typmod, Oid collation}
TupleSlot = {TupleDesc descriptor, Value[] values, optional ItemPointer}
```

固定宽类型按平台无关格式存储；varlena 使用 statement/tuple memory context 管理，NULL 独立于零长度值。`IOperator::next(std::string&)` 先加 `next(TupleSlot&)` 适配器，再逐算子迁移，最后删除字符串接口和 `MaterializedRowsOp` 的字符串形态。热路径不为每列创建 `std::string`。

### 2.3 输出只编码一次

新增 `src/executor/ExecutionResult.h`：

```text
ExecutionResult = RowStream(TupleDesc, next) | CommandResult(tag, affected_rows) |
                  CopyStream | EmptyResult
DbError = {sqlstate, severity, message, detail, hint, position, internal_position,
           internal_query, where, schema, table, column, datatype, constraint}
```

- CLI sink 调用类型的 `typoutput`；PostgreSQL protocol sink 调用 `typoutput`/`typsend`；COPY sink 调用 COPY codec。三者消费同一 `ExecutionResult`，不得捕获或解析 `std::cout`。
- RowDescription 直接来自 `TupleDesc`，包括 name、table OID、attnum、type OID、typlen、typmod、format。NULL 发送 `-1`，空串发送长度 0。
- command tag 由 utility/ModifyTable 节点产生；notice/notification 独立排队；ErrorResponse 全字段来自 `DbError`。
- 浮点、numeric、date/time、bytea、array、range、record、JSONB 等均只有一套 canonical text/binary codec，协议、COPY、cast、函数 I/O 复用它。

### 2.4 性能硬约束

功能兼容不能靠全量 materialize 或每行文件 I/O 达成。所有实现同时满足：

- scan/join/aggregate/protocol 使用流式 `TupleSlot`；可增加 256～1024 行的 `TupleBatch` 快路径，但逐行和批量路径必须语义相同。
- statement/portal/transaction `MemoryContext` 与 `ResourceOwner` 负责统一释放；长查询的内存受 `work_mem`/`maintenance_work_mem` 限制，超限 spill，而不是 OOM。
- catalog 走 syscache/relcache，失效消息按 OID 定位；禁止逐行读 CSV/JSON/sidecar。
- buffer、WAL、锁、统计采用分区锁或无锁计数；禁止新的进程全局大锁。WAL 采用 group commit，索引采用 page-level WAL。
- 每项都建立 microbenchmark 和端到端 benchmark。默认门槛：相同 plan 下相对 PostgreSQL 18 的 p95 延迟不高于 2 倍、吞吐不低于 50%，且不能出现随数据量增长而额外增加一个复杂度阶；最终发布门槛再按目标 workload 收紧。

## 3. 目标模块和迁移方式

| 层 | 在当前代码上的落点 | 最终职责 |
|---|---|---|
| SQL 前端 | 保留 `src/parser/ast.*`，新增 `src/sql/*`，逐步收缩 `main.cpp` | lexer、grammar、analyze、bind、rewrite，输出 fully typed query tree |
| 类型/fmgr | 扩展 `src/catalog/type_registry.*`、`src/types/*`，新增 `src/fmgr/*` | Datum、I/O、cast、operator/function resolution、调用 ABI |
| catalog | 迁移 `src/catalog/*` 和 `TableManage` 虚拟目录 | MVCC/WAL system relations、OID、dependency、syscache/relcache |
| planner | 重构 `src/executor/ExecutionPlan.*` 为 `src/planner/*` + plan nodes | RelOptInfo/Path、统计、成本、plan cache 和 invalidation |
| executor | 由 `IOperator` 迁移到 typed `TupleSlot` | 流式执行、spill、interrupt、instrumentation、ModifyTable |
| transaction | 扩展 `src/transaction/*` | snapshot、CLOG/SubTrans/MultiXact、lock、SSI、resource owner |
| storage | 扩展 `src/storage/*`，拆分 `TableManage.cpp` | relation/fork/page/buffer/FSM/VM/TOAST/large object/control file |
| WAL/recovery | 由 `storage/WAL.*` 拆出 `src/wal/*`、`src/recovery/*` | resource manager、redo、checkpoint、timeline、归档和恢复 |
| protocol | 扩展 `src/network/PostgresProtocol.*`，拆除本地字符串 `QueryResult` | 完整 v3/v3.2、COPY、cancel、pipeline、replication |
| HA/backup | 替换 `ReplicationManager` 骨架和写路径 `LogicalDecoder` | walsender/receiver、standby、WAL decoding、apply、backup |
| process/ops | 扩展 `src/process/*`、`src/common/Config.*` | postmaster/backend、GUC、信号、统计、日志、资源治理 |

迁移采用 strangler pattern：旧入口在每个 statement family 完成前仍可存在，但新旧路径绝不能同时宣称拥有同一语义。先引入 typed 适配器和 differential test；每迁移一个 family，就删除对应字符串 fallback。任何尚未迁移的 PostgreSQL 命令统一返回 `0A000`，不写兼容记录。

## 4. 实施批次和发布门

1. **B0 兼容基线**：冻结 PG 18.6 Docker/reference cluster，生成 SQL/protocol/catalog/isolation 差分用例；建立 273 项机器可读 manifest。
2. **B1 typed I/O**：`Datum`、`TupleSlot`、`ExecutionResult`、`DbError`、类型 codec；CLI/网络共享 sink；移除 `OutputCapture` 的结果职责。
3. **B2 analyze/rewrite**：正式 lexer/grammar、binder、cast/function resolution、rewrite、权限标记；DQL/DML 全进 typed plan。
4. **B3 catalog/DDL**：system relation、dependency、syscache、事务化 DDL；迁移 sidecar，关闭隐式提交。
5. **B4 storage/transaction**：control file、locator/fork、page WAL、MultiXact/freeze、完整 lock/SSI、autovacuum 和所有索引 durability。
6. **B5 planner/executor**：Path/CBO、统计、spill、参数化/parallel/AIO、完整 node 和 EXPLAIN。
7. **B6 protocol/security/ops**：COPY、portal、cancel、binary I/O、TLS/auth、ACL/RLS、监控、工具兼容。
8. **B7 backup/HA**：多 timeline recovery、base/incremental backup、physical/logical replication、subscription、rewind。
9. **B8 extension/ecosystem**：fmgr、AM、hooks、worker、FDW、PL、extension upgrade 和常用扩展。
10. **B9 release proof**：长期故障/并发/升级/安全/性能验证。只有这一批通过后才能使用“PostgreSQL 18 compatible”表述。

每批都提供 feature flag 仅用于开发；公开兼容模式不得静默降级。磁盘格式迁移使用 `dbms_upgrade --check/--link/--copy`，先离线校验、写新 control/catalog、逐 relation 转换并保留 rollback manifest；原目录在成功原子切换前保持可恢复。

## 5. P0 差距逐项方案

### P0-01 统一 SQL 执行管线

在 `main.cpp::execute` 前建立唯一 `QueryPipeline::execute(SourceText, Session)`；Parser 输出 AST，Analyzer 输出 typed query tree，Rewriter 输出权限/RLS/view/rule 展开树，Planner 输出 plan，UtilityExecutor 只处理 utility。先给 legacy 分支加计数和 `0A000` 边界，再按 statement family 迁移并删除。原始输入只通过 span 引用；计划缓存使用 normalized jumble，不改原文。验收为全量 SQL corpus 中 legacy counter 恒为 0，CLI/协议结果一致；parse/analyze 的 p95 不高于 PG 基线 2 倍且 prepared execution 不重复 parse。

### P0-02 结构化结果与错误

落地第 2 节 `ExecutionResult/DbError`，将 `DmlResult`、`PlanExecutionResult`、网络层局部 `QueryResult` 合并；先为旧字符串 executor 写只读适配器，禁止新增调用，再逐算子返回 `TupleSlot`。错误用异常边界或 `Expected<T,DbError>` 单向传播，EOF 与 error 分离。验收覆盖 NULL/空串/空格/NUL/多字节、所有 command tag、SQLSTATE 和 error fields 的 wire capture；流式百万行时内存保持有界。

### P0-03 catalog 唯一事实源

以 `CatalogManager/CatalogService` 为迁移入口，bootstrap 固定 OID 的 system relations，再把 schema/compat/ACL/cast/publication 等 sidecar 导入 catalog。所有 lookup 只能经 `CatalogAccessor`，虚拟 view 改为查询真实 catalog，旧文件只由一次性 importer 读取。syscache/relcache 以 OID 和 `(namespace,name)` 为键并接收事务提交 invalidation。验收随机 DDL 后磁盘、内部 lookup、`pg_catalog`、dump 四者一致；catalog 热查不产生文件系统调用。

### P0-04 完整事务化 DDL

删除 `DdlExecutor::checkAndImplicitCommit`；`DdlTransaction` 扩成 catalog tuple MVCC、pending file create/unlink、relcache invalidation 和 resource owner undo。CREATE 先写 temp/新 relfilenode，commit 原子发布；DROP 在 commit 后安全 unlink；ALTER rewrite 使用新 relfilenode swap。按 PG lock level 获取 heavyweight lock。验收在 BEGIN/savepoint/2PC 与每个 fsync/rename kill point 差分对象集合、文件泄漏和 WAL replay。

### P0-05 完整持久性证明

为 heap/index/catalog/FSM/VM/TOAST/sequence/slot/control/config 定义 WAL owner 和 flush dependency；每个 dirty page 携带 page LSN，写数据页前保证 WAL durable，commit tag 只在 commit record flush 后返回。建立 fault-injection registry 覆盖 write/fsync/fdatasync/rename/unlink。验收自动枚举 kill point 后重启，并验证 committed 全在、aborted 全无、索引/约束/catalog 一致；性能记录 WAL bytes/txn、fsync/txn 和 group commit 合并率。

### P0-06 索引 WAL 重构

废弃 `RM_INDEX_ID` 整文件 before/after image；给每个 AM 定义 meta/page insert/delete/split/vacuum WAL record 和幂等 redo，首次 checkpoint 后修改写 FPI。索引页使用 buffer content lock、page LSN 和 checksum；sidecar 先转 relation fork。验收逐 AM 做 split 中每个指令点崩溃、redo 两次幂等、heap-index cross-check；WAL 放大随修改页数增长而非索引文件大小增长。

### P0-07 完整 MVCC/SSI

在 `LockManager` 增加 SIREAD relation/page/tuple/index-range lock，维护 rw-conflict 图、pivot/dangerous-structure 检测、safe snapshot 和 read-only DEFERRABLE wait。所有 scan AM 报告访问范围，空结果也加 gap lock；commit 清理按 oldest active serializable horizon。验收移植 PostgreSQL isolation specs 并用 history checker 验证 serializability；predicate lock 内存有升级阈值和 spill/拒绝策略。

### P0-08 XID 生命周期

新增 `pg_control` 风格 nextXid/checkpoint horizon，SLRU 化 CLOG/SubTrans/MultiXact，tuple 支持 MultiXact xmax；VACUUM 计算 global xmin、catalog xmin、slot xmin，执行 freeze/all-frozen。启动和分配 XID 前执行 wraparound stop limit。验收用缩小 XID 位宽加速 wrap 测试、长事务/slot/standby horizon 测试；SLRU 分页缓存避免每 tuple 文件 I/O。

### P0-09 单实例并发与故障隔离

采用 postmaster + 独立 backend 进程：postmaster 持有 data-dir lock、shared memory、监听 socket 并监督子进程；buffer/lock/WAL/proc array 放共享内存，backend crash 触发 sibling terminate、recovery 后重启。嵌入式单进程模式只用于测试且不得共享目录。验收双启动互斥、backend SIGKILL、postmaster crash、共享锁 owner cleanup；多核吞吐不得被进程全局 mutex 串行化。

### P0-10 真实备份和恢复链

实现 backup start/stop WAL record、checkpoint、backup_label、tablespace map、manifest/checksum；base backup 流式读取 relation 并包含所需 WAL，增量按 block change summary。restore 先验证 manifest，再按 target 重放并创建新 timeline。验收持续写入下 base/incremental/PITR 恢复，逐文件损坏诊断，恢复点事务边界差分；限速与并行复制可配置且不阻塞 checkpoint。

### P0-11 物理复制和 hot standby

新增 `WalSender/WalReceiver/StartupRecovery/StandbySnapshot`，复用 replication protocol CopyBoth；receiver 落盘并 flush 后反馈 write/flush/apply LSN，startup 逐 record redo，reached consistency 后开放只读。promotion 写 timeline history；同步复制在 commit wait queue 等满足 priority/quorum。验收网络断连、级联、slot retention、promotion/rejoin 和冲突取消；sender/receiver 使用流式 buffer，不累计整段 WAL。

### P0-12 wire protocol 合规

扩展 `PostgresProtocol` 为独立 frame codec/state machine；Portal 保存 plan、params、snapshot、row cursor 和 formats，不保存完整结果集。实现 COPY、CancelRequest、Flush/Sync/pipeline、SSL/GSS negotiation、Notification 和 replication mode。验收 psql/libpq/JDBC/ORM matrix 与 golden pcap；百万行 portal/COPY 有 backpressure，内存为 O(fetch-size)。

### P0-13 权限边界统一

Analyzer 为每个 RangeTblEntry/object 记录 required privileges 和 checkAsUser；Rewriter 注入 RLS/security-barrier，Executor 在打开 relation/function/COPY/LO/maintenance 时统一调用 `AclCheck`。SECURITY DEFINER 通过栈式 security context 和受控 search_path，异常自动恢复。验收按对象/角色/列/RLS 建差分矩阵并覆盖 view/trigger/function 旁路；权限缓存按 membership/ACL invalidation。

### P0-14 资源治理

引入 MemoryContext、ResourceOwner、TempFileSet、InterruptToken 和 workload limit；sort/hash/window/aggregate/bitmap/reorder buffer 达到 work_mem 后分区 spill，所有锁等待、I/O、worker 定期 `CHECK_FOR_INTERRUPTS`。磁盘满保持 WAL/数据不变量并返回精确错误。验收超时/cancel/OOM/ENOSPC/EMFILE/慢客户端测试无泄漏或半提交；spill 算法复杂度和 I/O bytes 有基准。

### P0-15 可升级性

`ControlFile` 保存 system identifier、format/catalog version、checksum、block size、feature flags；每版提供 catalog migration 和 relation converter。`dbms_upgrade --check` 检查 extension/collation/endianness，`--link/--copy` 生成可回滚 manifest；旧 v2 仅由只读 importer 读取。验收 N-2→N、失败回滚、崩溃续跑、dump diff 和升级前后 workload checksum。

### P0-16 差分兼容测试

建立 `tests/compat/manifest.yaml`，每项记录 reference SQL、expected rows/types/errors/catalog、isolation schedule、protocol bytes、crash points 和 perf case；runner 同时驱动 PG 18.6 和本项目，规范化仅限 PID/OID 等声明过的非稳定字段。任何差异必须显式 allowlist 且带过期版本。CI 按快速/夜间/发布分层，发布要求 allowlist 为零（项目扩展除外）。

## 6. 183 条 SQL 命令的闭环方案

183 条命令不再各写一个字符串 handler。每条命令在 `tests/compat/sql_commands.yaml` 有独立记录，覆盖 grammar、analyze、owner/ACL、dependency、transaction、lock、command tag、SQLSTATE、catalog 和 dump/restore。实现归入以下可复用 utility family：

| 命令集合 | 完整实现路径 | 关闭条件 |
|---|---|---|
| 全部 `CREATE/ALTER/DROP` | typed utility AST → `UtilityExecutor` → object-specific catalog API → dependency/lock/WAL；EXT/FDW/复制对象必须有真实 runtime | 审计中的 42 ALTER、42 CREATE、43 DROP 每条均有独立 PG differential 和 crash rollback case |
| `SELECT/VALUES` | Analyzer/Rewriter → Path planner → typed executor | QRY/OPT/TYPE/SEC 对应项通过，RowDescription 与 PG 相同 |
| `INSERT/UPDATE/DELETE/MERGE/COPY` | 统一 `ModifyTable`/`CopyFrom/CopyTo`，共享 constraint/trigger/RLS/partition/WAL path | DML/CONS/TXN/PROTO 项通过，所有入口无 legacy fallback |
| transaction/locking/cursor/prepared | `TransactionManager`、Portal、PreparedPlan、LockManager | 状态机、snapshot、SQLSTATE、ReadyForQuery 和并发 schedule 与 PG 一致 |
| `GRANT/REVOKE/SET ROLE/SESSION AUTHORIZATION` | catalog ACL/membership + security-context stack | SEC 全矩阵与依赖/回滚通过 |
| `VACUUM/ANALYZE/CLUSTER/REINDEX/CHECKPOINT` | maintenance dispatcher + progress + interrupt/resource owner | 存储/WAL/索引/VAC/监控项通过 |
| `LISTEN/NOTIFY/UNLISTEN` | transactional shared notification queue + protocol async sink | commit/rollback、跨 backend、queue full 和异步 wire 测试通过 |
| `EXPLAIN` | 对任意 plannable/utility statement 序列化统一 instrumentation tree | 所有 option/format 的 schema 与 PG 差分通过 |
| `IMPORT FOREIGN SCHEMA` | FDW API 调用远端 introspection 后生成 typed utility command | FDW runtime 完整，事务和错误可回滚 |
| `LOAD` | 仅在 fmgr ABI 和受控动态加载完成后启用，否则 `0A000` | 真实加载/卸载、权限、ABI、错误/信号安全测试通过 |

`P/S/X` 只用于现状审计，不作为实现阶段。命令只有“全语义通过”或“明确 `0A000`”两种公开状态。

## 7. SQL 前端、名称解析和语义分析

### SQL-01

用可生成 LALR/GLR 表的 grammar 或经过 corpus 验证的 hand-written grammar 替换二次字符串解析；保留 `ast.h` 但让每个节点含 source span。新增 scanner differential corpus，覆盖全部 183 命令。lexer 单遍 O(n)，token 用 `string_view`；验收原始 query 不变、尾随 token/position/SQLSTATE 与 PG 一致。

### SQL-02

在 Lexer 实现 quoted/U& identifier、U&/E/string、dollar quote、bit/hex、nested comment 和 `standard_conforming_strings` 状态；统一 client encoding 转码和 character position。token 延迟解码以避免复制；按 PG regression 的合法/非法字节和 escape 输出逐字节比对。

### SQL-03

删除 `main.cpp` 的 boolean/array/CASE/ANY/ALL rewrite；Parser 生成专用表达式节点，Analyzer 做类型和三值逻辑转换。原始 literal span 用于错误，deparse 由 AST 产生；表达式编译后复用，避免逐行 parse，结果/NULL 与 PG differential。

### SQL-04

新增 `ParseState/NamespaceItem/RangeTblEntry`，分层解析 relation、column、function、operator、type，记录 lateral visibility、outer reference 和歧义。lookup 批量走 syscache；验收 nested scope、alias shadow、ambiguous/undefined 的 SQLSTATE 和 error position。

### SQL-05

把 `search_path` 解析为 namespace OID 列表，隐式注入 `pg_catalog`、session `pg_temp_N`；所有 qualified/unqualified lookup 复用 Binder。缓存键含 role/temp namespace/path generation；测试跨 schema 同名、prepared plan path 变化和权限结果。

### SQL-06

实现 `NAMEDATALEN=64` 兼容规范：未 quoted UTF-8 identifier 折叠并按字符安全截断，quoted 保留大小写；catalog 使用 OID locator，物理路径不拼名称。名称规范化一次并缓存；比对 notice、collision、rename 和 multibyte 边界。

### SQL-07

在 Analyzer 实现 unknown pseudo-type、type category/preferred type、common supertype 和 `$n` 推断，信息来自 `pg_type/pg_cast`。使用候选剪枝和 memoization；验证 UNION/CASE/ARRAY/VALUES/operator/function 参数类型、OID 和 42P18/42804 等错误。

### SQL-08

把 cast context（implicit/assignment/explicit）、method（function/inout/binary）和 dependency 写入 `pg_cast`；Analyzer 用最短合法 cast path，不允许循环/歧义。built-in path 预计算，catalog invalidation 后重建；验证 CREATE/DROP CAST 立即影响 plan 与执行、binary-coercible 不复制 datum。

### SQL-09

实现候选收集、named/default/variadic 展开、exact/preferred/polymorphic 打分以及 search_path tie-break；procedure/function/aggregate/operator 使用同一 fmgr signature。signature cache 按 namespace generation 失效；差分所有重载歧义、默认值、VARIADIC 和 anycompatible 推导。

### SQL-10

每个 expression 带 input/result collation，Analyzer 按 explicit/implicit/none 规则合并并报 conflict；`pg_collation` 保存 provider、locale、deterministic、version。sort/hash/index path 携带 collation OID；版本变化标记相关索引需重建，验证排序/比较/错误与 PG/ICU 版本。

### SQL-11

Parser 必须消费 EOF；`DbError` 从 token span 计算字符 position，并支持 detail/hint/context/object fields。建立错误码映射表而非统一 `XX000`；错误构造不做昂贵字符串格式化直到发送，回归比对全部负例和事务 abort 状态。

### SQL-12

以 `TupleSlot` 替换 `StorageEngine::vector<string>` 和算子空格行；先提供 schema-aware decode adapter 只读旧 v2 row，再将 heap tuple 存为 typed datum。热路径用 slot/arena 复用，验证空格、delimiter、NULL、empty、NUL byte、Unicode、复合值和百万行内存上界。

### SQL-13

统一 `PreparedStatement{raw, analyzed_tree, param_types, result_desc, dependencies}` 与 `Portal{plan, params, formats, cursor,snapshot}`；SQL PREPARE 只是 session catalog entry，协议 prepared 是同一核心对象。generic/custom plan 按执行统计选择，依赖/GUC/role/path 改变失效；比对 Describe、unnamed replacement、错误时机和 portal suspension。

### SQL-14

新增 copy-on-write `QueryTree`、RTE permission info、rewrite provenance 和 dependency list；view/rule/RLS 展开用深度/循环保护。共享不可变子树减少复制，plan cache 以 dependency generation 失效；差分 view rule、RETURNING、permission check-as-user 和递归错误。

## 8. Catalog、OID、对象和 DDL

### CAT-01

bootstrap `pg_class/attribute/type/proc/namespace/depend` 为普通 heap/index relation，使用 MVCC、buffer、WAL；CSV/sidecar 由 `CatalogImporter` 一次导入并写 migration marker。syscache 避免每次 heap scan；crash/downgrade 测试证明原子切换和 catalog tuple 可见性。

### CAT-02

将 `queryPgCatalog` 虚拟拼装改成 catalog view SQL/内置 scan；DDL、binder、planner 只能依赖 `CatalogAccessor`。启动时做 bootstrap index scan，禁止读取另一套 schema 文件；内部 lookup 与用户 SELECT 同 snapshot，差分列 OID/type/visibility。

### CAT-03

按 PG18 catalog schema 建完整 relation/unique index/toast，先生成声明式 catalog descriptor 再 bootstrap，避免手写结构漂移。按功能批次填 `pg_constraint/index/am/opclass/operator/cast/collation/rewrite/trigger/policy/auth/default_acl/database/tablespace/statistic/replication`；catalog init 和升级由同一 descriptor 生成，逐表对比 `information_schema.columns` 和关键约束。

### CAT-04

OID allocator 持久化 nextOid，bootstrap OID 固定，用户 OID 在 cluster 范围防冲突；所有对象用 ObjectAddress `(classId,objectId,subId)` 引用。实现 reg* I/O 和 qualified deparse；批量 restore 可保留/映射 OID，缓存 OID lookup；验证 dump/restore 后引用、dependency 和 reg* round-trip。

### CAT-05

统一 `pg_authid/acl/comment/security_label/depend/extension`，对象创建默认写 owner/dependency，ALTER OWNER 重写 ACL 并检查 membership。读取走 ACL/syscache；验证所有对象的 owner/comment/label/extension membership 在 rename/drop/restore 后一致。

### CAT-06

DependencyManager 从 root ObjectAddress 做有向图遍历，区分 normal/auto/internal/pin/extension；RESTRICT 先收集并报告，CASCADE 排序执行，循环使用 visited set。按 OID index 查询避免全表扫；比对 NOTICE 顺序、错误对象、subobject drop 和事务回滚。

### CAT-07

DDL catalog tuple 和 physical pending action 注册到同一 transaction/resource owner；WAL 先行，commit 发布 relcache invalidation 和 deferred unlink，abort 删除未发布文件。文件操作批处理 fsync；随机 DDL kill-point 后 catalog/文件/索引无孤儿。

### CAT-08

实现 cluster 级 `pg_database`、template clone strategy、encoding/locale/ICU/collation version/tablespace/connlimit；复制 template 时阻止并发连接并 WAL/manifest 记录。clone 优先 reflink/copy_file_range 后回退 buffered copy；比对 option defaults、权限、template0/1 和失败清理。

### CAT-09

物理表名改 `RelFileLocator`，schema 仅为 `pg_namespace` OID；实现 CREATE/ALTER/DROP SCHEMA、USAGE/CREATE ACL 和 search_path。迁移器解析旧 `schema__table` 并处理碰撞；namespace lookup 有 cache，差分 rename/owner/concurrent object 与临时 schema。

### CAT-10

每个 ALTER TABLE action 先 analyze 并计算 lock/rewrite/dependency，再在一个 utility transaction 执行；类型改变用 `USING` typed expression，新 relfilenode rewrite/swap，no-rewrite action 只改 catalog。支持 ONLY/递归和多 action 顺序；rewrite 流式并可并行读，验证中途错误完全回滚。

### CAT-11

catalog 表示 partition key/bound/default/inheritance；Analyzer canonicalize bound，attach 前用约束证明减少 scan，否则在受控 lock 下验证；planner 做 runtime/startup pruning。partition index/unique 追踪 parent validity；差分 DEFAULT move、concurrent detach/finalize 和跨分区 DML。

### CAT-12

用 inheritance graph 合并列、constraint/default/generated/identity/statistics/ACL，记录 `attinhcount/islocal` 并检测多父冲突。图 lookup 缓存且 DDL invalidation；验证 ONLY、递归 DDL、drop inherited column/constraint 和权限。

### CAT-13

backend 启动分配 `pg_temp_N` OID namespace，temp relation 使用 backend-local locator 和 catalog rows；commit 执行 PRESERVE/DELETE/DROP，2PC 明确拒绝含 temp state。backend exit resource owner 清理；temp catalog/relcache 本地化避免全局 invalidation，差分多 session 同名和异常退出。

### CAT-14

unlogged relation 建 main/init forks；checkpoint 保证 init fork，crash recovery 从 init 重置 main 并清索引，正常 shutdown 保留。WAL 仅记录必要 catalog/init 操作，复制/base backup遵循 PG可见性；验证 crash/non-crash 与 standby 行为。

### CAT-15

sequence 作为 relation + `pg_sequence`，用轻量 sequence lock、cache block、WAL log-ahead 实现；nextval 非事务，setval/currval/lastval session state，OWNED BY dependency。缓存减少锁/WAL频率；差分 rollback/crash/cache gap/cycle/concurrency/identity。

### CAT-16

CREATE VIEW 保存 analyzed query 到 `pg_rewrite`，Rewriter 展开并检测 recursive；判断 simple view 自动可更新，执行 LOCAL/CASCADED CHECK OPTION、security barrier/invoker。不可变 query tree 缓存按 dependency 失效；验证 DML、列权限、RLS、rename/deparse。

### CAT-17

materialized view 保存 typed query、populate flag、dependency 和 heap relation；refresh 普通模式新 relfilenode swap，CONCURRENTLY 要求合格唯一索引并以 diff/upsert 事务应用。使用 maintenance_work_mem/spill；验证未填充访问错误、并发读写和失败回滚。

### CAT-18

`pg_trigger` 引用真实 trigger function OID，Executor 建 BEFORE/INSTEAD/AFTER 与 statement/row event queue；transition table 按 statement tuplestore，constraint trigger 接 deferred queue，按名称排序并处理递归深度。tuplestore 可 spill；比对 OLD/NEW、WHEN scope、RETURNING 后值、savepoint。

### CAT-19

event trigger 订阅 DDL command lifecycle 并提供 command/drop object SRF；rule 保存 action/qualification query tree，由 Rewriter 执行 ALSO/INSTEAD。事件命令列表在 transaction context 中有界累积；比对 firing order、disabled state、递归和回滚。

### CAT-20

cluster `pg_tablespace` 保存 OID/location/owner/ACL/options，data dir 使用受控 symlink/locator；CREATE/DROP 做路径所有权、空目录、跨设备 fsync 和 WAL/backup map。I/O 层按 tablespace 分离并发；验证 rename/drop busy、备份恢复和 ENOSPC。

### CAT-21

COMMENT/SECURITY LABEL 统一以 ObjectAddress+subId 存 catalog，provider validate hook 决定 label；覆盖 PG 对象全集并随 dependency cascade。索引 lookup O(1)；比对 NULL 删除、shared object、column label 和 dump。

### CAT-22

删除 compat object 成功路径；manifest 标记未完成 feature 时，Analyzer/UtilityExecutor 在任何写入前返回 `0A000`。只有对应 catalog+runtime+tests feature gate 完成才注册命令。验证每个原 S/X 命令既不产生文件/catalog，也不改变 transaction，完成后再切换成功结果。

## 9. 数据类型、I/O、函数和操作符

所有 TYPE/FUNC 项共用第 2 节 Datum 与 `TypeCodec{input,output,receive,send,compare,hash}`，类型 OID/typmod/collation 全程保留。每种类型在 text、binary protocol、COPY text/CSV/binary、cast、heap、index、sort/hash 六条路径使用同一实现和 golden vector。

### TYPE-01

实现 base-10000 可变长 `NumericDatum`、weight/sign/dscale/NaN/±Infinity，算术使用 guard digits 并按 PG rounding/overflow；send/receive 严格按 PG numeric binary frame。小值内联、聚合用可复用 accumulator；以 PG 生成的随机高精度向量验证算术、sort/hash/index 和 wire bytes。

### TYPE-02

int2/4/8 使用定宽 little-endian storage+显式字节序 codec，所有算术用 checked operation；float 遵循 PG NaN ordering、Infinity 和 shortest-roundtrip output。执行器专门化常见 cast 避免 fmgr 开销；覆盖边界、除零、溢出、不同优化器/架构和 binary protocol。

### TYPE-03

money 内部用 int64 最小货币单位，输入输出读取 session locale 的 decimal/thousands/currency，算术检查溢出，禁止 double。locale descriptor 缓存；对各 lc_monetary、负数格式、cast/numeric 运算和 8-byte wire/storage 做差分。

### TYPE-04

text/char/varchar/name 使用 varlena，无 65535 人为上限；typmod 在 assignment cast 检查，bpchar 比较忽略尾空格但输出保真。encoding/collation 由 codec/operator 处理，大值自动 TOAST；验证多字节截断、invalid encoding、padding、LIKE/regex/sort 和 binary bytes。

### TYPE-05

bytea 保存任意字节并实现 hex/escape I/O、substring/overlay/bitwise/encode/decode/digest 等函数，NUL 不特殊；protocol binary 直接发送 payload，text 走 bytea_output。大值流式 TOAST，避免十六进制中间副本；验证 0～大对象、非法 escape、COPY/索引/hash。

### TYPE-06

date/time/timestamp/timestamptz/interval 用 PG epoch 和微秒 int64，导入 IANA tzdata 并实现 BC/infinity/DST gap/fold/timezone abbrev/typmod。timezone cache 按 zone/version，共用 parser/formatter；以 PG 的全时区边界和 binary epoch vector 比对运算、`AT TIME ZONE`、输出。

### TYPE-07

boolean Datum 仅含 true/false，NULL 在 Value 标志；Lexer/codec 接受 PG 合法缩写，输出固定 `t/f`，不再转换成整数。表达式实现 SQL 三值 truth table 和 short-circuit；bit-packed batch 可优化但与标量差分，覆盖 CASE/WHERE/CHECK/ANY/ALL。

### TYPE-08

enum label/order 存 `pg_enum`，Datum 为 enum OID；ADD VALUE 在事务提交后才可安全使用并处理 BEFORE/AFTER，rename 保持 OID。比较优先 OID/order cache，异常序列回退 catalog；验证并发 snapshot、sort/index/hash、dump restore。

### TYPE-09

按 PG double-based representation 实现 point/line/lseg/box/path/polygon/circle，补 parser、operator、distance/containment 和 NaN 规则；GiST/SP-GiST support function 走 fmgr。SIMD 只用于等价距离批处理；用随机/退化几何 differential 和 KNN recheck 验证。

### TYPE-10

inet/cidr 使用 family+prefix+16-byte address，macaddr/8 定宽；实现网络包含、mask、算术、排序/hash 和 PG binary layout。prefix operation 用位运算且可被 GiST/SP-GiST/BRIN opclass 复用；差分 IPv4/IPv6 映射、非法 host bits、wire bytes。

### TYPE-11

bit/varbit 用 bit length+packed bytes，typmod 强制长度；实现 concat、shift、substring、compare/hash 和 text/binary codec，尾部 unused bits 清零。字长批处理优化；验证非字节对齐、负/大 shift、TOAST 和索引顺序。

### TYPE-12

建立 text search catalog runtime：parser→token→dictionary chain→lexeme/position/weight `tsvector`，`tsquery` 为 typed tree；实现 rank/headline 和 GIN/GiST opclass。词典/配置缓存按 catalog invalidation，共享词干资源；用 PG 配置语料比对 binary/text、match/rank 和 index recheck。

### TYPE-13

UUID Datum 固定 16 byte，parser/output 遵循 PG 宽松输入和 canonical 小写输出，比较为 unsigned byte order；实现 v4/v7/extract timestamp/version。避免字符串化参与 hash/index；对 RFC/PG vector、时间单调边界和 binary protocol 验证。

### TYPE-14

链接 libxml2 实现 xmloption document/content、encoding declaration、xpath/XMLTABLE/XMLNAMESPACES/serialize；构建时无 libxml2 则明确禁用并 `0A000`。解析树限深/限实体并在 memory context 释放；与 PG/libxml 版本固定环境差分，加入 XXE/DoS 安全用例。

### TYPE-15

JSON 保留原文本并验证；JSONB 转成 versioned container（object/array/scalar、offset table、canonical key order）和 numeric Datum，补 SQL/JSON/jsonpath。GIN `jsonb_ops/path_ops` 从容器抽 key；迭代器流式避免反复 parse，差分 duplicate key、number、Unicode、path errors、binary和索引。

### TYPE-16

ArrayDatum 保存 element OID、ndim、dims、lower bounds、null bitmap 和 aligned elements；subscripting/assignment、comparison/hash、unnest/array functions 均操作 typed datum。大数组迭代和 slice 不复制，必要时 TOAST；差分非 1 lower bound、多维空数组、record array 和 binary frame。

### TYPE-17

CompositeDatum 为 TupleDesc OID/typmod + TupleSlot，匿名 record 由 typmod registry 管理；实现 field select/update、row compare、text/binary record codec 和 RETURNS record descriptor negotiation。descriptor cache 按 type alteration 失效；验证 dropped columns、NULL record/field、function/portal metadata。

### TYPE-18

RangeDatum 保存 subtype OID/collation、empty/lower/upper inclusive/infinite 与 typed bounds；调用 catalog canonical/subdiff，Multirange 是排序合并的不交叠 range vector。GiST/SP-GiST 操作 typed bounds；差分 discrete/continuous、NaN/infinity、aggregate、binary和索引。

### TYPE-19

domain Datum 复用 base type但保留 domain OID；Analyzer 递归展开 coercion，assignment/函数返回时执行 NOT NULL 和全部 CHECK，新 constraint 用 table scan revalidate 后原子生效。constraint expression cache按 dependency 失效；验证嵌套 domain、array/composite domain、错误 constraint 名。

### TYPE-20

OID/reg* Datum 使用 uint32 OID，input 通过 namespace/signature resolver，output 在可见且无歧义时最短 deparse否则 qualified；支持 numeric OID 输入和 snapshot。syscache 加速，验证 rename/drop/search_path/overload 和 dump round-trip。

### TYPE-21

实现 xid/xid8/cid/tid/pg_lsn/pg_snapshot/aclitem/name/char/cstring 的定宽或专用 varlena layout 和 codec；内部-only 类型拒绝普通表使用。LSN/快照算术连接 transaction state；逐类型比对 text/binary、sort/hash、wrap 和 privilege visibility。

### TYPE-22

在 fmgr call frame 表示 pseudo/polymorphic constraints，Analyzer 解出 anyelement/array/range/multirange/compatible family；internal/trigger/event_trigger/handler 类型只能出现在允许签名。resolution 结果缓存；负例必须在 analyze 阶段给 PG 相同 SQLSTATE，正例比对 result OID/typmod。

### FUNC-01

以 PG18 `pg_proc` manifest 生成函数注册和差分用例，按数学、字符串、格式、日期、网络、全文、XML、JSON、array/range、系统/管理/统计/复制分批实现；每个函数声明 strict/volatility/parallel/cost。底层算法共享 typed codec，不做 SQL 字符串往返；关闭条件是 signature/结果/错误/权限全集 manifest 无缺口。

### FUNC-02

`pg_aggregate` 保存 trans/final/combine/serial/deserial/inverse 函数、state type 和 init value；Aggregate node 为每 group 管 typed state，支持 DISTINCT/ORDER/FILTER、ordered/hypothetical 和 partial/final。hash state 超 work_mem 分区 spill、sort aggregate 外排；差分空输入、parallel、moving window 和序列化。

### FUNC-03

`pg_operator` 连接 fmgr function、commutator/negator、restrict/join selectivity、hash/merge 标记并写 dependency；Analyzer 走重载解析，planner 只在声明且 opfamily 验证后使用 hash/merge。operator lookup cache；测试自定义 cross-type operator 对 result、plan 和 drop cascade 的影响。

### FUNC-04

Analyzer/Planner 使用 volatility 决定 constant fold/index/parallel，Executor 落 strict NULL shortcut、security definer/leakproof/SET context；cost/rows 进入 path cost。fmgr 元数据缓存，调用可内联 fast path；用恶意 leak function、volatile counter、parallel plan 和 EXPLAIN cost 验证。

### FUNC-05

SQL function 保存 analyzed body或延迟 parse body，Planner 对安全 scalar/table body做 inlining，support function可简化/估算/index condition；调用绑定 polymorphic/default/named/variadic。缓存依赖 body/object，差分 search_path、DDL invalidation、SRF 和递归。

### FUNC-06

重写 PL/pgSQL 为 bytecode/statement tree runtime，支持 records/%ROWTYPE、exception/subtransaction、GET DIAGNOSTICS、EXECUTE USING、cursor、trigger vars、RETURN QUERY 和 plan cache。每 block memory context、SPI prepared plan复用；移植 PG plpgsql regression 并比对 context stack、SQLSTATE、transaction restriction。

## 10. 约束和数据完整性

约束统一进入 `ConstraintManager`，由 `ModifyTable/CopyFrom` 在同一顺序调用：BEFORE trigger→generated→NOT NULL/CHECK/RLS WCO→partition route→unique/exclusion/FK→heap/index→AFTER/deferred→RETURNING。任何入口不得旁路。

### CONS-01

PK/UNIQUE 由 catalog constraint+unique index支撑，支持多列、NULLS NOT DISTINCT、DEFERRABLE；immediate 用 speculative insertion/index conflict，deferred 保存 typed key event 到提交检查。分区唯一要求覆盖 partition key；批量冲突排序降低锁抖动，差分并发 upsert/deadlock和错误 constraint。

### CONS-02

FK 保存两侧列/operator OID和 action；statement/commit 用 RI trigger执行 indexed lookup并取得 key-share，支持多列 MATCH FULL、循环和 partition。批量 key 去重与按 index order检查；移植 PG ri/isolation tests，验证 snapshot race、cascade order、savepoint。

### CONS-03

CHECK 保存 cooked expression+dependency，DML 对新行执行；NOT VALID 跳历史 scan，VALIDATE 在合适 lock/snapshot 下并发扫描。immutable restriction和partition proof共用 predicate engine；表达式编译一次，差分 UNKNOWN pass、error context和domain behavior。

### CONS-04

exclusion constraint 通过 catalog operator/opclass 的 GiST scan查冲突并锁候选，支持 expression、多列、where、deferrable；插入采用 wait/recheck loop避免竞态。性能由GiST选择性与batch排序保证；并发 isolation验证重叠、NULL、abort/wakeup。

### CONS-05

generated expression在 Analyzer 验证依赖/volatility；stored 在写入时计算入 heap/WAL，virtual 在读取/索引/复制时按 PG18规则计算并保留 column metadata。表达式 program缓存；差分 ALTER、COPY、trigger、logical replication 和 privilege。

### CONS-06

identity 绑定 owned sequence，区分 ALWAYS/BY DEFAULT和 OVERRIDING SYSTEM/USER，全部 sequence option可ALTER；partition/inheritance复制或引用规则按 PG catalog。nextval cache提供性能；验证COPY/INSERT、rollback、restart、drop dependency。

### CONS-07

constraint trigger事件存 transaction级 deferred queue，SET CONSTRAINTS切换时按创建顺序立即drain；subtransaction记录queue watermark以便rollback，2PC序列化事件并恢复。队列可spill且key去重；验证savepoint/crash/prepare/commit错误时机。

### CONS-08

删除 `TableManage`、legacy view/COPY/MERGE的独立constraint代码，所有写路径构造 `ModifyTableContext` 并调用同一 pipeline；写入测试hook记录每阶段。验收每种入口注入相同违规行得到同SQLSTATE/constraint、无heap/index残留，批量路径吞吐不因逐行catalog lookup退化。

## 11. 查询、DML 和执行语义

### DML-01

Planner 为 INSERT/UPDATE/DELETE/MERGE 生成 `ModifyTablePlan`，source 是任意 plan，target含relation/partition/view mapping；Executor逐slot修改并共享constraint/trigger/RLS/RETURNING。删除 `tryDmlBridge` fallback 后才关闭；流式消费 INSERT SELECT，差分CTE/view/partition和affected row tag。

### DML-02

Analyzer 将 conflict target解析为唯一/排除索引并验证predicate/expression/collation/opclass；Executor做 speculative heap insert+index token，冲突时等待/recheck并执行 typed DO UPDATE。缓存 inference result随index失效；移植 PG upsert isolation，验证 RETURNING、trigger和高冲突吞吐。

### DML-03

UPDATE FROM/DELETE USING 的 source由普通join tree规划，target ctid作为resjunk；一个target遇多source遵循PG行为，WHERE CURRENT OF从portal取position。修改前做 EvalPlanQual recheck；参数化index path避免全量materialize，差分outer/lateral/subquery/self-join。

### DML-04

MERGE Analyzer生成有序action list，支持MATCHED、NOT MATCHED BY TARGET/SOURCE、DELETE/UPDATE/INSERT/NOTHING和RETURNING；Executor每candidate只选择首个true action并在并发变化时EPQ。source流式hash/merge join；差分重复match cardinality violation、trigger/RLS和并发。

### DML-05

RETURNING 是 ModifyTable上方projection，slot同时携带 OLD/NEW/final trigger row并支持 alias、subquery/window所需plan；TupleDesc直接给协议。禁止从打印文本推类型；差分PG18 WITH OLD/NEW、partition/view/ON CONFLICT/MERGE和binary metadata。

### DML-06

实现 `CopyState` 和 incremental text/CSV/binary parser/formatter，支持STDIN/OUT/PROGRAM/FREEZE/ON_ERROR/REJECT_LIMIT/HEADER MATCH/encoding；protocol CopyData有backpressure和CopyFail abort。64KiB～1MiB buffer批量转换/insert但共享constraint；wire/copy文件逐字节比对并测慢客户端/坏行。

### QRY-01

target list 用 typed `TargetEntry`（resno/resname/resjunk/origin），支持任意expression、SRF ProjectSet、whole-row/star expansion和alias scope。TupleDesc在analyze后确定；expression program编译并批量evaluate，差分列名/type/attorigin/SRF交错。

### QRY-02

FROM item统一为RTE，支持LATERAL、table function、ROWS FROM、ordinality、TABLESAMPLE、XMLTABLE/JSON_TABLE；parameter slots显式传递外层值。SRF/scan流式且sample用AM callback，差分null padding、alias descriptor、repeatable seed。

### QRY-03

Join tree保存join type和USING alias；Analyzer合并USING/NATURAL输出并生成coalesce列，Executor正确null extension，parameterized path支持lateral。hash/merge/nestloop共享joinqual/otherqual；全组合差分列顺序、FULL NULL和嵌套scope。

### QRY-04

SubLink分析为SubPlan/InitPlan，外部引用变Param；实现scalar cardinality、EXISTS、IN/ANY/ALL和row compare三值逻辑，安全时转semi/anti join。相关结果可Memoize且键含NULL/collation；差分空集/NULL/多行错误与相关百万行性能。

### QRY-05

CTE保存materialization policy和同snapshot计划；recursive用WorkTable+RecursiveUnion迭代，SEARCH/CYCLE增加隐藏列，data-modifying CTE共享command ID并按PG可见性。tuplestore受work_mem并spill；差分递归终止/循环、执行一次和RETURNING。

### QRY-06

SetOperation tree按grammar precedence分析每支列的common type/collation，UNION/INTERSECT/EXCEPT ALL用计数算法，顶层ORDER/LIMIT绑定正确scope。hash或sort均可spill；差分NULL、duplicate multiplicity、nested parentheses和metadata。

### QRY-07

Expand GROUPING SETS/CUBE/ROLLUP为canonical grouping sets并保留grouping bitmap，Planner选sort/hash/mixed aggregate；functional dependency来自PK/unique catalog。共享transition state减少重复计算并可spill；与PG差分GROUPING bits、ordered/distinct aggregate和空组。

### QRY-08

WindowPlanner把兼容spec分组排序，WindowAgg实现ROWS/RANGE/GROUPS全部bounds/exclusion/peer和inverse transition；named/inherited window在Analyzer展开。partition tuplestore受work_mem并支持外排/滑窗；差分所有frame边界、NULL/collation和大partition。

### QRY-09

DISTINCT用hash或sort unique，DISTINCT ON强制首部pathkeys与ORDER BY匹配并保留每组首行；typed equality遵循NULL/collation。两种实现均spill，差分错误条件、NaN/NULL/nondeterministic collation和稳定结果。

### QRY-10

Sort key保存expression/operator/collation/nulls/direction，使用类型sortsupport；实现bounded heap top-N、incremental sort和外部多路merge。tie不额外保证除非pathkeys要求；EXPLAIN显示方法/内存/磁盘，差分operator USING和大数据排序。

### QRY-11

LockRows节点按RTE执行UPDATE/NO KEY UPDATE/SHARE/KEY SHARE、OF、NOWAIT/SKIP LOCKED，tuple并发变化走EPQ并返回最新可见row。锁批次遵循扫描顺序且及时响应cancel；移植PG rowlocks isolation并验证partition/inheritance。

### QRY-12

Rewriter/Planner统一展开inheritance、partition、ONLY、view和RLS为Append/permission quals；runtime pruning使用Param变化，禁止executor旁路扫描。relcache缓存partition descriptor；差分prepared plan、role/path变更、partition attach和权限。

### QRY-13

把PG statement execution order写成可执行phase machine：snapshot/command counter、rewrite/RLS、BEFORE、constraint、heap/index、AFTER、deferred、RETURNING；rule/view route仍生成ModifyTable。通过trace hook与PG触发函数日志逐步比对，并确保phase本身无逐row catalog lookup。

## 12. 优化器和执行器

### OPT-01

新增 `PlannerInfo/RelOptInfo/Path/ParamPathInfo/EquivalenceClass/PathKey`；各AM/foreign/custom贡献Path，最终选最小cost再create plan，替换直接拼树。Path放planner arena并剪除dominated paths；用PG plan shape corpus和cost单元测试验证。

### OPT-02

小join数用动态规划枚举合法left/right/bushy joinrel，大于阈值使用GEQO；SpecialJoinInfo约束outer/semi/anti顺序。joinrel hash key用relation bitmap，等价path剪枝控制爆炸；TPC-H join order、outer join correctness和planning time设门槛。

### OPT-03

建立canonical predicate/equivalence/proof engine，做constant propagation、implication、join removal、outer reduction和静态/运行时partition pruning；只对immutable/leakproof安全变换。表达式hash-cons避免重复；随机query与关闭优化的结果差分，plan收益基准。

### OPT-04

ANALYZE采用block/tuple reservoir sample，写nullfrac/ndistinct/MCV/histogram/correlation及extended dependencies/MCV/expressions；统计有版本和inheritance/partition合并。采样内存受maintenance_work_mem；与PG统计列和估算误差分布比较。

### OPT-05

selectivity dispatcher调用operator restrict/join和type stats，cost模型计seq/random page、CPU、parallel、cache、collation；缺统计有明确default。estimate结果memoize；以实际rows校准q-error并确保自定义support function能改变plan。

### OPT-06

Path携带required outer relids；NestLoop给inner Param并支持index scan、SubPlan/InitPlan/Memoize。Memoize键是typed datum+collation并按work_mem淘汰/spill策略；差分相关子查询和LATERAL，验证不退化为N次全表扫。

### OPT-07

为Seq/TID/TIDRange/Sample/Function/Values/CTE/WorkTable/Foreign/Custom/Append/MergeAppend定义typed plan/operator、cost、rescan、parallel和instrumentation契约。各scan流式、Append按需打开child；每node都有PG EXPLAIN与结果差分。

### OPT-08

IndexOnlyScan从index tuple返回key+INCLUDE，先查VM all-visible，否则heap fetch并做visibility；page不是all-visible时不能猜。VM buffer缓存，heap fetch按block排序/预取；验证HOT/vacuum并发、heap fetch count和PG EXPLAIN。

### OPT-09

实现block bitmap含exact offsets/lossy pages，AND/OR与range candidate，超过work_mem转lossy/分段spill；BitmapHeap按block排序prefetch并recheck predicate。parallel bitmap使用共享iterator；差分lossy recheck和内存/I/O统计。

### OPT-10

统一 `WorkMemAccount`；sort外排runs、hash join/agg按hash bit分批、window/tuplestore写临时block、skew bucket有限额。temp文件由ResourceOwner删除并支持加密；测试极低work_mem结果不变、cancel无残留、复杂度和spill bytes。

### OPT-11

补IncrementalSort/Memoize/Materialize/Unique/LockRows/ModifyTable/ProjectSet/RecursiveUnion的open/next/rescan/end和参数变化语义；Materialize按随机访问需要选择内存/磁盘。统一instrumentation；逐node移植PG regression和EXPLAIN字段。

### OPT-12

postmaster worker pool分配parallel context、DSM和tuple queue；实现parallel-aware append/bitmap/hash、partial/final aggregate、Gather/GatherMerge、leader participation和错误/取消传播。worker数服从GUC/成本；验证worker crash、snapshot/role安全及扩展性曲线。

### OPT-13

I/O abstraction提供sync/worker/io_uring三种method，AIO queue携带buffer tag/resource owner/interrupt，scan和vacuum发prefetch并按`io_combine_limit`合并相邻block。queue深度受限；对不同method结果差分，记录pg_stat_io/aios和冷缓存吞吐。

### OPT-14

可选LLVM模块将expression和tuple deform编译为typed函数，达到jit_above_cost才触发，失败安全回退解释器；缓存键含plan/type/collation generation。构建无LLVM明确显示off；差分结果/错误并验证编译时间、EXPLAIN JIT和长query收益。

### OPT-15

PlanCache保存analyzed tree、generic/custom plans和ObjectAddress/GUC/role/search_path/RLS dependencies；执行统计决定custom与generic，invalidation只标记相关entry。使用分段LRU和内存限额；差分DDL/ANALYZE/SET ROLE/path改变后的重新规划与Describe metadata。

### OPT-16

每个plan node输出统一ExplainProperty tree，再序列化text/JSON/XML/YAML；支持ANALYZE/VERBOSE/COSTS/BUFFERS/WAL/MEMORY/SETTINGS/SERIALIZE/SUMMARY。instrumentation计时可按开关避免热路径开销；schema/golden输出与PG比对，ANALYZE失败保持error context。

### OPT-17

Session持有atomic InterruptToken，网络cancel/timeout/signal设置它；所有operator loop、lock wait、AIO、spill和worker queue在有界间隔检查。error unwinder按ResourceOwner反序释放pin/file/lock/context；压测取消延迟、无泄漏/脏portal，关闭检查时性能差异可量化。

## 13. 索引和访问方法

### IDX-01

定义 `TableAmRoutine/IndexAmRoutine` fmgr handler及build/insert/delete/scan/rescan/cost/vacuum/validate callback，`pg_am`/handler OID驱动planner/executor，不按名称switch。routine syscache后为直接函数指针；写最小测试AM验证API、事务/WAL/错误安全。

### IDX-02

`pg_opclass/opfamily/amop/amproc` 表示strategy/support/cross-type/collation/sortsupport；Analyzer校验定义，planner/AM按OID调用。cache构建按family generation失效；用自定义cross-type opclass验证search/order/unique和drop dependency。

### IDX-03

把BPTree迁成8KiB buffer page：meta/root/internal/leaf、high key/right link、suffix truncation/dedup，crabbing lock split、page delete/recycle和amcheck。page WAL/FPI+checksum；随机并发insert/delete/scan与PG排序差分，每次crash后amcheck且无整文件rewrite。

### IDX-04

B-tree path builder在前导列低ndistinct且后列有约束时生成skip-scan，executor循环定位下一前缀，成本用multi-column stats。设最大probe防退化；与seq/普通index结果差分并在选择性矩阵验证正确plan和收益。

### IDX-05

Hash AM实现metapage、bucket/overflow bitmap、incremental split、bucket/page lock，key只存hash+TID并recheck equality；support function来自opclass。每结构变化有WAL且vacuum回收overflow；并发倾斜key/crash/amcheck及扩容延迟测试。

### IDX-06

GIN实现entry tree、posting list/tree、pending list/fastupdate、extract/consistent/triConsistent和lossy recheck，支持array/jsonb/tsvector opclass。maintenance_work_mem批量build/cleanup，page WAL；高频term、长posting、并发vacuum/crash与PG结果/plan差分。

### IDX-07

替换flat `.gist` sidecar为buffered balanced GiST，调用consistent/union/compress/decompress/penalty/picksplit/same/distance，支持KNN priority queue。split使用page WAL和right-link恢复；随机range/geometry、bad opclass、concurrent split/vacuum/amcheck验证。

### IDX-08

SP-GiST实现meta/inner/leaf/redirect/dead tuple页和choose/picksplit/inner-consistent/leaf-consistent callback，支持radix/quad/k-d；scan维护search stack并recheck。page级WAL/vacuum；文本prefix/network/point opclass差分及退化树性能。

### IDX-09

BRIN每range保存summary tuple，revmap定位，insert更新或标dirty，autosummarize queue；支持minmax/minmax-multi/bloom/inclusion并可desummarize。summary小且顺序I/O；差分相关/乱序数据、vacuum/concurrent append/crash和false-positive recheck。

### IDX-10

expression/partial index在catalog保存analyzed expression/predicate和dependency，只允许immutable；Planner用predicate proof和结构等价匹配，HOT eligibility检查所有引用列。缓存表达式program；差分函数变更、prepared plan、NULL/collation和错误SQLSTATE。

### IDX-11

index tuple区分key与INCLUDE payload，AM返回TupleDesc；IndexOnlyScan按VM决定heap fallback，included列不参与ordering/unique。大payload受index tuple size规则；验证UPDATE non-key、HOT/VM race、heap fetch统计和binary metadata。

### IDX-12

partitioned index为无storage parent catalog对象，child attach需结构/opclass/collation一致；validity聚合，detach同步更新dependency。unique/exclusion必须覆盖partition key。批量DDL并行child build但原子发布；差分attach invalid index与constraint行为。

### IDX-13

CONCURRENTLY按多事务阶段：catalog invalid→build snapshot→wait writers→validate second scan→wait old snapshots→valid；失败保留invalid可drop/reindex。REINDEX用new relfilenode swap。progress可见且阶段可恢复；高并发DML/kill点/unique race验证。

### IDX-14

每AM实现bulkdelete/vacuumcleanup/page deletion和统计，维护pending/recycle安全horizon；提供amcheck结构/parent-link/key-order/heap-TID检查和REINDEX恢复建议。vacuum按maintenance_work_mem批量TID；损坏注入、progress/pg_stat和并发snapshot测试。

### IDX-15

所有AM只通过BufferManager/Smgr/WAL/TDE/backup block interface读写，禁止独立fstream sidecar；DDL注册pending file，DML page LSN，reindex原子swap。静态检查阻止access模块直接文件I/O；逐AM跑加密、备份、恢复、PITR和WAL retention矩阵。

## 14. 事务、MVCC、锁和 VACUUM

### TXN-01

TransactionManager将 READ UNCOMMITTED映射READ COMMITTED；每条statement开始取新snapshot，CommandCounterIncrement控制同事务命令可见性，tuple visibility读取xmin/xmax/cid。snapshot从ProcArray一次复制并缓存到statement；移植PG mvcc/isolation用例，比对游标、函数内多命令和ReadyForQuery。

### TXN-02

RR在首个非事务控制statement固定snapshot，Serializable在其上注册SSI；read-only/deferrable等待safe snapshot并使用PG相同限制/SQLSTATE。snapshot wait用condition variable非轮询；验证write skew、只读异常、BEGIN options组合和长事务horizon。

### TXN-03

完成HeapTupleHeader的xmin/xmax/cmin/cmax、combo CID、ctid、infomask hint、HOT/redirect/dead line pointer规则；可见性集中于 `HeapTupleSatisfies*`，所有AM/maintenance复用。hint bit写受content lock且可不WAL；真值表、HOT chain并发和page corruption测试覆盖。

### TXN-04

新增SubTrans SLRU和SubTransaction stack，记录parent SubXID、command/resource owner、locks和state；overflow后snapshot按top xid+pg_subtrans判定。子事务commit把资源转父owner，abort局部释放；验证深嵌套、exception block、overflow/crash和性能上界。

### TXN-05

每个savepoint保存subtransaction、catalog/file pending action、deferred event、portal/notify/listen和GUC watermark；ROLLBACK TO创建新的同名savepoint状态并释放之后资源，sequence非事务行为按PG保留。watermark O(1) rollback加局部undo；逐资源差分和重复savepoint测试。

### TXN-06

PREPARE把xid/subxid、locks、invalidation、notify、pending files、deferred state写两阶段文件并WAL flush后从backend detach；启动恢复到shared prepared array，任意授权backend可COMMIT/ROLLBACK。文件有CRC/version并限制大小；crash每阶段、duplicate gid、max_prepared和备份恢复验证。

### TXN-07

Heavyweight LockManager实现PG locktag、8种table mode、page/tuple/transaction/advisory/object locks、fast path、FIFO wait queue和deadlock wait-for graph含soft edge reorder。锁表分区，wait用latch；移植deadlock/isolation并测高并发无单mutex瓶颈。

### TXN-08

tuple lock用xmax+infomask/MultiXact表达key-share/share/no-key-update/update，等待transaction/MultiXact后重读tuple；update/delete/unique/FK/EPQ共享接口。MultiXact成员SLRU+cache；全冲突矩阵、lock upgrade、HOT update、crash wrap测试。

### TXN-09

PredicateLockManager支持relation/page/tuple及B-tree/各AM逻辑range，空scan锁gap；超过per-xact/per-relation阈值安全升级。锁hash分区且只记录Serializable；用PG isolation和随机history验证phantom/false positive仅导致40001、不允许漏冲突。

### TXN-10

advisory lock key规范化为(dbOid,key64)或(dbOid,key1,key2)，支持session/xact、shared/exclusive、blocking/try；复用heavy lock并在session/resource owner清理。验证两个key namespace不碰撞、savepoint/commit/disconnect和pg_locks字段。

### TXN-11

commit顺序固定：precommit checks→WAL records→commit record→按synchronous_commit flush/wait replica→CLOG/procarray visible→notify/invalidation→reply；abort不需flush但清资源。group commit leader/follower合并flush，记录commit timestamp可选；kill-point和延迟分布验证。

### TXN-12

LISTEN状态在commit生效，NOTIFY在commit后写shared ring/SLRU queue，payload按UTF-8和长度验证；backend在查询间和可中断点发送NotificationResponse，rollback丢弃。queue按最慢listener回收并告警；跨backend、queue full、2PC和协议时序差分。

### VAC-01

Vacuum按page prune HOT/dead tuple、收集dead TID批量index cleanup、freeze、设置VM all-visible/all-frozen和安全truncate；failsafe在接近wrap时跳非关键工作。maintenance_work_mem限制TID batch；移植vacuum tests并验证old snapshot、slot horizon和crash。

### VAC-02

postmaster启动autovacuum launcher，按n_dead_tup/insert/scale factor/freeze age选表并派worker；支持per-table options、cost delay、worker slots和anti-wraparound抢占/取消冲突。调度用priority queue；长期混合负载验证bloat、wrap保护和前台p99。

### VAC-03

VACUUM FULL/CLUSTER创建新relfilenode、按snapshot复制live rows并重建index，短锁原子swap，失败回滚；VACUUM/ANALYZE option由typed utility解析，progress共享槽更新。copy流式/parallel build；逐option差分及swap kill-point验证。

### VAC-04

HOT eligibility查询relcache汇总的所有index key/expression/predicate依赖列且新tuple能放同page；prune遵循oldest xmin并维护redirect/ctid链，VM失效与page WAL同原子操作。依赖bitmap缓存；并发表达式/partial index下做heap-index cross-check和链长性能测试。

## 15. 存储、WAL、checkpoint 和恢复

### STO-01

新增双副本 `ControlFile`，包含magic、format/catalog version、system identifier、block/WAL segment size、endianness、checksum/TDE flags、checkpoint/timeline并带CRC；启动不匹配即拒绝。更新采用write+fsync+原子rename+目录fsync；损坏一/两副本、跨版本和错误平台测试。

### STO-02

用 `RelFileLocator{spcOid,dbOid,relNumber}` 和 fork(main/fsm/vm/init)统一Smgr，relation按固定segment blocks切文件，temp加backend id。旧名称只在upgrader解析；locator直算path避免catalog查找，验证tablespace/database move、segment boundary和备份清单。

### STO-03

定义versioned 8KiB PageHeader（LSN/checksum/flags/lower/upper/special）、ItemId和aligned tuple/varlena/toast pointer；页初始化记录version，读取严格校验bounds。tuple deform缓存offset，固定列快速路径；fuzz任意page bytes、torn page和跨端序upgrade。

### STO-04

BufferPool提升为共享buffer table：partitioned tag hash、pin/usage、content/io locks、I/O-in-progress wait、clock sweep、prefetch和bulk ring；ResourceOwner记录pin，异常自动unpin。读写锁不跨I/O持有；并发同页单次读取、eviction race、worker crash和hit-rate/scaling测试。

### STO-05

FSM/VM作为relation fork经Buffer/WAL管理；FSM tree近似空闲空间，VM两个bit为all-visible/all-frozen，heap修改先清VM，vacuum设置前保证heap/index状态。损坏可离线/启动重建；index-only/vacuum/crash差分并测FSM避免全表找空页。

### STO-06

实现1/4-byte varlena、pglz/lz4 compressed和external toast pointer，按PLAIN/MAIN/EXTERNAL/EXTENDED与toast_tuple_target选择；toast relation分chunk并有index，update复用未变datum，delete/vacuum清理。detoast支持slice/stream，验证超大value、崩溃、logical/backup和压缩收益。

### WAL-01

在 `src/wal/rmgr` 为xact/heap/heap2/btree/hash/gin/gist/spgist/brin/smgr/catalog/fsm/vm/toast/multixact/sequence/standby/slot定义typed record、describe、redo；所有mutation API要求WAL context。构建期覆盖检查防未注册资源，逐rmgr golden decode和redo幂等测试。

### WAL-02

WALInsertLock分区预留LSN并并发copy到WAL buffers，record支持block reference、FPI、compression、continuation和CRC；page LSN在record insert后设置，segment switch/preallocate/recycle遵循timeline。group flush保持，压测writers扩展和跨segment/torn record恢复。

### WAL-03

Checkpointer记录redo horizon后分批刷当时dirty buffers，completion更新control file再回收WAL；bgwriter平滑写脏页，checkpoint_completion_target节流。dirty queue按LSN/年龄调度；kill任意阶段恢复，监测checkpoint latency、backend fsync和WAL retention。

### WAL-04

StartupRecovery读取checkpoint/timeline history，redo到一致点建立standby snapshot；checkpoint期间做restartpoint，冲突按lock/snapshot/buffer pin/deadlock/tablespace处理。promotion写end-of-recovery record和新timeline；多timeline、pause/resume、hot query与冲突测试。

### WAL-05

生产恢复改为ARIES式物理/逻辑redo：只重放page LSN较旧的record，未提交tuple由MVCC隐藏并由vacuum清理；移除before-image undo作为一致性依赖。若保留仅用于测试/flashback须隔离格式。steal/no-force、并发checkpoint每个write窗口经model/crash test证明。

### WAL-06

所有page含基于block number的checksum，WAL/meta/control/manifest各自CRC；`dbms_checksums --check/--enable/--disable` 在离线或安全在线流程重写并记录进度。Buffer load集中校验避免重复CPU；损坏定位到relation/fork/block并验证工具可续跑。

### WAL-07

封装DurableFile API明确create/write/fsync/rename/link/unlink/dir-fsync顺序和允许错误；tablespace跨设备不用rename假设，partial write循环并保留dirty。ext4/XFS和dm-flakey/powercut matrix验证；批量目录fsync降低DDL开销但不越过commit边界。

### WAL-08

恢复状态机分别处理unlogged init reset、temp delete、2PC restore、sequence log-ahead、pending DDL file action和slot xmin/restartLSN；每类有control/catalog marker防重复。启动批处理清理；每类在commit前后kill验证，且不扫描无关data file。

### STO-07

large object用 `pg_largeobject_metadata/pg_largeobject` catalog+chunk heap，ACL/owner/dependency/transaction统一；实现64-bit seek/read/write/truncate、lo_*函数和libpq fast-path protocol。chunk index支持流式随机访问，不全量载入；差分权限、savepoint、vacuum、dump/restore。

### STO-08

用OpenSSL EVP或libsodium AEAD封装envelope encryption：cluster KMS master→per-db/data key→page/WAL/temp/backup object nonce+tag；key catalog只存wrapped key并支持在线rotation generation。TDE在Buffer/Smgr层覆盖所有fork/AM，保留checksum语义；外部密码学审计、lost/old key恢复、性能/AES-NI测试后才能发布。

## 16. 复制、高可用和备份

### REPL-01

protocol replication startup建立WalSender，支持IDENTIFY_SYSTEM/TIMELINE_HISTORY/START_REPLICATION；按LSN从WAL segment流CopyData并发keepalive，接收standby status feedback。WalReceiver写临时segment、fsync后原子发布；bounded send buffer和零拷贝，golden wire/断线续传测试。

### REPL-02

standby由StartupRecovery持续replay，ProcArray发布recovery snapshot并只允许read-only；记录KnownAssignedXids和running-xacts WAL，冲突按max_standby_*取消，feedback反馈xmin。apply和query共享buffer content lock；延迟/长查询/bloat权衡及一致性点测试。

### REPL-03

slot作为持久化目录+catalog object，双文件CRC原子保存type/plugin/restart/confirmed/xmin/catalog_xmin/two_phase/failover/sync；WAL回收和vacuum horizon读取最小值。状态更新节流但关键advance先fsync；crash、磁盘满、inactive retention和copy/sync验证。

### REPL-04

WalSender维护每standby write/flush/apply LSN，SyncRepConfig解析FIRST/ANY quorum并动态选sync standbys；commit waiter按目标LSN/mode入有序队列，配置/断线唤醒重算。批量唤醒降低锁开销；差分remote_write/flush/apply、超时和p99延迟。

### REPL-05

standby可作为sender形成cascading，receiver按timeline history follow；promotion加timeline且fence旧primary，`dbms_rewind`用page checksums/WAL找divergence并复制changed blocks。文档定义外部STONITH边界；网络分区/双promote/rejoin演练验证不静默分叉。

### REPL-06

删除DML旁路采集作为真源；heap/catalog/transaction WAL带logical identity和commit ordering，LogicalDecodingContext从restart LSN读WAL，经ReorderBuffer按xid/subxid组装，只在commit输出。decode不阻塞WAL writer；结果与写路径无关且crash重启不丢不重。

### REPL-07

实现pgoutput v1-v4 message codec：Begin/Commit/Relation/Type/Insert/Update/Delete/Truncate/Origin/Stream/2PC，按publication和binary option发送精确type/replica identity。relation metadata cache按schema invalidation；使用PG subscriber/decoder双向golden wire和large transaction测试。

### REPL-08

publication catalog保存all tables、schema、table column list、row filter和publish actions/root option；Analyzer验证filter/column，decoder在正确old/new tuple上评估并处理partition root映射。compiled filter缓存；ALTER立即对新change生效，差分PG publication视图和输出。

### REPL-09

postmaster管理launcher/apply/table-sync workers；CREATE SUBSCRIPTION建slot/conninfo/origin，initial copy取得一致snapshot和start LSN，再apply，支持disable/refresh/skip/conflict/2PC/failover。apply批量transaction但保留commit边界；断线、schema change、unique conflict和resume验证。

### REPL-10

定义LogicalOutputPlugin callbacks（startup/begin/change/commit/filter/shutdown）和DecodingContext；ReorderBuffer按work_mem将大事务spill为checksummed segments，支持snapshot export和subxid/TOAST重组。plugin在受限memory/resource owner运行；crash/cancel/plugin error清理和顺序验证。

### BACKUP-01

BaseBackup API执行checkpoint并写backup-start WAL，返回backup_label/tablespace_map，遍历snapshot文件集合流式发送并在stop写end WAL，选fetch/stream WAL且支持rate limit。relation segment读取处理并发truncate规则；持续DML下恢复和慢客户端测试。

### BACKUP-02

生成PG兼容概念的manifest：文件path/size/mtime/checksum、WAL range/system id；verify先校验manifest自身再并行校验文件并输出精确missing/extra/corrupt诊断。I/O限速和分块hash；随机bit flip/truncate/rename、加密备份和退出码测试。

### BACKUP-03

checkpointer/WAL维护block change summary，incremental backup只发base LSN后changed blocks+new/deleted files；combine工具验证chain/system id后重建full image并生成新manifest。summary分段/压缩，避免扫描全数据；多级chain、missing base、timeline change和结果hash验证。

### BACKUP-04

启动读取recovery.signal/standby.signal和typed recovery GUC，支持name/time/xid/LSN/immediate、inclusive、promote/shutdown/pause；replay在transaction boundary判断target并持续可观测。restore_command有重试/超时；每种target与PG恢复点差分。

### BACKUP-05

archive按timeline WAL文件和.history管理，archive_command使用受控placeholder expansion且不经不必要shell，失败保留.ready并指数退避；cleanup只删不再被backup/slot/timeline需要的segment。多timeline restore/promote/retry/命令注入安全测试。

### BACKUP-06

优先让真实pg_dump通过完整protocol/catalog工作；实现缺失的catalog query、COPY、large object和ACL/owner，验证plain/custom/directory archive，再让pg_restore并行按TOC dependency执行。对象deparse使用catalog OID；与PG dump做语义规范化diff和跨库round-trip。

### BACKUP-07

实现rewind后自动生成standby配置/slot关联和追赶检查，提供promote/rejoin runbook、dry-run和不可rewind原因；每次发布执行自动HA演练并记录RPO/RTO。changed-block并行复制且限速；故障期间transaction ledger证明零/已声明范围内丢失和无双写。

## 17. PostgreSQL 协议和客户端兼容

### PROTO-01

把PostgresProtocol改成startup/auth/query有限状态机，协商3.0/3.2和不支持minor，保存application_name/client_encoding/options/replication；逐项发送PG相同ParameterStatus、BackendKeyData和事务ReadyForQuery。参数表来自GUC，golden packet和非法长度fuzz验证。

### PROTO-02

Simple Query用grammar-aware splitter一次parse整串，在idle时包implicit transaction；一个statement失败后跳余下并最终ReadyForQuery，空串发EmptyQueryResponse，tag来自ExecutionResult。逐statement流式结果；多语句/DDL/error/explicit txn packet序列与PG逐帧比对。

### PROTO-03

完整Parse/Bind/Describe/Execute/Close/Flush/Sync状态机；Bind保存原始typed params，Portal流式游标，maxRows到限额发PortalSuspended，error后忽略到Sync，pipeline维护sync groups。portal不物化结果；libpq pipeline和随机message sequence model test验证。

### PROTO-04

RowDescription直接从TupleDesc和TargetEntry origin填name/tableOid/attnum/typeOid/typlen/typmod/format；DataRow逐Value调用send/output，NULL=-1。descriptor在Parse/Describe即可获得时稳定；覆盖join/expression/domain/array/RETURNING和binary混合format的wire bytes。

### PROTO-05

每个TypeCodec注册send/receive，Bind和DataRow按列format独立选择；unknown/user type通过pg_type fmgr，不支持binary时按PG错误而非猜text。编码buffer预估长度并scatter/gather；全部built-in/array/range/record binary用PG producer/consumer互测。

### PROTO-06

实现CopyIn/Out/BothResponse、CopyData/Done/Fail和CopyState连接；网络读写用bounded queue与socket backpressure，CopyFail使statement/transaction进入PG相同状态。支持混合text/csv/binary和flush；pcap、慢端、半包/断线、GB级流内存测试。

### PROTO-07

postmaster维护backend PID/128-bit secret映射，CancelRequest校验后只置目标InterruptToken且不回包；锁/AIO/worker/executor传播。secret启动随机且退出清除；伪造/旧key/并发cancel、阻塞锁和长函数取消延迟测试。

### PROTO-08

DbError编码完整S/V/C/M/D/H/P/p/q/W/s/t/c/d/n/F/L/R字段；Notice和Notification独立异步队列，在协议允许边界发送并保序。限制单消息/队列大小防DoS；与PG错误golden、notice during query和通知交错比对。

### PROTO-09

raw socket先处理SSLRequest/GSSENCRequest，再创建TLS socket；OpenSSL配置server cert/key/CA/CRL、SNI、client cert、min/max protocol/cipher和SCRAM-PLUS channel binding，reload用新context原子换。sslmode矩阵、证书轮换、恶意握手和TLS性能测试。

### PROTO-10

startup `replication=database/true` 切到ReplicationCommand parser，仅允许IDENTIFY_SYSTEM、TIMELINE_HISTORY、CREATE/DROP_REPLICATION_SLOT、START_REPLICATION等，CopyBoth承载physical/pgoutput。与普通SQL权限/状态隔离；真实pg_receivewal/pg_recvlogical互操作验证。

### PROTO-11

NetworkServer支持Unix socket目录/权限、IPv4/IPv6 bind、keepalive/user timeout和listen reload；client_encoding在SourceText/codec边界转换，lc_*为session GUC。event loop避免慢连接占worker；多地址、非UTF8、socket cleanup和连接风暴测试。

### PROTO-12

CI矩阵固定libpq/psql/JDBC/Npgsql/psycopg/pgx及SQLAlchemy/Django/Hibernate版本，跑connect/auth/prepare/txn/COPY/introspection/migration/pool/error/cancel。保存失败packet和server trace；每次协议/catalog改动必跑，项目专用driver workaround视为失败。

### CLIENT-01

不重写psql；以协议/catalog兼容为主，让官方psql的describe、变量、脚本ON_ERROR_STOP、COPY、password、watch等工作。缺服务端能力按对应项实现；CI运行psql regression scripts并比对stdout/stderr/exit code，输出格式不由服务端伪造。

### CLIENT-02

提供薄CLI `dbms_init/ctl/ready/bench` 或让PG工具直接工作：createdb/dropdb/createuser等依赖协议即可复用；vacuumdb/reindexdb/clusterdb/pg_isready/pgbench列入互操作。工具遵守data-dir lock、exit code和machine-readable输出；端到端安装/服务/失败测试。

## 18. 安全、认证和权限

### SEC-01

pg_hba parser支持local/host/hostssl/hostnossl/hostgssenc/hostnogssenc、CIDR/samehost/samenet、database/role列表/replication、include/include_dir/map/options；配置parse成不可变rule set，SIGHUP成功才原子替换。逐行error position和PG hba matrix差分。

### SEC-02

定义AuthenticationProvider接口并分模块实现peer/ident/cert/LDAP/PAM/RADIUS/GSS/SSPI/BSD/OAuth；构建/平台不支持的provider在配置load时报明确错误，绝不降级password。认证I/O有timeout/rate limit；用真实服务容器、失败/回退/审计测试验收。

### SEC-03

扩展SCRAM实现SHA-256-PLUS、tls-server-end-point binding、可配置iteration和server nonce；verifier只存SASL string，ALTER ROLE按password_encryption生成并支持轮换。使用常数时间比较和成熟crypto；RFC/PG/libpq互操作、downgrade和credential日志泄漏测试。

### SEC-04

发布构建强制OpenSSL且TLS stub仅测试可用；GUC控制cert/key/CA/CRL/cipher/protocol/passphrase，SIGHUP原子重载，pg_stat_ssl暴露version/cipher/bits/client DN。OCSP按明确策略实现；CI扫描弱协议/cipher并做并发握手基准。

### SEC-05

`pg_auth_members`保存role/member/grantor及admin/inherit/set options，授权检查图遍历防循环并尊重session SET ROLE；DROP/REASSIGN OWNED走dependency。membership closure按role generation缓存；PG18 GRANT ROLE组合、grant chain revoke和并发变更差分。

### SEC-06

ACLItem用grantee/grantor/privilege+grant-option bitmap，AclCheck按对象类型和PUBLIC/owner/superuser/predefined role处理database到parameter全集及column fallback。ACL cache按object/role invalidation；每privilege/对象/命令正负矩阵和error message验证。

### SEC-07

`pg_default_acl`以owner/namespace/object type为键，在CREATE时合并硬编码default后写对象ACL，ALTER DEFAULT PRIVILEGES事务化并有dependency。lookup命中syscache；差分多owner/schema、FUNCTION/ROUTINE、large object和dump/restore。

### SEC-08

policy catalog在Rewriter按command/role注入USING/WITH CHECK，owner/bypass/force规则在统一security context判定；partition/inheritance各child映射列，prepared plan依赖policy/role。leakproof qualifier排序受安全屏障；恶意函数、role切换和旁路入口验证。

### SEC-09

fmgr调用建立SecurityContextFrame，分别跟踪session/current user、SECURITY DEFINER、proconfig和受控search_path；任何return/error/subtransaction都RAII恢复。函数计划缓存键含effective role/path；对象劫持、异常、nested definer和SET LOCAL差分。

### SEC-10

view RTE标记security_barrier/invoker和securityQuals，Planner只有leakproof qual可越过barrier，RLS qual同理；function metadata来自pg_proc且只有superuser可设leakproof。用side-channel counter函数验证无提前求值，并对plan/result与PG比对。

### SEC-11

SecurityLabelProvider API校验/应用label并在object access hook执行MAC decision；未加载provider时按PG规则保存/拒绝对应provider而不声称MAC。sepgsql需独立SELinux集成与审计；label lookup cache受invalidation，策略正负测试证明强制执行。

### SEC-12

集中 `PrivilegedOperation` 检查COPY PROGRAM/文件、server file roles、large object、extension/LOAD和目录边界；文件使用openat+canonical root+O_NOFOLLOW，PROGRAM参数不拼shell。审计记录脱敏；路径穿越/symlink/command injection和预定义角色矩阵。

### SEC-13

AuditEvent是结构化不可变record，写append-only受限目录或远端sink，包含session/role/object/result/SQLSTATE且参数按policy脱敏；异步bounded queue在overflow按配置阻塞或fail-closed并告警。rotation/retention签名链可选；篡改、磁盘满和敏感数据测试，同时文档明确它是项目扩展。

## 19. 系统目录、information_schema、监控和运维

### MON-01

从PG18 catalog descriptor生成 `pg_catalog` relation/view/function兼容清单，真实catalog列保持OID/type/attnum，view以SQL定义并执行ACL过滤；项目额外列放独立schema。syscache/普通planner执行避免手工分支；用psql `\d`、pg_dump和逐view schema/result差分关闭。

### MON-02

导入SQL标准information_schema view定义并适配真实catalog，不再由 `queryInformationSchema` 硬编码；所有view按current user过滤并返回标准domain/identifier类型。planner可内联安全view；逐表column descriptor和多角色结果与PG比较。

### MON-03

建立共享StatsShmem分片计数器和持久snapshot文件，支持transactional/nontransactional snapshot语义、reset timestamp和track_*；覆盖table/index/function/SLRU/WAL/checkpointer/bgwriter/I/O。热计数per-backend/per-CPU聚合降低cacheline竞争；crash/reset/权限和开销基准。

### MON-04

BackendStatus array由每backend seqlock更新pid/datid/user/state/query_id/xact/query times/wait/backend type/client/leader；reader取一致快照并按角色脱敏query。query文本放bounded shared buffer；状态转移、并行worker、idle-in-xact和权限与PG差分。

### MON-05

LockManager将完整LockTag/mode/granted/fastpath/waitstart和predicate lock导出只读snapshot，事务/virtualxid/object/page/tuple/advisory均映射PG列。分批复制避免长持锁；构造每种lock和wait queue，与pg_locks行及blocking函数差分。

### MON-06

WalSender/Receiver/Archiver/Slot/Subscription/Recovery/TLS/GSS各维护typed stats provider，catalog view统一读取并按权限隐藏conninfo。计数原子、低频字段seqlock；运行复制/归档/SSL场景逐列与PG视图结构和状态变化比对。

### MON-07

Buffer/AIO/WAL/SLRU/MemoryContext在操作边界打统一I/O event，聚合到backend type/object/context/read-write-extend/fsync timing；实现pg_stat_io、pg_aios、wal/checkpointer/slru和memory context views。采样/计时可配置，关闭时热路径开销设<1%；计数守恒测试。

### MON-08

为ANALYZE/VACUUM/CREATE INDEX/CLUSTER/COPY/base backup定义固定progress slot和phase enum，worker按节流频率更新blocks/tuples/bytes；退出/error清slot。更新不逐tuple做共享锁；各phase故障/取消与PG progress列、单位和生命周期差分。

### MON-09

parser生成query jumble/queryid，SqlStats扩展plans/calls/rows/time/block/temp/WAL/JIT/parallel和minmax；共享hash按queryid+db+user，容量满用clock/LRU淘汰，文本单独arena。reset权限和时间原子；跑pg_stat_statements regression、并发计数和开销基准。

### MON-10

LoggingCollector从backend pipe接收结构化LogRecord并输出stderr/csvlog/jsonlog/syslog，支持rotation、line prefix、verbosity、statement/duration/sampling；超大消息分帧重组。队列有背压/丢失计数且敏感参数脱敏；格式golden、rotate/crash/disk full测试。

### MON-11

在EXT完成前，auto_explain/pg_buffercache明确为内置compat provider但保持extension相同schema/权限；EXT完成后改标准control/SQL库实现。auto_explain复用Explain tree，buffercache取Buffer snapshot；extension install/drop、字段和开销与PG比较。

### OPS-01

Config重构为GUC registry，记录type/unit/context/source/default/reset/pending_restart和assign/check/show hook；启动显式 `-D`/env定位data dir，control file决定cluster状态，禁止依赖CWD。配置层级与PG一致且SIGHUP原子；路径/来源/错误差分。

### OPS-02

`SHOW/pg_settings`直接查询GUC registry，输出name/setting/unit/category/short_desc/extra_desc/context/vartype/source/min/max/enum/boot/reset/pending_restart；敏感项权限过滤。registry索引O(1)；逐GUC descriptor golden和ALTER SYSTEM/SET/RESET行为测试。

### OPS-03

postmaster处理SIGHUP、smart/fast/immediate shutdown、startup PID/lock和child supervision：smart等session、fast取消事务、immediate直接退出后强制recovery。signal handler只写self-pipe/latch；双启动、每阶段信号、backend crash loop和PID复用测试。

### OPS-04

init工具创建data/WAL/log/temp/archive目录并校验owner/mode/umask，服务用户不可被其他用户读写；所有路径由DataDir对象解析，tablespace/归档明确跨设备。安装包提供systemd/container约定；权限篡改、只读目录、symlink和升级测试。

### OPS-05

ResourceGovernor监控free space/inode/FD/memory/CPU/I/O/connections/output queue，设reserved WAL/control FD和superuser connection slots；阈值触发拒绝新work、取消temp-heavy query或告警，不破坏active commit。压力/慢读/ENOSPC/EMFILE/OOM测试证明fail-safe与恢复。

## 20. 扩展、FDW、过程语言和生态

### EXT-01

ExtensionManager解析`.control`，按version graph在指定schema事务执行install/update SQL，写pg_extension和member dependency；relocatable检查对象资格，dump只输出CREATE EXTENSION及config tables。脚本有受控search_path；升级/回滚/并发安装和常用extension control corpus测试。

### EXT-02

定义稳定fmgr C ABI、Datum/NullableDatum、FunctionCallInfo、PG_FUNCTION_INFO和MemoryContext/error trampoline；动态库用dlopen/dlsym但校验ABI/version、路径/权限，backend退出卸载。调用fast path避免虚函数；C/C++测试扩展覆盖NULL/error/interrupt/leak并做ABI兼容策略。

### EXT-03

CREATE TYPE关联input/output/receive/send/typmod/analyze/subscript handler，composite/range/multirange复用catalog descriptor；varlena/datum ownership写入ABI。Planner调用自定义analyze，协议调用codec；测试extension自定义base+array+range的heap/index/COPY/binary/dump。

### EXT-04

function/procedure/aggregate/operator/cast/collation/conversion对象全部写真实catalog并由Analyzer/fmgr/Planner执行，删除compat record；定义/alter/drop触发dependency/invalidation。lookup走syscache；编写一个extension组合所有对象，验证schema visibility、plan变化、错误和restore。

### EXT-05

对外暴露IDX-01/02的Table/Index AM handler和WAL generic page API，validator检查callback全集/opclass；extension AM只能经Buffer/Smgr/WAL写page。generic WAL记录页diff/FPI且有大小限制；测试AM build/scan/vacuum/crash/upgrade。

### EXT-06

HookRegistry按load order提供planner/executor/utility/object access/emit log和CustomScan methods，调用链支持next hook并在error时resource owner清理；hook generation进入plan cache key。空hook为零/极低分支开销；多hook顺序、卸载禁止、并行安全和异常测试。

### EXT-07

实现shared memory allocator、named LWLock tranche、DSM segment/TOC和shared/session preload生命周期；postmaster启动前确定固定shmem，DSM运行时创建并由resource owner回收。锁统计/死锁规则明确；扩展跨backend counter、worker crash和容量/安全测试。

### EXT-08

BackgroundWorker注册name/flags/start/restart time/library/function，postmaster按时机fork并监督signal/restart；worker可安全初始化DB connection和shmem。crash频率退避防fork bomb；动态/静态worker、shutdown、upgrade和权限测试。

### EXT-09

`pg_language`关联call/inline/validator handler，CREATE FUNCTION调用validator，执行经fmgr/SPI；先完整plpgsql，再为PL/Python/Perl/Tcl定义可信/非可信sandbox边界和可选包。语言runtime有per-backend cache/内存限额；官方语言regression及异常/事务/安全测试。

### FDW-01

实现FdwRoutine handler/validator和GetForeignRelSize/Paths/Plan、Begin/Iterate/ReScan/End scan、modify/direct modify、transaction callbacks；server/user mapping/foreign table options来自catalog并权限过滤。批量fetch/insert和连接复用；最小mock FDW与postgres_fdw互操作测试。

### FDW-02

IMPORT FOREIGN SCHEMA调用FDW callback生成CREATE FOREIGN TABLE AST并在一个事务执行；Planner支持parameterized/join/aggregate/async paths和remote quals，EXPLAIN脱敏remote SQL。async event loop有backpressure；远端错误/事务/savepoint/cancel和pushdown result差分。

### EXT-10

将logical output plugin、archive module、OAuth validator和injection point都建成versioned callback ABI，明确process/context/interrupt/thread约束；配置只加载受信路径。热callback直接函数指针；分别提供reference module与生命周期、错误、reload、升级测试。

### EXT-11

按依赖顺序建立真实互操作门：先pg_stat_statements/auto_explain（hooks），再pg_trgm/btree_gin/gist（type/opclass），hstore/citext/uuid-ossp（fmgr/type），最后postgres_fdw（FDW）。每个使用上游源码未经项目patch构建，跑官方regression、dump/upgrade和性能；未通过不得列兼容。

## 21. 工程质量、发布和生产化

### ENG-01

生成 `docs/generated/feature-status.md`，唯一来源为compat manifest和当次机器报告；README/RELEASE/CHANGELOG只引用，不手填PASS数。CI校验文档链接、版本和声明不漂移；历史文档顶端标日期/基线/已替代，禁止作为当前证据。

### ENG-02

建立单一`VERSION`文件或CMake project version，构建生成`version.h`、`--version`、协议server_version、包名和文档变量；删除手写常量。CI断言全部输出等于manifest，release tag必须匹配且dirty build带明确后缀。

### ENG-03

release pipeline从clean tag在固定GCC/Clang、dependency lock和真实OpenSSL容器构建，运行unit/regression/isolation/protocol/crash/compat并产JUnit/JSON、编译器和环境指纹。制品只在所有门通过后签名；报告长期归档且可复现。

### ENG-04

接入SQLLogicTest、PG regression/isolation schedule转换器、ORM suite和grammar-based differential generator；每个bug先加最小case。按feature manifest分片并固定seed，失败保存数据库和query；结果规范化只允许声明字段。

### ENG-05

为Lexer/Parser/Expr/Protocol/WAL/Page/Catalog/Backup提供libFuzzer/AFL入口和结构化mutator，seed来自生产/PG corpus，crash自动minimize并进回归。每target设内存/时间，sanitizer常开；覆盖率和无新crash时长是release指标。

### ENG-06

所有durable/alloc/thread/lock操作经可注入wrapper并有稳定point ID；runner枚举errno/partial write/power cut，重启后验证control/WAL/catalog/heap-index约束。采用状态空间削减但关键commit/DDL/checkpoint组合全覆盖；生成kill-point覆盖报告。

### ENG-07

CI矩阵运行ASAN/UBSAN/LSAN、TSAN独立job、debug assertions、O0/O2/O3、GCC/Clang；磁盘格式以显式字节序支持不同主机，32-bit不支持则control/config明确拒绝。任何suppression需owner/expiry，race/UB为release blocker。

### ENG-08

test hook记录transaction invocation/response和real-time order，导出Elle/Jepsen checker可读history；覆盖RC/RR/Serializable、DDL、failover和network fault。generator聚焦冲突key但周期加入全随机；发现nonserializable history保存最小schedule。

### ENG-09

建立24h/7d soak：连接/锁风暴、checkpoint/vacuum/backup/DDL/DML/replication混合并注入慢/满/错误磁盘；持续采集p99、RSS、FD、bloat、WAL、lag和invariant。阈值自动失败，结束执行amcheck/manifest/ledger核对，无“只有crash才算失败”。

### ENG-10

同机同FS/配置运行pgbench、TPC-C/H/DS，固定数据、warm/cold cache、client数，记录TPS/p50/95/99、CPU/RSS/I/O/WAL/temp/plan。结果存版本化JSON并做统计置信区间；性能回归预算按模块，功能不得靠禁用durability取胜。

### ENG-11

维护N-2 data-directory fixtures和catalog migration chain，CI做正常/崩溃中断upgrade、续跑/rollback、升级后crash recovery及逻辑downgrade（dump restore）。每format变更必须给converter和compat test；无法降级须在check阶段明确。

### ENG-12

为支持平台产deb/rpm/tar/container，生成CycloneDX/SPDX SBOM，锁依赖并扫描CVE、签名制品/provenance、验证reproducible build。容器不以root运行且volume权限正确；安装/升级/卸载不得删除data dir，供应链策略进release gate。

### ENG-13

每个release执行threat model和代码审查，覆盖crypto/TLS/auth/protocol/parser/ACL/RLS/path/extension/backup/DoS/log；使用SAST、dependency scan、fuzz和外部渗透。发现项带severity/SLA，critical/high未关闭不发布；安全测试不输出secret。

### ENG-14

定义支持平台/workload/容量、可用性与数据持久SLA、RPO/RTO和明确限制；编写故障诊断、WAL/archive损坏、backup restore、failover/rejoin、key recovery和incident流程。季度演练用自动证据验证手册可执行，不能仅文档评审。

### ENG-15

正式选择“PG18行为兼容”为默认目标；项目扩展置 `compatibility_mode=extended` 和独立schema/命令空间。manifest为每项标exact/extension/unsupported，server启动和version输出不冒充PostgreSQL二进制fork；发布声明必须匹配P0-16证据。

## 22. 非 PostgreSQL 语法和行为偏移

这一组在审计中补充稳定 ID `DIV-01`～`DIV-14`。处理原则统一为：默认 `compatibility_mode=postgresql18` 时使用PG grammar/SQLSTATE；扩展只能在显式 `extended` 模式启用，`SHOW dbms.extensions` 可审计当前偏移。mode是session-start/DB级受限配置，不能在prepared statement执行中偷偷改变语义。

### DIV-01（USE DATABASE）

PG模式Lexer可识别后返回42601/0A000且不切连接；extended模式实现为重新建立session上下文前强制无事务、无portal/temp/listen，并明确它不是原子SQL。推荐客户端重连；验证PG模式状态完全不变，extended模式清理无跨库泄漏。

### DIV-02（REPLACE INTO）

PG模式按syntax error；extended模式在Analyzer转换为明确的INSERT ON CONFLICT策略，但要求用户给唯一target，不能模拟MySQL“delete then insert”副作用。输出tag/RETURNING走ModifyTable；触发器/FK差异写文档和差分测试。

### DIV-03（LOAD DATA INFILE）

PG模式拒绝；extended模式只做COPY FROM的语法糖并使用同一CopyState、ACL、encoding和error options，不另写导入器。EXPLAIN/日志显示规范化COPY；GB级输入性能与COPY相同且路径安全测试通过。

### DIV-04（SELECT INTO OUTFILE）

PG模式严格把SELECT INTO解释为建表，OUTFILE产生syntax error；extended导出使用独立 `EXPORT` 命令或客户端COPY，不劫持SELECT grammar。结果经过CopyTo codec、权限/path检查；验证PG ORM迁移不会误导出文件。

### DIV-05（DESC/VIEW/SHOW 项目命令）

PG模式不接受服务端元命令，只通过catalog支持psql `\d`；extended命令转成只读catalog query并返回typed TupleDesc，不打印自定义文本。每个命令有等价SQL和权限过滤，性能依赖syscache/catalog index。

### DIV-06（MySQL/非 PG 类型别名）

PG模式仅保留PG真实别名，TINYINT/DATETIME/BLOB/NCHAR/unsigned/AUTO_INCREMENT等按PG报错；extended模式在Analyzer显式映射并发NOTICE，catalog保存实际PG兼容type而非假type。dump默认输出canonical type，边界/溢出差异有测试。

### DIV-07（索引快捷语法）

PG模式只接受`CREATE INDEX ... USING am`；extended parser将FULLTEXT/HASH快捷语法生成同一CreateIndexStmt并要求真实AM/opclass完成，否则0A000。catalog/deparse始终canonical，性能和durability走IDX路径。

### DIV-08（CREATE ASSERTION）

两个模式默认返回0A000，因为PG18也未实现且当前无全局constraint runtime；不得保存compat object或返回created。若未来作为项目扩展，必须有增量/并发全局constraint设计、dependency、deferred/crash和成本证明后另立feature。

### DIV-09（普通 SQL 复制槽/逻辑消费命令）

PG模式只支持系统函数与replication protocol规定接口；extended SQL wrapper调用同一Slot/LogicalDecoding API并执行相同权限、事务限制和typed output。禁止另存slot状态；协议/函数/wrapper三路advance结果与WAL retention一致。

### DIV-10（DUMP/BACKUP/RESTORE/CLEAR PLAN CACHE）

PG模式拒绝非reference命令，使用pg_dump/base backup API和DISCARD/函数；extended wrapper只提交后台job并返回typed job id/status，不在SQL线程复制整库。job复用BACKUP/PlanCache实现，有限速/取消/权限和crash resume。

### DIV-11（SET GLOBAL/SHOW）

PG模式按GUC的SET SESSION/LOCAL、ALTER SYSTEM和SHOW规则；extended SET GLOBAL映射ALTER SYSTEM前做superuser/context检查并NOTICE，不能直接改全局内存。GUC registry是唯一状态，SIGHUP/restart/pending结果与canonical命令一致。

### DIV-12（内置连接池/TDE/备份扩展）

保留为`dbms_*`命名扩展或外部进程，catalog/view不占用PG保留对象并清楚暴露版本；连接池transaction/statement mode必须执行session-state reset审计。各扩展有独立SLA/性能/安全测试，不能用于宣称PG核心差距已关闭。

### DIV-13（端口/目录/CWD 身份偏移）

可以保留自有默认端口和目录，但启动必须显式data dir、写本项目magic/system id，握手server_version说明compat层且不声称文件可被PG打开。所有运行状态离开CWD；工具面对错误cluster立即拒绝，双产品同机测试无路径冲突。

### DIV-14（骨架命令假成功）

立即把所有仅写compat record的路径改为先检查feature manifest并返回0A000；迁移时只读旧记录生成诊断，不自动激活。命令完成必须由真实runtime返回且事务/错误/command tag通过对应测试；CI扫描 `.pg_compat_objects` 写调用和误导tag为零。

## 23. 每项实施工单的固定模板

开始任何条目前，把本蓝图条目展开为工单，并必须同时提交：

```text
gap_id / PG18 reference section / current source evidence
accepted SQL + rejected SQL + raw input/encoding cases
typed AST/query/plan/catalog/storage/WAL/protocol changes
success rows/types/tag + error SQLSTATE/fields + transaction status
lock/snapshot/crash invariants + migration/rollback
memory limit + I/O complexity + contention analysis + benchmark
unit + differential + isolation + wire + fault/crash + upgrade tests
docs/status manifest update (only after all tests pass)
```

禁止拆成“先返回成功，runtime以后再补”的工单；一个大项可以按内部里程碑拆PR，但feature gate只能在完整闭环后开启。

## 24. 可执行的覆盖和验收

新增代码时应同步创建：

- `tests/compat/manifest.yaml`：273个gap ID、183个command、PG文档锚点、状态和测试集合。
- `tests/compat/run_differential.py`：同驱动PG18.6与本项目，比较typed rows/errors/catalog。
- `tests/protocol/golden/`：startup/auth/simple/extended/COPY/cancel/pipeline/replication字节序列。
- `tests/isolation/specs/`：移植并扩展PG schedule。
- `tests/fault/points.yaml`：持久化kill point和期望恢复invariant。
- `bench/baselines/pg18/`：同机plan、吞吐、延迟、CPU/RSS/I/O/WAL基线。

完成判据由脚本计算，不能人工改README勾选：manifest中某gap的必需测试全绿、无未过期allowlist、性能未超预算且所依赖gap已完成，才允许状态从`open`变`complete`。

## 25. 依赖关键路径

```text
typed Datum/result/error
    -> lexer/analyzer/binder/rewrite
        -> catalog唯一事实源 -> 事务化DDL -> ACL/RLS/extension
        -> typed planner/executor -> 完整query/DML -> protocol/client
storage control/page/buffer
    -> 全资源WAL/checkpoint -> MVCC/MultiXact/VACUUM/index AM
        -> crash/PITR/base backup -> physical standby
        -> WAL logical decoding -> pgoutput/subscription
以上全部 + differential/isolation/fault/perf/upgrade证据
    -> PostgreSQL 18 compatible 发布声明
```

不能把复制、备份或扩展提前接到旧字符串/sidecar真源上；那会产生第二轮必须推翻的实现。可并行的是同一稳定接口后的不同类型codec、不同index AM、不同catalog view和客户端测试。

## 26. PostgreSQL 18 规范来源

- [PostgreSQL 18.6 Documentation](https://www.postgresql.org/docs/18/)
- [SQL Commands](https://www.postgresql.org/docs/18/sql-commands.html)
- [Frontend/Backend Protocol](https://www.postgresql.org/docs/18/protocol.html)
- [System Catalogs](https://www.postgresql.org/docs/18/catalogs.html)
- [Type Conversion](https://www.postgresql.org/docs/18/typeconv.html)
- [Indexes](https://www.postgresql.org/docs/18/indexes.html)
- [Concurrency Control](https://www.postgresql.org/docs/18/mvcc.html)
- [Reliability and WAL](https://www.postgresql.org/docs/18/wal.html)
- [High Availability and Replication](https://www.postgresql.org/docs/18/high-availability.html)
- [Backup and Restore](https://www.postgresql.org/docs/18/backup.html)
- [Extending SQL](https://www.postgresql.org/docs/18/extend.html)
- [Monitoring Database Activity](https://www.postgresql.org/docs/18/monitoring.html)

这份蓝图是实现约束，不是工期承诺。数据库兼容和可靠性只能由可重复证据证明；任何未通过上述完成定义的条目都保持未完成。

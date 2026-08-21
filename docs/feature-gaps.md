# 功能缺失清单 (Feature Gaps)

> 生成日期: 2026-08-14
> 基于 `docs/postgresql-comparison.md` 代码验证结果整理
> 本 DBMS 当前状态（2026-08-14）: 生产化重构进行中，统一回归基线 PASS=139 FAIL=0（137 个 C++ 测试 + 协议 E2E + 窗口函数 E2E）；v2/8 KiB 存储格式已统一，heap 文件头和页面完整性校验 fail-closed，旧数据不迁移；数据库初始化、checkpoint 和物理备份标记采用原子持久化，部分创建失败会清理目录；事务提交现在先保证 COMMIT WAL 刷盘再发布 CLOG 可见性，PREPARE 前刷 heap/index 缓存并原子 durable 发布 prepared 元数据，跨进程可恢复表/row/page/gap 锁；snapshot export/import 已升级为数据库绑定的 v2 格式并限制在首次读写前导入，但完整 DDL 依赖事务和全局 2PC 语义仍在建设。

WAL 当前明确区分合法 LSN 0 与无效哨兵；恢复会忽略未初始化页的无效页 LSN，多个 WAL writer 通过进程/文件锁和磁盘尾部刷新串行化追加。heap 使用 page before/after image，B+Tree/Hash 使用完整索引文件 before/after image；同事务重复页 before-image 已去重，WAL 追加维护增量尾部状态（常开 fd + 按库互斥）。归档复制采用临时文件、文件/目录 `fsync` 和原子状态发布，失败保留 `.ready` 供重试；GIN/GiST/SP-GiST/BRIN、原生 page-level 索引 WAL、PITR 和完整 PostgreSQL 恢复语义仍属于差距。

2026-08-14 性能与并发硬化轮次已完成（详见 production-status.md）：元数据/schema/序列内存缓存、缓冲池扩容（256/128 帧，环境变量可覆盖）与页校验降频（仅磁盘加载时）、B+ 树二分查找 + 节点缓存、BufferPool 磁盘 I/O 移出池锁（单加载者规则 + 孤儿帧失效语义，TSAN/ASAN 压测 0 竞态 0 损坏）、FSM/VM/PageAllocator/索引内部锁补全、INSERT 意图锁降级。性能差距项（缓冲池大小、逐条校验、线性查找、每行元数据重读）不再是缺口；仍未覆盖的差距：查询优化器代价模型、并行查询、page-level 索引 WAL、PITR、多进程 postmaster 架构。

本文件列出与 PostgreSQL 18 生产级完整度的所有差距，按优先级分级，
每项标注类别、影响范围、预估工作量，供下一阶段实施参考。

执行器架构当前以 `src/executor/ExecutionPlan.{h,cpp}` 为唯一 Volcano 算子实现，
`src/interfaces/executor.h` 只保留实际使用的 `IOperator` 契约；未接入的旧计划/表达式
接口已删除。索引回表按 RID 直接读取并复用 page lock/MVCC 边界，但复杂 SQL producer
仍可能通过明确的 legacy/materialized-row 适配边界进入算子树。唯一执行入口为
`executePlanChecked()`，显式区分 EOF 与执行失败；旧的空结果兼容入口已经删除。

DDL CREATE 失败清理已覆盖 view、materialized view、UDF/TVF、procedure、trigger、RLS policy 和 collation；显式外层事务的 CREATE undo 及变更前快照恢复已接入 StorageEngine 事务上下文，外层 `ROLLBACK` 可恢复 DROP/REPLACE 并继续清理行级变更。为避免复用整库快照破坏语句原子性，快照已污染后另一条快照型 DDL 会 fail-closed；当前仍缺完整依赖图回滚、DDL 后 SAVEPOINT 的完整子事务语义和 PostgreSQL 隐式提交边界；含内存闭包式 DDL undo 的事务暂不支持 PREPARE TRANSACTION。

表空间物理路由已在本轮收敛：关系文件统一放在 `pg_default` 数据库目录或
`<LOCATION>/<DATABASE>/`，CREATE TABLE、ALTER TABLE SET TABLESPACE、重启读取和
跨文件系统迁移均有回归覆盖；缺失 `.path` marker 不会回退到默认目录。剩余差距是
权限/owner、ALTER TABLESPACE 完整语义，以及 PostgreSQL OID/符号链接布局。

协议当前已通过真实 E2E 覆盖常用标量、`numeric` 以及 `date`/`time`/`timestamp`/`timestamptz`/`uuid` 的 binary 参数与结果、基础 portal `maxRows` 分页；legacy 文本执行器的结果捕获已采用线程局部输出路由，不再以全局锁串行化会话；数组 binary I/O、亚秒时间精度、holdable/scrollable cursor 和完整 libpq 语义仍属于协议差距。

本轮 DML 架构进展：普通单表 `INSERT ... VALUES`/`DEFAULT VALUES`、无 JOIN/聚合/排序的单表 `INSERT ... SELECT`、无 target 或显式匹配主键/唯一约束 target 的 `ON CONFLICT DO NOTHING`，以及显式匹配单列或复合主键/唯一约束 target、配合常量或只引用 `excluded` 的 evaluator 受限标量表达式 `SET`、目标行/`excluded` 受限 `WHERE` 的窄版 `ON CONFLICT DO UPDATE`、以当前目标行列值为输入的受限标量表达式单表 `UPDATE`、单源表 `UPDATE ... FROM`、单源表 `DELETE ... USING` 和简单谓词单表 `DELETE` 已进入 `src/commands/DmlExecutor.cpp` 的 AST 执行路径；UPDATE FROM/DELETE USING 进一步支持来源 INNER/CROSS JOIN、来源别名、限定连接谓词和受限 `RETURNING`。解析器保留混合 `DEFAULT` 的列位置并对 DML 尾随垃圾 fail-closed。普通单表 INSERT/UPDATE/DELETE 的列投影和 evaluator 支持的受限标量表达式 `RETURNING` 也已由 StorageEngine 在实际修改边界收集，并由协议层作为结构化结果集发送。视图写入、复杂 `INSERT ... SELECT`、部分/索引推断 conflict target、引用子查询或其他关系的 `DO UPDATE` 表达式/`WHERE`、复杂/子查询/窗口 `RETURNING`、外连接/复杂 JOIN、多表 DML 和复杂表达式仍明确由 legacy 路径处理。

普通顶层 `INSERT`/`UPDATE`/`DELETE`/`MERGE`/`REPLACE` 以及包含写 CTE 的 `WITH` 语句已统一包在内部事务中，成功后自动提交，错误或异常自动回滚；触发器、视图 action、CTE 和兼容性辅助执行通过递归深度复用同一事务边界，`WITH ... RETURNING` 的简单结果可经协议返回。显式 `BEGIN` 仍由连接事务管理，复杂 DML 的语义支持范围未因此扩大。

RLS 当前执行路径已补齐 PostgreSQL 基础策略组合：策略默认为 `PERMISSIVE`，`AS RESTRICTIVE` 策略要求同时通过；显式 `TO PUBLIC` 与空角色列表均按 PUBLIC 处理；`ALL`/`UPDATE` 省略 `WITH CHECK` 时继承 `USING`；`NOINHERIT` 不自动继承成员角色权限，`SET ROLE` 后按有效角色评估；表 owner、`SUPERUSER`/`BYPASSRLS` 绕过普通 RLS，`FORCE ROW LEVEL SECURITY` 重新强制执行。表 owner 已写入正式 schema 与 `pg_class.relowner`，并在统一 ACL 查询中获得隐含表/列权限和 `GRANT OPTION`；表级 `GRANT OPTION` 支持独立撤销、依赖授权的 `RESTRICT`/`CASCADE`，`pg_auth_members.admin_option` 已接入角色授权与撤销；`ALTER TABLE ... OWNER TO` 会同步更新两者并执行所有者/目标角色授权检查；查询、更新、删除和结构化 DML 来源仍共享关系感知扫描。Volcano SELECT 在 RLS 生效时显式禁用索引、bitmap 和并行访问路径，统一通过 `forEachVisibleRow(..., "SELECT")`，避免优化路径成为安全绕过；复杂对象 owner/ACL 组合语义仍属于差距。

### P1-0: DML AST 全量执行迁移
- **类别**: 执行器 / 架构一致性
- **现状**: 普通单表 `INSERT ... VALUES` / `DEFAULT VALUES`、简单单表 `INSERT ... SELECT`、无 target 或显式匹配主键/唯一约束 target 的 `ON CONFLICT DO NOTHING`、显式匹配单列或复合主键/唯一约束 target 的常量或只引用 `excluded` 的 evaluator 受限标量表达式 `DO UPDATE`、目标行/`excluded` 的受限 `WHERE`、以当前目标行列值为输入的受限标量表达式单表 UPDATE、单源表 UPDATE FROM、单源表 DELETE USING、简单谓词单表 DELETE 和窄版单源表 MERGE 已由 `DmlExecutor` 结构化执行，并通过 parser/协议回归；其中 UPDATE FROM/DELETE USING 支持来源 INNER/CROSS JOIN，MERGE 支持单个 MATCHED UPDATE/DO NOTHING、单个 NOT MATCHED INSERT/DO NOTHING，并拒绝多源行匹配同一目标行。普通单表 INSERT/UPDATE/DELETE 的列投影和 evaluator 支持的受限标量表达式 RETURNING 已统一结果集和 command tag。复杂 INSERT SELECT、部分/索引推断 conflict target、引用子查询或其他关系的 `DO UPDATE`/`WHERE`、复杂/子查询/窗口 RETURNING、外连接/复杂 UPDATE FROM/DELETE USING、MERGE 的多 WHEN/BY SOURCE/BY TARGET/DELETE/复杂 source/RETURNING 以及视图写入仍 fail-closed 或回退 legacy。
- **PG 参考**: PostgreSQL 的 parse/analyze/rewrite/plan/execute 分层，以及 `ModifyTable`。
- **影响**: 当前 DML 仍存在两套执行入口，错误码、RETURNING、权限和计划可观测性的统一程度不足。
- **实现路径**:
  1. ✅ 抽出普通 INSERT AST executor，并验证多行、DEFAULT、StorageEngine 约束和 ACL。
  2. ✅ 抽出单表 UPDATE（含受限行级标量表达式）/简单谓词 DELETE AST executor，并验证触发器、RLS/FK/索引写入入口和 fail-closed 解析。
  3. ✅ 迁移普通单表 INSERT/UPDATE/DELETE 的列投影和 evaluator 支持的受限标量表达式 RETURNING，统一结果集与 command tag；复杂/子查询/窗口表达式仍待迁移。
  4. 迁移复杂 INSERT SELECT、部分/索引推断 conflict target、子查询/其他关系 `DO UPDATE`/`WHERE`、复杂 RETURNING、UPDATE/DELETE 复杂表达式与多表语义，统一 `ModifyTable` 风格执行。
  5. 删除已完全迁移子集对应的 legacy 分支，并为每个回退边界增加 fail-closed 测试。
- **相关文件**: `src/commands/DmlExecutor.{h,cpp}`, `src/parser/ast.h`, `src/main.cpp`, `src/commands/TableManage.cpp`

---

## 总览

| 优先级 | 数量 | 说明 |
|--------|------|------|
| P0 严重缺失 | 7  | 生产级必需，无此功能不可上线 |
| P1 重要缺失 | 13 | 影响实用性，大多数应用需要 |
| P2 次要缺失 | 13 | 易用性/运维/性能优化 |
| P3 锦上添花 | 7  | 企业级/兼容性/生态 |

> 2026-08-16 对照代码复核后新增 8 项（P1-10 SQL 级 prepared statements 对齐、P1-11 EXPLAIN ANALYZE 每节点统计、P1-12 统计驱动代价模型（消费侧）、P1-13 并行 JOIN/聚合、P2-9 deferrable 扩展、P2-10 数组表达式语义、P2-11 表继承 CREATE 侧、P2-12 interval 运算/时区、P2-13 dollar-quoting、P3-7 全文检索执行链路）。每项「现状」均先经代码检索核实已有部分（如 MCV/直方图已采集、`ALTER TABLE INHERIT` 已有、`= ANY()` 经 rewrite 支持、interval 字面量已解析），差距描述只列真正缺失的部分。

---

## P0 — 严重缺失 (生产级必需)

### P0-1: 并行查询执行
- **类别**: 性能 / 执行器
- **现状**: 已实现 `ParallelTableScanOp`：非分区 heap 按 page range 分片，由多个 worker 读取并按范围顺序 Gather；事务内和分区表安全回退到普通扫描。尚无并行 join/aggregate、GatherMerge、work-stealing worker pool 或完整 parallel-aware planner。
- **PG 参考**: `parallel_workers`, `parallel_leader_participation`, `Gather`/`GatherMerge` 节点
- **影响**: 大表全表扫描、聚合、JOIN 无法利用多核，性能差距 10x+
- **实现路径**:
  1. ✅ 在 `QueryPlanner` 中接入 `ParallelTableScanOp`，按 heap page range 分片
  2. ⚠️ 当前使用每次查询创建的固定 worker，仍需独立 ThreadPool/work-stealing 生命周期
  3. ⚠️ 已有确定性 Gather，仍需 GatherMerge、并行 HashJoin/Aggregate
  4. ✅ 增加 `max_parallel_workers_per_gather` 配置入口
- **预估工作量**: 2-3 周
- **相关文件**: `src/executor/ExecutionPlan.{h,cpp}`, `src/commands/TableManage.{h,cpp}`, `src/common/Config.{h,cpp}`

### P0-2: JIT 编译 (LLVM)
- **类别**: 性能 / 表达式求值
- **现状**: `ExprEvaluator` 纯解释执行，无编译优化
- **PG 参考**: `llvmjit_expr`, `llvmjit_deform`
- **影响**: 复杂表达式、聚合计算性能差距 3-5x
- **实现路径**:
  1. 引入 LLVM C API 或 `libclangJIT` 依赖
  2. 实现 `JITCompiler` 类：将 `Expr` 树翻译为 LLVM IR
  3. 在 `FilterOp` / `ProjectOp` / `GroupAggregateOp` 中热点路径使用 JIT
  4. 增加 `jit` GUC 开关 + `jit_above_cost` 阈值
- **预估工作量**: 3-4 周（依赖 LLVM 库）
- **相关文件**: `src/expression/ExprEvaluator.cpp`

### P0-3: GiST 索引
- **类别**: 索引 / 全文搜索 / 几何
- **现状**: 仅有 SP-GiST (Point quadtree)，无通用 GiST
- **PG 参考**: `gist`, `tsvector` 全文搜索, `geometry` 最近邻
- **影响**: 全文检索、地理空间查询、范围类型索引缺失
- **实现路径**:
  1. 实现 `GistIndex` 基类：统一接口 `consistent()`, `union()`, `compress()`, `decompress()`, `penalty()`, `picksplit()`
  2. 实现内置 operator class: `gist_tr_ops` (tsvector), `gist_int4_ops` (int4 range)
  3. 接入 `IndexScanOp` 路径
  4. 实现 `@@` 全文搜索操作符 + `to_tsvector()` / `to_tsquery()` 函数
- **预估工作量**: 2-3 周
- **相关文件**: `src/access/GistIndex.cpp` (新建), `src/access/GistIndex.h` (新建)

### P0-4: Gap Locks / Predicate Locks
- **类别**: 并发控制 / 隔离性
- **现状**: 已有行级锁、表级锁、简化 gap 锁；SERIALIZABLE 非空索引谓词和顺序扫描已登记页级 SIREAD，单列主键/二级 B+Tree 的比较谓词另有事务级逻辑索引 SIREAD，空结果/无法安全暴露页来源的谓词保留关系级兜底；仍无 PostgreSQL 真正的物理索引范围 predicate lock
- **PG 参考**: `gap_lock`, `predicate_lock`, `SIReadLock`
- **影响**: 复合/表达式/部分索引及其他访问方法的精确 phantom 覆盖仍不完整，存在幻读风险
- **实现路径**:
  1. 在 `LockManager` 中增加 `GapLock` 结构：`(table, gapRange, mode)`
  2. ✅ 增加关系级 SIREAD 覆盖，保护空范围读
  3. ✅ 增加 heap page 级 SIREAD 覆盖，区分非相交页的读写
  4. ✅ 在 SSI 事务集合中实现单列主键/二级 B+Tree 比较谓词与写键的逻辑重叠检测
  5. 在 `IndexScanOp` 遍历时获取物理索引范围 Gap 锁，并覆盖复合/表达式/部分索引
  6. 增加其他访问方法、safe snapshot 与完整 `serializable` rw-conflict 规则
- **预估工作量**: 1-2 周
- **相关文件**: `src/transaction/LockManager.cpp`, `src/transaction/LockManager.h`

### P0-5: SSI (Serializable Snapshot Isolation)
- **类别**: 并发控制 / 隔离级别
- **现状**: SERIALIZABLE 已跟踪关系限定的行级读写集合、非空扫描的页级 SIREAD、单列 B+Tree 逻辑索引谓词和空结果/不安全谓词读的关系级兜底；真实写偏差、空谓词、跨页危险结构、非相交页并发和索引键重叠回归可返回正确结果，仍不等价于 PostgreSQL 完整 SSI
- **PG 参考**: `SERIALIZABLE` + `SIREAD` + `SERIALIZATION_FAILURE`
- **影响**: 复合/表达式/部分索引及其他访问方法的精确 phantom 推理、完整 rw-conflict 图规则和安全快照仍缺失；空结果关系级兜底仍会牺牲部分串行化并发度
- **实现路径**:
  1. ✅ 实现行级读写集合与关系级 SIREAD 跟踪
  2. ✅ 在 `COMMIT` 时将行级/页级/关系级覆盖纳入序列化冲突检测
  3. ✅ 冲突时返回 `SERIALIZATION_FAILURE` 错误码
  4. ✅ 增加单列 B+Tree 逻辑索引谓词/写键及提交时重叠检测
  5. 增加物理索引范围 predicate lock、完整 rw-conflict 图、安全快照和 `serialization_failure_retries` GUC
- **预估工作量**: 1-2 周
- **相关文件**: `src/transaction/LockManager.cpp`, `src/commands/TableManage.cpp`

### P0-6: Bitmap Index/Heap Scan
- **类别**: 执行器 / 索引组合
- **现状**: `BitmapHeapScanOp` 和 `BitmapOrHeapScanOp` 已接入 planner：对等值 B-tree/Hash/PK 索引执行候选 RID 的 AND/OR 组合，再通过带 page lock/MVCC/SSI 边界的 RID heap fetch 与原谓词重检；范围 bitmap、并行 bitmap、真正 block bitmap 和 visibility-map 驱动 index-only scan 仍缺
- **PG 参考**: `BitmapIndexScan`, `BitmapHeapScan`, `BitmapAnd`, `BitmapOr`
- **影响**: 多索引 WHERE 条件性能差（如 `WHERE a=1 AND b=2` 有两个索引时）
- **实现路径**:
  1. ✅ 实现多等值索引候选 RID 集合
  2. ✅ 在 `BitmapHeapScanOp` 中执行 Bitmap AND 和 heap fetch
  3. ✅ 实现 Bitmap OR、分支 union 和原谓词重检
  4. 增加范围条件、并行 bitmap 和真正 page/block bitmap 表示
  5. ✅ 在 `QueryPlanner` 中为 AND/OR 等值索引生成 bitmap path
- **预估工作量**: 1 周
- **验证**: `tests/volcano_select_phase51_test.cpp` 与 `tests/postgres_protocol_test.py` 覆盖双索引 AND/OR、heap recheck 和节点选型；`tests/gin_brin_index_test.cpp` 覆盖 B+Tree 跨叶/内部节点分裂与范围扫描
- **相关文件**: `src/executor/ExecutionPlan.{h,cpp}`

### P0-7: INSTEAD OF 视图触发器
- **类别**: 触发器 / 数据完整性
- **现状**: 已支持在视图上创建行级 `INSTEAD OF INSERT/UPDATE/DELETE`，并通过协议连接执行 action SQL；`NEW`/`OLD` 变量、`WHEN`、动作失败传播、server/CLI 会话执行和简单单表视图的逐行派发已接入
- **剩余差距**: 尚未完整支持复杂 `INSERT ... SELECT`（JOIN/聚合/排序/CTE 等）、复杂视图行映射、完整 `RETURNING`、transition tables，以及真正的 `EXECUTE FUNCTION`/PL 触发器运行时
- **PG 参考**: `CREATE TRIGGER ... INSTEAD OF INSERT OR UPDATE OR DELETE ON view FOR EACH ROW`
- **验证**: `tests/postgres_protocol_test.py` 覆盖通过视图触发器插入、更新、删除底表行
- **相关文件**: `src/commands/DdlExecutor.cpp`, `src/main.cpp`, `tests/postgres_protocol_test.py`

---

## P1 — 重要缺失 (影响实用性)

### P1-1: Window Function Executor
- **类别**: 执行器 / 分析函数
- **现状**: 常见排名/偏移函数、窗口聚合和 `ROWS/RANGE/GROUPS` frame/exclusion、OFFSET 已通过 Volcano `WindowOp`/`OffsetOp` 执行；复杂表达式仍回退 legacy `main.cpp` 路径
- **PG 参考**: `WindowAgg` 节点, `row_number()`, `rank()`, `lag()`, `lead()` 等
- **影响**: 常见分析查询已具备结构化执行器；复杂窗口表达式和完整 planner/explain 语义仍未达到 PostgreSQL 级别
- **实现路径**:
  1. ✅ 实现 `WindowOp` 算子：按每个窗口的 PARTITION BY 分组 + ORDER BY 排序
  2. ✅ 接入 `row_number`, `rank`, `dense_rank`, `lag`, `lead`, `first_value`, `last_value`, `ntile`, `percent_rank`, `cume_dist` 及常见窗口聚合
  3. ✅ 在 `QueryPlanner` 中识别稳定窗口子集并插入 `WindowOp`
  4. ✅ 为结构化 `WindowOp` 增加 `WindowAgg` 文本/JSON EXPLAIN 输出
  5. ⚠️ 将复杂目标列表和主 SQL EXPLAIN 的窗口解析迁移到结构化执行器
- **预估工作量**: 1-2 周（剩余完整语义）
- **相关文件**: `src/executor/ExecutionPlan.{h,cpp}`, `src/main.cpp`

### P1-2: UNION/INTERSECT/EXCEPT Executor
- **类别**: 执行器 / 集合操作
- **现状**: 所有 operand 的集合组合已统一由 Volcano `SetOperationOp` 执行，parser AST 已按 PostgreSQL 优先级和链式左结合构建，支持错误传播以及 `UNION/INTERSECT/EXCEPT [ALL]` 的重复行语义；简单单表 SELECT 直接构建子计划，复杂 operand 暂由 legacy producer 经 `MaterializedRowsOp` 接入
- **PG 参考**: `Append` (UNION), `HashSetOp` (INTERSECT/EXCEPT)
- **影响**: 复杂 operand 的数据生产仍不是完整结构化计划，类型合并、collation 和完整排序/分页作用域仍缺
- **实现路径**:
  1. ✅ 将 legacy operand 结果通过 `MaterializedRowsOp` 接入结构化集合节点
  2. ✅ 实现 `SetOperationOp`：合并两个 Volcano 子计划并执行 UNION/INTERSECT/EXCEPT [ALL]
  3. 将 parser AST 的集合操作直接下推到 `QueryPlanner`，覆盖 JOIN/聚合/表达式 operand
  4. 补齐类型合并、collation、排序/分页作用域
- **预估工作量**: 3-5 天
- **验证**: `tests/set_operation_volcano_test.cpp` 与 `tests/postgres_protocol_test.py` 覆盖 UNION、UNION ALL、INTERSECT、INTERSECT ALL、EXCEPT、EXCEPT ALL
- **相关文件**: `src/main.cpp`, `src/executor/ExecutionPlan.cpp`

### P1-3: GROUP BY ROLLUP/CUBE/GROUPING SETS Executor
- **类别**: 执行器 / 分组扩展
- **现状**: 无 GROUP BY 的普通聚合、常见 `GROUP BY`、`ROLLUP`、`CUBE`、`GROUPING SETS`、`HAVING` 已统一接入结构化 `GroupAggregateOp`；复杂表达式、`GROUPING()`/`GROUPING_ID`、完整排序作用域仍回退 legacy
- **PG 参考**: `GroupAggregate` + `sortgroups`, `GROUPING()` 函数
- **影响**: 基础分组查询可使用 Volcano 执行器，多维分析的完整 PostgreSQL 语义仍未覆盖
- **实现路径**:
  1. ✅ 新增 `GroupAggregateOp`，消费过滤后的 Volcano 子计划，并覆盖无 GROUP BY 的隐式空 grouping set
  2. ✅ 支持常见 `count/sum/avg/min/max/bool_and/bool_or/every`、简单 `FILTER`/`HAVING`
  3. ✅ `ROLLUP/CUBE/GROUPING SETS` 生成多个分组集合并输出省略列的 `NULL`
  4. 为结构化目标列表实现类型检查、`GROUPING()`/`GROUPING_ID` 和完整排序/分页作用域
- **预估工作量**: 3-5 天
- **相关文件**: `src/executor/ExecutionPlan.cpp`, `src/main.cpp`, `tests/volcano_select_phase51_test.cpp`

### P1-4: 子查询结构化解嵌套 (Subquery Unnesting)
- **类别**: 优化器 / 子查询
- **现状**: 未关联的单列 `IN`/`NOT IN` 已由主 SQL 路径识别并下推为结构化 `SemiJoinOp`/anti 模式；未关联的单表 `EXISTS`/`NOT EXISTS`（含简单内层 `WHERE`）已下推为 `ExistenceFilterOp`；单个未关联标量目标已下推为 init-plan + `ScalarSubqueryProjectOp`，严格执行 0 行为 NULL、超过 1 行报错；单列未关联 `ANY/ALL` 已下推为 `QuantifiedSubqueryFilterOp`，保留 NULL/空集三值逻辑；关联子查询、复杂标量目标、row comparison 和复杂布尔组合仍回退 legacy
- **PG 参考**: `pull_up_subqueries`, `convert_EXISTS_to_join`
- **影响**: 关联子查询性能差（O(n*m) 嵌套循环）
- **实现路径**:
  1. ✅ 在 `QueryPlanner` 中实现单列未关联 IN/NOT IN 到 SEMI/ANTI 计划的下推
  2. ✅ 实现 `SemiJoinOp`（anti 模式）及 inner Filter 子计划
  3. ✅ 实现未关联单表 `EXISTS`/`NOT EXISTS` 的 `ExistenceFilterOp` 及 inner Filter 子计划
  4. ✅ 实现单个未关联标量目标的 init-plan、NULL 和 cardinality error 语义
  5. ✅ 补齐单列未关联 `ANY/ALL` 的结构化量化过滤、NULL/空集语义与 EXPLAIN
  6. 补齐复杂标量目标、关联性、row comparison 和完整 planner 入口
- **预估工作量**: 1-2 周
- **相关文件**: `src/executor/ExecutionPlan.cpp`

### P1-5: TOAST 压缩
- **类别**: 存储 / 大字段
- **现状**: 当前 TOAST relation 对线外值统一使用 zlib 压缩（不可压缩时保留原文），chunk header 保存压缩标记和原始长度；尚无 PostgreSQL 的 lz4/pglz、列级 storage strategy 和 `toast_tuple_target`
- **PG 参考**: `TOAST` + `lz4`/`pglz` 压缩策略
- **影响**: 基础大字段压缩已可用，但与 PostgreSQL 的压缩算法、reloptions 和 pointer/catalog 语义仍有差异
- **实现路径**:
  1. ✅ 引入 zlib 并纳入 shell/CMake 统一构建入口
  2. ✅ 在 TOAST 写入路径中自动压缩，读取时校验并解压
  3. 为 lz4/pglz、`PLAIN`/`EXTENDED`/`EXTERNAL`/`MAIN` 策略和 `toast_tuple_target` 建模
- **预估工作量**: 3-5 天
- **相关文件**: `src/commands/TableManage.cpp`, `scripts/build_common.sh`, `CMakeLists.txt`, `tests/toast_test.cpp`

### P1-6: 后台统计收集器 (Stats Collector)
- **类别**: 可观测性 / 运行时统计
- **现状**: 已有线程安全 `process/RuntimeStats`，在 SQL 执行、StorageEngine、Volcano 顺序/索引扫描算子和 DML 边界记录数据库/表级运行时计数；`SHOW STATUS`、`pg_stat_database`、`pg_stat_tables` 已消费真实数据，协议回归覆盖 `idx_scan`/`idx_tup_fetch`。当前格式的 `.runtime_stats` 已在 checkpoint/引擎关闭时以 sidecar flock、临时文件、fsync 和原子 rename 持久化，并在启动时严格校验加载；多 backend 采用增量合并，关系重建/删除不会继承旧估计。仍无独立后台采样线程、按索引方法的完整分类和 planner 深度反馈。
- **PG 参考**: `pg_stat_database`, `pg_stat_user_tables`, `pg_stat_activity`
- **影响**: 基础运行监控已可用且正常 checkpoint/重启可保留当前快照；统计维度、后台采样和 planner 反馈仍不足以替代 PostgreSQL 的完整 collector
- **实现路径**:
  1. ✅ 共享 `RuntimeStats` 在执行器/存储边界记录数据库和表级计数，包含顺序扫描、索引扫描和索引取行
  2. ✅ 实现 `pg_stat_*` 统计子集 + `SHOW STATUS` 运行时字段
  3. ✅ 在 `QueryPlanner` 中使用带来源有效性的运行时 live-row 估计做 Join 成本与 EXPLAIN 行数估计，未知时回退物理计数
  4. 补充独立后台采样线程、按索引方法细分和 `stats_collector` GUC
- **预估工作量**: 1 周
- **相关文件**: `src/process/RuntimeStats.{h,cpp}`, `src/main.cpp`, `src/network/NetworkServer.cpp`, `src/commands/TableManage.cpp`

### P1-7: PL/pgSQL 运行时 ✅（最小实现，2026-08-20）
- **类别**: 存储过程 / 扩展语言
- **现状**: ✅ 最小 PL/pgSQL 解释器已落地：UDF 体按 `DECLARE/BEGIN/END` 解析为语句树（Compound/Assign/Sql/Return/Exit/Raise/If/While/For），两阶段 parse→interp 执行；表达式求值复用 SQL 表达式求值器（右结合扫描保证左结合、括号深度、一元负号、宿主变量回显）；`RETURNING ... INTO`/`SELECT ... INTO` 支持返回值装填。`tests/plpgsql_test.cpp` 覆盖赋值/IF/WHILE/FOR/RAISE/嵌套表达式
- **PG 参考**: `plpgsql` 解释器, `CALL proc()`
- **残余缺口**: 异常块（`EXCEPTION`）、`EXIT WHEN` 之外的游标控制、`PERFORM`、触发器函数体、`plpgsql_check` 类静态校验
- **实现路径**:
  1. ✅ 实现 `PlPgsql`：解析 PL/pgSQL 语句树（`src/utils/plpgsql.{h,cpp}`）
  2. ✅ 支持基本语法: `IF/ELSE`, `LOOP`, `FOR`, `WHILE`, `EXECUTE`(SQL), `RETURN`, `RAISE`
  3. ✅ DECLARE 变量声明与默认值求值
  4. ✅ 在 UDF 调用路径接入解释器（language=plpgsql）
- **预估工作量**: ~~2-3 周~~ 已完成最小集
- **相关文件**: `src/utils/plpgsql.cpp` (新建), `src/utils/plpgsql.h` (新建), `src/commands/TableManage.cpp`

### P1-8: 并行 Vacuum ✅（2026-08-20）
- **类别**: 存储 / 维护
- **现状**: ✅ `VACUUM (PARALLEL n)` 已实现：heap 页按 `(pages + n - 1) / n` 分片，worker 线程池并行回收死元组并回填 FSM；索引清理在合并阶段串行执行（PG 亦然，基于 bulkdelete 语义）。`CONCURRENTLY` 与 `PARALLEL` 可组合。`tests/parallel_vacuum_test.cpp` 验证分片覆盖、多线程回收正确性与 FSM 一致性
- **PG 参考**: `VACUUM (PARALLEL n)`
- **残余缺口**: 独立 `parallel_vacuum_worker` 后端进程池、按索引大小的负载均衡（leader 参与回收）、`PARALLEL` 上限自动推导（`min(maintenance_workers, indexes/2)`）
- **预估工作量**: ~~3-5 天~~ 已完成
- **相关文件**: `src/commands/TableManage.cpp`, `src/main.cpp`

### 2026-08-20 第二批补记（本批 6 项）
1. ✅ P1-7 PL/pgSQL 最小解释器（UDF body 两阶段解释执行，见 P1-7）
2. ✅ P1-8 `VACUUM (PARALLEL n)` 页分片并行回收（见 P1-8）
3. ✅ `pg_stats`/`pg_statistic` 兼容视图（`queryPgCatalog` 读取 `.stats` 行数/MCV/直方图，`tests/pg_stats_test.cpp`）
4. ✅ `unnest()` 表函数（`UnnestOp`，FROM 子句与投影两路径）
5. ✅ TimeZone GUC 会话渲染（timestamptz 输出 `+HH:MM` 后缀，防双重应用）
6. ✅ 多表 join 贪心搜索（≥2 个 JOIN 的 FROM 链解析为表+ON 谓词，按 `1/max(ndistinct)` 估计选最小对起步，中间结果物化临时表，列名冲突去重+来源映射，`tests/multijoin_e2e_test.py`）
7. ✅ deferrable EXCLUDE（元素级队列 + COMMIT 当前版本/HOT 重定向 recheck）
8. ✅ FTS `ts_rank` weights float4[] 与 `<->` 短语匹配（递归下降 `! > <-> > & > |`，`setweight()`）
回归基线：PASS=160 FAIL=0 + E2E（multijoin/timestamptz/unnest 等 6 套 Python E2E 全绿）
9. ✅ P1-9 自定义成本函数（`QueryPlanner::CostModel` + 8 个成本 GUC + 自定义成本 hook，PASS=161）
10. ✅ P2-2 Bloom 索引（`BloomIndex` AM：bloom filter + 精确侧表、全写路径维护、扫描探测，PASS=162）
11. ✅ P2-4 连接池（`ConnectionPool`：三模式 BackendContext 池化 + 3 个 GUC + `SHOW POOLS`，PASS=163）
12. ✅ P2-5 逻辑解码（双输出插件 + PublicationCatalog + 按槽变更流 + 全 SQL 面，PASS=164）
13. ✅ P2-8 TDE（SHA-256-CTR+EtM 页级加密、边车信封、keyring 0600、崩溃恢复兼容，PASS=165）

### P1-9: 自定义成本函数 (Custom Cost Functions) ✅（2026-08-20）
- **类别**: 优化器 / 扩展性
- **现状**: ✅ `QueryPlanner::CostModel` 落地：PG 风格成本参数（`seq_page_cost`/`random_page_cost`/`cpu_tuple_cost`/`cpu_index_tuple_cost`/`cpu_operator_cost`）+ 每算法 enable 开关（nestloop/hashjoin/mergejoin，禁用计价 1e18）+ 自定义成本 hook（函数指针，返回 <0 回退内置模型）。`costJoinAlgorithm`/`costScan` 按活跃模型求值，`estimateJoinCost` 委托之 → join 算法选择完全由成本参数驱动。GUC 会话级 `SET` 原地修补模型（保留先前会话覆盖），`SET GLOBAL`/`ALTER SYSTEM` 持久化并在启动/reload 同步；`SHOW`/`pg_settings` 暴露。成本保持 row-touch 量纲，planner 绝对阈值语义不变（`tests/cost_model_test.cpp`）
- **PG 参考**: `cost_seqscan`, `cost_index`, `cost_hashjoin` 等 hook
- **残余缺口**: per-opclass 索引成本、`custom_cost_hook` 的多注册与优先级、`geqo`/遗传算法路径
- **实现路径**:
  1. ✅ `CostModel` 结构（参数+开关+hook）
  2. ✅ `QueryPlanner` 中函数指针 hook 调用（自定义优先，负值回退）
  3. ✅ `SET seq_page_cost = ...` 等 8 个 GUC（会话级粘性 + 全局持久化）
- **预估工作量**: ~~2-3 天~~ 已完成
- **相关文件**: `src/executor/ExecutionPlan.{h,cpp}`, `src/common/Config.{h,cpp}`, `src/main.cpp`, `tests/cost_model_test.cpp`

### P1-10: SQL 级 prepared statements 对齐 PostgreSQL 语法
- **类别**: 会话 / 协议兼容
- **✅ 已完成（本批实现）**: parser 接受 `PREPARE name [(types)] AS stmt` 与 `$1..$n` 占位符（`ps_*` 辅助：类型感知的头部解析、顶层逗号切分、参数替换），`EXECUTE name(args)` 位置实参绑定，`DEALLOCATE` 兼容；旧 `FROM 'sql'`/`?`/`USING` 语法保留。类型标注存入会话 `preparedStmtTypes`（`tests/prepare_pg_test.cpp`）
- **现状**: 已有：会话级命名 prepared statements（`main.cpp` handleExecutePrepared：`PREPARE name FROM 'sql'` 存模板、`EXECUTE name USING (vals)` 按 `?` 文本替换、`DEALLOCATE name`）与协议层 `Parse/Bind/Execute`（含参数类型与 binary 编解码）。但与 PostgreSQL 语法不兼容：PG 是 `PREPARE name [(types)] AS stmt` + `$1..$n` 占位符 + `EXECUTE name(args)`；当前是自有 `FROM 'sql'` + `?` + `USING`。且模板按文本展开，无类型推断、无计划复用（每次重新解析执行）、无 `pg_prepared_statements` 视图
- **PG 参考**: `PREPARE name [(int,text)] AS SELECT ... WHERE id=$1`, `EXECUTE name(1,'a')`
- **影响**: 使用标准 SQL PREPARE 语法的客户端/ORM/dump 无法工作；预编译得不到计划级加速
- **实现路径**:
  1. parser 接受 PG 语法的 `PREPARE ... AS` 与 `$n` 占位符（与扩展协议共享参数绑定机制）
  2. EXECUTE 实参求值后走类型化绑定而非文本替换
  3. 计划缓存按 (模板, 参数类型) 复用，DDL 失效
  4. `pg_prepared_statements` 兼容视图
- **预估工作量**: 3-5 天
- **相关文件**: `src/main.cpp`（handleExecutePrepared）, `src/network/NetworkServer.cpp`（协议层参数绑定）, `src/parser/parser.cpp`

### P1-11: EXPLAIN ANALYZE 每-节点实际统计
- **类别**: 可观测性 / 诊断
- **✅ 已完成（本批实现）**: `IOperator` 内建运行时插桩（`NextInstrument` RAII：每节点累计 actual time、loops、emitted rows，21 个算子全覆盖），`explain` 在 ANALYZE 选项下按节点追加 `(actual time=… rows=… loops=…)`；main.cpp 重执行计划后对同一棵已执行树 re-explain，`BUFFERS` 输出按查询的 shared hit/read 增量（BufferPool 计数器差分）（`tests/explain_analyze_test.cpp` + `explain_analyze_e2e_test.py`）
- **现状**: `EXPLAIN (ANALYZE)` 解析选项并整体执行一次计划输出总行数/总耗时（`src/main.cpp` handleExplain），但无 PostgreSQL 的每-节点 `actual time=.. loops=.. rows`、共享 buffer 命中（`shared hit/read` 按节点）、worker 明细；`buffers` 选项输出的是全池累计统计而非本查询的
- **PG 参考**: `ExplainPrintPlan` per-node `actual time`, `loops`, `Shared Hit Blocks`
- **影响**: 无法定位计划中哪个算子是瓶颈；调优能力远弱于 PostgreSQL
- **实现路径**:
  1. 在 `Operator` 基类增加 `stats_`（rows produced、next() 累计时间、buffer 命中增量）
  2. `executePlanChecked` 执行路径在 `next()` 前后计时并累计
  3. explain 输出遍历时把每节点实际值与估算值并列输出
  4. BufferPool 增加 per-query 计数作用域（fetch 前后差分）
- **预估工作量**: 1 周
- **相关文件**: `src/executor/ExecutionPlan.{h,cpp}`, `src/main.cpp`, `src/storage/BufferPool.h`

### P1-12: 统计驱动的完整代价模型 (统计采集已备，消费不全)
- **类别**: 优化器 / 统计
- **✅ 已完成（本批实现）**: `estimateSelectivity` 消费 MCV（热值精确频率 count/rows）与等深直方图（范围谓词桶内线性插值，数值/文本双路径，`!=` 取补）；新增 `estimateJoinSel`（eqjoinsel：1/max(nd_l,nd_r)）接入三种 join 的 EXPLAIN rows 与 `buildJoinPlan` 成本（无索引 NLJ 计入输出基数、hash build 侧按键密度选边）（`tests/stats_planner_test.cpp`）。多表 join 顺序 DP 搜索与 `pg_stats` 视图仍未做（后续批次已补：`pg_stats`/`pg_statistic` 视图 + 多表 join 贪心搜索，见下「第二批」注记）
- **现状**: `ANALYZE` 已采集并持久化到 `.stats`：每列 cardinality、min/max、等深直方图、MCV top-N（`TableManage.h` `ColumnStats`，`computeMCV`，含多列统计 `getMultiColumnStats`）；但 planner 消费极不完全——`estimateSelectivity` 仅 `=` 用 cardinality（回退 0.1），`!=/like` 硬编码 0.9/0.2，范围谓词一律 0.3（**直方图与 MCV 采了没人用**）；join 算法选择用行数+索引存在的固定公式（`estimateJoinCost`），无 `eqjoinsel` 风格的键重叠估计；join 顺序只有 build/outer 交换，无多表动态规划/贪心搜索；且这些选择率目前只影响 EXPLAIN 展示，是否影响实际计划生成需逐路径核对
- **PG 参考**: `pg_stats` 直方图/MCV/correlation 驱动的 `eqsel/neqsel/scalarea_sel/eqjoinsel` + join 顺序动态规划
- **影响**: 范围查询行数估计偏差 3×+，倾斜数据（MCV 本可纠正）下选错 join 算法/顺序，多表 join 顺序次优
- **实现路径**:
  1. `estimateSelectivity` 范围谓词走等深直方图插值；`=` 先查 MCV 再回退 `1/ndistinct`
  2. join 键选择率用两侧列 cardinality 的 `min(nd1,nd2)/(nd1*nd2)` 估计并接入 `estimateJoinCost`
  3. 三表以上 join 的顺序搜索（贪心或 DP，PG 为 GEQO 阈值内 DP）
  4. `pg_stats` 兼容视图暴露已采集的统计
- **预估工作量**: 1-2 周（采集侧已就绪，主要是消费侧接线）
- **相关文件**: `src/executor/ExecutionPlan.cpp`（`estimateSelectivity`/`estimateJoinCost`/`buildJoinPlan`）, `src/commands/TableManage.cpp`（`.stats` 读写）

### P1-13: 并行 JOIN/聚合与 GatherMerge
- **类别**: 性能 / 执行器
- **✅ 已完成（本批实现）**: 新增 `ParallelHashJoinOp`（build 侧按 page range 分片多线程建哈希、按 rid 排序合并保证确定性）、`ParallelGroupAggregateOp`（worker 局部分组桶 + 合并终态，count/sum/avg/min/max/bool_*、FILTER、HAVING）、`GatherMergeOp`（有序流 k 路归并；ORDER BY 并行路径=分区排序+归并）。planner 在非事务+并行开启时接入；EXPLAIN 显示 workers 与 used/fallback（`tests/parallel_exec_test.cpp`）。worker 池复用仍每次建线程
- **现状**: 仅 `ParallelTableScanOp` 按 page range 分片；`HashJoinOp`/`MergeJoinOp`/`GroupAggregateOp`（均已存在）无并行版本；Gather 输出按范围顺序拼接，无 `GatherMerge`；worker 每查询创建、无复用池
- **PG 参考**: `Parallel Hash Join`, `Partial Aggregate` + `Finalize Aggregate`, `GatherMerge`
- **影响**: 大表 join/聚合无法利用多核（这是分析查询的常见瓶颈）
- **实现路径**:
  1. 并行 HashJoin：build 侧每 worker 本地哈希（或共享哈希表 + 锁分区）
  2. Partial + Finalize 两阶段聚合（worker 局部累计、leader 合并）
  3. `GatherMergeOp`：按排序键 k 路归并 worker 输出
  4. 独立 worker 池 + work-stealing
- **预估工作量**: 2-3 周
- **相关文件**: `src/executor/ExecutionPlan.{h,cpp}`
- **备注**: 与 P0-1 高度重叠，可作为其第二阶段

---

## P2 — 次要缺失 (易用性/运维)

### P2-1: pg_stat_statements
- **类别**: 可观测性 / SQL 统计
- **现状**: 已有线程安全的 SQL 统计模块，`SHOW STATEMENTS` 和 `pg_stat_statements` 风格虚拟表可查询调用次数、总/最小/最大/平均耗时；常量与空白会归一化，主程序和 PostgreSQL 协议入口共用同一实现。当前格式的 `.sql_stats` 已在 checkpoint/引擎关闭时以 sidecar flock、临时文件、fsync 和原子 rename 持久化，并在启动时严格校验加载；`pg_stat_statements.max` 默认限制为 5000 条，超限按调用次数/耗时确定性淘汰；仍缺 reset 权限和完整扩展/catalog 语义。
- **PG 参考**: `pg_stat_statements` extension
- **影响**: 基础热点识别及版本化运行时快照已可用；仍缺少 PostgreSQL 级统计视图、权限控制和更完整的监控扩展接口
- **实现路径**:
  1. ✅ 实现线程安全 `SqlStats`：key=数据库+归一化 SQL，value=call_count/total/min/max/mean
  2. ✅ 在交互式和 PostgreSQL 协议 SQL 入口收集统计
  3. ✅ 提供 `SHOW STATEMENTS` 与 `pg_stat_statements` 风格虚拟表
  4. ✅ 增加 `pg_stat_statements.max` 上限与确定性淘汰
  5. 补充 reset 权限和 rows/blocks 等 PostgreSQL 字段
- **预估工作量**: 2-3 天
- **相关文件**: `src/common/Config.cpp`, `src/main.cpp`

### P2-2: Bloom 索引 ✅（2026-08-20）
- **类别**: 索引 / 等值探测
- **现状**: ✅ `src/access/BloomIndex.{h,cpp}`：m 位/k 哈希 bloom filter（FNV-1a double hashing，PG 风格 128 bit/entry、k=7、~1% 假阳率）+ 精确 key→rid 侧表，O(k) 探测零假阴；删除保留位图（bloom 不可逆）而侧表精确。'BLM1' 磁盘格式 + 原子改名保存，坏文件拒绝加载。引擎侧 `createBloomIndex`（forEachRow 全量构建）/`dropBloomIndex`/`getBloomIndex`（缓存）/`.bloomidx` 元数据，接入 `dropIndexByAccessMethod`、建表/删表清理、ALTER TABLE 改名、reindex；INSERT/UPDATE（旧值入 `oldIdxVals` 后交换）/DELETE 三写路径同步维护；`collectEqualityIndexCandidates` 探测 bloom（IndexScan/BitmapScan/BitmapOr 共享），`hasEqualityIndex` 识别 bloom 列；`CREATE INDEX ... USING bloom` + DROP INDEX 名称注册表 + 回滚（`tests/bloom_index_test.cpp`，PASS=162）
- **PG 参考**: `bloom` access method
- **残余缺口**: 多列 bloom 签名（PG bloom 支持多列组合签名）、per-index `length`/`col1..col2` 参数
- **实现路径**:
  1. ✅ `BloomIndex` 类：m 位数组 + k 个哈希函数 + 精确侧表
  2. ✅ 接入扫描路径（collectEqualityIndexCandidates + hasEqualityIndex）
  3. ✅ `CREATE INDEX ... USING bloom` 语法
- **预估工作量**: ~~3-5 天~~ 已完成
- **相关文件**: `src/access/BloomIndex.{h,cpp}`, `src/commands/TableManage.{h,cpp}`, `src/commands/DdlExecutor.cpp`, `src/executor/ExecutionPlan.cpp`, `tests/bloom_index_test.cpp`

### P2-3: 增量备份
- **类别**: 运维 / 高可用
- **现状**: 仅有全量 DUMP/BACKUP
- **PG 参考**: `pg_basebackup` + WAL archiving
- **影响**: 大库备份慢、无法做 PITR
- **实现路径**:
  1. 实现 WAL archiving：`archive_command` 配置
  2. 实现 `pg_basebackup` 协议
  3. 实现 PITR 恢复：`recovery_target_time`
- **预估工作量**: 1-2 周
- **相关文件**: `src/replication/ReplicationManager.cpp`

### P2-4: 连接池 (PgBouncer 式) ✅（2026-08-20）
- **类别**: 网络 / 连接管理
- **现状**: ✅ `src/network/ConnectionPool.{h,cpp}`：BackendContext 按 (user, database) 池化，session/transaction/statement 三模式 —— session 模式客户端独占至断开（断开 discard），transaction/statement 模式按语句租借（客户端状态投影到后端、执行后回写 GUC/DB 变更并归还，空闲客户端零后端占用）；容量满时条件变量阻塞、release 唤醒；命名 prepared statement 随后端存续（PgBouncer transaction 模式语义）。接入 `handleClient`/`executeForProtocol` 与 accept 循环（`max_client_conn` 客户端上限，0=无限），客户端已认证会话始终权威（后端槽不泄漏权限态）；服务器停止时 shutdown 唤醒全部等待者。GUC `pool_mode`/`pool_size`/`max_client_conn` 全链路（解析/校验/持久化/启动/reload/SET GLOBAL 回滚），`SHOW POOLS` + 单参数 SHOW + pg_settings 行（`tests/connection_pool_test.cpp`，PASS=163）
- **PG 参考**: `pgbouncer`, `pgpool-II`
- **残余缺口**: 事务中途归还的后端状态回滚依赖协议层先行 rollback；LISTEN/NOTIFY、游标等会话级特性在短租模式下不跨租借（与 PgBouncer 相同限制）
- **实现路径**:
  1. ✅ 实现 `ConnectionPool`：session/transaction/statement 三级池化
  2. ✅ 在 `NetworkServer` 中增加池化层
  3. ✅ 增加 `pool_size`, `max_client_conn` GUC（+ `pool_mode`、`SHOW POOLS`）
- **预估工作量**: ~~1 周~~ 已完成
- **相关文件**: `src/network/ConnectionPool.{h,cpp}`, `src/network/NetworkServer.cpp`, `src/common/Config.{h,cpp}`, `src/main.cpp`, `tests/connection_pool_test.cpp`

### P2-5: Logical Decoding / 逻辑复制 Plugin ✅（2026-08-20）
- **类别**: 复制 / 生态
- **现状**: ✅ `src/replication/LogicalDecoder.{h,cpp}`：双输出插件 —— `pgoutput`（帧式二进制：B/R/I/U/D/C 消息、长度前缀行像）与 `test_decoding`（可读文本）。`PublicationCatalog` 持久化发布（FOR TABLE / FOR ALL TABLES、insert/update/delete 发布过滤）；`LogicalChangeStore` 按槽流式存储（peek 恢复 LSN、acknowledge 推进、kMaxRetained 限界）。引擎侧 `TransactionContext` 增逻辑变更缓冲，三写路径（INSERT/UPDATE/DELETE）对已发布表渲染全列文本行像入队，COMMIT 按提交 LSN/xid 批量流向全部逻辑槽，ROLLBACK 丢弃（订阅者只见已提交变更）；`advanceSlotLsn` 单调推进确认 LSN。SQL 面（解析器前置拦截）：CREATE/DROP PUBLICATION、CREATE/DROP REPLICATION SLOT（LOGICAL 插件 | PHYSICAL）、`SHOW PUBLICATIONS` / `SHOW REPLICATION SLOTS`（含 pending 深度）/ `SHOW LOGICAL CHANGES FOR SLOT` / `SHOW LOGICAL CONFIRM FOR SLOT`（`tests/logical_decoding_test.cpp`，PASS=164）
- **PG 参考**: `pgoutput`, `test_decoding`, `wal2json`
- **残余缺口**: pgoutput 未做二进制 wire 级兼容（自描述帧式）、无 CREATE SUBSCRIPTION 拉取端、变更不经 WAL 重放而是写路径直采（崩溃恢复后无 replay）
- **实现路径**:
  1. ✅ `LogicalDecoder`：提交级变更批（表/op/新旧行像）
  2. ✅ `pgoutput` 协议（+ `test_decoding` 文本）
  3. ✅ `CREATE PUBLICATION`（CREATE SUBSCRIPTION 需要真实网络复制端，残余）
- **预估工作量**: ~~2-3 周~~ 已完成（本批范围）
- **相关文件**: `src/replication/LogicalDecoder.{h,cpp}`, `src/replication/ReplicationManager.{h,cpp}`, `src/commands/TableManage.{h,cpp}`, `src/main.cpp`, `tests/logical_decoding_test.cpp`

### P2-6: 异步 I/O (io_uring)
- **类别**: 性能 / I/O
- **现状**: 同步 read/write
- **PG 参考**: `io_uring` (PG18 实验性)
- **影响**: I/O 密集型场景延迟高
- **实现路径**:
  1. 引入 `liburing` 库
  2. 在 `BufferPool` 中实现异步预取
  3. 在 `WALWriter` 中实现异步写入
- **预估工作量**: 1 周
- **相关文件**: `src/storage/BufferPool.cpp`, `src/storage/WAL.cpp`

### P2-7: 安全标签 (SE-PostgreSQL)
- **类别**: 安全 / MAC
- **现状**: 无强制访问控制
- **PG 参考**: `SECURITY LABEL`, SELinux 集成
- **影响**: 无法满足高安全等级部署
- **实现路径**:
  1. 实现 `SecurityLabel` 结构：`(objtype, objname, label)`
  2. 在 ACL 检查前增加 MAC 检查
  3. 增加 `SECURITY LABEL ON ...` 命令
- **预估工作量**: 3-5 天
- **相关文件**: `src/utils/permissions.h`

### P2-8: TDE (透明数据加密) ✅（2026-08-21）
- **类别**: 安全 / 加密
- **现状**: ✅ `src/storage/PageCrypto.{h,cpp}`：页级 TDE，SHA-256-CTR 密流 + SHA-256 EtM 认证（等价 AES-GCM 组合语义，零外部依赖）。存储模型：**整页密文**，每页信封（nonce+MAC，48B）住边车文件 `<datafile>.tde`（页号索引）；堆页头部 line-pointer 向下生长/tuple 向上生长，页内无处安放信封——边车保持物理布局不变。全零信封 = 明文页（未加密库可就地加密）。页 0（文件头）永不加密。`BufferPool` 读写钩子：读验证+解密（MAC 失败 fail-closed）、写封缄+落信封。密钥环 0600 首建/重载/坏格式拒绝；GUC `tde_keyring` 在引擎前静态初始化装载（崩溃恢复可解密）；`SHOW TDE STATUS`。WAL 崩溃恢复验证通过（before-image 明文、redo 经 buffer pool 再封缄）
- **PG 参考**: `pg_tde` extension
- **残余缺口**: 索引文件（.idx/.gin/.brin 等）未经 BufferPool 页路径的部分未覆盖；无密钥轮换（re-encrypt）、无 per-database 密钥隔离
- **实现路径**:
  1. ✅ `PageCrypto`：SHA-256-CTR + EtM 页级加密
  2. ✅ `BufferPool` 读写路径加解密（页 0 豁免）
  3. ✅ 密钥管理：keyring 文件（0600、首建随机、坏格式拒绝）
- **预估工作量**: ~~1 周~~ 已完成（本批范围）
- **相关文件**: `src/storage/PageCrypto.{h,cpp}`, `src/storage/BufferPool.{h,cpp}`, `src/common/Config.{h,cpp}`, `src/main.cpp`, `tests/tde_test.cpp`

### P2-9: Deferrable 约束扩展到 CHECK 之外
- **类别**: 数据完整性
- **✅ 已完成（本批实现）**: parser 在 named/unnamed 两种表约束形式的 PK/UNIQUE/FK/EXCLUDE 上解析 `DEFERRABLE [INITIALLY {DEFERRED|IMMEDIATE}]`；`DeferredCheck` 扩展 Kind{Unique,ForeignKey}+payload，单列 UNIQUE（列级+表级）与单列 FK 当前 deferred 时入队、COMMIT 时验证（UNIQUE 重扫排除自身 rid，FK 查父表），失败回滚；`SET CONSTRAINTS ALL IMMEDIATE` 恢复立即检查（`tests/deferrable_constraints_test.cpp`）。PK（严格 B+ 树插入）与 EXCLUDE 的延迟、级联交互未做（EXCLUDE deferrable 已于后续批次实现：元素级 recheck 队列、COMMIT 时按当前版本+HOT 重定向解析验证、autocommit 立即检查）
- **现状**: 已有：列级 CHECK 约束的 `DEFERRABLE INITIALLY DEFERRED`（延迟队列 commit 时验证，`runDeferredCheck`），`SET CONSTRAINTS {name|ALL} {DEFERRED|IMMEDIATE}` 通过 `constraintMode_` 生效（per-transaction，all-gaps-todo 1.1.53 已记）。仍缺：FK 的 deferrable（`ON DELETE CASCADE` 等动作与延迟检查的交互）、UNIQUE/PRIMARY KEY/EXCLUDE 的 deferrable（parser 无这些约束上的 `DEFERRABLE` 修饰）、constraint trigger 的 deferred 触发
- **PG 参考**: 各类约束的 `DEFERRABLE INITIALLY {DEFERRED|IMMEDIATE}`
- **影响**: 环状 FK、自引用行、批量重排唯一键等模式无法工作
- **实现路径**:
  1. parser 在 FK/UNIQUE/PK/EXCLUDE 约束子句接受 `DEFERRABLE [INITIALLY ...]`
  2. 复用现有 deferred 队列：UNIQUE 延迟到 commit 时做存在性检查（需并发事务冲突处理）
  3. FK 延迟检查与级联动作排序
  4. constraint trigger（`CREATE CONSTRAINT TRIGGER`）挂入同一队列
- **预估工作量**: 1 周
- **相关文件**: `src/parser/parser.cpp`（约束子句）, `src/commands/TableManage.cpp`（deferred 队列）

### P2-10: 数组类型的表达式/函数完整语义
- **类别**: 类型系统
- **✅ 已完成（本批实现）**: `ARRAY[...]` 构造器（parser 产生式→`__array_construct` 内置函数，NULL/引用规则同 PG 输出）、切片 `a[lo:hi]`（含负下标/开放边界/钳制）、下标 `a[i]` 负数从尾部、`@>`/`<@` 与 SQL 数组/JSON 自动判别的包含运算（`tests/array_expr_test.cpp`）。`unnest()` 表函数已补（`FROM unnest(...)` 与无 FROM SELECT 两路径，`UnnestOp` 结构化展开，`tests/unnest_test.cpp`+E2E）；协议 binary 数组 I/O 仍缺
- **现状**: 已有：数组列识别与字面量校验/规范化（`tests/array_test.cpp`）、数组下标 `a[i]`（parser postfix `[]` + `ExprEvaluator` 求值，含多维）、`= ANY(arr)`/`ALL` 经 text-rewrite 到 `array_contains`（`src/main.cpp` 预处理，Volcano 路径是否覆盖需核对）、`array_agg` 聚合（`TableManage.cpp`）。仍缺：`ARRAY[...]` 构造器表达式、切片 `a[1:3]`（parser 注释提及但求值未见）、`@>`/`<@` 包含操作符、`unnest()`、数组比较/哈希语义与协议 binary 数组 I/O（all-gaps-todo 2.15 已记）
- **PG 参考**: array functions/operators, `unnest`, `@>`
- **影响**: 常用数组运算（构造、切片、包含判断、展开为行）缺失
- **实现路径**:
  1. parser `ARRAY[...]` 构造与 `ExprEvaluator` 求值
  2. 切片下标求值；`@>`/`<@` 操作符
  3. `unnest()` 表函数（FROM 子句展开）
  4. 协议 binary 数组编解码
- **预估工作量**: 1 周
- **相关文件**: `src/parser/parser.cpp`, `src/expression/ExprEvaluator.cpp`, `src/main.cpp`（ANY rewrite）

### P2-11: 表继承的 CREATE 侧与 ONLY 语义
- **类别**: 数据模型
- **✅ 已完成（本批实现）**: `CREATE TABLE child (...) INHERITS (parents)`（`executeCreateTable`：父列前插合并、继承列去 NOT NULL/PK、`.inherits` 登记子表）；`SELECT/UPDATE/DELETE` 的 `ONLY [()]` 修饰：主循环按词边界剥除并置 `onlyNext`，三处 children 展开点（volcano/聚合/SELECT 合并）受其抑制，RAII 语句级复位（`tests/inherit_only_e2e_test.py`）。约束/索引/默认值继承传播未做
- **现状**: 已有：`ALTER TABLE ... INHERIT/NO INHERIT`（`DdlExecutor.cpp` 读写 `.<table>.inherits`）、`getInheritedChildren` 被 SELECT/DDL 消费（查询默认含子表行，main.cpp 多处）；`DROP TABLE` 级联清理继承关系。仍缺：`CREATE TABLE child (...) INHERITS (parent)` 产生式（列合并继承）、`ONLY (table)` 限定扫描（parser 无该修饰符；TRUNCATE 已支持 ONLY，SELECT/UPDATE/DELETE 未支持）
- **PG 参考**: `CREATE TABLE ... INHERITS`, `SELECT * FROM ONLY (t)`
- **影响**: 继承只能 ALTER 补建且子表不自动继承父表列；无法只操作父表行
- **实现路径**:
  1. parser `INHERITS (parent, ...)` 产生式 + 列合并（父列 + 子列，冲突检测）
  2. SELECT/UPDATE/DELETE 的 `ONLY` 前缀修饰（查询改写处已有 children 展开点，加开关即可）
  3. 约束/索引/默认值的继承传播语义
- **预估工作量**: 3-5 天
- **相关文件**: `src/parser/parser.cpp`, `src/commands/DdlExecutor.cpp`（Inherit action）, `src/main.cpp`（children 展开）

### P2-12: Interval 运算与时区转换
- **类别**: 类型系统 / 时区
- **✅ 已完成（本批实现）**: `ExprEvaluator` interval 双基（月+微秒）算术：`timestamp±interval`（日历月进位+日钳制+微秒进位）、`interval±interval`、`interval*/number`、无类型 interval 字面量邻接推断；`AT TIME ZONE`（`ts AT TIME ZONE 'zone'` 双向：UTC/GMT/±HH[:MM]/±HHMM 偏移，`timezone(zone,ts)` 函数）；`::type` 扫描不再吞掉 AT TIME ZONE（`tests/interval_arith_test.cpp`）。TimeZone GUC 会话显示已补（`SET TIME ZONE`/`timezone` GUC 驱动 timestamptz 输出按会话偏移渲染 `+HH:MM` 后缀，SELECT 输出边界防双重应用，E2E `timestamptz_e2e_test.py`）；interval typmod 限定仍缺
- **现状**: interval 已有列类型、字面量解析与 PostgreSQL 风格规范化（`tests/interval_test.cpp`：verbose 单位/`HH:MM:SS`/`Y-M`/裸秒/`ago`）。仍缺：`timestamp + interval` 等 interval 算术（`ExprEvaluator` 无 interval 运算分支）、`AT TIME ZONE` 双向转换（代码库无任何实现）、`TimeZone` GUC 与 timestamptz 按会话时区显示、interval 字段限定（`INTERVAL DAY TO SECOND`）
- **PG 参考**: interval arithmetic, `AT TIME ZONE`, `TimeZone` GUC
- **影响**: 时间偏移计算与跨时区应用无法正确表达
- **实现路径**:
  1. interval 微秒/月双基存储 + timestamp±interval/interval±interval/date_part
  2. `AT TIME ZONE` 转换 + `TimeZone` GUC + 常用时区表（可先 POSIX TZ 串）
  3. interval typmod 字段限定
- **预估工作量**: 1 周
- **相关文件**: `src/parser/parser.cpp`, `src/expression/ExprEvaluator.cpp`

### P2-13: Dollar-quoting 与 SQL 函数体
- **类别**: 解析器 / 兼容性
- **✅ 已完成（本批实现）**: tokenizer 识别 `$tag$ ... $tag$` 为单一字符串 token（任意 tag、嵌套防护），CREATE FUNCTION/PROCEDURE/TRIGGER 函数体与普通字符串字面量处均可使用（`tests/prepare_pg_test.cpp` 内含 dollar-quote 用例）
- **现状**: parser 无 `$$body$$` dollar-quoted 字符串；CREATE FUNCTION 的函数体目前依赖普通字符串字面量承载；`\gset`/psql 元命令不属于本差距范围
- **PG 参考**: `$$ ... $$`, `$tag$ ... $tag$`
- **影响**: 任何使用 dollar-quoting 的 DDL dump（尤其函数/触发器定义）无法导入
- **实现路径**:
  1. tokenizer 识别 `$tag$ ... $tag$` 为单一字符串 token
  2. CREATE FUNCTION/TRIGGER/PROCEDURE 体解析改为接受该 token
- **预估工作量**: 1-2 天
- **相关文件**: `src/parser/parser.cpp`（tokenizer）

---

## P3 — 锦上添花 (企业级/生态)

### P3-1: C 扩展加载 (fmgr)
- **类别**: 扩展性
- **现状**: 无法加载 `.so` 动态库
- **PG 参考**: `CREATE FUNCTION ... LANGUAGE C`, `fmgr`
- **影响**: 无法使用 C 扩展生态
- **实现路径**:
  1. 实现 `dlopen`/`dlsym` 包装
  2. 实现 `PG_FUNCTION_INFO_V1` 宏兼容
  3. 实现 `fmgr` 调用接口
- **预估工作量**: 1-2 周

### P3-2: Hook 系统
- **类别**: 扩展性
- **现状**: 无 plugin hook
- **PG 参考**: `ProcessUtility_hook`, `ExecutorStart_hook`, `plpgsql_plugin`
- **影响**: 无法做 APM、审计、查询重写等插件
- **实现路径**:
  1. 定义 `Hook` 模板类
  2. 在关键路径插入 hook 点
  3. 实现 hook 注册/注销 API
- **预估工作量**: 1 周

### P3-3: Background Worker API
- **类别**: 扩展性
- **现状**: 11 种后端类型框架有，无注册 API
- **PG 参考**: `RegisterBackgroundWorker`, `BackgroundWorkerInitializeConnection`
- **影响**: 无法开发自定义后台任务插件
- **实现路径**:
  1. 实现 `BackgroundWorker` 注册接口
  2. 实现 shared memory 注册
  3. 实现 worker 生命周期管理
- **预估工作量**: 3-5 天

### P3-4: pg_upgrade 工具
- **类别**: 运维 / 升级
- **现状**: 无跨版本升级工具
- **PG 参考**: `pg_upgrade`
- **影响**: 大版本升级需要导出/导入
- **实现路径**:
  1. 实现 `pg_upgrade` 二进制
  2. 实现数据文件格式迁移
  3. 实现 catalog 版本检查
- **预估工作量**: 1-2 周

### P3-5: 内置扩展 (pg_stat_statements, auto_explain 等)
- **类别**: 生态
- **现状**: 无内置扩展
- **PG 参考**: `pg_stat_statements`, `auto_explain`, `pg_prewarm`
- **影响**: 缺少常用运维工具
- **实现路径**:
  1. 实现 `CREATE EXTENSION` 运行时加载
  2. 移植常用扩展为内置
- **预估工作量**: 1-2 周

### P3-6: 多租户 Schema 隔离
- **类别**: 企业级
- **现状**: Schema 支持基础，无行级租户隔离
- **PG 参考**: `ROW LEVEL SECURITY` + `CURRENT_USER` 过滤
- **影响**: SaaS 多租户场景需要应用层处理
- **实现路径**:
  1. 实现 `SET app.current_tenant = '...'` GUC
  2. 自动为每个查询增加 tenant_id 过滤
  3. 实现 `CREATE POLICY ... USING (tenant_id = current_tenant())`
- **预估工作量**: 3-5 天

### P3-7: 全文检索执行链路 (tsvector/tsquery 已有类型，缺检索)
- **类别**: 类型系统 / 检索
- **✅ 已完成（本批实现）**: `ExprEvaluator`：`to_tsvector`（分词+位置+经引擎 canonicalizer 规范化，`normalizeTsVectorText` 公开）、`to_tsquery`/`plainto_tsquery`（词表 & 连接，`&|!` 形态透传）、`@@`（词位集合匹配：& 全体命中、| 任一命中，容忍存储 tsvector 字面量或纯文本左值）、`ts_rank`（覆盖率+位置密度加权）（`tests/fulltext_search_test.cpp`）。GIN 倒排索引、`<->` 距离语义、`ts_headline`、文本搜索配置（停用词/词干）仍缺
- **现状**: tsvector/tsquery 已有字面量解析与 canonicalization/文法校验（`tests/tsearch_test.cpp`），列可存值；`@@` 已是 lexer 的双字符 token（`parser.cpp:265`），但无匹配求值；无 `to_tsvector`/`to_tsquery` 函数、无 GIN 倒排 opclass、无 `ts_rank`/`ts_headline`/文本搜索配置（all-gaps-todo 2.11 已记类型侧）
- **PG 参考**: `@@`, GIN `gin_tsvector_ops`, `to_tsvector('english', ...)`
- **影响**: 有类型无检索——无法在 SQL 内做全文查询
- **实现路径**:
  1. `ExprEvaluator` 实现 `@@`（tsquery 树与 tsvector 词位/位置匹配，含 `<->` 距离）
  2. `to_tsvector`/`to_tsquery`（先 simple 配置 + 英文停用词）
  3. GIN 倒排索引（词位 → RID）
  4. `ts_rank`/`ts_headline` 排序与高亮
- **预估工作量**: 2-3 周
- **相关文件**: `src/expression/ExprEvaluator.cpp`, `src/parser/parser.cpp`, `src/commands/TableManage.cpp`（GIN）

---

## 实施路线图建议

### 第一优先级 (并发正确性 + 查询完整性) — 约 5 周
1. P0-7 视图触发器复杂视图/函数运行时语义 (后续)
2. P0-6 Bitmap Scan (1w)
3. P1-1 Window Function Executor (1-2w)
4. P1-2 UNION/INTERSECT Executor (3-5d)
5. P1-3 ROLLUP/CUBE Executor (3-5d)
6. P0-4 Gap Locks (1-2w)
7. P2-13 Dollar-quoting (1-2d，dump 导入前置)

### 第二优先级 (优化器 + 性能) — 约 6 周
1. P1-12 列级统计与直方图 (2w，一切代价模型的地基)
2. P0-1/P1-13 并行查询二阶段：并行 JOIN/聚合 + GatherMerge (2-3w)
3. P0-2 JIT 编译 (3-4w)
4. P0-3 GiST 索引 (2-3w)
5. P2-6 异步 I/O (1w)

### 第三优先级 (兼容性 + 可观测性) — 约 5 周
1. P1-10 SQL 级 prepared statements (3-5d)
2. P1-11 EXPLAIN ANALYZE 每节点统计 (1w)
3. P1-6 后台统计收集器 (1w)
4. P2-1 pg_stat_statements (2-3d)
5. P2-10 数组类型 (1-2w)
6. P2-12 interval/AT TIME ZONE (1w)

### 第四优先级 (运维 + 生态 + 安全) — 约 6 周
1. P1-7 PL/pgSQL 运行时 (2-3w)
2. P3-7 全文检索 (2-3w)
3. P2-3 增量备份 (1-2w)
4. P2-4 连接池 (1w)
5. P2-9 Deferrable 约束 (3-5d)
6. P2-11 表继承 (1w)
7. P3-1 C 扩展加载 (1-2w)
8. P2-8 TDE 加密 (1w)

---

## 统计

| 维度 | 数量 | 预估总工作量 |
|------|------|-------------|
| P0 严重缺失 | 7 | ~11-15 周 |
| P1 重要缺失 | 13 | ~14-20 周 |
| P2 次要缺失 | 13 | ~10-14 周 |
| P3 锦上添花 | 7 | ~7-10 周 |
| **合计** | **40** | **~42-59 周** |

> 注: 工作量估算基于单人开发、熟悉 PostgreSQL 内部实现的前提。
> 多人协作可并行推进多个 P0/P1 项，总时间可压缩至 25-35 周。

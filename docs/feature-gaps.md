# 功能缺失清单 (Feature Gaps)

> 生成日期: 2026-08-13
> 基于 `docs/postgresql-comparison.md` 代码验证结果整理
> 本 DBMS 当前状态（2026-08-13）: 生产化重构进行中，统一回归基线 PASS=138 FAIL=0（136 个 C++ 测试 + 协议 E2E + 窗口函数 E2E）；v2/8 KiB 存储格式已统一，旧数据不迁移；事务提交现在先保证 COMMIT WAL 刷盘再发布 CLOG 可见性，PREPARE 前刷 heap/index 缓存并原子 durable 发布 prepared 元数据，跨进程可恢复表/row/page/gap 锁；snapshot export/import 已升级为数据库绑定的 v2 格式并限制在首次读写前导入，但完整 DDL 依赖事务和全局 2PC 语义仍在建设。

WAL 当前明确区分合法 LSN 0 与无效哨兵；恢复会忽略未初始化页的无效页 LSN，多个 WAL writer 通过进程/文件锁和磁盘尾部刷新串行化追加。heap 使用 page before/after image，B+Tree/Hash 使用完整索引文件 before/after image；归档复制采用临时文件、文件/目录 `fsync` 和原子状态发布，失败保留 `.ready` 供重试；GIN/GiST/SP-GiST/BRIN、原生 page-level 索引 WAL、PITR 和完整 PostgreSQL 恢复语义仍属于差距。

本文件列出与 PostgreSQL 18 生产级完整度的所有差距，按优先级分级，
每项标注类别、影响范围、预估工作量，供下一阶段实施参考。

执行器架构当前以 `src/executor/ExecutionPlan.{h,cpp}` 为唯一 Volcano 算子实现，
`src/interfaces/executor.h` 只保留实际使用的 `IOperator` 契约；未接入的旧计划/表达式
接口已删除。索引回表按 RID 直接读取并复用 page lock/MVCC 边界，但复杂 SQL producer
仍可能通过明确的 legacy/materialized-row 适配边界进入算子树。

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
| P1 重要缺失 | 9  | 影响实用性，大多数应用需要 |
| P2 次要缺失 | 8  | 易用性/运维/性能优化 |
| P3 锦上添花 | 6  | 企业级/兼容性/生态 |

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
- **相关文件**: `src/executor/ExecutionPlan.{h,cpp}`, `src/commands/TableManage.{h,cpp}`, `src/common/Config.{h,cpp}`, `src/common/GUC.cpp`

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
- **现状**: 已有线程安全 `process/RuntimeStats`，在 SQL 执行、StorageEngine、Volcano 顺序/索引扫描算子和 DML 边界记录数据库/表级运行时计数；`SHOW STATUS`、`pg_stat_database`、`pg_stat_tables` 已消费真实进程内数据，协议回归覆盖 `idx_scan`/`idx_tup_fetch`。完整可见表扫描得到的 live-row 估计现在会安全反馈给 Join 成本与 EXPLAIN，部分/索引扫描不会伪造精确估计，表重建/截断会使旧估计失效。仍无独立后台采样线程、持久化、按索引方法的完整分类和 planner 深度反馈。
- **PG 参考**: `pg_stat_database`, `pg_stat_user_tables`, `pg_stat_activity`
- **影响**: 基础运行监控已可用，但重启丢失，统计维度和 planner 反馈仍不足以替代 PostgreSQL 的完整 collector
- **实现路径**:
  1. ✅ 共享 `RuntimeStats` 在执行器/存储边界记录数据库和表级计数，包含顺序扫描、索引扫描和索引取行
  2. ✅ 实现 `pg_stat_*` 统计子集 + `SHOW STATUS` 运行时字段
  3. ✅ 在 `QueryPlanner` 中使用带来源有效性的运行时 live-row 估计做 Join 成本与 EXPLAIN 行数估计，未知时回退物理计数
  4. 补充独立后台采样线程、持久化、按索引方法细分和 `stats_collector` GUC
- **预估工作量**: 1 周
- **相关文件**: `src/process/RuntimeStats.{h,cpp}`, `src/main.cpp`, `src/network/NetworkServer.cpp`, `src/commands/TableManage.cpp`

### P1-7: PL/pgSQL 运行时
- **类别**: 存储过程 / 扩展语言
- **现状**: `CREATE PROCEDURE/FUNCTION` 仅 DDL 存储，无解释执行
- **PG 参考**: `plpgsql` 解释器, `CALL proc()`
- **影响**: 存储过程无法执行，业务逻辑无法下沉到数据库
- **实现路径**:
  1. 实现 `PlPgSqlInterpreter`：解析 PL/pgSQL AST
  2. 支持基本语法: `IF/ELSE`, `LOOP`, `FOR`, `WHILE`, `EXECUTE`, `RETURN`
  3. 支持变量声明与类型系统
  4. 在 `CALL` 命令中调用解释器
- **预估工作量**: 2-3 周
- **相关文件**: `src/pl/plpgsql.cpp` (新建), `src/pl/plpgsql.h` (新建)

### P1-8: 并行 Vacuum
- **类别**: 存储 / 维护
- **现状**: Autovacuum 已工作但单线程
- **PG 参考**: `VACUUM (PARALLEL n)`
- **影响**: 大表 VACUUM 慢，影响高负载下的维护窗口
- **实现路径**:
  1. 将 `VACUUM` 实现拆分为多个并行 worker
  2. 每个 worker 负责一个 page range
  3. 增加 `parallel_vacuum_workers` GUC
- **预估工作量**: 3-5 天
- **相关文件**: `src/commands/TableManage.cpp`

### P1-9: 自定义成本函数 (Custom Cost Functions)
- **类别**: 优化器 / 扩展性
- **现状**: 成本模型硬编码，无法扩展
- **PG 参考**: `cost_seqscan`, `cost_index`, `cost_hashjoin` 等 hook
- **影响**: 无法为自定义索引/算子调整成本
- **实现路径**:
  1. 定义 `CostFunction` 接口类
  2. 在 `QueryPlanner` 中通过函数指针/hook 调用
  3. 增加 `SET cost_seqscan = ...` GUC 参数
- **预估工作量**: 2-3 天
- **相关文件**: `src/executor/ExecutionPlan.cpp`

---

## P2 — 次要缺失 (易用性/运维)

### P2-1: pg_stat_statements
- **类别**: 可观测性 / SQL 统计
- **现状**: 已有线程安全的进程内 SQL 统计模块，`SHOW STATEMENTS` 和 `pg_stat_statements` 风格虚拟表可查询调用次数、总/最小/最大/平均耗时；常量与空白会归一化，主程序和 PostgreSQL 协议入口共用同一实现。仍缺持久化、大小上限/淘汰、reset 权限和完整扩展/catalog 语义。
- **PG 参考**: `pg_stat_statements` extension
- **影响**: 基础热点识别已可用，但重启后统计丢失，无法作为 PostgreSQL 完整监控扩展直接使用
- **实现路径**:
  1. ✅ 实现线程安全 `SqlStats`：key=数据库+归一化 SQL，value=call_count/total/min/max/mean
  2. ✅ 在交互式和 PostgreSQL 协议 SQL 入口收集统计
  3. ✅ 提供 `SHOW STATEMENTS` 与 `pg_stat_statements` 风格虚拟表
  4. 补充持久化、`pg_stat_statements.max`、reset 权限和 rows/blocks 等 PostgreSQL 字段
- **预估工作量**: 2-3 天
- **相关文件**: `src/common/Config.cpp`, `src/main.cpp`

### P2-2: Bloom 索引
- **类别**: 索引 / 多列等值
- **现状**: 无 Bloom filter 索引
- **PG 参考**: `bloom` access method
- **影响**: 多列等值查询无法使用单一索引
- **实现路径**:
  1. 实现 `BloomIndex` 类：m 位数组 + k 个哈希函数
  2. 接入 `IndexScanOp` 路径
  3. 增加 `CREATE INDEX ... USING bloom` 语法
- **预估工作量**: 3-5 天
- **相关文件**: `src/access/BloomIndex.cpp` (新建)

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

### P2-4: 连接池 (PgBouncer 式)
- **类别**: 网络 / 连接管理
- **现状**: 每连接一个线程，无池化
- **PG 参考**: `pgbouncer`, `pgpool-II`
- **影响**: 高并发下线程数爆炸
- **实现路径**:
  1. 实现 `ConnectionPool`：session/transaction/statement 三级池化
  2. 在 `NetworkServer` 中增加池化层
  3. 增加 `pool_size`, `max_client_conn` GUC
- **预估工作量**: 1 周
- **相关文件**: `src/network/NetworkServer.cpp`

### P2-5: Logical Decoding / 逻辑复制 Plugin
- **类别**: 复制 / 生态
- **现状**: 逻辑复制框架有，无 plugin 实现
- **PG 参考**: `pgoutput`, `test_decoding`, `wal2json`
- **影响**: 无法做 CDC、数据同步到外部系统
- **实现路径**:
  1. 实现 `LogicalDecoder` 接口：将 WAL 记录转为逻辑变更
  2. 实现 `pgoutput` 协议
  3. 实现 `CREATE PUBLICATION` / `CREATE SUBSCRIPTION`
- **预估工作量**: 2-3 周
- **相关文件**: `src/replication/ReplicationManager.cpp`

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

### P2-8: TDE (透明数据加密)
- **类别**: 安全 / 加密
- **现状**: 数据文件明文存储
- **PG 参考**: `pg_tde` extension
- **影响**: 无法满足合规要求
- **实现路径**:
  1. 实现 `PageCrypto`：AES-256-GCM 页级加密
  2. 在 `PageAllocator` 读写路径中加解密
  3. 实现密钥管理：`pg_tde_keyring`
- **预估工作量**: 1 周
- **相关文件**: `src/storage/PageAllocator.cpp`

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

---

## 实施路线图建议

### 第一优先级 (并发正确性 + 查询完整性) — 约 4 周
1. P0-7 视图触发器复杂视图/函数运行时语义 (后续)
2. P0-6 Bitmap Scan (1w)
3. P1-1 Window Function Executor (1-2w)
4. P1-2 UNION/INTERSECT Executor (3-5d)
5. P1-3 ROLLUP/CUBE Executor (3-5d)
6. P0-4 Gap Locks (1-2w)

### 第二优先级 (性能) — 约 4 周
1. P0-1 并行查询 (2-3w)
2. P0-2 JIT 编译 (3-4w)
3. P0-3 GiST 索引 (2-3w)
4. P2-6 异步 I/O (1w)

### 第三优先级 (可观测性 + 运维) — 约 3 周
1. P1-6 后台统计收集器 (1w)
2. P2-1 pg_stat_statements (2-3d)
3. P2-3 增量备份 (1-2w)
4. P2-4 连接池 (1w)

### 第四优先级 (生态 + 安全) — 约 4 周
1. P1-7 PL/pgSQL 运行时 (2-3w)
2. P3-1 C 扩展加载 (1-2w)
3. P3-2 Hook 系统 (1w)
4. P2-8 TDE 加密 (1w)

---

## 统计

| 维度 | 数量 | 预估总工作量 |
|------|------|-------------|
| P0 严重缺失 | 7 | ~11-15 周 |
| P1 重要缺失 | 9 | ~10-14 周 |
| P2 次要缺失 | 8 | ~7-10 周 |
| P3 锦上添花 | 6 | ~6-9 周 |
| **合计** | **30** | **~34-48 周** |

> 注: 工作量估算基于单人开发、熟悉 PostgreSQL 内部实现的前提。
> 多人协作可并行推进多个 P0/P1 项，总时间可压缩至 20-25 周。

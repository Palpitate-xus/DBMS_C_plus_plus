# 生产化状态

最后更新：2026-08-14

当前版本处于生产化重构阶段，不能宣称已经达到 PostgreSQL 的生产级完整度。当前可验证基线为：主程序构建成功，137 个 C++ 回归测试和 2 个 E2E（协议、窗口函数）共 `PASS=139 FAIL=0`，其中窗口函数 E2E 为 `13/13`。

2026-08-14 序列持久化与边界安全收紧：用户序列状态写入统一使用临时文件、文件/目录 `fsync` 和原子 rename；读取严格拒绝尾随字段、非法 cycle 标志、零增量、无效范围和非法 cache。`nextval` 的 cache 批量计算、`currval`、`setval`、创建/修改序列均使用 checked int64 算术，`INT64` 上下界和耗尽序列不会触发未定义行为；重启后状态保持，损坏元数据 fail-closed。旧序列文件不作为兼容格式接受。

2026-08-14 DDL 生命周期安全修复：DDL 回滚现在会清理自身创建的物理快照，避免失败的 `ALTER SEQUENCE` 在下次启动被误恢复为旧数据库状态；typed `DdlExecutor` 以作用域守卫恢复线程局部 `Session*`，避免会话销毁后嵌入式 `nextval` 解引用悬空指针。重启、损坏序列文件、整数边界和失败 DDL 快照均有回归覆盖。

2026-08-13 WAL/恢复输入边界收紧：WAL 记录长度、8 字节对齐、CRC、记录链和 segment 尾部在打开时严格验证；`xl_prev` 保留截断历史时允许指向已删除 segment，但禁止指向当前保留流的未来位置。恢复 heap page image 必须通过当前 8 KiB 页 checksum/layout 校验，page ID 只能连续扩展一页，非法 page image、越界 page ID 和损坏 WAL 均 fail-closed，避免无界分配或把损坏页写入关系。`XLogFlush` 只接受当前 WAL 流中的有效记录位置，并同步覆盖记录所在完整 segment。

2026-08-13 DDL 路由收敛：删除 `src/main.cpp` 中已被 typed bridge 完整覆盖的旧字符串实现，
包括 `CREATE/DROP DOMAIN`、`CREATE/DROP SEQUENCE`、`CREATE/DROP SCHEMA`、数据库、角色/用户/组
和排序规则的标准路径。这些命令现在只有 parser → `DdlExecutor` → `StorageEngine` 一条标准
执行链；新增真实 bridge 路由回归，验证创建、查询和删除均不再依赖 legacy 分支。未迁移的
高级对象命令仍保留明确的兼容边界。

2026-08-13 序列变更路由收敛：`ALTER SEQUENCE` 的 `RESTART`、`INCREMENT`、边界、缓存、
循环、`OWNED BY` 和 `RENAME TO` 均由 typed `DdlExecutor` bridge 执行。重命名通过
`StorageEngine` 原子改名并同步目录、保留 catalog OID、更新 `nextval` 默认表达式和依赖关系；
冲突或中途失败由 DDL 快照回滚。新增真实路由回归验证重启、增量、重命名、重启持久化和冲突保持。

2026-08-13 配置作用域与计划缓存收敛：普通 `SET` 只修改当前 `Session` 的
`statement_timeout`、`lock_timeout` 和 `deadlock_timeout`；进程级参数必须通过管理员
`SET GLOBAL` 或 `ALTER SYSTEM SET` 修改，非管理员和误用普通 `SET` 均 fail-closed。
协议 backend 启动时显式复制全局默认 timeout，并将锁管理器的 timeout/resource namespace
绑定到当前 backend。`EXPLAIN` plan cache 纳入 `work_mem`、seq/hash/merge join、并行 worker
等 planner 设置，缓存开关和配置容量生效，配置变化会清空旧缓存；协议 E2E 新增跨连接隔离、权限和
缓存失效回归。完整 PostgreSQL GUC 体系、真正 per-query planner context 仍未完成。

2026-08-13 配置持久化边界收敛：`Config` 现在对未知键、重复等号、非法布尔值、尾随字符、
负数/越界整数和非有限浮点值 fail-closed；加载使用候选对象，失败不会污染现有运行配置；
保存先校验，再通过临时文件、文件 `fsync`、原子 rename 和目录 `fsync` 发布。`SET GLOBAL`
和 `pg_reload_conf()` 复用同一严格解析边界，新增配置专项回归。真正的完整 PostgreSQL
GUC 类型/来源/权限矩阵仍未完成。

2026-08-13 DDL 类型边界收紧：typed DDL 的列类型转换不再把未知类型静默降级为
`varchar(255)`；CREATE TABLE、ADD COLUMN、ALTER COLUMN TYPE、`CREATE TABLE OF` 等路径在
物理变更前统一拒绝未知类型和非法类型修饰符，并补齐 `serial`/`smallserial`/`bigserial`、
`nchar`/`nvarchar`、`binary`/`varbinary`、`timetz`、`pg_lsn` 的现有存储工厂映射。专项回归
验证失败不会创建关系或改变既有 schema。自定义类型的完整 catalog/type I/O 语义仍未完成。

2026-08-13 Volcano 执行失败边界：新增 `PlanExecutionResult` 与 `executePlanChecked()`，将正常 EOF 和 `open()`/`next()` 失败分离；排序、分页、去重、集合、连接、窗口、聚合及子查询物化算子会向根节点传播子算子错误。主 SQL 的集合操作、普通 Volcano SELECT、聚合、窗口和派生子查询入口已检查执行结果并 fail-closed；旧空结果兼容入口已删除，契约回归验证失败不会被误报为空结果。

2026-08-13 运行时统计持久化：`RuntimeStats` 增加当前格式版本化 `.runtime_stats` 快照，在 checkpoint 和引擎关闭时通过 sidecar `flock`、临时文件、文件/目录 `fsync` 和原子 rename 发布；启动严格校验 magic、版本、长度、数据库归属和尾随字节，损坏文件 fail-closed。多个 backend 按已加载基线做增量合并，DROP/重建关系不会恢复旧统计；新增持久化、重载和损坏文件回归。

2026-08-13 SQL 统计持久化：`SqlStats` 复用共享的统计快照发布基础设施，按数据库写入版本化 `.sql_stats`；checkpoint、shutdown 和启动加载均接入，跨 backend 按已加载基线增量合并，magic/版本/数据库归属/长度/数值/尾随字节严格校验，损坏文件 fail-closed；新增重载与尾随数据拒绝回归。

2026-08-13 SQL 统计边界：`pg_stat_statements.max` 默认限制为 5000 条，可通过 `SET [GLOBAL] pg_stat_statements.max` 调整到 1–1,000,000；超限按调用次数、累计耗时和 key 确定性淘汰，并在加载/持久化合并时再次执行上限，避免长时间运行或多 backend 合并造成无界内存增长。

2026-08-13 SSI 增量审计：SERIALIZABLE 对单列主键/二级 B+Tree 的 `=、<、<=、>、>=` 谓词新增事务级逻辑 SIREAD 记录；INSERT/UPDATE/DELETE 同步登记对应索引键，提交时按 B+Tree 固定宽度顺序检测读谓词与写键重叠，并纳入双向 dangerous-structure 判断。该能力补齐了精确索引谓词的逻辑覆盖，但不等同于 PostgreSQL 的物理索引 predicate lock；复合/表达式/部分索引、其他访问方法、安全快照和完整 SSI 图规则仍待实现。

2026-08-13 B+Tree 键边界收敛：插入、查找、多值查找、删除和范围扫描的公共 API 统一按 20 字节固定键规范化；长键截断、重开索引后的比较和范围端点不再因调用方是否预先填充而产生不同结果。新增长键持久化/重开回归；旧索引文件按当前格式重新创建，不提供历史索引兼容。

2026-08-13 堆页损坏边界收紧：`PageAllocator` 现在校验当前格式文件头的 magic、版本、页数、`rowSize` 元数据、空闲链表范围和 FNV 校验和；页读取同时校验页 checksum、页布局边界、line pointer 和 special space，任何损坏均 fail-closed，不再把坏页交给执行层。`rowSize` 可超过单页容量，由 TEXT/BYTEA/TOAST 等逻辑处理路径负责外部化，文件头校验不错误限制其大小。文件截断/非整页文件拒绝打开；页压缩修正为保留末尾 special space，避免压缩后覆盖 free-list 元数据。新增损坏页和压缩布局回归；本格式不接受 checksum 为 0 的未校验页。

2026-08-13 执行器架构与索引回表收敛：删除未被任何生产/测试代码使用的 `IExpr`、`PlanNode` 和 `IExecutionPlanner` 伪接口，保留 `IOperator` 作为 Volcano 算子唯一生命周期契约；修正 `src/executor/README.md` 对已不存在 `src/optimizer/` 的过时描述。`IndexScanOp` 现在按 RID 直接读取目标 heap tuple，在统一 page shared lock 和 MVCC 可见性边界内完成回表，不再对每个索引命中重新扫描整张表；锁冲突和页面读取失败会终止扫描并向上层传播。

2026-08-13 RLS 与 Volcano 安全边界收敛：结构化 SELECT 发现当前关系需要 RLS 时，统一使用 `forEachVisibleRow(..., "SELECT")` 的策略感知扫描，并禁用索引、bitmap、并行访问路径，避免访问方法绕过 USING 策略；新增索引存在时的 RLS 绕过回归。

2026-08-13 Volcano 访问方法安全边界收敛：删除未具备 visibility map、heap 可见性和 NULL 位图证明的伪 `IndexOnlyScanOp`，所有等值索引路径统一回表并复用 page lock/MVCC/SSI；Bitmap AND/OR 也改用相同的受保护 RID 回表，并传播锁冲突和页面 I/O 失败。visibility-map 驱动的真正 index-only scan 仍明确标记为未实现。

复制管理器本轮完成线程安全收敛：复制槽查询改为返回受锁保护的值快照，standby、primary conninfo、同步复制和 slot active 状态统一受同一把锁保护；slot 名称/类型/plugin 组合在创建时校验，并提供显式激活/停用 API。新增并发回归覆盖状态写入、slot 生命周期和快照读取；这仍是进程内管理层，不代表已经具备 PostgreSQL 级真实 WAL sender/receiver、流复制或 PITR。

本轮并发安全审计修复了 `LockManager` 的真实生命周期问题：等待 row/page 锁时不再持有可能被清除的 map 元素引用；批量解锁只释放当前线程拥有的 token；表锁重入不会重复锁底层 `shared_mutex`，共享锁升级会先平衡自身 token；等待图在等待期间持续刷新并检测双线程死锁。新增 `tests/lock_manager_concurrency_test.cpp`，验证死锁受害者释放、重入/升级、row token 归属和清理；真正的物理索引范围 predicate lock、完整 PostgreSQL 锁模式矩阵和 SSI 规则仍未完成。

Snapshot export/import 已收紧为当前 v2 二进制格式：快照携带数据库身份，严格校验版本、长度、尾随字节、XID 列表排序/重复和非法 XID；仅允许在 REPEATABLE READ/SERIALIZABLE 事务首次读写前导入，禁止跨数据库、重复导入或导入后替换快照。该边界通过 `snapshot_export_import_test` 验证，但仍不等同于 PostgreSQL 的逻辑解码快照、跨集群生命周期或完整安全快照语义。

2026-08-12 增量审计：`StorageEngine` 实例现在引用进程级 `LockManager` 注册表，独立 embedded backend 对同一数据库的表、行和 gap 资源可见；表/行/gap key 使用数据库 namespace，避免不同数据库的同名表误冲突。锁资源注册表全局共享，但 namespace、lock timeout 和 deadlock timeout 按 backend 线程隔离，避免一个连接改变另一个连接的等待策略。表、row、page 锁新增数据库目录下安全编码文件的跨进程 `flock` 协调，gap 锁以每表保守互斥协调跨进程 predicate-lock 注册，独立进程冲突、释放后获取和 gap 阻塞已有 fork 回归；锁文件不会进入物理备份。仍未完成的是完整 PostgreSQL heavyweight/lightweight lock 矩阵、索引 predicate lock 和 wait event。

锁 API 现在全部标记为 `[[nodiscard]]`，`TableManage` 的 DDL、DML、索引、扫描、TOAST、JOIN、聚合和 VACUUM 调用方均显式传播锁冲突；新增 `tests/lock_failure_propagation_test.cpp` 验证真实表锁竞争返回 `LOCK_CONFLICT` 并清理等待状态。

DDL 回滚边界继续收敛：`DdlTransaction` 现在可以撤销 view、materialized view、UDF/TVF、procedure、trigger、RLS policy 和 collation 的 CREATE 记录；这些 CREATE undo 会注册到显式外层事务，并按 SAVEPOINT 边界逆序回放。DROP/REPLACE 及文件级 DDL 在变更前建立事务快照，外层完整 `ROLLBACK` 先恢复快照，再回放行级 undo；快照已被 DDL 修改后不允许继续执行另一条快照型 DDL，也不允许创建或回滚 SAVEPOINT，避免用整库快照伪造错误的语句/子事务边界。`ddl_transaction_skeleton_test` 已覆盖对象清理、跨语句 CREATE/DROP/REPLACE 外层 ROLLBACK 和安全拒绝路径。包含内存闭包式 DDL undo 的事务暂不允许 PREPARE TRANSACTION；完整依赖图仍待补齐。

本轮新增事务回归验证 INSERT、UPDATE、DELETE 的普通回滚和 `ROLLBACK TO SAVEPOINT` 会一致恢复主键、单列二级、复合、Hash 索引及 TOAST 线外值；索引写入失败会传播并回滚堆元组。事务 DELETE 保留死 tuple 到事务结束，savepoint 回滚清除 `xmax` 后仍可在同一事务提交并读取；提交后旧 UPDATE/DELETE 版本的 TOAST 块才回收。B+Tree/Hash 已接入索引文件 before/after WAL 镜像和恢复，但 GIN/GiST/SP-GiST/BRIN、原生 page-level WAL、跨访问方法原子提交和完整崩溃窗口仍未完成。

本轮补强保存点资源边界：保存点记录当前 backend 的表锁递归深度、row/page 锁 token 和 gap token；`ROLLBACK TO SAVEPOINT` 会释放保存点之后新增的锁，并恢复保存点前被升级的锁模式。真实锁回归覆盖 token 释放和共享/排他模式恢复；真正 PostgreSQL 子事务 ID、错误状态和完整资源隔离仍未完成。

本轮修复协议失败事务的提交边界：显式事务中的语句错误会进入 `ReadyForQuery('E')`/`25P02` 状态，普通后续语句被拒绝；此时收到 `COMMIT` 会按 PostgreSQL 语义执行完整回滚而不会发布此前写入，`ROLLBACK TO SAVEPOINT` 成功后可恢复到事务中。该状态目前仍由协议 backend 维护，尚未演进为完整 PostgreSQL 子事务 ID/错误状态目录。

本轮进一步收紧两阶段命令边界：`COMMIT PREPARED`/`ROLLBACK PREPARED` 不会被失败事务状态误改写为本地 `COMMIT`/`ROLLBACK`；未知 prepared transaction 现在返回执行错误，不再以成功响应吞掉失败。PREPARE 现在先刷出 backend 私有 heap/index 缓存，prepared 元数据使用临时文件、文件/目录 `fsync` 和原子 rename 发布，并写入并刷盘 PREPARE WAL；表、row、page、gap 的本地 mutex token 会安全转移，进程级 advisory lock 按 xid 保留，由任一 backend 的 COMMIT/ROLLBACK PREPARED 释放。prepared 文件保存结构化锁资源，启动恢复会在服务可用前重建表/row/page/gap 的本地与跨进程 advisory ownership；任何资源无法完整恢复都会 fail-closed。启动恢复现在区分 committed、aborted、prepared 和普通未决 xid：仍处于 prepared 的 xid 会注册回全局 ReadView 活跃集合，顺序扫描和索引回表都拒绝泄露其行；终结 WAL 已落盘但元数据清理中断时会在 redo/undo 后安全清理。普通事务 CLOG 故障产生的 COMMIT→ABORT 序列仍被允许。`prepared_transaction_test` 已通过独立进程验证跨 backend 锁阻塞、四类锁跨重启恢复、提交可见性、索引条件不可见、回滚不可见性和事务内禁止完成 prepared transaction。完整 2PC 全局目录和崩溃后 in-doubt 决策语义仍未完成。

本轮已完成的基础收敛：

- 删除未接入的 `ClusterLayout` 和旧 4 KiB `Page` 实现。
- 删除旧数据文件自动迁移路径；启动恢复不会把事务备份/归档目录误识别为数据库。
- 删除 CatalogService 对旧 `.stc` 元数据的隐式导入和 `.migrated` 标记；当前 catalog 只加载当前版本 `.cat` 文件，升级必须通过 SQL 导出后重建。
- 存储统一为 v2、8 KiB PostgreSQL 风格 heap page 和当前 schema 格式。
- schema、sequence、trigger 读取路径只接受当前格式；旧格式回退和截断文件的部分解析已删除，损坏元数据 fail-closed，不会按默认值继续写入。
- `CREATE TABLE` 的 schema、heap/partition、TOAST、主键/唯一索引和 `tlist.lst` 初始化现在检查失败并清理已写入的半成品；索引元数据写入失败不会遗留锁或缓存指针。
- DDL executor 在物理表创建成功后立即登记事务回滚记录；约束 metadata、EXCLUDE 或后续 catalog 步骤失败时不会留下已发布的表对象。`ALTER TABLE` 现在在 DDL 事务中使用当前格式整库快照，后续子命令失败会恢复 schema、参数、索引、TOAST、catalog 和关系文件；事务快照同时包含外置 tablespace 的数据库子目录，并正确排除 UNLOGGED 关系的物理文件；完整 DROP/ALTER 跨对象依赖 undo 仍待补齐。
- DDL 的规范执行器和 legacy 兼容分发现在都检查并传播 `commitTransaction()` 失败；WAL/CLOG/fsync、延迟约束或 SSI 提交失败时不会继续执行后续 DDL，也不会输出伪成功。`deferrable_test` 覆盖延迟 CHECK 阻断 `CREATE DATABASE` 隐式提交的路径；legacy DDL 共用同一提交守卫。
- DDL 包装事务现在检查 `StorageEngine::commitTransaction()` 的最终状态；延迟约束或 SSI 导致提交失败时会向执行器传播失败，不再输出伪成功，并在本事务拥有物理快照时恢复 DDL 改动。文件级 DDL 回滚只在显式启用时创建 `<db>.txn_backup.<xid>`，普通事务不再复制整库；快照事务持有数据库级排他锁，避免物理恢复覆盖并发提交。跨对象依赖 undo、全部 PostgreSQL 隐式提交边界仍待补齐。
- 事务快照恢复已接入启动恢复：重启扫描 WAL 的提交证据，已提交快照直接清理，未完成快照恢复到 DDL 前状态，PREPARE TRANSACTION 的快照保留到 COMMIT/ROLLBACK PREPARED；快照恢复失败会保留现场供后续诊断，不伪报成功。
- WAL/CLOG 提交路径已收紧：`XLogFlush()` 和 `CommitLog::flush()` 都返回并传播 segment/目录 `fsync` 失败；事务先刷盘 COMMIT WAL，再刷盘 CLOG committed 状态，CLOG 不可用时追加 ABORT WAL、执行 undo 并 fail-closed，避免运行时报告成功而恢复重新提交。CLOG 段现在通过临时文件原子替换、段文件 `fsync` 和 `pg_xact` 目录 `fsync` 持久化，写入失败保留 dirty 状态供重试；文件锁和按位合并避免独立 backend 的整段缓存互相覆盖，已打开的读缓存会按文件时间戳刷新；CLOG 截断在保存或目录 `fsync` 失败时保留段，不会先删后报错。
- Catalog 持久化现在使用临时文件、文件 `fsync`、原子 rename 和目录 `fsync`；checkpoint、事务快照创建和关键 DDL 路径会检查 catalog 持久化结果，失败时 fail-closed，不再把半写 catalog 报告为成功。
- WAL 归档现在先对源段 `fsync`，再以临时文件完整复制并 `fsync` 后原子替换归档文件，最后原子发布 `.done` 并同步 `archive_status` 目录；归档或状态发布失败时保留可重试的 `.ready` 状态，不会把未完整归档的段交给截断路径。
- WAL LSN 语义已收敛：LSN 0 作为首个合法日志位置，`INVALID_LSN` 使用范围外哨兵；恢复逻辑明确跳过未初始化页的无效页 LSN。已提交/非事务 page image 按 WAL 正序重做，未提交事务的 before-image 按逆序 undo，避免同一事务多次修改后恢复到中间状态。多个 WAL writer 通过进程互斥、WAL 文件锁和磁盘尾部刷新避免过期 LSN 覆盖日志。
- WAL 恢复完整性已收紧：索引镜像 payload 必须完整、仅允许合法对齐填充，路径必须位于当前数据库关系目录或已登记 tablespace 的数据库子目录且使用受支持的索引扩展名；索引写入失败或 heap image 无法解析/应用时，启动恢复 fail-closed 并输出数据库与 LSN，禁止在部分恢复状态下提供服务。物理备份带显式标记，启动扫描不会把离线备份当作活动数据库。
- INSERT 回滚覆盖复合/Hash 索引和 TOAST：普通事务与 SAVEPOINT 回滚按 `(key, RID)` 精确移除多值索引项，并为堆删除写入 WAL before/after image，避免回滚行在重启恢复时复活；Hash AM 的插入/删除失败会向上返回。
- UPDATE/DELETE 回滚覆盖复合/Hash 索引和 TOAST：UPDATE 回滚只删除新版本独占的 TOAST 块并恢复旧索引键；DELETE 在显式事务内保留 heap tuple，普通回滚和 SAVEPOINT 回滚清除 `xmax` 并写入 WAL before/after image；提交后清理旧版本 TOAST，避免回滚后行或线外值丢失。
- BufferPool/Checkpoint 刷盘已收紧：`pwrite/fsync` 失败会保留 dirty 状态并向 `PageAllocator`、`checkpoint()` 和交互式 `CHECKPOINT` 传播；checkpoint 统一刷已加载的 heap/index 缓存，并在数据库仍有活动事务时拒绝推进恢复起点，避免活动事务的 WAL 证据被 checkpoint 跳过；clock-sweep 不再强制淘汰 pinned 页或丢弃无法写出的脏页，读取失败返回空指针；checkpoint 元数据和 archive status 未完成持久化时不会报告成功；归档成功后会在同一 WAL 文件锁内回收 checkpoint 之前的完整段，恢复从最早保留段开始扫描。
- PageAllocator 与 B+Tree 已适配 BufferPool 的失败契约：heap header/page 和索引 node/header 读写遇到 I/O 失败会返回失败，不再直接解引用空页；B+Tree 打开时拒绝损坏的 order/header。DDL 快照创建前、COMMIT WAL 发布前和引擎退出时会统一刷已加载的 heap、B+Tree、TOAST index、Hash 缓存；B+Tree/Hash 刷盘会先写 before-image、再刷文件、最后写 after-image，并在恢复时按事务提交状态选择镜像。GIN/GiST/SP-GiST/BRIN、原生 page-level WAL、跨访问方法原子提交和完整崩溃窗口仍未完成。
- Hash/GIN/BRIN 独立索引文件现在使用严格格式校验；写入采用临时文件 + fsync + 原子 rename，写入失败保留 dirty 状态，截断、非法版本和尾随垃圾会拒绝打开。BRIN 当前格式按本项目策略重建，不兼容旧索引文件；StorageEngine 中各访问方法的完整 WAL-safe 增量维护仍未完成。
- `StorageEngine::forEachRow()` 现在返回并传播 heap/partition 页面打开与读取失败；B-tree、复合、全文、GiST、SP-GiST、Hash、GIN、BRIN 构建以及 `REINDEX` 会在扫描失败时返回错误；全文/GiST/SP-GiST/GIN/BRIN 文件在完整扫描成功后才原子发布，B-tree/Hash 的 WAL-safe 构建仍未完成。过滤器、聚合、JOIN、FK/EXCLUDE 检查、`ANALYZE`、表重写、TOAST 写入和 Volcano 并行 page-range scan 现在也区分 I/O 失败与合法空结果；统计文件采用原子替换。BRIN 使用长度前缀格式保留带空格的边界值，损坏索引读取 fail-closed。索引增量维护的 WAL 语义仍未完成。
- `DROP TABLE` 现在先生成只读 `CASCADE/RESTRICT` 依赖计划，物理删除成功后才应用 catalog 删除计划，避免物理失败时 catalog 先被移除。
- `DROP SCHEMA` 现在同样先生成只读 namespace 依赖计划，物理 schema 删除成功后才应用 catalog 计划；若 catalog 后处理失败则恢复当前格式事务快照，避免 schema 目录与物理对象分裂。
- 当前 schema 格式升级为 `0x44420009`，表名、列名、类型名和约束名字段统一保留 64 字节（最多 63 字节标识符），不再静默截断 15 字节以上的合法标识符；旧 schema 按设计拒绝读取。
- `PageAllocator`、`PageWrapper`、TOAST 路径统一使用同一页格式。
- 表空间物理路径已收敛：pg_default 关系文件保留在数据库目录，自定义表空间使用
  `<LOCATION>/<DATABASE>/`，heap、FSM/VM、各类索引、分区和 TOAST 共用此路由；
  `ALTER TABLE ... SET TABLESPACE` 支持同文件系统 rename 与跨文件系统 copy+remove，
  目标表空间不存在或运行时 marker 丢失时 fail-closed，不再静默创建默认目录空表。
- TOAST 线外值已纳入 zlib 压缩：chunk header 保存压缩标记与原始长度，读取路径校验后解压；当前格式不兼容旧 TOAST chunk，符合本项目不保留旧数据兼容的策略。lz4/pglz、列级 storage strategy 和 `toast_tuple_target` 仍未完成。
- B+Tree 已修复 root leaf 分裂、叶分裂中间键丢失、重复键跨叶查找、范围扫描重复返回和多值索引按 `(key, RID)` 精确删除；跨叶唯一键、跨内部节点重复键及精确删除回归已纳入索引测试。PG B-tree 的 dedup、删除合并、opclass/collation 和完整并发构建语义仍未完成。
- `DROP INDEX` 已迁移到 typed DDL：标准的 name-only、多名称、`IF EXISTS`、`CASCADE/RESTRICT` 语法维护物理索引、`pg_class` 和依赖关系；单列/Hash 索引持久化 SQL 名称到物理键的映射，旧 `ON table` 形式仅作为迁移期兼容语法保留。真正的 `CONCURRENTLY` 两阶段语义仍未完成。
- `CREATE INDEX ... USING {hash,gin,gist,brin,spgist}` 已接入同一 typed AST/DdlExecutor，按访问方法调用真实的 StorageEngine 实现并登记索引名；非 PostgreSQL 的 `CREATE GIN INDEX` 等特殊分支已从 `main.cpp` 删除。各访问方法的 PostgreSQL opclass、WAL、并发构建和维护语义仍不完整。
- 标准 `CREATE/UNIQUE INDEX` 的旧字符串解析块已从 `main.cpp` 删除（约 138 行）；分类为 `CreateIndex` 的语句现在只能由 parser + DdlExecutor 处理，解析失败会 fail-closed，不再存在第二套索引分发。
- 删除确认无引用的 helper 和重复的 parser/聚合辅助代码。
- 删除未被执行器使用的 `IStorageEngine` 伪适配层；`StorageEngine` 不再提供会静默返回空结果、0 或伪成功的接口 wrapper。
- 统一构建源文件清单为 `cmake/dbms_sources.txt`；CMake、主程序脚本和测试脚本不再各自维护生产源列表。
- 四个 shell 构建/测试入口统一复用 `scripts/build_common.sh` 的编译选项、include、TLS 检测和链接库；生产二进制和测试对象都记录配置指纹，编译参数、TLS 模式或源码变化会自动触发重建。测试编排唯一由 `build_tests.sh` 负责，且会自包含地构建 E2E 所需的 `dbms_main`；`run_all_tests_fast.sh` 仅提供安静输出并在失败时保留完整诊断。CMake 现在提供 `check`/CTest 标准入口，直接调用同一编排器并在配置阶段校验 manifest 源文件、重复项和入口文件。
- 生产二进制构建失败时测试入口立即 fail-closed，不会继续运行旧的 `dbms_main` 或报告混合版本结果。
- 测试运行时隔离已收敛：每个独立 C++ 测试使用临时工作目录，退出后回收其 WAL、catalog、`.txnid`、日志和测试数据库；`build_one_test.sh` 与完整入口共享同一隔离函数。
- DDL bridge 对已归属 AST 路径的解析失败改为 fail-closed，不再把语法错误交给 legacy 分发；`DdlExecutor` 解析失败也明确返回错误。
- `ALTER TABLE` 的基础高频子命令已迁移到 typed AST：ADD/DROP COLUMN、ALTER COLUMN（TYPE/DEFAULT/NULL）、RENAME COLUMN/CONSTRAINT/TABLE、CHECK/PRIMARY KEY/UNIQUE/FK/EXCLUDE 约束增删、SET LOGGED/UNLOGGED、STATISTICS、INHERIT、RLS enable/disable/force、分区 ATTACH/DETACH、trigger enable/disable、CLUSTER、REPLICA IDENTITY、VALIDATE/ALTER CONSTRAINT 和基础 storage 参数；约束状态写入统一 `.params`，副本标识同步 `pg_class.relreplident`。
- `CREATE TABLE ... PARTITION BY` 与 `CREATE TABLE ... PARTITION OF` 已统一进入 typed AST/DdlExecutor，子表 schema、bound 和 catalog 注册均有 SQL 回归覆盖；分区约束证明与 global/local index 语义仍未完成。
- 持续删除 `main.cpp` 中已被 typed bridge 遮蔽的 ALTER TABLE 字符串分支；本轮再移除约 300 行约束/CLUSTER/REPLICA 元数据重复处理，ALTER TABLE 这些动作现在统一由 parser → DdlExecutor → StorageEngine 执行。
- 测试入口改为缓存生产对象、逐测试独立链接运行；避免每个测试重复编译完整 DBMS，同时保留自定义源和本地 stub 测试覆盖。
- 文档入口已收敛：删除未引用且内容过时的根目录 `MANUAL.md`，`docs/MANUAL.md` 作为唯一完整使用手册，避免两份能力描述长期漂移。
- 修复 `DROP DATABASE` 未释放数据库级 page/index/TOAST/WAL/CLOG/catalog 缓存的问题；新增同名数据库重建回归测试，防止旧缓存迟写入新数据库。
- CLOG 刷盘在数据库目录或 `pg_xact` 子目录已被删除时不会重建目录或把旧事务状态写入同名新数据库；段更新采用原子替换，避免截断写入留下半段状态文件。
- 网络服务默认 fail-closed：证书/私钥缺失、OpenSSL 不可用或 TLS 初始化失败时拒绝启动；明文只能通过显式 `--insecure` 开启，且仅用于本地开发。
- 网络服务生命周期已收紧：启动失败通过返回值传播，端口占用不会伪报成功；`SIGINT`/`SIGTERM` 和显式 shutdown 请求会停止监听、唤醒活动连接并 join 所有客户端 worker，不再永久 detached。`network_server_lifecycle_test` 覆盖端口冲突和优雅退出。
- 删除运行时自动生成自签名证书的 shell 调用，避免私钥落盘位置和命令参数不可控；部署必须显式提供 TLS 材料。
- 网络服务已切换到 PostgreSQL Frontend/Backend protocol 3.0 核心路径：支持 SSLRequest 协商、StartupMessage、catalog SCRAM-SHA-256、参数状态、简单 Query，以及 Parse/Bind/Execute/Sync 基础流程；协议回归由 `tests/postgres_protocol_test.py` 覆盖真实 SCRAM 握手。
- legacy `execute()` 的协议结果捕获已改为线程局部 `process/OutputCapture` multiplexing；移除网络入口对全局 `std::cout` 缓冲区和 `g_outputCaptureMutex` 的依赖，协议会话不再因文本捕获而全局串行化，并有多线程无串扰回归。
- 单表 DML 的结构化路径已扩展到 `commands/DmlExecutor`：普通 `INSERT VALUES/DEFAULT VALUES`、简单单表 `INSERT ... SELECT`、无 target 或显式匹配主键/唯一约束 target 的 `ON CONFLICT DO NOTHING`、显式匹配单列或复合主键/唯一约束 target 的常量或只引用 `excluded` 的 evaluator 受限标量表达式 `ON CONFLICT DO UPDATE` 及目标行/`excluded` 的受限 `WHERE`、以当前目标行列值为输入的受限标量表达式单表 `UPDATE`、单源表 `UPDATE ... FROM`、单源表 `DELETE ... USING`、简单谓词单表 `DELETE` 和窄版单源表 `MERGE` 在进入 legacy 分发前统一执行；其中 UPDATE FROM/DELETE USING 支持来源 INNER/CROSS JOIN、来源别名和限定连接谓词，并复用 StorageEngine 约束、触发器、索引、RLS、FK/MVCC 和 ACL。MERGE 当前支持单个 MATCHED UPDATE/DO NOTHING、单个 NOT MATCHED INSERT/DO NOTHING，并在执行前拒绝多源行匹配同一目标行；多 WHEN、BY SOURCE/BY TARGET、DELETE、复杂 source query 和 RETURNING 明确 fail-closed。普通单表 INSERT/UPDATE/DELETE 的列投影和 evaluator 支持的受限标量表达式 `RETURNING` 在实际存储修改边界收集，并通过协议层发送结构化结果集与 command tag。复杂 SELECT 源、复杂谓词、外连接/复杂 JOIN、视图、复杂 `UPDATE ... FROM`/`USING`、部分/索引推断 conflict target、引用子查询或其他关系的 `ON CONFLICT DO UPDATE`/`WHERE`、复杂/子查询/窗口 RETURNING 仍回退到旧路径。列引用不再被误判为 NULL，带表限定的列引用优先解析，复合 UNIQUE 中含 NULL 的键按 PostgreSQL 默认语义不互相冲突，存储层默认值只对缺失列生效。
- 简单单表视图已支持行级 `INSTEAD OF INSERT/UPDATE/DELETE` action SQL；多行 `VALUES`、按实际匹配行的 UPDATE/DELETE、`NEW`/`OLD`/`WHEN` 和 server 会话执行均有协议回归，复杂视图映射与函数/PL 触发器运行时仍未完成。
- 顶层 `INSERT`/`UPDATE`/`DELETE`/`MERGE`/`REPLACE` 以及包含写 CTE 的 `WITH` 语句现在统一由 `execute()` 建立内部事务；普通语句成功自动提交，错误或异常自动回滚，递归触发器/视图/CTE 执行复用外层事务，避免多行 DML 在中途失败后留下部分写入。协议层同时把 `WITH` 查询识别为结果集。显式 `BEGIN` 仍由连接事务状态管理。
- `CREATE TEMP/TEMPORARY TABLE` 已进入 typed DDL：物理对象名包含 backend PID，临时表可遮蔽同名永久表但不会污染持久 catalog；用户临时表与查询内部 CTE/派生表临时对象分离管理，连接断开时统一清理。`ON COMMIT PRESERVE ROWS/DELETE ROWS/DROP` 已绑定 backend-local transaction commit/rollback，真正 `pg_temp` schema/catalog/search_path 语义仍未完成。
- 临时 relation 生命周期已覆盖异常退出边界：启动时在 WAL 恢复后清理残留的会话临时表、分区/TOAST fork、孤儿文件和 `tlist.lst` 条目，避免进程重启后的名称冲突和磁盘泄漏；`__tmp_<backend>_...` 是保留的内部物理命名空间。
- `UNION`/`INTERSECT`/`EXCEPT` 的组合语义已统一由 Volcano `SetOperationOp` 执行，覆盖顶层优先级、错误传播及 `ALL` 重复行语义；简单单表 operand 直接构建子计划，复杂 operand 通过 `MaterializedRowsOp` 接入，完整 AST 下推和类型合并尚未完成。
- 并行执行已具备可验证的 `ParallelTableScanOp`：非分区 heap 按 page range 由多个 worker 读取并按范围顺序 Gather，`max_parallel_workers_per_gather` 可配置；事务内、分区表、并行 join/aggregate、GatherMerge 和长期 worker pool 仍未完成。
- 窗口执行已具备可复用的 `WindowOp`/`WindowAgg` 计划节点：主 SQL 入口已接入常见排名/偏移、窗口聚合、`ROWS/RANGE/GROUPS` frame/exclusion、独立分区/排序、OFFSET 和最终结果排序；复杂目标列表和主 SQL EXPLAIN 的窗口解析仍保留 legacy fallback。
- 聚合执行已统一到可复用的 `GroupAggregateOp` 计划节点：无 GROUP BY 的普通聚合与常见 `GROUP BY`、`HAVING`、`ROLLUP/CUBE/GROUPING SETS` 均消费过滤后的 Volcano 子计划；复杂目标、`GROUPING()`/`GROUPING_ID`、完整排序作用域和并行聚合仍待完成。
- 未关联单列 `IN`/`NOT IN` 已下推到 Volcano `SemiJoinOp`（anti 模式）；未关联单表 `EXISTS`/`NOT EXISTS` 已下推到 `ExistenceFilterOp`；单个未关联标量目标已下推到 init-plan + `ScalarSubqueryProjectOp`，严格处理 NULL 和多行 cardinality error；单列未关联 `ANY`/`ALL` 已下推到 `QuantifiedSubqueryFilterOp`，严格处理 NULL/空集三值逻辑。相关行为均有单测与协议回归；关联/复杂标量、row comparison 和复杂组合仍保留 legacy fallback。
- 多个等值索引条件已由 `BitmapHeapScanOp`/`BitmapOrHeapScanOp` 执行候选 RID 的 AND/OR 组合，再统一 heap fetch 和原谓词重检；范围 bitmap、并行 bitmap 和真正 block bitmap 扫描尚未完成。
- SERIALIZABLE 事务对非空索引谓词登记命中页、对顺序扫描登记实际扫描页，并保留空集/无法安全暴露页来源时的关系级 SIREAD 兜底；提交时页级覆盖参与 rw-conflict 检测，非相交页可并发提交，跨页危险结构仍会返回 serialization failure。真正的索引范围 predicate lock、完整 SSI 冲突图和安全快照仍未完成。
- 扩展查询已支持文本及常用类型二进制参数/结果（bool/int2/int4/int8/oid/float4/float8/text/varchar/date/time/timestamp/timestamptz/uuid/numeric；日期时间按当前引擎秒精度存储，numeric 使用 PostgreSQL base-10000 wire 格式并以精确 decimal 文本保存）、`Parse` 参数描述、`Bind` 数量/格式/NULL 校验、`Describe`/`Close` 生命周期、`$n` 字面量绑定和基础 portal `Execute maxRows` 分批返回（含 `PortalSuspended`）；常见单表列会返回 catalog/table schema 驱动的 OID、长度、属性号和表 OID，复杂表达式仍回退为 text。数组等复杂类型的二进制 I/O、完整 RowDescription 类型推导以及 holdable/scrollable cursor 等完整 portal 语义仍待实现。
- 表/列 ACL 检查已统一解析会话用户自身、递归继承角色和 `PUBLIC` 授权；`NOINHERIT` 用户不会自动获得成员角色的 ACL/RLS 权限，原始成员关系仍单独供 `pg_hba.conf` 角色匹配使用；真实协议和策略回归验证了继承与拒绝边界。RLS 现已通过关系感知扫描统一应用到查询、更新、删除及结构化 DML 来源关系，并实现默认 `WITH CHECK`、显式 `TO PUBLIC`、基础 `PERMISSIVE/RESTRICTIVE` 组合、表 owner 绕过、基于 `pg_authid` 的 `SUPERUSER/BYPASSRLS` 绕过和 `FORCE ROW LEVEL SECURITY`；无适用策略默认拒绝，策略求值失败安全回退。对象全集 owner/依赖、完整 ACL item/继承语义、schema/database/function ACL 和完整 ACL 组合语义仍待补齐。
- `ALTER TABLE ... OWNER TO` 已收紧为表所有者/超级用户操作，并要求当前会话能够 `SET ROLE` 到目标角色；`SET ROLE` 按原始成员关系授权，`NOINHERIT` 不会错误阻止显式切换。`current_user`、RLS、结构化 DML ACL 和 CREATE TABLE owner 均使用当前有效角色；完整对象 owner 传播和 ACL 组合仍待补齐。
- 表 owner 已在统一 ACL 查询中获得隐含表/列权限和 `GRANT OPTION`；表级 `REVOKE` 复用同一授权判定。schema/database/function ACL、ACL item 和默认权限完整传播仍未完成。
- 表级 `GRANT OPTION` 已支持独立撤销、依赖授权的 `RESTRICT` 拒绝和 `CASCADE` 递归回收；表级授权链会记录实际 grantor，并在撤销后清理失效链路。完整 ACL item、对象类型和默认权限传播仍未完成。
- `ALTER DEFAULT PRIVILEGES` 已迁移到 typed AST/DdlExecutor，支持 `TABLE/TABLES` 的 `GRANT`、`REVOKE`、多权限/多 grantee、schema 校验和幂等规则，并在新建表时按 effective owner 应用；默认权限的 grant option、sequence/function 等对象类型和完整 catalog ACL 仍未完成。
- `TRUNCATE` 已迁移到 typed AST/DdlExecutor，支持 `ONLY`、多表、`RESTART/CONTINUE IDENTITY`、递归 FK `CASCADE` 与 statement-atomic `RESTRICT` 预检；trigger/foreign table 和完整 PostgreSQL transactional/locking 语义仍未完成。
- `pg_auth_members.admin_option` 已接入角色 `GRANT ... WITH ADMIN OPTION`、重复授权升级和 `REVOKE ADMIN OPTION FOR`；角色授权现在按超级用户、CREATEROLE 或现有 ADMIN OPTION 检查，完整 ADMIN OPTION 级联和 grantor 生命周期仍待补齐。
- 协议错误状态已收敛：扩展查询在 Parse/Bind/Execute 错误后进入 PostgreSQL 的 ignore-until-Sync 状态；事务外简单查询错误返回 `ReadyForQuery('I')`，连接可在 Sync/错误响应后继续使用。数组等复杂类型、完整类型映射、二进制扩展消息语义仍未完成。
- 事务上下文已从共享 `StorageEngine` 实例移为连接工作线程局部：事务 ID、快照、回滚日志、savepoint、隔离级别、延迟约束和 `lastval` 不再在协议连接之间互相覆盖；连接断开时会回滚未完成事务并丢弃 backend 上下文。全局锁管理器和提交状态仍用于跨 backend 协调。双连接协议回归已验证未提交行隔离、回滚恢复、断开回滚和提交后可见性。
- TCL 路由已收敛到事务 AST：`BEGIN`/`START TRANSACTION` 的隔离级别与 READ ONLY/WRITE 选项、`SAVEPOINT`、`ROLLBACK TO` 和 `RELEASE` 不再依赖固定字符串偏移；分类顺序已修复，`ROLLBACK TO`/`COMMIT PREPARED`/`ROLLBACK PREPARED` 不会被通用前缀吞掉。`DEFERRABLE` 在执行层明确拒绝，避免静默宣称未实现语义。
- SQL 统计已从 `main.cpp` 提取为线程安全 `process/SqlStats` 模块，交互式与 PostgreSQL 协议入口共用；字符串/数字常量和空白归一化后聚合，`SHOW STATEMENTS` 与 `pg_stat_statements` 风格虚拟表可查询。当前格式 `.sql_stats` 已在 checkpoint/引擎关闭时持久化并在启动时严格加载；上限/淘汰、reset 权限和完整 PostgreSQL 扩展字段仍未完成。
- 运行时统计已从显示层下沉到线程安全 `process/RuntimeStats`：SQL 执行、失败、提交/回滚，以及 StorageEngine 和 Volcano 扫描算子的顺序扫描、索引扫描和实际 DML 行数会进入共享计数器；完整可见表扫描建立的 live-row 估计会反馈给 Join 成本和 EXPLAIN，部分/索引扫描只保留展示用下界，表重建/截断会清除旧关系身份的估计；`SHOW STATUS`、`pg_stat_database` 和 `pg_stat_tables` 不再输出固定零值。当前格式 `.runtime_stats` 已持久化，索引访问方法细分和后台采样线程仍未完成。
- 构建质量收敛：修复 planner 的 merge join cost 参数错误，清理 parser 未使用参数和测试冗余 helper；legacy 输出捕获已从全局重定向改为线程局部路由；`./scripts/build.sh` 在 `-Wall -Wextra` 下通过且无编译警告，完整回归与 OpenSSL Docker 构建均通过。
- 构建缓存现在按编译配置、源码清单、生产源码和头文件内容计算 SHA-256 签名；测试对象另按全部测试源计算独立签名，不再仅依赖 mtime。Git 回滚、工作区恢复或复制数据目录后会安全失效并重编译，避免测试链接到过期对象，也不会因测试改动无谓重编译生产主程序。
- PostgreSQL 协议 E2E 测试不再把服务端 stdout/stderr 连接到无人消费的管道，避免长流程输出填满 pipe 后阻塞服务；协议和窗口 E2E 的超时可通过环境变量覆盖，默认值适配慢速持久化/CI 环境，避免把正常慢执行误判为随机失败。

启动安全边界：非空但 magic/版本不匹配的数据文件会直接拒绝打开，不会被清零或按新格式覆盖。部署时必须将数据目录初始化为当前格式，并通过备份恢复或 SQL 导入完成升级。

数据兼容边界：旧 schema、旧 4 KiB 数据页和旧行头不会被读取或迁移。升级前必须导出 SQL，或删除并重建数据目录。

仍不能称为生产就绪的主要原因包括：SSI 目前已增加页级 SIREAD 和空范围关系级兜底，但索引范围 predicate lock、完整 rw-conflict 规则和安全快照仍不完整；当前 wire protocol 仍缺完整类型/错误/扩展消息语义、channel binding 和结构化执行结果，owner/依赖和完整 ACL 组合语义仍不完整；表空间仍缺权限/owner、ALTER TABLESPACE 完整语义及 PostgreSQL OID/符号链接布局；并行执行、流复制/PITR、完整系统目录接入、审计/可观测性和系统化故障注入测试也仍不完整。后台 writer/checkpointer 与 DDL/database lifecycle 的文件缓存并发访问已加锁并纳入回归验证。网络连接容量现在通过原子槽位预留控制并发 accept，TLS 握手失败和认证失败都会释放槽位。后续改动必须以代码路径、回归测试和故障恢复验证为准，不能只以功能清单宣称完成。

2026-08-13 parser 数值选项继续 fail-closed：`CREATE FUNCTION` 的 `COST/ROWS`、角色 `CONNECTION LIMIT` 和 `ALTER TABLE ... SET STATISTICS` 现在拒绝缺失、非法、非有限、越界或不允许的值，不再吞异常后使用 AST 默认值。

2026-08-13 sequence DDL 输入边界收紧：`CREATE/ALTER SEQUENCE` 的 `START`、`INCREMENT`、`MINVALUE`、`MAXVALUE`、`CACHE` 等整数选项现在要求完整 int64 数字；缺失值、溢出值和未知选项在 parser/executor 两层 fail-closed，不再静默使用默认序列参数。

2026-08-13 DDL 路由继续收敛：删除 `main.cpp` 中已被 `tryDdlBridge → DdlExecutor` 完整遮蔽的 `ALTER ROLE/ALTER USER` 字符串执行器（约 100 行）；`ALTER GROUP` 等尚未迁移的兼容路径仍保留。

验证入口：`./scripts/build.sh`、`./scripts/run_all_tests_fast.sh`、`./scripts/build_tests.sh`；两个 E2E 已由统一测试入口自动执行。DML AST 路径另由 parser 单测和协议 E2E 覆盖。Docker 镜像构建使用 `docker build`。CMake 验证需要环境提供 `cmake` 可执行文件。
- SQL 解析边界继续收敛：`LIMIT/OFFSET/FETCH` 的无符号计数现在要求完整十进制整数，非法值、缺失值和不完整 FETCH 子句均返回解析错误，不再被异常吞掉后静默变成默认的“无限制”。`FETCH FIRST/NEXT` 省略 count 时按 PostgreSQL 语法默认 1 行处理。

# 生产化状态

最后更新：2026-08-11

当前版本处于生产化重构阶段，不能宣称已经达到 PostgreSQL 的生产级完整度。当前可验证基线为：主程序构建成功，125 个 C++ 回归测试和 2 个 E2E（协议、窗口函数）共 `PASS=127 FAIL=0`，其中窗口函数 E2E 为 `13/13`。

本轮已完成的基础收敛：

- 删除未接入的 `ClusterLayout` 和旧 4 KiB `Page` 实现。
- 删除旧数据文件自动迁移路径；启动恢复不会把事务备份/归档目录误识别为数据库。
- 删除 CatalogService 对旧 `.stc` 元数据的隐式导入和 `.migrated` 标记；当前 catalog 只加载当前版本 `.cat` 文件，升级必须通过 SQL 导出后重建。
- 存储统一为 v2、8 KiB PostgreSQL 风格 heap page 和当前 schema 格式。
- schema、sequence、trigger 读取路径只接受当前格式；旧格式回退和截断文件的部分解析已删除，损坏元数据 fail-closed，不会按默认值继续写入。
- `CREATE TABLE` 的 schema、heap/partition、TOAST、主键/唯一索引和 `tlist.lst` 初始化现在检查失败并清理已写入的半成品；索引元数据写入失败不会遗留锁或缓存指针。
- DDL executor 在物理表创建成功后立即登记事务回滚记录；约束 metadata、EXCLUDE 或后续 catalog 步骤失败时不会留下已发布的表对象。`ALTER TABLE` 现在在 DDL 事务中使用当前格式整库快照，后续子命令失败会恢复 schema、参数、索引、TOAST、catalog 和关系文件；事务快照同时包含外置 tablespace 的数据库子目录，并正确排除 UNLOGGED 关系的物理文件；完整 DROP/ALTER 跨对象依赖 undo 仍待补齐。
- DDL 包装事务现在检查 `StorageEngine::commitTransaction()` 的最终状态；延迟约束或 SSI 导致提交失败时会向执行器传播失败，不再输出伪成功，并在本事务拥有物理快照时恢复 DDL 改动。跨对象依赖 undo、并发 DDL 锁和全部 PostgreSQL 隐式提交边界仍待补齐。
- 普通事务的 `.txn_backup` 生命周期已收敛：成功提交和正常回滚都会清理备份；DDL wrapper 明确声明需要保留备份，避免测试/生产运行中累积孤儿快照；重新开始事务时不会吞掉前一事务的隐式提交错误。
- WAL 提交路径已收紧：`XLogFlush()` 返回并传播 segment `fsync` 失败；事务先成功写入并刷盘 COMMIT WAL 记录，再发布 CLOG committed 状态，WAL 不可用时 fail-closed 回滚，避免崩溃恢复缺少提交证据。
- WAL LSN 语义已收敛：LSN 0 作为首个合法日志位置，`INVALID_LSN` 使用范围外哨兵；恢复逻辑明确跳过未初始化页的无效页 LSN。多个 WAL writer 通过进程互斥、WAL 文件锁和磁盘尾部刷新避免过期 LSN 覆盖日志。
- BufferPool/Checkpoint 刷盘已收紧：`pwrite/fsync` 失败会保留 dirty 状态并向 `PageAllocator`、`checkpoint()` 和交互式 `CHECKPOINT` 传播；clock-sweep 不再强制淘汰 pinned 页或丢弃无法写出的脏页，读取失败返回空指针；checkpoint 元数据和 archive status 未完成持久化时不会报告成功。WAL segment 截断仍未实现，当前保留日志用于恢复。
- PageAllocator 与 B+Tree 已适配 BufferPool 的失败契约：heap header/page 和索引 node/header 读写遇到 I/O 失败会返回失败，不再直接解引用空页；B+Tree 打开时拒绝损坏的 order/header。索引多页更新的完整 WAL/原子提交仍未完成。
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
- B+Tree 已修复 root leaf 分裂、叶分裂中间键丢失、重复键跨叶查找和范围扫描重复返回；跨叶唯一键、跨内部节点重复键回归已纳入索引测试。PG B-tree 的 dedup、删除合并、opclass/collation 和完整并发构建语义仍未完成。
- `DROP INDEX` 已迁移到 typed DDL：标准的 name-only、多名称、`IF EXISTS`、`CASCADE/RESTRICT` 语法维护物理索引、`pg_class` 和依赖关系；单列/Hash 索引持久化 SQL 名称到物理键的映射，旧 `ON table` 形式仅作为迁移期兼容语法保留。真正的 `CONCURRENTLY` 两阶段语义仍未完成。
- `CREATE INDEX ... USING {hash,gin,gist,brin,spgist}` 已接入同一 typed AST/DdlExecutor，按访问方法调用真实的 StorageEngine 实现并登记索引名；非 PostgreSQL 的 `CREATE GIN INDEX` 等特殊分支已从 `main.cpp` 删除。各访问方法的 PostgreSQL opclass、WAL、并发构建和维护语义仍不完整。
- 标准 `CREATE/UNIQUE INDEX` 的旧字符串解析块已从 `main.cpp` 删除（约 138 行）；分类为 `CreateIndex` 的语句现在只能由 parser + DdlExecutor 处理，解析失败会 fail-closed，不再存在第二套索引分发。
- 删除确认无引用的 helper 和重复的 parser/聚合辅助代码。
- 删除未被执行器使用的 `IStorageEngine` 伪适配层；`StorageEngine` 不再提供会静默返回空结果、0 或伪成功的接口 wrapper。
- 统一构建源文件清单为 `cmake/dbms_sources.txt`；CMake、主程序脚本和测试脚本不再各自维护生产源列表。
- 四个 shell 构建/测试入口统一复用 `scripts/build_common.sh` 的编译选项、include、TLS 检测和链接库；对象缓存记录配置指纹，编译参数或 TLS 模式变化会自动失效。测试编排唯一由 `build_tests.sh` 负责，`run_all_tests_fast.sh` 仅提供安静输出并在失败时保留完整诊断。
- DDL bridge 对已归属 AST 路径的解析失败改为 fail-closed，不再把语法错误交给 legacy 分发；`DdlExecutor` 解析失败也明确返回错误。
- `ALTER TABLE` 的基础高频子命令已迁移到 typed AST：ADD/DROP COLUMN、ALTER COLUMN（TYPE/DEFAULT/NULL）、RENAME COLUMN/CONSTRAINT/TABLE、CHECK/PRIMARY KEY/UNIQUE/FK/EXCLUDE 约束增删、SET LOGGED/UNLOGGED、STATISTICS、INHERIT、RLS enable/disable/force、分区 ATTACH/DETACH、trigger enable/disable、CLUSTER、REPLICA IDENTITY、VALIDATE/ALTER CONSTRAINT 和基础 storage 参数；约束状态写入统一 `.params`，副本标识同步 `pg_class.relreplident`。
- `CREATE TABLE ... PARTITION BY` 与 `CREATE TABLE ... PARTITION OF` 已统一进入 typed AST/DdlExecutor，子表 schema、bound 和 catalog 注册均有 SQL 回归覆盖；分区约束证明与 global/local index 语义仍未完成。
- 持续删除 `main.cpp` 中已被 typed bridge 遮蔽的 ALTER TABLE 字符串分支；本轮再移除约 300 行约束/CLUSTER/REPLICA 元数据重复处理，ALTER TABLE 这些动作现在统一由 parser → DdlExecutor → StorageEngine 执行。
- 测试入口改为缓存生产对象、逐测试独立链接运行；避免每个测试重复编译完整 DBMS，同时保留自定义源和本地 stub 测试覆盖。
- 修复 `DROP DATABASE` 未释放数据库级 page/index/TOAST/WAL/CLOG/catalog 缓存的问题；新增同名数据库重建回归测试，防止旧缓存迟写入新数据库。
- CLOG 刷盘在数据库目录已被删除时不会重建目录或把旧事务状态写入同名新数据库。
- 网络服务默认 fail-closed：证书/私钥缺失、OpenSSL 不可用或 TLS 初始化失败时拒绝启动；明文只能通过显式 `--insecure` 开启，且仅用于本地开发。
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
- SERIALIZABLE 事务对关系级读取登记 SIREAD 覆盖，即使谓词返回空集也会参与 rw-conflict 检测；该粒度是保守安全边界，页/索引级 predicate lock 和完整 SSI 冲突图仍未完成。
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
- SQL 统计已从 `main.cpp` 提取为线程安全 `process/SqlStats` 模块，交互式与 PostgreSQL 协议入口共用；字符串/数字常量和空白归一化后聚合，`SHOW STATEMENTS` 与 `pg_stat_statements` 风格虚拟表可查询。统计当前仅驻留内存，持久化、上限/淘汰和完整 PostgreSQL 扩展字段仍未完成。
- 运行时统计已从显示层下沉到线程安全 `process/RuntimeStats`：SQL 执行、失败、提交/回滚，以及 StorageEngine 和 Volcano 扫描算子的顺序扫描、索引扫描和实际 DML 行数会进入共享计数器；`SHOW STATUS`、`pg_stat_database` 和 `pg_stat_tables` 不再输出固定零值。统计仍仅驻留内存，索引访问方法细分、历史持久化和后台采样线程仍未完成。
- 构建质量收敛：修复 planner 的 merge join cost 参数错误，清理 parser 未使用参数和测试冗余 helper；legacy 输出捕获已从全局重定向改为线程局部路由；`./scripts/build.sh` 在 `-Wall -Wextra` 下通过且无编译警告，完整回归与 OpenSSL Docker 构建均通过。

启动安全边界：非空但 magic/版本不匹配的数据文件会直接拒绝打开，不会被清零或按新格式覆盖。部署时必须将数据目录初始化为当前格式，并通过备份恢复或 SQL 导入完成升级。

数据兼容边界：旧 schema、旧 4 KiB 数据页和旧行头不会被读取或迁移。升级前必须导出 SQL，或删除并重建数据目录。

仍不能称为生产就绪的主要原因包括：SSI 目前已增加关系级 SIREAD 以覆盖空范围读，但页/索引粒度 predicate lock 和完整 rw-conflict 规则仍不完整；当前 wire protocol 仍缺完整类型/错误/扩展消息语义、channel binding 和结构化执行结果，owner/依赖和完整 ACL 组合语义仍不完整；表空间仍缺权限/owner、ALTER TABLESPACE 完整语义及 PostgreSQL OID/符号链接布局；并行执行、流复制/PITR、完整系统目录接入、审计/可观测性和系统化故障注入测试也仍不完整。后台 writer/checkpointer 与 DDL/database lifecycle 的文件缓存并发访问已加锁并纳入回归验证。网络连接容量现在通过原子槽位预留控制并发 accept，TLS 握手失败和认证失败都会释放槽位。后续改动必须以代码路径、回归测试和故障恢复验证为准，不能只以功能清单宣称完成。

验证入口：`./scripts/build.sh`、`./scripts/run_all_tests_fast.sh`、`./scripts/build_tests.sh`；两个 E2E 已由统一测试入口自动执行。DML AST 路径另由 parser 单测和协议 E2E 覆盖。Docker 镜像构建使用 `docker build`。CMake 验证需要环境提供 `cmake` 可执行文件。

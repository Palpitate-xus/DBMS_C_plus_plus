# 生产化状态

最后更新：2026-08-08

当前版本处于生产化重构阶段，不能宣称已经达到 PostgreSQL 的生产级完整度。当前可验证基线为：主程序构建成功，120 个 C++ 回归测试和 2 个 E2E（协议、窗口函数）共 `PASS=122 FAIL=0`，其中窗口函数 E2E 为 `13/13`。

本轮已完成的基础收敛：

- 删除未接入的 `ClusterLayout` 和旧 4 KiB `Page` 实现。
- 删除旧数据文件自动迁移路径；启动恢复不会把事务备份/归档目录误识别为数据库。
- 存储统一为 v2、8 KiB PostgreSQL 风格 heap page 和当前 schema 格式。
- schema、sequence、trigger 读取路径只接受当前格式；旧格式回退和截断文件的部分解析已删除，损坏元数据 fail-closed，不会按默认值继续写入。
- `PageAllocator`、`PageWrapper`、TOAST 路径统一使用同一页格式。
- TOAST 线外值已纳入 zlib 压缩：chunk header 保存压缩标记与原始长度，读取路径校验后解压；当前格式不兼容旧 TOAST chunk，符合本项目不保留旧数据兼容的策略。lz4/pglz、列级 storage strategy 和 `toast_tuple_target` 仍未完成。
- B+Tree 已修复 root leaf 分裂、叶分裂中间键丢失、重复键跨叶查找和范围扫描重复返回；跨叶唯一键、跨内部节点重复键回归已纳入索引测试。PG B-tree 的 dedup、删除合并、opclass/collation 和完整并发构建语义仍未完成。
- 删除确认无引用的 helper 和重复的 parser/聚合辅助代码。
- 删除未被执行器使用的 `IStorageEngine` 伪适配层；`StorageEngine` 不再提供会静默返回空结果、0 或伪成功的接口 wrapper。
- 统一构建源文件清单为 `cmake/dbms_sources.txt`；CMake、主程序脚本和测试脚本不再各自维护生产源列表。
- 四个 shell 构建/测试入口统一复用 `scripts/build_common.sh` 的编译选项、include、TLS 检测和链接库；对象缓存记录配置指纹，编译参数或 TLS 模式变化会自动失效。
- DDL bridge 对已归属 AST 路径的解析失败改为 fail-closed，不再把语法错误交给 legacy 分发；`DdlExecutor` 解析失败也明确返回错误。
- `ALTER TABLE` 的基础高频子命令已迁移到 typed AST：ADD/DROP COLUMN、ALTER COLUMN（TYPE/DEFAULT/NULL）、RENAME COLUMN/CONSTRAINT/TABLE、约束增删、SET LOGGED/UNLOGGED、STATISTICS、INHERIT 和基础 storage 参数；未迁移动作仍明确回退到 legacy 路径。
- `CREATE TABLE ... PARTITION BY` 与 `CREATE TABLE ... PARTITION OF` 已统一进入 typed AST/DdlExecutor，子表 schema、bound 和 catalog 注册均有 SQL 回归覆盖；分区约束证明与 global/local index 语义仍未完成。
- 删除 `main.cpp` 中 146 行只执行 `break` 的空路由分支；未迁移命令现在直接经过统一 bridge/legacy 边界。
- 测试入口改为缓存生产对象、逐测试独立链接运行；避免每个测试重复编译完整 DBMS，同时保留自定义源和本地 stub 测试覆盖。
- 修复 `DROP DATABASE` 未释放数据库级 page/index/TOAST/WAL/CLOG/catalog 缓存的问题；新增同名数据库重建回归测试，防止旧缓存迟写入新数据库。
- CLOG 刷盘在数据库目录已被删除时不会重建目录或把旧事务状态写入同名新数据库。
- 网络服务默认 fail-closed：证书/私钥缺失、OpenSSL 不可用或 TLS 初始化失败时拒绝启动；明文只能通过显式 `--insecure` 开启，且仅用于本地开发。
- 删除运行时自动生成自签名证书的 shell 调用，避免私钥落盘位置和命令参数不可控；部署必须显式提供 TLS 材料。
- 网络服务已切换到 PostgreSQL Frontend/Backend protocol 3.0 核心路径：支持 SSLRequest 协商、StartupMessage、catalog SCRAM-SHA-256、参数状态、简单 Query，以及 Parse/Bind/Execute/Sync 基础流程；协议回归由 `tests/postgres_protocol_test.py` 覆盖真实 SCRAM 握手。
- 简单单表视图已支持行级 `INSTEAD OF INSERT/UPDATE/DELETE` action SQL；多行 `VALUES`、按实际匹配行的 UPDATE/DELETE、`NEW`/`OLD`/`WHEN` 和 server 会话执行均有协议回归，复杂视图映射与函数/PL 触发器运行时仍未完成。
- `UNION`/`INTERSECT`/`EXCEPT` 的组合语义已统一由 Volcano `SetOperationOp` 执行，覆盖顶层优先级、错误传播及 `ALL` 重复行语义；简单单表 operand 直接构建子计划，复杂 operand 通过 `MaterializedRowsOp` 接入，完整 AST 下推和类型合并尚未完成。
- 并行执行已具备可验证的 `ParallelTableScanOp`：非分区 heap 按 page range 由多个 worker 读取并按范围顺序 Gather，`max_parallel_workers_per_gather` 可配置；事务内、分区表、并行 join/aggregate、GatherMerge 和长期 worker pool 仍未完成。
- 窗口执行已具备可复用的 `WindowOp`/`WindowAgg` 计划节点：主 SQL 入口已接入常见排名/偏移、窗口聚合、`ROWS/RANGE/GROUPS` frame/exclusion、独立分区/排序、OFFSET 和最终结果排序；复杂目标列表和主 SQL EXPLAIN 的窗口解析仍保留 legacy fallback。
- 分组执行已具备可复用的 `GroupAggregateOp` 计划节点：主 SQL 入口和 EXPLAIN 已接入常见 `GROUP BY`、`HAVING`、`ROLLUP/CUBE/GROUPING SETS` 及常见标量聚合；复杂目标、`GROUPING()`/`GROUPING_ID`、完整排序作用域和并行聚合仍待完成。
- 多个等值索引条件已由 `BitmapHeapScanOp`/`BitmapOrHeapScanOp` 执行候选 RID 的 AND/OR 组合，再统一 heap fetch 和原谓词重检；范围 bitmap、并行 bitmap 和真正 block bitmap 扫描尚未完成。
- SERIALIZABLE 事务对关系级读取登记 SIREAD 覆盖，即使谓词返回空集也会参与 rw-conflict 检测；该粒度是保守安全边界，页/索引级 predicate lock 和完整 SSI 冲突图仍未完成。
- 扩展查询已支持文本及常用类型二进制参数/结果（bool/int2/int4/int8/oid/float4/float8/text/varchar/date/time/timestamp/timestamptz/uuid/numeric；日期时间按当前引擎秒精度存储，numeric 使用 PostgreSQL base-10000 wire 格式并以精确 decimal 文本保存）、`Parse` 参数描述、`Bind` 数量/格式/NULL 校验、`Describe`/`Close` 生命周期、`$n` 字面量绑定和基础 portal `Execute maxRows` 分批返回（含 `PortalSuspended`）；常见单表列会返回 catalog/table schema 驱动的 OID、长度、属性号和表 OID，复杂表达式仍回退为 text。数组等复杂类型的二进制 I/O、完整 RowDescription 类型推导以及 holdable/scrollable cursor 等完整 portal 语义仍待实现。
- 表/列 ACL 检查已统一解析会话用户自身、递归继承角色和 `PUBLIC` 授权；真实协议回归验证了角色继承的 `SELECT` 和未授权 `INSERT` 拒绝。对象 owner、`GRANT OPTION` 完整转移/回收、schema/database/function ACL 及 RLS 与 ACL 的完整组合语义仍待补齐。
- 协议错误状态已收敛：扩展查询在 Parse/Bind/Execute 错误后进入 PostgreSQL 的 ignore-until-Sync 状态；事务外简单查询错误返回 `ReadyForQuery('I')`，连接可在 Sync/错误响应后继续使用。数组等复杂类型、完整类型映射、二进制扩展消息语义仍未完成。
- 事务上下文已从共享 `StorageEngine` 实例移为连接工作线程局部：事务 ID、快照、回滚日志、savepoint、隔离级别、延迟约束和 `lastval` 不再在协议连接之间互相覆盖；连接断开时会回滚未完成事务并丢弃 backend 上下文。全局锁管理器和提交状态仍用于跨 backend 协调。双连接协议回归已验证未提交行隔离、回滚恢复、断开回滚和提交后可见性。
- 构建质量收敛：修复 planner 的 merge join cost 参数错误，清理 parser 未使用参数和测试冗余 helper；`./scripts/build.sh` 在 `-Wall -Wextra` 下通过且无编译警告，完整回归与 OpenSSL Docker 构建均通过。

启动安全边界：非空但 magic/版本不匹配的数据文件会直接拒绝打开，不会被清零或按新格式覆盖。部署时必须将数据目录初始化为当前格式，并通过备份恢复或 SQL 导入完成升级。

数据兼容边界：旧 schema、旧 4 KiB 数据页和旧行头不会被读取或迁移。升级前必须导出 SQL，或删除并重建数据目录。

仍不能称为生产就绪的主要原因包括：SSI 目前已增加关系级 SIREAD 以覆盖空范围读，但页/索引粒度 predicate lock 和完整 rw-conflict 规则仍不完整；当前 wire protocol 仍缺完整类型/错误/扩展消息语义、channel binding 和结构化执行结果，ACL、admin option、owner/依赖和完整角色继承语义仍不完整；并行执行、流复制/PITR、完整系统目录接入、审计/可观测性和系统化故障注入测试也仍不完整。后台 writer/checkpointer 与 DDL/database lifecycle 的文件缓存并发访问已加锁并纳入回归验证。网络连接容量现在通过原子槽位预留控制并发 accept，TLS 握手失败和认证失败都会释放槽位。后续改动必须以代码路径、回归测试和故障恢复验证为准，不能只以功能清单宣称完成。

验证入口：`./scripts/build.sh`、`./scripts/run_all_tests_fast.sh`、`./scripts/build_tests.sh`；两个 E2E 已由统一测试入口自动执行。Docker 镜像构建使用 `docker build`。CMake 验证需要环境提供 `cmake` 可执行文件。

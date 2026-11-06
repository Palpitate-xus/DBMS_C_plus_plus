# Changelog

<!-- P36 physical-row-null-reuse begin -->
<a id="physical-row-null-reuse-current"></a>
## 2026-11-06：普通物理扫描／排序投影 NULL 元数据复用

扫描展开长字段时，借用该物理行已捕获的 NULL 位图；排序后的投影在核对真实来源、具体 Sort 类型和全部物理列声明后，借用排序已保存的位图。借用仅存在于一次取行调用内，退出与异常均恢复原绑定；表结构不匹配、Sort 子类及其他来源继续原路径。没有新增页／allocator 缓存或改变文件身份检查本身。

固定数据下的 720 次完整查询对照中，固定字段排序、单长字段排序、多长字段全列无排序的墙钟中位数分别下降约 27%～29%、23%～24%、10%～11%。只选 ID 的服务器 CPU 高于两次旧版约 12.37%／5.00%，墙钟居间；不能称所有查询或数据库操作都更快。

另外 7 项相关原生回归在旧版和 C1 均实际 exit0。既有 toast_test 在两版均于第 604 行 `!corruptScan.open()` 断言中止（actual -6）；保留为失败，后续断言未运行，未修改测试预期或调查／修复该故障模块。首个原生控制器因此停止，后续仅执行原计划尚未启动的 11 个运行，没有重跑已完成项，也没有宣称全套通过。

原273项及暂缓范围保持，新增1156项检查与完整990操作开销证据。[性能报告](docs/performance-report-2026-10-30.md#physical-row-null-reuse-20261106)。
<!-- P36 physical-row-null-reuse end -->

<!-- P35 view-column-dependencies begin -->
## 2026-11-06 — 视图关系／列依赖与破坏性 DDL 保护

创建／兼容替换视图时，从已绑定的真实来源、具体列及查询子树记录依赖，覆盖投影、谓词、排序、分组、窗口、子查询与 USING/NATURAL 键。删除前先检查依赖；RESTRICT 返回 2BP01，未引用列仍可删除。CASCADE 按依赖顺序删除相关视图，后续子命令失败时由已有 DDL 快照恢复文件和目录记录。列序号压缩后，剩余引用沿用已有重映射。

合法编译目录读来源由完整查询绑定校验，别名保留实际列身份；旧视图依赖只读恢复。新增原生 242 检查、7 既有专项、真实 PG／旧库／保存点对照通过有限范围验证；[报告](docs/performance-report-2026-10-30.md#view-column-dependencies-20261106)保留 270+27 操作全部开销及退化。本批没有整体性能提升结论，PG 家族仍 partial、Goal active。
<!-- P35 view-column-dependencies end -->

<!-- P34 projected-toast-demand begin -->
## 2026-11-06 — 普通部分列投影按需求读取 TOAST

- 仅为直接物理游标的部分列投影解码所需字段；维持选中长字段错误、NULL 与虚拟列完整输入。全列使用原路径。
- 新增 14 组/83 项回归，并复核 5 项相关原生测试及全部普通协议回复。
- 完整保留 C1 回退及 C2 的各 270 项正常性能对照、漂移和长尾；更新[性能报告](docs/performance-report-2026-10-30.md#projected-toast-demand-20261106)，证据仅以压缩归档提交。
<!-- P34 projected-toast-demand end -->

<!-- P33 pg-class-declaration begin -->
## 2026-11-06 — owned pg_class declaration (partial)

- Persist the actual `pg_class` identity, all34 attributes, rowtype83/array273 and required metadata-only types through the existing locked startup owner. Validate loaded declarations/dependencies and reject conflicting bootstrap input before edits.
- Use the PostgreSQL18.6 `aclitem` declaration (16 bytes, alignment `d`); keep unavailable value/result codecs explicit, including NULL input `0A000`.
- Add the550-check native regression, full unfiltered41-query PG comparison, real old-image upgrade/cold evidence and one complete270-timing control cohort. Performance regressions, virtual origins and remaining catalog/value functionality stay open. [Report](docs/performance-report-2026-10-30.md#pg-class-declaration-20261106).
<!-- P33 pg-class-declaration end -->

本项目的所有显著变更记录于此文件。
格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

<!-- CURRENT_LEGACY_LOG_APPEND_20261106 -->

- 普通日志以每次O_APPEND/writev追加，保留原字节、权限、轮转及打开错误行为；新增13组原生回归。正常30案例服务器CPU为16低/6高/8居间，多LOB回退及原9项协议差异保持，未证明一致提速，默认程序未替换。见[完整报告](docs/performance-report-2026-10-30.md#legacy-log-append-20261106)。

- 补充真实主键查询CPU诊断：同一三表闭库状态、三个正常进程、1,536次完整目标查询及1,575响应独立复核；原始PC/符号与全wire压缩归档，保留P30回退和目录差异。明确pg_class完整声明的类型准入依赖，本批没有生产修复或提速验收。
- 整理文档入口：补齐 754 份 Markdown 的分类清单，近期重复状态段落集中到审查索引；增加本地文件链接、空文档、整份重复与清单覆盖检查工具，明确当前验证与历史记录的边界，并同步手册中的排序取消检查范围。
- 修复检查点截断后首个保留 WAL 段从记录中间开始时无法重启的问题：利用现有已持久化检查点和经过 CRC/链衔接检查的前驱记录确定恢复起点，继续校验保留的完整记录；缺少有效锚点的非对齐截断在删除前拒绝。增加跨段重开及损坏锚点/记录回归。
- 优化数据库写入和维护路径：CLOG 原子发布跳过全零块并合并连续数据块，使用文件身份和时间戳校验缓存；DML、约束校验和 DDL 重写复用已有表结构读取 NULL 位图；在实际持有完整 DDL 回滚快照时消除重写行的重复整库保存点；绑定 UPDATE/DELETE 的同类型整数等值条件先过滤候选，再执行原类型表达式。协调后台刷盘、自动 VACUUM、CHECKPOINT 与 DROP DATABASE，重叠的同一引擎 CHECKPOINT 复用实际结果，保留活动用户事务拒绝及持久化错误传播。增加[性能测试报告](docs/performance-report-2026-10-30.md)与可复现基准脚本。
- 优化查询热路径：目录文件未变化时跳过全目录序列化，扫描复用已有表结构读取 NULL 位图；同类型 SMALLINT/INTEGER/BIGINT 单列主键参数等值查询可使用物理索引，旧快照、自身写入及重叠事务继续使用堆扫描并提前过滤整数键；整数等值 JOIN 在构造组合行上下文前跳过不匹配键，保留原表达式复核、读取顺序及事务可见性检查。
- 增加全仓库代码审查、可复现 SQL/协议问题及逐项修复方案；文档索引区分当前结论与历史证据，修正文档中的旧测试数量、复制示例和失效链接。审查记录本身不代表产品问题已修复。
- 已合入引号类型名称归一化修复，以及递归 SQL 执行期间 Session 指针的异常安全恢复；具体范围和验证见对应 issue 文档。
- 修复 NUMERIC 声明精度/scale 在存储和赋值中丢失的问题，覆盖标量及数组元素舍入、溢出检查、ALTER 重写和协议类型修饰符；支持负 scale 与大于 precision 的 scale。裸 NUMERIC 与旧无修饰符声明仍保持原语义。
- 带 NUMERIC 修饰符的声明也使用 schema 格式 `0x4442000C`；仅支持 INTERVAL 修饰符的早期 C 格式读取器不能读取这类 NUMERIC 声明。请保留升级前备份，不要直接降级打开已写入新声明的数据目录。
- 修复 INTERVAL 列字段范围和小数秒精度在 INSERT、UPDATE、数组元素及 ALTER TYPE 中丢失的问题，并为实际表列返回对应的协议类型修饰符。
- 带 INTERVAL 修饰符的表声明使用 schema 格式 `0x4442000C` 保存精度；未带修饰符的旧声明仍可读取且保持原格式。写入新格式的声明不能由旧版程序读取，请保留升级前备份，不要直接降级打开这类数据目录。
- PostgreSQL 18 兼容审计仍在逐项执行；完成状态只以 `docs/postgresql-18-gap-audit.md` 与 `docs/gap-progress.json` 为准。
- GitHub Actions workflow 按维护要求保持 `.disabled`；`scripts/build_tests.sh` 是本地完整验证入口。

## [0.2.0] - 2026-08-22

并发硬化与写放大根除（"concurrency hardening"）。
Tag 内容：156 个 `*_test.cpp` 测试源 + 7 个 Python E2E；发布时记录崩溃恢复矩阵 12/12、ASAN/TSAN 核心集 CLEAN。

### 并发正确性
- TypeRegistry 单例 bootstrap 竞态：`static bool` 旗标不受 Meyers 单例保护，两线程并发 bootstrap 重写注册表——TSAN 1288 条报告的单一根因，`call_once` 修复后 0 报告
- stats-map 擦除竞态：closeDatabaseCaches 无锁擦 deadTupleCounts_/modifyCounts_ 与连接线程读写竞争，补对应 mutex
- 事务级堆页 flush：commit 只刷本事务写过的页（仍在事务页锁下，无竞态无死锁）；曾试 bgwriter 逐页加锁（锁序死锁,array_test 复现）已否决
- LockManager 锁序反转定性为设计权衡（调用协议文档化）

### 性能
- TCP_NODELAY：服务器 socket Nagle+delayed-ACK 造成每语句 41ms 固定地板（含空查询），修复后 SELECT 1 0ms / INSERT 19ms
- WAL group commit：fsync 队列与插入锁解耦，N 个串行 fsync 合一
- 索引 WAL 图像按 checkpoint 纪元去重：每 commit 整索引文件×2 写 WAL（实测 40 字节 INSERT 写 120KB WAL）→ 同纪元只写一次；WAL 量降两个数量级（70MB/30s → 120KB/60s）
- auto-vacuum 移出语句路径：阈值(50 dead tuples)触发的同步全表 vacuum 曾把全部并发语句串到 30s+ 饿死；改为后台循环执行
- commit 去掉全池堆页 flush（jbd2 world-stop 主放大器，207MB/90s）

### 运维定性
- 残余 ~30s 周期停顿取证结案：worker 单次 fsync 在 ext4 jbd2 等待 25s,由宿主机邻居进程周期性大写入（iostat 225 w/s burst）拖慢 jbd2 提交所致——部署要求独占 WAL 卷,非引擎缺陷
- kill-storm 验证方法论：必须带 commit 确认屏障（无屏障会把 kill-before-commit 误判为丢行）

## [0.1.0] - 2026-08-21

首个公开发布版本（v0.1.0 "first public cut"）。
Tag 内容：156 个 `*_test.cpp` 测试源 + 7 个 Python E2E；发布时记录全绿。

### 核心引擎
- 堆表存储：8 KiB PostgreSQL 风格页布局（页头 + line pointer + tuple 从页尾生长），Fletcher-16 页校验，Free Space Map
- BufferPool：Clock-Sweep 驱逐、pin/usage 分离、并发加载单加载者规则、孤儿帧语义；堆池 256 帧/索引池 128 帧可配
- WAL：段式 WAL、CRC 校验、记录链验证、增量尾部状态、崩溃恢复页像重放（redo/undo）、XID 事务提交日志
- MVCC：快照隔离、事务级读写集合、页级 SIREAD、单列 B+Tree 逻辑索引谓词（SERIALIZABLE 部分实现，见已知限制）
- TOAST：线外大字段存储，zlib 压缩，chunk header 保留压缩标记与原始长度
- 事务：BEGIN/COMMIT/ROLLBACK、SAVEPOINT、语句级隐式事务、DDL 快照回滚、两阶段边界
- 锁：行级/表级/意图锁、死锁检测、锁超时、简化 gap 锁

### SQL 兼容面
- DDL：CREATE/DROP/ALTER TABLE（含分区、继承、表空间）、VIEW、SEQUENCE、DOMAIN、TYPE、FUNCTION（PL/pgSQL）、TRIGGER、INDEX（B+Tree/Hash/GIN/BRIN/SP-GiST/Bloom）、POLICY（RLS）、PUBLICATION、REPLICATION SLOT、STATISTICS、CAST、CONVERSION、AGGREGATE、EXTENSION 兼容对象
- DML：INSERT（VALUES/SELECT/DEFAULT/ON CONFLICT）、UPDATE（FROM/RETURNING）、DELETE（USING/RETURNING）、MERGE 窄版、RETURNING（受限表达式）
- 查询：SELECT 全语法（WHERE/GROUP BY/HAVING/ORDER BY/LIMIT）、JOIN（INNER/LEFT/RIGHT/FULL/CROSS/SEMI/ANTI）、子查询（IN/EXISTS/ANY/ALL/标量）、窗口函数（排名/偏移/聚合 + ROWS/RANGE/GROUPS frame）、UNION/INTERSECT/EXCEPT、ROLLUP/CUBE/GROUPING SETS、CTE、LATERAL
- 执行器：Volcano 风格结构化算子（SeqScan/IndexScan/BitmapHeapScan/HashJoin/MergeJoin/NestLoop/GroupAggregate/Window/SetOp/SemiJoin 等），复杂路径回退 legacy
- PL/pgSQL 最小运行时：变量、控制流、EXCEPTION 之外的游标
- 协议：PostgreSQL wire protocol v3（SCRAM-SHA-256 认证、Parse/Bind/Execute、扩展查询）

### 近期完成的特性（2026-08 批次）
- P1-7 PL/pgSQL 运行时（最小实现）
- P1-8 并行 Vacuum（leader/workers 分索引回收）
- P1-9 自定义代价函数（CostModel 钩子 + GUC 同步）
- P2-2 Bloom 索引访问方法
- P2-4 PgBouncer 式连接池（session/transaction/statement 三模式 + 3 GUC + SHOW POOLS）
- P2-5 逻辑解码（pgoutput 帧式 + test_decoding 文本双输出插件、PublicationCatalog、按槽变更流、CREATE/DROP PUBLICATION|REPLICATION SLOT + 4 个 SHOW 面）
- P2-8 TDE 透明数据加密（SHA-256-CTR + EtM 页级加密、边车信封 `<file>.tde`、keyring 0600、WAL 崩溃恢复兼容）

### 发布工程（本批次新增）
- 版本号单一事实源：CMakeLists project version ↔ src/common/version.h ↔ CHANGELOG.md（package.sh 打包前校验一致）
- `dbms_main --version/-V`
- CI：scripts/ci.sh（全量构建 + 回归 + E2E）+ GitHub Actions workflow
- Sanitizer 例程：scripts/sanitizer.sh（ASAN/TSAN 编译跑核心并发/存储测试）
- 崩溃恢复矩阵：tests/crash_matrix_test.sh（kill -9 × {WAL, TDE, 连接池} × commit 前/后）
- Soak 负载：scripts/soak.sh（可配置并发/时长的多客户端压测）
- 打包：scripts/package.sh（源码 tarball，含目录约定）

### 已知限制（v0.1 明示）
1. **DML 双执行入口**（P1-0）：复杂 INSERT SELECT、部分/索引推断 conflict target、引用子查询或其他关系的 ON CONFLICT DO UPDATE、复杂/子查询/窗口 RETURNING、外连接/复杂 UPDATE FROM/DELETE USING、多 WHEN MERGE、视图写入仍回退 legacy 字符串路径；错误码与行为一致性受限
2. **SERIALIZABLE 非 PostgreSQL 完整 SSI**：复合/表达式/部分索引及其他访问方法的精确 phantom 推理不完整；空结果关系级兜底牺牲部分串行化并发度
3. **TDE 覆盖范围**：未经 BufferPool 页路径的索引文件（.idx/.hidx/.fti 等）未加密；无密钥轮换、无 per-database 密钥隔离
4. **逻辑解码**：无 CREATE SUBSCRIPTION 拉取端；变更不经 WAL 重放（写路径直采），崩溃恢复后逻辑槽不 replay
5. **代价模型**（P1-12）：直方图/MCV 已采集但 planner 消费不全，范围选择率硬编码 0.3，EXPLAIN 行数估计有误导性
6. **并行执行**：仅并行表扫描（page range 分片 Gather）；并行 join/aggregate、GatherMerge 未实现
7. **prepared statements**（P1-10）：SQL 级语法为自有 `PREPARE name FROM 'sql'` + `?` + `USING`，非 PG 的 `(types) AS` + `$n`；无类型推断与计划复用
8. **统计收集器**：无独立后台采样线程；planner 深度反馈缺失

## [0.1.0 之前]
开发期 874+ commits 不在此列，演进细节见 docs/production-status.md 与 docs/all-gaps-todo.md。

[Unreleased]: https://github.com/Palpitate-xus/DBMS_C_plus_plus/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/Palpitate-xus/DBMS_C_plus_plus/releases/tag/v0.2.0
[0.1.0]: https://github.com/Palpitate-xus/DBMS_C_plus_plus/releases/tag/v0.1.0

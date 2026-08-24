# 生产化状态

最后更新：2026-08-22（v0.2 并发硬化批次）

当前版本处于生产化重构阶段，不能宣称已经达到 PostgreSQL 的生产级完整度。当前可验证基线为：主程序构建成功，**165 个 C++ 回归测试 + 7 个 E2E**（协议、窗口函数、EXPLAIN ANALYZE、多表 JOIN、timestamptz、inherit-only、unnest）共 `PASS=165 FAIL=0`；ASAN/TSAN sanitizer 核心集 12/12 CLEAN；崩溃恢复矩阵 12/12（见下）；发布工程管线（CI、sanitizer、soak、打包）就绪。

## v0.2 并发硬化批次（2026-08-22）

soak/TSAN 实测驱动的四个提交，每步全量回归绿色：

- **TypeRegistry 单例竞态修复**（df040ee）：`static bool bootstrapped` 旗标不受 Meyers 单例的线程安全保护，两连接线程首次类型查找并发跑 bootstrap() 重写注册表 map——TSAN 实测 **1288 条数据竞争报告几乎全部是它的连锁**（507 TypeEntry 赋值、139+138 string 读、154 红黑树节点、132 compare…）。`std::call_once` 修复后同样负载 **0 报告**。
- **WAL group commit**（c932912）：XLogFlush 先在专用 flushMutex_ 排队再进 dirMutex_——fsync 期间等待者不占插入锁，fsync 落盘后等待者的目标几乎总被已同步前缀覆盖（纯内存检查返回）。N 个串行 fsync 合并为 1 个。
- **commit 去掉堆页同步刷**（f4e8728）：world-stop 根因链取证完成——停顿窗口内冻结线程 `state=D wchan=jbd2_log_wait_commit`，放大器是 commitTransaction 每事务刷全部脏堆页（90s 压测 207MB 写入把 ext4 日志打满，fsync 排队 30s+ 且持插入锁）。堆页持久性本就由 WAL before/after 图像对 + 200ms bgwriter 承担。**验证方法论修正**：kill-storm 必须带 commit 确认屏障（无屏障版会把"kill 在 commit 前"误判为丢行——12/15 假阳性来源）。修复后带屏障 kill-storm 10/10、矩阵 12/12、soak 吞吐 43→613 ops/60s。
- **stats-map 擦除竞态修复**（d2a9e46）：closeDatabaseCaches 无锁擦 deadTupleCounts_/modifyCounts_，与连接线程在各自 mutex 下的读写竞争（TypeRegistry 修复后的时序变化暴露了它）。擦除循环补上对应 mutex。
- **LockManager 锁序报告定性为设计权衡**（dd5e347）：M1(物理令牌)跨调用持有→M0(注册表) 与 acquireLock 内 M0→M1 构成 TSAN 环；真实 AB-BA 死锁由 wait-graph 环检测 + 1s 超时兜底。修复需重写锁表协议，记录调用协议约定。
- **残留（v0.2-next）**：索引文件 WAL 图像是"每次 flush 整文件写 WAL ×2"——剩余写放大源与偶发停顿来源；需重构为索引逻辑 WAL + 恢复期重建。
- **已关闭——PgPage 同页插入竞态**：TypeRegistry 修复后的 live TSAN server 压力复测（4 协议客户端 × 50s）**0 数据竞争、0 PgPage 报告**——此前的 PgPage::insert/setLpOff 报告确认为坏 map 时代的连锁症状，不是独立的页锁缺失。该项关闭。

## v0.2-next 批次（2026-08-22 第二轮）

写放大根除批次——三项提交，每步全量回归绿色：

- **PgPage 同页竞态关闭**（89ef2ef）：live TSAN server 压力复测 0 报告，确认为坏 map 时代连锁症状。
- **索引 WAL 图像按 checkpoint 纪元去重**（7e59b1e）：实测每条 40 字节 INSERT 写 ~120KB WAL（70MB/574 次插入）——每次 commit 把**整个索引文件**写进 WAL 两次 × 每个脏索引。纪元化后同量负载 WAL 降两个数量级（60s soak 492KB→120KB）。索引 kill -9 一致性经屏障 kill-storm 验证依赖物理 flush 而非图像（10/10）。
- **事务级堆页 flush**（92167cd）：堆页全池 flush 移除后的并发正确性闭环。bgwriter 撕页竞态（TSAN 捕获 insert vs pwrite）与 bgwriter 页锁死锁（array_test 复现锁序反转）两条歧路都被否决；正解是 commit 只 flush 本事务写的页（txnWrittenPages，SSI 已有记录）——这些页仍在事务页锁下,写入无竞态。新增 BufferPool::flushPage。

**批次验证**：回归 163/0、崩溃矩阵 12/12、kill-storm 双项 10/10、ASAN/TSAN 核心集 CLEAN、soak PASS-WITH-NOTES（120KB WAL/60s，批前 ~70MB/30s）。

## v0.2 收尾批次（2026-08-22 第三轮）

world-stop 残余核查与根因终局，三项提交：

- **TCP_NODELAY**（2ca3faf）：服务器 accept 的 socket 从未关 Nagle——协议响应分多个小 send（行数据、命令状态、ReadyForQuery），第二个段被 Nagle 扣住等客户端 delayed-ACK（~40ms），形成**每条语句 41ms 的固定地板**（空查询也是 41ms 实锤）。修复后单客户端 SELECT 1 41→0ms、INSERT 58→19ms、UPDATE 75→39ms。并发下这个地板×队列深度就是此前"多秒级停顿"的主要构成。
- **auto-vacuum 移出语句路径**：DELETE/UPDATE 路径在 dead-tuple 计数过阈值（50）时**同步跑全表 vacuum 并持表锁**——4 个 soak worker 在 t≈8s 同时过阈值，全部语句被一次 vacuum 串到后面（连续 30s+ 饿死、client 30s 读超时雪崩重连）。现改为语句路径只登记 (db,table)，200ms 后台循环执行 vacuum。soak 从 crash FAIL 变 PASS。
- **残余 30s 周期停顿定性为宿主环境（结案）**：修正探针 pid 错误（setsid fork 导致 $! 失效,此前 wchan 追踪一直看着错误进程）后取到真栈：停顿窗口内 worker 线程**一次 fsync 在 ext4 jbd2_log_wait_commit 上等待 25 秒**。裸 fsync canary（覆写旧块）仅 6ms——WAL 追加新块必须等 jbd2 元数据事务提交,而宿主机上 3 个 100% CPU 的 RL 训练进程每 ~30s 周期性写大 checkpoint（iostat: sde burst 225 w/s、w_await 312ms、%util 63%）,jbd2 提交被拖到 25s。**触发条件**：共享存储宿主机上邻居进程的周期性大写入 + ext4 data=ordered 的 fsync 语义。引擎侧缓解已尽（group commit、纪元化索引图像、事务级堆页 flush、异步 auto-vacuum、TCP_NODELAY）；根治需独占存储或 XFS/ext4 nojournal WAL 卷——已记录为部署要求而非代码缺陷。

**批次验证**：soak 150s×4 PASS-WITH-NOTES（errors 从连续饿死降为宿主噪声级 3-4 次/worker,ops 恢复预算上限）。

## v0.1.0 发布批次（2026-08-20 ~ 08-21）

功能补全（每项独立提交，全量回归逐步 163 → 165 保持绿色）：

- **P1-9 自定义代价函数**：`QueryPlanner::CostModel` 可注入钩子 + GUC 同步（`seq_page_cost`/`random_page_cost`/`cpu_*_cost`），`custom_cost_hook` 式注册路径。
- **P2-2 Bloom 索引访问方法**：签名位图索引、CREATE INDEX ... USING bloom、等值查询走 bloom 预过滤。
- **P2-4 PgBouncer 式连接池**：session/transaction/statement 三模式 BackendContext 池化、3 个 GUC（`pool_mode`/`pool_size`/`max_client_conn`）、`SHOW POOLS`。修复权限泄漏：客户端会话认证状态权威，后端槽位只跟踪租借。
- **P2-5 逻辑解码**：`pgoutput`（帧式二进制）+ `test_decoding`（文本）双输出插件、`PublicationCatalog`（FOR TABLE/ALL TABLES + 按操作发布过滤）、按槽变更流（peek/acknowledge/kMaxRetained 限界）、COMMIT 流向逻辑槽/ROLLBACK 丢弃、`CREATE/DROP PUBLICATION|REPLICATION SLOT` + 4 个 SHOW 面（解析器前置拦截——否则 CREATE PUBLICATION 被通用 parser 误映射为 CREATE TABLE）。
- **P2-8 TDE 透明数据加密**：SHA-256-CTR 密流 + SHA-256 EtM 认证（组合语义等价 AES-GCM，零外部依赖）。**边车信封**模型：整页密文 + `<file>.tde` 每页 48B nonce+MAC（堆页 line-pointer 向下生长/tuple 向上生长，页内无处安放信封）；全零信封 = 明文页（就地渐进加密）；页 0 永不加密。keyring 0600 首建/坏格式拒绝。**关键修复：TDE 装载从 main 深处提前到 pre-engine 静态初始化**——否则崩溃恢复读到裸密文中止。E2E：秘密数据 at-rest 全文件 grep 不到、重启后 WAL 恢复 + 解密读回全部行。

质量与并发（sanitizer 例程直接产出的修复）：

- **WAL LSN 数据竞争修复**：`WALManager::currentLsn_` 原在 `dirMutex_` 下写、经 `currentWriteLsn()` 无锁读（flush 决策 + 后台 flusher）——并发 insert 与后台 flusher 竞争。改为 `std::atomic<Lsn>` relaxed 序（进度提示语义；记录插入仍由 `dirMutex_` 串行化）。TSAN 12/12 CLEAN 验证。
- **时间格式化线程安全修复（soak 发现）**：`ctime()`/`localtime()` 返回 libc 静态缓冲；协议服务器是 thread-per-connection，每条连接格式化时间戳（slow-query/audit/auto-explain 日志、引擎 getTime、now() 求值器、LockManager nowIso8601）都在竞争同一静态缓冲 → native 构建偶发 SIGSEGV（soak 捕捉到的不稳定崩溃）。全部调用点改用可重入变体（`ctime_r`/`localtime_r`）。验证：TSAN 服务器竞态 8→3；native 6 客户端×70s 压测连续 3 轮服务器存活（此前崩溃复现）。
- **已知（未修）——PgPage 同页插入竞态**：TSAN 在真实并发负载下报告 `PgPage::insert`/`setLpOff`/`writeChecksum` 数据竞争——两个线程无页锁并发插入**同一页**。属于引擎页级锁不变量缺失（调用方应持页锁），v0.2 引擎项；当前表现为并发 INSERT 吞吐受限 + 偶发语句级错误，不影响已提交数据正确性（WAL + 崩溃矩阵 12/12 仍绿）。
- **已知（设计权衡，非缺陷）——LockManager 锁序报告**：TSAN lock-order-inversion（复现于 lock_manager_concurrency_test 的双表死锁场景）：物理锁令牌 `LockState::mtx`（M1）跨 API 调用持有期间再次进入 acquireLock 会先取注册表锁 `globalMutex_`（M0），而 acquireLock 内部正常序为 M0→M1。真实死锁需两线程以相反序交叉持有——该场景正是测试复现的双表 AB-BA，**由 wait-graph 环检测 + deadlockTimeoutMs=1000 兜底解除**（测试断言二者至少一个失败返回）。修复需要把锁表重构为"注册表短锁 + 等待队列条件变量"协议（PostgreSQL LOCKMODE 结构），影响全部 lock/pageLock/rowLock 路径——收益是消除 TSAN 报告，代价是重写核心并发原语。定性为有意权衡：sanitizer 例程对该类报告保持 detect_deadlocks=0（数据竞争仍致命），文档记录锁序协议如下——任何持有 state->mtx 的路径不得再调用锁管理器公开 API。
- **world-stop 停顿——根因链已完整定位（v0.2 修复进行中）**：并发负载下所有连接统一冻结。取证（停顿窗口内 /proc 线程态采样 + 每秒语句吞吐直方图）：
  1. 卡死线程 `state=D wchan=jbd2_log_wait_commit`——ext4 日志提交队列堵塞；
  2. 放大器 = commitTransaction 每次事务执行 `flushDatabaseCaches`（全部堆页 pwrite + 每文件 fsync）+ WAL XLogFlush fsync——90s 压测写 207MB，jbd2 过载后每个 fsync 排队 30s+，持有 dirMutex_ 期间全部插入停摆；
  3. 客户端 30s 超时是表象周期，不是引擎周期。
  已落地缓解：
  1. WAL group commit（flushMutex_ 排队合并，等待者搭已落盘前缀的便车，消除 N 个 fsync 串行）；
  2. commit 路径不再同步刷堆页/TOAST 页（flushDatabaseCaches(heapPages=false)）——堆页持久性由 WAL before/after 图像对 + 200ms bgwriter 承担，即 PostgreSQL 模型。崩溃安全验证：带 commit 确认屏障的 kill-storm 10/10 行存活、崩溃矩阵 12/12。
  待做（设计级）：索引文件的 WAL 图像是“每次 flush 整文件写 WAL ×2”的保守机制，是剩余写放大源；需要改为索引逻辑 WAL 记录 + 恢复期重建。soak 以错误预算区分 pass-with-notes 与硬失败。

发布工程（本批次核心产出）：

- **版本单一事实源**：CMake project version ↔ `src/common/version.h` ↔ CHANGELOG.md；`dbms_main --version/-V`；git tag v0.1.0。
- **CI**：`scripts/ci.sh`（build → 165 回归 + 7 E2E → 版本一致性 → sanitizer 核心）+ `.github/workflows/ci.yml`（push/PR/tag 触发）。
- **Sanitizer 例程**：`scripts/sanitizer.sh`（out-of-tree ASAN+UBSAN / TSAN 构建，核心 12 测试；`setarch -R` 规避内核 6.8 高熵 ASLR 与 TSAN 的不兼容；LSAN suppressions 记录有意泄漏的单例）。
- **崩溃恢复矩阵**：`tests/crash_matrix_test.sh` —— {wal-insert, tde-insert, ddl-mixed, connection-pool} × {mid-transaction, post-commit, post-checkpoint} = 12 组合，SIGKILL 后继进程验证已提交行存活/未提交行消失。post-commit 用输出确认屏障消除 kill 与 flush 的竞态 flake。
- **Soak 负载**：`scripts/soak.sh` + `scripts/soak_client.py` —— 单 `--server` 进程（thread-per-connection，真实并发架构）+ N 个 PG wire protocol 客户端（SCRAM 认证 + 混合 DML 循环），验证 server 存活、行数一致性、CHECKPOINT 后干净重开。**架构发现：process-per-connection 共享数据目录不是安全并发形态**（每进程 XID 计数器在共享 WAL 中冲突 → 恢复时 contradictory COMMIT）；多用户必须走单 --server 进程。文档化于脚本注释。soak 的直接产出：两个真修复（时间格式化线程安全、WAL LSN 原子化伴随项）与三项 v0.2 已知发现（PgPage 竞态、锁序反转、world-stop）。
- **打包**：`scripts/package.sh` 源码 tarball（版本三方一致性校验）+ 目录约定（docs/PACKAGING.md）。

遗留（v0.2 候选，详见 RELEASE-NOTES.md 已知限制）：DML 双执行入口收尾（P1-0 第 4 步）、LockManager 锁序、TDE 索引文件覆盖 + 密钥轮换、逻辑解码 SUBSCRIPTION 拉取端与 WAL replay、代价模型统计消费（P1-12）、prepared statements PG 语法对齐（P1-10）。

---

2026-08-14 性能与并发硬化轮次（13 个提交，每步全量回归保持绿色）：

- **WAL 追加路径重构**：`WALManager` 维护增量尾部状态（上一记录 LSN/长度、已刷盘 LSN、最早可用 segment、常开追加 fd、可复用文件锁 fd），追加不再每条记录重扫 segment 目录；同目录多 WALManager 通过 leaked-singleton 注册表互斥，避免静态析构期锁销毁。修复了空 WAL 上第二个 manager 缓存 `currentLsn_=0` 覆盖首个 manager 数据的多实例缺陷（LSN 0 是合法记录位置，采用独立 `hasLastRecord_` 标志判空）。
- **同事务重复页 before-image 去重**：恢复的 undo 遍历按最新到最早应用未提交 before-image，同事务重复记录同一页只膨胀 WAL；现在首个镜像后跳过（xid 0 的维护性成对记录仍保留精确配对）。
- **元数据/序列/schema 内存缓存**：`.secidx`、`.hashidx`、排除约束、表 schema（mtime 新鲜度校验，支持同进程多实例）与自增序列计数器全部内存缓存；自增计数器每事务一次刷盘（序列保持非事务语义，与 PostgreSQL 一致）。全部 DDL 写路径经 `writeSchemaFile()`/显式失效收敛，修复了 RLS 开关、fillfactor、表空间迁移、分区挂载/卸载等处原有的缓存缺失隐患（由 policy/fillfactor/catalog_snapshot 测试捕获）。schema 缓存命中仍填充事务 catalog 快照，快照隔离跨引擎实例保持正确。
- **缓冲池扩容与页校验降频**：堆池默认 256 帧、索引池 128 帧（`DBMS_BUFFER_FRAMES`/`DBMS_INDEX_BUFFER_FRAMES` 可覆盖，16..4096）；Fletcher-16 页校验从每次 fetch 降为仅磁盘加载时执行（经 BufferPool on-load validator，短读未写页跳过）。
- **B+ 树查找与节点缓存**：全部节点下降路径由线性扫描（每层最多 order=100 次比较）改为 lower/upper-bound 二分查找（约 7 次），并修复 removeMulti 等值键内需继续扫描匹配 RID 的语义；64 项不可变节点 LRU 缓存使多数树层级跳过整页反序列化。
- **BufferPool 并发加载与失效健全性**：磁盘 I/O 移出池锁——锁内预留并 pin 帧、放锁 pread 到私有 scratch、重新加锁校验发布；同页并发加载遵循单加载者规则；`invalidatePage`/`invalidateAll`/`close` 排干在途加载；发布时检测并淘汰重复加载。`invalidatePage` 在并发读者下改为孤儿帧语义（互斥不变量：任一页在 `pageMap_`/`orphanedPins_`/`loadingPages_` 中至多占一），修复持 pin 读者观察到帧被回收复用后读到他页数据的损坏缺陷。该缺陷由新增 TSAN/ASAN 多线程压测（读者+改写者+失效器+驱逐混合）发现：修复前约 1 万次内容损坏，修复后 0 竞态 0 损坏。注意 `invalidatePage`/`invalidateAll` 当前无生产调用方，属对潜在陷阱的加固。
- **内部锁补全**：FSM、VisibilityMap、PageAllocator（头页读改写串行）、BPTree（递归写锁）、HashIndex、`getCommitLog` 的共享 map 全部加锁；INSERT 降级为表意图锁（IX），与 UPDATE 对齐。
- **TableManage.h 瘦身与 AM 注册表收敛**：9 个存储/索引类改前置声明（仅作 unique_ptr 成员），去掉 `<iostream>`/`<fstream>`，103 个依赖 TU 不再重解析；索引物理删除的三处重复访问方法 if-else 链收敛为 `dropIndexByAccessMethod()` 单点分发（顺带修复 undo 路径 spgist 处理不一致）。
- **实测性能变化**：事务内带 PK 插入 11.7 → ~2000+ 行/秒，commit 267ms → ~13ms；并发套件 500 行插入 + GROUP BY 从 ~106s → ~0.3s（约 350×）；WAL 每 200 插入字节数 3.4MB → 1.7MB。验证：全量回归（137 C++ + 2 E2E）全部通过、TSAN/ASAN 压测 0 报告、WAL 截断/崩溃恢复/预备事务专项独立复跑通过。

2026-08-14 序列持久化与边界安全收紧：用户序列状态写入统一使用临时文件、文件/目录 `fsync` 和原子 rename；读取严格拒绝尾随字段、非法 cycle 标志、零增量、无效范围和非法 cache。`nextval` 的 cache 批量计算、`currval`、`setval`、创建/修改序列均使用 checked int64 算术，`INT64` 上下界和耗尽序列不会触发未定义行为；重启后状态保持，损坏元数据 fail-closed。旧序列文件不作为兼容格式接受。

2026-08-14 DDL 生命周期安全修复：DDL 回滚现在会清理自身创建的物理快照，避免失败的 `ALTER SEQUENCE` 在下次启动被误恢复为旧数据库状态；typed `DdlExecutor` 以作用域守卫恢复线程局部 `Session*`，避免会话销毁后嵌入式 `nextval` 解引用悬空指针。重启、损坏序列文件、整数边界和失败 DDL 快照均有回归覆盖。

2026-08-14 数据库生命周期持久化收紧：数据库初始化文件、checkpoint 元数据和物理备份标记统一使用临时文件、文件/目录 `fsync` 与原子 rename；创建数据库的目录/文件失败会清理半成品并返回 `IO_ERROR`，checkpoint 和备份标记不会留下可被恢复流程误认的截断文件。生命周期回归增加了初始化文件完整性和 checkpoint 尾随数据校验；planner 统计回归的数据库 teardown 改用带缓存锁的 `dropDatabase()`，避免后台 writer 与裸目录删除竞争。

2026-08-14 DDL 错误传播收紧：`CREATE DATABASE`/`DROP DATABASE` 现在只在 `StorageEngine` 返回 `OK` 时报告成功；`IO_ERROR`、未知状态和数据库清理失败均返回错误并映射 SQLSTATE，新增异常数据库路径回归，避免持久化失败被伪报为成功。

2026-08-14 DDL 路径边界收紧：数据库、schema、table、view、sequence 和 domain 的文件组件拒绝路径分隔符、`.`/`..` 及空名称；schema marker 和 domain sidecar 创建采用原子发布，重复 schema 只有 `IF NOT EXISTS` 才成功，非法路径不会被当作关系文件访问。

2026-08-14 辅助对象 sidecar 收敛：view、function、table-valued function、procedure 的定义文件统一使用安全对象名、原子发布和明确的 `IO_ERROR`；父路径被普通文件占用时 DDL fail-closed，重复定义不再覆盖旧文件。完整函数/过程回归保持通过。

2026-08-14 CASCADE 物理一致性收紧：`DROP TABLE ... CASCADE` 先根据只读依赖计划解析从属表、序列和各类索引的物理删除动作，无法解析索引物理元数据时 fail-closed；物理动作完成后才发布 catalog 删除计划，动作失败由 DDL 快照恢复，避免 catalog 已删而关系文件残留。

2026-08-14 索引 sidecar 持久化收紧：`.secidx` 与 `.idxnames` 统一使用 regular-file 校验、原子发布和错误传播；CREATE INDEX 元数据发布失败会清理已生成的物理索引，DROP INDEX 元数据失败不会伪报成功，复合索引删除保留 INCLUDE/WHERE 等原始选项。

2026-08-14 索引架构清理：审计确认 `IIndexAM`、BPTree/Hash 适配器以及独立 GIN/BRIN 类没有生产调用方，均从 manifest 和源码删除；`StorageEngine` 作为唯一实际索引入口，索引回归改为覆盖 canonical GIN/BRIN 路径及损坏 sidecar fail-closed。未接入的伪适配层不再增加构建和维护负担。

2026-08-14 CREATE INDEX 事务边界收紧：物理索引 sidecar、SQL 名称映射和 `pg_class/pg_depend` 注册现在由当前格式 DDL 快照共同保护；外层事务回滚会清理 B-tree、复合、Hash、GIN、BRIN 等真实访问路径，catalog 注册异常 fail-closed，不再只记录 warning 后伪报成功。重复名称支持 `IF NOT EXISTS` 跳过，普通重复创建拒绝。

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
- StorageEngine 的 Hash/GIN/BRIN 索引文件现在使用严格格式校验；写入采用临时文件 + fsync + 原子 rename，写入失败保留 dirty 状态，截断、非法版本和尾随垃圾会拒绝打开。BRIN 当前格式按本项目策略重建，不兼容旧索引文件；各访问方法的完整 WAL-safe 增量维护仍未完成。
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

## 2026-08-23 v0.3 批次：PITR（时间点恢复）落地

本批次关闭 P0 级差距"时间点恢复"，分三个提交完成并各自全量验证：

1. **WAL 归档执行**（`3976309`）：`archive_command='dir:<path>'` 配置（单引号值可含空格与 `=`，空引号=未设置）；checkpoint 边界标记 `.ready` → 原子复制 → `.done`，失败保留 `.ready` 由 bgwriter 归档线程每 200ms 重试（PostgreSQL 语义）。`tests/wal_archive_conf_test` 覆盖解析与端到端标记翻转。修复测试基建陷阱：测试内强定义 `g_config` 与 test_stubs 弱符号在增量构建下布局错位导致静态析构 double-free——改为 `extern` 声明使用桩定义。
2. **PITR 恢复**（`ab33d50`）：提交记录 v2 格式 `[xid][epoch]` 携带时间戳（v1 记录兼容，读作无限制）；`RESTORE DATABASE db FROM 'bk' PITR 'YYYY-MM-DD HH:MM:SS' ARCHIVE 'dir'` 归档段回填 pg_wal + 持久化单次消费的 `recovery_target`，下次启动恢复把目标后提交按未提交回滚（before-image 恢复）。新增 `PG_SWITCH_WAL`（零填充关闭当前段；`scanWalTail`/`validateRecordsOnDisk` 在填充处停止，`XLogInsert` 自愈空尾缓存）。顺带修复两个存量 CLI bug：物理 `BACKUP/RESTORE DATABASE` 前缀长度错误（`substr(0,14)` vs 15 字符关键字）从未匹配过、且 SQL-dump `RESTORE` 分支遮蔽物理恢复；`.sql_stats` 持久化零调用行导致重启 abort。Shell E2E：备份→分段写入→PITR 到中间时刻→目标后写入消失，PASS。
3. **文档**（`7857db2`）：`MANUAL.md` §20.1 归档配置/`PG_SWITCH_WAL`/四步 PITR 操作手册；`commandsList.md` 新增 `RESTORE DATABASE (PITR)` 与 `PG_SWITCH_WAL` 条目；`feature-gaps.md` P2-3 翻转为部分完成；`all-gaps-todo.md` 存储/WAL、复制/HA 矩阵行更新。

验证状态：完整回归 `PASS=164 FAIL=0`（含新增归档测试与 7 个 Python E2E）；ASAN 与 TSAN 全 CLEAN（归档测试已加入 sanitizer 核心集）；崩溃矩阵 12/12（首跑 11/1、基线对照 10/2 均为宿主机 RL 训练进程 ~30s IO 突发导致的 kill 时序偏移，复跑全绿）。sanitizer.sh 修复存量缺陷：对象重建只比较 .cpp mtime 不查头文件，布局变更头文件（如 Config 新字段）会与新测试代码错位（UBSAN 报非法 bool 读）——现已按任意项目头文件新于对象即重建。

## 2026-08-23 v0.4 批次：GiST 查询路径接入（P0-3 部分翻转）

P0-3 长期停留在"仅有 SP-GiST、无通用 GiST"的记录上，但审计发现 `CREATE INDEX ... USING GIST` 的存储层 sidecar（每行 `rid low high`）早已存在——缺的是**查询路径**：没有任何谓词会使用它，没有 EXPLAIN 节点，没有回归。本批把它接入计划器：

1. **`GiSTScanOp` 执行算子**（`src/executor/ExecutionPlan.{h,cpp}`）：把 WHERE 中同列的范围谓词对（`>= n AND <= m`、单边 `> n`/`< m`）折叠成 `[lo, hi]`，或把锚定前缀 `LIKE 'prefix%'` 折叠成 `[prefix, prefix+\x7f]`，对 `.gist` sidecar 做 overlap 候选 RID 收窄，再逐 RID 走与 IndexScan/Bitmap 相同的 page-lock/MVCC 边界回表。所有原谓词保留在上方 FilterOp 重检——本节点只收窄候选，不是正确性过滤器（与 BitmapHeapScan 同一契约）。RLS 表和分区表安全回退串行扫描。
2. **数值感知重叠判定**（`src/commands/TableManage.cpp`）：`giSTSearchOverlap`/`giSTSearchContainedBy` 原先纯文本比较，`"51" > "100"` 的字典序使数值列范围查询返回空。现在双侧全为数字字面量时按 double 比较，否则保持文本序（文本列前缀语义不变）。
3. **两个存量 off-by-one**：`.gist`/`.brin` 是 5 字符后缀但列发现代码用 `substr(size-6)` 比较 6 字符——`getGiSTIndexedColumns`/`getBrinIndexedColumns` 从未返回过任何列（意味着 BRIN 的查询侧消费也从未可能生效），已修正。
4. **EXPLAIN**：新增 `GiSTScan(table=..., col>=v AND col<=v)` 节点（行数上界 = 表行数，代价按随机回表近似）。
5. **文档矫正（顺带）**：P0-1 的现状描述同样过时——GatherMerge/并行 HashJoin/并行聚合已于 2026-08-20（`cc40031`）落地，feature-gaps 与 all-gaps-todo 已同步；两处残余（worker 生命周期、两阶段聚合）如实保留。

新增 `tests/gist_scan_test.cpp`：范围双边/单边、前缀 LIKE（含奇偶 parity 正确性探针——`even7%` 必须为 0 行）、非 GiST 列保持串行、EXPLAIN 节点存在、sidecar 列发现（依赖 off-by-one 修复）。

验证状态：完整回归 `PASS=165 FAIL=0`（164 既有 + 新增 gist_scan_test，含 7 个 Python E2E）；ASAN CLEAN。已知边界（记录于 feature-gaps P0-3 残余）：sidecar 是线性扫描而非键聚合树（union/penalty/picksplit）；无 tsvector/几何 opclass；无 `<->` KNN；DML 后需 REINDEX 刷新 sidecar（既有行为，本批未改）。

## 2026-08-23 v0.5 批次：join 视图 INSTEAD OF 触发器修复（P0-7 部分翻转）

P0-7 记录"INSTEAD OF 触发器已支持"但协议级审计发现 **join 视图上的整套路径从未工作过**，且一个回归让 UPDATE/DELETE 静默影响全表。四个连环存量缺陷：

1. **存储 SQL 的 token 空格拼接产生 `old . bid`**（`parser.cpp` 的 `CREATE VIEW` selectSql 与 `CREATE TRIGGER` legacy action 都用 `tokens[i] + " "` 拼接，而 lexer 把 `.` 单独切词）：join 执行器把 `jb . bid` 当未知列 → **join 视图 SELECT 恒返回空**；触发器 action 里 `old . bid` 与 `replaceTriggerReference` 的 `OLD.col` 匹配失配 → **UPDATE/DELETE 的 WHERE 失效、静默影响全表**（对生产是数据损坏级）。修复：`joinSqlTokens` 折叠 `ident . ident` → `ident.ident`（多段 `a.b.c` 连续折叠），两处存储路径统一使用。
2. **join 视图触发器行收集走 `getViewBaseTable`**（join 视图无 BASE_TABLE 行 → `collectViewRows` 返回空 → 触发器零次触发但外层仍报成功）。修复：无 BASE_TABLE 时执行视图自身 SELECT（把 DML 的 WHERE 折叠进去）解析输出行。
3. **join 投影 header/data 列序错位**（header 用 `set<string>` 字母序、data 用 FROM 序）——`SELECT ja.tag, jb.bid, jb.val` 的值会配错列。修复：按请求顺序输出 header，并把每行单元格重排到请求序（从引擎的 left-then-right 布局映射）。
4. **视图行 map 只有量化键**（`jb9.bid`）而触发器替换裸引用（`NEW.bid`）→ 替换失配。修复：收集时同时提供 bare 列名键。
5. **`INSERT ... RETURNING` 经视图触发器只有命令标签**（无 RowData）。修复：通过 `publishLastDmlResult`（新增 API）把 NEW 值投影发布为结构化结果，协议层发出真实行。

新增回归：`tests/postgres_protocol_test.py` 追加 join 视图 SELECT 两列序断言、经触发器的 UPDATE/DELETE 精确行路由、单表视图触发器 `RETURNING` 行返回。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E，新断言在 postgres_protocol_test 内）。协议级手工矩阵：join 视图 SELECT / UPDATE（单行命中）/ DELETE（单行命中）/ RETURNING（单列与 `*`）全部符合预期。已知边界：复杂 `INSERT ... SELECT` 经视图触发器、transition tables、`EXECUTE FUNCTION` 运行时仍未做（feature-gaps P0-7 残余）。

已知边界（记录于 feature-gaps P2-3 残余）：外部命令形式 `archive_command` 仅解析不执行（安全考虑本批只做内建 `dir:` 复制）；单时间线（timeline 1）；`pg_basebackup` 协议未实现。恢复目标为进程重启时消费,不支持在线滚动恢复。


## 2026-08-23 v0.6 批次：触发器 EXECUTE FUNCTION 接入 UDF 运行时（P0-7 残余项）

协议级审计发现 `CREATE TRIGGER ... EXECUTE FUNCTION fn()` 的函数体**从未执行**：action 以 `fn()` 文本存储后仍走 SQL 执行器，`fn()` 不是合法 SQL → 静默失败，INSERT 照常成功、函数副作用丢失。

1. **EXECUTE FUNCTION 分发**（`main.cpp` 触发器执行器）：action 匹配 `name(args)` 且 name 是已存储 UDF 时改走 UDF 运行时；顶层逗号拆分字面量参数（剥引号），`StorageEngine::callUDF`（新 API）按语言分发——PL/pgSQL 体进解释器（参数按名预绑定），SQL 体做参数替换求值。非 UDF 形态的 legacy SQL action 原样走 SQL 执行器（回归验证不回归）。
2. **PL 体提取公共化**：`applyScalarFunc` 内联的 UDF 求值块提取为 `evalUDFBody` 共享（标量投影与 `callUDF` 同一路径，避免双实现漂移）。
3. **PL/pgSQL INSERT 字面量带引号落库**（存量缺陷，直接调用与触发器路径同病）：`plpgsqlExecSql` 用 `LiteralExpr::toString()` 取值导致 `'fired'` 连引号入库；现剥一层引号。

新增回归：`tests/postgres_protocol_test.py` 追加 PL 触发函数逐行触发（2 次 INSERT → 2 行日志）、字面量参数 `pl_tagv(7)` 传递、字符串落库无引号、legacy SQL action 触发器不回归。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E）；ASAN CLEAN。已知边界：触发器函数内 `NEW`/`OLD`/`TG_*` 变量注入、`RETURNING` 语义（返回 NULL/触发器返回值丢弃为 PG 简化）、PL 体 UPDATE/DELETE 语句（`plpgsqlExecSql` 显式不支持并如实报错）。


## 2026-08-23 v0.7 批次：无 FROM 常量投影 SELECT

`SELECT 1+1`、`SELECT current_user`、`SELECT myfunc(21)` 这类无 FROM 的常量投影一直直接报 `SQL syntax error`——legacy SELECT 分派只特判了 `unnest(<array>)` 与序列函数，其余全部拒收。这是基础 PG 兼容面：任何常量表达式、伪函数、无 FROM 的 UDF 调用都不可用（也挡住了触发器函数的手动验证路径）。

实现 `handleFromlessSelect`（`main.cpp`）：
1. 顶层逗号拆分投影项（括号/字符串感知）；
2. 每项求值：`AS` 别名剥离后依次尝试伪函数（`current_user`/`session_user`/`version()`）、UDF 调用（`name(literal,...)` 经 `callUDF` 走 PL/pgSQL/SQL 运行时，字面量参数剥引号）、常量表达式（`ExprHelper`：算术优先级/一元负号/括号/`||`/NULL）；
3. 列名遵循 PG：显式别名 > 单 token 字面量原样 > `?column?`（多 token 表达式）；表头单元格保持单 token（cout 表格式以空格分列）；
4. 输出单行（`DISTINCT` 单行语义为无操作）；`unnest` 保持既有展开路径。

新增回归：`tests/postgres_protocol_test.py` 追加 `SELECT 1+1`/`SELECT -5`/`SELECT 1+2*3, 'a'||'b' AS cat`/`SELECT current_user`/`SELECT dbl21(21)`（无别名与有别名）断言。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E）；ASAN CLEAN。已知边界：FROM-less 下标量子查询、`SELECT ... WHERE`（PG 也不支持无 FROM WHERE）、多行 set-returning 函数（除 unnest）未覆盖。


## 2026-08-23 v0.8 批次：触发器函数 NEW/OLD/TG_* 变量注入

v0.6 让 `EXECUTE FUNCTION` 触发器真正执行函数体，但函数体内 `new.col`/`old.col`/`tg_op` 等触发器变量不可见（未绑定，dotted 引用原样落库为文本）。

1. **PL 解释器 dotted 变量替换**（`plpgsql.cpp substitute()`）：词法扫描在标识符后尝试逐段扩展 `.` 追加段，整段匹配小写 dotted 键（`new.id`、`tg_relname`）一次替换；找不到则回退单词路径（既有变量语义不变）。
2. **触发上下文管线**：`StorageEngine::TriggerCtx` + `setExecFunctionCtx` 暂存（NEW/OLD 值映射 + `tg_name/tg_when/tg_level/tg_op/tg_relname`）；9 个触发点火点（B/A × INS/UPD/DEL，行级/语句级）在调用执行器前暂存；main.cpp 触发器执行器的 UDF 分派改走 `callUDFWithCtx` 预绑定解释器参数（显式参数优先）。
3. **PG 语义对齐**：缺失侧是 NULL 记录——INSERT 触发器里 `old.col` 读 NULL（按表列全量绑 null）而非字面文本；AFTER UPDATE 的 OLD 从更新前捕获的行预映像（rid→列值 map，更新循环内记录）取值，而非重读已写入 NEW 的磁盘行。

新增回归：`tests/postgres_protocol_test.py` 追加同一审计函数在 INSERT（NEW 可见/OLD null）、UPDATE（OLD=旧值/NEW=新值）、DELETE（OLD 可见/NEW null）三事件的完整断言。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E）。已知边界：函数体引用不存在的字段仍静默保留文本（PG 报 "record ... has no field"）；`tg_argv`/`tg_tag`/`tg_table_name` 等扩展变量未提供。


## 2026-08-23 v0.9 批次：`use <db>` 后端崩溃修复

`use testdb;`（PG 生态客户端常见的手工切换写法）直接令后端 abort：parser 把所有以 "use" 开头的语句归类 `UseDatabase`，而处理器无条件 `sql.substr(13)`（跳过 "use database" 前缀）——短式 9 字符越界抛 `std::out_of_range`，未捕获 → 进程崩溃（协议连接即 DoS）。

修复两处：
1. `main.cpp` UseDatabase 分支：先识别 13 字符 "use database " 前缀，否则按短式 `use <name>`（`substr(3)`）解析；空名报语法错误而非崩溃。
2. `parser.cpp` 归类加词边界：`use` 后必须紧跟空白或行尾——`username_check` 这类标识符不再被误分类为 UseDatabase（此前会误吞进数据库切换逻辑）。

新增回归：`tests/postgres_protocol_test.py` 在协议连接上依次执行 `use info`、`use database info`、随后 `SELECT 1+1` 仍可用（连接存活断言；旧行为是第一条就杀死服务器）。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E）；协议级实测短式/长式/误拼标识符三路径（服务器全程存活）。


## 2026-08-23 v0.10 批次：transition tables 首个子集（REFERENCING OLD TABLE）

`CREATE TRIGGER ... REFERENCING OLD TABLE AS x FOR EACH STATEMENT` 此前直接被 parser 丢弃（FOR EACH 子句前不认识 REFERENCING，导致其泄漏进 action 文本）。本批落地经典审计场景所需的完整链路：

1. **Parser**：REFERENCING 子句按 PG 顺序在 FOR EACH 之前解析（`NEW TABLE [AS] n` / `OLD TABLE [AS] n`，可多个），存入 `CreateTriggerStmt::transitionTableNames`（`"old <name>"` 条目）；FOR EACH 恢复正确消费。
2. **存储**：`Trigger::transitions` 字段按 count-prefix 序列化进 `.triggers` sidecar；旧 sidecar 无此块读空向量（向后兼容）。
3. **引擎暂存**：AFTER DELETE 语句级点火点把 `rowsToDelete` 的完整预语句行集（列名→值 map）填进 `TriggerCtx::transitionRows[别名]`。
4. **执行器物化**：触发器执行器把暂存行集建成会话临时表（REFERENCING 别名注册进 `session.tempTables`，`resolveTableName` 自动解析），action SQL 里 `FROM deleted_rows` 直接可查；action 完成后 drop 并反注册，别名不留残迹。

验证：`delete ... where id <= 2` → 审计表收到被删两行、目标表剩一行；二次触发正常；语句后别名确实消失（查询报 not exist）；legacy 行级触发器不受影响。

新增回归：`tests/postgres_protocol_test.py` 追加完整场景断言（建触发器、删 2 行、审计表内容、目标表剩余、别名清理）。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E）。已知边界：`NEW TABLE`（INSERT/UPDATE 的转换表）需语句级批处理捕获（INSERT 引擎路径逐行调用）；`OLD TABLE` 仅 AFTER DELETE。


## 2026-08-23 v0.11 批次：likesel 接入统计（P1-12 残余项）

`LIKE` 选择率是 `estimateSelectivity` 中最后一个硬编码（恒 0.2）：90 行 'hot' + 10 行 'cN' 的列上 `LIKE 'hot%'` 估计 20 行（真实 90）——倾斜数据下 LIKE 谓词的行数估计完全失真。

实现 PG 风格 likesel（`ExecutionPlan.cpp`）：
1. 提取模式字面前缀（`%`/`_` 截断，`\\` 转义通配符计入前缀）；无字面前缀（如 `'%x'`）保持平坦 0.2。
2. 前缀上界 prefix++：末字符递增、255 进位回退为 \x01、全进位则追加字节——取直方图上 `>= prefix AND < prefix++` 的区间估计作为前缀匹配选择率。
3. MCV 修正：热值落在区间内时用精确频率（count/rows）对直方图的均匀假设做上修（取较大者）。
4. 模式值既接受 parseConditions 已剥引号的裸文本，也接受 PlanContext 直接传入的 `'hot%'` 字面量。

验证：`tests/stats_planner_test.cpp` 追加两个断言——`LIKE 'hot%'` 估计 ∈ [45,95]（真实 90，平坦值只给 20）、`LIKE 'c95%'` ∈ [0,10]；既有 MCV/直方图/join 场景不回归。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E）。P1-12 剩余：多表 join 顺序从贪心升级为 DP 搜索、`is null`/`in` 等边缘选择率。


## 2026-08-23 v0.12 批次：JOIN 两项修复（ON 词边界 + eqjoinsel 附表选择）

多表 join 审计中发现两个缺陷，其一为静默数据路径错误：

1. **`JOIN` 右表名含 "on" 即失败**（`main.cpp` 2 表 join 路径）：ON 子句定位用朴素 `sql.find("on", joinPos)`，`join location on ...` 中的 "location" 内嵌 "on" 被误认为关键字 → 右表被切至 `l` 前缀 → 报错误的 "not exist"（或更糟，join 错误片段）。实测 `select x,y from plain join location on plain.x = location.x` 稳定失败，而表名不含 "on" 的同构查询正常——纯表名运气。修复为词边界匹配（前后须为非字母数字/下划线）。别名形式（`join location as l`）同样受累并一并修复。

2. **多表贪心附表选择用裸表行数**（≥3 表路径）：原逻辑每次附加"行数最小的待连接表"，完全无视连接键选择性——事实表先连 region(nd=10) 还是 cust(nd=300) 产出中间结果差 30×，原逻辑只看表大小。升级为 eqjoinsel 估计：`est = interEstRows × tableRows / max(nd_l, nd_r)`（nd 取 ANALYZE 统计 cardinality，增量维护中间估计行数），cross-join 回退同样按笛卡尔积估计选择。外连接/初始对选择逻辑不变。

验证：`facts(5000)×cust(300)×prod(80)×region(10)` 星型 2/3/4 表 join + LEFT JOIN 全部行数正确；`location` 表名用例修复。新增协议回归：含 "on" 表名的 INNER/LEFT(别名) join 断言。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E）。P1-12 剩余：穷举 DP join 顺序（当前贪心+统计估计）、`is null`/`in` 选择率。


## 2026-08-23 v0.13 批次：null_frac 统计 + IS NULL/IS NOT NULL 选择率 + stats 解析对齐修复

P1-12 收尾项：NULL 选择率此前完全缺失（`isnull`/`isnotnull` 落入兜底 0.3，等值估计也不感知 NULL），且统计解析存在潜在字段错位。

1. **采集**（`TableManage.cpp` analyzeTable）：每列计数空值（引擎以零长度编码 NULL 的约定），`ColumnStats::nullCount` 新字段持久化为 `.stats` 第 5 个 `|` 字段；旧文件无此字段读 0（向后兼容）。
2. **解析对齐修复**：`parseStatsLine` 原用「跳过前导空字段」对齐 min/max/hist/mcv，但 min/max 可合法为空（全 NULL 列/varchar 列最小值为空串），两个空字段时 MCV/nullCount 全体左移错位（本次实测复现：nullCount 被吞、MCV 混入纯数字项）。改为位置对齐（parts[0] 为 cardinality 后定界符空段，min/max/hist/mcv/nulls 依次取 1..5），保留旧退化行回退路径。
3. **选择率**（`ExecutionPlan.cpp` estimateSelectivity）：`isnull` → nullCount/rows 精确值（无统计回退 0.1）；`isnotnull` → 1−null_frac（回退 0.9）；冷值等值按 PG 语义 `(1−null_frac)/ndistinct`（NULL 不参与等值匹配）。

验证：`stats_planner_test` 新增场景（100 行中 30 NULL：nullCount 断言、isnull 估 30 精确、isnotnull 估 70），既有 MCV/直方图/likesel/join 场景不回归。注意：固定宽度数值列的零值与 NULL 同编码（引擎既有约定），NULL 精确计数对 varchar/文本类列完全成立。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E）。P1-12 剩余：穷举 DP join 顺序、`IN` 列表选择率。


## 2026-08-24 v0.14 批次：IN/NOT IN 字面量列表——规划器可见化 + NOT IN 静默空结果修复

探测发现两个耦合缺陷：

1. **IN 谓词对规划器不可见**：`id in (1,3,5)` 执行正确（legacy 路径改写为 OR-of-=），但 EXPLAIN 完全无 Filter 节点、rows 不衰减——估算器与计划器都看不到该谓词。
2. **NOT IN 静默返回空**（长期潜伏）：legacy 改写器用朴素 `find(" in ")` 定位，"not in" 内嵌的 " in " 被误中 → 列名回扫成 "not" → `id not in (1,3)` 被改写为 `not=1 or not=3`（应为 `id!=1 and id!=3`）→ 恒无匹配 → 所有 NOT IN 查询静默返回零行。

修复链路：
- **modifyLogic/parseConditions/evalConditionOnRow/estimateSelectivity** 四点新增 `in`/`notin` 条件算子（值为空格连接的字面量）；IN 选择率按 PG eqsel 求和语义（MCV 精确值求和，clamp）。
- **compactInLists**：主执行流在 tokenize 前把 `col [not] in (…)` 折叠为无空白单 token（`in<col>(v1,v2)`），避免逐 token modifyLogic 把谓词拆碎。
- **legacy 改写器修复**：` not in ` 优先探测（词边界含列表括号 `(`），列名正确回扫；NOT IN 改写为 AND-of-`!=`（PG 语义），空列表 `not in ()` 恒真。

验证（协议级实测）：IN/NOT IN 数值与文本列表、AND/OR 组合、`not in ()` 全部正确；EXPLAIN 出现 Filter 节点且 rows=3（5 行 × 3 值 × 1/5 精确）；`tests/postgres_protocol_test.py` 新增 6 项断言（含 EXPLAIN Filter 存在性）。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E）。


## 2026-08-24 v0.15 批次：单表别名与表名限定符（两个静默错误结果缺陷）

协议级审计发现两个基础语法缺陷（均以错误结果而非报错呈现）：

1. **`FROM t [as] a` 完全不可用**：单表路径的 FROM 文本 `emp e` 未拆分别名，整个串被当作表名 → "Table emp e not exist"。任何带别名的单表查询（含聚合/星号/IN/ORDER/LIMIT 全部形状）一律失败。JOIN 路径别名正常，独单表路径缺失。
2. **表名限定谓词静默返回空**（长期潜伏）：`where emp.id = 2` 中 `emp.` 限定符未被剥离，条件列名解析为不存在的 "emp.id" → 谓词被丢弃 → 返回空集（应为 id=2 一行）。HEAD 基线复现确认非本会话引入。

修复（`main.cpp` 单表 SELECT 路径）：
- FROM 区早期解析出 `表名 + [as] 别名`，别名用于剥离投影与 WHERE 中的 `<alias>.` 前缀；
- `<table>.` 限定符同样从投影与 WHERE 剥离（WHERE 剥离同时覆盖别名与表名两种限定）；
- 投影剥离处理前导空白（FROM 后首个 token 提取修正）。

验证：别名（裸/as/限定列/聚合/IN/ORDER/LIMIT）、表名限定（投影+WHERE+聚合）、混合限定（表名限定投影+别名限定 WHERE）、JOIN 路径无回归、普通无限定查询无回归，全形状实测通过；`tests/postgres_protocol_test.py` 新增 6 项断言。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E）。已知相邻残留：投影列别名（`select id as no`）与 ORDER BY 组合的解析问题（独立缺陷，本批未触碰）。


## 2026-08-24 v0.16 批次：SELECT 列表别名（AS）+ 算术投影 + ORDER BY 别名

协议级审计发现三联缺陷（同一条语法链）：

1. **投影别名完全不可用**：`select id as no from t` 报 "Invalid column name id as no"——单表路径把整个 `expr as name` 文本当作列名校验。全部形状失败（普通列/表达式/GROUP BY 投影），仅聚合路径（`count(*) as c`）例外。
2. **算术投影不存在**：`select salary * 2 from t`（无论有无 AS）报同样错误——引擎无表达式投影能力。
3. **ORDER BY 别名被忽略**：`order by no`（no 为输出别名）不报错但静默按物理顺序返回（错误顺序结果）。

修复：
- 投影循环早期拆分 `expr AS alias`：表达式参与列校验与求值，displayName 用别名（PG 语义）；AS 尾部合法性校验（不含运算符/逗号）。
- 新增 `arith` 标量表达式（applyScalarFunc 内）：操作数（列/整型/浮点/字符串字面量经 getVal 行内解析）与运算符（+ - * / %）分离编码，左结合求值；除零/取模零返回 NULL；字符串 '+' 拼接；整数结果无小数点输出。主路径按"运算符位于非空操作数之间"检测算术项并路由。
- ORDER BY 简单列项先查 SELECT 别名映射（早期构建的 alias→expr map），别名解析为底层表达式/列。

验证：`salary*2`→200、`salary+id`→202、`salary/4`→100、`salary-100`→400、链式 `salary+id-1`→302、AS 别名（普通/order/group 投影）、ORDER BY 别名 desc/asc 全部实测正确；普通投影/ORDER BY 无回归。协议回归 +5 断言。已知残留：GROUP BY 引用输出别名（`group by d`）暂不支持（PG 允许，低频形状）。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E）。

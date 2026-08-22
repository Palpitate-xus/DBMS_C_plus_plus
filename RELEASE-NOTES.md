# RELEASE NOTES — DBMS v0.1.0

发布日期：2026-08-21
代号：first public cut

---

## 概览

v0.1.0 是首个公开发布版本。核心定位：**C++17 实现的 PostgreSQL 兼容单机数据库**，覆盖 SQL 兼容面（DDL/DML/查询/PL/pgSQL）、事务（MVCC + WAL 崩溃恢复）、PostgreSQL wire protocol v3（SCRAM-SHA-256 认证）、以及本批次补齐的运维面（连接池、逻辑解码、透明数据加密）。

**质量基线**：165 个 C++ 回归测试 + 7 个 Python E2E 全绿；ASAN/UBSAN 与 TSAN 核心集 12/12 CLEAN；崩溃恢复矩阵 12/12；soak 负载（单 server 多客户端混合 DML）通过。

## 本版本亮点

- **TDE 透明数据加密**（P2-8）：页级 SHA-256-CTR + Encrypt-then-MAC，边车信封文件，keyring 管理，与 WAL 崩溃恢复兼容。at-rest 数据文件全文无明文。
- **逻辑解码**（P2-5）：`pgoutput`/`test_decoding` 双输出插件、Publication 目录、按槽变更流（peek/confirm/LSN 推进）。
- **PgBouncer 式连接池**（P2-4）：session/transaction/statement 三模式。
- **Bloom 索引**（P2-2）与**自定义代价函数钩子**（P1-9）。
- **发布工程**：CI 管线、sanitizer 例程、崩溃恢复矩阵、soak 负载、打包脚本——本批次产出一个真修复（WAL LSN 数据竞争）。

## 已知限制（务必阅读）

### 1. DML 双执行入口（P1-0，最重要的架构债）
普通单表 INSERT/UPDATE/DELETE（含受限 RETURNING、窄版 MERGE、单源 UPDATE FROM/DELETE USING、简单 ON CONFLICT）已由结构化 `DmlExecutor` 执行。**以下仍回退 legacy 字符串路径**，错误码与行为一致性受限：
- 复杂 INSERT ... SELECT
- 部分/索引推断 conflict target 的 ON CONFLICT
- 引用子查询或其他关系的 DO UPDATE / WHERE
- 复杂/子查询/窗口 RETURNING
- 外连接/复杂 UPDATE FROM / DELETE USING
- 多 WHEN / BY SOURCE / BY TARGET / DELETE 子句的 MERGE
- 视图写入

### 2. SERIALIZABLE 不是 PostgreSQL 完整 SSI
关系级行级读写集合 + 页级 SIREAD + 单列 B+Tree 逻辑索引谓词已就位；**复合/表达式/部分索引及其他访问方法的精确 phantom 推理不完整**；空结果关系级兜底会牺牲部分串行化并发度。需要完整 SSI 保证的场景请使用 REPEATABLE READ 并配合显式锁。

### 3. TDE 覆盖范围
- **未经 BufferPool 页路径的索引文件未加密**（受影响的是不通过堆页池的关系旁路文件）
- 无密钥轮换（re-encrypt）、无 per-database 密钥隔离
- keyring 丢失 = 数据不可恢复（设计如此，fail-closed）

### 4. 逻辑解码
- 无 `CREATE SUBSCRIPTION` 拉取端（只有槽 + SHOW 面消费）
- 变更不经 WAL 重放而是写路径直采：**崩溃恢复后逻辑槽不 replay**，可能丢中断前的变更
- pgoutput 为自描述帧式，未做 PG 二进制 wire 级兼容

### 5. 并发架构约束
**多进程共享同一数据目录不是受支持形态**（每进程 XID 计数器冲突 → 共享 WAL 恢复出错）。多用户并发必须通过**单 `dbms_main --server` 进程**（thread-per-connection）。CLI 交互模式适合单人运维操作。

### 5a. 高并发下的已知问题（soak 发现，v0.2 处理）
- **同页插入竞态**：两个连接并发 INSERT 命中同一堆页时存在数据竞争（`PgPage::insert`/`setLpOff`）。当前表现为吞吐受限 + 偶发语句级错误；已提交数据的持久化正确性不受影响（WAL 与崩溃恢复矩阵 12/12）。
- **高负载停顿（v0.2 已缓解，未根除）**：根因链已定位——commit 路径同步刷全部脏页 + WAL fsync 使 ext4 jbd2 过载，fsync 排队期间持有插入锁导致全局冻结。已落地 WAL group commit 与堆页延迟刷（吞吐 43→613 ops/60s）；剩余放大源是索引文件的整文件 WAL 图像机制，待重构为索引逻辑 WAL。高并发部署建议磁盘支持低 fsync 延迟。
- **LockManager 锁序协议**：持有物理锁令牌期间不可再调用锁管理器公开 API（TSAN lock-order-inversion 报告为设计权衡；实际死锁由 wait-graph 环检测 + 1s 超时兜底）。
- **连接风暴**：大量客户端同时接入时 accept 队列 + 每连接 SCRAM PBKDF2 计算可能超时，客户端退避重连即可。

### 6. 代价模型（P1-12）
MCV/直方图已采集但 planner 消费不全：范围选择率硬编码 0.3，join 选择用固定公式。**EXPLAIN 的行数估计有误导性**；实际执行计划选择受影响有限（索引可用性主导）。

### 7. 并行执行
仅并行表扫描（page range 分片 + Gather）。并行 join/aggregate、GatherMerge 未实现。

### 8. prepared statements 语法（P1-10）
SQL 级为自有语法 `PREPARE name FROM 'sql'` + `?` + `EXECUTE name USING (vals)`，**不是** PostgreSQL 的 `PREPARE name (types) AS ... $1` 语法；协议层 Parse/Bind/Execute 是 PG 兼容的。无跨执行计划复用。

### 9. 其他
- PL/pgSQL 最小运行时：无 EXCEPTION 块、游标控制受限
- 统计收集器无后台采样线程
- JIT 编译、GiST 索引、C 扩展加载未实现（见 docs/feature-gaps.md P0/P3）

## 升级与安装

见 `docs/PACKAGING.md`（目录约定、tarball 内容、部署步骤）。

## 完整变更

见 `CHANGELOG.md`。

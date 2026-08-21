# Changelog

本项目的所有显著变更记录于此文件。
格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

## [0.1.0] - 2026-08-21

首个公开发布版本（v0.1.0 "first public cut"）。
基线：165 个 C++ 回归测试 + 5 个 E2E（Python 协议/窗口函数/EXPLAIN ANALYZE/多表 JOIN/timestamptz）全绿。

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

[Unreleased]: https://github.com/Palpitate-xus/DBMS_C_plus_plus/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Palpitate-xus/DBMS_C_plus_plus/releases/tag/v0.1.0

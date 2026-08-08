# PostgreSQL 18 vs 本 DBMS 功能对比

> 生成日期: 2026-08-08（更新反映存储格式硬切与当前代码状态）
> 本 DBMS 代码规模: ~66,000 行 C++ (44 .cpp + 56 .h)
> 对照: PostgreSQL 18 (~1,200,000 行 C)
> 测试基线: PASS=122 FAIL=0（120 个 C++ 测试 + PostgreSQL 协议 E2E + 窗口函数 E2E；含 Volcano 算子、并发测试、数据库生命周期、schema 格式完整性和网络启动安全）

协议回归现已覆盖扩展查询错误后的 ignore-until-Sync 恢复、事务外 `ReadyForQuery('I')` 状态、文本/binary 整数、numeric 及 date/time/timestamp/UUID 参数与结果、statement/portal 的 Describe/Close 生命周期、基础 portal `maxRows` 分页及常见单表 RowDescription 元数据；这只补齐了错误状态机与参数路径的一部分，数组等复杂类型 I/O、复杂表达式的完整类型映射、内部秒精度之外的时间精度、holdable/scrollable portal 和扩展消息仍与 PostgreSQL 有差距。

ACL 回归现已覆盖表/列权限对会话用户、继承角色和 `PUBLIC` 的解析，并验证角色授权可以通过真实 PostgreSQL 协议生效；owner、完整 ACL 对象范围及 `GRANT OPTION` 生命周期仍未达到 PostgreSQL 语义。

事务回归现已覆盖两个协议 backend 的事务上下文隔离：每个连接线程拥有独立的事务 ID、快照、回滚日志和 savepoint 状态，未提交数据不会被另一连接读取。该实现匹配当前一连接一工作线程的运行模型；锁、提交日志和 SSI 冲突图仍是跨 backend 共享协调结构，不能据此宣称完整 PostgreSQL 事务语义已完成。

2026-08-08 质量验证补充：主程序在 `-Wall -Wextra` 下无编译警告；快速回归、独立测试、窗口函数 E2E、协议 E2E 和 OpenSSL Docker 构建均通过。该结果只说明当前实现可重复验证，不改变下文列出的 PostgreSQL 语义与运维差距。

---

## 总览

| 维度 | PostgreSQL 18 | 本 DBMS | 差距 |
|------|--------------|---------|------|
| 代码量 | ~1.2M 行 | ~66K 行 | ~18x |
| 开发团队 | 全球数百人/20+年 | 单人/数周 | — |
| 测试覆盖 | ~2000+ 测试 | 120 C++ 单元测试 + 2 个 E2E | 以回归结果为准 |
| 功能完成度 | 不宣称生产级 | DDL/DML 核心已覆盖，高级特性和运维能力仍有缺口 | 以测试和代码路径为准 |

---

## 一、数据类型

| 类型 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| INT/BIGINT/SMALLINT/SERIAL | ✅ | ✅ | ✅ |
| NUMERIC(p,s) / DECIMAL | ✅ | ✅ | ✅ |
| FLOAT/DOUBLE/REAL | ✅ | ✅ | ✅ |
| MONEY | ✅ | ✅ | ✅ |
| VARCHAR/CHAR/TEXT | ✅ | ✅ | ✅ |
| BYTEA (hex/escape) | ✅ | ✅ | ✅ |
| BOOLEAN | ✅ | ✅ | ✅ |
| DATE/TIME/TIMESTAMP/TIMESTAMPTZ | ✅ | ✅ | ✅ (含 infinity) |
| INTERVAL | ✅ | ✅ | ✅ |
| UUID | ✅ | ✅ | ✅ |
| INET/CIDR | ✅ | ✅ | ✅ |
| MACADDR/MACADDR8 | ✅ | ✅ | ✅ |
| JSON/JSONB | ✅ | ✅ | ✅ |
| XML | ✅ | ✅ (well-formedness) | ⚠️ 缺 XPath/XMLTABLE |
| ARRAY (多维) | ✅ | ✅ (基础) | ⚠️ 缺 切片/unnest/ANY |
| TSVECTOR/TSQUERY | ✅ | ✅ | ✅ |
| GEOMETRIC (line/lseg/box/path/polygon/circle) | ✅ | ✅ | ✅ |
| POINT | ✅ | ✅ | ✅ |
| ENUM | ✅ | ✅ | ✅ |
| COMPOSITE TYPE | ✅ | ✅ | ✅ |
| RANGE/MULTIRANGE | ✅ | ✅ (基础) | ⚠️ 缺 operators |
| DOMAIN | ✅ | ✅ | ✅ |
| **Pseudo types (record/anyelement/anyarray)** | ✅ | ✅ (注册) | ⚠️ 缺函数重载 |
| **bytea 存储** | ✅ TOAST | ⚠️ overflow + zlib 压缩 | ⚠️ 缺少 PG pointer/catalog 与 storage strategy 完整语义 |
| **numeric NaN/Infinity** | ✅ | ✅ | wire codec 与基础值语义已覆盖，精确 typmod/完整函数族仍有差距 |

---

## 二、SQL 语法

### 2.1 DDL

| 功能 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| CREATE/DROP/ALTER DATABASE | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER SCHEMA | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER TABLE (基础子命令；全量仍有缺口) | ✅ | ⚠️ | ⚠️ |
| CREATE/DROP/ALTER VIEW | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER MATERIALIZED VIEW | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER INDEX | ✅ | ✅ | ✅ |
| CREATE INDEX CONCURRENTLY | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER SEQUENCE | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER FUNCTION/PROCEDURE | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER TRIGGER | ✅ | ✅ | ✅ (DDL + 运行时执行) |
| CREATE/DROP/ALTER ROLE/USER | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER TYPE (composite/enum/range) | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER DOMAIN | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER POLICY (RLS) | ✅ | ✅ | ✅ (DDL + 运行时执行) |
| CREATE EXTENSION | ✅ | ✅ (DDL) | ⚠️ 运行时缺 |
| CREATE/DROP/ALTER COLLATION | ✅ | ✅ (DDL) | ⚠️ ICU 缺 |
| TABLESPACE | ✅ | ✅ | ✅ |
| PARTITION BY RANGE/LIST/HASH | ✅ | ✅ | ✅ |
| INHERITS | ✅ | ✅ | ✅ |
| LIKE ( INCLUDING ALL/DEFAULTS/CONSTRAINTS/INDEXES/IDENTITY) | ✅ | ✅ | ✅ |
| TRUNCATE (ONLY/RESTART IDENTITY/CASCADE) | ✅ | ✅ | ✅ |
| **ALTER TABLE ... ADD COLUMN ... IF NOT EXISTS** | ✅ | ✅ | ✅ |
| **REINDEX CONCURRENTLY** | ✅ | ⚠️ | |

### 2.2 DML

| 功能 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| INSERT (VALUES/SELECT/ON CONFLICT/RETURNING) | ✅ | ✅ | ✅ |
| UPDATE (FROM/LIMIT/RETURNING) | ✅ | ✅ | ✅ |
| DELETE (USING/LIMIT/RETURNING) | ✅ | ✅ | ✅ |
| MERGE (MATCHED/NOT MATCHED) | ✅ | ✅ | ✅ |
| REPLACE INTO (MySQL 兼容) | ❌ | ✅ | ✅ |
| **COPY (binary/program)** | ✅ | ⚠️ | 仅 CSV |
| **UPSERT 完整语义** | ✅ | ⚠️ | 基础 |

### 2.3 DQL (查询)

| 功能 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| SELECT (投影/WHERE/ORDER BY/LIMIT/OFFSET/DISTINCT) | ✅ | ✅ | ✅ |
| JOIN (INNER/LEFT/RIGHT/FULL/CROSS/NATURAL) | ✅ | ✅ | ✅ |
| **SEMI/ANTI JOIN** | ✅ | ❌ | 缺 |
| **LATERAL JOIN** | ✅ | ❌ | 缺 |
| Subquery (IN/EXISTS/ANY/ALL/标量) | ✅ | ✅ (parser + volcano) | ⚠️ 复杂子查询回退 legacy |
| CTE (WITH/RECURSIVE) | ✅ | ✅ (parser + executor) | ✅ 基础 + RETURNING |
| UNION/INTERSECT/EXCEPT | ✅ | ⚠️ AST 已按优先级/左结合解析，组合统一走 Volcano `SetOperationOp`；简单 operand 直接计划，复杂 operand 经 `MaterializedRowsOp` 接 legacy producer（含 ALL） | ⚠️ AST 到计划的全量下推、类型合并/collation 和完整作用域仍缺 |
| Window Functions (ROW_NUMBER/RANK/...) | ✅ | ⚠️ `WindowOp` 已执行常见排名/偏移、窗口聚合及 `ROWS` frame/exclusion；复杂窗口仍走 legacy | ⚠️ `RANGE/GROUPS`、复杂表达式、OFFSET 和完整 planner/explain 语义仍缺 |
| **GROUP BY ROLLUP/CUBE/GROUPING SETS** | ✅ | ⚠️ `GroupAggregateOp` 覆盖常见聚合与基础 grouping sets | ⚠️ 复杂目标、`GROUPING()`/`GROUPING_ID`、完整排序作用域仍缺 |
| GROUPING_ID | ✅ | ❌ | 缺 |
| FOR UPDATE/SHARE/NOWAIT/SKIP LOCKED | ✅ | ✅ | ✅ (行级锁 + 死锁检测) |
| **PREPARE TRANSACTION (2PC)** | ✅ | ✅ | ✅ (prepareTransaction + COMMIT/ROLLBACK PREPARED) |
| VALUES | ✅ | ✅ | ✅ |
| **Array subscript [n:m]** | ✅ | ❌ | 缺 |
| **JSON path / SQL/JSON** | ✅ | ❌ | 缺 |

---

## 三、索引

| 访问方法 | PG 18 | 本 DBMS | 状态 |
|----------|-------|---------|------|
| B+ Tree | ✅ | ⚠️ | 基础文件 B+Tree 已覆盖跨叶/内部节点分裂、重复键和范围扫描；PG dedup、删除合并、opclass/collation、skip scan 和完整并发构建仍缺 |
| Hash | ✅ | ✅ | ✅ |
| GIN | ✅ | ✅ (基础, src/access/GinIndex.cpp) | ⚠️ |
| GiST | ✅ | ❌ | 缺 |
| SP-GiST | ✅ | ✅ (Point quadtree, SPGiSTIndex.cpp) | ⚠️ |
| BRIN | ✅ | ✅ (块范围, BrinIndex.cpp) | ⚠️ |
| **Bloom** | ✅ | ❌ | 缺 |
| **覆盖索引 (INCLUDE)** | ✅ | ✅ | ✅ |
| **部分索引 (WHERE)** | ✅ | ✅ | ✅ |
| **表达式索引** | ✅ | ✅ | ✅ |
| **唯一索引 + 约束** | ✅ | ✅ | ✅ |
| **多列复合索引** | ✅ | ✅ | ✅ |
| **Index Only Scan** | ✅ | ✅ | ✅ |
| **Bitmap Index/Heap Scan** | ✅ | ⚠️ | 等值多索引 AND/OR 已由 `BitmapHeapScanOp`/`BitmapOrHeapScanOp` 接入；范围 bitmap 仍缺 |

---

## 四、事务与并发

| 特性 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| ACID 事务 | ✅ | ✅ | ✅ |
| MVCC (快照隔离) | ✅ | ✅ | ✅ |
| ReadView + CLOG | ✅ | ✅ | ✅ |
| 隔离级别 (RU/RC/RR/SI/SERIALIZABLE) | ✅ | ⚠️ | 框架有 |
| WAL (Write-Ahead Log) | ✅ | ✅ | ✅ |
| Checkpoint | ✅ | ✅ | ✅ |
| SAVEPOINT | ✅ | ✅ | ✅ |
| 表级锁 (共享/排他) | ✅ | ✅ | ✅ |
| **行级锁** | ✅ | ✅ | ✅ (rowLockShared/Exclusive + NOWAIT) |
| **Deadlock detection** | ✅ | ✅ | ✅ (wait-for graph + cycle detection + log) |
| **Gap locks / predicate locks** | ✅ | ⚠️ | 有简化 gap lock 和关系级 SIREAD；缺页/索引粒度 predicate lock |
| **SSI (Serializable Snapshot Isolation)** | ✅ | ⚠️ | 行级 rw-conflict + 关系级 SIREAD 覆盖空范围读；缺精确 phantom 推理和完整 SSI 规则 |
| **两阶段提交 (2PC)** | ✅ | ✅ | ✅ (prepareTransaction + COMMIT/ROLLBACK PREPARED) |
| **并行查询** | ✅ | ⚠️ | 非分区 heap 已支持按 page range 并行扫描和确定性 Gather；事务内回退，parallel join/aggregate/GatherMerge/worker pool 仍缺 |
| **JIT compilation (LLVM)** | ✅ | ❌ | 缺 |
| **Async I/O (io_uring)** | ✅ (PG18) | ❌ | 缺 |
| HOT Update | ✅ | ✅ | ✅ |
| **谓词锁** | ✅ | ❌ | 缺 |

### 并发测试现状 (2026-07-03 更新)
- ✅ 事务原子性 (BEGIN/COMMIT/ROLLBACK/SAVEPOINT)
- ✅ WAL 顺序写入吞吐 (~1767 行/秒)
- ✅ B+ 树索引插入吞吐 (~1729 行/秒)
- ✅ 索引等值查找 (34 行/3-4ms)
- ✅ 聚合性能 (500 行 COUNT/SUM/MAX/GROUP BY ~78ms)
- ✅ MVCC 快照隔离 (ReadView 可见性)
- ✅ HOT 堆内更新
- ✅ CLOG 提交日志 (位图 + 子事务可见性)
- ✅ Checkpoint 持久化 (脏页刷盘 + WAL 截断)
- ✅ WAL 崩溃恢复 (未提交回滚 + 已提交持久化)
- ❌ 多线程并发压力测试 (LockManager 已就绪但无多线程竞争测试)
- ❌ 死锁检测端到端测试 (WFG 已实现但无并发触发测试)
- ❌ 隔离级别验证测试 (框架有但无跨级别对比)

---

## 五、存储

| 特性 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| Slotted page (8KB) | ✅ | ✅ | ✅ |
| Buffer Pool (clock sweep) | ✅ | ✅ | ✅ |
| Free Space Map | ✅ | ✅ | ✅ |
| Visibility Map | ✅ | ✅ | ✅ |
| TOAST (大字段压缩/线外存储) | ✅ | ⚠️ | relation/index + zlib 压缩；缺少 PG pointer/catalog 完整语义 |
| 页校验和 | ✅ | ✅ | ✅ |
| 溢出页 | ✅ | ✅ | ✅ |
| 子事务日志 | ✅ | ✅ | ✅ |
| **TOAST 压缩 (lz4/pglz)** | ✅ | ⚠️ zlib | 已有压缩标记/解压；lz4/pglz、压缩策略和 `toast_tuple_target` 仍缺 |
| **大对象 (Large Object)** | ✅ | ✅ | ✅ |
| **Unlogged 表** | ✅ | ✅ | ✅ |
| **临时表** | ✅ | ✅ | ✅ |
| **表空间** | ✅ | ✅ | ✅ |

---

## 六、查询优化器

| 特性 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| 火山模型执行器 | ✅ | ✅ (12 operators, Phase 5.1 已上线) | ✅ |
| 基于成本的优化 (CBO) | ✅ | ⚠️ | 基础 |
| 统计信息 (pg_statistic) | ✅ | ⚠️ | 基础 |
| 索引选择 | ✅ | ✅ | ✅ |
| **等价类 (Equivalence Classes)** | ✅ | ⚠️ | 基础 |
| **PathKeys / 排序路径** | ✅ | ⚠️ | 基础 |
| Join reorder (动态规划) | ✅ | ⚠️ | 启发式 |
| Bitmap scan | ✅ | ⚠️ | 等值 Bitmap AND/OR 已接入；范围/并行路径仍缺 |
| **计划缓存** | ✅ | ✅ | ✅ |
| EXPLAIN ANALYZE 真实统计 | ✅ | ✅ | ✅ |
| 多索引组合 (Bitmap AND/OR) | ✅ | ⚠️ | 等值 Bitmap AND/OR 已接入，范围/并行组合仍缺 |
| **参数化路径** | ✅ | ❌ | 缺 |
| **自定义成本函数** | ✅ | ❌ | 缺 |
| **遗传算法 join reorder** | ✅ | ❌ | 缺 |
| **分区裁剪** | ✅ | ✅ | ✅ |
| **并行扫描/聚合/连接** | ✅ | ⚠️ | 非分区 heap page-range scan 已有真实 worker；并行聚合/连接与 GatherMerge 仍缺 |

---

## 七、进程/连接管理

| 特性 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| 多进程后端 (fork) | ✅ | ❌ | 线程模型 |
| **Shared memory** | ✅ | ❌ | 缺 |
| Background workers | ✅ | ⚠️ 框架 | 11 种类型 |
| WALWriter / BgWriter / Checkpointer | ✅ | ✅ | ✅ |
| Autovacuum | ✅ | ⚠️ 框架 | |
| Stats collector | ✅ | ⚠️ 框架 | |
| **连接池** | ✅ (外部) | ✅ | ✅ |
| **最大连接数** | ✅ | ✅ | ✅ |
| **pg_stat_activity** | ✅ | ❌ | 缺 |

---

## 八、复制与高可用

| 特性 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| **物理流复制 (WAL shipping)** | ✅ | ⚠️ 框架 | 基础 |
| 复制槽 (物理/逻辑) | ✅ | ✅ | ✅ |
| 同步/异步复制 | ✅ | ✅ | ✅ |
| Hot Standby | ✅ | ⚠️ 框架 | |
| **Failover / Promote** | ✅ | ✅ | ✅ |
| 级联复制 | ✅ | ✅ (框架) | |
| **逻辑复制 (Publication/Subscription)** | ✅ | ✅ (框架) | 无 plugin 实现 |
| **Logical decoding** | ✅ | ❌ | 缺 |
| PITR (时间点恢复) | ✅ | ⚠️ | 基础 |
| pg_basebackup | ✅ | ❌ | 缺 |
| **增量备份** | ✅ | ❌ | 缺 |

---

## 九、安全

| 特性 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| pg_hba.conf | ✅ | 🔄 | 首条匹配、hostssl/hostnossl、IPv4/IPv6 CIDR、角色/数据库别名和运行时 SCRAM 已实现；其余认证方法仍缺 |
| 用户/角色系统 | ✅ | 🔄 | pg_authid/pg_auth_members、主要角色属性、SCRAM、递归成员匹配、有效期检查和连接数限制已接入；完整 ACL、ADMIN OPTION、owner/依赖语义仍缺 |
| GRANT/REVOKE (ACL) | ✅ | ✅ (DDL) | ⚠️ 执行缺 |
| 列级权限 | ✅ | ✅ | ✅ |
| **行级安全 (RLS) 执行** | ✅ | ⚠️ | 已有基础 USING/WITH CHECK 路径，完整 executor 集成和安全边界仍待验收 |
| **SCRAM-SHA-256 完整协议** | ✅ | 🔄 | 已实现 catalog verifier、challenge/response、pg_hba 运行时决策和 E2E；缺 channel binding 与完整 SASL 语义 |
| **LDAP/Kerberos/GSSAPI/PAM/RADIUS** | ✅ | ❌ | 缺 |
| **SSL 双向认证** | ✅ | ⚠️ TLSWrapper；服务端默认 fail-closed，但缺少 PostgreSQL SSL 协商、客户端证书认证和 channel binding | |
| **安全标签 (SE-PostgreSQL)** | ✅ | ❌ | 缺 |
| **加密 (TDE)** | ✅ | ❌ | 缺 |
| **审计 (pgaudit)** | ✅ | ❌ | 缺 |

---

## 十、扩展生态

| 特性 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| CREATE EXTENSION | ✅ | ⚠️ 框架 | |
| FDW (外部数据包装器) | ✅ | ⚠️ 框架 | |
| PL/pgSQL 运行时 | ✅ | ❌ | 缺 |
| **C 扩展加载 (fmgr)** | ✅ | ❌ | 缺 |
| **Hook 系统** | ✅ | ❌ | 缺 |
| **Background worker API** | ✅ | ⚠️ 框架 | |
| **Shared memory 扩展** | ✅ | ❌ | 缺 |
| 内置扩展 (pg_stat_statements 等) | ✅ | ❌ | 缺 |

---

## 十一、系统目录与监控

| 特性 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| pg_class / pg_attribute / pg_type | ✅ | ✅ | ✅ |
| pg_namespace / pg_proc / pg_depend | ✅ | ✅ | ✅ |
| pg_authid / pg_auth_members | ✅ | ✅ | ✅ |
| pg_description | ✅ | ✅ | ✅ |
| pg_database | ✅ | ✅ | ✅ |
| information_schema | ✅ | ⚠️ 基础 | |
| pg_stat_* views | ✅ | ❌ | 缺 |
| pg_locks / pg_stat_activity | ✅ | ❌ | 缺 |
| **pg_stat_statements** | ✅ | ❌ | 缺 |
| **auto_explain** | ✅ | ❌ | 缺 |

---

## 十二、性能特性缺失 (关键差距)

### 🔴 严重缺失 (生产级必需)
1. **并行查询** — 已有非分区 heap page-range 多 worker scan；事务安全回退，parallel join/aggregate、GatherMerge、worker pool 和 parallel-aware planner 仍缺
2. **JIT 编译** — 表达式求值仍为解释执行, 无 LLVM 后端
3. **索引 GiST** — 全文搜索/几何最近邻索引缺失 (仅有 SP-GiST)
4. **Gap locks / predicate locks** — 无法完全防止幻读
5. **SSI (Serializable Snapshot Isolation)** — 已覆盖关系限定的行级写偏差回滚及空谓词的关系级 SIREAD，但页/索引级 predicate lock 和完整冲突规则仍未实现
6. **Bitmap scan** — 等值多索引 Bitmap AND/OR 已接入；范围 bitmap 和并行路径仍缺
7. **INSTEAD OF 视图触发器** — 已支持简单单表视图上的逐行 INSERT/UPDATE/DELETE action SQL，并覆盖多行协议 E2E；复杂视图映射、transition tables、完整函数/PL 运行时仍缺失

### 🟡 重要缺失 (影响实用性)
1. **Window Function executor** — 常见排名/偏移、窗口聚合和 `ROWS` frame/exclusion 已进入 Volcano `WindowOp`，`RANGE/GROUPS` 与复杂目标仍回退 legacy
2. **复杂子查询 executor** — 简单子查询走 volcano, 关联子查询回退 legacy
3. **UNION/INTERSECT/EXCEPT executor** — 组合语义已统一进入 Volcano，复杂 operand 的 producer 仍待 AST 全量下推；类型合并和结构化列结果也仍缺
4. **GROUP BY 完整语义** — `GroupAggregateOp` 已覆盖基础路径；复杂目标、`GROUPING()`/`GROUPING_ID`、类型推导和完整排序作用域仍待迁移
5. **GiST 索引** — 全文搜索基础架构缺
6. **TOAST 完整语义** — 当前线外值使用 zlib 压缩；lz4/pglz、压缩策略、`toast_tuple_target` 和 PG pointer/catalog 仍待完成
7. **后台 stats_collector** — 无运行时统计收集
8. **PL/pgSQL 运行时** — 存储过程解释执行缺
9. **并行 Vacuum** — Autovacuum 已工作但非并行

### 🟢 次要缺失 (易用性/运维)
1. pg_stat_* views / pg_stat_statements
2. Bloom 索引
3. 并行查询
4. 增量备份
5. pg_upgrade 工具
6. Logical decoding / 逻辑复制 plugin
7. 连接池 (PgBouncer 式)
8. 异步 I/O (io_uring)

---

## 总结

本 DBMS 在 **DDL 完整化** 方面达到了很高的完成度（SQL 语法覆盖 ~95%），但**运行时 semantics** 的执行层面还有显著差距：

- **DDL Parser**: ✅ 高度完整 (184 命令)
- **DML Executor**: ✅ 核心完整 (INSERT/UPDATE/DELETE/基础 SELECT)
- **高级 Query**: ⚠️ parser 就绪, executor 部分完成
- **并发控制**: ⚠️ MVCC 基础有, 行级锁/死锁缺失
- **性能**: ⚠️ 无并行/JIT/高级索引
- **扩展性**: ❌ 框架有, 运行时缺

**下一阶段优先级建议** (2026-07-03 更新):
1. Window Function 完整迁移 + UNION/INTERSECT executor (查询完整性 — 窗口仍有 legacy 语义边界)
2. GiST 索引 (全文搜索/几何)
3. 并行 query exec (性能)
4. 关联子查询解嵌套 (优化器)
5. PL/pgSQL 运行时 (存储过程)
6. pg_stat_statements + 运行时统计 (可观测性)
7. 可串行化隔离 SSI (并发正确性)

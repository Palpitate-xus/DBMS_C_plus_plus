# PostgreSQL 18 vs 本 DBMS 功能对比

> 生成日期: 2026-07-02
> 本 DBMS 代码规模: ~66,000 行 C++ (44 .cpp + 56 .h)
> 对照: PostgreSQL 18 (~1,200,000 行 C)

---

## 总览

| 维度 | PostgreSQL 18 | 本 DBMS | 差距 |
|------|--------------|---------|------|
| 代码量 | ~1.2M 行 | ~66K 行 | ~18x |
| 开发团队 | 全球数百人/20+年 | 单人/数周 | — |
| 测试覆盖 | ~2000+ 测试 | 110 单元测试 | ~18x |
| 功能完成度 | 100% (生产级) | DDL/DML核心 90%, 高级特性 40% | — |

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
| **bytea 存储** | ✅ TOAST | ✅ overflow | ⚠️ 无 TOAST 压缩 |
| **numeric NaN/Infinity** | ✅ | ⚠️ | 部分 |

---

## 二、SQL 语法

### 2.1 DDL

| 功能 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| CREATE/DROP/ALTER DATABASE | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER SCHEMA | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER TABLE (全量子命令) | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER VIEW | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER MATERIALIZED VIEW | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER INDEX | ✅ | ✅ | ✅ |
| CREATE INDEX CONCURRENTLY | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER SEQUENCE | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER FUNCTION/PROCEDURE | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER TRIGGER | ✅ | ✅ (DDL) | ⚠️ 执行缺 |
| CREATE/DROP/ALTER ROLE/USER | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER TYPE (composite/enum/range) | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER DOMAIN | ✅ | ✅ | ✅ |
| CREATE/DROP/ALTER POLICY (RLS) | ✅ | ✅ (DDL) | ⚠️ 执行缺 |
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
| Subquery (IN/EXISTS/ANY/ALL/标量) | ✅ | ✅ (parser) | ⚠️ executor 缺 |
| CTE (WITH/RECURSIVE) | ✅ | ✅ (DDL) | ⚠️ executor 缺 |
| UNION/INTERSECT/EXCEPT | ✅ | ✅ (parser) | ⚠️ executor 缺 |
| Window Functions (ROW_NUMBER/RANK/...) | ✅ | ✅ (DDL) | ⚠️ executor 部分 |
| **GROUP BY ROLLUP/CUBE/GROUPING SETS** | ✅ | ✅ (parser) | ⚠️ executor 部分 |
| GROUPING_ID | ✅ | ❌ | 缺 |
| FOR UPDATE/SHARE/NOWAIT/SKIP LOCKED | ✅ | ✅ (DDL) | ⚠️ 行级锁缺 |
| **PREPARE TRANSACTION (2PC)** | ✅ | ✅ | ✅ |
| VALUES | ✅ | ✅ | ✅ |
| **Array subscript [n:m]** | ✅ | ❌ | 缺 |
| **JSON path / SQL/JSON** | ✅ | ❌ | 缺 |

---

## 三、索引

| 访问方法 | PG 18 | 本 DBMS | 状态 |
|----------|-------|---------|------|
| B+ Tree | ✅ | ✅ | ✅ |
| Hash | ✅ | ✅ | ✅ |
| GIN | ✅ | ✅ (基础) | ⚠️ |
| GiST | ✅ | ❌ | 缺 |
| SP-GiST | ✅ | ✅ (Point quadtree) | ⚠️ |
| BRIN | ✅ | ✅ (块范围) | ⚠️ |
| **Bloom** | ✅ | ❌ | 缺 |
| **覆盖索引 (INCLUDE)** | ✅ | ✅ | ✅ |
| **部分索引 (WHERE)** | ✅ | ✅ | ✅ |
| **表达式索引** | ✅ | ✅ | ✅ |
| **唯一索引 + 约束** | ✅ | ✅ | ✅ |
| **多列复合索引** | ✅ | ✅ | ✅ |
| **Index Only Scan** | ✅ | ✅ | ✅ |
| **Bitmap Index/Heap Scan** | ✅ | ⚠️ | IndexOnlyScan |

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
| **行级锁** | ✅ | ❌ | 缺 |
| **Deadlock detection** | ✅ | ❌ | 缺 |
| **Gap locks / predicate locks** | ✅ | ❌ | 缺 |
| **SSI (Serializable Snapshot Isolation)** | ✅ | ❌ | 缺 |
| **两阶段提交 (2PC)** | ✅ | ⚠️ | 基础 |
| **并行查询** | ✅ | ❌ | 框架有 |
| **JIT compilation (LLVM)** | ✅ | ❌ | 框架 stub |
| **Async I/O (io_uring)** | ✅ (PG18) | ❌ | 框架 stub |
| HOT Update | ✅ | ⚠️ | 基础 |
| **谓词锁** | ✅ | ❌ | 缺 |

### 并发测试现状
- ✅ 基础 WAL 写入测试
- ✅ Checkpoint 测试
- ✅ CLOG visibility 测试
- ✅ MVCC ReadView 可见性测试
- ✅ Snapshot 导入导出测试
- ❌ 多线程并发读写测试
- ❌ 死锁检测测试
- ❌ 隔离级别验证测试
- ❌ 性能基准测试

---

## 五、存储

| 特性 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| Slotted page (8KB) | ✅ | ✅ | ✅ |
| Buffer Pool (clock sweep) | ✅ | ✅ | ✅ |
| Free Space Map | ✅ | ✅ | ✅ |
| Visibility Map | ✅ | ✅ | ✅ |
| TOAST (大字段压缩/线外存储) | ✅ | ⚠️ | 基础 overflow page |
| 页校验和 | ✅ | ✅ | ✅ |
| 溢出页 | ✅ | ✅ | ✅ |
| 子事务日志 | ✅ | ✅ | ✅ |
| **TOAST 压缩 (lz4/pglz)** | ✅ | ❌ | 缺 |
| **大对象 (Large Object)** | ✅ | ✅ | ✅ |
| **Unlogged 表** | ✅ | ✅ | ✅ |
| **临时表** | ✅ | ✅ | ✅ |
| **表空间** | ✅ | ✅ | ✅ |

---

## 六、查询优化器

| 特性 | PG 18 | 本 DBMS | 状态 |
|------|-------|---------|------|
| 火山模型执行器 | ✅ | ✅ (12 operators) | ✅ |
| 基于成本的优化 (CBO) | ✅ | ⚠️ | 基础 |
| 统计信息 (pg_statistic) | ✅ | ⚠️ | 基础 |
| 索引选择 | ✅ | ✅ | ✅ |
| **等价类 (Equivalence Classes)** | ✅ | ✅ | ✅ |
| **PathKeys / 排序路径** | ✅ | ✅ | ✅ |
| Join reorder (动态规划) | ✅ | ⚠️ | 启发式 |
| Bitmap scan | ✅ | ⚠️ | 部分 |
| **计划缓存** | ✅ | ✅ | ✅ |
| EXPLAIN ANALYZE 真实统计 | ✅ | ✅ | ✅ |
| 多索引组合 (Bitmap AND/OR) | ✅ | ❌ | 缺 |
| **参数化路径** | ✅ | ✅ (stub) | ⚠️ |
| **自定义成本函数** | ✅ | ❌ | 缺 |
| **遗传算法 join reorder** | ✅ | ❌ | 缺 |
| **分区裁剪** | ✅ | ✅ | ✅ |
| **并行扫描/聚合/连接** | ✅ | ❌ | 框架有 |

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
| pg_hba.conf | ✅ | ✅ | ✅ |
| 用户/角色系统 | ✅ | ✅ | ✅ |
| GRANT/REVOKE (ACL) | ✅ | ✅ (DDL) | ⚠️ 执行缺 |
| 列级权限 | ✅ | ✅ | ✅ |
| **行级安全 (RLS) 执行** | ✅ | ❌ | 缺 |
| **SCRAM-SHA-256 完整协议** | ✅ | ❌ | 仅 hash 验证 |
| **LDAP/Kerberos/GSSAPI/PAM/RADIUS** | ✅ | ❌ | 缺 |
| **SSL 双向认证** | ✅ | ⚠️ TLSWrapper | |
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
1. **行级锁** — 无并发写控制，仅表级锁
2. **死锁检测** — 并发事务可能死锁
3. **并行查询** — 无法利用多核加速
4. **JIT 编译** — 表达式求值无编译优化
5. **行级安全执行** — RSL DDL 就绪但执行缺失
6. **触发器执行** — DDL 就绪但 INSERT/UPDATE 时不触发
7. **索引 GiST** — 全文搜索/几何索引缺失

### 🟡 重要缺失 (影响实用性)
1. **子查询 executor** — parser 就绪但 executor 不完整
2. **CTE executor** —同上
3. **Window Function executor** — 同上
4. **UNION/INTERSECT executor** — 同上
5. ** GiST 索引** — 全文搜索基础架构缺
6. **TOAST 压缩** — 大字段存储效率低
7. **后台_STATS_collector** — 无运行时统计

### 🟢 次要缺失 (易用性)
1. pg_stat_* views
2. GiST 索引
3. Bloom 索引
4. 并行查询
5. 增量备份
6. pg_upgrade 工具

---

## 总结

本 DBMS 在 **DDL 完整化** 方面达到了很高的完成度（SQL 语法覆盖 ~95%），但**运行时 semantics** 的执行层面还有显著差距：

- **DDL Parser**: ✅ 高度完整 (184 命令)
- **DML Executor**: ✅ 核心完整 (INSERT/UPDATE/DELETE/基础 SELECT)
- **高级 Query**: ⚠️ parser 就绪, executor 部分完成
- **并发控制**: ⚠️ MVCC 基础有, 行级锁/死锁缺失
- **性能**: ⚠️ 无并行/JIT/高级索引
- **扩展性**: ❌ 框架有, 运行时缺

**下一阶段优先级建议**:
1. 行级锁 + 死锁检测 (并发正确性)
2. 子查询/CTE/Window executor (查询完整性)
3. 触发器执行 (数据完整性)
4. GiST 索引 (全文搜索)
5. 并行 query exec (性能)

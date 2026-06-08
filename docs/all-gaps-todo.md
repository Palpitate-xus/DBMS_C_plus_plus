# DBMS 全部 PostgreSQL 差距补齐 TODO

> 生成日期：2026-06-03
> 来源：gap-vs-postgresql.md + 测试报告
> 原则：按**紧急程度 → 架构深度 → 功能广度**推进，每完成一批就 commit

---

## 进度总览

| 批次 | 功能域 | 状态 | Commit |
|------|--------|------|--------|
| Batch 0 | GiST/GIN/BRIN 索引 | ✅ 完成 | 4 commits |
| Batch 1 | 会话上下文 + 全局变量清理 | ✅ 已完成 | 已实现 |
| Batch 2 | 真正的 MVCC + ReadView | ✅ 已完成 | 已实现 |
| Batch 3 | Hash Join + Merge Join | ✅ 已完成 | 已实现 |
| Batch 4 | VARCHAR 变长行 + 溢出页 | ✅ 已完成 | 已实现 |
| Batch 5 | Checkpoint + fsync | ✅ 已完成 | 已实现 |
| Batch 6 | 剩余 TOP20 gaps | ⏳ 待开始 | — |
| Batch 7 | 其他功能缺失 | ⏳ 待开始 | — |

---

## Batch 1: 会话上下文（P0 - 安全漏洞）

当前 `g_nowUser`、`g_nowPermission`、`g_currentDB`、`g_preparedStmts` 是全局变量。
NetworkServer 多客户端并发时互相覆盖，是严重安全漏洞。

- [x] Step 1.1: 新增 `struct Session`，`execute()` 签名改为接收 Session 引用
- [x] Step 1.2: `checkAdmin()` / `checkTablePermission()` / `log()` 改为接收 Session
- [x] Step 1.3: NetworkServer 每连接创建独立 Session，登录后写入 Session
- [x] Step 1.4: main() 交互模式也使用 Session 对象
- [x] Step 1.5: 删除全局变量 `g_nowUser`、`g_nowPermission`、`g_currentDB`、`g_preparedStmts`
- [x] Step 1.6: 验证 `git grep g_nowUser` / `git grep g_currentDB` 无残留

**修改文件**: `main.cpp`, `NetworkServer.h/cpp`, `Session.h`

---

## Batch 2: 真正的 MVCC + ReadView（P0 - 核心架构）

当前 Undo Log 只做 ROLLBACK 恢复。多个事务并发时，未提交的修改对其他事务可见（没有隔离）。

- [x] Step 2.1: 新增 `TxnIdGenerator.h/cpp`，64 位单调递增 txId，持久化到 `.txnid` 文件
- [x] Step 2.2: 行格式扩展（已有 16 字节 MVCC header），`TableSchema::rowSize()` 增加 16 字节
- [x] Step 2.3: `StorageEngine` 新增活跃事务管理 `globalTxnMutex_` + `activeTransactions_`
- [x] Step 2.4: 实现 `ReadView` 结构和可见性规则
- [x] Step 2.5: `beginTransaction` 分配 txId 并注册到活跃集合，`commit/rollback` 移除
- [x] Step 2.6: `forEachRow` 增加 `const ReadView*` 参数，传入时跳过不可见行
- [x] Step 2.7: `filterRows`、`query`、`aggregate`、`join` 传递 ReadView
- [x] Step 2.8: 验证：BEGIN → INSERT → 同事务 SELECT 能看到；另一连接 SELECT 看不到；COMMIT 后能看到

**修改文件**: `TxnIdGenerator.h/cpp`, `TableManage.h/cpp`, `ExecutionPlan.cpp`

---

## Batch 3: Hash Join + Merge Join（P1 - 查询优化）

当前 JOIN 只有 NestedLoopJoin，大数据量时性能灾难。

- [x] Step 3.1: `ExecutionPlan.h/cpp` 新增 `HashJoinOp` 算子（右表驻内存哈希表）
- [x] Step 3.2: `ExecutionPlan.h/cpp` 新增 `MergeJoinOp` 算子（双指针线性扫描）
- [x] Step 3.3: `buildJoinPlan` 根据统计信息自动选择 JOIN 算法
  - 小表（< 100 行）→ NestedLoopJoin
  - 两表都大，无序 → HashJoin
  - 两表都大，join key 有索引/已排序 → MergeJoin
- [x] Step 3.4: EXPLAIN 输出显示使用了哪种 JOIN 算法

**修改文件**: `ExecutionPlan.h/cpp`

---

## Batch 4: VARCHAR / 变长行 + 溢出页（P1 - 存储格式）

当前全表定长行，存储浪费严重，无法存长文本。

- [x] Step 4.1: `Column` 增加 `isVariableLength` 标志，`makeVarCharColumn()` 构造函数
- [x] Step 4.2: CREATE TABLE 解析支持 `VARCHAR(n)` 语法（已部分支持，需验证）
- [x] Step 4.3: `rowSize()` 改为变长行计算（定长部分 + 变长偏移数组 + 变长数据）
- [x] Step 4.4: `extractColumnValue` 根据列类型选择定长/变长解析路径
- [x] Step 4.5: 溢出页处理：单行数据 > 页可用空间时，大字段存溢出页
- [x] Step 4.6: 主行中存溢出页指针（pageId + offset）
- [x] Step 4.7: get/update 时自动处理溢出页读写
- [x] Step 4.8: 验证：插入 10KB 文本，能正确存储和读取

**修改文件**: `TableManage.h/cpp`, `Page.cpp`, `PageAllocator.cpp`, `main.cpp`

---

## Batch 5: Checkpoint + fsync（P2 - 持久化加固）

当前 WAL 日志无限增长，崩溃恢复时间随运行时长线性增加；没有 fsync 保证可能丢数据。

- [x] Step 5.1: `TableManage.cpp` 新增 `checkpoint()` 方法
- [x] Step 5.2: Checkpoint 记录: `(checkpointLsn, dirtyPageList, activeTxnIds)`
- [x] Step 5.3: 调用时：脏页刷盘 → 写入 checkpoint 记录 → 截断 WAL
- [x] Step 5.4: StorageEngine 构造函数读取最新 checkpoint，从 checkpoint 位置开始恢复
- [x] Step 5.5: `BufferPool::flush()` 中对每个脏页调用 `fdatasync()` / `fsync()`
- [x] Step 5.6: `writeFileHeader` 后调用 `fsync()`
- [x] Step 5.7: `commitTransaction` 时先 fsync WAL，再标记事务提交
- [x] Step 5.8: 交互模式下每 N 条 SQL 或每 M 秒触发 checkpoint
- [x] Step 5.9: `CHECKPOINT` SQL 命令支持手动触发
- [x] Step 5.10: 验证：模拟崩溃（kill -9）后，已 COMMIT 的数据不丢失

**修改文件**: `TableManage.h/cpp`, `BufferPool.cpp`, `main.cpp`

---

## Batch 6: 剩余 TOP20 Gaps

TOP20 当前完成度 14/20，剩余 6 项：

| # | 功能 | 难度 | 计划 |
|---|------|------|------|
| 2 | **并行查询** | 高 | 需要执行计划并行化 + 多线程结果合并 |
| 5 | **流复制 / 逻辑复制** | 高 | 需要 WAL 解析 + 网络传输协议 |
| 6 | **PITR（时间点恢复）** | 高 | 依赖 Checkpoint + 连续 WAL 归档 |
| 8 | **PL/pgSQL 过程语言** | 高 | 需要词法/语法分析器 + 字节码 VM |
| 9 | **EXTENSION + FDW** | 高 | 需要动态加载机制 + 外部数据源适配器 |
| 15 | **SCRAM 认证 / pg_hba.conf** | 中 | 替换现有 SHA256 认证，增加配置解析 |

**建议**：Batch 6 放在 Batch 1-5 之后实施，因为：
- 并行查询需要稳定的执行计划框架
- 流复制/PITR 需要稳定的 Checkpoint + WAL
- PL/pgSQL 是独立的大模块

---

## Batch 7: 其他功能缺失（中低优先级）

### DDL 增强
- [x] ALTER TABLE RENAME COLUMN
- [x] ALTER TABLE RENAME TO
- [x] ALTER TABLE SET SCHEMA
- [x] ALTER TABLE ALTER COLUMN SET/DROP DEFAULT
- [x] ALTER TABLE ALTER COLUMN SET/DROP NOT NULL
- [x] ALTER TABLE ADD/DROP CONSTRAINT
- [x] ALTER TABLE ENABLE/DISABLE TRIGGER
- [x] CREATE TABLE AS SELECT
- [x] CREATE TABLE ... PARTITION OF（声明式分区独立语法）
- [x] SUBPARTITION（子分区）
- [x] DEFAULT PARTITION
- [x] TRUNCATE ... CASCADE / RESTART IDENTITY
- [x] COMMENT ON

### 数据类型
- [x] SERIAL4 / BIGSERIAL
- [x] GENERATED AS IDENTITY
- [ ] 范围类型 (int4range, tsrange 等)
- [x] 几何类型 - POINT（含空间运算符 << >> <^ >^ <@）
- [x] 网络类型 - INET/CIDR（IPv4，含 << >> && 运算符）
- [x] XML
- [x] pg_lsn
- [ ] tsvector / tsquery（PG 风格全文搜索类型）
- [x] 组合类型 (ROW 类型)
- [x] DOMAIN (CREATE DOMAIN)
- [x] 自定义类型 (CREATE TYPE)

### 约束
- [ ] DEFERRABLE / INITIALLY DEFERRED
- [ ] EXCLUSION CONSTRAINTS
- [ ] CREATE ASSERTION

### 索引
- [x] CREATE INDEX CONCURRENTLY
- [x] SP-GiST 索引

### DML
- [x] INSERT INTO t1 VALUES (...)（省略列名）
- [x] COPY ... FROM/TO（批量 COPY 协议）

### 查询优化器
- [x] 统计信息自动收集（auto-analyze，基于修改计数阈值触发）
- [ ] 直方图 / 相关性统计
- [ ] 子查询内联 / 物化决策优化
- [ ] 并行顺序扫描 / 并行索引扫描
- [ ] 并行 Gather / Gather Merge 节点
- [ ] JIT 编译 (LLVM)

### 事务/MVCC
- [x] SET TRANSACTION READ ONLY（只读事务显式声明）
- [ ] 子事务（SAVEPOINT 已支持，需完善嵌套）
- [x] 两阶段提交 PREPARED / ROLLBACK PREPARED（已完整支持）
- [x] LOCK TABLE 命令
- [x] Advisory Locks (pg_advisory_lock)
- [x] ALTER TABLE ... ENABLE ROW LEVEL SECURITY（FORCE 已支持）

### 存储引擎
- [ ] TABLESPACE 支持
- [ ] 存储参数 (fillfactor, toast.* 等)
- [ ] 压缩 (页级/行级)
- [ ] 透明数据加密 (TDE)

### 视图
- [x] INSTEAD OF 触发器（视图可更新）
- [x] WITH LOCAL/CASCADED CHECK OPTION
- [x] MATERIALIZED VIEW CONCURRENTLY（增量刷新）

### 触发器
- [x] 触发器诊断变量 (tg_name, tg_when, tg_level, tg_op, tg_relname)
- [x] WHEN 子句 (条件触发)
- [ ] 复合触发器
- [ ] Event Triggers

### 存储过程/函数
- [ ] PL/pgSQL 过程语言
- [ ] 游标 (DECLARE)
- [ ] 控制流 (IF/WHILE/LOOP/FOR)
- [ ] 异常处理 (BEGIN ... EXCEPTION)
- [ ] 自定义聚合函数 (UDAF)
- [ ] SECURITY DEFINER / INVOKER
- [ ] 外部语言 (plpython, plperl 等)

### 安全与权限
- [ ] SCRAM-SHA-256 认证
- [ ] LDAP / Kerberos 认证
- [ ] SSL 证书认证
- [ ] RADIUS 认证
- [ ] pg_hba.conf 风格访问控制
- [x] GRANT ON SCHEMA
- [x] GRANT ON SEQUENCE
- [x] GRANT ON FUNCTION
- [x] REVOKE CASCADE
- [x] DEFAULT PRIVILEGES
- [x] SECURITY LABEL
- [x] 行级安全 (RLS) 强制模式
- [x] REASSIGN OWNED / DROP OWNED

### 复制与高可用
- [ ] 物理流复制 (Streaming Replication)
- [ ] 逻辑复制 (Logical Replication)
- [ ] PUBLICATION / SUBSCRIPTION
- [ ] 逻辑解码 (Logical Decoding)
- [ ] 复制槽 (Replication Slot)
- [ ] 同步提交控制 (synchronous_commit)
- [ ] 级联复制 / 热备

### 备份与恢复
- [ ] 基础备份 (pg_basebackup 等价)
- [ ] PITR（时间点恢复）
- [ ] WAL 归档 / recovery.signal
- [ ] 增量备份
- [ ] 表级/数据库级备份（DUMP/RESTORE 已支持）

### 监控与诊断
- [x] pg_stat_statements 等价
- [ ] Wait Events
- [x] pg_buffercache 等价
- [ ] 锁等待拓扑统计
- [x] 慢查询日志（文件 + 内存缓冲区 SHOW SLOW LOG）
- [x] auto_explain

### 扩展性
- [ ] CREATE EXTENSION
- [ ] FDW (postgres_fdw 等)
- [ ] 自定义扫描节点
- [ ] 自定义 WAL 资源管理器
- [ ] 后台工作进程 (bgworker)
- [ ] 共享内存扩展
- [ ] 自定义索引访问方法
- [ ] 自定义类型输入/输出函数

### 全文搜索
- [ ] PG 风格全文搜索 (tsvector/tsquery)
- [ ] 文本搜索配置
- [ ] 同义词词典

### 高级特性
- [ ] 图查询 (SQL/PGQ, AGE)
- [ ] 表继承 (INHERITS)
- [ ] COPY 协议（客户端批量传输）
- [ ] Large Objects (lo_*)
- [ ] 外部表 (非 FDW 方式)
- [ ] 规则系统 (RULE)

### 系统管理
- [x] ALTER SYSTEM（postgresql.conf 动态修改）
- [x] pg_reload_conf()
- [x] pg_cancel_backend / pg_terminate_backend
- [ ] 连接池管理
- [x] VACUUM CONCURRENTLY

---

## 实施建议

### 推荐顺序

1. **Batch 1**（会话上下文）— 安全漏洞，无依赖，立即实施
2. **Batch 2**（MVCC + ReadView）— 核心架构，依赖 Batch 1
3. **Batch 3**（Hash/Merge Join）— 查询优化，无存储层依赖
4. **Batch 4**（VARCHAR / 变长行）— 存储格式大改，风险高
5. **Batch 5**（Checkpoint + fsync）— 持久化加固，依赖 Batch 4 格式稳定
6. **Batch 6**（剩余 TOP20）— 大模块，逐个突破
7. **Batch 7**（其他缺失）— 按需补充

### Commit 节奏

- 每个 Step 完成后编译通过就 commit
- 预计 Batch 1-5 共约 **15-20 个 commits**
- Batch 6 每个 TOP20 gap 约 **3-5 个 commits**
- Batch 7 按需 commit

---

*最后更新：2026-06-08*

# DBMS 全部 Gap TODO（统一来源）

> 生成日期：2026-06-10
> 来源：合并 postgresql-complete-gap-analysis.md + todos.md + feature-gap-analysis.md + gap-vs-postgresql.md
> 原则：本文件为唯一 TODO 来源，所有 gap 状态以此为准

---

## 总览统计

| 类别 | 已完成 | 剩余 | 备注 |
|------|--------|------|------|
| 编译/构建 | 3 | 0 | 零警告 + TLS 条件编译 + build.sh |
| P0 阻塞性 | 5 | 0 | 全部完成 |
| P1 核心功能 | 26 | 0 | 全部完成 |
| P2 增强功能 | 44 | ~8 | 大部分完成 |
| P3 高级特性 | 0 | 40+ | 架构级大模块，需长期投入 |
| P4 暂不列入 | — | 11 | 生态/极难实现，观望 |

---

## ✅ 已完成（按批次）

### 批次 0：GiST / GIN / BRIN 索引（4 commits）
- [x] GiST 索引（简化空间/范围索引）
- [x] GIN 索引（倒排索引，text/json/array）
- [x] BRIN 索引（块范围摘要）
- [x] SP-GiST 索引（四叉树空间索引）

### 批次 1：会话上下文（P0 - 安全漏洞）
- [x] `struct Session` 替换全局变量
- [x] `execute()` 签名接收 Session 引用
- [x] `checkAdmin()` / `checkTablePermission()` / `log()` 改为接收 Session
- [x] NetworkServer 每连接创建独立 Session
- [x] `main()` 交互模式使用 Session 对象
- [x] 删除 `g_nowUser`、`g_nowPermission`、`g_currentDB`、`g_preparedStmts`

### 批次 2：真正的 MVCC + ReadView（P0 - 核心架构）
- [x] `TxnIdGenerator` 64 位单调递增 txId
- [x] 行格式扩展（16 字节 MVCC header）
- [x] `StorageEngine` 活跃事务管理
- [x] `ReadView` 结构和可见性规则
- [x] `beginTransaction` 分配 txId，`commit/rollback` 移除
- [x] `forEachRow` 增加 `const ReadView*` 参数
- [x] `filterRows`、`query`、`aggregate`、`join` 传递 ReadView
- [x] 验证：BEGIN → INSERT → 同事务可见，其他连接不可见，COMMIT 后可见

### 批次 3：Hash Join + Merge Join（P1 - 查询优化）
- [x] `HashJoinOp` 算子
- [x] `MergeJoinOp` 算子
- [x] `buildJoinPlan` 自动选择 JOIN 算法
- [x] EXPLAIN 输出显示 JOIN 算法

### 批次 4：VARCHAR / 变长行 + 溢出页（P1 - 存储格式）
- [x] `Column` 增加 `isVariableLength` 标志
- [x] `VARCHAR(n)` 语法解析
- [x] `rowSize()` 变长行计算
- [x] `extractColumnValue` 定长/变长解析路径
- [x] 溢出页处理（大字段存溢出页）
- [x] 主行中存溢出页指针
- [x] get/update 自动处理溢出页读写

### 批次 5：Checkpoint + fsync（P2 - 持久化加固）
- [x] `checkpoint()` 方法
- [x] Checkpoint 记录 `(checkpointLsn, dirtyPageList, activeTxnIds)`
- [x] 脏页刷盘 → checkpoint 记录 → 截断 WAL
- [x] 构造函数读取 checkpoint 恢复
- [x] `BufferPool::flush()` 调用 `fsync()`
- [x] `writeFileHeader` 后 `fsync()`
- [x] `commitTransaction` 先 fsync WAL
- [x] 交互模式自动触发 checkpoint
- [x] `CHECKPOINT` SQL 命令

### 批次 6：TOP20 剩余 Gap
- [x] **ALTER ROLE RENAME** 支持用户（本次新增 `renameUser()`）
- [x] **GiST / GIN / BRIN 索引**（Batch 0）
- [x] **NOTIFY / LISTEN**（已实现）
- [x] **TRUNCATE TABLE**（已实现）
- [x] **两阶段提交** PREPARE TRANSACTION / COMMIT PREPARED（已实现）
- [x] **函数重载**（已实现）
- [x] **WITH CHECK OPTION**（已实现）
- [x] **GRANT WITH GRANT OPTION**（已实现）
- [x] **窗口帧 RANGE/GROUPS BETWEEN**（已实现）
- [x] **EXPLAIN 选项**（ANALYZE, BUFFERS, TIMING, COSTS, SETTINGS, VERBOSE）

### 批次 7：其他功能缺失
- [x] ALTER TABLE RENAME COLUMN / RENAME TO / SET SCHEMA
- [x] ALTER TABLE ALTER COLUMN SET/DROP DEFAULT / SET/DROP NOT NULL
- [x] ALTER TABLE ADD/DROP CONSTRAINT
- [x] ALTER TABLE ENABLE/DISABLE TRIGGER
- [x] CREATE TABLE AS SELECT
- [x] CREATE TABLE ... PARTITION OF / PARTITION BY
- [x] SUBPARTITION（子分区）
- [x] DEFAULT PARTITION
- [x] TRUNCATE ... CASCADE / RESTART IDENTITY
- [x] COMMENT ON
- [x] SERIAL4 / BIGSERIAL / GENERATED AS IDENTITY
- [x] 范围类型 (int4range, int8range, numrange, tsrange, tstzrange, daterange)
- [x] 几何类型 - POINT（含空间运算符）
- [x] 网络类型 - INET/CIDR（含 IPv6 格式化，<< >> && 运算符）
- [x] XML / pg_lsn / tsvector / tsquery（目录级列存取）
- [x] 组合类型 (ROW 类型)
- [x] DOMAIN (CREATE DOMAIN)
- [x] 自定义类型 (CREATE TYPE)
- [x] DEFERRABLE / INITIALLY DEFERRED（元数据登记）
- [x] EXCLUSION CONSTRAINTS（元数据登记）
- [x] CREATE ASSERTION（目录级登记）
- [x] CREATE INDEX CONCURRENTLY
- [x] SP-GiST 索引
- [x] INSERT INTO t1 VALUES (...)（省略列名）
- [x] COPY ... FROM/TO（批量 CSV 协议）
- [x] 统计信息自动收集（auto-analyze）
- [x] SET TRANSACTION READ ONLY
- [x] 子事务（SAVEPOINT 嵌套）
- [x] 两阶段提交 PREPARED / ROLLBACK PREPARED
- [x] LOCK TABLE 命令
- [x] Advisory Locks (pg_advisory_lock)
- [x] ALTER TABLE ... ENABLE ROW LEVEL SECURITY（FORCE 已支持）
- [x] TABLESPACE 支持（元数据命令）
- [x] 存储参数 (fillfactor, autovacuum_enabled 等，WITH 子句 + .params 文件)
- [x] INSTEAD OF 触发器（视图可更新）
- [x] WITH LOCAL/CASCADED CHECK OPTION
- [x] MATERIALIZED VIEW CONCURRENTLY（增量刷新标志）
- [x] 触发器诊断变量 (tg_name, tg_when, tg_level, tg_op, tg_relname)
- [x] WHEN 子句 (条件触发)
- [x] Event Triggers（目录级登记）
- [x] GRANT ON SCHEMA / SEQUENCE / FUNCTION
- [x] REVOKE CASCADE
- [x] DEFAULT PRIVILEGES
- [x] SECURITY LABEL
- [x] 行级安全 (RLS) 强制模式
- [x] REASSIGN OWNED / DROP OWNED
- [x] ALTER SYSTEM（postgresql.conf 动态修改）
- [x] pg_reload_conf()
- [x] pg_cancel_backend / pg_terminate_backend
- [x] 连接池管理（SHOW CONNECTIONS / SHOW STATUS）
- [x] pg_settings / pg_roles / pg_namespace / pg_database / pg_tables / pg_indexes 虚拟表
- [x] VACUUM CONCURRENTLY
- [x] 创建 `DO` 简化 SQL block 执行
- [x] `LOAD 'library'` 目录登记
- [x] `SELECT ... INTO [TEMP|UNLOGGED] table` 到 CTAS 转换
- [x] `.pg_compat_objects` 目录级兼容对象表
- [x] `ALTER DATABASE` 支持实际目录 rename
- [x] `ALTER POLICY` 支持 rename/roles/using/with check 真实改写
- [x] `ALTER DOMAIN` 真实改写 .domains
- [x] `ALTER TYPE` 支持 composite type rename/attribute 更新

### 本次新增完成（2026-06-10）
- [x] **编译零警告**：清理 30+ 处未使用变量/参数/函数，修复 range-loop-construct，添加缺失初始化器
- [x] **TLS 构建系统**：CMake + build.sh 自动检测 OpenSSL，条件编译 TLSWrapper
- [x] **ALTER ROLE RENAME for users**：新增 `renameUser()`，支持用户重命名
- [x] **IPv6 支持**：inet/cidr 类型新增 IPv6 地址格式化输出
- [x] **空目录 README**：executor / parser / replication / catalog 补充状态说明
- [x] **接口文件注释**：storage_engine.h / executor.h / index_am.h 补充迁移状态

---

## ⏳ 待办（按优先级）

### P2：增强功能（短期可补）

| # | 功能 | 难度 | 说明 |
|---|------|------|------|
| 1 | 直方图 / 相关性统计 | 中 | 优化器统计增强 |
| 2 | 子查询内联 / 物化决策优化 | 中 | CTE MATERIALIZED/NOT MATERIALIZED |
| 3 | 压缩 (页级/行级) | 中 | LZ4/Zstd 页压缩 |
| 4 | 透明数据加密 (TDE) | 中高 | 页级 AES 加密 |
| 5 | 复合触发器 | 中 | Compound Trigger |
| 6 | SECURITY DEFINER / INVOKER | 中 | 函数执行权限 |
| 7 | 自定义聚合函数 (UDAF) | 中 | CREATE AGGREGATE |
| 8 | 外部表 (非 FDW 方式) | 低 | 简单外部数据源读取 |

### P3：高级特性（架构级大模块）

#### 查询优化器
| # | 功能 | 难度 | 说明 |
|---|------|------|------|
| 9 | 并行查询计划 | 极高 | Gather/Gather Merge、并行扫描/Join/聚合 |
| 10 | 并行 Hash Join / 并行索引扫描 | 极高 | 需 worker 生命周期管理 |
| 11 | JIT 编译 (LLVM) | 极高 | 表达式 JIT |

#### 存储过程/PL
| # | 功能 | 难度 | 说明 |
|---|------|------|------|
| 12 | PL/pgSQL 过程语言 | 极高 | 需词法/语法分析器 + 字节码 VM |
| 13 | 游标完善 (DECLARE) | 中 | 可滚动/二进制/holdable cursor |
| 14 | 流程控制 (IF/WHILE/LOOP/FOR) | 高 | 过程语言控制流 |
| 15 | 异常处理 (BEGIN ... EXCEPTION) | 高 | PL/pgSQL 风格异常 |
| 16 | 外部语言 (plpython, plperl) | 极高 | 安全沙箱极难实现 |

#### 安全与认证
| # | 功能 | 难度 | 说明 |
|---|------|------|------|
| 17 | SCRAM-SHA-256 认证 | 中 | 替换现有 SHA256 |
| 18 | LDAP / Kerberos / GSSAPI 认证 | 高 | 需外部库集成 |
| 19 | SSL 证书认证 | 中 | 客户端证书验证 |
| 20 | RADIUS 认证 | 中 | 企业级认证 |
| 21 | pg_hba.conf 风格访问控制 | 中高 | Host-Based Authentication |

#### 复制与高可用
| # | 功能 | 难度 | 说明 |
|---|------|------|------|
| 22 | 物理流复制 (Streaming Replication) | 极高 | WAL 流式发送 + 备库回放 |
| 23 | 逻辑复制 (Logical Replication) | 极高 | Publication/Subscription |
| 24 | PUBLICATION / SUBSCRIPTION | 极高 | 基于表级别的变更捕获 |
| 25 | 逻辑解码 (Logical Decoding) | 极高 | WAL 解析为逻辑变更 |
| 26 | 复制槽 (Replication Slot) | 高 | 防止 WAL 被过早删除 |
| 27 | 同步提交控制 (synchronous_commit) | 高 | 同步/异步复制切换 |
| 28 | 级联复制 / 热备 | 极高 | 多层复制 + 热备只读 |

#### 备份与恢复
| # | 功能 | 难度 | 说明 |
|---|------|------|------|
| 29 | 基础备份 (pg_basebackup) | 高 | 一致性物理备份 |
| 30 | PITR（时间点恢复） | 极高 | 依赖 Checkpoint + 连续 WAL 归档 |
| 31 | WAL 归档 / recovery.signal | 高 | 连续归档恢复 |
| 32 | 增量备份 | 高 | 块级增量备份 |

#### 扩展性
| # | 功能 | 难度 | 说明 |
|---|------|------|------|
| 33 | CREATE EXTENSION | 极高 | 插件加载框架 |
| 34 | FDW (postgres_fdw 等) | 极高 | 外部数据包装器 |
| 35 | 自定义扫描节点 | 高 | 扩展 planner/executor |
| 36 | 自定义 WAL 资源管理器 | 极高 | 扩展 WAL 记录类型 |
| 37 | 后台工作进程 (bgworker) | 极高 | 自定义后台任务 |
| 38 | 共享内存扩展 | 高 | 扩展共享内存使用 |
| 39 | 自定义索引访问方法 | 极高 | Access Method API |
| 40 | 自定义类型输入/输出函数 | 高 | Base type 完整语义 |

#### 全文搜索
| # | 功能 | 难度 | 说明 |
|---|------|------|------|
| 41 | PG 风格全文搜索运行时 | 高 | tsvector/tsquery parser |
| 42 | 文本搜索配置 | 高 | 配置/词典/parser/template |
| 43 | 同义词词典 | 中 | 中文分词等 |

#### 高级特性
| # | 功能 | 难度 | 说明 |
|---|------|------|------|
| 44 | 图查询 (SQL/PGQ, AGE) | 极高 | Cypher 解析器 + 图遍历 |
| 45 | 表继承完善 | 中 | ONLY/NO INHERIT 完整语义 |
| 46 | COPY 协议（客户端批量传输） | 高 | Binary COPY |
| 47 | Large Objects (lo_*) | 中 | 大对象存储 |
| 48 | 规则系统 (RULE) | 高 | CREATE/DROP RULE |

### P4：暂不列入（生态/极难实现）

| 功能 | 理由 |
|------|------|
| PostGIS 风格空间扩展 | 需要完整 GIS 库（GEOS/PROJ/GDAL），体量等于本 DBMS |
| TimescaleDB 风格时序 | 需要超表 + 连续聚合 + 压缩策略，独立项目级别 |
| Apache AGE 风格图查询 | Cypher 解析器 + 图遍历引擎，独立项目级别 |
| MPP 大规模并行处理 | 架构级变更，不现实 |
| 自定义过程语言 (plpython/plperl) | 安全沙箱极难实现 |
| 几何类型 (POLYGON/LINE/CIRCLE) | 不引入 GIS 则意义有限 |
| MACADDR 类型 | 小众需求 |
| 范围类型完整语义 | 需排他约束基础设施完善 |

---

## 架构级根本差距（详见 postgresql-complete-gap-analysis.md）

以下差距不是单个功能点，而是系统级架构差异，需要整体重构：

1. **Parser/AST**：当前 `execute()` 是巨大字符串分发器，不是完整 SQL grammar 管线
2. **Catalog/OID**：没有完整 `pg_class`、`pg_attribute`、`pg_type`、`pg_proc`、`pg_depend`
3. **WAL redo**：当前 WAL 不是 redo log，缺少 LSN、segment、full page writes、redo routines
4. **MVCC 版本链**：只有 creator txid，缺少 `xmin/xmax`、ctid chain、HOT update
5. **DDL 事务化**：多处 DDL 隐式提交，与 PG 事务语义不一致
6. **PostgreSQL Wire Protocol**：不是 PG 协议，客户端仅文本登录
7. **扩展系统**：没有插件加载框架、Hook 系统、共享内存扩展

> **建议**：若目标是"更像 PostgreSQL"，需按以下顺序推进架构级重构：
> 1. Parser/AST → 2. Catalog → 3. WAL/MVCC → 4. DDL 事务化 → 5. 类型系统 → 6. Planner → 7. Wire Protocol → 8. 复制/PITR → 9. 扩展/FDW/PL

---

## Commit 节奏建议

| 阶段 | 范围 | 预估 commits |
|------|------|-------------|
| P2 补充 | 直方图/压缩/TDE/复合触发器/SECURITY DEFINER | 5-8 |
| P3 安全 | SCRAM/pg_hba | 3-5 |
| P3 存储过程 | PL/pgSQL 最小可行版本 | 10-15 |
| P3 复制 | 流复制基础框架 | 15-20 |
| P3 备份 | PITR 基础 | 10-15 |
| P3 扩展 | EXTENSION/FDW 框架 | 15-20 |

---

*最后更新：2026-06-10*
*关联文档：*
- [postgresql-complete-gap-analysis.md](postgresql-complete-gap-analysis.md) — 最详细的差距分析（不可删除）
- [test-report.md](test-report.md) — 功能测试报告
- [commandsList.md](commandsList.md) — 支持的 SQL 命令列表

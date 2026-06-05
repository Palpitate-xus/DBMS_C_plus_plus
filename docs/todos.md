# DBMS_C_plus_plus 开发 TODO 路线图

> 基于 [gap-vs-postgresql.md](./gap-vs-postgresql.md) 差距分析制定
> 更新日期：2026-06-04
> 影响文件：`main.cpp`（SQL 解析/命令分发）、`TableManage.cpp/.h`（存储引擎）、`ExecutionPlan.cpp/.h`（优化器）、其它模块

---

## 优先级说明

| 级别 | 含义 | 目标时间 |
|------|------|----------|
| **P0** | 阻塞性 Bug / 生产可用性底线 | 立即修复 |
| **P1** | 核心功能增强，提升独立性 | 短期（1-3 月） |
| **P2** | 显著提升实用性和用户体验 | 中期（3-6 月） |
| **P3** | 高级/企业级特性 | 长期（6+ 月） |
| **P4** | 生态/极难实现，暂不列入 | 观望 |

---

## P0: 阻塞性 Bug 与关键修复（全部完成 ✅）

| # | 任务 | 状态 | 说明 |
|---|------|------|------|
| 1 | **修复 `CREATE HASH INDEX` 解析器 Bug** | ✅ | `CREATE HASH INDEX name ON table(col)` 原生语法已支持 |
| 2 | **实现 `TRUNCATE TABLE`** | ✅ | 支持 CASCADE、RESTART IDENTITY |
| 3 | **修复 `SHOW USERS` / `SHOW ROLES`** | ✅ | `SHOW USERS` 读取 `user.dat`，`SHOW ROLES` 读取 `role.dat`（需 admin 权限） |
| 4 | **实现 `IS DISTINCT FROM`** | ✅ | NULL-safe 比较运算符 |
| 5 | **实现 `FETCH FIRST ... ROWS ONLY`** | ✅ | SQL:2008 标准分页语法，等价于 `LIMIT n` |

---

## P1: 核心功能增强（短期）

### P1-1: DDL 完善（全部完成 ✅）

| # | 任务 | 状态 | 说明 |
|---|------|------|------|
| 6 | **实现 `ALTER TABLE RENAME COLUMN / RENAME TO`** | ✅ | 已实现 |
| 7 | **实现 `ALTER TABLE ALTER COLUMN SET/DROP DEFAULT`** | ✅ | 已实现 |
| 8 | **实现 `ALTER TABLE ALTER COLUMN SET/DROP NOT NULL`** | ✅ | 已实现 |
| 9 | **实现 `ALTER TABLE ADD/DROP CONSTRAINT`** | ✅ | CHECK / UNIQUE / FK 约束 |
| 10 | **实现 `CREATE TABLE ... AS SELECT`** | ✅ | CTAS 已支持 |
| 11 | **实现 `COMMENT ON`** | ✅ | 表/列注释 |

### P1-2: DQL 完善（全部完成 ✅）

| # | 任务 | 状态 | 说明 |
|---|------|------|------|
| 12 | **实现 `LATERAL JOIN`** | ✅ | 已实现 |
| 13 | **实现 `GROUPING SETS` / `ROLLUP` / `CUBE`** | ✅ | OLAP 多级分组 |
| 14 | **实现 `DISTINCT ON`** | ✅ | PostgreSQL 扩展 |
| 15 | **实现窗口帧 `ROWS/RANGE/GROUPS BETWEEN ... AND ...`** | ✅ | 完整支持三种窗口帧 |
| 16 | **实现 `SIMILAR TO`** | ✅ | SQL 标准正则 |
| 17 | **实现 `OVERLAPS`** | ✅ | 日期区间重叠 |

### P1-3: 并发与事务（全部完成 ✅）

| # | 任务 | 状态 | 说明 |
|---|------|------|------|
| 18 | **实现真正的 SSI** | ✅ | ReadView + rw-conflict 检测 |
| 19 | **实现 `LOCK TABLE` 命令** | ✅ | 显式表级锁 |
| 20 | **实现 `lock_timeout` / `deadlock_timeout`** | ✅ | 锁等待超时 |

### P1-4: 存储过程增强

| # | 任务 | 状态 | 说明 |
|---|------|------|------|
| 21 | **实现简单的过程语言（变量/IF/WHILE）** | ⚠️ | CREATE PROCEDURE 已支持多条 SQL，但 IF/WHILE 流程控制缺失 |
| 22 | **实现 `OUT` / `INOUT` 参数** | ✅ | 已实现 |
| 23 | **实现游标 `DECLARE CURSOR / FETCH / CLOSE`** | ✅ | FETCH NEXT/PRIOR/FIRST/LAST/ABSOLUTE/RELATIVE/ALL/FORWARD/BACKWARD |

### P1-5: 存储引擎（全部完成 ✅）

| # | 任务 | 状态 | 说明 |
|---|------|------|------|
| 24 | **实现 `VACUUM FULL`** | ✅ | 已实现 |
| 25 | **实现 `ANALYZE`（全库）** | ✅ | 已实现 |
| 26 | **实现 `CREATE UNLOGGED TABLE`** | ✅ | 已实现 |

---

## P2: 实用性与体验增强（中期）

### P2-1: Schema 与命名空间

| # | 任务 | 影响文件 | 描述 | 难度 |
|---|------|----------|------|------|
| 27 | **实现 `CREATE SCHEMA` / `DROP SCHEMA`** | `main.cpp` + `TableManage.cpp` | 引入 Schema 命名空间层：database → schema → table。需修改路径解析、权限系统、`information_schema` | ★★★★★ |
| 28 | **实现 `ALTER TABLE SET SCHEMA`** | `main.cpp` + `TableManage.cpp` | 将表从一个 Schema 移动到另一个 | ★★★ |

### P2-2: 分区管理

| # | 任务 | 影响文件 | 描述 | 难度 |
|---|------|----------|------|------|
| 29 | **实现 `ATTACH PARTITION` / `DETACH PARTITION`** | `main.cpp` + `TableManage.cpp` | 声明式分区运维，允许在线添加/移除分区 | ★★★ |
| 30 | **实现子分区（Subpartition）** | `main.cpp` + `TableManage.cpp` | 支持两层分区，如 `PARTITION BY RANGE(date) SUBPARTITION BY HASH(id)` | ★★★★ |

### P2-3: 序列与自增

| # | 任务 | 影响文件 | 描述 | 难度 |
|---|------|----------|------|------|
| 31 | **实现 `CREATE SEQUENCE` / `nextval()` / `currval()`** | `main.cpp` + `TableManage.cpp` | 独立序列对象，支持 `CACHE`、`MINVALUE`、`MAXVALUE`、`CYCLE` 等选项 | ★★★ |
| 32 | **实现 `GENERATED AS IDENTITY`** | `main.cpp` | SQL 标准标识列，替代 SERIAL 伪类型 | ★★ |

### P2-4: 安全增强

| # | 任务 | 影响文件 | 描述 | 难度 |
|---|------|----------|------|------|
| 33 | **实现 `SET ROLE` / `RESET ROLE`** | `main.cpp` + `Session.h` | 允许用户在会话中切换角色 | ★★ |
| 34 | **实现 `GRANT ... WITH GRANT OPTION` / `REVOKE CASCADE`** | `main.cpp` + `TableManage.cpp` | 权限传递和级联撤销 | ★★★ |
| 35 | **实现行级安全策略 (RLS)** | `main.cpp` + `TableManage.cpp` + `ExecutionPlan.cpp` | `CREATE POLICY ... USING (condition)`，查询时自动附加过滤条件 | ★★★★ |
| 36 | **实现 `SCRAM-SHA-256` 认证** | `main.cpp` + `permissions.h` | 替代当前简单 SHA-256 哈希存储，使用 SCRAM 协议进行质询-响应认证 | ★★★ |
| 37 | **实现 `pg_hba.conf` 风格访问控制** | `main.cpp` + `permissions.h` | Host-Based Authentication 配置文件，支持按 IP/用户/数据库控制认证方式 | ★★★★ |

### P2-5: 数据类型扩展

| # | 任务 | 影响文件 | 描述 | 难度 |
|---|------|----------|------|------|
| 38 | **实现 `CREATE DOMAIN`** | `main.cpp` | 自定义域（带约束的类型别名） | ★★ |
| 39 | **实现 `CREATE TYPE`（组合类型）** | `main.cpp` + `TableManage.cpp` | 用户自定义复合类型 | ★★★★ |
| 40 | **实现 `GENERATED AS IDENTITY`** | `main.cpp` | SQL 标准标识列 | ★★ |

### P2-6: 触发器增强

| # | 任务 | 影响文件 | 描述 | 难度 |
|---|------|----------|------|------|
| 41 | **实现 `INSTEAD OF` 触发器** | `main.cpp` + `TableManage.cpp` | 视图上的替代触发器，拦截 INSERT/UPDATE/DELETE 并重定向 | ★★★ |
| 42 | **实现 `WHEN` 子句（条件触发器）** | `main.cpp` | `CREATE TRIGGER ... WHEN (condition)` | ★ |

### P2-7: 备份恢复

| # | 任务 | 影响文件 | 描述 | 难度 |
|---|------|----------|------|------|
| 43 | **实现时间点恢复 (PITR)** | `TableManage.cpp` + `main.cpp` | 基于 WAL 归档的回放到指定时间点 / 事务 ID | ★★★★ |
| 44 | **实现在线备份** | `NetworkServer.cpp` + `TableManage.cpp` | 不停止服务的情况下创建一致性物理备份 | ★★★ |

### P2-8: 监控诊断

| # | 任务 | 影响文件 | 描述 | 难度 |
|---|------|----------|------|------|
| 45 | **完善 `pg_stat_statements`** | `main.cpp` | 增强 SQL 统计视图：查询标准化、执行计划缓存命中率、I/O 时间 | ★★ |
| 46 | **实现 `pg_stat_activity` 完整视图** | `main.cpp` | 当前活跃查询详情：等待事件、事务状态、查询耗时 | ★★ |
| 47 | **实现 I/O 统计** | `BufferPool.cpp` + `main.cpp` | 缓冲池读写次数、磁盘读写次数、Checkpoint 统计 | ★ |

---

## P3: 高级特性（长期）

### P3-1: 查询优化器进阶

| # | 任务 | 影响文件 | 描述 | 难度 |
|---|------|----------|------|------|
| 48 | **公共子表达式消除** | `ExecutionPlan.cpp` | 检测重复子查询，共享计算结果 | ★★★★ |
| 49 | **合并连续 Filter 算子** | `ExecutionPlan.cpp` | `Filter(Filter(scan))` → `Filter(scan)` | ★ |
| 50 | **实现并行查询** | `ExecutionPlan.cpp` + `TableManage.cpp` | 并行扫描、并行 Hash Join、Gather 节点。需要线程池 + 任务调度 | ★★★★★ |
| 51 | **实现向量化执行** | `ExecutionPlan.cpp` | 列式批量处理代替逐行迭代，利用 SIMD | ★★★★★ |

### P3-2: 索引扩展

| # | 任务 | 影响文件 | 描述 | 难度 |
|---|------|----------|------|------|
| 52 | **设计可扩展索引框架** | `BPTree.h` | 抽象索引接口，允许注册自定义索引实现 | ★★★ |
| 53 | **实现 GIN 索引** | 新建 `GINIndex.cpp/.h` | 通用倒排索引，用于数组/JSON/全文搜索加速 | ★★★★★ |
| 54 | **实现 GiST 索引** | 新建 `GiSTIndex.cpp/.h` | 通用搜索树，用于空间数据/范围类型 | ★★★★★ |
| 55 | **实现 BRIN 索引** | 新建 `BRINIndex.cpp/.h` | 块范围索引，用于超大表的粗略过滤 | ★★★ |
| 56 | **实现 `CREATE INDEX CONCURRENTLY`** | `main.cpp` + `BPTree.cpp` | 不阻塞写入的在线索引构建 | ★★★★ |

### P3-3: 复制与高可用

| # | 任务 | 影响文件 | 描述 | 难度 |
|---|------|----------|------|------|
| 57 | **实现流复制（Streaming Replication）** | 新建 `Replication.cpp/.h` + `NetworkServer.cpp` | 主库 WAL 流式发送，备库实时回放 | ★★★★★ |
| 58 | **实现逻辑复制 (Publication/Subscription)** | 新建 `LogicalReplication.cpp/.h` | 基于表级别的逻辑变更捕获和重放 | ★★★★★ |
| 59 | **实现自动故障转移** | `NetworkServer.cpp` | 配合流复制的心跳检测和自动主备切换 | ★★★★ |

### P3-4: 扩展系统

| # | 任务 | 影响文件 | 描述 | 难度 |
|---|------|----------|------|------|
| 60 | **实现插件加载框架** | 新建 `ExtensionManager.cpp/.h` | 动态加载 `.so` 插件，注册 Hook 回调 | ★★★★★ |
| 61 | **实现 `CREATE EXTENSION`** | `main.cpp` + `ExtensionManager` | 插件安装/卸载/升级，依赖管理 | ★★★★ |
| 62 | **实现 FDW 框架** | 新建 `FDWManager.cpp/.h` | 外部数据包装器接口：扫描/插入/更新/删除回调 | ★★★★★ |
| 63 | **实现 `NOTIFY` / `LISTEN`** | `main.cpp` + `NetworkServer.cpp` | 异步消息通道，客户端订阅频道，服务端推送 | ★★★ |

### P3-5: 高级存储特性

| # | 任务 | 影响文件 | 描述 | 难度 |
|---|------|----------|------|------|
| 64 | **实现表空间 (TABLESPACE)** | `main.cpp` + `TableManage.cpp` | 指定数据文件存储位置，支持多磁盘管理 | ★★★ |
| 65 | **实现数据压缩** | `Page.cpp` | 页级（LZ4/Zstd）或行级压缩 | ★★★ |
| 66 | **实现透明数据加密 (TDE)** | `Page.cpp` + `BufferPool.cpp` | 页级 AES 加密，密钥管理 | ★★★★ |

### P3-6: 系统管理

| # | 任务 | 影响文件 | 描述 | 难度 |
|---|------|----------|------|------|
| 67 | **实现 `DISCARD ALL`** | `main.cpp` + `Session.h` | 重置会话状态：关闭预编译语句、释放临时表、重置参数 | ★★ |
| 68 | **实现 `pg_cancel_backend` / `pg_terminate_backend`** | `NetworkServer.cpp` | 通过信号取消/终止指定连接的后端进程 | ★★★ |
| 69 | **实现在线 DDL 支持** | `main.cpp` + `TableManage.cpp` | `ALTER TABLE` 不阻塞读写（至少支持 ADD COLUMN 不阻塞读） | ★★★★ |

---

## P4: 暂不列入（生态/极高难度）

以下功能短期内不计划实现，在可预见的将来依赖外部系统或作为独立项目：

| # | 功能 | 理由 |
|---|------|------|
| - | PostGIS 风格空间扩展 | 需要完整 GIS 库（GEOS/PROJ/GDAL），体量等于本 DBMS |
| - | TimescaleDB 风格时序 | 需要超表 + 连续聚合 + 压缩策略，独立项目级别 |
| - | Apache AGE 风格图查询 | Cypher 解析器 + 图遍历引擎，独立项目级别 |
| - | JIT 编译 (LLVM) | 需要 LLVM 集成，表达式 JIT 编译，适用范围窄 |
| - | 自定义过程语言 (plpython/plperl) | 安全沙箱极难实现 |
| - | 几何类型 (POINT/POLYGON) | 不引入 GIS 则意义有限 |
| - | 网络地址类型 (INET/CIDR) | 小众需求 |
| - | 范围类型 (int4range/tsrange) | 需排他约束基础设施 |
| - | LDAP / Kerberos 认证 | 需要外部库，当前 SHA256 够用 |
| - | MPP 大规模并行处理 | 架构级变更，不现实 |

---

## 汇总统计

| 优先级 | 数量 | 累计工作量（估计人月） |
|--------|------|------------------------|
| **P0** | 5 | ~0.5 |
| **P1** | 21 | ~6 |
| **P2** | 21 | ~8 |
| **P3** | 22 | ~12+ |
| **P4** | 11 | N/A |

**总计可实现项**：69 个 TODO，累计约 26+ 人月工作量。

## 建议执行顺序

```
Phase 1 (Month 1):   P0 #1-5         → 修复 Bug + 完善基础 DQL
Phase 2 (Month 2-3): P1 #6-11, #17   → DDL 完善 + SQL 标准细节
Phase 3 (Month 3-4): P1 #12-16       → DQL 高级查询（GROUPING SETS, LATERAL）
Phase 4 (Month 4-5): P1 #18-26       → 事务/存储过程/存储引擎增强
Phase 5 (Month 6-8): P2 #27-37       → Schema + 安全 + 序列
Phase 6 (Month 8-10): P2 #38-47      → 数据类型 + 触发器 + 备份 + 监控
Phase 7 (Month 10+): P3              → 按需选择高级特性
```

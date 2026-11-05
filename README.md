# DBMS C++

<!-- CURRENT_STORAGE_LIFETIME_20261105 -->

## 存储退出期生命周期（2026-11-05，P9-C2）

当前源码P9-C2（正常程序 `30c57ca8…`）修复表名列表标记与维护状态表的两处退出期生命周期错误。原七阶段恢复回归7个工作进程正常0，完整Memcheck的父进程及7个工作进程均未报告内存错误；正常Main仍有5项探针元数据差异。

旧／新／旧各3次完整恢复调用全部通过，CPU p50约±1%，墙钟受旧版前后漂移影响，未观察到稳定加速。本批是正确性修复。默认425及既有服务保持；详见[完整报告](docs/performance-report-2026-10-30.md#存储退出期生命周期2026-11-05p9-c2)和[机器归档](docs/benchmarks/relation-list-engine-lifetime-20261105.json)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user，Goal active。下方旧节保留当时版本的结果。


<!-- CURRENT_RESTORE_TRANSACTION_HORIZON_20261105 -->

## 物理恢复事务号水位（2026-11-05，P8-C3）

P8-C3 正常程序为 `d778afbd…`，补入公开物理恢复发布前的事务号水位对齐，并优化该公开入口的清单验证；保持C++17。原始较早目标的正常Main首次探针由1/4行恢复为4/4，数据、后续写入与冷启动检查通过，仍有5项描述符/目录身份检查失败。相同七阶段原生验收在最终C3仍于advance阶段异常退出，后四阶段未运行，不能称完整恢复验收通过。

第二轮独立公开API计时中，C3 wall p50较两次旧P7-C1低4.16%/6.42%；第一轮C2仍慢于旧版，两轮全部长尾保留。这是固定正确目标上的完整恢复调用，完整35操作/36查询与PostgreSQL速度未重测。默认程序仍为425，现有服务未重启。详见[完整报告](docs/performance-report-2026-10-30.md#物理恢复事务号水位2026-11-05p8-c3)和[机器归档](docs/benchmarks/restore-transaction-horizon-20261105.json)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧节保留各批次当时结果，不能覆盖本节的失败与范围。


<!-- CURRENT_BACKUP_ANALYZE_20261104 -->

## 备份摘要与 virtual 列统计（2026-11-04，P7-C1）

本批 P7-C1（正常程序 `724c1147…`）优化物理备份清单的创建端摘要，并按 PostgreSQL 18.6 排除 virtual 生成列统计；仍使用 C++17。新增两个永久测试共884项通过，相关12个原有入口通过。

完整备份：wall p50为737.771→587.376→677.477ms，较两次旧版低20.38%/13.30%；四类正常 SQL DDL：wall p50为2快/2慢，ALTER与TRUNCATE慢于两次旧版。完整35操作/36查询未对本批重跑，默认程序仍为425，现有服务未重启。SQL对照34项仍有17项差异，另保留一个旧版恢复缺口。详见[性能与范围报告](docs/performance-report-2026-10-30.md)和[机器归档](docs/benchmarks/backup-analyze-20261104.json)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方按批次保留当时记录，当前状态以本节为准。


<!-- CURRENT_GENERATED_DEMAND_20261104 -->

## 虚拟生成列读值需求与完整 SQL 实测（2026-11-04）

本批源码 P6-C8（正常程序 `45264111…`）补入虚拟生成列实际读值需求、声明赋值和排序输入身份验证，仍使用C++17。四条100行/八virtual的正常SQL在C2/C8/C2中全部正确；SELECT * p50 923.223→80.538→916.453ms，未选分支CASE为863.623→21.730→871.064ms。

固定258观察新增82个整项通过、无整项通过回退，仍有20项差异及6项组件描述符损失；原767观察保留129差异。默认仍是425，整库性能未验收。详见[性能与范围报告](docs/performance-report-2026-10-30.md)及[完整机器归档](docs/benchmarks/generated-column-demand-20261104.json)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧批次保留当时事实，当前状态以本节为准。


## ANALYZE 与 REINDEX 成本优化（2026-11-04）

本批源码 P5-C2（a3baaca6…）优化同次 ANALYZE 的类型/locale 比较和真实行 NULL 位图，以及 REINDEX 逻辑 map 保留。C++17、原维护频率/持久化和用户暂缓边界保持。ANALYZE p50 369–403→74 ms，范围 UPDATE 600–650→389 ms；完整 35 操作为4快/28慢/3居间，36组查询为4快/6慢/26居间，整体性能提升尚未验收，默认仍为425，现有服务未重启。

详见[完整性能报告](docs/performance-report-2026-10-30.md)和[本批机器归档](docs/benchmarks/analyze-reindex-main-costs-20261104.json)。下方旧批次保留其当时结论，当前以本节与报告首节为准。

## C4功能与完整成本记录（2026-11-04）

当前源码C4（9d85c688…）补入RETURNING纯声明与真实来源元数据、虚拟列拥有值及部分MERGE来源输出。新增123原生控制122通过/1个严格元数据差异，新132协议对照全过，原56项为55通过/1差异；273项状态未变，Goal继续active。

实际35操作为7快/19慢/9居间，范围UPDATE及REINDEX仍慢；36组完整查询25快/2慢/9居间。未通过本机默认程序的整体性能提升验收，默认仍为425（425165af…），现有服务未重启。独立API512行的分配/CPU收益及空表/单行负值分开报告，不能概括为所有数据库操作已提速。

使用本批新增功能需要构建当前源码。以下日期更早的章节保留当时结果。

详见[本批报告](docs/performance-report-2026-10-30.md#returning-声明真实行值与操作热路径2026-11-04)及[机器归档](docs/benchmarks/returning-declarations-hotpaths-20261104.json)。以下为历史记录。

使用 C++17 实现的关系型数据库管理系统，提供 SQL 交互、页式存储、索引、事务和查询执行模块，以及 PostgreSQL wire protocol 3.0 的部分实现。

本仓库面向数据库实现的学习、研究与开发。评估具体使用场景时，请先阅读兼容性契约，并在独立的数据目录验证所需的 SQL 与客户端行为。

## 内置函数注册与复合 RETURNING（2026-11-03）

根目录本地默认程序现已原子安装为同批实测C3 `425165af…`；原S3程序已备份，未重启现有服务。

C3 `425165af…` 将无实例状态的内置函数注册表共享为只读数据，保留每个求值器的覆盖、时钟与真实回调语义，并补入实际 INSERT/UPDATE/DELETE RETURNING 的拥有值传递。在已有正确描述符的路径，record、record[] 以及真实物理数组的二进制结果有新增严格控制；OLD/NEW 别名描述符、虚拟数组载荷与历史数组位图仍有缺口。

独立辅助求值夹具的分配减少，但公开游标首次执行CPU也有慢值；两种局部测量都不能替代最终完整SQL对照。

新增两个原生入口共34控制通过；固定301观察为188匹配/113差异，新56项 RETURNING 为48通过/8差异，没有声明功能族完成。完整代别、原失败与测量状态见[本批报告](docs/performance-report-2026-10-30.md#内置函数注册与复合-returning2026-11-03)；最终36组查询中位数全部低于两次S3对照；35类操作为18快/2慢/15居间，保留慢项和漂移，不代表所有操作均已加速，原273状态不变。独立机器归档见[全部原始统计](docs/benchmarks/function-registry-returning-20261103.json)。以下C8/C7记录按其当时版本保留。

## 最新功能验证（2026-11-03）

C8 `91d339db…` 保留原始类型行物化的分配优化，功能结果与C7一致；36组完整查询对照为5快/18慢/13居间，未证明稳定跨预算收益，默认安装仍为S3。局部分配减少不代表整体查询提速，完整样本见[本批报告](docs/performance-report-2026-10-30.md#原始类型行物化分配优化2026-11-03)及[机器归档](docs/benchmarks/owned-datum-primitive-20261103.json)。

前批C7记录保留：功能增量已提交 `89696698`。36组速度对照为4快/23慢/9居间，新程序未替换根目录S3；类型、目录及DML仍有缺口。全部样本、功能验证和后续优化点见[本批报告](docs/performance-report-2026-10-30.md#拥有值的复合类型输出与引用类型存储2026-11-03)。

## 快速导航

- [构建](#构建)与[运行](#运行)：环境准备、编译和启动。
- [SQL 示例](#sql-示例)与[使用手册](docs/MANUAL.md)：查询和事务操作。
- [测试](#测试)与[参与贡献](#参与贡献)：验证改动和报告问题。
- [性能测试报告](docs/performance-report-2026-10-30.md)：查询、增删改、事务、DDL、COPY、索引与维护实测；含有界排序、按页扫描、普通 LIMIT、外部归并、会话工作内存、类型化/集合排序、整数键缓存、VALUES 查询子句、全局计数表达式/动态排序上限及类型化窗口宿主/有序输出复用、窗口整数键缓存/实际执行成本、窗口结果共享与排序投影，以及未接受的暂存候选、类型输出调研的完整结果和复现方法。
- [兼容性契约](docs/compatibility-contract.md)：与 PostgreSQL 接口的差异和边界。
- [与 PostgreSQL 的距离](docs/postgresql-comparison.md)：273 项功能总账、主要缺口和 PostgreSQL 18.6 同机性能对照。
- [当前状态与验证范围](docs/production-status.md#当前状态与验证范围)：最新性能批次、剩余瓶颈及文档检查入口。

## 项目定位与兼容性

本项目使用自己的存储格式，不是 PostgreSQL 的直接替代品，也不能直接打开 PostgreSQL 数据目录。客户端能够连接，不代表 SQL、系统目录、事务或存储行为与 PostgreSQL 等价。

SQL 与客户端协议的接口边界见[兼容性契约](docs/compatibility-contract.md)，具体用法见[使用手册](docs/MANUAL.md)。

## 功能概览

- SQL 解析、AST、类型绑定，以及数据定义、查询和数据修改的执行入口。
- 页式堆存储、缓冲池、变长数据和 TOAST。
- B+ 树、Hash 等索引访问方法。
- Volcano 查询执行器，包含扫描、过滤、投影、连接、排序、聚合和窗口算子。
- 事务、MVCC、保存点、锁管理、WAL 和恢复模块。
- 交互式命令行，以及 PostgreSQL Simple / Extended Query 协议入口。

## 构建

### 环境与依赖

- Linux，支持 C++17 的编译器；脚本构建使用 `g++`。
- Bash、`pkg-config`。
- zlib 与 ICU 开发库。
- OpenSSL 开发库用于 TLS 网络服务；未检测到 OpenSSL 时构建 TLS stub。
- Python 3 用于测试；CMake 构建需要 CMake 3.16 或更高版本。

Ubuntu / Debian 安装依赖示例：

```bash
sudo apt-get update
sudo apt-get install build-essential cmake pkg-config python3 \
    zlib1g-dev libicu-dev libssl-dev
```

### 脚本构建

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
bash scripts/build.sh
```

生成的程序位于仓库根目录：`./dbms_main`。

### CMake 构建

```bash
cmake -S . -B build/cmake
cmake --build build/cmake --parallel
```

生成的程序为 `build/cmake/dbms_main`。两种构建方式使用同一份[生产源码清单](cmake/dbms_sources.txt)。

## 运行

每次启动都必须用 `-D` / `--data-dir` 或 `DBMS_DATA_DIR` 显式指定本项目的数据目录。不要使用 PostgreSQL 的数据目录。登录角色和访问配置须提前准备；数据目录、角色配置与部署约定见[打包与部署说明](docs/PACKAGING.md)。

交互式命令行：

```bash
./dbms_main -D /srv/dbms/main
```

网络服务：

```bash
export DBMS_TLS_CERT=/etc/dbms/tls/server.crt
export DBMS_TLS_KEY=/etc/dbms/tls/server.key
./dbms_main -D /srv/dbms/main --server 9999
```

服务端 TLS 需要真实 OpenSSL、证书和私钥。仅在本地开发时，可显式使用 `--insecure` 启动明文服务。

使用已有角色连接：

```bash
psql "host=localhost port=9999 dbname=info user=admin sslmode=require"
```

仓库也提供 [Dockerfile](Dockerfile) 和 [Compose 配置](docker-compose.yml)。使用 Compose 前需准备角色、访问配置与 `deploy/tls` 下的证书和私钥；数据库文件通过数据卷保存。

### 数据目录使用约定

- 一个数据目录只运行一个服务端进程；多个客户端通过该服务端连接。
- 不要让交互式命令行与服务端同时操作同一个数据目录。
- 数据目录包含数据库状态，请与源码和构建产物分开保存。更换程序或迁移环境前保留备份，并先在副本上验证兼容性。

## SQL 示例

在已连接的数据库中执行：

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    score INTEGER
);

INSERT INTO users VALUES (1, 'Alice', 85), (2, 'Bob', 72);

SELECT id, name, score
FROM users
WHERE score >= 80
ORDER BY id;

BEGIN;
UPDATE users SET score = 90 WHERE id = 1;
SAVEPOINT before_second_update;
UPDATE users SET score = 75 WHERE id = 2;
ROLLBACK TO SAVEPOINT before_second_update;
COMMIT;
```

`USE DATABASE` 等项目扩展命令需要显式启用 extended compatibility mode，不属于 PostgreSQL 标准接口。

## 测试

完整本地回归入口：

```bash
bash scripts/build_tests.sh
```

CMake 的 `check` 目标调用同一测试编排器：

```bash
cmake --build build/cmake --target check
```

运行单个 C++ 测试：

```bash
bash scripts/build_one_test.sh window_functions_test
```

以上命令均在仓库根目录执行。完整回归会编译生产程序，并运行原生测试和已注册的协议测试。

与 PostgreSQL 对比的协议测试需要可连接的参考实例；连接参数、参考版本及其他专项依赖以对应测试脚本为准。执行前检查测试配置，不要把已有的业务数据库用作参考实例或测试目标。

测试、示例和实验应使用独立的数据目录，不要指向正在使用的数据库。报告测试问题时，请附上运行命令、环境与相关输出，便于复现。

## 项目结构

```text
src/
├── main.cpp       # 命令行与服务启动入口
├── parser/        # SQL 解析、AST 与绑定
├── catalog/       # 类型与系统目录
├── commands/      # DDL、DML 与存储引擎接口
├── executor/      # 查询计划与执行算子
├── expression/    # 表达式与预绑定查询执行
├── storage/       # 页面、缓冲池与存储组件
├── access/        # 索引与访问方法
├── transaction/   # 事务与日志组件
└── network/       # PostgreSQL 协议与 TLS
tests/             # 原生测试与协议测试
scripts/           # 构建、测试与打包入口
cmake/             # 共享源码清单
docs/              # 使用、兼容性与开发文档
```

## 文档

- [文档索引](docs/README.md)：使用、开发、代码审查与历史记录的入口。
- [使用手册](docs/MANUAL.md)：SQL 与操作说明。
- [兼容性契约](docs/compatibility-contract.md)：SQL、协议及扩展模式的边界。
- [打包与部署](docs/PACKAGING.md)：源码包、数据目录和配置约定。
- [CHANGELOG](CHANGELOG.md)：版本变更记录。

## 参与贡献

欢迎通过 [Issue](https://github.com/Palpitate-xus/DBMS_C_plus_plus/issues) 报告问题、讨论使用与设计，或通过 [Pull Request](https://github.com/Palpitate-xus/DBMS_C_plus_plus/pulls) 提交改进。

提交问题时请提供：

- 使用的提交或版本、编译器和操作系统。
- 可在独立测试数据目录执行的最小 SQL 复现及启动参数。
- 预期与实际结果，以及相关错误信息。

不要附带真实数据库数据、私钥或凭据。

提交改动时：

- 从自己的分支提交修改，并在 Pull Request 中说明问题、改动与验证结果。
- 补充覆盖问题的回归测试，并说明实际运行的测试范围。
- 保持每个提交独立、易复查，避免混入无关修改。
- 新增生产源码时同步 `cmake/dbms_sources.txt`。
- 新增测试时接入对应测试入口。
- 更新受影响的使用文档，并在 CHANGELOG 中记录面向用户的变更。

## 许可证

仓库未提供独立的 `LICENSE` 文件。授权方式请向项目维护者确认。

## 致谢

- [hyrise/sql-parser](https://github.com/hyrise/sql-parser)
- [zcbenz/BPlusTree](https://github.com/zcbenz/BPlusTree)
- [Jefung/simple_DBMS](https://github.com/Jefung/simple_DBMS)
- [niteshkumartiwari/B-Plus-Tree](https://github.com/niteshkumartiwari/B-Plus-Tree)

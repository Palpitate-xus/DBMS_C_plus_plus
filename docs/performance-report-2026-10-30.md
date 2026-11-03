# 数据库操作性能测试报告（2026-10-30）

最新批次见[窗口暂存分配与类型输出调研](#窗口暂存分配与类型输出调研2026-11-03)：两版暂存候选均未接受，保留完整失败/慢值与类型功能缺口。

2026-11-02 已补充 [PostgreSQL 18.6 同机对照](#postgresql-186-同机对照2026-11-02)，原 10 月 30 日内部优化记录继续按原批次解释。

本轮在已有查询优化的基础上，继续优化提交状态、增删改、约束校验、DDL 重写和后台维护。编译标准仍为 C++17；收益来自执行路径和写放大优化，没有把语言标准升级计作加速因素。

## 测试范围与口径

- 35 类操作：8 类 SELECT，单条/批量/显式事务 INSERT，UPDATE/DELETE（含范围、索引维护和 CTE RETURNING），提交/回滚/保存点，序列，COPY 输入输出，6 KiB TOAST，建表/改表/TRUNCATE/删表，B-tree/Hash 建索引，以及 ANALYZE/VACUUM/REINDEX/CHECKPOINT。
- 主表 1,000 行，关联表 100 行，另有三个各 1,000 行的修改表；批量修改 20 行，ALTER/TRUNCATE/DROP 的独立夹具为 5 行。数据库整体大小、WAL 和其他对象也会影响这些操作。
- 每轮固定种子打乱操作顺序，每类连续采样 3 次。旧版第三轮 CHECKPOINT 返回真实错误，因此主表只比较两侧均完整的前两轮：每类 6 个样本，每侧 210 个样本。旧版原始记录仍为 `complete=false`，没有把失败样本当成延迟值或删除第三轮记录。
- 延迟为复用连接的客户端往返时间，包含修改及提交/持久化完成。建夹具、修改后验证和清理在计时外；查询结果校验在计时内。默认 WAL、fsync 和持久化设置保持开启，使用本机回环连接及 TLS stub。
- SELECT 与修改结果按预期值检查，RETURNING 同时检查返回值与落库结果，COPY TO 校验完整输出，TOAST 校验完整 6,144 字节。只读事务用例检查协议完成和错误；序列用例检查随后 `currval`。这两项不宣称逐项核验计时语句的所有返回单元格。
- 取样数小，p95 使用最近秩法；6 个样本的 p95 就是最大值。共享主机、操作顺序、后台维护、文件缓存和 WAL 大小会影响结果，表内倍率仅适用于本次夹具，不代表任意规模、并发或 SQL 组合。
- 两版各运行一次完整计划，按旧、新顺序进行，未交替重复；没有统计置信区间。CPU 与 I/O 是服务器进程的计数增量，包含同一时段的后台任务。

## 实现改动

1. **CLOG 减少写放大。** 保留每段 256 KiB 的完整逻辑格式、进程间锁、最新状态位合并、文件 fsync、原子 rename 和目录 fsync；临时文件的全零块用空洞表示，连续非零块合并写入。缓存校验用一次 stat 获取设备、inode、长度、mtime 和 ctime；发布时先在锁内关联缓存字节与文件身份，再释放锁，防止并发实例发布后误认旧缓存为新文件。
2. **复用已加载的表结构。** INSERT/UPDATE/DELETE、外键和唯一性校验、索引构建、DDL，以及排序/聚合/窗口内部路径读取存储行的 NULL 位图时复用当前表结构，减少同一行上的目录读取与解析。原生公开入口仍读取当前结构，NULL、空字符串和文本 `NULL` 保持区分。
3. **DDL 重写复用真实的外层回滚边界。** 仅在 DdlExecutor 持有活动、已标脏、完整物理回滚快照，且引擎、数据库和目标表一致时，内部重写行使用该语句边界，消除逐行创建整库保存点的开销。普通 INSERT、触发器发出的 INSERT、直接原生重写及缺少回滚所有者的调用保留原有保护。
4. **绑定 DML 提前筛选整数候选。** 无 FROM/USING 的 UPDATE/DELETE 对同类型 SMALLINT/INTEGER/BIGINT 直接列与字面量或语句参数的纯等值条件添加必要的堆过滤。原类型表达式继续复核，MVCC、RLS、SSI 和错误检查保留；不把这项写成 UPDATE/DELETE 已实现物理索引定位。
5. **协调维护操作。** 后台刷盘、自动 VACUUM、CHECKPOINT 与 DROP DATABASE 使用按数据库的维护所有权；前台可等待内部维护完成，同一 StorageEngine 的重叠 CHECKPOINT 复用实际成功或失败结果。不同引擎各自刷脏缓存；活动用户事务仍导致拒绝，刷盘/归档失败仍向上传播。后台检查点不再持有全局缓存锁等待其他维护，等待路径不持有维护锁读取全局事务集合。
6. **修复检查点截断后的 WAL 重开。** 旧版按段截断后，第一保留段可能从跨段记录的中间开始，启动完整性扫描会拒绝该 WAL。最终版从已有持久化检查点的精确位置出发，沿 `xl_prev` 验证 CRC、前驱方向和记录首尾衔接，找出保留段中的首条完整记录，再按 LSN 递增校验所有保留的完整记录。损坏/无效的锚点或记录继续报错；没有有效恢复起点的非对齐截断在删除文件前拒绝。复用现有检查点格式，没有增加磁盘格式标记。

主要源码：[CommitLog.cpp](../src/storage/CommitLog.cpp)、[WAL.cpp](../src/storage/WAL.cpp)、[TableManage.cpp](../src/commands/TableManage.cpp)、[DdlRewrite.h](../src/commands/DdlRewrite.h)、[DdlExecutor.cpp](../src/commands/DdlExecutor.cpp)、[DmlExecutor.cpp](../src/commands/DmlExecutor.cpp)。

## 实测结果

倍率为旧版 p50 ÷ 新版 p50；大于 1 表示本次观测更快，小于 1 表示更慢。所有操作均列出，未按收益筛选。

| 操作（一次的规模） | 优化前 p50 / p95 ms | 优化后 p50 / p95 ms | p50 倍率 |
| --- | ---: | ---: | ---: |
| SELECT 1 | 2.73 / 3.07 | 2.30 / 3.05 | 1.19× |
| 普通主键点查 | 32.56 / 70.60 | 32.12 / 40.78 | 1.01× |
| 参数化主键点查 int4 | 50.19 / 58.60 | 47.19 / 58.03 | 1.06× |
| 无索引文本等值 | 106.41 / 206.19 | 89.59 / 100.36 | 1.19× |
| 范围 SUM（100 行） | 157.73 / 338.56 | 150.86 / 202.51 | 1.05× |
| GROUP BY 聚合 | 111.65 / 267.98 | 100.86 / 108.56 | 1.11× |
| 双键排序 LIMIT 20 | 387.75 / 492.73 | 92.61 / 187.68 | 4.19× |
| JOIN 后 COUNT | 345.52 / 404.43 | 346.72 / 371.26 | 1.00× |
| INSERT 单行自动提交 | 46.08 / 190.62 | 48.21 / 76.88 | 0.96× |
| INSERT 多 VALUES（20 行） | 151.16 / 333.45 | 124.47 / 161.22 | 1.21× |
| INSERT 显式事务（20 条） | 215.76 / 362.47 | 193.38 / 230.89 | 1.12× |
| UPDATE 主键单行 | 366.84 / 437.98 | 106.59 / 139.28 | 3.44× |
| UPDATE 单行并维护二级索引 | 401.02 / 486.51 | 113.31 / 153.63 | 3.54× |
| CTE UPDATE 单行 RETURNING | 298.56 / 421.88 | 60.89 / 75.51 | 4.90× |
| UPDATE 范围（20 行） | 540.06 / 792.51 | 196.59 / 477.15 | 2.75× |
| DELETE 主键单行 | 392.52 / 541.14 | 57.48 / 62.50 | 6.83× |
| CTE DELETE 单行 RETURNING | 360.36 / 429.73 | 61.58 / 70.66 | 5.85× |
| DELETE 范围（20 行） | 415.41 / 860.36 | 158.96 / 526.54 | 2.61× |
| 只读事务 BEGIN/SELECT/COMMIT | 37.36 / 188.88 | 30.62 / 39.63 | 1.22× |
| 写事务 INSERT 后 ROLLBACK | 84.18 / 219.53 | 60.08 / 94.86 | 1.40× |
| 事务 UPDATE/保存点/回滚/提交 | 646.97 / 892.20 | 191.22 / 216.51 | 3.38× |
| 序列 nextval | 40.14 / 42.67 | 38.72 / 57.48 | 1.04× |
| COPY FROM（20 行） | 150.46 / 277.36 | 129.17 / 167.11 | 1.16× |
| COPY TO（1,000 行） | 127.12 / 260.54 | 103.73 / 130.70 | 1.23× |
| TOAST INSERT（6 KiB） | 103.06 / 392.90 | 78.10 / 148.28 | 1.32× |
| CREATE TABLE 空表 | 2030.07 / 2188.64 | 1705.77 / 1962.53 | 1.19× |
| ALTER ADD COLUMN（5 行） | 18453.43 / 21758.37 | 1970.73 / 2049.80 | 9.36× |
| TRUNCATE（5 行） | 2109.86 / 2307.75 | 1630.56 / 1937.31 | 1.29× |
| DROP TABLE（5 行） | 2210.86 / 2972.99 | 1906.12 / 2238.40 | 1.16× |
| CREATE INDEX B-tree（1,000 行） | 2451.75 / 3071.46 | 1941.16 / 2056.72 | 1.26× |
| CREATE INDEX Hash（1,000 行） | 2330.76 / 3075.81 | 1614.65 / 2640.75 | 1.44× |
| ANALYZE（1,000 行） | 397.83 / 491.68 | 372.10 / 418.68 | 1.07× |
| VACUUM 删除表 | 4.80 / 6.43 | 4.86 / 159.06 | 0.99× |
| REINDEX（1,000 行） | 335.05 / 447.28 | 318.68 / 354.31 | 1.05× |
| CHECKPOINT | 67.37 / 204.94 | 57.61 / 70.72 | 1.17× |

旧版共保留 297 个成功样本及第三轮失败记录；新版完整完成 315 个样本，35 类各 9 次，结果检查与协议错误数均为 0。主表使用双方前两轮各 210 个样本，第三轮新版数据另留在原始 JSON 中，没有混入共同窗口。

普通点查接近持平，单行 INSERT 的 p50 为 46.08→48.21 ms，VACUUM 为 4.80→4.86 ms；不能据此声称所有操作都稳定加速。较明显的改善集中在排序、UPDATE/DELETE、保存点及 ALTER 重写。

第三轮单行 INSERT 的三个原始样本为 195.24、56.76、203.29 ms，服务器进程 CPU 增量分别为 20、30、20 ms，显示这一时段有较大的等待开销；现有计数不足以确定每次等待的唯一来源。第三轮数据和新版本的全部长尾继续保留。

### 相邻补测

主测中单行 INSERT 等项目收益不明显，因此补充相邻旧版—新版—旧版对照。三次均使用同一成熟夹具的独立副本、相同 SQL 顺序和默认持久化/后台维护设置，每项 6 个样本。为让旧读取器能重开该夹具，在每个副本中恢复其已归档的完整 WAL 前缀；该准备不计时，三个输入完全相同，源目录文件哈希未变。

| 操作 | 旧版先测 p50 ms | 最终版 p50 ms | 旧版后测 p50 ms |
| --- | ---: | ---: | ---: |
| 范围 SUM | 161.80 | 165.69 | 157.88 |
| 单行 INSERT | 57.93 | 56.21 | 51.81 |
| CREATE TABLE | 3203.51 | 3157.45 | 3253.59 |

补测显示明显的时段和夹具影响，单行 INSERT 与范围 SUM 没有证明稳定、普遍的收益；建表仍受较大的固定开销影响。这是独立补测，未替换主表样本。54 次计时均完成且结果校验通过，原始记录和驱动见 `adjacent-regression-probes-v2/{results.json,summary.json}` 与 `adjacent_regression_probes.py`。

### 前一轮查询优化记录

前一轮先优化了目录发布检查、扫描 NULL 解码、参数化整数主键索引访问与整数 JOIN 候选过滤。本轮的“优化前”已经包含这些改动。
以下是原始版 `abe34a27…` 到本轮基线 `fbb066f4…` 的历史结果，使用同一 1,000 行查询夹具的独立副本、三轮热缓存测量；不是最终版的另一次对照，倍率不能与上表相乘。

| 查询 | 原始版 p50 ms | 本轮基线 p50 ms | 旧/新样本数 |
| --- | ---: | ---: | ---: |
| SELECT 1 | 1.390 | 1.392 | 900 / 900 |
| 普通主键点查 | 44.938 | 47.703 | 30 / 18 |
| 参数化主键点查 int4 | 2,947.000 | 95.425 | 9 / 17 |
| 无索引文本等值 | 739.058 | 166.212 | 9 / 9 |
| 范围 SUM | 2,993.472 | 198.014 | 9 / 9 |
| GROUP BY 聚合 | 1,917.560 | 178.732 | 9 / 10 |
| 双键排序 LIMIT 20 | 2,637.167 | 414.301 | 9 / 9 |
| JOIN 后 COUNT | 4,818.074 | 379.715 | 9 / 9 |

该轮普通点查没有证明稳定加速：单客户端吞吐主对照从 21.312 降至 11.615 QPS，相邻旧/新/旧补测也有明显波动；4 客户端聚合吞吐为 0.636→18.942 QPS，二者均保留。慢查询旧版样本较少。完整数据、不同查询的取样规则、早期失败与 23 项原生/26 项协议验证见本机 `build/query-opt-20261030-lwpnox22/{report.txt,comparison.json,validation.json}`。

## CLOG 写入字节对照

独立探针分别使用两版匹配的头文件与 CommitLog 对象，截取一次 `flush()` 的 write 调用，逐字节校验落盘文件，并重新打开读取状态；没有把文件空洞或 write 字节数当作端到端速度。

| 夹具 | 旧版 write 字节/次数 | 新版 write 字节/次数 | 两版逻辑文件长度 |
| --- | ---: | ---: | ---: |
| 仅一个已提交状态的年轻段 | 262,144 / 1 | 4,096 / 1 | 262,144 字节 |
| 每字节均有状态的稠密段 | 262,144 / 1 | 262,144 / 1 | 262,144 字节 |

年轻段本次发布的 write 字节减少 98.44%；稠密段仍为一次连续写入。稀疏文件的实际磁盘分配和收益取决于文件系统、事务数量和状态分布。

## 正确性与故障回归

最终候选使用 `stage-m-wal-retained-prefix` 的实际生产核心对象及二进制：**123 项原生入口（122 项主矩阵及 1 项独立退出回归）、45 项真实 PostgreSQL wire protocol 入口全部退出 0**。原生主矩阵与协议组串行运行，独立退出回归另行执行。原生测试链接实际生产核心对象，在私有临时数据目录执行；协议测试明确使用该候选。

- 原生覆盖 CLOG、WAL、INSERT/UPDATE/DELETE 故障原子性、触发器、DDL 回滚、保存点、目录、约束/RLS、NULL/TOAST、统计、VACUUM、备份恢复与索引。WAL 追加失败矩阵完整输出 `246 FAILURES 0`，不是将单个入口算成 246 个独立测试程序。
- 新增 [CLOG 稀疏发布](../tests/clog_sparse_publication_test.cpp)保留 14 项字节、短写、EINTR、截断/定位、fsync/rename 失败及重试检查；[发布身份竞态](../tests/clog_generation_publication_test.cpp)在实际解锁瞬间插入另一实例发布，修复前稳定断言失败，最终候选通过。
- [DDL 重写所有权](../tests/ddl_rewrite_row_owner_test.cpp)检查增加/删除/转换列、NULL/空串/最小整数，以及第三次 AFTER 触发器抛错后的整条语句回滚、审计表回滚、外层事务前缀与用户保存点保留。触发器发出的普通 INSERT 继续有自己的保护。
- [CHECKPOINT 合并](../tests/checkpoint_coalescing_test.cpp)与[后台维护占锁](../tests/background_maintenance_drop_test.cpp)用 fsync 门控制真实并发，覆盖等待、同引擎合并、不同引擎分别刷盘、真实 I/O 错误与重试、活动用户拒绝及 DROP；旧候选的对应控制稳定失败，最终通过。
- 新增[绑定整数 DML](../tests/dml_integer_prefilter_protocol_e2e_test.py)完整 49 项控制，含重复键、NULL、负数、反向条件、别名、volatile 求值次数、text/binary Bind、声明类型不匹配、大整数和异常列名回退。既有参数化 SELECT 索引 69 项、JOIN 44 项控制也在最终版本重跑通过。

早期候选 H 的协议运行曾有 43 项通过、1 项 `dml_explain_execution_protocol_e2e_test` 在动态 CAST 错误路径超时，随后清理 ROLLBACK 也超时。最终在原生重 I/O 任务结束后，以原始 15 秒 socket 期限串行跑完全部 45 项，包括该完整测试；原失败日志保留，不据此宣称历史 R164 根因已查明。Python 脚本语法检查及不重叠 DELETE 夹具的参数拒绝检查另行通过。

新增 [WAL 检查点保留前缀](../tests/wal_checkpoint_prefix_test.cpp)的 17 项控制，覆盖一条记录跨越一/两个段、检查点前完整记录保留、重开后追加，以及损坏前缀/检查点/后缀拒绝和无锚点截断拒绝；旧 L 在重开断言失败。PITR、PITR 分叉备份归档、恢复完整性、redo、UNLOGGED、回滚与临时文件恢复一并重跑。

压测结束后另用最终 M 读取器重开旧版、候选 L、最终 M 三份数据库的独立副本，按各自成功测量序列重算全部 11 张表的所有单元格，检查序列下一值与临时 DDL 对象清理；原始数据目录文件哈希保持不变。结果见 `restart-verification/results.json`。

这是 168 个指定入口的回归，未运行全仓库全部入口、TLS 或整库 sanitizer，也不自动关闭兼容性总账。

## 失败记录与测量边界

- 首个 TOAST 试跑用了不被当前声明接受的 64 KiB TEXT 值，返回 `22023`。正式夹具改用可支持的 `VARCHAR(12000)` 与 6 KiB 值；未将首次试跑计为通过。
- 初版 ALTER 夹具为 20 行，旧版混合负载下超过性能客户端的 60 秒期限；该记录保留。后续采用独立的 5 行 DDL 夹具，保证双方能完成共同测量窗口。另一次新建数据库的 CPU 采样中，旧版 20 行 ALTER 用时 44.19 秒，产生约 763 MB 的进程 write_bytes 增量；该受插桩数据只用于定位逐行整库保存点，未混入主表倍率。
- 旧版正式 35 类测试在第三轮 CHECKPOINT 返回 `XX000`。原日志没有指出具体失败阶段，不能仅凭后续通过判定该次失败的唯一原因；新增确定性并发测试分别验证后台维护占锁、重叠检查点与真实 fsync 失败。
- 一次候选原生测试将 WAL 故障矩阵截在自定义外层 180 秒监视期限。项目原测试没有该期限；最终对这一完整矩阵给 900 秒外层监视，未删故障项、改 WAL 设置或放宽协议客户端默认 15 秒期限。
- 目录发布测试原夹具将同一 inode 移出再移回，粗粒度时间戳下可能没有文件身份变化。修正为保留旧 inode 时复制产生新文件；原有新内容可见性断言保留。统计/VACUUM 测试曾在最终 DROP 清理处失败，数据断言已通过；补充状态诊断及独立后台占锁控制，不据此倒推每次旧失败的根因。
- CLOG 字节探针初次沿用头部注释的 `pg_xact/0000`，实际实现的段名是 `pg_xact/0`，因读取错误路径而断言失败。修正路径后两版均保留完整字节和重开读取断言。
- 第一版写入优化 L 完成了 315 个性能样本，但补测复用其数据库副本时，旧版读取器报 `cannot open WAL`。从完全未优化的本轮旧版数据也复现同一错误；新增跨段测试在 L 上稳定失败，确认是既有截断/重开缺陷。随后增加恢复起点修复 M，并重新进行功能、性能及真实数据库重开验证。L 的成功计时与重开失败均保留，主表最终仅使用 M 的测量。
- 退出阶段的初次诊断夹具在主线程的 atexit 回调中复用已结束生存期的线程局部引擎上下文而崩溃；该夹具不能证明产品退出缺陷。修正为先建立事务/目录/WAL 所有者，再在退出回调中新建工作线程执行 CHECKPOINT/DROP，保留原断言；有效夹具在 M 上通过。原始失败和修正后的来源分别保留在 `native-shutdown-control/` 与 `native-shutdown-accepted/`。

## 复现与证据

运行基准会新建私有数据库目录，不打开现有用户数据，也不会自动重编译指定二进制。示例命令：

```bash
python3 scripts/benchmark_operations.py \
  --binary /absolute/path/to/dbms_main \
  --output build/operations-benchmark-new-run \
  --rows 1000 --batch 20 --ddl-rows 5 --rounds 3 --samples 3

python3 scripts/benchmark_queries.py \
  --binary /absolute/path/to/dbms_main \
  --output build/query-benchmark-new-run --rows 1000
```

每次选用新的 output 路径。两版正式操作测试使用同一份固定的驱动和协议 helper；根目录脚本另补 DELETE 夹具不重叠的参数检查，并修正文档对校验范围的说明，正式参数不受影响。操作记录含每个样本的延迟、CPU、进程 I/O 计数、结果状态、二进制 SHA-256 和启动参数。`units_per_second` 中批量 DML/COPY/建索引按行计数，其他用例按整次操作计数，不能混用。

本机证据位于 `build/database-all-ops-20261030-kl187edl/`，含起始工作区清单、源文件快照、逐阶段构建清单、原始失败、完整测试日志、固定驱动、CLOG 探针和结果汇总。该目录由 build 忽略规则管理；如需跨机器审阅，请一并保存该目录，不能仅凭文档中的本地路径重建旧版工作区。

测量环境：Linux 6.8.0-136-generic x86_64，Intel Xeon E5-2678 v3 @ 2.50 GHz（2 路、48 逻辑 CPU），共享主机；GCC 13.3.0、`-std=c++17 -O2 -g1 -pthread`，ICU、zlib、TLS stub。两版编译和链接配置相同，未启用 LTO 或 `-march=native`。

| 版本 | 二进制 SHA-256 |
| --- | --- |
| 本轮基线（已含上一轮查询优化） | `fbb066f45cc975bea6df338bfc6623ead0b841b360390465ab6ce4a583efe354` |
| 本轮最终版 | `f47176388afe16f9e346db82e5e89b5af7588232921f63d9cf4a2ac10df4490e` |

起始 HEAD 为 `c365ff7902f57f9c7ee496f62c9056a6c93f9bfd`，当时工作区已有未提交修改；这两个二进制标识不能替换成 HEAD 的净源码。冻结快照与初始文件哈希保留原工作区内容。

共 62 个生产编译单元，按生产清单链接。每阶段对源文件及传递项目头文件逐字节核对后复用相同配置的 donor 对象，变更单元实际重编；最终阶段因 WAL 头文件变更重编 7 个单元并复用 55 个，其他修改单元已在前面阶段重编。最终根目录与冻结快照的 206 个生产文件逐一核对一致，不将增量构建写成全量新编译，也未伪造常规构建缓存签名。

证据相对本轮目录：

- `operations-before-v2/operations.json`：旧版完整原始记录（`complete=false` 及失败原因保留）。
- `operations-after-m/operations.json`：最终版完整 315 样本。
- `operations-{before,after}-common-window.json`：明确标注父记录状态的共同前两轮。
- `operations-comparison.{json,csv}`：35 类 p50/p95、CPU 与写入字节汇总；`clog-write-comparison.json` 为独立字节探针。
- `stage-m-wal-retained-prefix/{build.json,native-final/,protocol-final/}`、`final-provenance.json`：构建依赖、每个测试源文件/日志 SHA-256 与实际退出码。
- `driver/sha256.json`：两侧实际使用的固定驱动和 helper 哈希；其中操作驱动 SHA-256 为 `0ddc77735c997b7b9fe5b4ec5700830e069972922432298d8ff6012d6e6cffd7`。
- `original/`、`metadata.json`、`optimization.patch` 与各候选阶段日志：起始内容、本轮增量和失败追溯。

## PostgreSQL 18.6 同机对照（2026-11-02）

本节是新增的小表探索性对照，与上文 10 月 30 日的本项目版本比较分开解释。
本节保留当时程序及 PG 的配对结果；随后源码优化见下文[有界排序批次](#有界排序-top-n2026-11-02)，未重写这里的历史样本。
新建夹具、数据库文件和后台状态与旧批次不同，不能把本节较低的绝对延迟解释成新增源码优化收益。
两边各 35 类操作、每类 6 个计时样本，共 **420 个计时样本**；另各执行一轮 35 类不计时预热。
最终记录完整结束，既有驱动的结果/协议检查均通过。这里的数值回答指定夹具的客户端往返延迟，不能代表整体数据库性能。

### 环境、方法和限制

- 复用 [benchmark_operations.py](../scripts/benchmark_operations.py) 的 35 类操作与既有 oracle，新增 [benchmark_postgresql.py](../scripts/benchmark_postgresql.py) 负责两个独立的本地进程及配对计时。没有使用或修改已有数据库；临时服务均已正常退出。
- 主表 1,000 行、关联表 100 行，修改批量 20 行、DDL 夹具 5 行；同一主机、同一文件系统、回环 TCP_NODELAY，各复用一个连接，无 TLS。两轮每类各 3 次，固定种子打乱操作顺序；第一轮每项先本项目后 PostgreSQL，第二轮反过来。预热后再测，未清 OS 缓存。
- PostgreSQL 精确核验为 18.6（`server_version_num=180006`），发行二进制来自 `public.ecr.aws/docker/library/postgres:18.6-bookworm`，提取后以本机进程运行；manifest digest 为 `sha256:5c4783a8cc3f0074912164f94dfa1be4e043438f0f707cf968d7af9a0d31a2c8`，所需额外库仅作用于该进程。
- PostgreSQL 的 `fsync`、`synchronous_commit`、`full_page_writes`、`data_checksums` 均为 `on`；`shared_buffers=128MB`、`work_mem=4MB`、`checkpoint_timeout=5min`、`jit=on`。本项目使用默认持久化与后台维护，堆/索引池默认 256/128 帧。**未统一两边的内存额度或调优参数**，不能声称完全相同配置。
- 本项目使用 11 月 1 日 CMake 4.4.4 全新构建的 C++17 / `-O2` / TLS stub 程序，SHA-256 为 `996c0b6ee761116f413f4594e722138e5972c58229f42f48bc98b3cc3f72702f`；生产输入为 `9b71e118`，运行期间未修改生产代码。根目录原程序未替换。
- 计时包括 action、响应及客户端解码；SELECT oracle 在 action 中，修改后的额外读取验证、setup 与 cleanup 不计时。只读事务和序列仍沿用原驱动的有限检查，见本报告开头。COPY TO 也包含流量和客户端处理，不是纯服务器执行耗时。
- 这是共享主机、两轮配对的小样本观察，单项 PostgreSQL 测量时间很短，客户端/调度噪声会影响亚毫秒值；6 个样本的 p95 就是最大值，没有置信区间，不能推导 p99、并发吞吐、冷缓存或大表曲线。CPU/I/O 统计范围在 PG 多进程与本项目多线程间不同，本节不比较这些计数。
- 没有运行标准 pgbench/TPC 或长时间压力测试；[PostgreSQL 官方 pgbench 建议](https://www.postgresql.org/docs/18/pgbench.html)强调足够长的重复测量和规模/维护/客户端瓶颈控制。下面倍率仅是本轮对应操作的延迟比，不是总体“落后倍数”。

### 完整结果

延迟比 = 本项目 p50 / PostgreSQL p50；大于 1 表示本项目在该操作样本中延迟更高。
原始延迟和 p95 均保留在 JSON/CSV；表内保留 p50，避免将六个样本的最大值解读为稳定尾延迟。

| 操作 | 本项目 p50 ms | PostgreSQL 18.6 p50 ms | 延迟比 |
| --- | ---: | ---: | ---: |
| SELECT 1 | 1.780 | 0.246 | 7.2× |
| 普通主键点查 | 15.280 | 0.360 | 42.5× |
| 参数化主键点查 int4 | 16.024 | 0.320 | 50.2× |
| 无索引文本等值 | 57.339 | 0.569 | 100.8× |
| 范围 SUM（100 行） | 118.290 | 0.537 | 220.4× |
| GROUP BY 聚合 | 75.109 | 2.946 | 25.5× |
| 双键排序 LIMIT 20 | 60.028 | 1.417 | 42.4× |
| JOIN 后 COUNT | 279.252 | 0.999 | 279.5× |
| INSERT 单行自动提交 | 20.323 | 0.625 | 32.5× |
| INSERT 多 VALUES（20 行） | 92.355 | 0.925 | 99.9× |
| INSERT 显式事务（20 条） | 133.274 | 4.225 | 31.5× |
| UPDATE 主键单行 | 55.423 | 0.653 | 84.9× |
| UPDATE 单行并维护二级索引 | 63.590 | 0.756 | 84.2× |
| CTE UPDATE 单行 RETURNING | 23.411 | 0.796 | 29.4× |
| UPDATE 范围（20 行） | 130.900 | 0.858 | 152.5× |
| DELETE 主键单行 | 26.934 | 0.573 | 47.0× |
| CTE DELETE 单行 RETURNING | 34.143 | 0.729 | 46.8× |
| DELETE 范围（20 行） | 110.730 | 0.923 | 120.0× |
| 只读事务 BEGIN/SELECT/COMMIT | 12.390 | 0.569 | 21.8× |
| 写事务 INSERT 后 ROLLBACK | 27.660 | 0.658 | 42.0× |
| 事务 UPDATE/保存点/回滚/提交 | 113.760 | 1.538 | 74.0× |
| 序列 nextval | 12.049 | 0.250 | 48.2× |
| COPY FROM（20 行） | 76.849 | 0.813 | 94.5× |
| COPY TO（1,000 行） | 83.468 | 15.481 | 5.4× |
| TOAST INSERT（6 KiB） | 33.381 | 0.774 | 43.1× |
| CREATE TABLE 空表 | 913.311 | 2.027 | 450.5× |
| ALTER ADD COLUMN（5 行） | 920.287 | 0.916 | 1004.3× |
| TRUNCATE（5 行） | 1081.687 | 1.191 | 908.1× |
| DROP TABLE（5 行） | 1027.397 | 1.517 | 677.4× |
| CREATE INDEX B-tree（1,000 行） | 992.384 | 2.293 | 432.9× |
| CREATE INDEX Hash（1,000 行） | 922.623 | 3.216 | 286.9× |
| ANALYZE（1,000 行） | 309.694 | 2.951 | 105.0× |
| VACUUM 删除表 | 2.515 | 0.521 | 4.8× |
| REINDEX（1,000 行） | 235.688 | 3.768 | 62.5× |
| CHECKPOINT | 19.966 | 13.762 | 1.5× |

本轮普通点查约 42.5×、单行 INSERT 约 32.5×、主键 UPDATE 约 84.9×、主键 DELETE 约 47.0×；
5 行表 ALTER ADD COLUMN 约 1,004.3×。CHECKPOINT 约 1.5×，不能据此推导 WAL/恢复能力已等价。
不同操作差别很大，未计算一个综合性能百分比，也没有将这些倍率与上文内部优化倍率相乘。

### 复现与原始证据

需要独立可运行的 PostgreSQL 18.6 安装；驱动自行初始化新 cluster，拒绝把其他版本当参考：

```bash
python3 scripts/benchmark_postgresql.py \
  --binary "$PWD/build/cmake/dbms_main" \
  --pg-bin /path/to/postgresql-18.6/bin \
  --output build/pg-comparison-new-run \
  --rows 1000 --batch 20 --ddl-rows 5 --rounds 2 --samples 3
```

可选 `--pg-library-dir /path/to/private/libs` 只给参考进程设置库查找路径；output 必须是新目录。
本机证据在 `build/pg-gap-assessment-20261102-i3wba7p7/`：`postgres-bundle.json`、`inputs.json`、
`paired-run-v3/comparison.{json,csv}`、`paired-run-v3.log`、双方启动日志和原始 EXPLAIN。
`comparison.json` 包含两个轮次的每个样本、预热记录、PostgreSQL 设置、双方操作计数、二进制/驱动哈希；
`assessment-verification.json` 记录对样本数、oracle、总账/生产输入未变及文档表格的复核。
这是本地 build 证据；跨机器复核需保存该目录和所用 PostgreSQL 安装/镜像，不能只依赖本地路径。

环境准备的失败也保留：原默认 `pgref` 实际为 17.2，未用作 18.6 对照；Docker Hub 首次拉取超时后改用镜像渠道。
前两次私有初始化分别缺 `libpq.so.5` 和配置样本的链接目标，记录在 `paired-run/`、`paired-run-v2/`；
这两次没有计时样本，补齐运行文件后才完成 v3，未将失败尝试写成通过。未重启已有参考服务或用户数据库。

## 有界排序 Top-N（2026-11-02）

### 实现与适用边界

从 `e7dcd673` 开始，对照 PostgreSQL 18 的 [tuplesort](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/sort/tuplesort.c)
及 [tuple slots](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/executor/execTuples.c)、
[DuckDB TopN](https://duckdb.org/2024/11/14/optimizers)和[外部排序](https://duckdb.org/2025/09/24/sorting-again)、
[SQLite 临时结构](https://www.sqlite.org/tempfiles.html)与[原子提交](https://sqlite.org/atomiccommit.html)，独立实现本项目的有限改动：

- `PreparedSortOp` 对普通正数 LIMIT 使用最大堆，候选上限为溢出安全的 `K=LIMIT+OFFSET`，最终排序后交给原投影/OFFSET/LIMIT 算子。
  必要排序键和排序前输出仍对全部原输入求值；原始序号保持同键输入顺序，周期检查取消。
- 可支持的单表排序 LIMIT 在执行阶段进入已有类型化执行器，修复参数化路径的多键顺序、OID 表达式和 volatile 投影需求。
  Parse/Bind 规划未改，R244 暂缓边界不变。
- 物理行只读取一次 NULL 位图，复用已有属性解码并恢复嵌套绑定，避免同一排序行重复解码；文件身份检查保留。
  已求值的精确内置整数键只转换一次；其他类型保留原比较职责，浮点特殊值改由表达式比较器处理。
- 排序候选约为 O(min(N,K))，比较工作约为 O(N log K + K log K)。这不是整个执行器的 O(K) 内存承诺：
  **底层 TableScan 仍可能物化全部原始行**；无界排序、DISTINCT、WITH TIES、聚合/分组和边界溢出继续原路径，没有完整 work_mem/spill。

### 相邻旧／新／旧延迟

冻结旧程序为此前 CMake 产物 `996c0b6e…`；最终候选 SHA-256 为
`c7f832ddbef2dd1811b87f25bf96474a5fb71b20b564f53b881c391b1df22d15`。
两者同为 GCC 13.3 / C++17 / O2 / ICU / zlib / TLS stub；本批正常增量编译变更单元，未宣称重新 fresh 编译全部 62 单元。
验收后已将同一候选原子安装到根目录 `dbms_main`，原根目录程序备份为证据目录内的 `root-before-dbms_main`；未重启既有数据库服务。

同一份正常关闭的 1,000 行夹具复制为三个独立目录，源文件逐项哈希不变；依次旧、新、旧，每次独立启动，
每类 3 次校验预热后做 3 轮×4 个样本，固定打乱每轮顺序，总计 **144 个计时样本**，全部校验完整结果。
回环 TCP_NODELAY、单连接、热缓存；计时包含响应与结果验证；默认持久化不变、socket 15 秒，不与编译或其他测试重叠。

| 操作 | 旧首轮 p50 ms | 新 p50 ms | 旧末轮 p50 ms | 结论 |
| --- | ---: | ---: | ---: | --- |
| amount DESC,id ASC LIMIT 20 | 56.219 | 41.612 | 58.482 | 延迟约降低 26%–29%，约快 1.35–1.41 倍 |
| 同排序 LIMIT 20 OFFSET 500 | 57.896 | 42.462 | 57.481 | 延迟约降低 26%–27%，约快 1.35–1.36 倍 |
| 主键点查控制 | 9.816 | 10.736 | 9.482 | 未证明提速，候选在此窗口略慢 |
| 同排序无 LIMIT 控制 | 86.066 | 87.517 | 85.673 | 未证明提速 |

这是共享主机上的小样本，不推导 p99、吞吐或大表收益，也不将新版延迟除以前一批独立 PG 样本来制造新的 PG 倍率。

### 内存与正确性

`prepared_topn_memory_test` 使用真正的类型化算子和 12,000 行生成流（每行 2 KiB payload），
在子算子 EOF、输出消费之前用 `mallinfo2` 读取活跃分配增量；不是 RSS、峰值内存或实际 TableScan 的整条查询占用。
同一冻结测试旧版完整 8 项有 3 项内存失败、结果均正确；最终新版 8 项全过。
LIMIT 20 保留量由 72,817,360 字节（约 69.4 MiB）降到 131,952 字节（约 129 KiB）；
OFFSET 13/LIMIT 7 为 124,656 字节，LIMIT 1 为 18,224 字节。
LIMIT 0 原输入读取为 0；超量 LIMIT、无界、DISTINCT/WITH TIES 控制仍保留完整输入，证明观测没有隐藏全量缓冲。

最终 11 个原生入口、11 个协议入口全过；新增完整 48 条在严格 PG 18.6 和新版各全过，旧版 11 条失败。
10 个相邻协议入口涵盖完整 190 条聚合排序、69 条参数索引及取消/上下文等检查；原生含原完整 80 项排序中断检查。
另外重跑既有 35 类操作，一轮 35 个样本和预热的现有结果检查均通过；与部分原生编译重叠，只作正确性 smoke，不用于性能结论。
本批不是全仓库、真实 TLS、并发、崩溃恢复或完整 PG 兼容验收，原始总账保持 22/166/70/15。

### 失败保留与待办

- 早期仅接入类型化有界排序时，LIMIT 20 的 p50 实测回退到 160.678 ms（两侧旧版 59.503/62.479）；
  独立 CPU 采样显示文件身份核验开销占主导，继而修复重复解码/按列位图读取。中间候选为 61.700 ms；
  后续采样确认类型名反复规范化的成本，再加入整数键缓存，才得到上述最终结果。各候选与原始样本均保留。
- 接入新分流曾暴露 2 条 Infinity 比较失败，按浮点比较语义修复后才通过；没有删除断言。
  NULL 位图回归首次编译缺少 DbError 头文件，补齐后完整运行通过。
- 10,000 行夹具在加载阶段触及 15 秒期限，清理等待 30 秒后被 helper 强制终止；未产生完整计时样本，
  没有提高期限、降低持久化或称为大表通过。加载瓶颈仍待调查，不据此重开用户暂缓的退出/CTAS 专项。
- 三方额外探针确认虚拟生成列失败返回 NULL 的既有差异，见[测试报告](test-report.md#2026-11-02-有界排序与类型化执行)；
  流式物理扫描、完整工作内存/spill、修改路径快照开销仍是后续目标。

### 复现与证据

```bash
python3 scripts/benchmark_topn.py \
  --before /path/to/frozen-before-dbms_main \
  --after "$PWD/build/cmake/dbms_main" \
  --output build/topn-new-run --rows 1000 --rounds 3 --samples 4
DBMS_MAIN="$PWD/build/cmake/dbms_main" python3 tests/prepared_topn_protocol_e2e_test.py
```

`--fixture` 可指定同一驱动创建并正常关闭的夹具（必须匹配 `--rows`），各次运行只修改其副本。
证据目录 `build/pg-feature-topn-20261102-bc582b_s/`：
`latency-keycache/comparison.json` 为最终 144 样本；`latency-comparison.json` 与 `latency-decode/comparison.json` 为早期候选；
`native-before.log`、`native-keycache.json`、`protocol-keycache.json`、`protocol-{old,reference}-48.log` 保留完整检查；
`operations-smoke/operations.json`、`fixture-10000-failure.json`、`virtual-demand-three-way.json` 记录操作回归及未关闭差异；
`final-provenance.json` 记录生产输入、实际对象、测试与程序哈希。

## 按页物理扫描（2026-11-02）

### 实现与验证

以 `17df7ff3` 的 Top-N 版本为旧版继续优化。参考 PostgreSQL 18 的
[SeqScan](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/executor/nodeSeqscan.c)和
[heap scan](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/heap/heapam.c)：
保留扫描状态、snapshot 和资源生命周期，由上层按需求取后续行，避免 open 时就读完整张表。
实现为本项目自有游标，没有移植 PostgreSQL 文件格式。

- 普通非分区 `TableScanOp` 打开时保留一个 MVCC 视图和堆页范围，逐页复制可见行及 NULL 位图，
  页锁/pin 在交付前释放，避免持锁调用写函数；新出现的 combo ID 仅补充解码表，不推进该扫描的 command ID 或可见性边界。
- 同一套扫描初始化和 SSI 登记被回调扫描与游标复用；空扫描仍登记 relation SIREAD，读取页内候选时保留原 tuple/page 覆盖。
  游标拥有 allocator 引用，继续使用原有文件身份、缺失文件、tablespace 和格式核验。
- 最多保留一页未展开的行；只对当前交付行展开 TOAST，类型化消费者直接使用捕获的 NULL 位图。
  扫描行缓冲不再随表内行数线性增长；buffer pool、snapshot/SSI 跟踪、上层结果缓存和单个大值不包含在这项界限内。
- 提前停止记录实际交付行数，只有到 EOF 才发布完整 live-row 估计；重复 close 不重复计数，析构、I/O 错误与未打开子算子均核验资源/NULL 绑定。

最终 **23 个原生入口、17 个真实协议入口** 全过，另 35 类操作预热及一轮既有结果检查全过。
新增物理扫描完整 6 项旧版 1 个内存失败、新版全过；跨后端修改和 fresh scan 共 8 项覆盖自提交、RC、RR、SERIALIZABLE，均保留正确 OLD/NEW 与 TOAST 值。
新增写函数排序协议完整 13 条在冻结旧版、PG 18.6 和新版均全过；相邻完整 48/80/190/69 及其他范围见[测试报告](test-report.md#2026-11-02-按页物理扫描与固定快照)。

### 内存与延迟

同一原生夹具含 192 行，普通非 NULL payload 为 16 KiB，并含 NULL、空串和字面文本 NULL；
先完整读过 heap/TOAST 而不保留行，再以 `mallinfo2` 测扫描 open 前后的活跃分配增量。
冻结旧版为 **2,820,192 字节（约 2.69 MiB）**，新版为 **17,088 字节（约 16.7 KiB）**，约降低 99.4%。
这不是 RSS、峰值或整条查询内存；最终仍检查完整 192 行、TOAST 值、NULL、提前关闭及重开。

延迟使用与前批相同的闭合 1,000 行夹具，三个独立副本按旧/新/旧启动；源文件逐项哈希不变。
每类预热 3 次，3 轮×4 个已完整校验样本，六类共 **216 个计时样本**，不与构建或其他测试重叠。
默认耐久性、15 秒 socket、单连接、回环 TCP_NODELAY、热缓存及计时内结果验证均保持不变。

| 操作 | 旧首轮 p50 ms | 新 p50 ms | 旧末轮 p50 ms | 结论 |
| --- | ---: | ---: | ---: | --- |
| amount DESC,id ASC LIMIT 20 | 53.883 | 50.111 | 55.118 | 约快 1.08–1.10 倍 |
| 同排序 LIMIT 20 OFFSET 500 | 56.884 | 57.428 | 56.940 | 未证明提速 |
| 已有类型化 ARRAY[id] LIMIT 1 | 31.893 | 23.128 | 33.955 | 约快 1.38–1.47 倍 |
| 普通列投影 id LIMIT 1 | 43.985 | 43.106 | 48.122 | 仍走旧分流，未证明稳定提速 |
| 主键点查控制 | 22.702 | 24.593 | 24.419 | 未证明提速，此窗口略慢 |
| 无 LIMIT 排序控制 | 107.620 | 108.408 | 107.282 | 未证明提速 |

最终统计锁发布修正前的六类 **216 样本**也保留：按上表顺序，旧/新/旧分别为
37.827/32.737/37.298、38.450/33.965/38.552、17.274/9.739/16.965、
22.969/22.755/23.789、9.947/9.387/10.209、87.579/90.310/89.795 ms。
该窗口类型化 LIMIT 的改善约 1.75 倍，最终窗口约 1.4 倍；包括未改旧程序在内的绝对延迟明显波动，不能把前阶段较好的倍率当作最终稳定收益。

此前还完成四类旧/新/旧 **144 样本**：排序 LIMIT 20 为 39.574/34.960/37.829 ms，
OFFSET 500 为 40.559/39.668/39.993 ms，点查为 12.067/11.577/9.656 ms，无界排序为 89.799/92.357/88.234 ms。
三次观察不合并择优；OFFSET 等收益会受窗口波动影响。此处不推导大表、并发、p99、完整业务负载或新的 PG 性能比。

### 构建、失败与未完成范围

最终程序 SHA-256 为 `531a7ef325c604ec23a57a707d38e3b45607c535e84c50953a6506c56076d02c`，
GCC 13.3 / C++17 / O2 / ICU / zlib / TLS stub 未变。公共头变化触发 13 个单元重编；
另明确重编两次 ExecutionPlan，保证资源释放及统计锁边界修改已进入对象；共链接当前 62 个生产对象，复建不再编译/链接。
统计在释放关系锁前发布，避免 DROP/重建重置统计后又被旧扫描写回；最后修正后完整重跑上述 23/17 入口及 35 类操作。
验收后的同一候选原子替换根目录程序，旧程序保留备份，既有服务未重启。

PG 参考首次监听后尚未就绪，返回 57P03，专项 SQL 尚未运行；原日志保留。
辅助驱动补 `pg_isready` 探针，在原 15 秒启动期限内确认就绪后才认证、核验 18.6 并完成 13 条检查，没有放宽协议断言或期限。
新增扫描测量前试选的 `~id` 投影在冻结旧版报 42703，保留探针记录；最终采用已支持的数组投影，未据此认定整数位非功能已完成。

分区/RLS 保留原路径，ParallelTableScan 和其他 legacy 消费者仍可能全量缓冲；普通 `SELECT id ... LIMIT 1` 尚未普遍接入按需执行，
是下一批明确的执行分流缺口。无界排序/work_mem/spill、生成列表达式错误传播、修改快照成本及大表加载失败仍开放。
本批不修改 Bind 规划、用户暂缓项或原始 273 项状态，也不是全仓库/真实 TLS/崩溃恢复验收。

### 复现与证据

```bash
python3 scripts/benchmark_topn.py \
  --before /path/to/17df7ff3-dbms_main --after "$PWD/build/cmake/dbms_main" \
  --output build/scan-new-run --rows 1000 --rounds 3 --samples 4 --include-scan-limit
DBMS_MAIN="$PWD/build/cmake/dbms_main" python3 tests/table_scan_effect_snapshot_protocol_e2e_test.py
```

本机证据 `build/pg-feature-scan-20261102-i7t0b3bo/`：`latency-release/comparison.json` 为最终 216 样本，
`latency-with-scan/comparison.json` 与 `latency/comparison.json` 为前阶段 216/144 样本；`native-before.log`、`native-release.json`、`protocol-release.json`、
`statistics-final.json`、`table_scan_snapshot_test-release.log`、`protocol-reference-v2.log` 保留实际退出码和完整控制。
`operations-release/operations.json`、`limit-probe-v2.json`、首个 PG 启动失败日志、CMake 编译/复建日志及 `final-provenance.json` 保留来源与边界。

## 普通 LIMIT 按需执行与错误前缀（2026-11-02）

### 实现与适用范围

以 `bcde5b3e` 为旧版，参考 PostgreSQL 的 [LIMIT/OFFSET 语义](https://www.postgresql.org/docs/18/queries-limit.html)、
[Limit 算子](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/executor/nodeLimit.c)及
[SELECT 求值说明](https://www.postgresql.org/docs/18/sql-select.html)，把普通、可支持的单表 LIMIT 接入已有类型化算子链。
之前只有数组、特定标量角色或有序 LIMIT 等会接入，普通列投影仍先做全表工作。

- 执行阶段接入普通有限 LIMIT；LIMIT 0 不打开输入，OFFSET 行仍按原投影需求求值，未请求的后续行不额外执行 WHERE/投影。
  DISTINCT/聚合仍完成其所需输入，不直接截断原输入。没有 ORDER BY 时不承诺固定子集，本轮验证使用固定无索引堆夹具。
- 共享计数辅助函数对可识别的负数 LIMIT/OFFSET 返回 2201W/2201X；保留负零、NULL/ALL 等原有解析边界。
  单列星号保留已绑定列类型，避免扩大接入后把星号推断为 unknown/text。
- 新增显式 `executePlanWithPrefix` 接收契约：失败仍为失败，保留原 SQL 异常及恰好一次清理，但允许顶层 SQL 接收器保留已经产生的类型化行/NULL 位图。
  默认 `executePlanChecked` 和嵌套调用继续失败即清空。Simple Query 和不限 portal 行数的 Extended Execute 在正常格式下先发送该前缀再报错，不发送成功标签。
- 这仍是结果缓冲接收器，不是网络端完全流式。有限行数 portal 的错误/悬停需求没有完成；未修改 Bind/Parse 规划、参数帧、custom/generic 或格式 2 策略。
  R303/R304/R305 继续按[已有用户暂缓](deferred-scope.md)处理，不能据普通 SQL LIMIT 修复恢复其共享生命周期调查。

### 正确性、未通过项与构建

**24 个原生入口通过**，其中新增前缀接收完整 16 项：默认清空与显式保留、SQL/非 SQL 异常身份、open/close/hasError、主错误不被清理覆盖、清理一次、NULL/空串/文本 NULL/UTF-8 和正常 EOF/限额。
完整所选 **21 个协议入口为 20 通过、1 失败**；新增完整 **62 条**在 PostgreSQL 18.6/候选各全过，冻结旧版有 9 条失败。
覆盖 Simple、text/binary Bind 及结果、计数与求值需求、序列/写函数次数、阻塞 DISTINCT/聚合、OID/typmod/属性来源及错误前原始帧。

首份 54 条夹具误用“错误前无行”的通用检查，PG 在第 6 行除零前已经发出 5 行；原失败保留，随后改成精确检查 5 行及 T/D/E/Z 顺序，再扩展为 62 条。
仅修改分流的中间候选仍有错误前缀和负数 LIMIT 2 条失败，补生产实现后才全过。
未通过入口为既有 R304/R305 完整 463：旧/新均 40 处差异，463 条观察逐字节一致；不是 40 个独立 bug，也没有排除后称全绿。
额外有限行数 portal 共 6 条观察，PG 全过、旧/新各相同 2 条差异，保留而不接受为修复；相关共享模块继续暂缓。
中央索引此前遗漏 2026-10-27 的专属暂缓记录，本轮补入原引用，不创建新暂缓项或改变原始 273 项状态。

35 类操作完成预热及一轮现有 oracle 检查；与相邻编译/回归重叠，只作有限正确性 smoke。
生产程序 SHA-256 为 `8a428080cc42e1809cf41ef8f3d09f5658d6d42f095623326d0d18b5ae00ebf6`，仍为 GCC 13.3 / C++17 / O2 / ICU / zlib / TLS stub。
先重编 1 个 Main，再因接收器 API/头变化重编 6 个单元，共 7 次编译、6 个不同单元；链接当前 62 个生产对象，不冒称 fresh 62。
原生前缀测试先链接已完成的 61 个 core 对象，再逐项哈希确认与最终对象相同；其余原生重新编译/链接，未以 stub 代替真实协议 Main。
验收后同一程序原子安装到根目录，原版保留备份，既有服务未重启；不是全仓库、真实 TLS、恢复或完整 PG 行为验收。

### 六类相邻旧／新／旧延迟

与前批相同的闭合 1,000 行夹具复制为三个独立目录，源文件哈希不变；每类预热 3 次、3 轮×4 个计时样本，六类共 **216 个样本**。
不与构建或其他测试重叠；默认耐久性、15 秒 socket、单连接、热缓存、回环 TCP_NODELAY 和完整结果校验均未变。

| 操作 | 旧首轮 p50 ms | 新 p50 ms | 旧末轮 p50 ms | 结论 |
| --- | ---: | ---: | ---: | --- |
| 普通列投影 id LIMIT 1 | 22.893 | 9.791 | 22.670 | 约快 2.32–2.34 倍，延迟约降低 57% |
| 已有类型化 ARRAY[id] LIMIT 1 | 10.166 | 10.136 | 11.438 | 作为原路径控制，不认定稳定提速 |
| amount DESC,id ASC LIMIT 20 | 33.646 | 33.696 | 34.348 | 未证明稳定提速 |
| 同排序 LIMIT 20 OFFSET 500 | 36.969 | 37.363 | 36.131 | 未证明提速 |
| 主键点查 | 9.453 | 10.118 | 11.224 | 窗口波动，不认定提速 |
| 无 LIMIT 排序 | 94.457 | 84.531 | 85.651 | 旧两侧漂移，不认定稳定提速 |

只将普通列 LIMIT 1 的改善归属于本批接入，不用其他窗口倍率相乘，也不推导大表、并发、p99、全业务或新的 PG 性能比。
主结果仍在主机共享负载下测量，不能把绝对延迟当作固定保证。

### 复现与原始证据

```bash
python3 scripts/benchmark_topn.py \
  --before /path/to/bcde5b3e-dbms_main --after "$PWD/build/cmake/dbms_main" \
  --output build/ordinary-limit-new-run --rows 1000 --rounds 3 --samples 4 --include-scan-limit
DBMS_MAIN="$PWD/build/cmake/dbms_main" python3 tests/physical_limit_demand_protocol_e2e_test.py
```

证据目录 `build/pg-feature-limit-20261102-tafn38be/`：`latency/comparison.json`、`native-final.json`、`protocol-final.json`、
`protocol-{before,reference,after}-62.log`、`protocol-before-reference-62.json`、`executor_result_prefix_test.log`、
`portal-existing-failure-comparison.json`、两侧完整生命周期日志及 `portal-quota-{before,reference,after}.log` 保留全部观察。
`operations-smoke/operations.json`、各次 CMake 编译/复建日志与 `final-provenance.json` 记录结果、来源和实际构建范围。

## 普通物理排序的内存预算与外部归并（2026-11-02）

以下是 `d09cb6c1`→`471dad3f` 批次的历史记录；其中最终按需归并的当时缺口已在[后续批次](#最终归并按需输出与排序绑定恢复2026-11-02)补充，数字和未通过项保持原口径。

### 研究、实现与实际 SQL 接入

基于 `d09cb6c1`，参考 PostgreSQL 的[每操作工作内存](https://www.postgresql.org/docs/18/runtime-config-resource.html)和
[tuplesort 有序段/归并](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/sort/tuplesort.c)，
结合 [DuckDB 减少排序搬运和归并 I/O 的设计](https://duckdb.org/2025/09/24/sorting-again)及
[SQLite 对排序临时数据与事务日志的区分](https://www.sqlite.org/tempfiles.html)，独立实现普通物理 Sort 的工作预算与外部归并。

- 原先全量行、多个辅助向量及排序后副本改成一个行载体，保留原始字节、已求值键、完整物理 NULL 位图和实际行来源。
  按 `work_mem_kb` 估算批次的字符串及向量容量；预算内直接排序，超预算生成有序段，按行宽/预算作 2–16 路归并。
  段头在文件中串联，不保留随输入增长的段目录，不为每段打开描述符；每个 Sort 最多两个文件描述符。
- 临时文件创建后立即 unlink，设置 CLOEXEC；正常结束、提前关闭、异常和进程退出均释放。短读/短写与读写 EINTR 保留完整字节，
  ENOSPC/描述符耗尽/I/O 错误分别传递 53100/53000/58030；输入错误保留原异常，子节点清理一次。
  没有修改 WAL、降低 fsync/耐久性或放宽 socket 期限。
- **首个仅改算子的候选并未覆盖真实 SQL**：原前端为捕获结构化结果而主动绕过 Volcano；实际 SQL 文件探针失败，原日志保留。
  随后接入能够保留结构化单元格的普通单表、物理单键排序，保留列顺序/别名、NULL/空串/文本 NULL 和 OID；
  将可解析的 ORDER BY 别名/序号降低到真实列，同时修复 `ORDER BY 1` 把整数当文本排序的既有差异。
- 接入只用于正常 Simple 或不限 portal 行数的 Execute、受支持内置键及普通 AND 条件。
  多键、NUMERIC、数组/对象身份、显式排序 COLLATE、DISTINCT/WITH TIES、OR 分流、虚拟生成列及有限行数 portal 等保持各自既有消费者。
  `SET work_mem` 会话语义、类型化排序/Top-N 的工作预算、最终归并按需输出及 hash/aggregate/window/join 仍未完成。
  单个超大行、有限读头及元数据可超出预算；结果接收器仍可能保留全部输出，**不是整条查询/RSS 的硬上限**。

### 内存、正确性和未通过项

最终 **29 个原生入口通过**：前批 24、三个新排序入口及两个恢复入口。新入口共 39 控制：内存 17、I/O/清理 16、真实堆表 6；
冻结旧版分别 13/15/4 不符，新版全部通过。这些不符不是 32 个独立 bug。
真实堆表核对 8 KiB TOAST、NULL/空串/文本 NULL、>2^53 BIGINT、行来源、全部值、提前关闭和同一图重开；
I/O 控制含真实 syscall 的短传输/EINTR、ENOSPC、EMFILE、读取 EOF/EIO、截断失败、取消、原始主异常及强杀时匿名文件释放。

| 内存观察，8,192 行、每行额外 2,048 字节 | 旧版，字节 | 新版，字节 |
| --- | ---: | ---: |
| 各输入调用边界的最大活跃分配增量 | 18,219,056–18,231,760 | 50,816–55,376 |
| 子输入 EOF 时保留的活跃分配增量 | 18,219,056–18,231,760 | 10,352–14,912 |

配置为 64 KiB；四个方向/NULL 顺序控制均完整消费全部输出。另有 1 KiB 预算、32×256 KiB 超大记录控制。
这些是该原生进程在指定输入边界的分配观察，**不含所有归并瞬时峰值，也不是 SQL 全进程峰值/RSS**。

新增完整 **47 条协议**在严格 PG 18.6/最终程序全部通过，冻结旧版有 1 条整数序号排序失败。
完整所选 **22 个协议入口为 21 通过、1 失败**：既有暂缓的 Bind/portal 完整 463 中有 40 处不符，旧/新全部观察逐字节相同。
前一 `1865fddb` 候选的同组 22 为 20 通过/2 失败，另一个是 statement_timeout 的部分 COPY 帧关闭检查；
隔离旧/新/旧三次也均未通过，但失败位置不同，未定位共同原因。最终该完整入口通过，**不改写原失败，也不声称 R197 已修复**。
专属 R197 的既有用户暂缓补入中央索引，未恢复 COPY 响应/中断所有权调查；R303/R304/R305、共享 CPU 排序中断 helper 和 NetworkServer 源码均未修改。

首份 37 条配置检查使用 `pg_settings.setting::integer`，PG 全过而本项目返回 0A000；保留原失败，改用原生 text 列严格核验值 64。
这个独立目录表达式缺口没有修复，不能因排序专项通过将其关闭。随后扩展到 47，原 36 个排序/协议控制均保留。
原生内存夹具另增加逐输入边界分配观察；EOF-only 和首版输出消费日志也保留，没有减少最终控制或放宽断言。

实际 O2 Main 在观察到匿名排序文件后被强杀；进程退出后无命名残留，重开逐项核对 **1,000 行/4,000 单元格**，再次排序全值一致。
原生 redo_crash_recovery/recovery_integrity 也完整通过；这不是全部故障、并发、OOM、TLS 或整库恢复矩阵的验收。
同一最终程序的 35 类操作完成预热及一轮现有结果/修改检查；与相邻回归重叠，仅作有限正确性 smoke。

### 性能：最终程序的两个独立窗口

沿用闭合 1,000 行夹具，每个窗口分别复制为旧/新/旧三个目录；每类预热 3 次、3 轮×4 个样本，七类每窗口 **252 样本**。
默认实例预算 4 MiB；另一窗口在独立源副本中配置 64 KiB，旧代码没有消费该预算。各窗口源文件哈希均不变，
默认耐久性、15 秒 socket、完整结果校验、回环 TCP_NODELAY 不变；没有与本任务编译/测试并行计时。

默认 4 MiB，p50 ms：

| 查询 | 旧首 | 新 | 旧末 |
| --- | ---: | ---: | ---: |
| 普通物理单键无界排序 | 106.927 | 76.283 | 96.752 |
| 类型化双键 LIMIT 20 | 31.798 | 34.313 | 33.428 |
| 同排序 OFFSET 500 | 33.701 | 37.370 | 35.028 |
| 其他分流的双键无界排序 | 86.993 | 88.447 | 81.982 |
| 主键点查 | 8.994 | 10.822 | 10.057 |
| ARRAY[id] LIMIT 1 | 9.459 | 11.065 | 10.345 |
| 普通 id LIMIT 1 | 8.689 | 11.153 | 9.929 |

64 KiB，p50 ms：

| 查询 | 旧首 | 新 | 旧末 |
| --- | ---: | ---: | ---: |
| 普通物理单键无界排序 | 108.897 | 78.528 | 101.329 |
| 类型化双键 LIMIT 20 | 38.296 | 38.987 | 35.513 |
| 同排序 OFFSET 500 | 35.955 | 40.023 | 35.623 |
| 其他分流的双键无界排序 | 94.835 | 89.597 | 86.612 |
| 主键点查 | 11.344 | 13.258 | 11.729 |
| ARRAY[id] LIMIT 1 | 11.585 | 14.188 | 11.205 |
| 普通 id LIMIT 1 | 10.438 | 12.046 | 10.504 |

普通物理单键无界排序在默认窗口约快 **1.27–1.40 倍**，64 KiB 窗口约快 **1.29–1.39 倍**。
最终两个窗口中的多项控制也有慢值：例如默认普通 LIMIT 为 8.689/11.153/9.929 ms，64 KiB 数组 LIMIT 为 11.585/14.188/11.205 ms。
不能认定这些差异全部由环境造成；本批**未证明所有查询不回退**，后续仍需对混合工作负载及其余控制继续定位。
其他控制按表保留，不概括为全查询提速；这是共享主机的小表热缓存、单连接窗口，不推导大表、并发或 p99。
没有重测 PG 延迟，也没有将不同批次的改善倍率相乘或拿新值除以历史 PG 样本。

### 前一候选的回退、诊断与构建来源

`1865fddb` 的三个窗口各 252、合计 756 个样本原样保留，不与最终结果合并。
单键无界排序三组旧/新/旧 p50 分别为 104.707/80.001/107.823、109.615/77.708/109.362、101.986/77.385/101.631 ms。
同时发现不利控制：默认 Top-N 为 33.726/38.174/36.141，重复默认组为 32.630/36.715/32.302 ms；
重复组双键无界排序 84.516/98.234/85.703 ms。没有删除慢值或据目标项的改善宣称全面加速。

单独控制诊断 400 个完整校验查询，把额外开销缩小到 Main 接入构建；将判定移出大型 executeInternal，保持 AST 临时对象在独立函数。
后续五版本诊断 500 查询中，Top-N 服务进程 CPU 为旧首 29.4、仅算子 28.4、内联接入 31.0、独立判定 29.2、旧末 28.2 ms/次。
点查仍有主机窗口漂移，诊断按每个 50 查询批次的进程计数取值，不能当作逐请求 CPU p50 或与正式延迟混合。

最终生产程序 SHA-256 为 `6ee04376b8276c36de9ea3726582d50125adeb8a6bca15d9a57f4cd7e59dd94f`，仍为 GCC 13.3 / C++17 / O2 / ICU / zlib / TLS stub。
本批初次重编 5 个生产 CPP，再先后完成两次 Main O2 构建，共 7 次成功编译、5 个不同 CPP；另两次 Main 编译因补范围护栏/序号修复而停止，不算通过。
一次实际 Main O0+61 O2 core 的快速功能预验另存，没有将它用于最终性能。最终链接 62 个当前生产对象，未冒称 fresh 62。
29 个原生只链接其实际使用的 61 个 core；后续只改 Main，逐对象哈希核对一致，再在最终实际 Main 重跑协议/操作/崩溃探针。
验收后同一程序原子安装到根目录，原版备份保留，既有服务未重启。
原始 273 项、38 条原子要求及专属暂缓记录保持；本批只新增局部证据，完整 work_mem/资源治理及全 PG 行为仍未完成。

### 复现与证据

```bash
DBMS_MAIN="$PWD/build/cmake/dbms_main" python3 tests/sort_spill_protocol_e2e_test.py
python3 scripts/benchmark_topn.py \
  --before /path/to/d09cb6c1-dbms_main --after "$PWD/build/cmake/dbms_main" \
  --fixture /path/to/closed-1000-row-fixture --rows 1000 --rounds 3 --samples 4 \
  --include-scan-limit --include-full-sort --output build/sort-new-run
```

证据目录 `build/pg-feature-sort-1be1avdu/`：最终两组 `latency-final-{default,low64}/comparison.json`，
三个前期窗口 `latency-{default,low64,default-repeat}/comparison.json`、两份 `control-cpu*.json`、
`native-final.json`、`protocol-final.json`、新原生旧/新完整日志、`protocol-{before,reference}-47.log` 及最终排序协议日志均保留。
`protocol-final-first/`、`timeout-isolated.json`、对应三份失败日志和 `portal-existing-failure-comparison.json` 记录未通过范围。
`main-sort-crash-outlined.json`、`operations-outlined/operations.json`、各构建/预验日志与 `final-provenance.json` 记录实际程序和构建来源。


## 最终归并按需输出与排序绑定恢复（2026-11-02）

以下是 `471dad3f`→`049c0bce` 批次的历史记录；会话 SET 的当时缺口已在[后续批次](#会话工作内存与实际排序预算2026-11-02)补充，其他原范围和数据保持。

基于 `471dad3f`，进一步参考 [PostgreSQL 18 tuplesort 的最终归并](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/sort/tuplesort.c)：
顺序访问的消费者不必把最后一个完整归并结果重新写入临时文件。普通物理 Sort 现在只完成必要中间轮次，剩余至多 2–16 个段时保留有限读头和堆，按 `next()` 合并输出。
输入及必要排序键仍在输出前全部消费/求值；不把这个改动描述为跳过输入或扩展所有排序分流。

获胜行的后继记录延至下一次需求才读取，提前关闭不读后继。这个精确 I/O 需求策略由本项目自行定义；
PG 的 TSS_FINALMERGE 在返回当前行前补读，**没有据此声称注入文件故障时的行前缀与 PG 完全相同**。
读错、分配异常等使本 Sort 立即关闭并保留主异常；再次读取不会从半条记录重试。子节点返回失败也立即清理，保留原诊断。
Sort 同时捕获调用方 NULL 行绑定，在子节点清理后恢复，覆盖正常、提前关闭、失败、析构和嵌套；
未打开/已关闭对象再次关闭不影响后来设置的绑定。没有改公共头、Main 分流、网络、持久化或共享 CPU 排序中断 helper。

### 确定的 I/O 收益与正确性

新增两个原生入口完整 **40 项**：最终归并 8、绑定生命周期 32；冻结旧版分别 6/26 不符，新版全部通过，不能合计为 32 个独立缺陷。
归并夹具为 1 KiB 预算、8 条 >4 KiB 记录，强制 8→4→2 段；实际 syscall 字节计数证明少一次完整输出写入和再读取，
同时完整检查 >2^53 BIGINT、全部原始字节、三列 NULL 位图、ASC/DESC/NULL 顺序和真实空来源。
真实堆表生命周期入口覆盖实际 RID/NULL/空串/文本 NULL、内存/落盘、嵌套与重开、六种子节点失败、异常身份及锁/文件释放。

现有 I/O 的输出阶段 EIO 检查从“0 行后报错”改为严格“1 行后报错”，不是允许零或任意前缀；
新增检查还要求错误后立即无描述符，连续 `next()` 为 EOF 且无额外 I/O。保留原版断言在新程序上的 1 处失败及首次旧版夹具未捕获预期 EIO 而中止的日志，
随后只补测试异常捕获，让旧版完整跑完 8 项，并保留其 6 处不符。

本批 **31 个原生入口通过**，所选完整 **22 个协议入口为 21 通过/1 失败**；同一排序完整 47 条在严格 PG 18.6 和最终程序全过。
未通过项仍为已有暂缓的 Bind/portal 完整 463 中 40 处差异，本批与前版全部观察逐字节相同；未删除失败或恢复 R197/R303/R304/R305 调查。
实际 Main 在匿名排序文件存活时 SIGKILL，重开核对 1,000 行/4,000 单元格和再次排序通过；35 类操作完成现有检查，
该 smoke 与回归重叠，只用于有限正确性观察。没有冒称全仓库、全故障、真实 TLS 或完整 PG 等价。

### 独立延迟与既有慢值诊断

沿用闭合 1,000 行夹具，各窗口独立旧/新/旧副本，七类、各预热 3 次、3 轮×4 样本，每窗口 252、共 **504 个完整校验样本**。
默认耐久性/15 秒 socket/TCP_NODELAY不变；计时阶段没有本任务构建、测试或其他基准并行。没有重测 PG 延迟。

默认 4 MiB，p50 ms：

| 查询 | 旧首 | 新 | 旧末 |
| --- | ---: | ---: | ---: |
| 普通物理单键无界排序 | 80.034 | 80.373 | 88.792 |
| 类型化双键 LIMIT 20 | 35.310 | 35.762 | 40.847 |
| 同排序 OFFSET 500 | 41.264 | 36.854 | 43.779 |
| 其他分流的双键无界排序 | 101.066 | 91.330 | 101.203 |
| 主键点查 | 12.155 | 11.432 | 13.652 |
| ARRAY[id] LIMIT 1 | 11.449 | 11.673 | 13.307 |
| 普通 id LIMIT 1 | 10.739 | 12.721 | 12.893 |

64 KiB，p50 ms：

| 查询 | 旧首 | 新 | 旧末 |
| --- | ---: | ---: | ---: |
| 普通物理单键无界排序 | 417.722 | 87.753 | 72.164 |
| 类型化双键 LIMIT 20 | 295.640 | 38.125 | 36.440 |
| 同排序 OFFSET 500 | 408.172 | 42.659 | 35.535 |
| 其他分流的双键无界排序 | 409.825 | 99.639 | 83.271 |
| 主键点查 | 313.746 | 12.295 | 12.019 |
| ARRAY[id] LIMIT 1 | 394.422 | 12.096 | 12.730 |
| 普通 id LIMIT 1 | 311.290 | 11.349 | 11.310 |

默认预算的普通排序在内存内，80.034/80.373/88.792 ms；低预算窗口旧首整体达到 295–418 ms，之后显著回落，
普通单键为 417.722/87.753/72.164 ms，新值仍比旧末慢约 21.6%。控制值亦有慢值；
**本批未证明 SQL 延迟稳定改善或全部查询不回退**，不能把旧首的慢值换算为数倍收益，也不能确定所有差异由主机造成。

宽行原生对照另用 8,192 行、每行额外 2,048 字节、64 KiB 预算；同一源代码链接冻结旧/当前 61 core，
旧/新/旧各预热 1 次、计时 5 次，完整逐字节及 NULL 校验，包含源生成、排序、输出验证和关闭。
三个 p50 为 **811.820/456.265/847.333 ms，约快 1.78–1.86 倍**；共 15 个样本，不能当作 SQL/网络/PG 延迟或大规模并发结果。
在 8 条宽行的 syscall 控制中，输入 EOF 后写入 100,952→67,344 字节，总读取 134,544→100,952 字节；
它们直接证明少一轮数据写入/读取，包含少量私有段头开销，不等同整库 I/O。


本轮另保留 960 次混合诊断：旧/当前/旧各在普通完整排序前后测四类 LIMIT/点查，每类 40 次、预热 3 次、默认耐久性和 15 秒 socket。
全部结果校验通过，但旧末、启动段和各控制明显漂移，未建立“执行一次完整排序后持续变慢”的一致因果关系；这些数据不与独立窗口合并，也不将旧慢值归咎于环境。
反汇编还显示先前四个 executeInternal 版本的栈序言相同（0xd000 探测加 0xf78），没有支持“新增大栈帧”假设；代码布局或其他原因仍未证实。
绑定生命周期缺陷已独立复现和修复，尚无证据证明它就是之前多毫秒慢值的原因。

### 构建、范围与证据

最终程序 SHA-256：`dd5e21a765f87e2a5a8fe432e3443444293d9f86a777d758858e98e78a3fda6e`。
CMake 两次成功重编同一个 ExecutionPlan.cpp 并链接 62 个当前对象；第二次补错误终止清理，第一候选完整保留，未用于最终声明。
只有一个生产 CPP 变化，61 个 core 原生测试和真实 Main 分别验证；没有声称 fresh 编译全部 62。
C++17/O2、ICU/zlib/TLS stub、耐久性、数据格式和 socket 期限保持原配置。
最终按需归并仅用于普通物理 Sort，类型化/多键其他排序、会话 work_mem、其他算子和结果缓冲的资源预算仍开放。
本批不关闭原始 273 项或其原子条款，Goal 保持 active，直接在 master 提交，未推送。

原始证据在 `build/pg-feature-query-oxy0qhou/`：`native-final.json`、`protocol-final.json`、两组新测试旧/新日志、
`protocol-reference.json`、`existing-deferred-comparison.json`、`main-sort-crash-final.json`、`operations/operations.json`、
`mixed-controls.json`、`merge-benchmark.json`、`latency-{default,low64}/comparison.json`、构建日志及 `final-provenance.json`。
复现沿用上一节脚本，将 `--before` 改为本批冻结的 `471dad3f` 程序；新原生源码位于 tests，包装 syscall 的链接注解由现有构建入口读取。

## 会话工作内存与实际排序预算（2026-11-02）

基于 `049c0bce`，按 [PostgreSQL SET](https://www.postgresql.org/docs/18/sql-set.html)、
[RESET](https://www.postgresql.org/docs/18/sql-reset.html)及
[work_mem](https://www.postgresql.org/docs/18/runtime-config-resource.html)的会话/事务语义，补齐原来仅有实例配置的运行接线。
普通物理排序的算法与前一批保持同一职责；这次让每个连接选择自己的预算，不将设置成功等同所有算子均受控。

- 支持 `SET [SESSION|LOCAL] work_mem`、DEFAULT、FROM CURRENT、SHOW、RESET/RESET ALL/DISCARD ALL、启动参数/options 与现有 `pg_settings(setting,unit)` 子集。
  数值按 PG 18.6 的 64..INT_MAX kB、大小写敏感单位和舍入处理；SQL 数值常量与引号内 GUC 文本分别保留各自解释。
  `work_mem` 配置别名使用同一解析器，既有项目 `work_mem_kb` 仍可设置原生微型预算，不把它当 PG SQL 会话别名。
- 状态属于 Session，区分继承文件默认、启动覆盖、显式 SET、事务原值/持久值和本地值；保存点支持重复名称、释放和回滚。
  RESET 继续继承最新重载文件默认，启动 options 的默认优先级保留，FROM CURRENT 则成为显式覆盖。
  顶层隐式语句的失败与显式事务/保存点恢复分别收束；没有把错误清理延至下一次无关查询。
- 普通物理 Sort 在 open 时截取有效预算；输入期间修改设置不影响已打开的排序，重开使用新值。
  SHOW、pg_settings 和现有 EXPLAIN SETTINGS 读取同一有效值，普通 EXPLAIN 文本缓存也区分会话值，避免陈旧或跨连接设置观察。
- 工作内存不是 GUC_REPORT 参数，不新增 ParameterStatus；SHOW 的固定 text 描述无需执行查询读取值。
  新的执行期作用域只区分顶层 SET LOCAL 的显式用户块与协议内部隐式块，块外返回 WARNING/25P01 且不修改设置。
  没有修改何时创建隐式事务、保留计划、Bind/portal/输出格式或 COPY 中断所有权策略。

### 正确性、失败候选与实际资源观察

最终 **36 个原生入口全部通过**：前批 31、两个新入口及 Config/搜索路径/时区语法入口。
新入口完整 **163 个控制**：解析/会话状态 136、实际 Sort/Config 27；其单位/舍入的 82 个字符串输入有独立 PG 18.6 实测，嵌入 NUL 为原生边界检查。
最终真实 Main 的所选完整 **25 个协议入口为 24 通过/1 失败**，新完整 **470 条**在严格 PG 18.6/最终程序全过。
唯一最终失败是已有暂缓 Bind/portal 的 463/40；本批与前版全部 463 条观察逐字节相同。
`b190` 的完整同组 25 曾为 23/2，另一个是部分 COPY 帧超时关闭检查；最终完整入口通过不改写原失败，
没有恢复 R197 的专属调查或宣称它已修复。R303/R304/R305、原子条款及专属暂缓记录保持。

同一最终 Main 的 35 类操作完成现有 oracle 检查，仅作有限 smoke；实际匿名排序文件存活时强杀，
重开完整核对 1,000 行/4,000 单元格并再次排序通过，不概括为全部修改故障/恢复、真实 TLS 或整库兼容性验收。

首候选 `11486506` 的完整 448 条有 **8 处不符**：2 个块外 SET LOCAL 警告、3 个缓存 EXPLAIN 值、2 个 Simple RESET 语法错误码、1 个 Extended SHOW 语法错误阶段。
前两处警告分别是 01000 和未发送警告；其值/行/Ready 检查仍正确。旧/PG/首候选同源完整结果和实际首候选程序均保留。
随后补普通缓存键、Simple 的原语法校验、SHOW 42601 及独立执行期显式块上下文，再完整重跑；没有将断言改成多个可接受值/帧。
原 391 条保持步骤/断言及顺序，追加 13 个缓存和 44 个语法控制后成为 448；该 448 条旧版完整 382 不符，不等同 382 个独立 bug。
进一步核对 [PG SET 执行顺序](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/misc/guc_funcs.c)，发现块外 LOCAL 的非法值/非法多值也必须先警告再报错。
新增独立 22 项，严格校验 Simple 的 N/E/Z、Extended 的 1/2/n/N/E/Z 及语法错误无警告；
原 448 控制与断言保持，扩展为 **470**，PG 全过、旧版 404 不符，`b190c065` 虽通过 448 仍有新增 4 处不符。
保留该候选和全部完整日志，再移动警告至值校验前，没有通过删除错误路径关闭这个差异。
第一版检查器另有 143 个 bytes/str 字段名期待错误，已保留原源码/PG 日志；只修检查器表示后，原 391 在 PG 全过，再扩展 448。

实际 Main 使用测试进程的 LD_PRELOAD/mkstemp 计数，只观察 `.dbms-sort-` 创建，不含生产注入 hook，也不用于计时。
31 个完整控制核对两个连接、64 KiB 与 4 MiB、LOCAL 结束恢复、文件重载/显式值/RESET/FROM CURRENT：
低预算实际 SQL 每次创建一个匿名排序文件，高预算为零，完成后无临时描述符；最后完整核对 1,000 行/4,000 单元格。
原生另完整核验 512×2 KiB、>2^53 整数、三列 NULL、空行来源、两个同时存在的排序、预算打开时快照及错误清理；不把合成来源伪装为堆 RID。

### 性能与尚未完成的范围

沿用闭合 1,000 行夹具，各窗口旧/新/旧独立副本，七类各预热 3 次、3 轮×4 样本，每窗口 252，合计 **504 个完整校验样本**。
默认耐久性、15 秒 socket、TCP_NODELAY 不变；计时阶段没有本任务编译、测试或其他基准并行，没有重测 PG 延迟。

默认 4 MiB，p50 ms：

| 查询 | 旧首 | 新 | 旧末 |
| --- | ---: | ---: | ---: |
| 普通物理单键无界排序 | 75.976 | 84.046 | 82.820 |
| 类型化双键 LIMIT 20 | 34.814 | 42.266 | 37.495 |
| 同排序 OFFSET 500 | 34.107 | 40.169 | 39.469 |
| 其他分流的双键无界排序 | 90.779 | 94.554 | 96.302 |
| 主键点查 | 10.495 | 14.206 | 11.913 |
| ARRAY[id] LIMIT 1 | 10.768 | 12.779 | 12.931 |
| 普通 id LIMIT 1 | 9.740 | 11.486 | 11.242 |

64 KiB，p50 ms：

| 查询 | 旧首 | 新 | 旧末 |
| --- | ---: | ---: | ---: |
| 普通物理单键无界排序 | 77.242 | 69.557 | 77.659 |
| 类型化双键 LIMIT 20 | 36.637 | 34.599 | 33.041 |
| 同排序 OFFSET 500 | 36.433 | 35.434 | 35.795 |
| 其他分流的双键无界排序 | 90.035 | 80.373 | 87.935 |
| 主键点查 | 12.462 | 11.798 | 11.297 |
| ARRAY[id] LIMIT 1 | 11.991 | 11.781 | 12.118 |
| 普通 id LIMIT 1 | 10.681 | 9.896 | 10.926 |

默认窗口 Top-N 为 34.814/42.266/37.495 ms、点查 10.495/14.206/11.913 ms，存在明显慢值；
64 KiB 的普通单键排序为 77.242/69.557/77.659 ms。**未证明 SQL 延迟稳定改善或所有查询无回退**，
不能把不同窗口的较低值挑出后推广为全面加速，也未证明全部差异由环境造成。

另有 **600 次**默认预算诊断，旧/新/旧每类 50 查询，CPU 按服务进程的每批累计计数折算：

| 查询 | 旧首 CPU ms/次 | 新 | 旧末 |
| --- | ---: | ---: | ---: |
| 双键 LIMIT | 31.800 | 30.000 | 31.800 |
| 点查 | 8.400 | 8.800 | 7.800 |
| 普通 LIMIT | 6.600 | 10.200 | 9.800 |
| 数组 LIMIT | 9.600 | 10.600 | 10.800 |

这些不是逐请求 CPU 中位数，不与 504 延迟样本合并。旧末普通 LIMIT 和数组控制也发生漂移，尚未定位既有慢值的共同原因。
本批的确定收益是会话可独立控制实际普通排序预算与生命周期；查询热路径、其他算子的预算及剩余开销继续优化。


配置函数 `current_setting`/`set_config` 仍没有实现，函数 SET 子句与内部 CALL/DML 保存点的完整 GUC 联动尚未验收；
这些是普通未完成工作，不新增用户暂缓项。语法诊断的消息文本/位置还未逐项对齐 PG，work_mem 的共享标量解析分支仍有 TimeZone 措辞。
会话 work_mem 已接普通物理 Sort，类型化/多键/Top-N 其他分流及 hash/aggregate/window/join、结果接收器仍缺完整预算。
没有据本批关闭整个资源治理要求、原始 273 项或完整 PG 兼容性门；Goal 保持 active。

### 构建与证据

反汇编检查显示 dd5/114/b190 三份 executeInternal 的栈序言均为 0xd000 探测加 0xf78；
新设置 helper 在调用处均为独立函数调用，没有支持“新 helper 内联扩大主函数栈”的假设，因此没有加入无证据的 noinline 修改。
代码布局是否影响延迟仍未证实。

最终程序 SHA-256 为 `af67a1cfd6c7e4ce2774e23345e288acaa366038054bd738b81dd97eb04bc8fb`。
公共 Session 头及新增工作内存状态使首次 CMake 重编 16 个生产单元；后续两次 Main 单元重编和一次 Main/Network/Parser 重编，
合计 21 次成功 CPP 编译、16 个不同 CPP，最终链接 62 个当前生产对象，未称 fresh 全部编译。
最后仅移动 Main 中警告顺序，逐对象核对原生所链接的 61 个 core 哈希相同，复用明确不含 Main 的最终 36 原生结果；实际 Main 完整重跑协议/资源/操作/强杀观察。
C++17/O2/ICU/zlib/TLS stub 的构建范围保持，原始 273 项及专属记录不变；验收后原子安装同一程序，直接在 master 提交，不推送或重启既有服务。

证据目录 `build/pg-feature-workmem-_a2m0fg2/`：`native-final.json`、`protocol-final.json`、391/448/470 的旧/PG/各候选完整日志与冻结源码，
`parser-reference-observations.json`、`parser-reference-state-receipt.json`、`runtime-main-accepted.json`、`operations-accepted/operations.json`、
`main-sort-crash-accepted.json`、延迟窗口、实际构建记录及 `final-provenance.json`。

## 普通类型化排序的预算与比较缓存（2026-11-02）

本批输入为 `d8533bc9`，冻结旧程序 `af67a1cf…`，最终程序 `6d6f1724…`。
目标是约束普通类型化排序的输入保留量，减少重复比较准备，并补 NaN WITH TIES 的实际差异；原始 273 项没有关闭。

### 调研与实现

[PostgreSQL 18 tuplesort](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/sort/tuplesort.c)按调用方工作预算生成有序段、有限路归并，并在顺序消费者请求时执行最终归并。
[DuckDB 排序设计](https://duckdb.org/2025/09/24/sorting-again)将可溢写布局与比较开销一起考虑；
[SQLite 临时文件说明](https://www.sqlite.org/tempfiles.html)区分排序临时结构与事务日志。
本批采用按操作预算与 owning 快照，不实现 DuckDB 的 normalized keys/并行排序，也没有更改本项目的持久化/WAL 格式。

- [BoundedSortRuns.h](../src/executor/BoundedSortRuns.h)把拥有值容量、vector 容量和行上下文计入批次估算，按预算生成段、最多两个匿名已 unlink 文件、有限读头归并。TopN 能放入预算时保留候选堆，超预算转段；之前已淘汰的行仍被保留候选支配。最终上限在下一次 pending refill 前检查。
- [TypedSortSpill.h](../src/executor/TypedSortSpill.h)保留 type/NULL/collation、对象及数组 OID、递归 record、四类 RowContext 值容器、已求值的键/提前投影及输入 ordinal。只序列化 owning 值，不搬动 AST、执行器、参数帧、provider 或 cursor 的所有者。physical cells 不再另存重复副本；Filter 传递真实 typed 值。
- [ExecutionPlan.cpp](../src/executor/ExecutionPlan.cpp)在原比较需求处惰性缓存每键最近类型/排序规则描述符，不改成 double 比较 NUMERIC；同一排序比较定义普通 float4/float8 WITH TIES，修复两项 NaN peer 差异。非 key volatile 仍在输出需求处执行，必要键仍检查全部输入。
- [main.cpp](../src/main.cpp)在执行阶段接通受支持的普通无 LIMIT 多键、表达式及没有单物理键快路径的类型。已有单键物理快路径保留；聚合/HAVING/SetSort 及其 provider/生命周期不在此次接线中。[parser.cpp](../src/parser/parser.cpp)仅修 work_mem 无效语法误写成 TimeZone 的消息，不声明完整 PG 诊断文本/位置一致。

### 内存、资源与正确性

相同 64 KiB 设置、12,000 行×2 KiB payload、真实 child EOF 的活跃分配增量如下；全部值/顺序/NULL/行数继续核验。

| 普通类型化路径 | 冻结旧版 bytes | 最终版 bytes |
| --- | ---: | ---: |
| 大 LIMIT | 73,910,976 | 18,064 |
| DISTINCT | 42,615,632 | 14,768 |
| WITH TIES | 71,604,640 | 50,048 |
| 无界多键 | 73,906,016 | 18,064 |

这不是查询/进程峰值。批次预留约 25% 估算空间，但稳定排序 scratch 与分配器开销没有逐字节纳入 Codec；单个超大行、固定归并读头、上游、AST 和结果接收器还可额外占用。
低预算 SourceContexts 控制实际观测约 155 KB 活跃分配，而不是 64 KiB 硬 RSS 上限。其他算子的完整内存治理仍开放。

最终 39 个原生入口通过，新增 codec 51、通用段 17、真实算子 17 共 85 控制全过。
原 TopN 八个 SQL/输出/需求检查保留，资源要求改为同 64 KiB 下全部小于 1 MiB；旧版四项失败、新版全过。
原八项旧源码和“必须保留全输入”的四个旧资源断言、旧默认预算、候选四失败也完整保留，不能把预期资源契约变化称为四项生产错误。
实际 SourceRows/SourceContexts、OID/嵌套 record、NUMERIC >2^53、TopN、空需求、同图重开、实际 CTE/derived VALUES 和真实堆 volatile 需求均核验。
JOIN 控制使用真实绑定上下文，不表示公共 buildPreparedQueryPlan 已新增 JOIN 算法。

所选完整 26 协议为 25 通过/1 失败；新固定 108 在严格 PG 18.6/最终程序均全过，旧版仅两个 NaN ties 不符。
原 80 顺序与预期不变，追加 28 个无 ARRAY 的普通无 LIMIT 入口；保留正常 text/binary、不限额 Execute。
唯一失败为已有暂缓 Bind/portal 完整 463/40，全部观察与上一批逐字节相同；R197/R303/R304/R305 没有恢复调查。
实际 Main 同一九项旧版六项低预算资源失败、最终九项全过：六项低预算确有匿名文件、三项高预算无文件，完整值正确、最多两个 FD 并全部释放。
35 类操作有限检查通过；实际普通多键类型化排序匿名文件仍打开时 SIGKILL，重开核对 1,000 行/4,000 单元格并再次排序通过。这不是任意修改崩溃或整库恢复验收。

初次完整 38 原生有一个入口失败：新回调用 std::sort，原 80 中十六项 stable_sort 分配观察未发生。
恢复原批次稳定排序回调后最终 80 全过，共享模板/原断言字节不变；没有重开 R185 的旧专项诊断。
初始 codec 声明、AST 字段/命名空间编译错误、NAME 声明/SHOW 标签/statement 名和算子夹具观察时点的失败均保留。
输入 NAME 改用受支持的 cast 后核验，不声称修复 NAME 物理声明；所有最终断言保持单一期望。

### 相邻 SQL 延迟：所有案例与慢值

同一闭合的 1,000 行夹具、共享主机（48 逻辑 CPU）、GCC 13.3/-O2/C++17、ICU/zlib、TLS stub。
每个预算窗口为旧/新/旧独立副本，11 类各 3 轮×4 次，三个 label 共 396 次计时；每 label 每类三次 warmup，固定种子打乱次序。
显式 SET/SHOW work_mem，原默认持久化/后台维护、15 秒 socket 与 TCP_NODELAY 保留。计时含全部结果校验，夹具准备在外。
类型化全排序含 ARRAY 输出以保持旧/新相同接入；普通无 ARRAY 的实际落盘由上述九项独立验证。
NUMERIC 性能夹具是常规小值，超 2^53 精度由独立正确性测试验证，不把两者混为同一性能负载。

首测为 64kB、默认值 4MB、高值 16MB，共 1,188 次。
16MB 的旧基线及点查控制出现大幅漂移，随后仅按完全相同条件增加一次完整 396 次复测；原首测没有被替换。
合计 1,584 次计时、396 次 warmup、24 次 SET/SHOW，即 2,004 条 SQL 全值/前置校验均通过，源夹具 1,072 个文件哈希不变。
表中均为每段全部十二个样本的 p50 ms；没有置信区间，不能从小表共享主机样本推导并发/大数据普遍收益。

#### 64kB

| 操作 | 旧版先测 ms | 最终版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| C 文本多键全排序 | 162.263 | 155.436 | 177.105 |
| NUMERIC 多键全排序 | 227.480 | 229.655 | 231.684 |
| float8 多键全排序 | 285.572 | 293.548 | 286.770 |
| 取模表达式全排序 | 149.936 | 163.022 | 155.354 |
| 整数多键全排序 | 136.740 | 147.104 | 142.231 |
| 大 LIMIT 10000 | 146.019 | 150.391 | 141.790 |
| LIMIT 20 OFFSET 500 | 117.882 | 122.106 | 115.607 |
| DISTINCT 后 LIMIT | 40.016 | 47.655 | 39.674 |
| WITH TIES | 66.879 | 74.814 | 67.158 |
| 整数 TopN 控制 | 37.712 | 35.178 | 35.432 |
| 主键点查控制 | 13.210 | 11.864 | 14.362 |

#### 4MB（默认值）

| 操作 | 旧版先测 ms | 最终版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| C 文本多键全排序 | 162.299 | 150.548 | 163.762 |
| NUMERIC 多键全排序 | 221.335 | 210.214 | 221.526 |
| float8 多键全排序 | 280.128 | 249.347 | 287.877 |
| 取模表达式全排序 | 147.507 | 150.333 | 149.163 |
| 整数多键全排序 | 135.663 | 141.992 | 135.832 |
| 大 LIMIT 10000 | 133.343 | 143.584 | 133.667 |
| LIMIT 20 OFFSET 500 | 114.208 | 120.503 | 124.413 |
| DISTINCT 后 LIMIT | 37.323 | 42.030 | 37.634 |
| WITH TIES | 63.615 | 69.909 | 63.920 |
| 整数 TopN 控制 | 33.106 | 37.797 | 35.589 |
| 主键点查控制 | 10.623 | 11.641 | 11.511 |

#### 16MB 首测（基线漂移）

| 操作 | 旧版先测 ms | 最终版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| C 文本多键全排序 | 669.305 | 146.135 | 667.971 |
| NUMERIC 多键全排序 | 657.308 | 219.671 | 305.462 |
| float8 多键全排序 | 764.393 | 282.616 | 360.045 |
| 取模表达式全排序 | 361.609 | 158.837 | 457.788 |
| 整数多键全排序 | 411.766 | 141.772 | 348.307 |
| 大 LIMIT 10000 | 619.482 | 141.261 | 404.240 |
| LIMIT 20 OFFSET 500 | 569.729 | 120.514 | 302.633 |
| DISTINCT 后 LIMIT | 648.548 | 42.807 | 202.819 |
| WITH TIES | 269.703 | 65.953 | 192.876 |
| 整数 TopN 控制 | 340.562 | 35.545 | 47.400 |
| 主键点查控制 | 551.628 | 11.341 | 18.019 |

#### 16MB 唯一复测

| 操作 | 旧版先测 ms | 最终版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| C 文本多键全排序 | 162.492 | 141.371 | 172.210 |
| NUMERIC 多键全排序 | 227.946 | 212.380 | 230.624 |
| float8 多键全排序 | 293.755 | 255.990 | 313.377 |
| 取模表达式全排序 | 153.890 | 163.260 | 168.170 |
| 整数多键全排序 | 152.355 | 140.367 | 172.918 |
| 大 LIMIT 10000 | 151.798 | 148.697 | 154.604 |
| LIMIT 20 OFFSET 500 | 115.575 | 114.292 | 120.866 |
| DISTINCT 后 LIMIT | 41.997 | 38.469 | 44.082 |
| WITH TIES | 75.541 | 75.006 | 78.764 |
| 整数 TopN 控制 | 37.792 | 35.852 | 39.168 |
| 主键点查控制 | 12.087 | 28.488 | 16.041 |

默认 4MB 的文本/NUMERIC/float8 全排序在本次窗口有所改善，其他若干路径变慢；64kB 外排的 I/O/编码成本也使多项变慢。
16MB 首测连点查都出现 551.628/11.341/18.019 ms，不把其巨大倍率归因于本次代码。
唯一复测的文本为 162.492/141.371/172.210 ms，但点查仍是 12.087/28.488/16.041 ms；较慢控制与波动原因尚未解决。
复测前后主机/既有进程 CPU/I/O 快照保留，这些计数不能证明初测漂移原因，也不能证明控制项变化与本批无关。
资源保留量改善已证实；不能宣称所有 SQL 稳定提速、无回退或达到 PostgreSQL 性能水平。

### 构建来源与复现

CMake 共 17 次成功 C++ 对象编译、16 个不同 CPP，最终链接全 62 对象（61 core+Main）；最后标准 CMake target 仅链接、零额外重编译。
初次 Main 失败保留，修正后的 Main 输入逐字节冻结；core 构建期间 Main 单独修正的差异另列审计，实际 core 输入不变。
后续仅 ExecutionPlan 的新回调重编译；最终原生 39 和实际 Main 26 使用相同冻结 core/程序。
最终输入、62 对象、61 core、测试来源和产物 SHA 见本批 `release-binary.json`、`native-release-core-hashes.json`、`final-provenance.json`。

可复现驱动为 [benchmark_typed_sort.py](../scripts/benchmark_typed_sort.py)，沿用 [benchmark_queries.py](../scripts/benchmark_queries.py) 的真实服务/客户端。
计时驱动 `59792eb4…` 与仓库驱动 `9a8d8e0b…` 只因所在目录不同改变 REPO parents[2]→parents[1]；逆替换源码与规范化 AST 全等，--help/py_compile 通过。
使用冻结旧二进制、同一个已关闭夹具和每次全新输出目录，例如：

```bash
python3 scripts/benchmark_typed_sort.py \
  --before build/pg-feature-typed-sort-zm9x0zst/before-dbms_main \
  --after build/cmake/dbms_main \
  --fixture build/pg-feature-topn-20261102-bc582b_s/fixture-1000 \
  --work-mem 64kB --rows 1000 --rounds 3 --samples 4 \
  --output build/retest-typed-sort-64k
```

4MB/16MB 使用相同命令的对应预算和新目录；这会启动私有副本，不打开源夹具，也不重启用户服务。
新建夹具可用既有 benchmark_topn.py 的无 --fixture 模式，但新夹具的历史/WAL/缓存不同，不能冒称与本次冻结输入字节相同。
本机证据目录 `build/pg-feature-typed-sort-zm9x0zst/` 保留四个 `typed-latency-*/comparison.json` 全部样本、
初测独立汇总、复测/主机快照、原版本与每个失败候选、PG108、39/26 汇总、实际 Main 九项和强杀重开记录。
这些 build 证据是本机保留工件；仓库提交测试与驱动，不能把未打包工件当作可分发发布物。

聚合/GROUP/HAVING/SetSort、窗口/哈希/JOIN 本身、结果接收器、虚拟生成列/未接入旧分流、完整配置函数/GUC 和其他原始功能族继续开放。
原始 273 项、原子要求与专属暂缓原文不变，Goal 继续 active；无新 Markdown、分支或推送。

## 集合排序预算与 VALUES 查询子句（2026-11-02）

本批以 `8a8532a0` 为输入，冻结旧程序 `6d6f1724…`，最终程序为 `a3bdab66…`。
产品仍为 0.2.0 / C++17、PG18.6 行为基线 / wire3.0，GCC13.3/-O2、ICU/zlib、TLS stub；不将本批写成整库 PG 等价或 TLS 验收。

### 调研与实现边界

| 一手来源 | 本批采用的做法与限制 |
| --- | --- |
| [PostgreSQL18.6 Sort](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeSort.c) | 从真实输出描述符和 work_mem 初始化排序，首次实际需求才读取输入；静态 LIMIT 可给有界候选，仍处理所需全部输入。 |
| [PostgreSQL VALUES](https://www.postgresql.org/docs/18/sql-values.html)与[分析器](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/analyze.c) | VALUES 各列先选择共同类型，随后注册真实输出范围供排序使用；不把列名称伪装成运行时参数或物理表。 |
| [PostgreSQL Limit](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeLimit.c)与[计数分析](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/parse_clause.c) | 计数是 bigint 表达式，拒绝本查询层变量，按每次实际调用求 OFFSET 再求 LIMIT；零条不要求行源，WITH TIES 使用真实边界键。 |
| [SQLite vdbesort](https://github.com/sqlite/sqlite/blob/master/src/vdbesort.c) | 超预算生成有序段，记录同时保存排序键和输出；复用本项目完整 owning codec，保持 NULL、collation、OID、数组和递归 record 元数据。 |
| [DuckDB 排序设计](https://duckdb.org/2025/09/24/sorting-again) | 比较开销和可溢写布局同样影响速度；本批只惰性缓存原表达式比较描述符，没有实现其字节规范化键或并行向量执行。 |

[ExecutionPlan.cpp](../src/executor/ExecutionPlan.cpp)的普通 UNION ALL SetSort 复用现有 BoundedRuns/TypedSortRowCodec，
按打开时的会话预算分批、最多两个匿名文件 FD、稳定 ordinal、最终按需归并。
存在非零静态 LIMIT、没有 WITH TIES、LIMIT+OFFSET 加法安全时保留有界候选；动态 VALUES 计数目前使用完整有预算排序，没有动态下推 Top-N。
键比较仍由原表达式/绑定类型所有者执行；scalar float4/float8 的 NaN/Infinity 与枚举次序采用真实比较元数据，嵌套复合类型的其他既有比较缺口不因此关闭。

VALUES 的原 token/span、共同类型、真实 column1/column2 范围、关联子查询及隐藏排序键均保留；
LIMIT/OFFSET 表达式在实际生产者的同一执行器中求值，新调用由既有 VALUES owner 刷新自己的 carrier，无新参数帧所有者。
[prepared_query_execution.cpp](../src/expression/prepared_query_execution.cpp)只新增两项 AST 子节点索引，原 retained constant-planning 算法不改。
原 PreparedAppend 整类/执行器工厂 owner 前缀、共享 CPU-sort helpers 和用户暂缓模块保持原字节。

[DmlResult](../src/commands/DmlExecutor.h)拥有真实输出描述符，正常 Network 描述及结果传递实际 OID/typmod/目录长度；
验证为 scalar enum 的二进制输出发送标签字节，保留物理 table/attribute origin 与 NULL/空标签/文本 NULL 区别。
输入解码器、既有 Parse/Bind/portal/format2 策略及 handleClient 主体不改。未知 OID 计数参数推导、enum 数组二进制输入/输出及任意自定义类型 codec 仍不完整。

### 正确性、失败与实际资源

最终 44 个原生入口全部通过；新 SetSort26、grammar860、VALUES execution63 共 949 控制全部通过。
同最终源码完整旧 ABI/旧头/旧 stub 基线分别有 20/694/63 项不符，不能把控制数当成独立缺陷数。

| 固定协议矩阵 | PG18.6 | 最终程序 | 冻结旧版不符 |
| --- | ---: | ---: | ---: |
| 集合排序112 | 112/0 | 112/0 | 8 |
| VALUES查询子句143 | 143/0 | 143/0 | 127 |
| 实际枚举描述符33 | 33/0 | 33/0 | 20 |
| 显式输入 OID 计数59 | 59/0 | 59/0 | 51 |
| 计数类型/语法阶段52 | 52/0 | 52/0 | 48 |

原 31 个协议入口整批首轮为 29通过/2失败；暂缓 Bind/portal 完整 463 项及其中 40 差异整份日志与前版逐字节相同。
另一项在 runner.start_ours 的 sock.connect、发送任何 SQL 之前 ConnectionAbortedError；同程序/源码完整独立重跑通过，最终逐入口复核为 30/1。
原始整批 JSON/连接错误留存，没有改启动重试、socket超时或网络策略。新增52永久入口另按同一程序验证，全32个已运行入口按各自最终结果复核为31/1；不把两次验证冒称一次全库测试。

记录全部失败而不放宽成多种可接受结果：

- C1 SetSort26 有两处真实取消/超时清理失败，修复原 TIES 包装器在成功取得一行后的主异常清理，C2起26/0。
- C2 set112仍有4处 VALUES语法/枚举 RowDescriptionOID 差异；C3 enum33/set112/旧typed108全过，但VALUES143仍7处失败。补真实 Main scalar VALUES admission与计数AST/绑定后最终143/0。
- C3原生44中的两个失败为 grammar child/span oracle 与 collation规范值夹具；按真实单一 AST/运行期 c修正，原始620/14和30/4保留。C4的4个::AST形状及4个参数槽位预期也仅修fixture，参数仍按真实PreparedQuery.uses关联，没有重排生产帧。
- C4 grammar860中缺操作数两处是真语法错误；type52探针完成49观察后在Extended Parse不完整CASE处SIGABRT，另外3观察未执行。修复局部语法边界/异常返回与结构化42601，最终相同完整52/0；绝不将该首轮称作完整52项结果。
- 实际PG/C4反例证明括号inline VALUES集合的全局表达式计数曾被静默忽略；现已解析到该形状时明确0A000，仍列未实现，未括号全局形式还可能42601。9个局部/静态正对照与PG一致；额外工具对未括号形式的错误预测及全部13观察保留。

实际 Main 同一七项全部4,107行/12,321单元值正确。旧版4项资源失败；最终低64kB全排/OFFSET/TIES/超大行实际落盘，高16MB全排/OFFSET和低预算LIMIT1无文件，最多2 FD且结束全部关闭/无命名残留。
首轮采样曾截到mkstemp→unlink间的瞬时命名FD；工具原“所有瞬时目标必须已deleted”断言失败，原源码/报告保留。
修正后的观察器严格校验实际数据目录归属、同FD/path随后deleted或确实关闭且路径消失，且spill必须有真实deleted证据；SQL、预算、全值、FD上限、最终清理要求不变，旧/新完整7项重新验证。
生产匿名文件 helper没有为此修改。预算按拥有值估算，稳定排序scratch、分配器、超大行/固定读头、上游与结果接收器仍可额外占用，不能称硬RSS上限。

35类操作现有有限检查全部通过；真实全局SetSort匿名文件仍打开时，仅向验证exe/data-dir/start-ticks的自建PID发送SIGKILL，
同一私有目录重开核对1,000行/4,000单元，再次核对2,000行集合排序及清理。源fixture1,072文件SHA不变；不是任意修改崩溃矩阵或全库恢复验收。

### 相邻旧/新/旧 SQL 延迟：全部操作

同一已关闭1,000行fixture，每次独立私有副本；集合排序实际有两个分支，共2,000行输入，三列完整输出。
11类各3轮×4次、旧/新/旧共396计时/预算；64kB、4MB、16MB共1,188计时、297预热、18条SET/SHOW，合计1,503条全值/预算校验通过。
固定种子打乱轮内次序，保留默认持久化/后台维护、15秒socket与TCP_NODELAY；准备在计时外，完整结果校验在计时内。
计时前所有自建编译/回归/资源进程已终态，窗口内不启动其他测试或编译；共享主机48逻辑CPU，非并发/冷缓存/大数据验收，无置信区间。
同行顺序只属于冻结heap夹具，不承诺没有完整ORDER键的生产查询固定子集；NUMERIC性能夹具是常规值，超2^53精度另由正确性测试验证。
表中为全部12样本p50毫秒，没有删慢值或追加择优窗口。

#### 64kB

| 操作 | 旧版先测 ms | 最终版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| 文本全排序 | 257.193 | 296.194 | 249.444 |
| NUMERIC 全排序 | 380.867 | 410.851 | 374.816 |
| float8 全排序 | 454.178 | 489.810 | 441.665 |
| float4 全排序 | 397.745 | 383.115 | 396.692 |
| 整数全排序 | 241.006 | 268.703 | 228.674 |
| 固定夹具同行稳定排序 | 247.592 | 272.855 | 237.755 |
| LIMIT 20 | 195.501 | 181.917 | 185.988 |
| LIMIT 20 OFFSET 500 | 196.011 | 198.347 | 188.215 |
| WITH TIES | 204.690 | 201.640 | 198.980 |
| 普通整数 TopN 控制 | 35.369 | 35.428 | 34.236 |
| 主键点查控制 | 11.945 | 12.600 | 11.653 |

#### 4MB

| 操作 | 旧版先测 ms | 最终版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| 文本全排序 | 253.475 | 260.524 | 255.186 |
| NUMERIC 全排序 | 380.262 | 379.447 | 388.789 |
| float8 全排序 | 443.540 | 436.245 | 453.670 |
| float4 全排序 | 399.986 | 366.521 | 399.462 |
| 整数全排序 | 234.316 | 243.532 | 236.073 |
| 固定夹具同行稳定排序 | 257.177 | 249.853 | 246.380 |
| LIMIT 20 | 192.492 | 185.582 | 195.918 |
| LIMIT 20 OFFSET 500 | 187.691 | 233.081 | 189.220 |
| WITH TIES | 200.362 | 208.650 | 225.586 |
| 普通整数 TopN 控制 | 36.174 | 34.579 | 36.437 |
| 主键点查控制 | 12.045 | 13.442 | 13.992 |

#### 16MB

| 操作 | 旧版先测 ms | 最终版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| 文本全排序 | 250.781 | 264.514 | 251.167 |
| NUMERIC 全排序 | 376.221 | 388.562 | 377.866 |
| float8 全排序 | 437.895 | 444.773 | 443.356 |
| float4 全排序 | 397.567 | 361.624 | 390.029 |
| 整数全排序 | 229.612 | 239.280 | 229.361 |
| 固定夹具同行稳定排序 | 242.945 | 235.692 | 243.289 |
| LIMIT 20 | 196.006 | 191.012 | 194.417 |
| LIMIT 20 OFFSET 500 | 195.155 | 227.381 | 189.901 |
| WITH TIES | 195.336 | 210.169 | 207.504 |
| 普通整数 TopN 控制 | 33.919 | 35.731 | 35.035 |
| 主键点查控制 | 12.705 | 11.793 | 12.581 |

float4全排序在本窗口改善；4MB的LIMIT20为192.492/185.582/195.918ms，幅度有限。
低64kB的多项全排变慢，旧SetSort不执行该预算而新版承担临时编码/I/O成本；资源约束不能当成普遍加速。
即使4MB/16MB不以落盘解释，也有未解决慢值：OFFSET分别187.691/233.081/189.220ms与195.155/227.381/189.901ms，文本/整数全排及部分控制也较慢。
有界堆、比较临时上下文/值拷贝及其他执行成本仍需后续profiling与优化；本批未证明全部查询提速、无回退或达到PostgreSQL性能水平。

### 构建与复现

C3/C4/C5标准CMake target分别523.40/522.38/32.60秒，成功重编17/16/3对象，共36次/17不同CPP，最终链接62对象（61core+Main）。
这是按依赖重建，不声称62对象全部fresh；AST/DmlResult ABI变化的所有依赖者与原生test_stubs使用同代头和对象。
最后仅三CPP变化，Main不因此重编；最终44原生与实际Main所有协议/资源使用相同冻结61core/程序。
全部212输入源码、62对象、61core、测试/驱动及工件SHA保存在`final-provenance.json`；初始const Expr*编译错误及所有候选失败留存。

复现驱动[benchmark_set_sort.py](../scripts/benchmark_set_sort.py)复用[benchmark_queries.py](../scripts/benchmark_queries.py)真实服务/客户端。
计时artifact脚本1099dfa1…与仓库脚本39b1450e…仅REPO.parents[2]→[1]，源码逆替换和AST归一完全相等。

```bash
python3 scripts/benchmark_set_sort.py \
  --before build/pg-feature-set-sort-wi9y4qzg/before-dbms_main \
  --after build/cmake/dbms_main \
  --fixture build/pg-feature-topn-20261102-bc582b_s/fixture-1000 \
  --work-mem 64kB --rows 1000 --rounds 3 --samples 4 \
  --output build/retest-set-sort-64k
```

4MB/16MB使用对应参数与新目录。旧程序、fixture与完整raw样本/PG对照/失败/资源/crash/source snapshots在本机`build/pg-feature-set-sort-wi9y4qzg/`，不是已打包可分发发布物；仓库提交永久测试、驱动和文档。
原始273 items/原子要求/725受保护记录未改，R185/R197/R244/R303/R304/R305等用户暂缓保持；Goal继续active。
聚合/其他集合分流/窗口/哈希/JOIN/结果接收器/完整GUC与其他原始功能族继续开放；无新Markdown、分支或推送。

## 集合整数键缓存与比较参数复制（2026-11-02）

输入bb74ae97/前版a3bdab66…，最终95defdc5…；仍为C++17/product0.2.0/PG18.6基线、GCC13.3/-O2、ICU/zlib、TLS stub。
本批针对前次OFFSET慢值先测真实算子，再仅修改ExecutionPlan.cpp的类型化键缓存与比较参数复制。

### 一手调研与实际计数

[PG18.6 tuplesort](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/sort/tuplesort.c)在输入超过2K等条件时转堆；
[DuckDB1.5.6](https://github.com/duckdb/duckdb/blob/v1.5.6/src/optimizer/topn_optimizer.cpp)结合上限/比例选择TopN；
[SQLite3.50.4](https://github.com/sqlite/sqlite/blob/version-3.50.4/src/select.c)以LIMIT+OFFSET控制临时有序索引。
PG/DuckDB在N2000/K520的本例仍选TopN，不能说照抄阈值就能消除回退。
[DuckDB排序设计](https://duckdb.org/2025/09/24/sorting-again)强调紧凑类型化键；本项目只复用精确整数缓存，完整输出值仍拥有自己的metadata。

冻结私有计数overlay不改正式堆/中断helper；两个真实1000行UNION ALL分支、三键amountDESC/idASC/branchASC与独立oracle，完整4040行/16160单元校验通过。

| 输入/路径 | less调用（前/新相同） | 前版标量探测 | 新版标量探测 |
| --- | ---: | ---: | ---: |
| 固定夹具全排 | 12561 | 19553 | 0 |
| 固定夹具OFFSET/K520 | 17040 | 27475 | 0 |
| 固定洗牌全排 | 21511 | 37282 | 0 |
| 固定洗牌OFFSET/K520 | 14367 | 23247 | 0 |

replacement/rejection、每阶段比较和容量遍历次数也相同；没有记录逐比较decision trace，不以计数相同替代完整值验证。
原夹具OFFSET有998次替换/482拒绝，洗牌则687/793；前者增加比较，后者减少，故保留堆策略并降低每次比较成本。
这些命中整数缓存的样本合计107557次表达式探测归零，不代表其他类型/全查询不再调用求值器。
排序phase的new请求186049/291154/391674/246365→13/12/13/12，仅覆盖Runs阶段，不含add之前的缓存构造；不能说全查询零分配。
系统perf_event_paranoid4拒绝perf，本批未改全局设置。instrumented数据只作计数，不用于速度结论。

精确raw smallint/integer/bigint在必要表达式求值之后，用from_chars全串及int16/int32范围检查缓存int64；只有同原类型、非enum且两缓存有效才用关系比较，避免差值溢出。
NULL/方向/ordinal与TIES原等号owner保持；非法/越界值不提前抛错，域/OID/NUMERIC/float等仍走原比较器，不扩类型转换规则。
缓存直接关联原cells/隐藏keys，不复制另一份输出键；私有codec独立验证integerCount，ordinary TypedSort三参数描述符仍默认cacheCount=keys。
K个optional缓存容量计入预算，普通全非整数键Set不加该向量，VALUES复用原槽位；fixed codec对象增16B，缓存不是零内存/硬RSS上限。
原比较使用每次自己的owning vector，再move入RowContext，省去initializer_list额外深拷贝；不跨比较缓存datum/上下文/帧，求值器仍可能复制值。

### 实际分配、资源与回归

同源码真实算子两次同图open并改变输入，独立核对完整递归metadata；下列为epoch0从open到首输出的C++new累计请求，不是活跃内存、RSS或CPU时间。

| 路径 | 前版请求数/bytes | 新版请求数/bytes |
| --- | ---: | ---: |
| 128行整数全排 | 22761 / 817676 | 8468 / 499584 |
| 128行整数OFFSET | 17890 / 662245 | 8467 / 439088 |
| 128行record/8191B尾/OID257 | 见完整记录 / 237506623 | 见完整记录 / 181385965 |

record EOF后累计请求204125226→148004552B，特殊payload/OID尺寸请求各6168→4468；N1无比较控制的EOF后两特殊请求均0。
record仅按record键排序（无次级整数键），first-int决定次序而大尾保留，独立验证复制优化。
整数EOF前请求8339→8467，新增128个缓存向量也被计入总量，没有只报省下的部分。

既有44原生全过，另加永久19控制全过，最终45入口全过；同源码旧19中6项仅新成本预算失败，全部metadata/resource通过，不是6个功能bug。
真实64kB整数spool/重开覆盖keys0/integerCountK独立arity；公共typed-source宽度破坏在已落盘后精确XX000/FD0/重复next无工作，不称私有磁盘header破坏测试。
既有32协议31/1，加严格PG18.6/前版/新版均66/0矩阵后33入口32/1；唯一暂缓Bind/portal完整463/40整份日志逐字节不变。
新66覆盖极值/±2^53邻值、NULL/方向、LIMIT/OFFSET/TIES、类型提升、隐藏键、真实enum/OID/domain/numeric/record/text回退及显式输入OID的正常text/binary不限额Execute。
首版uppercase OID前提共47处失败（2个DDL的42704、45个表不存在42P01级联），无RowDescription错误记录也留存；最终仅物理列BIGINT+查询o::oid，所有非SQL预期逐项相等，DDL缺口未修也未删除。
实际Main七项资源、35类有限操作、匿名文件内强杀/重开1000行4000单元及再次2000行排序通过；源fixture1072文件SHA未变，不是整库/任意修改恢复验收。

### 实际SQL：全部窗口、漂移和慢值

canonical benchmark_set_sort.py未改，同一关闭的1000行fixture/私有副本，两个分支2000输入，11类各3轮×4次/旧新旧396计时每窗口。
三预算前版a3对照，加预先安排的4MB更早6d6对照，共1584样本；首测4MB旧基线漂移后唯一追加同条件396复测，原窗口全部保留。
合计1980计时、495预热、30SET/SHOW=2505校验，默认持久化/后台维护、socket15s/TCP_NODELAY保持；完整结果验证在计时内、准备在外。
所有自建编译/回归/探针终态后才计时，窗口内不跑其他测试；共享48逻辑CPU主机，无置信区间，不是并发/冷缓存/大数据验收。
以下为各label全12样本p50ms，不删慢值；同行顺序仅属于固定heap夹具，NUMERIC性能值不代替超2^53精度验收。

#### 64kB：前版a3

| 操作 | 旧版先测 ms | 新版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| 文本全排 | 306.011 | 283.568 | 298.034 |
| NUMERIC全排 | 424.416 | 403.522 | 420.206 |
| float8全排 | 493.887 | 470.951 | 512.460 |
| float4全排 | 400.367 | 379.857 | 393.336 |
| 整数全排 | 278.636 | 194.661 | 278.608 |
| 固定同行 | 281.674 | 194.388 | 284.325 |
| LIMIT20 | 177.627 | 142.863 | 184.013 |
| LIMIT20OFFSET500 | 203.829 | 155.364 | 203.259 |
| WITH TIES | 205.008 | 148.963 | 209.882 |
| 普通整数TopN控制 | 36.283 | 35.308 | 39.386 |
| 主键点查控制 | 14.468 | 13.073 | 15.317 |

#### 4MB首测：旧基线漂移

| 操作 | 旧版先测 ms | 新版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| 文本全排 | 440.970 | 250.256 | 281.011 |
| NUMERIC全排 | 724.082 | 375.367 | 397.042 |
| float8全排 | 918.079 | 437.287 | 447.548 |
| float4全排 | 385.274 | 366.722 | 377.301 |
| 整数全排 | 250.721 | 198.485 | 236.286 |
| 固定同行 | 265.279 | 201.279 | 263.426 |
| LIMIT20 | 576.116 | 147.216 | 183.965 |
| LIMIT20OFFSET500 | 520.119 | 149.502 | 230.586 |
| WITH TIES | 210.991 | 152.629 | 217.142 |
| 普通整数TopN控制 | 261.315 | 38.425 | 34.789 |
| 主键点查控制 | 13.743 | 13.771 | 14.021 |

#### 16MB：前版a3

| 操作 | 旧版先测 ms | 新版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| 文本全排 | 259.612 | 246.910 | 252.193 |
| NUMERIC全排 | 387.230 | 378.437 | 384.089 |
| float8全排 | 430.923 | 419.164 | 430.148 |
| float4全排 | 359.159 | 357.055 | 357.244 |
| 整数全排 | 244.719 | 184.605 | 241.352 |
| 固定同行 | 249.064 | 188.882 | 244.198 |
| LIMIT20 | 187.448 | 146.256 | 185.549 |
| LIMIT20OFFSET500 | 232.827 | 147.081 | 226.088 |
| WITH TIES | 213.177 | 145.723 | 207.864 |
| 普通整数TopN控制 | 37.848 | 37.105 | 38.944 |
| 主键点查控制 | 13.111 | 12.449 | 13.976 |

#### 4MB：更早6d6对照

| 操作 | 旧版先测 ms | 新版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| 文本全排 | 250.976 | 247.990 | 254.868 |
| NUMERIC全排 | 373.740 | 362.496 | 379.129 |
| float8全排 | 430.850 | 411.550 | 453.733 |
| float4全排 | 386.174 | 354.832 | 396.239 |
| 整数全排 | 233.550 | 194.164 | 237.939 |
| 固定同行 | 243.413 | 190.354 | 245.487 |
| LIMIT20 | 191.975 | 139.974 | 206.527 |
| LIMIT20OFFSET500 | 188.110 | 145.427 | 200.599 |
| WITH TIES | 200.874 | 149.127 | 200.655 |
| 普通整数TopN控制 | 37.968 | 37.321 | 40.359 |
| 主键点查控制 | 11.429 | 13.971 | 11.710 |

#### 4MB唯一同条件复测：前版a3

| 操作 | 旧版先测 ms | 新版 ms | 旧版后测 ms |
| --- | ---: | ---: | ---: |
| 文本全排 | 263.290 | 254.034 | 254.771 |
| NUMERIC全排 | 392.293 | 387.081 | 375.557 |
| float8全排 | 459.073 | 441.150 | 438.891 |
| float4全排 | 371.953 | 365.252 | 368.331 |
| 整数全排 | 247.619 | 197.770 | 235.018 |
| 固定同行 | 249.608 | 192.337 | 242.707 |
| LIMIT20 | 187.244 | 150.307 | 183.609 |
| LIMIT20OFFSET500 | 228.916 | 152.181 | 222.064 |
| WITH TIES | 205.166 | 153.472 | 206.106 |
| 普通整数TopN控制 | 37.897 | 36.778 | 38.647 |
| 主键点查控制 | 15.666 | 12.562 | 11.334 |

首4MB的普通TopN控制261.315/38.425/34.789ms及若干旧版先测明显漂移，不能把巨大倍率归因于代码；唯一同条件复测也完整列出。
64kB整数全排278.636/194.661/278.608ms、OFFSET203.829/155.364/203.259ms；16MB OFFSET232.827/147.081/226.088ms，支持这些窗口的实际改善。
预先安排6d6对照OFFSET188.110/145.427/200.599ms，重新核查先前回退；但其点查11.429/13.971/11.710ms较慢，不能称全查询无回退。
其他类型与控制波动也保留，不声明稳定普遍提速或达到PG整体性能。

### 构建与复现

标准CMake45.99秒仅重编ExecutionPlan.cpp并链接62对象；60core/Main不变，最终45原生使用相同61core，同代fresh stub仍0b53…。
212源码仅该CPP变化；私有计数overlay/对象完全隔离，未装入正式程序。全部源码/对象/驱动/测试/失败/样本SHA见本机build/pg-feature-set-key-33l7ewyf/final-provenance.json，工件不是已打包发布物。

```bash
python3 scripts/benchmark_set_sort.py \
  --before build/pg-feature-set-key-33l7ewyf/before-dbms_main \
  --after build/cmake/dbms_main \
  --fixture build/pg-feature-topn-20261102-bc582b_s/fixture-1000 \
  --work-mem 64kB --rows 1000 --rounds 3 --samples 4 \
  --output build/retest-set-key-64k
```

4MB/16MB使用相应参数和新目录，更早对照换前批6d6 before-dbms_main。原273/原子条款/725受保护记录与全部用户暂缓不变，Goal active；全局集合计数/FETCH/未知OID/GUC和其他功能族继续开放。无新Markdown/分支/推送。

## 剩余瓶颈与适用范围

普通修改、保存点和其他 DDL 仍有物理快照、WAL 全文件映像及同步 I/O 开销；大数据库下会继续放大。整数 DML 筛选仍扫描堆，JOIN 优化仍可能枚举大量键对，索引覆盖、统计信息、计划选择、批处理及更细粒度的回滚机制仍有优化空间。

本次未改变 C++ 标准、存储逻辑格式或持久化契约，未做 TLS、跨主机网络、大数据冷缓存、长时间压力测试或全仓库兼容性验收。既有兼容性缺口、明确暂缓项及历史 R164 超时根因不因这一性能批次自动关闭，参见[测试报告](test-report.md)与[兼容性契约](compatibility-contract.md)。

## 全局计数表达式与动态排序上限（2026-11-02）

本批输入baba4bcb/95defdc5…，保持C++17、product0.2.0、PG18.6行为基线、GCC13.3/-O2、ICU/zlib与TLS stub。
证据目录为 `build/pg-feature-global-count-xqbytfo5/`，构建、全部对照、初次失败和最终哈希分别见该目录的回执；这些本地证据不是可移植发行包。

### 参考实现与生产范围

[PG18.6 nodeLimit](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeLimit.c)在每次执行/重扫重新读取实际参数，先OFFSET后LIMIT，零上限不向子计划取行，动态NULL上限表示无界。
[parse_clause](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/parse_clause.c)区分FETCH WITH TIES的原始NULL常量和运行时NULL；
[gram.y](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/gram.y)限制FETCH和带ROWS后缀的OFFSET为c_expr/有符号数字，括号内才容纳一般算术。
[analyze.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/analyze.c)在全局集合计数分析前恢复临时ORDER命名空间，
[parse_param.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/parse_param.c)按实际输入上下文推导参数声明。
[DuckDB1.5.6 PhysicalLimit](https://github.com/duckdb/duckdb/blob/v1.5.6/src/execution/operator/helper/physical_limit.cpp)把未求值边界保存在执行状态，并处理NULL与最大边界；其求值顺序与本项目的PG目标不能直接等同。
[SQLite3.50.4 select.c](https://github.com/sqlite/sqlite/blob/version-3.50.4/src/select.c)用LIMIT/OFFSET寄存器控制临时排序索引；本项目复用已有有界排序设施，仍按PG规则处理负数、作用域与错误阶段。

真正的UNION ALL根（SELECT、inline VALUES、括号集合及受支持CTE/derived/physical分支）现可绑定并执行全局LIMIT/OFFSET表达式；VALUES/set FETCH保留真实计数AST、角色与源字节。
全局计数只有空的本层范围与真实祖先，不能借用ORDER输出列或分支FROM；子查询仍有自己的局部范围。OFFSET先分析/求值，失败不触碰LIMIT，零结果不打开数据分支。
当前层aggregate/window/SRF按实际函数身份与语义来源拒绝；同名已选stored scalar不从拼写继承aggregate角色。
普通MetadataPlaceholder沿原identity/slot和实际选中输入类型推导声明，更新同一输入既有使用处；Network仅在现有Parse元数据阶段按真实源位置映射到$n，并整体验证后发布。
已选CAST/算术/比较/连接/CASE/COALESCE上下文有覆盖；这不是所有函数重载、域/OID或参数约束的完整PG推导，也没有修改实际参数帧、保留计划或portal所有权。

动态计数借用原Append的执行开始/结束回调；它们移到真实Count/Sort/Append外围，未另建或替换borrowed owner。
关闭时先释放原子图/文件再结束原owner，保留主异常。真正同一图重绑定使用当前祖先行与允许覆盖的原参数来源；StatementInput继续不可被调用方伪装成RuntimeCell。
有限非TIES的limit+offset安全相加后作为本次排序上限；每次open都重置，NULL、TIES和溢出组合回到完整预算排序。全部源值和排序键仍需求值，错误不能因候选被丢弃而消失。
新动态路径也用于已有VALUES表达式计数，K很小时允许不落盘；完整输出metadata、预算和匿名文件上限保持。普通独立SELECT的表达式计数和其他集合算法没有因此全部补齐。
顶层无写CTE的这些真实计数查询复用已有运行错误接收器，执行失败保留真实RowDescription及已产生的行前缀；分析/规划失败不提前发布描述符，不改格式或portal策略。

### 严格对照与保留的失败

最终正式 CMake 18 次成功编译、16 个不同 CPP（另一次非冻结私有诊断编译不作验收证据），两次链接各62对象；相关头文件消费者与新test stub同代，未声称重编全部62对象。
最终程序58c94e05…与212源码/62对象哈希冻结；48原生入口全过，新增global count执行33、binding34、grammar840全过，同最终测试源旧ABI分别33/31/572处失败。
新运行协议75与FETCH协议900在PG18.6/最终程序全过，旧版分别61/604处不符；binding58在PG全过、旧53处不符、新版仍1处窗口宿主差异。上述观察数不是独立bug数。900协议逐项比较rows、SQLSTATE、headers、tag、OID、frames、Ready及parameter OID八字段，完整错误诊断raw保留，未声明所有错误文案与PG相同。
原完整364SQL/910协议观察仍保留：首候选25处不符中，接受范围内16个Simple负FETCH缺RowDescription已修复；最终前900全过，另4个独立SELECT的10观察中9个差异仍开放，没有从原始记录删除。

原860语法控制首候选恰2处失败：旧断言拒绝PG合法的括号参数FETCH。保留原源码/日志后仅迁移为严格合法AST/参数源位置检查，其他858字节/顺序不变，最终860全过。
既有VALUES执行63首候选仅1个资源前提失败：K1已有界排序不应强制落盘。该控制改为真实动态NULL以保证先落盘再排序键错误，所有22012/文件/终止断言保留；新增K1控制由PG核实精确源/键计数25/24，最终64全过，旧64仅新增成本契约失败。
新原生最初32中2个同类落盘前提失败的源码/日志也保留；修正为有限无I/O→NULL落盘→再有限不新增I/O，并追加独立K1被丢弃候选错误，最终33全过。

所选原33协议首候选31/2：除既有暂缓Bind完整463/40外，普通锁前SELECT出现57014；原源码和失败日志保留。最终全部按原参数单独顺序执行，原33为32/1，该超时入口通过；未改超时、连接、重试或中断策略。
加新75/58/900三个入口后，最终36入口按各自完整结果为34通过/2失败：上述Bind日志与前版逐字节相同；另一项是新58中的窗口子查询宿主缺口，未当成暂缓或从严格矩阵排除。


实际Main最终12资源控制全过，共4161行/12483单元完整值和精确描述符，最多2个排序FD，EOF全部释放；固定夹具1072文件未变。
35类操作是每类一次的有限正确性检查，其并行负载下时间不用于速度结论；匿名全局计数排序文件打开时对自建进程SIGKILL，重开核对1000行/4000单元并再排序2000行通过，不能推及任意写入崩溃恢复或真实TLS。


窗口子查询 `LIMIT (SELECT row_number() OVER())` 已在PG合法、当前真实执行宿主仍报0A000；严格58项中的该差异保留，未新设暂缓项或将它算作通过。
未知参数全约束、任意窗口/聚合/其他集合lowering、统一资源治理及全部原始功能要求继续开放。
原273项仍22 complete/166 partial/70 unverified/15 deferred；原始项与725受保护记录保持，整体Goal active。

### 性能方法与完整结果

三个预算64kB/4MB/16MB、每例每label12样本与3次预热，全部串行old/new/old。六个预定窗口完成1620计时样本（VALUES432、既有静态集合1188）、405预热与36次SET/SHOW，共2061次SQL校验，无删除/重测或样本调整。
前版95defdc5…/新版58c94e05…、canonical脚本39b1450e…/VALUES脚本59c59442…、原1072文件夹具均不变。

四个新对照只用前后版本共同支持的VALUES表达式计数：1000行、1536字符payload尾、精确大于2^53的整数或C字节文本键、LIMIT20/OFFSET500，SQL约1.6MB；独立Python oracle校验全部返回单元。
既有静态集合沿用原canonical脚本和1000行fixture，未把旧版不支持的global-set表达式SQL算作速度收益。
两driver均计入请求/解析/网络及完整值比较，raw/hash/JSON写入在单样本计时外。VALUES预先准备SQL/oracle；canonical的execute_case在计时内调用SQL/expected生成器，分别解释各自前后对照。
构建、正确性测试和资源探针都已终止后才开始计时；共享宿主的其他负载没有由此排除。每label12样本，不作置信区间或普遍无回退声明。

下表为全部45例的中位数，单位ms；最后一列依次是“新版对旧首/新版对旧回测”的百分比，正数表示较慢。全部min/max、漂移和每样本完整值见 `runtime-performance-summary.csv/.json` 及其六个原始comparison路径。

| 预算 | 路径 | 旧首 | 新版 | 旧回测 | 新对旧首/旧回测 |
| --- | --- | ---: | ---: | ---: | ---: |
| 64kB | `VALUES integer_limit20` | 2326.700 | 2440.648 | 2458.232 | +4.9% / -0.7% |
| 64kB | `VALUES integer_offset500` | 2338.878 | 2415.781 | 2451.795 | +3.3% / -1.5% |
| 64kB | `VALUES text_limit20` | 2490.506 | 2507.286 | 2529.758 | +0.7% / -0.9% |
| 64kB | `VALUES text_offset500` | 2537.030 | 2569.724 | 2612.343 | +1.3% / -1.6% |
| 64kB | `union_text_full` | 291.040 | 272.397 | 274.903 | -6.4% / -0.9% |
| 64kB | `union_numeric_full` | 408.769 | 396.188 | 424.088 | -3.1% / -6.6% |
| 64kB | `union_float_full` | 485.349 | 478.176 | 473.033 | -1.5% / +1.1% |
| 64kB | `union_real_full` | 384.783 | 370.679 | 374.494 | -3.7% / -1.0% |
| 64kB | `union_integer_full` | 193.483 | 196.648 | 190.306 | +1.6% / +3.3% |
| 64kB | `union_stable_peers` | 199.489 | 195.239 | 180.443 | -2.1% / +8.2% |
| 64kB | `union_limit` | 149.324 | 143.600 | 144.436 | -3.8% / -0.6% |
| 64kB | `union_offset` | 158.530 | 153.155 | 151.618 | -3.4% / +1.0% |
| 64kB | `union_ties` | 150.937 | 149.658 | 164.121 | -0.8% / -8.8% |
| 64kB | `integer_topn_control` | 37.769 | 35.013 | 35.159 | -7.3% / -0.4% |
| 64kB | `pk_lookup_control` | 12.986 | 13.566 | 12.131 | +4.5% / +11.8% |
| 4MB | `VALUES integer_limit20` | 2339.081 | 2348.506 | 2288.715 | +0.4% / +2.6% |
| 4MB | `VALUES integer_offset500` | 2329.020 | 2287.373 | 2320.789 | -1.8% / -1.4% |
| 4MB | `VALUES text_limit20` | 2332.329 | 2334.331 | 2346.631 | +0.1% / -0.5% |
| 4MB | `VALUES text_offset500` | 2344.556 | 2379.820 | 2328.149 | +1.5% / +2.2% |
| 4MB | `union_text_full` | 236.213 | 254.126 | 343.138 | +7.6% / -25.9% |
| 4MB | `union_numeric_full` | 370.573 | 386.552 | 424.901 | +4.3% / -9.0% |
| 4MB | `union_float_full` | 417.952 | 421.002 | 473.911 | +0.7% / -11.2% |
| 4MB | `union_real_full` | 351.501 | 370.111 | 391.872 | +5.3% / -5.6% |
| 4MB | `union_integer_full` | 184.642 | 204.147 | 197.171 | +10.6% / +3.5% |
| 4MB | `union_stable_peers` | 186.323 | 202.277 | 204.564 | +8.6% / -1.1% |
| 4MB | `union_limit` | 144.879 | 173.353 | 170.487 | +19.7% / +1.7% |
| 4MB | `union_offset` | 145.566 | 156.832 | 156.697 | +7.7% / +0.1% |
| 4MB | `union_ties` | 145.595 | 173.395 | 172.651 | +19.1% / +0.4% |
| 4MB | `integer_topn_control` | 36.817 | 40.839 | 40.206 | +10.9% / +1.6% |
| 4MB | `pk_lookup_control` | 13.729 | 18.029 | 17.819 | +31.3% / +1.2% |
| 16MB | `VALUES integer_limit20` | 2327.953 | 2456.218 | 2311.315 | +5.5% / +6.3% |
| 16MB | `VALUES integer_offset500` | 2280.406 | 2303.361 | 2311.418 | +1.0% / -0.3% |
| 16MB | `VALUES text_limit20` | 2319.640 | 2332.816 | 2335.783 | +0.6% / -0.1% |
| 16MB | `VALUES text_offset500` | 2343.659 | 2317.537 | 2406.004 | -1.1% / -3.7% |
| 16MB | `union_text_full` | 246.626 | 248.347 | 247.733 | +0.7% / +0.2% |
| 16MB | `union_numeric_full` | 382.807 | 410.088 | 373.104 | +7.1% / +9.9% |
| 16MB | `union_float_full` | 433.061 | 762.679 | 425.205 | +76.1% / +79.4% |
| 16MB | `union_real_full` | 360.739 | 361.523 | 368.218 | +0.2% / -1.8% |
| 16MB | `union_integer_full` | 206.268 | 192.747 | 192.664 | -6.6% / +0.0% |
| 16MB | `union_stable_peers` | 204.092 | 211.344 | 193.783 | +3.6% / +9.1% |
| 16MB | `union_limit` | 145.802 | 166.316 | 149.940 | +14.1% / +10.9% |
| 16MB | `union_offset` | 148.587 | 149.714 | 148.416 | +0.8% / +0.9% |
| 16MB | `union_ties` | 169.116 | 150.759 | 148.876 | -10.9% / +1.3% |
| 16MB | `integer_topn_control` | 35.913 | 37.896 | 36.363 | +5.5% / +4.2% |
| 16MB | `pk_lookup_control` | 11.179 | 12.442 | 10.911 | +11.3% / +14.0% |

整条VALUES SQL未证明普遍提速；16MB整数LIMIT20为2327.953/2456.218/2311.315ms，新值比两旧值慢约5.5%/6.3%，继续开放。
4MB静态旧首/旧回漂移明显，例如text236.213→343.138ms，不能把单一前后倍率直接归因于代码。
16MB静态float8为433.061/762.679/425.205ms，新12个原样本的后6个约1034–1783ms，前6个约415–492ms，整个慢簇保留，没有剔除或另测一组替代。
原生资源证明动态小K可不落盘，未据此推导端到端速度；这些慢值与其他瓶颈仍需后续独立归因和优化。

### 已绑定执行阶段的独立对照

在上述六窗口全部结束后，再串行编译/运行一个仅用既有public API的同源原生驱动，旧头/旧61core/旧stub与新头/新61core/新stub分别匹配，没有私有算子克隆、SQL重写或参数帧替代。
固定四条SQL/独立Python oracle与wholeSQL的specifications对象逐项相等；同样1000行/1536字符尾、三预算、每例3预热/12样本、old/new/old，四例按固定洗牌三轮执行。
初始prepareBoundQuery（解析+绑定合并）和buildPreparedQueryPlan/makePreparedCursor放计时外并单独保留36条preparation记录；计时含真实同图restart(RowContext{})→consume→close→完整owning值/metadata及图身份校验，evidence写入在计时外。
原生432计时+108预热共540调用、10800行/32400单元校验全过，9进程exit0、原夹具与源码/core/header/stub输入均稳定。
它测public cursor执行的墙钟时间；每次重启仍包含真实执行owner/表达式准备，不能当成排序CPU、wire prepared/保留计划性能或R303修复，也不能和wholeSQL直接相减归因。

| 预算 | 路径 | 旧首ms | 新版ms | 旧回测ms |
| --- | --- | ---: | ---: | ---: |
| 64kB | `integer_limit20` | 601.459 | 575.879 | 588.664 |
| 64kB | `integer_offset500` | 599.521 | 576.055 | 582.680 |
| 64kB | `text_limit20` | 678.158 | 648.592 | 655.284 |
| 64kB | `text_offset500` | 679.743 | 658.013 | 681.877 |
| 4MB | `integer_limit20` | 572.658 | 570.643 | 572.208 |
| 4MB | `integer_offset500` | 586.738 | 583.943 | 576.529 |
| 4MB | `text_limit20` | 627.241 | 597.790 | 609.378 |
| 4MB | `text_offset500` | 623.136 | 627.820 | 614.868 |
| 16MB | `integer_limit20` | 566.255 | 561.893 | 574.040 |
| 16MB | `integer_offset500` | 566.110 | 569.077 | 583.433 |
| 16MB | `text_limit20` | 613.485 | 587.727 | 621.266 |
| 16MB | `text_offset500` | 610.354 | 611.377 | 619.679 |

这组也未证明普遍/显著提速：4MB textOFFSET为623.136/627.820/614.868ms，新值比两个旧值均慢；若干LIMIT有小幅改善，基线也波动。
初始合并解析+绑定206.307–242.332ms、建图/初始准备453.918–524.358ms另记；没有细分parser/analyzer CPU或断言某一阶段解释了全部开销。
后续还需要优化并归因执行准备、非整数比较与整条SQL开销。原wholeSQL六窗的慢值仍独立保留，未用这组替代它们。
完整每次值/CSV/图身份、初始准备、旧新编译输入见 `bound-values-results/execution-summary.json` 与 `prepared-values-cursor-plan.json` 的冻结方法和命令。


复现：以本目录 `runtime-performance-plan.json` 和 `runtime-performance-windows-receipt.json` 的六条冻结命令/输出为准；
例如 `python3 scripts/benchmark_set_sort.py --before build/pg-feature-global-count-xqbytfo5/before-dbms_main --after build/pg-feature-global-count-xqbytfo5/candidate2-dbms_main --fixture build/pg-feature-topn-20261102-bc582b_s/fixture-1000 --output build/pg-feature-global-count-reproduce --work-mem 64kB --rows 1000 --rounds 3 --samples 4`。
输出目录必须是新的私有目录。VALUES driver、固定SQL和独立oracle同样留在本批build证据目录；原始输入/前后旧版本、raw慢值和方法缺一不可，当前代码重建不能冒充旧二进制。

最终文档检查的初次脚本误继承前批scope-audit命令，覆盖了本地前批的一份 `scope-audit.json`；该旧JSON已不匹配原证据索引的哈希。
误运行报告、原预期哈希和完整性说明保存在本批 `prior-audit-overwrite-incident.json`，旧通过日志/原证据索引仍保留；没有改写原哈希来假装恢复。
源码、已提交文档、原生/协议原始日志、性能raw与二进制未被该误运行修改。本批范围检查改为独立对照baba4bcb与更早未改的保护索引；不再用被覆盖的旧JSON证明本批通过。
另一次本地链接检查器未识别旧文档合法的`:行号`文件链接，已仅修检查器；受保护旧文档未动。最初检查结果保留。


## 类型化窗口宿主与有序输出复用（2026-11-03）

### 范围、研究与实现

本批输入 `9f019ffa8367a472bff9801c5fd6e43dfdcacc05`，直接在 master 开发；前版程序 SHA256 `58c94e05ce38e1744e5ec792de97a73d960c8ec284834f764ebb78d5374951ab`，
最终 C9 程序 `c696d3b224748ee010d1f8bb9994494430730f2ffaeb9a5581247f7142f2abeb`。
本批补入受支持查询形状的类型化窗口执行宿主，**没有实现普遍提速**：按同预算旧版回测，`full_first_last_nth` 在三个预算约快5.15/5.33/5.84倍；
其余7类查询的21组新版p50延迟均更高，为旧回测的约1.92–2.70倍。64kB旧首/旧回测漂移很大，完整表和原始慢值一并保留，不能只引用旧首声称加速。
这不是新的PostgreSQL速度对照，也没有把减少复制次数当作已证明的整库收益。

工件目录 `build/pg-feature-window-host-557k7fi8/` 保存旧源/完整62对象、C1..C9程序/输入、原生/协议原始失败、PG预期、资源和性能记录；本地build工件不纳入Git。
产品仍是0.2.0/C++17，GCC13.3/-O2、ICU/zlib及TLS stub，未通过切换语言标准宣称查询提速。
通过验证的C9已原子安装到根目录dbms_main，文件和父目录已fsync；根目录、CMake产物和冻结C9 SHA一致，原58备份保留，没有重启既有服务。

研究以 PostgreSQL 18.6 实际源码和独立协议观察为准：[窗口语义](https://www.postgresql.org/docs/18/functions-window.html)、
[planner](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/optimizer/plan/planner.c)、
[setrefs](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/optimizer/plan/setrefs.c)、
[parse_agg](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/parser/parse_agg.c)、
[WindowAgg](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/executor/nodeWindowAgg.c)、
[windowfuncs](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/utils/adt/windowfuncs.c)与
[timestamp interval除法](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/utils/adt/timestamp.c)。
据此实现真实输入键投影、窗口栈排序复用、逐行aggregate transition与按需结果；WindowClause的原始身份和语义排序键身份分别保存，避免把两个初始化不同的窗口错误合并。

另参考[SQLite窗口接口](https://www.sqlite.org/windowfunctions.html)的逆转移机制，以及DuckDB官方的
[窗口执行说明](https://www.duckdb.org/2021/10/13/windowing)、[窗口优化文章](https://duckdb.org/2025/02/14/window-flying)和
[窗口函数文档](https://duckdb.org/docs/2.0/sql/functions/window_functions)。
逆转移、完整segment tree/vectorized窗口及窗口spill仍是后续方向，本批没有把这些系统的实现或性能当成本项目已完成项。

实际管线为真实行源/过滤→既有普通聚合→拥有类型和值的窗口输入槽→一个或多个窗口阶段→必要时普通最终排序→投影/DISTINCT/OFFSET/计数。
完整分区/排序表达式只投影一次；SELECT、最终ORDER、窗口参数/FILTER/default内相同深层键引用真实槽，窗口键自身的生产者不任意做子表达式消除。
命名继承、真实作用域/sourceOccurrence/ordinal、声明比较类型/枚举OID和NULL/方向均保留。
窗口内核支持已验证的ROWS/GROUPS/数值RANGE、peer/exclusion、排名、偏移/取值、分桶及有限聚合；无键阶段保持已有顺序；相同语义键的阶段复用已有排序，最终ORDER的已证明兼容前缀可省去冗余Sort。不同前缀stage的组内incremental sort尚未实现。
运行前缀聚合复用累计状态；移动帧头/exclusion按需重算，尚无一般逆转移或segment tree。

C9的私有窗口read把真实上游拥有的current context直接用于同步的原execution.context复制，不再先经过lastPreparedContext产生一个临时完整副本。
返回的输入context仍独立拥有数据，原execution.context方法/调用次数与参数frame/provider不改，不保存任何跨调用借用；已有current_/currentKeys_输出快照、close/restart均保持。
输入键存储由每行K+P+O个datum降为K个，分区/排序/RANGE通过原索引读取唯一keys；保留原read位置的防御性边界检查。这里是复制次数/数据结构变化，不是实测RSS或整库内存倍率。

只有真实无条件投影窗口保证排序存在、且所有随后可能排序阶段都保持最终ORDER前缀时，才去掉冗余最终排序。
LIMIT 1 的兼容前缀样例窗口参数求值从6次降至PG要求的1次；不兼容/条件窗口负控制仍需6次。
这是求值需求证明，不是整库6倍速度声明；有序WITH TIES使用真实窗口保存键和投影前瞻，OFFSET、零LIMIT及错误前行均逐项核对。
Main、Core与真实SQL子查询共享类型化宿主；Main真实SourceContexts JOIN已验证，通用Core JOIN lowering仍报不支持。
Describe只用真实绑定/目录/来源推导，不打开游标或执行SQL；物理来源使用实际表OID/attnum，view使用自己的OID，表达式/合并USING等保持无来源。

### 正确性、保留失败与资源

最终61core/Main/新stub全部与头文件匹配，C9 link冻结src下216文件无漂移，生产对象为61core加Main共62个。主回归68原生全过，加两个独立入口共70个不同原生入口全过。
六个新增入口分别223/37/3/39/30/16，共348控制全过；测试实现真实公共游标、同图重启、原始参数来源、heap/FD/NULL和异常清理，不以私有替身代替产品执行。
新协议窗口宿主226、输入键241、有序输出201有独立PG18.6严格预期；类型/修饰/格式/来源/行值/ParameterDescription/错误阶段都参与比较。
固定目录身份只凭实际物理LIMIT0/pg_type探针归一，不把本项目OID伪装成PG数值，也不改已有测试的固定预期。

interval datum除法修正月/日小数向下级联、TSROUND/rint与边界检查，有限窗口AVG随之正确；原乘法及其他求值器逻辑保持字节一致。
严格interval106保留10个常量错误的Bind/Execute阶段差异；值、类型、SQLSTATE、20个AVG控制均符合。它们属于既有R303等暂缓范围，未过滤、改预期或恢复该模块。
最终同一C9的60个不同协议入口为56通过/4失败；主58协议为55通过/3失败，另键241通过、interval106/10保留失败。原Bind/portal完整463/40日志与前版字节相同，另外CTE/window-owner747/29及nested-window-projection325/42仍有严格差异。
两个旧版完整结果分别747/453与325/77，改进不等于全部通过；剩余问题不能一律归为暂缓参数阶段。
本批没有重新宣告前批完整910或全部数据库入口验收，新宿主消除了前批binding58中合法窗口子查询的0A000缺口。

C1..C9全部候选、失败与harness版本保留。中途原生JOIN使用错误计划入口、错误阶段前提、排序图遍历未展开真实LIMIT/OFFSET子节点、metadata入口数前提均分别纠正并留原始失败；
这些测试前提改动没有改既有2302个测试。实际窗口放置/继承、键复用、Frame初始化身份、ntile首行NULL、interval级联、清理与行需求问题由生产实现修复。

逐观察审计发现C7相对旧58新增14个回退，不能因总失败数下降就称无回退：8个CTE串联Simple查询错误、6个LIMIT/OFFSET副作用计数过量。
C8在真实whole-read图准入处保留CTE原生产者图，COALESCE builtin首参数必经证明去除冗余最终Sort；完整1072观察确认14项全部恢复，且无新增退化；最终C9逐观察另行复核。
两完整入口仍有71个差异：CTE29为3个真实非零OID数值、2个42P01错误阶段、24个缺RowDescription；其所有行值/SQLSTATE已符合PG，没有剩余XX000。
Nested42为18个OID数值、18个合法ANY/SOME/ALL仍被42883拒绝、3个重复volatile真实行值和3个对应少调用计数；它们不是71个独立bug，也不全属R303。
新增native16首轮15/1仅最后控制的序列值1→5，计划/返回行正确；它使用默认未启用root常量规划的低层Core图。
真实C8 Main同源PG14/0；仅新增15/16改为公开PreparedQueryExecution.planStatementConstants及真实executionProvider图后16/0，原14完整保留。
原未规划结果、诊断和源码不删除；没有更改通用Core默认常量规划策略或宣告未规划API与PG等价。

资源阶段不计时：共同8查询旧/新/旧共24次1,000行完整核对；普通最终TypedSort在64kB采到最多2个匿名FD，在16MB未采到FD，均验证1,000行/3,000单元，EOF无遗留FD/命名临时文件。
强杀验证捕获自有PID/数据目录/启动身份及实际匿名FD后执行，退出-9并断连；同目录重开核对基表1,000行/4,000单元及再次排序1,000行。
三阶段共43请求（28个完成全值读取、1个强杀中的读取、14个SET/SHOW），1072夹具文件不变，自有进程均结束。35类操作另以每类一次完成35个结果核对、0个操作错误，但服务器正常退出时抛std::system_error: Invalid argument并SIGABRT（-6），driver exit1/complete=false，该完整入口失败。
原log/operations.json及operations-candidate9-failed-shutdown-receipt.json保留；既有R15停机链用户暂缓保持，未恢复调查/改代码/重跑挑选通过样本，未从源码未改就推断因果无关。
窗口分区缓存和阶段内部排序仍无work_mem上限，只有保留的普通最终排序具有上述预算/落盘证明；没有声称硬RSS上限、所有窗口spill、任意写入崩溃恢复、并发或真实TLS通过。

### 性能方法与完整结果

性能使用固定1072文件、1,000行夹具，前版58/最终C9/前版58；构建、正确性和资源进程均结束后再计时。
共同支持的8条SQL在64kB/4MB/16MB顺序执行，每budget每label3轮×4样本及3次预热；最终完成864计时、216预热、18次SET/SHOW，共1098次校验；1080个查询结果合计1,080,000行/2,970,000单元完整相等。
计时包含整条SQL的解析、执行、网络和完整结果比较；逐样本原始数据与所有慢值/漂移保留，不将旧版拒绝的新SQL算作性能改善，也没有重新测PG性能。
共同夹具base payload是row-i（5–8字节），不是前批VALUES基准的1536字符尾。
八查询均有grp分区、最终ORDER id；当前保留普通TypedSort并使用会话预算，完整typed上下文含未投影源字段；旧WindowOp的最终排序在内存vector中，不使用该work_mem或落盘。
相同SET不代表资源策略等同，不能直接把全部慢值因果归为I/O。已有匿名FD实证来自另行repeat(payload,512)宽投影控制，未逐条采样common8的FD。

冻结driver为本批 `runtime_benchmark_windows.py`（SHA256 `f3d025e21e9ddfabff7cd9980faa27a295222b2d7b6d01178a5c2fbd8fb7450b`）；实际完成结果在下表及本批原始comparison文件中。

### C9 全部24项结果

每个p50来自12个样本，单位ms；新相对旧首/旧回测百分比的正值表示较慢。全部min/max、原序12个样本和9份CSV/JSONL见本批 `runtime-window-performance-candidate9-summary-v1.json/.csv` 及其raw_artifacts。
三个预算全部自然exit0、无重测或剔除；原夹具1072文件/驱动/二进制前后哈希不变，自有进程均退出。
内部构建、测试和资源任务已停止后才计时，但共享宿主其他负载未被排除；每label12样本不支持置信区间、全负载或无回退结论。

| 预算 | 查询 | 旧首p50 ms | C9 p50 ms | 旧回测p50 ms | 新对旧首 | 新对旧回测 | 旧漂移 |
|---|---|---:|---:|---:|---:|---:|---:|
| 64kB | partition_ranking | 349.26 | 227.63 | 95.78 | -34.8% | +137.7% | -72.6% |
| 64kB | partition_lag | 710.17 | 195.43 | 88.12 | -72.5% | +121.8% | -87.6% |
| 64kB | partition_lead | 554.23 | 199.50 | 76.65 | -64.0% | +160.3% | -86.2% |
| 64kB | running_sum | 377.79 | 212.39 | 78.52 | -43.8% | +170.5% | -79.2% |
| 64kB | sliding_sum | 487.96 | 186.21 | 87.15 | -61.8% | +113.7% | -82.1% |
| 64kB | partition_full_sum | 407.95 | 188.53 | 90.89 | -53.8% | +107.4% | -77.7% |
| 64kB | full_first_last_nth | 1869.42 | 231.01 | 1190.03 | -87.6% | -80.6% | -36.3% |
| 64kB | mixed_same_keys | 384.88 | 240.49 | 122.94 | -37.5% | +95.6% | -68.1% |
| 4MB | partition_ranking | 98.25 | 190.63 | 90.83 | +94.0% | +109.9% | -7.6% |
| 4MB | partition_lag | 81.33 | 164.60 | 75.27 | +102.4% | +118.7% | -7.5% |
| 4MB | partition_lead | 77.76 | 165.18 | 68.58 | +112.4% | +140.9% | -11.8% |
| 4MB | running_sum | 78.71 | 164.75 | 72.62 | +109.3% | +126.9% | -7.7% |
| 4MB | sliding_sum | 87.40 | 165.81 | 77.93 | +89.7% | +112.8% | -10.8% |
| 4MB | partition_full_sum | 85.54 | 146.53 | 72.97 | +71.3% | +100.8% | -14.7% |
| 4MB | full_first_last_nth | 1136.89 | 213.54 | 1139.02 | -81.2% | -81.3% | +0.2% |
| 4MB | mixed_same_keys | 100.39 | 215.90 | 98.35 | +115.1% | +119.5% | -2.0% |
| 16MB | partition_ranking | 96.80 | 181.23 | 93.93 | +87.2% | +92.9% | -3.0% |
| 16MB | partition_lag | 75.68 | 160.17 | 83.49 | +111.7% | +91.8% | +10.3% |
| 16MB | partition_lead | 79.49 | 160.13 | 69.43 | +101.5% | +130.6% | -12.6% |
| 16MB | running_sum | 81.68 | 160.87 | 71.96 | +97.0% | +123.5% | -11.9% |
| 16MB | sliding_sum | 76.07 | 165.41 | 77.34 | +117.4% | +113.9% | +1.7% |
| 16MB | partition_full_sum | 80.94 | 144.00 | 73.58 | +77.9% | +95.7% | -9.1% |
| 16MB | full_first_last_nth | 1148.02 | 195.17 | 1139.79 | -83.0% | -82.9% | -0.7% |
| 16MB | mixed_same_keys | 100.16 | 204.05 | 97.18 | +103.7% | +110.0% | -3.0% |

4MB/16MB的7类查询比前后两个旧版都慢，未关闭这些慢项。64kB旧首/旧回测下降36.3%–87.6%，不能用对旧首的负百分比掩盖对旧回测的回退。
first/last/nth组合相对旧回测的5.15/5.33/5.84倍收益只适用于本1,000行、100分区、热缓存、单连接夹具；不是PostgreSQL对照或全部窗口收益。
这些结果覆盖功能/执行/资源策略的整批差异，没有用跨候选C7→C9未配对数据估算复制优化的独立倍率。

### C7 已完成性能窗口（保留历史）

C7在发现14个新增正确性回退前，已完成64kB旧/新/旧的288计时、72预热和6次SET/SHOW，共366次全值校验；该完整窗口未删除或被C9替代。
发现回退后当前budget自然完成，后续4MB在创建输出目录时受控拒绝、未启动任何server/SQL/样本，16MB未启动；控制性exit1不记为产品性能失败。
下表保留全部8例的p50/min/max和旧版漂移；C7是失败候选历史，不能作为最终C9的测量结果。原CSV/JSONL、启动闸门和控制收据均留在本批工件中。

| 查询 | 旧版 p50 [min,max] ms | C7 p50 [min,max] ms | 旧版回测 p50 [min,max] ms | 旧版漂移 |
|---|---:|---:|---:|---:|
| partition_ranking | 159.52 [103.35, 417.16] | 205.12 [184.83, 268.37] | 103.56 [92.55, 117.98] | -35.1% |
| partition_lag | 151.29 [103.71, 217.30] | 227.08 [160.14, 451.93] | 85.10 [75.09, 214.51] | -43.7% |
| partition_lead | 125.09 [90.13, 210.61] | 177.94 [164.53, 192.14] | 86.34 [73.62, 101.88] | -31.0% |
| running_sum | 150.58 [81.51, 217.52] | 191.16 [178.94, 240.35] | 87.38 [77.53, 90.70] | -42.0% |
| sliding_sum | 97.09 [76.20, 247.98] | 197.18 [170.32, 303.56] | 90.47 [78.45, 93.80] | -6.8% |
| partition_full_sum | 170.25 [144.42, 370.49] | 209.13 [144.75, 712.67] | 93.20 [87.03, 102.17] | -45.3% |
| full_first_last_nth | 1216.49 [1093.03, 1265.98] | 257.77 [207.17, 299.29] | 1128.10 [1123.14, 1248.50] | -7.3% |
| mixed_same_keys | 138.15 [101.90, 200.23] | 229.35 [208.28, 364.00] | 106.71 [89.87, 186.39] | -22.8% |

旧首/旧回测漂移为-6.8%到-45.3%，原样保留；不能把单一前后比值直接归因于代码。C9另用独立新目录完整执行冻结的三预算计划。

### 开放项、范围与复现

性能下一步优先诊断窗口键比较中的typed datum复制/重复数值解析、上下文准备及最终TypedSort成本；可评估仅Window-owned的精确内置整数键缓存，保留enum/OID/domain/NaN/NULL等真实回退。
静态热点尚不证明其解释了全部慢值；不改共享Set/R185助手或用legacy接收器绕过类型/资源正确性。当前7类慢项未关闭。

完整窗口内存治理、统计/更多窗口聚合、temporal RANGE的in_range族、通用Core JOIN lowering、其他普通SELECT动态计数、全部类型/嵌套边界和统一执行预算仍开放。
WITH TIES的枚举/NaN/record/array peer全组合及OOM/double-close故障矩阵未全部覆盖；本批有限核验不能关闭这些要求。
原273项仍22 complete/166 partial/70 unverified/15 deferred；只追加近期partial进展，原完整对象/条款/725受保护记录不变，整体Goal active。
受保护共享排序/中断helper、PreparedAggregateOp/prepareAggregateRoles、legacy WindowOp、PreparedOperatorCursor及真实参数帧/provider/portal等职责保持。
最终只读源码审计33/33通过，既有2302测试不变；原144公共头仅AST/执行注册接口两处授权改动，所有受影响消费方和Main重新编译。

当前复现使用本批 `runtime-candidate9-resources-v1-receipt.json` 的三个实际阶段命令，以及 `runtime-window-latency-candidate9-64kB-v1-receipt.json`、`runtime-window-latency-candidate9-4MB-v1-receipt.json`、`runtime-window-latency-candidate9-16MB-v1-receipt.json` 的最终冻结计时命令，在新的私有输出目录执行。C7 observation/validate计划仅作历史，不能冒充C9命令。
原生入口经 `run_native.py` 与C9冻结core配置，协议入口使用已保存的真实PG18.6预期及对应二进制；不能用当前重建冒充旧58程序。
例如在新的私有输出目录复现64kB（其余预算按各自receipt命令）：

```bash
python3 build/pg-feature-window-host-557k7fi8/runtime_benchmark_windows.py --before build/pg-feature-window-host-557k7fi8/before-dbms_main --after build/pg-feature-window-host-557k7fi8/candidate9-dbms_main --fixture build/pg-feature-topn-20261102-bc582b_s/fixture-1000 --output build/window-host-reproduce-64kB --work-mem 64kB --rows 1000 --rounds 3 --samples 4
```

原始raw与复现输出目录不能覆盖；研究和功能对照、资源、计时三个阶段分别解释。

输入、完整资源记录、每入口日志、源码/core/header/stub收据和最终文档检查统一列入本批 `final-provenance.json`；上一批已披露旧scope-audit误覆盖事件不改写或伪造恢复。


## 窗口整数键缓存与实际执行成本（2026-11-03）

### 实现与研究依据

本批基线 `b04db8010bc478a84dd767d6c6cac08da4bfcd42`、前版C9 SHA256 `c696d3b224748ee010d1f8bb9994494430730f2ffaeb9a5581247f7142f2abeb`；
候选C1 SHA256 `56d06bca0ddacd1d7c801b51d360d1d393765717f8d1aeda5b31b7f0196c6a19`。工件目录 `build/pg-feature-window-key-w4qeo1ai/`。
根目录 `dbms_main` 已以同一C1原子替换并对文件/父目录fsync，C9副本保留；没有重启既有服务，安装收据为 `candidate1-root-install-v1.json`。
本批确有执行收益，但有资源代价与未决慢值：4MB/16MB的16个整SQL对照p50降低约22.3%–39.1%，64kB仍有3项慢于两次C9。
1000行整数控制的allocator uordblks在首/末行多约68KB，close后有保留容量；不是内存普遍降低或完整预算治理。全部快慢值均保留。

仍是0.2.0/C++17、GCC13.3/-O2、ICU/zlib、TLS stub；没有改语言标准或持久化保护。

[PG18.6 WindowAgg](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/executor/nodeWindowAgg.c)在初始化时准备分区/同行比较状态，
[execTuplesMatchPrepare](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/executor/execGrouping.c)解析operator函数OID并构造后续复用的表达式；
[DuckDB排序设计](https://www.duckdb.org/2025/09/24/sorting-again)和[原始论文](https://duckdb.org/pdf/ICDE2023-kuiper-muehleisen-sorting.pdf)说明解释式比较的成本与预生成排序键的方向。
本项目仅借鉴把重复解释移出循环：缓存已求值builtin整数，不声称实现了PG compiled ExprState、完整排序键编码或向量化。

先用真实public cursor分阶段诊断，再实施唯一生产改动 `PreparedWindowOp.inc`。每次原非NULL比较会创建两个owning RuntimeCell并执行1–2次通用表达式比较；
新路径在真正到达的比较处、双侧raw类型相同且为smallint/integer/bigint、真实声明OID21/23/20、无enum绑定时，惰性解析int64一次并缓存到该行实际key槽。
NULL仍首先决定；比较用大小关系而非减法，不引入整数溢出或浮点舍入。全串解析/位宽检查未命中时交回原Set比较owner，不新增SQL拒绝。
真实range OID（包括0）优先于stale binding或投影声明；未绑定Column不以投影救回，domain/enum/unknown0/没有导出权威OID的隐藏表达式保留原路。
不重跑infer/search_path/catalog，不重新执行键表达式，也不缓存或替换实际frame/provider/上下文；RANGE数学、原键值与当前输出快照不变。

这会增加常驻成本：每个buffered Input新增vector头及外层容量，实际整数payload和小型OID元数据按比较需求分配，domain/OID0拒绝前也可能分配小metadata vector。
close释放行payload及metadata，但原input_外层容量可保留到图销毁；purefallback也有新增行header，不能称其零内存成本。
窗口分区、内部排序和新增缓存仍无work_mem/spill；普通最终TypedSort的预算与完整context codec完全保持。

### 完整正确性与范围证据

C1单TU编译/链接exit0，冻结src216文件无drift，61core仅Exec对象变化；147头、Main、ExecCPP整文件（含sharedSet/R185/R177）及实际frame/context/provider/codec均与基线字节一致。
原70native全过，加新native36入口全过，合计71个不同入口。新36在C9仅3个成本断言失败，其余33语义控制全过；原源和所有日志保留。
原60wire仍56pass/4fail，Bind463/40、CTE747/29、nested325/42整log逐字节相同，interval106/10完整观察JSON相同（log仅输出路径变化）。
新增固定143在独立PG18.6全0；C9和C1都14处差异且label/SQL/expected/fullactual/raw消息/错误全部相等，0新增回退。
14为catalog来源2、rank secondary record-json错误前缀3、重复volatilewindow调用共享6、compound二进制结果3；不改PG预期、不删用例，也不笼统归入暂停。
合并为61个不同协议入口56pass/5fail，未把controls/重复执行数当独立入口或独立bug数量。

原2313测试、273完整对象/raw数组、248历史近期记录及725保护文件保持，pre-doc范围29/29通过；本批只追加partial，整个Goal active。
原C9的35类操作数据已核对但正常停机-6失败仍保留；本批不重跑或调查R15，不用只读资源进程正常退出宣称它已解决。

### 独立public游标阶段诊断

8条SQL与前批f3d025e驱动specifications AST逐字语义一致，不改SQL；原1072文件fixture只在私有副本上读取。
before/C1分别用同一原CPP编译两个程序：默认allocator的clocks和覆盖完整new家族的allocations，另独立evidence pass保存所有owning类型/NULL/OID/record字段和公开上下文。
每代48进程/144次结果核对（32warmup、48clock observations、48allocation observations、16evidence），各144000行/396000单元全部正确；无重测/源或fixture漂移。
16组evidence逐行/descriptor/typeOID/来源/公开context/实际图rowsloops相同，只排除跨进程指针数值和运行时间。真实图同址重启由public API核对；8条无参数，不将这份诊断冒称三origin全覆盖。

prepareBoundQuery、PQE构造/根常量规划、build实际计划/cursor、同图restart、first-next、remaining/EOF/close及childcleanup分别记录。
first-next包含真实源、键投影、必要排序、窗口和首结果验证；每node runtimeMs是含child的wall，不能加总或相减后硬称exclusive comparator CPU。
CLOCK_PROCESS_CPUTIME_ID与THREAD分别保留，allocation版elapsed不混clocks。已有背景数据库服务保持运行，/proc采样记录实际背景CPU/IO；不是全机独占测试。
初始绑定单观测约1.27–3.58ms；建图每case只有一次，前版11.73–37.61ms/C1 11.58–38.17ms，不能当三样本分布或被执行倍率掩盖。

下表每格为before/C1，执行观测p50各3个样本；完整min/max、所有raw/warmup/setup/128阶段表和inclusive节点在 `profile-window-public-api-before-C1-comparison-v1.json`、`profile-window-public-api-before-C1-phases128-v1.csv`。

| budget kB | case | wall before/C1 ms | process CPU before/C1 ms | new requests before/C1 | requested bytes before/C1 |
|---|---|---:|---:|---:|---:|
| 64 | partition_ranking | 114.459/55.861 | 113.007/54.972 | 378415/167235 | 27084041/22692189 |
| 64 | partition_lag | 97.826/42.938 | 97.826/42.938 | 349224/138044 | 20374226/15982374 |
| 64 | partition_lead | 88.733/43.067 | 89.297/43.067 | 349224/138044 | 20375242/15983390 |
| 64 | running_sum | 92.462/42.394 | 92.443/42.369 | 348731/137551 | 20470639/16078787 |
| 64 | sliding_sum | 96.009/42.892 | 96.702/42.892 | 351347/140167 | 20773842/16382130 |
| 64 | partition_full_sum | 78.560/41.134 | 78.560/41.134 | 293690/132411 | 18822687/15506147 |
| 64 | full_first_last_nth | 105.651/62.184 | 103.804/60.360 | 391907/180727 | 27480529/23088677 |
| 64 | mixed_same_keys | 142.681/61.721 | 141.507/59.918 | 427214/187589 | 29928150/24860382 |
| 16384 | partition_ranking | 89.697/37.685 | 89.843/37.685 | 344717/133537 | 19391291/14999439 |
| 16384 | partition_lag | 87.763/33.516 | 87.763/33.516 | 330670/119490 | 16443052/12051200 |
| 16384 | partition_lead | 87.185/35.018 | 87.345/34.941 | 330670/119490 | 16444068/12052216 |
| 16384 | running_sum | 88.229/33.614 | 88.229/33.614 | 330177/118997 | 16539465/12147613 |
| 16384 | sliding_sum | 85.023/35.539 | 85.023/35.539 | 332793/121613 | 16842710/12450998 |
| 16384 | partition_full_sum | 71.316/29.808 | 71.595/29.808 | 275136/113857 | 14891513/11574973 |
| 16384 | full_first_last_nth | 89.818/45.167 | 90.332/45.167 | 358209/147029 | 19787779/15395927 |
| 16384 | mixed_same_keys | 102.681/44.807 | 102.565/44.807 | 393516/153891 | 22235400/17167632 |


16组执行wall p50下降41.14%–61.90%、processCPU下降41.85%–61.90%，C++new请求下降53.89%–63.96%、请求字节下降15.98%–26.71%。
allocation程序完整scope（含warm/初建图/公开观察）累计请求23,693,613→10,349,519，bytes1,454,924,054→1,177,938,750；两侧outside-phase观察各1236请求/40346bytes，仍计入whole，没有隐藏暂停计数。
新native36的另一warm实际cursor成本控制为450603→73176请求、19260574→10314925bytes，阈值只是已验证的至少N请求节省；warm已分配的outercapacity不含在该执行差量中。
以上仅C++new主线程请求，未计malloc家族/C库、不是live/truepeak/RSS。不能用诊断的执行倍率直接代表整条SQL，或以跨前批old58倍率推导本次代码独立收益。

### 额外allocator快照控制

额外三条新SQL（兼容最终顺序的integer分区、puretextfallback、单行无窗口键比较）与common8分开，在真实public图、16MB、C9/C1各独立3进程上采样。
mallinfo2的uordblks/hblkhd和/proc VmRSS只作进程allocator/近似resident快照；保留C/C++库、tcache、arena碎片、代码页、采样时点与observer限制，不作truepeak或精确live C++断言。
不注入allocator头、不调tcache/mallopt/trim、不访问私有cache。18进程/18查询全部自然exit0，12006行/24012单元及162快照核对通过，源码/core/两程序/1072fixture哈希不变。
整数控制首次/末次取行的uordblks中位数C9 4,145,824B/C1 4,213,984B（+68,160B）；文本fallback +22,544B，单行 +48B。
EOF/显式close后差量仍为+20,544/+22,560/+48B，符合行payload被销毁而外层Input容量可保留的代码生命周期；不是精确cache对象live分解或泄漏阈值。
所有hblkhd样本266240B并单列；同期VmRSS中位差为−60/−92/+144kB，方向与uordblks不一致，不能把RSS全归cache、声称truepeak下降或认为destroy后必须归零。
完整3rep各点allraw/min/max/median在 `window-memory-snapshots-v3-summary54.csv` 与 `window-memory-snapshots-v3-summary.json`；27行双代表在 `window-memory-snapshots-v3-median27.txt`。
首个观察器私有类型Snapshot与dbms::Snapshot重名导致编译失败，未启动数据库；仅另建v2源将其改名ProcessMemorySnapshot，反向后原字节完全一致，原失败source/log/receipt保留。这是观察器修订，不是产品修复。

### 实际Main资源与整SQL性能

资源三阶段共43请求（28个完成fullreads、1个故意强杀read、14SET/SHOW），28000行/79000单元通过；64kB真实ordinary finalSort采到最多2匿名FD，16MB未采到，两者EOF无FD/命名文件残留。
在真实owned deleted FD上核exe/data/starttick后STOP/KILL，预期退出-9；同私有目录重开基表1000行/4000单元及再次1000行排序通过。
1072fixture与两程序/三个driver哈希不变，自有进程终态。只证明普通最终Sort，不能据此宣称Window/cache预算、硬RSS或任意写崩溃恢复完成。

整SQL最终计划固定C9/C1/C9，64kB→4MB→16MB，原common8与独立oracle、每label3warmup/12样本，计划864timed+216warmup+18SET-SHOW=1098请求。
该新对照两侧均为typed最终Sort资源策略，before不再使用old58；所有慢值/漂移/退出保留，任何失败不自动重跑。
最终完整三预算共864计时、216预热、18SET/SHOW=1098 SQL全部完成，全值1,080,000行/2,970,000单元相等；无失败、删样本、超时调整或重测，9个正常server contexts全部0，自有进程终态。
4MB/16MB的16项p50都低于两次相邻C9约22.3%–39.1%；64kB只有2项比两旧都快，3项比两旧都慢（partition_lag/running_sum/partition_full_sum），其余混合。
64kB基线首/回测漂移+13.2%到+75.8%，不能只挑某一基线或把慢值全归因宿主；该三项低预算慢值仍开放，没有统一加速结论。
原C9对old58的7类慢项是历史事实；本次未重测同轮old58，不用跨历史倍率推断它们已全部消除或更新PG速度差距。
共享宿主既有服务继续运行，内部测试/构建/资源全部终态后才计时；不是全机CPU隔离、并发吞吐或cold-cache验收。

下表为全部24项，单位ms，每label12样本；正百分比表示C1较慢。完整min/max/全部raw12样本、漂移及9份返回行JSONL的摘要在 `sort-audit-performance-candidate1-summary-v1.json/.csv`，不只保存中位数。

| 预算 | 查询 | C9首p50 ms | C1 p50 ms | C9回测p50 ms | C1对首% | C1对回测% | C9漂移% |
|---|---|---:|---:|---:|---:|---:|---:|
| 64kB | partition_ranking | 224.051 | 224.390 | 309.619 | +0.2 | -27.5 | +38.2 |
| 64kB | partition_lag | 170.931 | 400.267 | 298.498 | +134.2 | +34.1 | +74.6 |
| 64kB | partition_lead | 177.401 | 140.872 | 206.573 | -20.6 | -31.8 | +16.4 |
| 64kB | running_sum | 172.756 | 434.772 | 283.620 | +151.7 | +53.3 | +64.2 |
| 64kB | sliding_sum | 177.061 | 178.258 | 220.254 | +0.7 | -19.1 | +24.4 |
| 64kB | partition_full_sum | 164.327 | 356.838 | 288.815 | +117.2 | +23.6 | +75.8 |
| 64kB | full_first_last_nth | 228.509 | 238.205 | 258.738 | +4.2 | -7.9 | +13.2 |
| 64kB | mixed_same_keys | 249.660 | 242.527 | 316.148 | -2.9 | -23.3 | +26.6 |
| 4MB | partition_ranking | 210.563 | 150.408 | 199.489 | -28.6 | -24.6 | -5.3 |
| 4MB | partition_lag | 180.954 | 118.696 | 169.690 | -34.4 | -30.1 | -6.2 |
| 4MB | partition_lead | 170.904 | 116.212 | 167.337 | -32.0 | -30.6 | -2.1 |
| 4MB | running_sum | 170.419 | 120.963 | 183.011 | -29.0 | -33.9 | +7.4 |
| 4MB | sliding_sum | 170.152 | 122.839 | 168.741 | -27.8 | -27.2 | -0.8 |
| 4MB | partition_full_sum | 148.926 | 108.010 | 143.067 | -27.5 | -24.5 | -3.9 |
| 4MB | full_first_last_nth | 226.684 | 157.405 | 205.585 | -30.6 | -23.4 | -9.3 |
| 4MB | mixed_same_keys | 239.275 | 163.375 | 210.252 | -31.7 | -22.3 | -12.1 |
| 16MB | partition_ranking | 186.171 | 140.304 | 189.022 | -24.6 | -25.8 | +1.5 |
| 16MB | partition_lag | 170.351 | 119.973 | 191.287 | -29.6 | -37.3 | +12.3 |
| 16MB | partition_lead | 179.062 | 118.965 | 174.545 | -33.6 | -31.8 | -2.5 |
| 16MB | running_sum | 191.407 | 116.495 | 185.632 | -39.1 | -37.2 | -3.0 |
| 16MB | sliding_sum | 171.317 | 118.596 | 194.087 | -30.8 | -38.9 | +13.3 |
| 16MB | partition_full_sum | 168.116 | 108.054 | 154.448 | -35.7 | -30.0 | -8.1 |
| 16MB | full_first_last_nth | 210.216 | 151.616 | 206.870 | -27.9 | -26.7 | -1.6 |
| 16MB | mixed_same_keys | 211.227 | 152.916 | 211.433 | -27.6 | -27.7 | +0.1 |



### 未完成范围与复现

隐藏表达式无权威OID、不同行/跨stage缓存复用、非整数类型的一般预备比较、Window完整内存治理与其他原273能力仍开放。
本批不改保留计划、默认未规划Core策略、portal、格式错误需求、R177/R185、域扩展或任何用户暂停边界。
最终命令使用本批 `candidate1-cmake-receipt.json`、publicAPI冻结plans/矩阵、allocator计划、`sort-audit-resource-candidate1-final-v1.json` 和各预算实际计时receipt，在新的私有输出目录运行；不能覆盖原结果或用新重建冒充前版C9。
例如复现64kB整SQL，使用新的私有输出目录（另两预算按各自原receipt）：

```bash
python3 build/pg-feature-window-key-w4qeo1ai/runtime_benchmark_windows.py --before build/pg-feature-window-key-w4qeo1ai/before-dbms_main --after build/pg-feature-window-key-w4qeo1ai/candidate1-dbms_main --fixture build/pg-feature-topn-20261102-bc582b_s/fixture-1000 --output build/window-integer-key-reproduce-64kB --work-mem 64kB --rows 1000 --rounds 3 --samples 4
```

研究、源/core/header/stub、完整原始数据/所有失败、文档检查与安装/提交证据统一在本批 `final-provenance.json`；原273状态不因缓存或样本改善关闭。


## 窗口结果共享与排序投影（2026-11-03）

### 输入、实现和研究依据

本批基线master `d7793044fc8abdbd5d4d7e28432f7cabf3ada32b`，C1程序SHA256 `56d06bca0ddacd1d7c801b51d360d1d393765717f8d1aeda5b31b7f0196c6a19`。
工件目录 `build/pg-feature-window-compat-xi39wepx/`；S1 `570863d34ffea3c5341e94d4ff6dd8e5ebd4853e7cd7cdc3d572d4cbe43f6808`，S2 `779617cb2afb9067fdf329c7fbd367c8fec53785734b37cd7ebc29721dddda8d`。
S1完整正确性通过后减少准备期重复元数据工作，得到S2；下方保留S2完整首次性能失败事实。随后S3 `aa52b5fe35e1d1ef92a301bcad8b72d95b32edb705e650d3539b999d8c5b6b0f` 继续验证，不将S2慢值抹去或提前宣告回退已修。
仍为0.2.0/C++17、GCC13.3/-O2、ICU/zlib、TLSstub，没有修改持久化格式或语言标准。

[PG18.6 WindowAgg](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeWindowAgg.c#L2684)复用等价且不含易变函数的窗口调用；
[make_sort_input_target](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/optimizer/plan/planner.c#L6574)在合适的真实最终排序前拆出窗口值，
[add_new_columns_to_pathtarget](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/optimizer/util/tlist.c#L761)可能让推迟投影中的相同窗口叶引用同一已计算列。
没有实际Sort时该投影会恢复；若两个易变目标都要供排序，它们也不因此合并。不能仅检查有ORDER BY或合并相同SQL文字。
隐藏排序子树还影响调用顺序：原155观察第74项的调用总数相同，但PG可见值是3,5,…，C1为2,4,…，必须保持虚拟输入目标顺序。

仅修改ExecCPP私有状态/接线及PreparedWindowOp.inc，147头、Main、网络/真实frame-context-provider、原共享比较/CPU排序助手与R177/R185及所有用户暂缓策略保持。
每个原call/计算列、targetOwners、实际需求及原窗口声明winref均保留；同一实际stage内选择真正被需求的代表，逐行计算并向原列广播完整owning ExprValue，最后别名move，其余必要copy。
不跨行/跨重启缓存结果或替换真实上下文。真实最终Sort的top demanded stage按完整保留根和抽出窗口叶的顺序处理，中间stage只采用非易变等价复用；完整wrapper不被误当作其子窗口已经是顶层值。
S2另外仅在本次prepare内复用同原Expr的纯身份字符串，以必要函数/窗口/输入结果签名跳过不可能的完整身份计算；无postponed目标时不建没人读取的root身份，但仍按原hidden/priorKey遍历。
没有重跑infer/fold、改写用户AST或改变保留规划/参数帧策略。通用已分析表达式的CAST/常量归一化仍未完成；额外6个PG合法正例24观察单独保留，未混入固定220或冒称候选已通过。

### 正确性、成本与保留失败

新固定220包括53条窗口查询及DDL/序列seed/post，独立PG18.6为220/0，C1为220/27，S1/S2均220/0；S1/S2完整case/fullactual/raw/normalized/match逐对象相同。
原155完整raw保持，唯一对象身份归一化由真实id LIMIT0锚定物理表OID，字段类型/格式/消息顺序/实际值/错误/计数均严格保留。S3新220也完整通过并与S2逐对象相同，其他最终门见文末。
原71native加新10入口共72全部通过；新10实际读取512行物理表，经公开绑定/真实图/cursor验证纯LAG/SUM、递归record及OID数组、重启/提前close、不同命名窗口、LIMIT0与22012。
独立审查后错误前缀加强为固定完整integer行[[1,0,0],[2,-1,-1]]、3列真实OID23和两次重启一致；a585bd7a源在C1/S1/S2/S3均10/0，较弱旧源和原日志保留，不改任何原2316测试文件。

S2旧61协议首次全跑55pass/6fail，其中新增1项在start_ours连接103、planned190/recorded0/attempted0，是0SQL采集失败，不是190项语义错误或验收通过。
首次1158B日志SHA `cc55476f92162b5dd7beb49e57439916caa3db5e64e1d45ea1052b84b4e2893d` 保留；全部任务终态后同源同S2独立一次完整190/0，含53项预期SQL错误，cleanup和两组序列基线健康，前后自有S2进程均无残留。
按各入口最终完成观察旧61为56pass/5fail，加新220后共62不同入口57pass/5fail；首轮合并new220的62入口56pass/6fail没有被改写。
Bind463/40、CTE747/29、nested325/42整log与C1/S1字节一致，interval106/10完整JSON一致（log只输出路径变化），原四1641观察不变。
原143恰42/43/85/86/128/129六项改善，14→8，另外137项fullactual/normalized/match相同；S2整143观察JSON与S1逐字节一致。
剩余catalog来源2、record-json同行错误需求3、compound普通format1二进制输出3仍开放，不笼统归入暂停。二进制完整真实字段OID/域枚举codec/元素结构方案保存在binary-design-v2-complete-metadata.json，尚无产品修复。

下表为同源512行warm公开cursor，计量包括restart→完整读取→显式close及owning接收行，初始绑定/建图在外。仅当前线程累计C++new请求，不是malloc全量、live/truepeak/RSS或整SQL速度。

| 512行warm公开cursor | C1 new请求 | S2 new请求 | C1请求字节 | S2请求字节 |
|---|---:|---:|---:|---:|
| lag-1 | 226489 | 226490 | 25625206 | 25625214 |
| lag-2 | 419723 | 232335 | 48048290 | 26317466 |
| lag-4 | 806565 | 244394 | 92906086 | 27713486 |
| sum-2 | 44542 | 43514 | 4621224 | 4432054 |

双/四LAG请求减少44.65%/69.70%，请求字节减少45.23%/70.17%；单LAG仍多1请求/8B，双SUM请求只少2.31%，不隐藏控制项。
S1/S2所有版本、构建与失败均保留。调查脚本首次PG路径契约断言在SQL前停止，仅新v2 resolve路径；原型v2隔离syntax因if-init/else同名found编译失败，v3只改aggregateFound；首次native runner在读取缺省配置时失败、未编译/执行，v2只优先显式--config。
这些是观察器/原型修订，不冒充产品运行故障修复。新增O(producer)元数据和必要owning别名拷贝，不宣称零成本或窗口work_mem/spill已完成。

### S2实际资源和首轮全部整SQL数据

S2资源55=40完整读取+1预期强杀在途读取+14SET/SHOW，40000行/118000单元严格核对通过，3stage各exit0，5个正常服务context成功。
common12三label全值与digest相等；64kB宽finalTypedSort最多2匿名deletedFD、16MB本次未采到，EOF无FD/命名残留（高预算未偷加硬no-spill断言）。
真实ownedPID/exe/data/starttick和deletedFD确认后SIGKILL -9，同私有目录重开1000×4基表及1000×3再次排序通过。216source/2exe/3driver/1072原fixture前后exact，自有进程全退出。
只证明普通最终Sort及该只读强杀场景，不能推广为Window内部预算、硬RSS或任意写崩溃恢复。原C9的35类操作值0错误但停机-6失败继续保留，本批不重复或调查R15。

最终时钟计划在看到候选速度前已冻结：原common8完整SQL/oracle不变，加length_lag_1/2/4与duplicate_running_sum，共12类；64kB→4MB→16MB，每预算C1/S2/C1、各3warm和12timed样本。
一次完成1296timed+324warm+18SET/SHOW=1638请求，结果查询1620000行/4725000单元全值相等，9正常contexts通过，源/二进制/driver/1072fixture不变，无删样本/超时调整/重复窗口。
全36项为10快于两旧、19慢于两旧、7夹在两旧；原common8的24项仅1快/17慢/6混合。S2不能宣称整体提速。
重复两份LAG(length)相对两旧减少37.95%–41.66%，四份减少64.46%–66.16%；单份及双SUM仍有混合/回退。
16MB lead的C1首307.820→回测115.244ms漂移−62.56%，S2 285.429ms；全部漂移保留，不只挑某一基线，也不归因宿主。
下表单位ms；“降低%”正值较快、负值较慢。完整min/max/每label12个raw、CSV与所有返回行JSONL均在perf-window-sharing-S2-latency-summary-v1.json及所引用工件，未只保存中位数。

| work_mem | case | C1 first p50 ms | S2 p50 ms | C1 last p50 ms | S2 reduction vs first % | S2 reduction vs last % | C1 last drift % |
|---|---|---:|---:|---:|---:|---:|---:|
| 64kB | partition_ranking | 173.532 | 177.773 | 182.156 | -2.44 | 2.41 | 4.97 |
| 64kB | partition_lag | 125.788 | 148.910 | 129.082 | -18.38 | -15.36 | 2.62 |
| 64kB | partition_lead | 126.005 | 132.281 | 114.355 | -4.98 | -15.68 | -9.25 |
| 64kB | running_sum | 143.138 | 129.086 | 145.252 | 9.82 | 11.13 | 1.48 |
| 64kB | sliding_sum | 140.393 | 143.582 | 127.438 | -2.27 | -12.67 | -9.23 |
| 64kB | partition_full_sum | 117.366 | 119.709 | 119.305 | -2.00 | -0.34 | 1.65 |
| 64kB | full_first_last_nth | 180.729 | 206.624 | 182.445 | -14.33 | -13.25 | 0.95 |
| 64kB | mixed_same_keys | 182.069 | 196.358 | 171.680 | -7.85 | -14.37 | -5.71 |
| 64kB | length_lag_1 | 469.501 | 459.604 | 478.152 | 2.11 | 3.88 | 1.84 |
| 64kB | length_lag_2 | 819.013 | 491.956 | 831.152 | 39.93 | 40.81 | 1.48 |
| 64kB | length_lag_4 | 1527.467 | 542.869 | 1539.974 | 64.46 | 64.75 | 0.82 |
| 64kB | duplicate_running_sum | 145.805 | 142.622 | 143.136 | 2.18 | 0.36 | -1.83 |
| 4MB | partition_ranking | 137.750 | 150.121 | 131.746 | -8.98 | -13.95 | -4.36 |
| 4MB | partition_lag | 106.364 | 123.369 | 116.593 | -15.99 | -5.81 | 9.62 |
| 4MB | partition_lead | 122.814 | 123.321 | 115.625 | -0.41 | -6.66 | -5.85 |
| 4MB | running_sum | 126.085 | 119.856 | 111.394 | 4.94 | -7.60 | -11.65 |
| 4MB | sliding_sum | 114.992 | 126.621 | 108.795 | -10.11 | -16.39 | -5.39 |
| 4MB | partition_full_sum | 106.378 | 114.607 | 104.872 | -7.74 | -9.28 | -1.41 |
| 4MB | full_first_last_nth | 149.097 | 160.410 | 152.103 | -7.59 | -5.46 | 2.02 |
| 4MB | mixed_same_keys | 171.065 | 176.192 | 151.359 | -3.00 | -16.41 | -11.52 |
| 4MB | length_lag_1 | 451.819 | 452.725 | 447.706 | -0.20 | -1.12 | -0.91 |
| 4MB | length_lag_2 | 818.602 | 478.372 | 770.915 | 41.56 | 37.95 | -5.83 |
| 4MB | length_lag_4 | 1509.066 | 522.556 | 1471.379 | 65.37 | 64.49 | -2.50 |
| 4MB | duplicate_running_sum | 149.271 | 133.864 | 121.386 | 10.32 | -10.28 | -18.68 |
| 16MB | partition_ranking | 137.037 | 135.843 | 133.899 | 0.87 | -1.45 | -2.29 |
| 16MB | partition_lag | 156.787 | 152.173 | 114.846 | 2.94 | -32.50 | -26.75 |
| 16MB | partition_lead | 307.820 | 285.429 | 115.244 | 7.27 | -147.67 | -62.56 |
| 16MB | running_sum | 116.051 | 119.982 | 124.878 | -3.39 | 3.92 | 7.61 |
| 16MB | sliding_sum | 125.055 | 164.343 | 117.692 | -31.42 | -39.64 | -5.89 |
| 16MB | partition_full_sum | 111.952 | 131.466 | 108.724 | -17.43 | -20.92 | -2.88 |
| 16MB | full_first_last_nth | 150.704 | 155.455 | 148.759 | -3.15 | -4.50 | -1.29 |
| 16MB | mixed_same_keys | 154.055 | 228.243 | 152.106 | -48.16 | -50.06 | -1.27 |
| 16MB | length_lag_1 | 456.716 | 443.929 | 455.575 | 2.80 | 2.56 | -0.25 |
| 16MB | length_lag_2 | 799.178 | 466.274 | 787.610 | 41.66 | 40.80 | -1.45 |
| 16MB | length_lag_4 | 1529.988 | 517.803 | 1474.087 | 66.16 | 64.87 | -3.65 |
| 16MB | duplicate_running_sum | 130.807 | 132.309 | 130.052 | -1.15 | -1.74 | -0.58 |

### 为定位回退而另行完成的诊断

前批64kB三慢项另用C9/C1单64kB交错旋转：16warm+48观察+4SET/SHOW=68请求，64000行/176000单元全过，真实自有PID/TID的CPU/IO/schedstat及first/lastDataRow/Ready另列。
该短实验C1三旧慢项lag/running/fullsum的wall中位158.609/143.099/143.357ms、serverCPU粗样130/130/110ms；C9为231.649/218.707/202.783ms和200/190/180ms。
旧慢值按case×执行位置成簇；新短交错未复现旧高段不等于旧回退修复。该数据既不替代前批864原样本，也不是S3加速证据。

S2首轮普通查询回退后，又冻结单4MB C1/S2交错同common8的68请求，64000行/176000单元全部正确，2正常contexts和输入哈希通过；不是重测/替换原36表。
8类wall p50为S2五高三低，processCPU三高三低二同；排名、lead、sliding的线程运行时间中位132.761→163.345、117.614→136.233、122.466→136.571ms，增长主要在首DataRow前。
但lag/running/mixed CPU较低、fullsum/firstlast相同，不能把所有回退解释为CPU或某一种等待。7/8类logical IO跨六观察相等，read_bytes均0不能排除写等待/锁/网络；100Hz CPU/proc非原子窗口较clientwall宽3.609–8.461ms。
外层wrapper首轮误用helper路径scripts/而非tests/，在0SQL预检前失败，原receipt保留；新wrapper只修路径，frozen诊断driver/SQL/序列完全不变。

然后采用逐字原CPP6437e216、独立默认allocator clocks和覆盖new的allocation可执行文件，以真实prepareBoundQuery→新PQE/原rootplanner→实际图/provider→cursor执行。
单4MB、C1/S2/C1、common8共48独立进程，各首cold+3同图真实重开=192执行、192000行/528000单元全部校验通过，48PID全终态；未运行可选16个额外evidence，不能冒称该lane也通过。
warm执行CPU的C1首/S2/C1回测（ms）依次为：ranking41.758/39.632/42.871，lag35.141/35.301/34.580，lead34.987/35.456/34.173，running35.838/34.791/32.700，sliding38.181/42.261/35.371，fullsum33.622/32.645/32.303，first-last-nth48.792/51.132/49.387，mixed48.017/48.392/49.143。
暖CPU仍1低/4高/3混合，新请求只在每次open多1/6/7个，并未观察到逐行新增请求；不是“所有phase无回退”。初始setup每进程只有一次，S2 12.829–42.504ms CPU不是建图分布。
label私有路径长度不同（S2比C1-first短6、比C1-last短1字符），请求bytes含真实路径字符串，少量几KB差异不能归因共享节省。所有原始phase和inclusive节点wall保留，不相减命名exclusive comparator CPU。

### S3改进与仍未完成范围

静态核对确认common8的完整共享身份构造均为0次：单窗口早return，三不同函数以必要签名前缀拒绝；allretained不建root身份。
有限的metadata/open额外请求不足以静态解释全部10–20ms；因此既不猜测主因，也不删除正确共享语义。S3只对完整stage的两套owner均self、无stage外incoming alias、两套order投影均原producer次序的情况恢复原C1类型化demand/move热循环。
这个条件对实际demand任意子集和top-stage切换都成立；隐藏重排第74项及任何可能共享/only-alias需求仍走S2通用路径，构造期flag跨close/restart保持。不是恢复legacy receiver、伪造上下文或改变默认Core准入。
S3构建与新220通过后继续执行完整回归、资源和原计划性能；其终态及全部慢值在下方追加，恢复旧循环本身不作为加速证据。

原273完整对象、249历史、2316既有测试文件与725保护记录保持，22complete/166partial/70unverified/15originaldeferred不变，整体Goal active。
本批额外请求不证明内存live/peak下降，Window partition/stage排序仍无完整预算。所有原始慢值、初始失败、S1/S2版本和诊断保存在同工件目录；最终安装/提交及命令收据见文末验收说明。

### S3最终完整结果

最终候选S3 `aa52b5fe…` 的原71native与新10入口全部通过，共72不同入口；完整旧61协议首轮56pass/5fail，加新220后62不同入口57pass/5fail，本轮190首次完整通过、没有startup失败或复试。
原三失败入口全文log、interval106/10及143/8完整JSON与S2字节一致。190的全部服务observation/rawFrames/oracle及终态相同，只有请求中的自有schema UUID不同，不声称整log byteequal或归一化服务响应。严格wholelog首版complete=false和后续逐服务对象正确对照均保留。

S3同源512行warm cursor的实际累计请求如下；单份请求数已等于C1，-3B只是微小布局记账，不作为内存收益。S2原表和所有版本继续保留。

| 512行warm公开cursor | C1 new请求 | S3 new请求 | C1请求字节 | S3请求字节 |
|---|---:|---:|---:|---:|
| lag-1 | 226489 | 226489 | 25625206 | 25625203 |
| lag-2 | 419723 | 232335 | 48048290 | 26317460 |
| lag-4 | 806565 | 244394 | 92906086 | 27713474 |
| sum-2 | 44542 | 43514 | 4621224 | 4432054 |

独立S3实际Main资源55请求全部通过：40完整读取+1指定强杀读取+14SET/SHOW，40000行/118000单元，5正常context成功退出。64kB宽finalSort最多2个匿名deleted FD、16MB未采到，EOF/退出无残留；确认自有PID/exe/data/starttick与FD后SIGKILL -9，同目录重开完整1000×4基表及1000×3排序。
216源、2程序、3driver、1072夹具文件前后exact，自有进程无残留。范围仍是普通最终Sort及该只读强杀场景；不证明窗口内部预算或任意写崩溃恢复，旧35操作停机-6失败不被改写。

S3沿用观察任何候选速度前冻结的同一12类、三预算、C1/S3/C1顺序，完整运行一次1296timed+324warm+18SET/SHOW=1638请求，1620000行/4725000单元全部校验通过，9正常contexts成功，输入哈希不变；没有删样本或为慢值重测。
全部36组按p50数值为11快于两旧/19慢于两旧/6夹两旧；原common8×3为3快/16慢/5夹两旧。64kB共同8项全部高于两旧；4MB滑动SUM仍高8.13%–10.16%，不能称整体回退已修。
重复两份LAG(length)在三预算相对两旧降低37.94%–40.83%，四份降低63.65%–66.34%；单份和双SUM控制仍有较慢项。全矩阵最大增长17.48%，两旧p50漂移−11.01%…+11.92%，均原样保留。
分类只是数值大小，不是显著性检验；4MB first/last/nth较old-last只高约0.004ms，也按严格规则计入慢项。样本中较大离群值没有剔除，也不凭数据自动归因宿主或某一等待。
单位ms；降低百分比正为更快、负为更慢。下表完整保留36组；每label原12样本/min/p50/max在perf-window-sharing-S3-latency-summary-v1.json的108组数据、labels108/all1296 CSV和9份完整返回值JSONL中，没有只保留中位数。

| work_mem | case | C1 first p50 ms | S3 p50 ms | C1 last p50 ms | S3 reduction vs first % | S3 reduction vs last % | C1 last drift % |
|---|---|---:|---:|---:|---:|---:|---:|
| 64kB | partition_ranking | 195.299 | 196.812 | 175.287 | -0.77 | -12.28 | -10.25 |
| 64kB | partition_lag | 128.646 | 130.350 | 123.430 | -1.32 | -5.61 | -4.05 |
| 64kB | partition_lead | 125.024 | 129.338 | 128.027 | -3.45 | -1.02 | 2.40 |
| 64kB | running_sum | 125.922 | 135.268 | 124.349 | -7.42 | -8.78 | -1.25 |
| 64kB | sliding_sum | 127.261 | 131.225 | 130.674 | -3.11 | -0.42 | 2.68 |
| 64kB | partition_full_sum | 113.076 | 125.923 | 116.411 | -11.36 | -8.17 | 2.95 |
| 64kB | full_first_last_nth | 188.388 | 208.098 | 177.136 | -10.46 | -17.48 | -5.97 |
| 64kB | mixed_same_keys | 181.970 | 200.893 | 189.618 | -10.40 | -5.95 | 4.20 |
| 64kB | length_lag_1 | 468.338 | 469.167 | 457.220 | -0.18 | -2.61 | -2.37 |
| 64kB | length_lag_2 | 830.362 | 495.874 | 799.023 | 40.28 | 37.94 | -3.77 |
| 64kB | length_lag_4 | 1521.358 | 545.786 | 1501.571 | 64.13 | 63.65 | -1.30 |
| 64kB | duplicate_running_sum | 139.884 | 144.773 | 140.875 | -3.49 | -2.77 | 0.71 |
| 4MB | partition_ranking | 146.149 | 145.419 | 144.285 | 0.50 | -0.79 | -1.27 |
| 4MB | partition_lag | 116.119 | 111.141 | 129.964 | 4.29 | 14.48 | 11.92 |
| 4MB | partition_lead | 117.443 | 113.130 | 117.826 | 3.67 | 3.99 | 0.33 |
| 4MB | running_sum | 119.568 | 110.775 | 114.544 | 7.35 | 3.29 | -4.20 |
| 4MB | sliding_sum | 118.785 | 130.852 | 121.009 | -10.16 | -8.13 | 1.87 |
| 4MB | partition_full_sum | 109.375 | 115.551 | 115.172 | -5.65 | -0.33 | 5.30 |
| 4MB | full_first_last_nth | 155.423 | 156.515 | 156.511 | -0.70 | -0.00 | 0.70 |
| 4MB | mixed_same_keys | 157.088 | 158.507 | 157.522 | -0.90 | -0.63 | 0.28 |
| 4MB | length_lag_1 | 454.208 | 450.737 | 461.849 | 0.76 | 2.41 | 1.68 |
| 4MB | length_lag_2 | 800.620 | 485.409 | 793.899 | 39.37 | 38.86 | -0.84 |
| 4MB | length_lag_4 | 1506.153 | 513.692 | 1488.218 | 65.89 | 65.48 | -1.19 |
| 4MB | duplicate_running_sum | 132.668 | 132.040 | 143.253 | 0.47 | 7.83 | 7.98 |
| 16MB | partition_ranking | 143.034 | 140.652 | 138.311 | 1.67 | -1.69 | -3.30 |
| 16MB | partition_lag | 115.122 | 120.195 | 112.400 | -4.41 | -6.93 | -2.36 |
| 16MB | partition_lead | 118.813 | 120.234 | 112.952 | -1.20 | -6.45 | -4.93 |
| 16MB | running_sum | 122.489 | 114.241 | 113.765 | 6.73 | -0.42 | -7.12 |
| 16MB | sliding_sum | 126.976 | 121.956 | 114.475 | 3.95 | -6.54 | -9.85 |
| 16MB | partition_full_sum | 117.731 | 110.108 | 104.769 | 6.48 | -5.10 | -11.01 |
| 16MB | full_first_last_nth | 154.657 | 156.755 | 147.194 | -1.36 | -6.50 | -4.83 |
| 16MB | mixed_same_keys | 153.827 | 154.108 | 148.808 | -0.18 | -3.56 | -3.26 |
| 16MB | length_lag_1 | 448.733 | 452.893 | 442.649 | -0.93 | -2.31 | -1.36 |
| 16MB | length_lag_2 | 798.865 | 472.681 | 783.154 | 40.83 | 39.64 | -1.97 |
| 16MB | length_lag_4 | 1518.115 | 510.973 | 1459.820 | 66.34 | 65.00 | -3.84 |
| 16MB | duplicate_running_sum | 134.385 | 132.332 | 128.922 | 1.53 | -2.64 | -4.07 |

### 复现与当前边界

本地不可变S1/S2/S3、前后源/62对象、61 core+实际test stub、flags、全部协议raw、资源/时钟/phase、首次失败与最终收据均在上述工件目录；它们是本机证据，不是发布包内容。新原生和固定220协议资产已纳入版本管理和标准发现/注册。
使用新的输出目录可按同一driver重放单预算（其他两预算分别改为4MB、16MB）；此命令是复现说明，本批实际执行只计前述一次完整矩阵：

```sh
p=build/pg-feature-window-compat-xi39wepx
python3 "$p/runtime_benchmark_window_sharing_v1.py"   --before "$p/before-dbms_main" --after "$p/sharing3-dbms_main"   --fixture build/pg-feature-topn-20261102-bc582b_s/fixture-1000   --output build/window-sharing-manual-replay-64kB   --work-mem 64kB --rows 1000 --rounds 3 --samples 4
```

S3解决固定27差异并显著减少重复调用成本，但普通查询仍有回退；一般已分析表达式归一化、完整窗口内存预算、剩余catalog/peer-error/binary及其他原273要求继续开放。没有新的PG速度对照，不能用本项目两版倍率宣称达到PG性能。
原273对象及raw数组、原249历史原文、2316既有测试文件和725保护记录保持，近期记录只新增1条partial（合计250），总账22/166/70/15不变；Goal active。最终范围/元数据/文档检查、安装与提交收据在同目录final-provenance.json及postcommit-provenance-v1.json。

最终结构检查：版本/兼容契约、273总账覆盖、注册脚本语法、新协议AST/fixture JSON及diff空白检查均通过。754份Markdown、2429个本地文件目标无缺失/空文档/整篇重复或清单遗漏；该检查不包含全部外部URL/历史锚点。当前新增报告标题唯一，局部锚点由本批范围审计另核。

## 窗口暂存分配与类型输出调研（2026-11-03）

本批基线为 `3d576b8b` 的 S3 `aa52b5fe…`，并非旧 C1/C9。研究和隔离候选使用 `build/pg-feature-typed-wire-zbkoh3z7`；本节中的未接受提案不代表已发布功能。原273仍为22complete/166partial/70unverified/15originaldeferred，Goal active。

### 原理与验证边界

[PostgreSQL 18.6 WindowAgg](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeWindowAgg.c)保留每个窗口调用的执行状态，并区分结果/聚合上下文与分区缓存；这提供状态复用的参考，不能由此推导本程序查询会更快。
本批候选仅复用实际 WindowOp 的结果暂存容量和调用分类。每行仍依原次序计算普通窗口函数、推进聚合、计算聚合窗口、发布完整拥有的值；成功发布后重置每个暂存值，异常和close清空，不跨行缓存结果。三份vector头增加每算子72B；未接入窗口输入/分区缓存的完整work_mem预算。

scratch1在open时分配，源码 `61eb91c2…`、程序 `c0c8f259…`；scratch2将同一初始化移到原输入初始化/成功读取之后，源码 `23f7b8e0…`、程序 `14b834be…`。后者避免空输入分配并保留原输入先行次序；对CPU的影响仍只是待验证假设，不能作为第一版退化的已证实原因。

### 原生累计分配成本

以下为同一永久原生10控制源码 `a585bd7a…`、真实512行物理表、实际bind/graph/public cursor及完整结果校验。表中new是累计C++请求次数/请求字节，不是live/peak/RSS、预算或延迟。

| control | S3 calls/bytes | scratch1 calls/bytes | scratch2 calls/bytes | scratch2 call reduction vs S3 |
|---|---:|---:|---:|---:|
| lag-1 | 226489/25612768 | 225977/25522653 | 225977/25522656 | 512 |
| lag-2 | 232335/26304944 | 231823/26214826 | 231823/26214832 | 512 |
| lag-4 | 244394/27700796 | 243882/27610672 | 243882/27610684 | 512 |
| sum-2 | 43514/4430452 | 42490/4336244 | 42490/4336244 | 1024 |

另一个隔离6控制源码 `386bc30e…` 对N=0/1/512和LAG/SUM分别新建真实图，再同图重启；每代12次完整cursor执行、2052行4104单元，三代全部通过。引擎已初始化，“cold”仅指新图首次执行，图析构不在测量区间。scratch2相对S3：

| 输入 | LAG cold calls/bytes增量 | SUM cold calls/bytes增量 | warm calls减少 |
|---|---:|---:|---:|
| N=0 | 0 / +72 | 0 / +72 | 0 / 0 |
| N=1 | +1 / +80 | 0 / +72 | 1 / 2 |
| N=512 | -510 / -89856 | -1022 / -93952 | 512 / 1024 |

scratch1的N=0 cold额外2次/256B；scratch2去掉其中2次/184B，剩72B对象头。N1/N512各阶段与scratch1计数/字节相同。永久10控制的三版全部原始字节也保留，scratch2的LAG1/2/4请求字节相较scratch1分别多3/6/12B，不归一化或据此编造原因。详细阶段见 `perf-window-scratch2-cold6-phase-summary-v1.json`。

### scratch1整SQL：拒绝接受的退化候选

冻结原12案例、64kB/4MB/16MB顺序、S3-first/scratch1/S3-last，每组合3warm+12timed，15秒原超时；一次完整1638请求=1296timed+324warm+18SET，完整验证1620000行4725000单元，9正常context退出0，216源码/两程序/三driver/1072fixture前后一致。默认生产allocator和持久化设置；计时从发送查询至完整结果校验，期间不采资源/调度计数。
全部36组p50仅4组低于两次S3、21组高于两次S3、11组夹在两旧间；原common8的24组为3/15/6。最高增幅161.63%，基线首末漂移-26.05%至+13.91%。**scratch1未安装、未提交为优化。** 完整慢值、所有1296样本、每组raw12/min/p50/max保留，不使用后来的短诊断覆盖这轮数据。

| work_mem | case | S3 first p50 ms | scratch p50 ms | S3 last p50 ms | scratch reduction vs first % | scratch reduction vs last % | S3 last drift % |
|---|---|---:|---:|---:|---:|---:|---:|
| 64kB | partition_ranking | 203.111 | 354.824 | 176.540 | -74.69 | -100.99 | -13.08 |
| 64kB | partition_lag | 126.282 | 320.000 | 122.308 | -153.40 | -161.63 | -3.15 |
| 64kB | partition_lead | 127.657 | 164.768 | 122.826 | -29.07 | -34.15 | -3.78 |
| 64kB | running_sum | 128.549 | 138.051 | 123.931 | -7.39 | -11.39 | -3.59 |
| 64kB | sliding_sum | 124.450 | 181.079 | 122.034 | -45.50 | -48.38 | -1.94 |
| 64kB | partition_full_sum | 113.881 | 125.901 | 114.627 | -10.55 | -9.84 | 0.65 |
| 64kB | full_first_last_nth | 179.434 | 198.195 | 175.571 | -10.46 | -12.89 | -2.15 |
| 64kB | mixed_same_keys | 206.266 | 189.023 | 173.798 | 8.36 | -8.76 | -15.74 |
| 64kB | length_lag_1 | 470.819 | 454.355 | 464.055 | 3.50 | 2.09 | -1.44 |
| 64kB | length_lag_2 | 492.316 | 496.843 | 483.284 | -0.92 | -2.81 | -1.83 |
| 64kB | length_lag_4 | 607.205 | 612.477 | 545.463 | -0.87 | -12.29 | -10.17 |
| 64kB | duplicate_running_sum | 191.176 | 147.362 | 141.367 | 22.92 | -4.24 | -26.05 |
| 4MB | partition_ranking | 153.456 | 148.074 | 152.613 | 3.51 | 2.97 | -0.55 |
| 4MB | partition_lag | 118.254 | 130.883 | 121.368 | -10.68 | -7.84 | 2.63 |
| 4MB | partition_lead | 117.002 | 127.851 | 115.770 | -9.27 | -10.44 | -1.05 |
| 4MB | running_sum | 117.963 | 127.260 | 134.369 | -7.88 | 5.29 | 13.91 |
| 4MB | sliding_sum | 120.300 | 135.320 | 128.492 | -12.49 | -5.31 | 6.81 |
| 4MB | partition_full_sum | 111.160 | 114.533 | 107.668 | -3.03 | -6.38 | -3.14 |
| 4MB | full_first_last_nth | 157.987 | 158.967 | 168.015 | -0.62 | 5.38 | 6.35 |
| 4MB | mixed_same_keys | 160.712 | 161.856 | 158.707 | -0.71 | -1.98 | -1.25 |
| 4MB | length_lag_1 | 454.972 | 501.635 | 454.137 | -10.26 | -10.46 | -0.18 |
| 4MB | length_lag_2 | 477.550 | 501.047 | 506.483 | -4.92 | 1.07 | 6.06 |
| 4MB | length_lag_4 | 522.525 | 536.967 | 529.167 | -2.76 | -1.47 | 1.27 |
| 4MB | duplicate_running_sum | 135.406 | 136.775 | 134.179 | -1.01 | -1.93 | -0.91 |
| 16MB | partition_ranking | 138.604 | 140.286 | 136.501 | -1.21 | -2.77 | -1.52 |
| 16MB | partition_lag | 116.817 | 116.316 | 116.731 | 0.43 | 0.36 | -0.07 |
| 16MB | partition_lead | 114.565 | 114.882 | 124.229 | -0.28 | 7.52 | 8.44 |
| 16MB | running_sum | 118.356 | 113.567 | 114.579 | 4.05 | 0.88 | -3.19 |
| 16MB | sliding_sum | 122.800 | 118.270 | 116.523 | 3.69 | -1.50 | -5.11 |
| 16MB | partition_full_sum | 116.870 | 108.085 | 103.104 | 7.52 | -4.83 | -11.78 |
| 16MB | full_first_last_nth | 152.575 | 155.131 | 151.999 | -1.67 | -2.06 | -0.38 |
| 16MB | mixed_same_keys | 149.062 | 152.287 | 151.586 | -2.16 | -0.46 | 1.69 |
| 16MB | length_lag_1 | 453.689 | 451.642 | 445.748 | 0.45 | -1.32 | -1.75 |
| 16MB | length_lag_2 | 473.076 | 470.906 | 465.330 | 0.46 | -1.20 | -1.64 |
| 16MB | length_lag_4 | 513.528 | 519.661 | 514.196 | -1.19 | -1.06 | 0.13 |
| 16MB | duplicate_running_sum | 135.170 | 130.543 | 127.859 | 3.42 | -2.10 | -5.41 |

完整108标签分布/1296样本/slow-values分别见 `perf-window-scratch-latency-labels108-v1.csv`、`perf-window-scratch-latency-all1296-v1.csv`、`perf-window-scratch-slow-values-v1.json`，汇总为 `perf-window-scratch-latency-summary-v1.json`。旧S2/S3数据仍各按原输入解释。

### scratch1阶段诊断与资源

真实公开API的默认allocator计时和全局new计数分开执行：共48进程、192次执行、192000行528000单元，全部值/输入/进程清理核对通过。原common8的warm CPU，2项低于两次S3、2项高于两次S3、4项夹两旧间；runningSUM的增长主要出现在firstNext，不能仅归为逐行计算。新图setup只有每进程单个值，不作为分布；节点wall含子调用，不能当互斥阶段相减。诊断没有建立统一的CPU/宿主/调度因果。
另一个真实Main 4MB的68请求诊断（16warm/48observation/4SET）验证64000行176000单元、两个正常context。8项Ready p50有5增3降，进程CPU粗粒度计数3增1同4降；这些是独立短观察，不替代完整1638计时，不将宽时间括号的线程/proc数据称为独占会话CPU。来源为 `perf-scratch-causal-diagnostics-static-v1.json` 与 `perf-main-S3-scratch-static-analysis-v1.json`。

scratch1另有55资源请求：40次完整读取、1次已验证归属的只读进程kill、14SET/SHOW，40000行118000单元通过；5正常context退出0、故意kill退出-9、同目录重开完整base与最终Sort输出通过。64kB最终Sort实际最多2个deleted FD，16MB采样未见，正常EOF和正常context退出后清理通过。采样0不证明绝不spill；不据此宣称窗口预算、写入崩溃一致性或R15停机专项已修。首次静态汇总使用覆盖整个本批目录的过宽进程选择器，触发断言失败且未记录该瞬时PID身份，原失败保留；v2只核对实际资源目录/程序与原owned_after=[]，当时精确扫描为空，未重跑资源以取代失败。

### scratch2完整对照及最终决定

scratch2真实CMake编译退出0；仅Exec对象变化，147公共头和其余生产输入不变。71个既有原生入口及永久10控制全部通过（72个不同入口），额外cold6同源全部通过。既有61协议为56通过/5保留失败，加窗口220为62个不同入口57/5；五失败完整日志或JSON与scratch1/S3逐字相同，原四1641及143/8均未改写。
首轮220保留全部观察和COMPLETE/0失败日志，但未持久化外层进程退出码；为补终态证据，同源独立v2跑220/0、退出0，完整响应与首轮/scratch1/S3相同，未覆盖首轮文件。固定301也完整采集且与S3全部相同，driver仍退出1（201处既有PG差异），其自有服务器正常退出0，不能把它计成PG验收通过。

独立资源55也全部通过：40000行118000单元、5正常context退出0；64kB最终Sort最多2deleted FD、16MB采样0，正常EOF/退出无残留。实际归属PID2960022在有1个deleted FD时被预期kill(-9)，同目录PID2960055重开完整base1000×4、sort1000×3校验通过并正常退出。结果/输入/归属证明见 `perf-resource55-scratch2-final-summary-v1.json`；作用仍限这些实际资源控制。

随后单独一次完整1638请求/1296timed/324warm/18SET、1620000行4725000单元，64kB→4MB→16MB及9正常context全部退出0，216source/两binary/三driver/1072fixture前后一致。原driver/12案例/3warm/12timed/15秒超时未改，最终时钟未并行执行本任务编译、SQL回归或proc采样。共享主机仍非独占，未建立性能因果或置信区间。

全部36组p50为12项低于两次S3、8项高于两次S3、16项居间；原common24为8/4/12。64kB为10/0/2，4MB为1/5/6，16MB为1/3/8；4MB和16MB合计2/8/14。最高增幅13.56%；S3首末漂移-36.89%至+30.93%，如4MB ranking的S3 237.028→149.596ms，不能只引用对首次S3的32.59%下降作为稳定收益。
**最终不接受scratch1或scratch2性能候选。** scratch2的分配减少真实存在，但本轮没有证明稳定的跨预算wholeSQL收益；这不表示它所有查询都慢，也不表示已确定第一版退化原因。原S3源码精确恢复，安装程序一直保持S3 `aa52b5fe…`；两候选、全部慢值/首末基线/原36项数据及提案均保留。不盲目追加候选或重跑以选择较好样本。

| work_mem | case | S3 first p50 ms | scratch2 p50 ms | S3 last p50 ms | scratch2 reduction vs first % | scratch2 reduction vs last % | S3 last drift % |
|---|---|---:|---:|---:|---:|---:|---:|
| 64kB | partition_ranking | 168.863 | 165.506 | 194.942 | 1.99 | 15.10 | 15.44 |
| 64kB | partition_lag | 127.816 | 120.470 | 121.470 | 5.75 | 0.82 | -4.97 |
| 64kB | partition_lead | 123.401 | 121.385 | 125.238 | 1.63 | 3.08 | 1.49 |
| 64kB | running_sum | 126.616 | 124.065 | 136.188 | 2.01 | 8.90 | 7.56 |
| 64kB | sliding_sum | 141.045 | 127.873 | 147.104 | 9.34 | 13.07 | 4.30 |
| 64kB | partition_full_sum | 114.080 | 118.556 | 130.882 | -3.92 | 9.42 | 14.73 |
| 64kB | full_first_last_nth | 203.337 | 178.214 | 187.997 | 12.36 | 5.20 | -7.54 |
| 64kB | mixed_same_keys | 171.972 | 179.893 | 192.292 | -4.61 | 6.45 | 11.82 |
| 64kB | length_lag_1 | 464.166 | 454.071 | 510.167 | 2.17 | 11.00 | 9.91 |
| 64kB | length_lag_2 | 495.212 | 471.201 | 482.554 | 4.85 | 2.35 | -2.56 |
| 64kB | length_lag_4 | 547.066 | 536.814 | 543.175 | 1.87 | 1.17 | -0.71 |
| 64kB | duplicate_running_sum | 138.571 | 138.437 | 181.427 | 0.10 | 23.70 | 30.93 |
| 4MB | partition_ranking | 237.028 | 159.783 | 149.596 | 32.59 | -6.81 | -36.89 |
| 4MB | partition_lag | 137.228 | 117.219 | 119.963 | 14.58 | 2.29 | -12.58 |
| 4MB | partition_lead | 118.466 | 121.378 | 119.795 | -2.46 | -1.32 | 1.12 |
| 4MB | running_sum | 157.600 | 131.259 | 118.569 | 16.71 | -10.70 | -24.77 |
| 4MB | sliding_sum | 126.283 | 135.038 | 118.914 | -6.93 | -13.56 | -5.84 |
| 4MB | partition_full_sum | 121.171 | 123.472 | 112.251 | -1.90 | -10.00 | -7.36 |
| 4MB | full_first_last_nth | 181.334 | 173.797 | 160.384 | 4.16 | -8.36 | -11.55 |
| 4MB | mixed_same_keys | 169.199 | 172.107 | 157.288 | -1.72 | -9.42 | -7.04 |
| 4MB | length_lag_1 | 461.927 | 495.500 | 450.207 | -7.27 | -10.06 | -2.54 |
| 4MB | length_lag_2 | 537.071 | 495.075 | 478.965 | 7.82 | -3.36 | -10.82 |
| 4MB | length_lag_4 | 537.250 | 533.582 | 527.479 | 0.68 | -1.16 | -1.82 |
| 4MB | duplicate_running_sum | 150.563 | 143.306 | 133.031 | 4.82 | -7.72 | -11.64 |
| 16MB | partition_ranking | 135.686 | 137.009 | 137.184 | -0.98 | 0.13 | 1.10 |
| 16MB | partition_lag | 110.749 | 116.205 | 121.303 | -4.93 | 4.20 | 9.53 |
| 16MB | partition_lead | 115.357 | 115.780 | 119.256 | -0.37 | 2.91 | 3.38 |
| 16MB | running_sum | 115.231 | 114.998 | 114.320 | 0.20 | -0.59 | -0.79 |
| 16MB | sliding_sum | 121.090 | 120.307 | 118.668 | 0.65 | -1.38 | -2.00 |
| 16MB | partition_full_sum | 105.654 | 106.061 | 109.378 | -0.39 | 3.03 | 3.53 |
| 16MB | full_first_last_nth | 152.534 | 148.212 | 151.688 | 2.83 | 2.29 | -0.55 |
| 16MB | mixed_same_keys | 144.463 | 147.821 | 147.837 | -2.32 | 0.01 | 2.34 |
| 16MB | length_lag_1 | 444.167 | 455.528 | 448.246 | -2.56 | -1.62 | 0.92 |
| 16MB | length_lag_2 | 474.490 | 474.529 | 479.227 | -0.01 | 0.98 | 1.00 |
| 16MB | length_lag_4 | 523.441 | 526.490 | 516.397 | -0.58 | -1.95 | -1.35 |
| 16MB | duplicate_running_sum | 128.771 | 129.585 | 127.052 | -0.63 | -1.99 | -1.34 |

完整108组raw12/min/p50/max与所有1296计时样本分别见 `perf-window-scratch2-latency-labels108-v1.csv`、`perf-window-scratch2-latency-all1296-v1.csv`；汇总、慢值及round4索引为 `perf-window-scratch2-latency-summary-v1.json`、`perf-window-scratch2-slow-values-v1.json`。拒绝/源码恢复凭据为 `scratch-rejection-and-source-restore-v1.json`。这两轮各自只跑一遍预先固定的完整计划，不把独立时窗拼成scratch2/scratch1因果倍率。

### PostgreSQL 类型输出：尚未接受的功能工作

[PG 18.6 record_send](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/rowtypes.c#L687)逐字段写真实声明OID，即使字段为NULL；[array_send](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/arrayfuncs.c#L1548)保留真实元素OID、维度、下界、由真实NULL位图决定的has-null头标志及逐NULL元素的-1长度，线上不发送位图片段；空数组也先检查元素发送能力。[pg_type.dat](https://github.com/postgres/postgres/blob/REL_18_6/src/include/catalog/pg_type.dat)的record[] OID2287为pseudo/P而非category A；NAME虽有typelem仍走scalar sender，ACLITEM没有二进制输出。不能用category/typelem非零或本项目当前typsend=0单一规则判断所有编码能力。
[SQLite列API](https://sqlite.org/c3ref/column_blob.html)区分原始类型与按请求转换后的表示，并限制返回值生命周期；[DuckDB 2.0 LTS DataChunk](https://duckdb.org/docs/2.0/clients/c/data_chunk)把类型化向量和NULL有效性作为数据接口，reset后须重新获取指针。这里据此选择保留真实拥有的结构化值与声明元数据，不从文本重建record/array，不借用已失效行值；这仍是本项目的设计选择，不等于移植了这些数据库的执行器。

新增隔离固定301观察包括73查询形状×Simple/text/binary、两个混合format、51builtin和10user目录观察及DDL/来源锚/副作用postcheck。独立实际PG18.6的301协议结构/预期错误阶段/完整字节/来源锚验证全过，冻结参考未因候选而修改；基线S3、scratch1和scratch2各有201处差异，三版全部301实际响应逐项相同。差异包含42codec、36物理REGTYPE创建失败级联、61目录元数据、7描述/行元数据、12枚举数组文本误标、6domain数组缺失、3domain-over-array基底、6append/prepend重解析、12named目录、6匿名输入阶段及10其他；不是201个独立根因，不能把未实现项改判为预期通过。

最早误连现有PG17.2端点，版本guard在SHOW后终止，退出1；其本任务UUID库在finally删除成功，原日志保留，管理请求另计。随后用本任务拥有的新建原生18.6 cluster重新采集完整301，正常退出并核对PID；没有把17.2数据标成18.6，也未操作其他数据库对象。

类型载体/metadata factory/receiver/routine/codec/array/spill提案仍在隔离目录。统一完整头树下ExprEvaluator、TypedSortSpill和PostgresProtocol的syntax-only通过，不代表新ABI链接或SQL运行通过。首个混用新旧头的语法检查63个AST重定义错误原样保留，统一树复核后0error；没有删除错误或冒称生产编译通过。每个ExprValue新增shared_ptr的全路径成本、真实值producer/物理REGTYPE/domain/named类型及完整协议/恢复/性能仍待验证。
R151明确暂停的顶层enum/enum[]协议描述符与原103门禁、R177/R185/R65、retained planning/frame/portal/format2等范围均不因通用metadata提案而恢复；generic提案尚无全链旧描述符字节连续性证明，未应用到生产。原Bind/CTE/nested/interval/integer-key五失败入口继续独立保留，不能把所有未解决问题一概算作已暂缓。

### 本批最终恢复与文档检查

拒绝候选后正常执行CMake构建，实际重编Exec并链接，退出0、59.60秒（构建时间，非查询性能）。216生产源与基线全文相同，构建程序和安装程序均精确为S3 SHA256 `aa52b5fe35e1d1ef92a301bcad8b72d95b32edb705e650d3539b999d8c5b6b0f`，见 `restored-S3-cmake-receipt-v1.json`；这不是新全库fresh build，也没有把旧S3回归写成恢复后的新测试。
九份既有文档同步，结构检查覆盖754 Markdown及2435个本地文件链接，缺失/空文件/全文重复/清单遗漏均0；一般外部URL和历史fragment不在该检查范围。原273条目和250历史原文保留，新增近期记录仅partial；原2319测试/147头/725保护记录不改，没有新增Markdown或永久测试，Goal active。

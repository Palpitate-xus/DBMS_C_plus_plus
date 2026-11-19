# DBMS 功能测试报告

> 历史基准原始数据不随 Git 仓库提供。本文中的 `benchmarks/` 路径仅指可能存在的本地可选证据；新克隆仓库没有这些文件，原始结果只能在另行取得数据后复核。文中的测试与性能摘要仍保留。

<a id="same-row-writes-20261119"></a>
## 2026-11-19 · 两个连接先后修改同一行

测试让第一个连接先写入并继续持有行锁。第二个连接发出写入语句，测试在看到实际的锁等待后，才让第一个连接提交或回滚。PostgreSQL 18.6 使用 `pg_blocking_pids()` 确认阻塞来源，项目测试从 `pg_locks` 检查等待和持锁记录。[协议测试](../tests/dml_lock_error_savepoint_protocol_e2e_test.py)

PostgreSQL 18.6 在默认隔离级别下使用最新记录重查 `WHERE`。提交 `v=20` 后，等待的 `SET v=v+1` 返回 `21`。主键和其他列不再满足条件时跳过该行，删除返回当前版本。先前写好两次再一起提交时，后来的语句可以继续找到最后一个版本。原写入回滚时沿用原值；在 `REPEATABLE READ` 或 `SERIALIZABLE` 下遇到已提交的并发写入时返回 `40001`。[PostgreSQL 隔离级别](https://www.postgresql.org/docs/18/transaction-iso.html) · [PostgreSQL 18.6 的修改执行器](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/executor/nodeModifyTable.c)

旧版 `2e81f67c` 在 11 种同一行的写入顺序中有 10 种返回 `58030`，仅持锁者整笔回滚的用例按预期返回。持锁者改主键或删除的用例中，旧版还会重新显示已经失效的旧记录。旧版先找行、后等待锁，等待结束后仍用原来找到的位置写入，和已经移动的索引条目冲突。

新版在拿到行锁后读取记录头，使用提交日志判断前一个写入是否提交。每次查找新版本都核对它是否由前一个版本记录的事务创建，并对新版本加锁。保留最初选中的候选范围，只对被改过的行重查谓词。没有并发修改的行无需额外查提交日志。

11 个注册用例全部通过。验证了有无显式事务、主键改变后仍满足或不再满足条件、普通列条件变化、删除当前版本、先前删除、隔离级别冲突、回滚和连续更新两次。核对命令标签、返回值和事务状态，并用逐行查询和逐主键查询比对最终值。实际向服务端发送 `SIGKILL` 并用原目录重新启动后，两种查询仍一致。

另外核对了 9 个场景，包括 `SERIALIZABLE` 下的更新、删除和不同记录的写入冲突，持锁者撤消第二次更新，结束显式事务后的默认隔离，`WITH UPDATE` 以及实际扩展协议的参数执行。`UPDATE FROM` 和 `DELETE USING` 仍与 PostgreSQL 有差异。PostgreSQL 成功修改最新版本，受测项目版本返回 `40001` 并保留已经提交的值。

精确候选在 14 套协议测试和 11 套原生测试中通过。原生测试还覆盖日志写入失败、外键动作、保存点、准备提交的事务，以及主键与次级索引保持同步。受测程序 SHA-256 为 `d3147c82d32d6034d6becfaa0d9a1ae0708c21c45ec3ea62064af270ff2845e6`。注册协议、补充协议和原生结果清单的 SHA-256 依次为 `fd8b05f38f29f72e711d6844eefb300a08f5c382794a06f4d7d6a5ebb7e0f201`、`df73b4028faacfbe31c045a880db41cc3101a6ab86fb53955efde5c0e1224ff9`、`a59020822f321bdee99132152b3ea4aa55487677fcce2dd3ab214149417ccbbf`。[性能报告](performance-report-2026-10-30.md#same-row-write-cost-20261119)

在仓库根目录可以用构建好的程序重跑注册用例。

```bash
DBMS_MAIN="$PWD/build/cmake/dbms_main" python3 tests/dml_lock_error_savepoint_protocol_e2e_test.py
```

对照端使用同一测试加 `--reference`，通过 `PGREF_HOST`、`PGREF_PORT`、`PGREF_USER`、`PGREF_DATABASE` 和 `PGREF_PASSWORD` 指定 PostgreSQL 18.6 测试库。完整仓库回归和完整的 PostgreSQL 行锁行为待验证。

<a id="integer-primary-key-mutation-20261119"></a>
## 2026-11-19 · 按整数主键更新和删除

旧版在首次写入时就把事务标记为写入状态，常见主键等值更新和删除也因此读取整张表。在隔离测试中，命中的行位于第 1 页，旧版仍等待与目标无关的第 4 页解锁。新版保持原有事务写入标记，在确认当前快照可使用现行索引时，只把经过核对的索引命中交给原有写入流程。[整数主键协议测试](../tests/integer_index_write_protocol_e2e_test.py)

在真实 PostgreSQL 18.6 和项目新旧版上核对了同一组 50 条 SQL。覆盖三种带符号整数的边界值、命中与未命中、空值返回、主键和次级索引键修改、已有写入、保存点回滚以及旧快照。核对了命令标签、结果值、列类型、错误码和事务状态。对照按列序及类型比较，数据库各自分配的表编号单独记录。

新旧版各完成 156 条预检 SQL，每类操作后的堆文件、主键与次级索引文件逐字节一致。冷重启后的次级索引查询实际读取一行。六项原生回归通过，包括写入日志故障、回滚和重试。[性能报告](performance-report-2026-10-30.md#integer-primary-key-mutation-cost-20261119)

六项协议回归也通过。注册测试把目标行放在第 1 页，为新版记录预留 261 字节。新增记录需要 41 字节。锁住后面的满页时，正负主键更新及主键删除能够返回结果。未命中、旧快照、已写入的事务、范围、普通列和复合主键用例仍等待该页。回滚保留原主键，强制结束服务进程并重新打开原目录后，提交的更新和删除仍正确。[协议测试](../tests/integer_index_write_protocol_e2e_test.py)

本次主键优化验证时，同一行的并发写入仍返回 `58030`，随后观察连接在读回结果前关闭。后续版本已修复受测的普通更新和删除。[并发写入复测](#same-row-writes-20261119)

受测程序 SHA-256 为 `7d2b611d11457b4f53d60253668704eedadd6551b26f136ca941a30afc6f3e97`。完整回归套件待验证。

<a id="partition-analyze-vacuum-parallel-20261119"></a>
## 2026-11-19 · 分区表统计进度与并行清理选项

在真实 PostgreSQL 18.6 服务上，父表有两个含数据的分区和一个空分区。454 组协议交互确认父表编号在采样期间保持不变，当前分区编号和采样页数随扫描切换。`ANALYZE ONLY` 更新父表继承统计，普通 `ANALYZE` 还更新各分区的统计。另有 50 组协议交互核对清理参数，`VACUUM FULL` 拒绝正数 `PARALLEL`，允许 `PARALLEL 0`。有效的重复选项以最后一个为准。[PostgreSQL 采样实现](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/commands/analyze.c) · [PostgreSQL 清理选项实现](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/commands/vacuum.c)

项目旧版接受 `VACUUM (FULL, PARALLEL 1)`，在所测保留三行的表上将 24,576 字节的堆文件重写为 16,384 字节。新版拒绝时保留测试中的堆和长值文件。先指定正数再指定零时，长值文件仍可从 188,416 缩至 57,344 字节，主表保持 16,384 字节且逐字节一致，冷重启后读取的值全部一致。[清理参数协议测试](../tests/vacuum_relation_options_protocol_e2e_test.py)

分区原生用例用真实页锁控制扫描顺序。第一分区只有一页，第二分区为空，第三分区有多页。旧版在第一页锁处显示普通采样以及零个子表。新版依次报告已完成 0 个和 2 个分区，并从第三分区的第一页开始计数。取消操作以 `57014` 退出，清除进度行。完整读取仍得到 31 行。[分区原生测试](../tests/partition_test.cpp)

在单独的真实客户端对照中，两个版本读取同一个现有格式的三分区表。新版在第一个分区报告已完成 0/3 个分区、已读 1/20 页，到第三个分区时报告已完成 2/3 个分区、已读 0/59 页。旧版第二次观察仍显示普通采样、20/79 页和零个分区。两版成功或取消后都清除了进度行，仍可读出 3,200 行。这个旧格式样本的父表没有目录编号，`relid` 为 SQL `NULL`，当前分区编号为 `0`。

共通过六项原生回归和八项协议回归，包括重写后的 33 个冷恢复阶段。受测程序 SHA-256 为 `125a9ad6a69c821ce2807d01ccae19a24705c9a5dbfcffb94c6292d1e45b5fb9`。完整回归套件待验证。[性能报告](performance-report-2026-10-30.md#partition-analyze-observation-cost-20261119)

<a id="vacuum-full-placement-20261119"></a>
## 2026-11-19 · 重写表时减少取页

`VACUUM FULL` 用内存中的空位记录选择第一个能容纳该行的目标页。每个分区有自己的空位记录。写入前先设置行的新物理位置，然后只写入一次。[使用手册](MANUAL.md#分开清理主表和长值)

PostgreSQL 18.6 把堆重写的记录依次放进当前页，空间不足时批量写出该页。SQLite 将数据复制到临时数据库后替换原文件，DuckDB 通过检查点整理符合条件的已删除数据。[PostgreSQL 堆重写](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/access/heap/rewriteheap.c) · [SQLite VACUUM](https://www.sqlite.org/lang_vacuum.html) · [DuckDB 空间回收](https://duckdb.org/docs/current/operations_manual/footprint_of_duckdb/reclaiming_space)

- 原生测试保留了 14 行不同长度的记录，最后一行能放进两处空位。新旧版都把它从第 14 页移到第 1 页，保留同一槽位顺序。直接拦截取页接口测得，写入这 14 行时，新版访问 14 次目标页，旧版访问 92 次。两版的页校验和、行物理位置和重建后的主键均通过检查。[取页回归](../tests/vacuum_full_test.cpp)
- 模拟目标堆的第三次取页失败后，原始主表和主键文件逐字节恢复，可以继续读取。另有 33 个原生冷恢复阶段通过，包含长值存储、BRIN 和新旧列统计。[冷恢复回归](../tests/vacuum_full_cold_recovery_test.cpp)
- 独立检查从三个样本的原始堆页推导出 9,335 行应写入的页和槽。两版的实际结果都与计算一致，13 份重写后的堆、索引和长值文件逐字节相同。两版独立冷启动后，三个样本都实际使用重建的次级索引，返回正确的值。

共通过八项原生回归和五项协议回归。受测程序 SHA-256 为 `0ea3d92b534db10cfa2694aab115ae961d451142978255d5f23ad1c0ff6012dd`。完整回归套件待验证。[性能报告](performance-report-2026-10-30.md#vacuum-full-placement-cost-20261119)

<a id="maintenance-progress-views-20261119"></a>
## 2026-11-19 · 实时查看表维护进度

PostgreSQL 18.6 的参考服务完成了 471 组协议交互，独立复核比对列名、类型、计数和权限。普通清理实际进入扫表和清理索引阶段。分析采样读到 600 页。一次 FULL 扫描并写入了 4,500 行，只在重写进度视图中出现。在参考库中，无权查看详情的角色仍能看到进程号和数据库身份。三个命令在等待表锁时以及结束后没有进度行。[PostgreSQL 18.6 视图定义](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/catalog/system_views.sql) · [权限实现](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/pgstatfuncs.c#L235-L301)

项目协议测试用实际页锁暂停三个命令。每次暂停时保留四个数据页，已扫描一页。并行清理可报告其他工作线程完成的三页。另一连接通过简单和扩展协议读取 FULL 进度，校验了真实进程号、表编号、列类型及角色可见性。取消、锁超时、断开和正常完成后，进度行均清除。同名的查询内临时结果保持原有解析。三个手册查询也都通过。[协议回归](../tests/maintenance_progress_protocol_e2e_test.py)

正式测量前还观察了三个无人工暂停的维护命令。所见进度行的表编号与目录一致，命令完成后视图为空。受测程序 SHA-256 为 `5d7b08aa17927b85915b12034b1a7c025aa5823d9e979367b8938d997dbb276f`。[性能报告](performance-report-2026-10-30.md#maintenance-progress-views-cost-20261119)

七项协议回归及三项原生回归通过。原生回归覆盖 33 个冷恢复阶段、并行清理和页锁。较广的协议脚本在对照版和第一版候选中都因既有 `search_path` 用例失败。它预期错误码 `22023`，两版实际都返回 `42601`。完整回归套件待验证。

SQLite 通过连接级回调报告虚拟机指令进度。DuckDB 的连接级接口提供处理行数和百分比。[SQLite 进度回调](https://www.sqlite.org/c3ref/progress_handler.html) · [DuckDB 查询进度](https://duckdb.org/docs/current/clients/c/connect#duckdb_query_progress)

<a id="vacuum-target-columns-20261119"></a>
## 2026-11-19 · 只更新指定列的统计

分析阶段按表绑定列名，再读取并汇总指定列的数据。未选列继续使用各自上次分析的行数。表重写提交前会为既有统计记录新旧两套表文件的身份和表定义摘要。重启恢复决定哪组表文件生效，两种身份都使用同一份统计值。[使用示例](MANUAL.md#只更新指定列的统计)

- PostgreSQL 18.6 完成两组共 111 次协议交互。已核对指定列、多表、带引号的名字、重复或缺失列、保留其他列统计和空表。参考库中，指定列更新后，所测等值查询的行数估算从 1 变为 540，实际返回 540 行。后续列名无效时，已完成的前一张表分析或当前表重写会保留。[PostgreSQL 分析实现](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/commands/analyze.c)
- 项目协议测试用两次重写和两次冷启动核对指定列、其余列和实际查询结果。无效列的 FULL 样本实际从 57,344 缩至 16,384 字节，旧统计仍可读。另测显式事务回滚以及表行数从 80 增至 160 后的旧统计回退，未选列仍使用原先 80 行的统计。[协议回归](../tests/legacy_forced_analyze_protocol_e2e_test.py)
- 原生恢复测试在统计文件目录同步后、事务提交前终止进程，恢复旧表与索引。另一场景在事务提交、清理恢复记录之前终止进程，保留新表与索引。各两次冷启动都读回原样的统计文件内容和正确的剩余行。读取测试同时覆盖两个统计文件版本。[恢复回归](../tests/vacuum_full_cold_recovery_test.cpp) · [统计文件测试](../tests/analyzed_statistics_snapshot_test.cpp)

七项协议回归、572 项统计文件检查、33 个原生恢复阶段和 13 条手册命令通过。持有同一数据库的独占事务时，原生分析返回未完成，统计文件保持原样。受测程序 SHA-256 为 `992ef052e5031af62ec2d59560dbb60003f719d3f25e2c6520570eb6f8e08de7`。完整回归套件待验证。[性能报告](performance-report-2026-10-30.md#vacuum-target-columns-cost-20261119)

SQLite 的 `ANALYZE` 可以指定库、表或索引。DuckDB 的 `ANALYZE` 重算供连接顺序使用的表统计。[SQLite ANALYZE](https://www.sqlite.org/lang_analyze.html) · [DuckDB ANALYZE](https://duckdb.org/docs/current/sql/statements/analyze)

<a id="vacuum-combined-full-20261119"></a>
## 2026-11-19 · 默认重写同时整理主表和长值

新版在重写主表时记录保留行引用的长值编号，随后只把仍在使用的值复制到新的数据文件。主表、长值文件、索引和配套的加密文件共享一次提交与恢复记录。[使用示例](MANUAL.md#分开清理主表和长值)

- 协议测试用默认 `VACUUM FULL` 将主表从 57,344 缩至 16,384 字节，长值文件从 188,416 缩至 57,344 字节。收到完成消息后立刻终止服务，重启后逐字检查保留值和主键查找。只重写长值的命令也继续通过。[协议测试](../tests/vacuum_relation_options_protocol_e2e_test.py)
- 原生恢复测试通过全部 27 个子进程阶段。两项新增场景都在新主表、新 BRIN 索引和新长值数据文件就位后触发。一项让长值索引替换返回 `EIO`，另一项在同一处用 `SIGKILL` 结束进程。每项经过两次独立冷启动，均恢复原始文件、加密配套文件、索引和行内容。普通长值清理的物理引用测试通过 16 组场景、182 项检查。[恢复测试](../tests/vacuum_full_cold_recovery_test.cpp) · [物理引用测试](../tests/vacuum_toast_physical_reference_test.cpp)
- PostgreSQL 18.6 参考服务完成 217 组协议交互。项目新版与参考服务在八项共同检查上取得相同的结果，核对了保留行、按键查找和已删除的键。新版冷启动后实际使用重建的次级索引读回长文本。两边均观察到主表和长值文件缩小，物理尺寸各自记录。SQLite 通过重建整个数据库文件腾出空间，DuckDB 通过检查点回收符合条件的已删除空间。[PostgreSQL 重写实现](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/commands/cluster.c) · [SQLite VACUUM](https://www.sqlite.org/lang_vacuum.html) · [DuckDB 空间回收](https://duckdb.org/docs/current/operations_manual/footprint_of_duckdb/reclaiming_space) · [性能报告](performance-report-2026-10-30.md#vacuum-combined-full-cost-20261119)

合计通过八项原生回归、四项协议回归和十一条手册命令。相邻测试覆盖主表重写、旧快照、并行清理、锁、统计更新和 BRIN 查询。受测程序 SHA-256 为 `cc9dcba1f6482a436a2c5d75595e2311352771c6dc794ca1d310767413e72d8a`。完整回归套件和跨越该重写的时间点恢复待验证。

<a id="vacuum-toast-full-20261118"></a>
## 2026-11-18 · 只重写长值文件

`VACUUM (FULL, PROCESS_MAIN FALSE)` 在排他维护期间检查主表引用。确认记录在当前视图中不可见且事务状态已经确定后，才跳过相应的引用。仍被使用的长值按原编号复制，新索引指向复制后的数据位置。重写只替换长值堆、索引和各自的配套文件，一次提交决定整组保留或恢复。[使用示例](MANUAL.md#分开清理主表和长值)

PostgreSQL 18.6 对照样本有 160 个 6,144 字节的长值。删除其中 156 个后，独立长值重写将数据文件从 1,310,720 字节缩至 32,768 字节，索引从 32,768 字节缩至 16,384 字节。主表和主键文件逐字节一致。相同样本上的普通长值清理保留数据文件长度。SQLite 的 `VACUUM` 重建整个数据库文件，DuckDB 通过检查点整理符合条件的已删除数据。[PostgreSQL 维护实现](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/commands/vacuum.c) · [SQLite VACUUM](https://www.sqlite.org/lang_vacuum.html) · [DuckDB 空间回收](https://duckdb.org/docs/current/operations_manual/footprint_of_duckdb/reclaiming_space)

- 本项目在同样删除 156 行后，主表仍有 160 个物理行槽。只重写长值使数据文件从 663,552 缩至 24,576 字节，索引从 28,672 缩至 8,192 字节。主表和主键文件逐字节一致，4 个保留长值和 2 个新插入长值均原样读回。8 条共同操作与 PostgreSQL 18.6 的结果和错误码一致。总共 102 条语法与协议对照中，100 条一致，差异来自带列清单的两条 `VACUUM`。[性能报告](performance-report-2026-10-30.md#vacuum-toast-full-cost-20261118)
- 协议回归另测实际删除后的长值空间、没有长值文件的定长表和只有短文本的表。收到命令完成消息后立即用 `SIGKILL` 结束进程，首次冷启动读回相同值，并能写入新的长值。[协议回归](../tests/vacuum_relation_options_protocol_e2e_test.py)
- 原生故障回归在新数据文件已就位时模拟索引替换返回 `EIO`，并分别在数据文件替换后、索引文件替换后用 `SIGKILL` 结束进程。六次重新打开恢复了原始数据、索引及对应的加密文件，主表、长值编号和内容保持一致。共 21 个子进程阶段通过，另有五项原生回归、三项相邻协议回归和十条手册命令通过。完整回归套件待验证。受测程序 SHA-256 为 `b708ea44cf49d455c6bf5aad8698f3f96912a17890612c2c1c0736ff561b000c`。[恢复回归](../tests/vacuum_full_cold_recovery_test.cpp)

<a id="vacuum-process-options-20261118"></a>
## 2026-11-18 · 分开清理主表和长值

普通 `VACUUM` 默认清理两种文件。新的选项可关闭任一阶段，`ANALYZE` 的统计收集仍独立运行。只清理长值时，同库有活跃事务会延后回收。实际检查文件时，发现一侧文件缺失会返回 `58030`。原本没有长值文件的定长表仍可正常处理。[使用示例](MANUAL.md#分开清理主表和长值)

PostgreSQL 18.6 的两组参考测试核对了 204 组协议消息。主表和长值附表的维护计数可独立变化。特别是 `FULL, PROCESS_MAIN FALSE` 只重写长值附表，`FULL, PROCESS_TOAST FALSE` 报 `0A000`。SQLite 的 `VACUUM` 重建数据库文件，DuckDB 通过检查点回收一部分已删除数据占用的空间。[PostgreSQL 清理实现](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/commands/vacuum.c) · [SQLite VACUUM](https://www.sqlite.org/lang_vacuum.html) · [DuckDB 空间回收](https://duckdb.org/docs/current/operations_manual/footprint_of_duckdb/reclaiming_space)

- 真实协议回归在同一张表同时准备了主表空闲空间和孤立长值。分别执行主表清理、长值清理和两者一起清理，检查主表可用空间、长值数据块和索引文件。单独清理时，跳过的文件保持逐字节一致。两个阶段都关闭只更新统计时，主表和长值文件保持一致。[协议与磁盘回归](../tests/vacuum_relation_options_protocol_e2e_test.py)
- 已经读取旧长值的可重复读事务继续读到原值。另测不含长值文件的定长表，以及单独缺失长值索引的表。重新打开同一数据目录后，保留的长值和清理后的文件与关闭前一致。
- 102 次命令与 PostgreSQL 18.6 比对消息类型、命令标签、事务状态和 SQLSTATE，共 89 次一致。其余 13 次涉及长值单独重写和带列清单的 `VACUUM`。六项 C++ 回归、三项协议回归及九条手册命令均通过，其中包含现有的 `ANALYZE = true` 写法。完整回归套件待验证。受测程序 SHA-256 为 `7e987e0b59234aae202aaa0065896ad8f2028530df8e1fcc037ed036f9efd3b2`。[性能报告](performance-report-2026-10-30.md#vacuum-process-options-cost-20261118)

<a id="vacuum-empty-toast-20261118"></a>
## 2026-11-18 · 清理表时跳过无用的扫描

普通 `VACUUM` 会先清理主表，然后检查单独存放的长值。新版本在第二步先确认长值文件是否有数据页。两份文件齐全且长值文件只有文件头时，直接核对索引，无需再读取主表。存在长值时，仍会检查主表和分区的所有物理行，确认哪些值可回收。[使用示例](MANUAL.md#清理表占用的空间)

PostgreSQL 18.6 根据表目录中的长值附表标识决定是否另行清理。SQLite 把放不下的数据连到溢出页。DuckDB 的 `VACUUM` 主要更新部分统计信息，删除后的空间回收由检查点处理。[PostgreSQL 清理实现](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/commands/vacuum.c) · [SQLite 溢出页](https://sqlite.org/fileformat2.html#cell_payload_overflow_pages) · [DuckDB VACUUM](https://duckdb.org/docs/stable/sql/statements/vacuum) · [DuckDB CHECKPOINT](https://duckdb.org/docs/stable/sql/statements/checkpoint)

- 回归捕获实际主表页读取。针对不存在长值文件的定长表，以及长值文件只有文件头的短文本表，两种情况都由读取 1 页降至 0 页。整数、短文本、空字符串和 SQL `NULL` 原样读回。旧版只有这两项取页断言失败。[物理读取回归](../tests/vacuum_toast_physical_reference_test.cpp)

- 真正写有长值的 16 个既有用例继续通过。普通表和分区表都只回收失去引用的值，保留仍被引用的字节。历史快照、并行清理和页锁的相邻回归包含在本次验证中。[快照回归](../tests/vacuum_toast_test.cpp) · [并行清理回归](../tests/parallel_vacuum_test.cpp)

- 六项 C++ 回归全部通过。新旧版本执行手册示例并清理实际存有长值的表。在 `VACUUM` 返回后各用 `SIGKILL` 结束进程，重新打开同一数据目录，读取并再次清理。两版 16 组命令和回复逐字节一致。初次和重启后的结果都保留有效行和 `NULL`，已删行未出现。另有 65 组协议操作与 PostgreSQL 18.6 对照。完整回归套件待验证。受测程序 SHA-256 为 `1dcf8e2bad29c4fa7d5f69ffc4fd45d58f1aa7db82327114148833e5a4356fb7`。[性能报告](performance-report-2026-10-30.md#vacuum-empty-toast-cost-20261118)

<a id="brin-vacuum-full-generation-20261118"></a>
## 2026-11-18 · 整理表后可立即使用范围索引

`VACUUM FULL` 会重写表。旧版先重建 BRIN，再保存新表标识，导致查询发现索引与表不匹配。新版保存新表标识后重建 BRIN，提交前继续持有表锁和回滚所需文件。[使用示例](MANUAL.md#重写表后的-brin-查询)

PostgreSQL 18.6 更换表文件后重建索引。SQLite 的 `VACUUM` 重建整个数据库文件。DuckDB 使用数据块的最小值和最大值跳过不符合条件的区块，与 BRIN 的范围过滤原理类似。[PostgreSQL 表重写](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/commands/cluster.c) · [SQLite VACUUM](https://www.sqlite.org/lang_vacuum.html) · [DuckDB 范围过滤](https://duckdb.org/docs/stable/guides/performance/indexing)

- BRIN 维护测试执行 42 条 SQL，检查 12 份真实索引文件和 4 份执行计划。38 条非 `EXPLAIN` 回复与旧版逐字节一致。两份重写后的索引文件改为记录新表标识和表定义，其余十份逐字节一致。首次重写后的范围查询依然返回 8 行，扫描行数从 767 降至 24。另一张表连续重写两次，每次查询从扫描 319 行降至 22 行，返回的 6 行相同。[BRIN 维护测试](../tests/brin_maintenance_options_protocol_e2e_test.py)
- 故障测试在新表定义已写入后让索引文件替换返回 `EIO`，并另测索引文件替换成功后、提交前用 `SIGKILL` 结束进程。两种情形都恢复旧表定义、旧索引的原始字节及原有查询结果。分别用两个新进程再次打开数据，恢复后的索引都可用。旧版通过原有冷启动用例，但无法到达新增测试要求的发布顺序。[冷启动恢复测试](../tests/vacuum_full_cold_recovery_test.cpp)
- 六项 C++ 回归、四项相邻协议测试及手册示例均通过。另有 42 条共同操作与 PostgreSQL 18.6 对照，并对 27 条性能测试准备和读取操作再次核对结果。完整回归套件待验证。受测程序 SHA-256 为 `e7593c3f28d68c3236d03390ff3dc4b5f656ffaca4d0d96b2262e52fe49e0e9e`。[性能报告](performance-report-2026-10-30.md#brin-vacuum-full-generation-cost-20261118)

<a id="specialized-index-discovery-20261118"></a>
## 2026-11-18 · 写入时减少重复的索引查找

PostgreSQL 用关系缓存复用从系统目录取得的索引清单。SQLite 从表定义中的索引链表生成打开索引的指令。DuckDB 的文档将维护 ART 索引列为写入额外成本。本项目的这五类索引仍通过磁盘文件查找，因此把重复遍历合并成一次。[PostgreSQL 关系缓存](https://raw.githubusercontent.com/postgres/postgres/REL_18_6/src/backend/utils/cache/relcache.c) · [SQLite 插入实现](https://raw.githubusercontent.com/sqlite/sqlite/master/src/insert.c) · [DuckDB 索引维护](https://duckdb.org/docs/stable/guides/performance/indexing)

全文、GIN、GiST、SP-GiST 和 BRIN 共用一次目录遍历。插入、更新和删除前仍根据索引文件和已有失效标记决定是否发布失效状态。重建按原先的类型顺序处理。已有索引定义、表空间位置和文件格式继续使用。加列、删列、截断表和 `VACUUM FULL` 在改写前也共用同一份文件清单。目录读取错误沿原路径返回。[使用示例](MANUAL.md#写入后的索引维护)

- 新增原生回归在基线和新版上的完整输出逐字节一致。覆盖同目录中其他表的文件、大小写不同的扩展名、备份文件、目录和空列名。无法取得目录项状态时，写入在添加表行前失败。全部五类实际索引文件随提交更新，回滚保留原值。[索引写入回归](../tests/specialized_index_dml_test.cpp)
- 新旧版本与 PostgreSQL 18.6 的维护和恢复对照共核对 122 组请求与回复，其中 40 组共同操作同时通过。项目版检查了五类索引的实际文件和失效标记。强制结束进程后，新进程恢复出已提交的值。加列、删列、`VACUUM FULL` 和截断后重新插入也通过所测文件和行检查。
- 七项 C++ 回归、四项相邻协议测试及手册示例通过。另有一项 BRIN 测试在精确基线和新版均退出 1。两版都在 `VACUUM FULL` 后的第一次窄范围查询使用普通扫描，后续三个用例使用范围摘要。两版 38 组非 `EXPLAIN` 回复及 12 份实际 BRIN 文件逐字节一致，四份计划去掉计时字段后相同。完整回归套件待验证。最终程序 SHA-256 为 `55f726cc3b1b595630d0656e24e8a3171f477d837587b02a90b5ce3f29dd8476`。[BRIN 维护检查](../tests/brin_maintenance_options_protocol_e2e_test.py)

<a id="presorted-integer-primary-key-page-20261118"></a>
## 2026-11-18 · 保留整数主键页的已有顺序

PostgreSQL 和 SQLite 可以用符合 `ORDER BY` 的有序索引省去独立排序。DuckDB 用 `TopN` 减少带 `LIMIT` 的排序工作。本项目在实际取得已经有序的整数主键页后，复用该顺序。[PostgreSQL 索引排序](https://www.postgresql.org/docs/18/indexes-ordering.html) · [SQLite 按索引排序](https://www.sqlite.org/queryplanner.html#sorting_by_index) · [DuckDB TopN](https://duckdb.org/2024/11/14/optimizers#topn-optimizer)

排序节点在首次取行后检查数据源实际打开了哪条路径，并核对绑定的表、列和方向。普通条件筛选保留相对顺序。经过校验的旧索引数字前缀也可交付有序结果。表达式排序、旧快照或索引不可用而回退到表扫描时，按原方式排序。`TypedSort` 仍负责逐行解码和求值，复用顺序时报告 `presortedInput` 和 `spilled`。[使用示例](MANUAL.md#查看分页是否复用索引顺序)

达到排序内存预算后，已排序的批次按同一顺序写入一个临时文件。读取沿用已有的行编码和完整性检查，只读下游需要的行。原有的 `OFFSET` 求值顺序和错误位置保留。[性能报告](performance-report-2026-10-30.md#presorted-integer-primary-key-page-cost-20261118)

- 新增协议回归覆盖升序、降序、别名、序号，以及大于 `2^53` 的相邻主键。验证序列会为 `OFFSET` 跳过的结果行求值，依赖表行的排序转换错误仍返回 `22P02`。24 组功能回复与项目基线逐字节一致，其中 23 组规范化关系编号后与 PostgreSQL 18.6 一致。余下一组的错误代码相同，字段描述帧仍有既有差异。[协议回归](../tests/ordinary_scalar_order_protocol_e2e_test.py)
- 重新绑定同一条查询覆盖成功取页、空页、可串行化事务改用表扫描、再次成功取页及空参数。一个实际的关联 `ANY` 子查询复用同一计划，在三次重新绑定中按主键顺序交付六行。另核对全新与旧库的 325 组请求和回复及 64 次执行路径。`0xC552` 旧库打开后保持索引字节，成功读取的无条件分页仍可沿用其数值顺序。旧快照与正在写入的会话照常排序。实际强制结束进程后，已提交行可继续查询，需要重建时生成 `0xC553`。
- 实际跟踪临时文件创建、读写和退出时的清理。五项 C++ 回归、六项相邻协议测试和手册示例通过。排序组件的 20 项检查中，新增三项验证有序页不触发比较、超宽数据使用一个临时文件、损坏的临时文件头报 `58030` 后清理。完整回归套件待验证。最终程序的 SHA-256 为 `f0cd56c2b5278c4b44c88370d74d9e06855f4e536c551d77b5ac3a269f7d4c48`。[排序组件回归](../tests/bounded_sort_runs_test.cpp)

<a id="integer-primary-key-null-filter-20261118"></a>
## 2026-11-18 · 翻页时筛选空值

PostgreSQL 的有序索引扫描能用索引限制范围，再逐行筛选，取够 `LIMIT` 所需记录就停止。SQLite 也能沿索引的排序顺序读取，同时应用查询条件。DuckDB 用筛选下推与 `TopN` 减少进入排序的记录。[PostgreSQL 执行计划](https://www.postgresql.org/docs/18/using-explain.html) · [SQLite 查询计划](https://www.sqlite.org/queryplanner.html) · [DuckDB 优化器](https://duckdb.org/2024/11/14/optimizers)

项目沿有序的单列整数主键读取。可以独立使用 `IS NULL` 或 `IS NOT NULL`，也可以用 `AND` 与同一主键的范围条件或其他直接判空条件组合。目标和同表的其他列限于普通有符号整数、布尔或文本类型。NULL、空字符串、布尔假分别处理。`OFFSET` 从通过筛选的记录中跳过指定数量。原有的筛选、排序和输出计算仍会处理选出的记录。[手册示例](MANUAL.md#翻页时跳过空值)

读取过程中使用同一事务快照。检查过的表记录按存储的 NULL 标记筛选，索引分批继续读取，直到取够或确认范围结束。每批与上一批重叠一个索引键，用于核对跨批次的叶页链接。`heapRowsFetched` 含被筛掉的全部表行，`indexNullFilter` 表示实际使用该路径。旧格式索引、旧快照、超出读取预算、数组和域等复杂列、生成列以及带函数或计算的筛选沿用原查询计划。沿用现有磁盘格式。[性能报告](performance-report-2026-10-30.md#integer-primary-key-null-filter-cost-20261118)

- 六组新增分页分别读取 22、4、25、4、5 和 80 行，最后一组超过首批索引读取量。覆盖空字符串、布尔假、符合条件的记录不足一页和 `OFFSET` 后无结果。复杂条件、生成列、域与数组继续走普通路径。25 组功能请求与回复和 `26bd4e47` 逐字节一致，其中 24 组的行与字段描述经关系编号映射后也与 PostgreSQL 18.6 一致。余下一组两者都返回 `22P02`，PostgreSQL 在错误前会先发送字段描述，本项目仍沿用已有行为。[普通排序协议回归](../tests/ordinary_scalar_order_protocol_e2e_test.py)
- 900 行的稀疏数据分别在升序、降序和无匹配时读取 438、773 和 11 行。把 `work_mem` 降到 64 KiB 后，降序用例改用普通扫描读取 900 行，结果和已有索引统计保持正确。另一张表的 1,024 行全部不符合条件，沿索引逐行检查后返回零行。序列测试确认，只有通过筛选的行会调用 `nextval`，也包括 `OFFSET` 跳过的行。
- 新库和旧库的事务及恢复检查共核对 521 组请求与回复和 108 次执行路径观测。旧事务在其他连接提交后仍能读到原先匹配的行，新事务看到更新后的空值。旧索引正常打开时内容一致。实际强制结束服务进程后，重启保留已提交记录并清理未提交写入，需要重建时按现有规则生成 `0xC553`。另有五项 C++ 回归、五项相邻协议测试和手册 SQL 示例通过。完整回归套件待验证。最终程序的 SHA-256 为 `461917e6536ac3f30e2bdc6eba2570bd0774a25f6d3820f1d43281b2b33c0a53`。

<a id="prepared-integer-primary-key-range-20261118"></a>
## 2026-11-18 · 同一条查询用不同的整数主键翻页

PostgreSQL 的 `PREPARE` 支持每次用不同参数执行，也区分可按实参选择的计划和通用计划。SQLite 的翻页示例用参数传入上一页的最后一个键。DuckDB 同样允许应用保留已准备的语句，每次重新绑定参数。[PostgreSQL PREPARE](https://www.postgresql.org/docs/18/sql-prepare.html) · [SQLite 翻页示例](https://www.sqlite.org/rowvalue.html#scrolling_window_queries) · [DuckDB 参数绑定](https://duckdb.org/docs/api/c/prepared)

在默认表空间的普通单列有符号整数主键上，`PREPARE` 后的每次 `EXECUTE` 和网络客户端每次绑定的非空整数都可以确定本次索引起点或终点。支持 `smallint`、`integer` 和 `bigint`，也接受已验证的内置整数常量转换。查询仍使用原有筛选、排序和输出计算。空参数、小数、含列引用或函数的转换、额外筛选条件和不符合索引可见性条件的查询保持原有执行方式。磁盘格式沿用 `0xC553`。[使用示例](MANUAL.md#重复执行带参数的分页)

- 扩展[普通排序协议测试](../tests/ordinary_scalar_order_protocol_e2e_test.py)。同一条语句反复绑定不同值，覆盖三种整数宽度的文本和二进制参数、上下界、反写比较、极大和极小值、空值、非法整数，以及跳过记录时的序列调用。39 组回复与基线逐字节相同，规范化用例中的关系编号后，与 PostgreSQL 18.6 的行、字段描述和协议消息一致。网络测试中的两次实际范围分页从各读 7 行降至 3 行和 2 行，小数条件仍读 7 行。
- 另用真正的 SQL `PREPARE/EXECUTE` 检查三个整数实参。实际读取 4、2、4 行，基线每次读 10 行。序列参数只求值一次，`EXPLAIN` 不执行带副作用的表达式。两个交错执行的查询各用自己的边界，暂停后继续读取也未再次扫描。独立复核新旧和 PostgreSQL 共 195 组请求与回复。
- 新数据库和由 `0xC552` 数据目录打开的场景各完成 196 项协议检查和 28 项普通 SQL 检查。正常打开保留旧索引的字节，恢复需要重建时沿用现有格式规则。实际强制结束进程后重启，已提交的记录仍可查询。两个会话交错读写时，旧事务快照和正写入的会话回到普通扫描，新快照恢复范围读取。另有五项 C++ 回归、四项相邻协议测试和本节手册示例通过。
- PostgreSQL 对照仍有已有差异。客户端未声明参数类型时，本项目的语句描述可能保留 OID 0，二进制绑定返回 `0A000`，非法文本在执行时才报错。暂停后恢复的查询返回相同行，但测试中的完成标签为 `SELECT 3`，PostgreSQL 18.6 为 `SELECT 2`。完整回归套件待验证。最终程序 SHA-256 为 `fc24f96a672692a25f8755651044f5bdeefa422ca1f2c7ce820238b571f8a792`。[性能报告](performance-report-2026-10-30.md#prepared-integer-primary-key-range-cost-20261118)

<a id="integer-primary-key-range-20261118"></a>
## 2026-11-18 · 从上页主键继续读取

PostgreSQL 的 B 树能用整数范围限制扫描起点和终点，也能按查询顺序交付记录。SQLite 的翻页示例把上一页最后一个键写入 `WHERE` 后再查下一页。DuckDB 则可用每批数据的最小值和最大值跳过与条件无关的记录组。[PostgreSQL B 树](https://www.postgresql.org/docs/18/indexes-types.html#INDEXES-TYPES-BTREE) · [SQLite 翻页示例](https://www.sqlite.org/rowvalue.html#scrolling_window_queries) · [DuckDB 选择性读取](https://duckdb.org/2025/05/14/sorting-for-fast-selective-queries)

项目复用有序整数主键索引。符合条件时，它按上下界选择子节点，到达叶页后定位起始键，顺向或反向取到当前页结束。条件只接受同一主键与整数字面量的 `<`、`<=`、`>` 和 `>=` 比较，可用 `AND` 组合。返回前仍执行原来的筛选。旧索引、旧事务快照、参数、其他字段条件和非整数条件保持原查询路径。该改动使用已有的 `0xC553` 格式。[使用示例](MANUAL.md#从上页主键继续读取)

- 直接测试 B+ 树的 678 个范围。在 18,002 条记录的树中覆盖两种方向、正负整数边界、已删空的叶页、访问预算耗尽和叶页链接异常。遍历全树访问 261 个节点，测试中的窄范围最多访问八个节点。
- 普通 SQL 的独立检查通过 93 项，旧十进制索引打开场景也通过 93 项。合并 PostgreSQL 18.6 和基线，共核对 467 组请求与回复及 2,066 个回复帧，其中 60 条普通语句的结果一致。升降序跨叶页各读取 152 行、访问四个节点。严格极端条件和矛盾区间返回零行。两个会话交错读写时，旧快照继续扫描原表，当前查询恢复后使用范围读取。
- 用旧程序生成并关闭的 `0xC552` 文件由新版直接打开，文件内容一致，条件分页继续走原路径。两组项目进程均在尚有未提交写入时收到 `SIGKILL` 并实际退出 `-9`。新进程打开原数据目录后，已提交行可见，未提交行消失，可以继续写入。需要重建索引的恢复场景按现有创建规则生成 `0xC553`。
- 11 条支持的普通 `SELECT` 分别与 `EXPLAIN ANALYZE` 核对，实际表统计都记录索引读取。六种不符合条件的查询和四次带参数的重复执行继续记录普通扫描。两次复用带固定字面量的预编译语句仍走范围读取。独立核对 295 组请求与回复，其中 31 个普通及扩展协议操作在 PostgreSQL 18.6、新版和基线上返回相同的行、完成标记、事务状态和所核对的字段类型。
- 八项 C++ 回归、五项协议测试和手册示例通过。[普通排序协议测试](../tests/ordinary_scalar_order_protocol_e2e_test.py)新增范围、反写条件、页末、空区间和被 `OFFSET` 跳过的表达式求值检查。最终程序 SHA-256 为 `016c6d8ee4a3556fd29069f22f9a93b77661750547757cfa17a9f20f7de11c56`。完整回归套件待验证。[性能报告](performance-report-2026-10-30.md#integer-primary-key-range-cost-20261118)

<a id="signed-primary-key-prefix-20261118"></a>
## 2026-11-18 · 整数主键索引取够就停

PostgreSQL 的 B 树可以按索引顺序向两个方向读，凑够 `LIMIT` 所需记录即可返回。SQLite 也支持按索引顺序读取，覆盖索引可以省去读表。DuckDB 在部分 `ORDER BY … LIMIT` 查询中只保留排名靠前的候选。[PostgreSQL 索引排序](https://www.postgresql.org/docs/18/indexes-ordering.html) · [SQLite 索引排序](https://www.sqlite.org/queryplanner.html#sorting_by_index) · [DuckDB TopN](https://duckdb.org/2024/11/14/optimizers#topn-optimizer)

默认表空间的普通单列整数主键索引在创建或重建时使用新格式。它把有符号整数变成按数值排列的等宽键。分页从所需方向进入 B+ 树，取到前 `OFFSET + LIMIT` 个键就停止。检查访问到的节点、分隔键、叶页链接和表行，结果仍通过已有的排序和输出求值步骤。已有的十进制索引仍用原分页方法，直到重建该索引。旧快照、正在写入、外部表空间等条件继续走普通读取路径。[格式与部署](PACKAGING.md#整数主键索引格式)

- 新建数据库通过 93 项检查，覆盖三种整数宽度、正负数、`bigint` 边界、更新主键和 NULL、重复查询、回滚与崩溃恢复。旧库升级场景通过 123 项。用旧程序写入并正常关闭，换新版打开后读写原有 `0xC552` 文件，再用 `REINDEX TABLE` 重建为 `0xC553`。在尚有未提交写入时中断进程，重启后保留已提交行，撤销未提交行。两组最终数据目录都通过离线校验。含 PostgreSQL 18.6 与基线的独立审计共核对 404 组请求与回复及 1,933 个回复帧。
- 384 行用例用 `LIMIT 3 OFFSET 289` 测试跨叶页读取。升序和降序均从索引取得 292 个候选键，各访问五个节点，并与 PostgreSQL 结果相同。小数或超出整数范围的等值查询按 PostgreSQL 的行为返回空结果。无法转换的文本仍报 `22P02`。外键、更新与删除级联和保存点回滚的独立对照核对 187 个回复帧。
- 对已提交的 `ALTER TABLE … ADD PRIMARY KEY`，在成功回复后立即用 `SIGKILL` 中断进程。基线和修复前候选重启后，按主键查找报 `XX001`。最终版在返回成功前同步索引及其目录。同一数据目录由新进程打开，按主键查到 `2, b`，分页返回 `-10, a` 和 `2, b`，重复键插入仍报 `23505`。另用直接打开索引文件的 C++ 用例确认，返回成功时索引已经落盘。
- 九项 C++ 和六项协议回归通过，覆盖 B+ 树结构、并发、整数写入、冲突处理、离线校验、重建、物理恢复和未记录表。重建恢复用例覆盖八次冷启动和五次主动中断。直接添加主键及事务内清空表回滚的 12 条 SQL 也通过。最终程序 SHA-256 为 `5c769b7edae9143de4ce1cf2a726514f0fa18972c8846b5312f90dc24d83bee9`。完整回归套件待验证。[性能报告](performance-report-2026-10-30.md#signed-primary-key-prefix-cost-20261118)

<a id="integer-primary-key-page-20261118"></a>
## 2026-11-18 · 在旧索引上减少整数主键分页的表行读取

PostgreSQL 可以顺向或反向读数值有序的 B 树，取得所需行后停止。SQLite 也能按索引顺序取表行，覆盖索引可直接交付结果。DuckDB 可将 `ORDER BY` 和 `LIMIT` 合并成只保留前几行的操作，它的 ART 索引用于部分等值条件。[PostgreSQL 索引顺序](https://www.postgresql.org/docs/18/indexes-ordering.html) · [SQLite 按索引排序](https://www.sqlite.org/queryplanner.html#sorting_by_index) · [DuckDB 优化器](https://duckdb.org/2024/11/14/optimizers) · [DuckDB 索引](https://duckdb.org/docs/current/guides/performance/indexing)

该批的整数索引键以十进制字节保存，需要遍历索引并按数值选择候选。B+ 树读取同时检查子节点和叶链，限制访问的节点数和收集的键数。随后只读取分页需要的表行，从同一张加锁的页取得行数据和空值标记，再取出外置的长值。原有排序、计算输出和分页步骤负责交付结果。读取方需要能证明索引对应当前可见数据。旧快照、活动事务、外部表空间、空索引和超出预算的索引使用原扫描。可以继续读取原有磁盘格式。

- PostgreSQL 18.6 通过 77 项分页和事务检查，基线与新版各通过 76 项，另有一项 PostgreSQL 版本查询。独立核对三版的 229 组请求与回复及 1,069 个回复帧。实际比较 48 条普通 SQL 的行、类型、命令完成标记与事务状态。覆盖三种整数类型的边界值、排序别名和序号、空字符串、NULL、数组，以及 24,576 字节的长值。两个会话交错提交、更新主键、删除、插入和回滚时，旧的可重复读快照与写入中的会话都使用原扫描。
- 16 项独立用例比较被 `OFFSET` 跳过的表达式、`LIMIT 0` 和错误位置。三版共核对 168 个回复帧。另有 7 项二进制协议检查，通过同一个已解析语句重新绑定参数，并在两次执行之间提交主键和 NULL 的更新。新版的返回值与 PostgreSQL 一致。
- 表空间检查在 PostgreSQL 上通过 24 项，在项目版上通过 23 项。五项查询的完整结果回复一致。隐式和显式默认表空间都检查八个键并读取三行。确认文件确实在外部表空间后，检查到该表走普通扫描并读八行。新空表和删除后为空的表都走普通扫描并读零行。
- PostgreSQL 的正常回滚和重启通过 25 项。新旧项目版各通过 23 项。私有项目进程在事务尚未提交时收到 `SIGKILL` 并以 `-9` 退出。两个版本各自用不同 PID 打开原数据目录，保留已提交的主键更新、删除和插入，丢弃未提交的改动，仍能提交新写入。独立核对 240 个回复帧。
- [注册协议回归](../tests/ordinary_scalar_order_protocol_e2e_test.py)新增六项查询和两项实际计划检查。B+ 树结构、锁、快照、空值和排序的十二个 C++ 测试入口均通过。稀疏索引用例插入 4,096 个键，再删除中间 4,094 个，验证预算小时可以停止、充足时能读回两个保留键。另有三组原有协议测试通过，覆盖读快照、表扫描表达式和排序溢写。运行了手册中的三条 SQL，按预期返回 `-2, b` 和 `2, NULL`。
- 最终程序由 GCC 13.3、C++17、CMake 和 `-O2` 构建，SHA-256 为 `b1d2328eb2e98d62531ec68d66640cb60dc59e2dfa136e58d7fd2e5bec00e538`。完整回归套件待验证。原始记录保存在本机。[读取量和实际耗时](performance-report-2026-10-30.md#integer-primary-key-page-cost-20261118)

<a id="ordered-projection-20261118"></a>
## 2026-11-18 · 排序后计算查询结果

PostgreSQL 的 B 树可以提供顺序，也可以由优化器选择堆扫描后排序。显式排序时，它仍会提前计算排序键和一部分简单表达式，较昂贵的非排序表达式可以延后。SQLite 可让带排序的子查询按需交付行，以便外层只计算实际收到的行。DuckDB 对部分小结果查询先保留行编号，再补取输出所需的数据。[PostgreSQL 索引顺序](https://www.postgresql.org/docs/18/indexes-ordering.html) · [PostgreSQL 18.6 排序规划](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/optimizer/plan/planner.c#L6112-L6282) · [SQLite 子查询执行](https://www.sqlite.org/optoverview.html#subquery_co_routines) · [DuckDB 延后读取源码](https://github.com/duckdb/duckdb/blob/main/src/optimizer/late_materialization.cpp)

本项目用已有的类型化行数据完成排序。按基表单列整数主键排序时，剩余输出等到取行时再计算。普通堆表排序带 `LIMIT` 时，已识别的高成本内置整数数组与文本转换也延后。实际排序键继续提前求值，别名和序号共享实际排序值。`OFFSET` 经过投影算子，因此仍会计算被跳过行的表达式。该改动沿用现有排序，无需改变磁盘索引格式。

- [注册协议回归](../tests/derived_array_domain_protocol_e2e_test.py)新增 41 项，保留原 149 项。PostgreSQL 18.6 和最终版均通过 190 项，旧版通过 182 项。覆盖升降序、普通和派生输入、`LIMIT 0`、排序别名与序号、跳过行和实际取到的错误。原有联接与相关查询的 59 项对照全部通过，先前四项有序来源的差异已修复。
- PostgreSQL 的实际堆表计划中，简单数组转换和用作排序键的转换在排序前执行。在有限行数查询的实测形态中，七次数组和文本转换被放到排序之后。两种转换写法各通过 12 项独立检查。最终版显式保留排序键对应输出的原求值位置。额外的八项含两个潜在错误的用例与 PostgreSQL 回应一致，覆盖排序列别名、序号和重复表达式。
- 一组 22 项溢写对照在 64 KiB 下实际创建并清理两份临时文件，16 MiB 下没有创建，两种预算的完整回复一致。另一组使用转换后的值更少，低预算下从两份临时文件降为零份。[性能实测](performance-report-2026-10-30.md#ordered-projection-cost-20261118)
- 项目通过 61 项崩溃恢复检查，PostgreSQL 正常回滚和重启通过 62 项。独立核对 366 个回复帧。写入进程在事务未提交时收到 `SIGKILL` 并以 `-9` 退出。重启后保留五条已提交的带下标数组与同域空值，未提交的五行消失，可以重新写入。实际取到无效数组时，整条写入无部分结果。既有三组 29、53、53 项恢复对照复跑通过。
- GCC 13.3、C++17、CMake、`-O2`，关闭 OpenSSL。三个候选各只重编 `ExecutionPlan.cpp`，复用 61 个经核对的对象。最终程序 SHA-256 为 `2e7cdd79c5a14d02fe9df57fe72120b043d9cedc100340927412bc61d01aac12`。八项相邻排序、集合与协议回归及手册五条 SQL 检查通过。完整套件待验证。

受测主键排序仍由项目的排序算子完成。有序索引读取和 PostgreSQL 的完整代价选择仍待实现。所测普通堆表的低成本转换仍可能在实际取行前报错。原始记录保存在本机。[使用示例](MANUAL.md#排序后再转换数组)

<a id="nested-insert-20261118"></a>
## 2026-11-18 · 从包含联接的子查询写入

PostgreSQL 的普通 `LIMIT` 每次向子计划请求一行，达到上限后返回。SQLite 的部分子查询每算出一行就把控制权交回调用者，按需继续。DuckDB 的流式 `LIMIT` 按批接收行，达到上限后结束。本项目复用已绑定的普通联接计划，每次向写入查询交付它请求的一行，并在查询结束或报错时关闭输入。[PostgreSQL 18.6 LIMIT 源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeLimit.c) · [SQLite 子查询执行](https://www.sqlite.org/optoverview.html#subquery_co_routines) · [DuckDB 流式 LIMIT 源码](https://github.com/duckdb/duckdb/blob/main/src/execution/operator/helper/physical_streaming_limit.cpp)

- [注册协议回归](../tests/array_domain_join_using_protocol_e2e_test.py)新增 37 项。旧版通过 260/273，新版与 PostgreSQL 18.6 均通过 273/273，分别核对 1,135 个新版和 PostgreSQL 回复帧。覆盖一层和多层来源、数组下界和空元素、同域不重复检查、混合域重查、`LIMIT 0`、`LIMIT 1` 和 `OFFSET 2 LIMIT 1`。原 236 项全部复跑通过。
- 在真实 PostgreSQL 上核对三个执行计划。所测 `LIMIT 1` 和 `OFFSET 2 LIMIT 1` 分别执行一和三次序列调用，旧版各执行八次。另一个联接条件在第二次调用时报 `22P02`。只取一行会成功，要求第二行时报错并且该语句不留下部分写入。该用例的各个候选行输出相同，错误只取决于调用次数。旧版在只取一行时就报错。
- 独立的扩展协议对照用一次 Parse 和两次参数绑定执行同一条嵌套写入。新版与 PostgreSQL 均通过 10 项，分别写出同域 NULL 和带下界的数组，再读回两行。旧版两次执行都返回 `0A000`，目标为空。
- PostgreSQL 正常回滚和重启通过 54 项，项目进程在事务未提交时收到 `SIGKILL` 并以 `-9` 退出。新进程读取同一目录，53 项检查通过，独立比对 346 个回复帧。保留七行已提交数据，移除六行未提交数据，并可重新写入。实际覆盖目标列 `NOT NULL`、混合域 NULL 拒绝，以及延迟除零错误时目标表无部分写入。第一版私有测试夹具过早排除一行，修正筛选后重新对照。
- GCC 13.3、C++17、CMake、`-O2`，关闭 OpenSSL。重编 `DmlExecutor.cpp`，复用 61 个逐一核对的对象，新版 SHA-256 为 `4fcd987e5699b516aecf7d19d70125d60fff60834db4d78ddddfbe58b18d11bd`。既有 128、149、218、236、244 项协议用例、两组崩溃恢复及八个 C++ 测试入口复跑通过。手册示例执行了十条 SQL，末尾写入并读回预期的一行。完整套件待验证。

本次范围是普通写入 SQL 中直接给出的 `FROM` 子查询。视图、带 `WITH` 的写入、集合操作组合和其他需要不同执行计划的来源仍待验证。原始协议与恢复记录保存在本机。[使用示例](MANUAL.md#从包含表连接的子查询写入) · [实际性能](performance-report-2026-10-30.md#nested-insert-cost-20261118)

<a id="array-lookup-20261118"></a>
## 2026-11-18 · 按整数数组查找联接行

PostgreSQL 的哈希联接根据 `work_mem` 和 `hash_mem_multiplier` 设定内存限额，必要时分批。DuckDB 用哈希值划分输入，暂存尚不能匹配的分区。SQLite 可以为一条查询临时建索引，在预计有多次查找时省去反复扫描。[PostgreSQL 哈希联接源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeHash.c) · [DuckDB 哈希联接源码](https://github.com/duckdb/duckdb/blob/main/src/execution/join_hashtable.cpp) · [SQLite 临时索引](https://www.sqlite.org/optoverview.html#automatic_query_time_indexes)

本项目读完右侧且确有后续左侧行时，可以按比较摘要定位候选，候选按原来的右侧顺序返回，再逐个比较实际数组。提前停止的查询可以在读完右侧之前返回。新建的查找表以当前会话的 `work_mem` 为上限，超过限制时逐行查找。已读的右侧行和已有的摘要缓存仍有各自的内存开销。每次重新执行相关查询时使用该次读取的数据。

- [协议回归](../tests/array_domain_join_using_protocol_e2e_test.py)从 218 项扩展到 236 项。PostgreSQL 18.6、已提交基线和新版均通过，每版核对 1,025 个回复帧。新用例覆盖重复键、多键、排序后分页，以及右侧为空的内联接、左联接和全联接。注册回归可用 `DBMS_MAIN="$PWD/build/cmake/dbms_main" python3 -B tests/array_domain_join_using_protocol_e2e_test.py` 在仓库根目录运行。
- 另有 108 项对照，新旧版完整协议回复逐字节一致。三个不同的 `bigint[]` 产生相同的摘要，实际比较仍只返回真正匹配的行。行序检查覆盖同一摘要下交替出现的相等和不等数组，以及多次停止和恢复读取。右侧 4,099 行时，`work_mem='64MB'` 会建查找表，`'64kB'` 使用逐行查找，两种设置均返回同样四行。`LIMIT 0`、提前满足的 `LIMIT 1` 和只有一行左侧输入的控制也已核对。
- 两组项目崩溃恢复分别通过 29 和 53 项，独立复核 286 和 296 个回复帧。新增的全联接写入控制含重复键、摘要相同的不等数组、非默认下界和 NULL。进程在事务未提交时收到 `SIGKILL`，冷启动后保留已提交的 10 行，未提交批次消失，同一批可重新写入。PostgreSQL 的正常回滚及重启对照分别通过 30 和 54 项。
- GCC 13.3、C++17、CMake、`-O2`，关闭 OpenSSL。相对已提交基线首先重编七个对象，随后为避免在右侧为空时读取左键又重编一个对象。最终程序 SHA-256 为 `8061258031357b8a03ca8f05fe9bf8ecb6748ef22ca9bb436e15631ccf6dc1ab`。此前的 128、149、218、244 项协议用例和八个选定 C++ 入口复跑通过。149 和 244 项套件首次遇到本地证据路径重名，改用新路径后完成。完整套件待验证。

普通 `RIGHT JOIN` 的已测查询可以执行，部分 `EXPLAIN` 仍返回 `0A000`。原始协议和构建记录存于本机。[实际性能](performance-report-2026-10-30.md#array-lookup-cost-20261118) · [嵌套写入后续修复](#nested-insert-20261118) · [排序取数后续修复](#ordered-projection-20261118)

<a id="integer-array-join-20261118"></a>
## 2026-11-18 · 整数数组联接与带域类型的空值

PostgreSQL 的哈希联接先筛选候选，再执行联接条件并记录外联接需要补出的行。DuckDB 会复用部分编码列里的重复键，并只在需要时读取实际候选行。SQLite 可给一次查询建立临时 B 树，避免为每条外表记录重复扫描全部内表键。项目沿用现有联接的读取顺序，只为已经读到的整数数组计算一次摘要。摘要包含真实整数、元素中的 NULL、维度和下界。不同摘要的行对可在复制整行前跳过。相同摘要的行对仍由原数组比较决定匹配，因此重复键和摘要碰撞都由原比较处理。[PostgreSQL 哈希联接源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/nodeHashjoin.c) · [DuckDB 哈希联接源码](https://github.com/duckdb/duckdb/blob/main/src/execution/join_hashtable.cpp) · [SQLite 临时索引](https://www.sqlite.org/optoverview.html#automatic_query_time_indexes)

PostgreSQL 在值转换为域时执行域的 `NOT NULL`。普通 `INSERT … SELECT` 复制外联接产生的空值时，如果值本身已具有目标数组域类型，允许存入仅由域声明 `NOT NULL` 的列。目标列自身的 `NOT NULL` 仍然拒绝这种空值。普通 NULL、转换成基础类型或来自其他域的 NULL 仍需通过目标域转换。触发器替换目标列时使用修改后的值重新检查。[PostgreSQL 域约束说明](https://www.postgresql.org/docs/18/sql-createdomain.html#SQL-CREATEDOMAIN-NOTES)

### 实际验证

- [协议回归](../tests/array_domain_join_using_protocol_e2e_test.py)由 128 项扩展到 218 项，保留原来的 SQL 和预期。最终程序和 PostgreSQL 18.6 均通过 218 项，分别核对 925 个回复帧和 74 项字段来源。旧版通过 216 项，剩余两项是同域空值写入被拒绝及随后读不到该行。
- 三种整数数组的内联接和全联接，以及多数组键联接，都确认执行了筛选。重复键、相邻大整数、NULL、维度和下界各有实测。标量与数组混合作键的查询也通过对照，走现有比较。`LIMIT 0` 未打开联接，两种 `UNION ALL` 和扫描来源的实际需求测试共通过 34 项，包括先返回一行，再按需报告后续非法数组。四个改变输入的相关子查询和 PostgreSQL、旧版回复逐字节一致。
- 使用 GCC 13.3、C++17、CMake、`-O2` 构建，关闭 OpenSSL。相对于已提交基线重编七个对象，复用并核对 55 个对象。程序 SHA-256 为 `bc8f3f42b11ce6b8a56d68761ef7e95fe58e8e78cac1a7dd67d8afe2449eae4f`。此前的 128、149、244 项协议套件分别复跑通过。另外通过 11 个插入、默认值、约束和触发器协议入口，以及七个选定的 C++ 入口。完整套件待验证。
- 已提交的同域空值在实际 `SIGKILL` 后保留，未提交的值消失，原键可在冷启动后重新插入。域有无额外 `CHECK` 约束的两种情况均覆盖。项目通过 53 项，PostgreSQL 的正常回滚和重启通过 54 项。独立审计匹配了 53 对请求与回复、296 个回复帧。目标列自身的 `NOT NULL` 和普通无效空输入在重启前后仍返回 `23502`。

当时需要先排序的派生来源仍有四项取数顺序差异，原失败回复与旧版逐字节相同。普通 `RIGHT JOIN` 的 SQL 对照通过，所测语句的 `EXPLAIN` 仍返回 `0A000`。原始协议、进程和构建记录保存在本机。[实际性能](performance-report-2026-10-30.md#integer-array-join-cost-20261118) · [两种嵌套联接写入的后续修复](#nested-insert-20261118)

<a id="array-using-20261118"></a>
## 2026-11-18 · 数组列联接后保留类型

`JOIN … USING` 在绑定时分别确定两列怎样比较，以及合并列来自哪一侧。PostgreSQL 在两个输入确实同类型时保留类型，只有两边的类型和长度限制都相同时才沿用该限制。`LEFT JOIN` 取左侧，`RIGHT JOIN` 取右侧，`FULL JOIN` 用有值的一侧。`INNER JOIN` 会优先选无需转换的原列。DuckDB 也在绑定时记录 `USING` 的主侧和两边的来源。SQLite 对左右联接分别取保留侧，`FULL JOIN` 用两边列的 `coalesce`。项目按 PostgreSQL 的规则选择实际数组类型、输出表达式和字段来源。确认联接键全是所支持的数组时，只有要返回的行才生成合并列。[PostgreSQL 18.6 联接绑定](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/parse_clause.c#L1553-L1646) · [PostgreSQL 18.6 类型修饰符](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/parse_coerce.c#L1551-L1576) · [DuckDB 联接绑定](https://github.com/duckdb/duckdb/blob/main/src/planner/binder/tableref/bind_joinref.cpp) · [SQLite 联接规则](https://www.sqlite.org/lang_select.html)

- [新增协议回归](../tests/array_domain_join_using_protocol_e2e_test.py)在 PostgreSQL 18.6 和最终程序上均为 128/128。已提交基线为 69/128，第一版候选为 124/128。两边实际核对各 464 个回复帧，含 46 项严格的字段来源预期。覆盖四种联接、自然联接、限定星号、嵌套查询、同域和不同域、普通数组长度限制，以及两个相等的 `numeric[]` 值带有不同小数位时应返回哪一侧。
- 已检查的同域值写入允许空值的目标域时省去重复检查。混合域、普通数组和值类型不同的限定列在写入时重新检查。序列计数、实际写入内容和零行插入都已核对。不同基础数组类型在绑定时返回 `42883`，包括 `LIMIT 0`，回复先发错误，随后发客户端可继续使用的状态。显式转换为相同数组类型后联接成功。
- 最终版使用 GCC 13.3、C++17、CMake、`-O2`，关闭 OpenSSL。最后一轮重编三个对象，复用 59 个逐一核对过的对象，程序 SHA-256 为 `d9c9c5913fc6721a6b5c4b87e6b187f234d808f6f8de5b32db910474b12a92fd`。上一批 149 项派生数组和 244 项数组域回归均通过。九个既有协议入口通过八个，七个选定 C++ 入口通过五个。其余三个入口的原始失败输出与基线逐字节相同，涉及 `pg_settings`、引用字段的返回声明和部分系统目录视图。完整套件待验证。
- 在客户端仍持有未提交事务时，测试向私有服务进程实际发送 `SIGKILL`，进程退出码为 `-9`。新进程用同一目录启动后保留三条已提交记录，两条未提交记录消失，原主键可以重新插入并提交。项目 32/32，PostgreSQL 的正常回滚重启对照 33/33。独立审计核对了 32 组共同查询的回复和 190 个回复帧。
- 别名与权限检查保留了原目录实际记录的属性编号。删除列后本项目压缩了字段编号，PostgreSQL 保留原编号。该批向 `NOT NULL` 数组域写入外联接补出的同域 NULL 时仍报 `23502`，后续修复见上节。另有部分嵌套联接写入、跨数组独立比较与 `JOIN ON` 显式转换未通过原有预期。[实际耗时](performance-report-2026-10-30.md#array-using-cost-20261118)

<a id="derived-arrays-20261118"></a>
## 2026-11-18 · 派生表里的数组

派生表把数组值与真正的类型声明一起交给外层查询。客户端收到正确的基础数组类型，`pg_typeof` 仍能区分数组域。直接选取原表字段时，结果描述保留来源表和属性编号。已经验证的同一域的值可以原样复制；混合普通数组、不同域或无类型 NULL 的 `UNION ALL` 和 `VALUES` 会按共同类型绑定，在写回域列时执行应有的检查。

PostgreSQL 会展开符合条件的子查询，将域转换复制到每处引用。带 `OFFSET 0` 或显式物化的来源可以共享转换结果。真实 PostgreSQL 18.6 的计划和序列对照证实了这两种行为。SQLite 可展开子查询、用协程按需提供行或存到临时表。DuckDB 用带类型的列向量传递中间结果。这一批沿用项目现有的带类型子计划，并保留实际依赖的取数顺序。[PostgreSQL 18.6 子查询展开源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/optimizer/prep/prepjointree.c#L1939-L1948) · [PostgreSQL 域转换的易变性源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/optimizer/util/clauses.c#L636-L650) · [SQLite 查询优化](https://www.sqlite.org/optoverview.html#subquery_co_routines) · [DuckDB 执行格式](https://www.duckdb.org/docs/preview/internals/vector)

PostgreSQL 先比较数组元素，再比较元素个数、维度和下界，数组内的 NULL 排在非 NULL 值后。DuckDB 的变长列表也按位置和元素类型比较。SQLite 的五种存储类别没有原生数组。项目的数组排序沿用类型各自的比较规则，已解析的元素可直接参与比较。带时区的时间先比较换算后的微秒数，再比较偏移。JSONB 使用自身的类型和结构顺序，嵌套数组先比较元素个数。同一套排序规则用来判断参与 `DISTINCT` 的这些数组是否相等。[PostgreSQL 数组比较源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/arrayfuncs.c) · [时间偏移比较源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/date.c#L2368-L2400) · [JSONB 比较源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/jsonb_util.c#L175-L279) · [DuckDB 列表比较](https://duckdb.org/docs/current/sql/data_types/list#comparison-and-ordering) · [SQLite 存储类别](https://www.sqlite.org/datatype3.html#storage_classes_and_datatypes)

### 实际验证

- GCC 13.3、C++17、`-O2`、CMake、关闭 OpenSSL。最终版重新编译 7 个依赖受影响的对象，复用 55 个逐一核对的对象，程序 SHA-256 为 `abd8c5f1bec6cf69c867ff4635accc27dce3a769db268418e20f93a10e0da960`。第二版因 `Session` 前置声明的命名空间错误未编过，第三版已修正。
- [新增回归](../tests/derived_array_domain_protocol_e2e_test.py)在 PostgreSQL 18.6 和第三版上均通过 101/101。已提交基线通过 35/101，第一版候选通过 93/101。覆盖普通数组、数组域和引用数组的派生查询与写入，含嵌套、联接、维度、NULL、事务回滚和重启。相邻的 244 项原数组域协议回归全部通过，独立验证的 54 项取数和错误响应、35 项集合及 `VALUES` 对照也全部通过。各组覆盖可能重叠。
- 第三版的独立测试发现，部分旧版可用的数组 `ORDER BY` 和 `DISTINCT` 在新路径报错。第四版通过整数、精确数值、浮点、字符数组的 129 项扩展回归。第五版加入日期、时间、微秒和 `24:00:00`，通过 136 项。最终版加入带时区的时间及 JSONB，与 PostgreSQL 18.6 均通过 149 项，每边核对了 721 个实际回复帧。144 行排序在 64 kB 的内存设置下实际创建并清理两份临时文件，16 MB 下无此文件，两种设置回复逐字节一致，域检查没有重复执行。
- 错误响应检查保留了发生阶段。无效数组字面量在绑定时返回 `22P02`，包括 `LIMIT 0`。从实际行转换时发生的错误先返回列描述，再返回错误。成功的 `LIMIT 0`、假条件和空来源保留数组列描述，序列确认这些场景未执行域检查。
- 三个选定 C++ 入口退出 0。五个既有派生查询协议入口中四个通过。派生 WITH 测试的 139 个动作仍有 96 项失败，最终版与已提交基线的完整输出逐字节一致。另运行八个二进制结果及数组专项入口，七个退出 0。TIMETZ 的 2,563 个动作有 313 项与 PostgreSQL 不同，旧版为 314 项，唯一变化是 `12:34:56Z` 的二进制结果现在与 PostgreSQL 一致。完整套件待验证。
- 单独的崩溃恢复通过 43 项，PostgreSQL 18.6 的正常回滚和重连对照通过 44 项。测试进程收到 `SIGKILL` 后以 `-9` 退出。重启保留已提交的数组下界、空元素和引用编号，未提交的行消失，原主键可重新写入。独立审计核对了 87 对请求和回复及 282 个回复帧。

原始 106 项派生对照保留完整预期。PostgreSQL 18.6 通过 106 项，已提交基线通过 58 项，这一批通过 95 项。余下差异涉及可展开派生表被多次引用时的检查次数，以及视图结果类型和写入。显式物化与 `OFFSET 0` 的共享转换行为已通过所测控制。补充测试还保留同一数组域 `JOIN … USING` 的类型、两种同类型转换的字段来源、普通 JSON 数组排序的错误码，以及超宽 JSONB 数值的 `array_position` 差异。原始失败、实际回复和计划留在本地。[运行性能](performance-report-2026-10-30.md#derived-array-cost-20261118)

<a id="array-domains-20261118"></a>
## 2026-11-18 · 数组域

数组域是在已有数组类型上定义的一套可复用规则。表记录实际域类型，客户端查询使用相应的数组类型返回值。插入、更新或显式转换时，先按元素声明转换数据，再检查整个数组。支持所测普通数组和引用数组、域继承、默认值、元素 NULL、非默认下界、字符和位串长度。原样复制已检查的值时，读取和排序应保留已检查的域身份。切片和混用普通数组的条件表达式会产生普通数组，写回域列才重新检查。

PostgreSQL 用 `pg_type.typbasetype` 表达域的直接基础类型，并在输入转换后执行域规则。SQLite 的列类型亲和性控制常见转换，DuckDB 则区分固定长度 ARRAY 和变长 LIST。这两种设计有助于比较元素存储与类型约束的归属。项目选择按 PostgreSQL 的外部行为处理域，在自己的存储格式中保留数组元素及其类型信息。[PostgreSQL CREATE DOMAIN](https://www.postgresql.org/docs/18/sql-createdomain.html) · [PostgreSQL 18.6 类型定义源码](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/typecmds.c) · [SQLite 类型亲和性](https://www.sqlite.org/datatype3.html) · [DuckDB 类型文档](https://blobs.duckdb.org/docs/duckdb-docs.pdf)

先前版本可能写入基础类型登记错误的数组域。所测旧错误声明在建表或转换等用途中会返回 `XX001`，原目录字节和所测旧标量域保持可读取。现有非空表添加数组域列、改成或改出数组域、域的约束修改及重命名会返回 `0A000`。数组元素本身为域、触发器前后检查顺序、已带类型的空值写入 `NOT NULL` 域和长数组容量仍有差距。

当时的派生表正向对照在 PostgreSQL 18.6 通过 106 项，同批第六版通过 58 项。失败涉及列类型、派生来源的绑定和写入。上方的新一轮对照记录了后续修复和剩余差距。

### 已验证的操作

- 用 GCC 13.3、C++17、`-O2` 和 CMake 构建，关闭 OpenSSL。最终程序 SHA-256 为 `8fd1006f4a76fa11c5d65e5a5e5f5ff8118192288144a2bd74f8709b408fc234`。八个候选按依赖变化增量构建，最后一版只重编入口文件，复用了 61 个逐个核对过的对象。
- [新增协议回归](../tests/array_domain_protocol_e2e_test.py)在 PostgreSQL 18.6 和最终程序上均为 244/244。比较了查询结果、列类型和修饰符、SQLSTATE 以及事务状态。包括两种 `CASE` 转换写法、查询写入、长度限制、派生域、回滚和重启。非法默认值在声明时被拒绝。通过序列确认 `ARRAY[...]` 中的调用只在实际插入时发生。基线执行 7 项、通过 4 项，在创建派生域时中止，剩余 237 项未执行。
- 前一候选对全部 244 项通过 241 项，余下差异都来自旧的 SQL 改写把默认值里的 `ARRAY[...]` 当成了数组下标。最终版从语法树取得这段默认值。各次失败和回复都保留在本地。
- 16 个选定 C++ 入口和 16 个既有客户端回归通过。原有 108 项引用查询和数组域检查全部通过，保留了原预期和重启步骤。新增 C++ 回归验证类型目录、实际元素长度、拥有类型的转换及排序临时值的校验。

- 5 项旧目录兼容与 25 项恢复检查通过。真实测试进程在客户端和未提交事务仍存活时收到 `SIGKILL`。重新启动后已提交的类型、表、默认值与数据仍可使用，未提交的类型与表均未留下。保留了拒绝旧目录前后的字节比较。
- 排序写入的 15 项通过。当排序内存为 64 kB 时观察到两个临时排序文件，64 MB 时没有排序文件。两组各正确复制 128 行，包含 25 个全数组 NULL 及带非默认下界的数组。两组均未额外调用域检查。两个结果的协议来源属性与 PostgreSQL 仍有差异。
- 表结构保护的 77 项及非法默认值的 6 项通过。前者覆盖 8 处拒绝操作，比较了类型、表结构、堆及索引文件。拒绝前后观察到的文件哈希一致。该组包含 16 处有意保留的 PostgreSQL 功能差异。非法默认值的 6 项在两边预期相同。

完整套件与生产负载待验证。既有视图写入、继承 CAST 和协议来源属性差异仍保留。与上一版的实际查询、写入和建表成本另见[性能报告](performance-report-2026-10-30.md#array-domain-cost-20261118)。

<a id="stored-reference-output-20261117"></a>
## 2026-11-17：修复类型、表引用的查询显示和数组比较

普通物理列输出现在把 REGTYPE、REGCLASS 的存储编号转换成对应名称，例如类型编号 `23` 显示为 `integer`。排序、去重和内部临时结果继续使用原始编号；最终返回客户端时才转换，同一次输出缓存已经查过的名称。空结果、全 SQL NULL 或已经生成显示文本的字段不会为这个转换额外读取目录。数组保留元素 NULL、OID 0、未知编号、维度和非默认下界。

同时修复引用数组的普通排序与 DISTINCT；补齐无目录属性的内部临时表中引用字段的真实类型信息；父子表的所测普通列查询合并后统一排序、分页，包含隐藏排序字段、别名、序号、重叠 OR 和通配符。`LIMIT 0` 保留原来的类型绑定与空接收路径，序列检查确认 WHERE 未被求值。WITH 的子查询在拆分前保留原始表达式，避免把数组构造误写成取下标操作。所测普通视图和括号查询可以正常展示名称，物化消费者仍接收原有存储值。

### 实际验证

- **构建**：GCC 13.3、C++17、`-O2`、正常 CMake，关闭 OpenSSL。初次重编 13 个受依赖影响的目标，后续按改动增量重编；其余对象逐一核对源码、依赖、选项和哈希。最终程序 SHA-256 为 `e994f74d7d792181550851ac1993cd812f2246a192984872d7bc55535dd2d363`，旧版为 `b31537087e7a377992157670f01874cc1f069f164e93b6535b64401afc075174`。这不是全量清空重建，没有替换根目录程序或重启现有服务。
- **C++**：29 个选定入口全部通过。新增[文本输出](../tests/stored_reference_output_test.cpp) 88 项、[真实存储及执行计划](../tests/stored_reference_provenance_test.cpp) 347 项、[数组排序与去重](../tests/stored_reference_sort_test.cpp) 527 项、[内部临时表类型](../tests/internal_reference_metadata_test.cpp) 192 项，共 1,154 项新增检查。覆盖真正的元素 OID、原始行不被改写、NULL、错误不部分发布、低内存排序预算及类型来源阴性条件。
- **既有客户端入口**：14 个中 13 个通过；`cte_window_owner_protocol_e2e_test.py` 的 747 个动作仍有 29 个失败。旧版与各候选的未过滤结果一致，涉及固定来源表编号、错误发生阶段及嵌套 CTE 窗口缺少列描述；未修改预期，也未修复用户暂缓的规划或输出策略。
- **SQL、事务与恢复**：独立对照保存 PG18.6/旧版/新版 160/159/159 组回复，连同 3 条 PG 启动查询共审计 481 对请求与完整响应。新版通过 156/159，剩下三条都是继承表的 CAST 查询遗漏子表，分别在正常、冷启动和恢复阶段复现。48 个回滚/恢复控制全部通过；7 次正常停止、两个项目版本各一次真正 SIGKILL，客户端在杀进程时仍保持连接。重启后所测已提交引用保留、未提交修改消失。PG 对应控制使用回滚和正常重启。

新增自动协议入口均使用真实 PostgreSQL 18.6 核对，并用另一份解码器复核保留的请求、回应：

| 入口 | 旧版通过 | 最终版通过 | PostgreSQL 18.6 |
| --- | ---: | ---: | ---: |
| [类型引用](../tests/regtype_direct_projection_protocol_e2e_test.py) | 61/86 | 86/86 | 86/86 |
| [表引用](../tests/regclass_direct_projection_protocol_e2e_test.py) | 47/62 | 62/62 | 62/62 |
| [父子表查询](../tests/regtype_inherited_projection_protocol_e2e_test.py) | 28/60 | 60/60 | 60/60 |
| [派生表、视图与括号查询](../tests/regtype_materialization_protocol_e2e_test.py) | 32/35 | 35/35 | 35/35 |

合计 243 条检查全部通过并接入脚本测试入口。新克隆可构建后设置 `DBMS_MAIN` 为程序的绝对路径，逐一运行表中的 Python 文件；测试使用独立目录。其检查包含行值、SQL NULL、列名、类型 OID、命令标签和事务状态，不等于全部 PostgreSQL 列来源属性一致。完整 SQL 对照另保留 24 项既有属性序号差异，REGCLASS 专项也保留相同类别的差异。

### 保留的失败和未完成范围

[108 条完整诊断](../tests/regtype_direct_projection_known_gap.py)仍要求数组域及继承组合全部成功：PG108/108；最终版实际执行 51 条，50 通过，数组域建表返回 `0A000`，后 57 条未执行。[40 条完整物化诊断](../tests/regtype_materialization_known_gap.py)仍要求从视图 INSERT SELECT 成功：PG40/40；最终版实际执行 32 条，31 通过，该写入返回 `0A000`，后 8 条未执行。两个脚本保留全部原始正向预期，手动运行仍会失败；它们与自动入口共用用例/运行器，分组后的 SQL、预期、顺序和清理步骤已逐字段核对，没有失败允许名单。

前期候选曾漏掉数组 DISTINCT、继承通配符、全局排序以及临时表数组转文本。私有调用诊断确认，临时标记只在建表时存在、不写入 schema；最终改用实际命令登记、会话、schema 与 heap 验证来源。一个新增测试初用 C++20 默认比较运算符，改为等价 C++17 实现；原编译失败、运行失败、源码和修正记录均保留。测试驱动也曾误列旧文件名，该次未算通过，剩余入口改用实际文件重新执行。

参考 [PG18.6 的结果输出](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/access/common/printtup.c)、[引用名称转换](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/regproc.c)、[数组比较与输出](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/arrayfuncs.c)。SQLite 的[类型值和文本访问](https://github.com/sqlite/sqlite/blob/master/src/vdbeapi.c)、DuckDB 的[有类型的 Value 与文本转换](https://github.com/duckdb/duckdb/blob/main/src/common/types/value.cpp)仅作为数据与显示分离的实现参考，不表示两者实现 PostgreSQL 引用类型。

完整测试套件未运行；数组域、视图写入、继承表达式及来源属性等仍需继续工作。原 273 项事项及用户暂缓范围不变。性能测量见[本批报告](performance-report-2026-10-30.md#stored-reference-output-cost-20261117)，未证明稳定提速。原始源码快照、协议、故障与计时记录只保存在本机忽略目录 `build/stored-reference-output-20261117/`，不进入 Git。

<a id="scalar-storage-demand-20261117"></a>
## 2026-11-17：减少未引用字段的转换，并记录函数参数缺口

现有表达式 helper 保留行数据、列名绑定和类型提示的原有来源，只省去未被当前表达式引用字段的 REGTYPE/数组存储转换。已引用的字段仍按原来的顺序，在执行函数之前完成转换；生成列和输出值强制转换保留原路径。遇到尚未识别的语法节点时，也保留原转换。这没有省去底层表字段读取，更没有替换旧式函数参数解析。

另外新增独立的按需取列接口：类型分析使用声明，只有实际求值时才调用取值回调；一次 helper 调用内缓存已读取的字段，返回值拥有数据。生成列表达式仍使用原有声明和读取规则。回调生产者仍须保证源数据与 NULL 信息的寿命，接口本身不是表快照、TOAST 文件租约或并发存储修复。

- **构建**：GCC 13.3、C++17、`-O2` 的正常 CMake 构建，关闭 OpenSSL。按依赖重编 16 个目标，为实际转换裁剪再重编 helper；其余对象在源码、依赖、选项和哈希核对后复用。最终程序为 `b31537087e7a377992157670f01874cc1f069f164e93b6535b64401afc075174`，不称为全量清空重建。
- **C++ 回归**：[基础按需读取](../tests/lazy_scalar_columns_test.cpp)通过 27 项检查，[真实存储类型与生成列](../tests/lazy_scalar_metadata_test.cpp)最终通过 163 项检查。覆盖 OID 0、REGTYPE 身份、数组边界与元素 NULL/文本 NULL/空串、返回值寿命、限定列名与重名优先级、未引用损坏输入、已引用转换错误先于副作用，以及生成列/输出强制转换的保守处理。选定的 20 个 C++ 入口最终版本均通过；扩展后的类型测试单独重跑，原 102 项版本和初次失败记录仍保留。
- **客户端回归**：8 个既有入口通过，包含 REGTYPE、REGTYPE 数组、domain、行构造、numeric 默认值冷启动、复合类型、带引号目录字段和拥有实际类型身份的目录查询。测试驱动曾误列一个不存在的 Python 文件；该次不是通过的测试，改用现有类型目录入口单独完成，原错误日志保留。
- **实际查询与恢复**：相同性能数据的 PG18.6/旧版/新版保存 46/45/45 组请求和回复。另一份解码器核对 45 组新旧完整回复一致、90 组 PG 语义对照一致（只排除来源表 OID）。7 次服务正常退出，两个项目版本各执行一次真实 SIGKILL 后重启，已提交数据保留，所测未提交文本和 REGTYPE 修改消失。额外的裸 `SELECT id,t,r0,r7` 控制发现旧版与新版仍会显示内部 OID 23/25，而 PG 显示类型名；这个既有缺口单独保留，匹配的恢复控制明确使用 `r0::text`/`r7::text`，没有把不同结果归一化成通过。

这里的“新版”指当时的 `b3153708…`。上方[后续查询输出修复](#stored-reference-output-20261117)已覆盖所测普通引用投影；本段保留该批历史反例和测量，不代表当前仍有相同显示缺陷。

[严格参数对照脚本](../tests/scalar_argument_expression_known_gap.py)保留全部 67 项预期：真实 PG18.6 全部通过；旧版和本版均通过 47 项、失败 20 项。第二解码器复核后，新旧 67 份完整回复逐字节一致，测试对象的随机名称只用于核对 SQL 对应关系。失败涉及转义引号、字符串拼接、带引号字段、遗漏的序列副作用、被忽略的运行时除零，以及失败事务状态。所测 INSERT SELECT、UPDATE、DEFAULT 和正常重启持久化控制通过，不扩大为所有写入已验证。该脚本手动运行会返回非零，未纳入声称通过的测试列表；没有降低预期或添加失败允许名单。

直接把旧式函数调用切换到完整 AST helper 的方案没有接入：单独实验表明，它会让常量 NULL 调用产生额外序列副作用；PG 在执行前可消除这种严格调用，但运行时 NULL 列仍须求值其他参数。常量错误的处理顺序、实际选中重载的严格性和一次语句的程序所有者仍需完成，不能用函数名名单或每行探测值代替。用户暂缓的规划/portal 等范围不变，普通参数缺口仍开放。

初版新增基础测试误把 `upper(t,t)` 的既有运行时拒绝当成分析阶段拒绝；最终仍要求 `42883`，并核对重复实参只读取一次，不声称修复该错误阶段。扩展类型夹具也曾漏开 owning value 捕获，修正调用参数后保留全部原断言。所有失败版本均保留。完整测试套件未运行；[性能结果](performance-report-2026-10-30.md#scalar-storage-demand-cost-20261117)单独列出，没有把新增接口测试当作整个数据库提速证明。

原始数据、独立解码与构建记录保存在本机忽略的 `build/scalar-lazy-input-20261117/`、`build/scalar-argument-protocol-20261117/`、`build/scalar-demand-research-20261117/`，不进入 Git。

完成复核后删除了 23 个可重建的重复测试程序和 24 份成功计时数据副本，文件逻辑大小合计 412.1 MiB。普通构建程序、对象文件、两个新增测试的最终程序、初始化/恢复数据和全部回复仍保留。

<a id="namespace-snapshot-20261117"></a>
## 2026-11-17：减少函数查找的复制，并修复目录写入时的内存故障

函数查找只需要 schema 列表，现改为复制这部分信息；已有事务所有权、目录物理身份和发布状态检查仍执行。第一次读取及其他服务修改目录后的读取仍经过完整加载，公共完整快照接口仍返回原有内容。这里没有增加跨查询缓存，也没有改变函数选择规则。

故障测试另外发现：目录转换成文本时，输出流会吞掉部分缓冲区分配异常，随后可能发布不完整文件。旧版的独立写入控制在一次分配失败后退出 1，实际 `pg_namespace.cat` 变成只有 15 字节的 `10000,"rrrrrrrr`；新版本在同一个控制中退出 0，失败时保留全部原文件，重试后可重新打开完整内容。现在序列化输出流会明确抛出异常，所有目录内容完成转换后才进入文件发布。

- **构建**：正常 CMake、C++17、GCC 13.3、`-O2`，关闭 OpenSSL。先重编受头文件影响的 12 个目标，再为序列化修复重编 1 个；其余对象逐一核对源码、依赖、选项和哈希后复用。最终程序 SHA-256 为 `9d83cd45f29fcb54b041972d5ad4ffa87d1f296cbb9fe6774ee80803a07539ae`，不是全量清空后的构建。
- **新增 C++ 测试**：[快照测试](../tests/catalog_namespace_snapshot_test.cpp)完成 74 项检查，覆盖冷热读取、只读文件、返回值存续、其他线程的已提交视图、保存点、回滚、目录文件缺失/符号链接/目录替代，以及物理数据库替换。[内存故障测试](../tests/catalog_namespace_exception_test.cpp)完成 492 项检查、236 个实际分配故障点；短读取路径逐点注入，较长的完整加载路径各选 64 点，序列化的 13 个分配点在只读比较和实际发布中全部检查。
- **已有回归**：所选 26 个 C++ 入口通过 25 个。`schema_routine_namespace_identity_test.cpp:33` 在旧版和新版本均以 `-6` 失败，仍要求原有的 `app_aux.bump.func` 路径；未修改测试，也未恢复用户暂缓的原生参数/例程约定工作。8 个既有客户端协议入口全部退出 0，覆盖 schema 来源、schema 原子性、复合类型、过程、枚举、类型目录、numeric 默认值冷启动和损坏行目录。
- **实际 SQL 与恢复**：PostgreSQL 18.6、旧版、新版本分别保存 89/88/88 组请求和回复；另一份解码器重读全部回复，88 组新旧完整帧逐字节一致。每个项目版本有 82 组与 PG 的语义一致，另外 6 组差异保留：转义引号、字符串拼接、两条空结果查询的常量错误阶段，以及两条 namespace 查询的来源字段编号。7 次服务正常退出；两个项目版本各执行一次真实 `SIGKILL`（`-9`）并重启，提交的数据和函数保留，未提交的数据修改、schema 和函数替换消失。PG 对应控制使用回滚和正常重启，没有宣称执行了 PG 崩溃恢复对照。

初版故障夹具只接受 `bad_alloc`，过窄地拒绝了既有加载器返回的目录读错/校验错误；最终测试也接受 `58030`、`XX001`、`53200`，仍要求实际故障触发、返回失败、没有部分结果且能够重试。原来的失败、改进夹具后的失败和独立旧版写入反例全部保留在本机。选定无效目录文本仍可能被原有加载器接受，本次不是所有目录损坏、内存不足或并发情况的全面修复。完整测试套件未运行，原有查询问题及用户暂缓范围保持开放。

性能方法和逐项结果见[性能测试报告](performance-report-2026-10-30.md#namespace-snapshot-cost-20261117)。原始帧、测试程序、构建记录和测量脚本仅在本机忽略的 `build/namespace-snapshot-20261117/`，不进入 Git；仓库保留源码、回归测试和文字报告。

清理了本轮可重建的重复测试程序和成功计时数据副本；源码、对象文件、最终普通程序、两个新增测试程序及全部回复仍保留。

<a id="row-directory-reuse-20261117"></a>
## 2026-11-17：字段目录复用候选未采纳

两个未采纳候选尝试在完整行的大字段展开和直接选列解码中，各自在当前函数调用内复用已经校验的目录，并在遍历表结构时记录变长字段序号。仍在第一个实际需要读取的非 NULL 变长字段处检查完整目录，后续字段直接读取已验证的位置；只读固定字段或 NULL 时保持原来的读取需求。读取器借用当前不可变行，函数返回后销毁，没有跨行、跨连接或跨线程保留缓存。通用字段接口仍独立检查，虚拟列求值和其他调用者保留原路径。

两个候选均未证明稳定收益，生产改动与候选组件测试扩展已撤回。269 个生产输入恢复为 `624c365f`，只保留新增的 22 项现有解码接口回归，已与该生产版本链接并在新私有目录执行通过。以下记录最终候选的实验验证，不作为当前产品增加了读取器的声明：

- CMake 增量构建成功，C++17、`-O2`、关闭 OpenSSL；重编 TableManage.cpp，复用并核对 61 个未变化对象。269 个生产输入的构建前后哈希一致，最终程序为 `f0df0eae…`。这不是重新编译全部生产文件或 TLS 验证。
- 目录组件的普通与 ASan/UBSan 测试各通过 8,479 项，增加了读取器的反向取值、嵌套读取和异常、下一行及 32 位大字段目录控制。插桩仅覆盖目录组件。
- 新增并保留 `variable_row_bulk_decode_test.cpp`，22 项检查在最终候选和当前生产版本均通过：选列、完整行、失败后不发布部分结果、NULL／固定字段需求、嵌套位图恢复，以及固定与变长字段交错、未选字段和 NULL 字段的序号。首次夹具把整数的类型编号误当作字节数，声明了 8 字节、却只编码 4 字节，被目录检查正确拒绝；修正声明并增加宽度断言后通过，保留原失败。
- 26 个既有 C++ 入口中 25 个通过；TOAST 仍在既有 `toast_test.cpp:604` 断言失败，未删除或放宽。8 个协议入口全部通过，包含 51 项真实损坏页检查、多大字段、域、引用类型、数组、行构造、numeric 默认值重启和复合类型命名空间。完整套件未运行。
- PostgreSQL 18.6／旧版／最终候选记录 32／44／44 组 SQL 请求与回复；第二个解码器核对全部 120 组。新旧版 44/44 完整回复相同；与 PostgreSQL 的 64 组可比回复在值、NULL、列描述（排除来源表 OID）、标签、错误码和事务状态上相同。无 ORDER BY 的查询按保留重复次数的结果集合比较；首次对照误要求相同顺序，修正后完成，未把顺序差异当成数据库缺陷。
- 旧版与最终候选各有一次真实 SIGKILL，等待状态均为 `-9`；重启后已提交数据保留，所测未提交更新和插入消失。另有六次正常服务退出，仅证明这些终止点和数据形状。

单独的链接包装诊断使用相同生产对象，观察到有序宽表查询每个实际行调用完整行读取，直接选取五列的查询每个实际行调用部分列解码，三项各 256 行；返回地址对应 TableScanOp::nextRow。诊断程序只用于确认调用路径，未用于性能计时；它没有直接计数每次目录校验，也不证明所有查询都使用这两条路径。

参考了 [PostgreSQL 的字段偏移处理](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/access/common/heaptuple.c)、[SQLite 的列头解析与游标状态](https://github.com/sqlite/sqlite/blob/master/src/vdbe.c)和 [DuckDB 的字符串目录检查](https://github.com/duckdb/duckdb/blob/main/src/storage/compression/string_uncompressed.cpp)。三者的格式和复用范围不同；本次候选只在明确的一行解码范围内减少重复工作，未采用全局指针缓存。

[性能报告](performance-report-2026-10-30.md#row-directory-reuse-cost-20261117)保留两个实现的全部查询类别和慢项；未证明时延稳定改善，候选未合入生产源码。原 273 项状态及用户暂缓范围保持不变。原始请求回复、测试输出、构建和诊断记录仅在本机忽略的 `build/row-directory-reuse-20261117/`；已清理 54 个可重新链接的测试程序，释放约 863 MiB，源码、对象和日志保留。

<a id="variable-row-directory-20261117"></a>
## 2026-11-17：损坏的变长字段目录明确报错

此前，字段偏移越界、长度越界或指向行内其他区域时，查询可能成功返回空字符串或错误文本。现在读取非 NULL 变长字段时会校验完整的内联目录：目录必须完整，字段按存储顺序连续排列，不能重叠、越界或遗留未归属字节。损坏返回 `XX001`；已经带有展开标记但结构不合法的临时行也会报错。检查只读取行内目录，不加载未选中的外部大字段；只选固定字段和已由位图确定的 NULL，仍按各自读取路径处理。

实现复用同一套检查处理磁盘行的 16 位目录和大字段展开后临时行的 32 位目录。一次取值已完成检查后，外层只检查编码标记，避免再次遍历目录。现有磁盘布局、TOAST 指针、页校验和及日志格式保持原样。

验证结果：

- 完整 CMake、C++17、`-O2` 构建成功。本轮关闭 OpenSSL，与旧版使用相同编译配置；没有替换根目录程序或重启已有服务。
- 新增 `variable_row_directory_test.cpp`：普通构建和 ASan/UBSan 构建各通过 4,304 项检查，包含全部截断前缀、目录与固定区重叠、其他字段别名、零长度值、嵌入 NUL、1～64 个变长字段以及展开后超过 64 KiB 的值。插桩范围是目录组件，未对整个数据库插桩。
- 新增并注册 `variable_row_corruption_protocol_e2e_test.py`：在私有库停服后，修改真实页中的偏移或长度，并重新计算有效页校验和。5 类损坏均被查询和主键条件查询拒绝；错误后事务状态、回滚、另一张表的 NULL／空字符串、恢复原页后的读取通过，共 51 次检查、11 次正常服务退出。旧版运行同一测试失败，保留其错误成功值作为负向对照。读取另一个变长字段也会检查该行完整目录；这与读取未选字段的外部内容不同。
- PostgreSQL 18.6、旧版、新版分别记录 30、41、41 条 SQL 请求与完整响应。第二个解码器重新检查全部 112 组记录：新旧版 41/41 完整响应相同，与 PostgreSQL 的 60 组可比回复在值、NULL、列描述（排除数据库内部分配的来源表 OID）、命令标签、SQLSTATE 和事务状态上相同。覆盖 64 行窄表、30 列表、70/80 KB 长文本、更新和删除的 RETURNING、保存点与回滚，以及正常重启。
- 旧版和新版各另有一次真实 `SIGKILL`（退出 `-9`）：已提交数据在重启后保留，未提交的更新和插入未出现。其余六次对照服务正常退出。这仅验证两个终止时点，不代表所有断电、日志位置或并发恢复情况。
- 26 个选定既有 C++ 测试中 25 个通过，涉及长 TEXT/VARCHAR/BYTEA、多大字段、NULL、数组、域、REGTYPE、虚拟生成列、页引用、增删列、修改类型、分区及读取失败；`toast_test` 保留下述既有失败。7 个既有协议入口全部通过，覆盖多大字段、域、REGTYPE 及其数组、行构造、numeric 默认值重启、复合类型命名空间；加上新增的损坏页入口，共 8 个协议入口通过。首次私有原生测试脚本漏用了测试注明的链接包装参数，导致 VACUUM 测试链接失败，补齐原参数后该测试通过。完整测试套件未运行。

首次新协议测试把 BEGIN 后的期望状态误写为空闲态，5 项失败均来自这个测试断言；修正为事务态后完成上述 51 项。首次宽表对照尝试创建 33 列，旧版返回“不能超过 30 列”，故没有产生该场景的新旧性能对照；失败记录保留，后续使用当前支持的最大 30 列。这个列数限制仍未解决。TOAST 既有测试在旧版和新版均于 `toast_test.cpp:604` 的同一断言失败；未删去或放宽断言，也未将该问题标为修复。

本次仍沿用现有临时行编码标记，尚未为所有调用者引入独立的编码身份。结构合法的内容替换、固定字段或 NULL 位图损坏、全部历史数据格式、并发表结构变化和 TOAST 生命周期均不在这些检查的证明范围内。原 273 项状态和用户暂缓范围保持不变，不能称作 PostgreSQL 存储格式或整库兼容。原始页、请求回复、构建记录及专用测量脚本仅在本机被忽略的 `build/variable-directory-20261116/`；新克隆通过已提交的两个测试复现回归，不能直接读取本机记录。

实现参考：[PostgreSQL 18.6 的字段读取与偏移缓存](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/access/common/heaptuple.c)、[SQLite 的记录格式](https://www.sqlite.org/fileformat2.html#record_format)及[列读取检查](https://github.com/sqlite/sqlite/blob/master/src/vdbe.c)、[DuckDB 未压缩字符串的目录边界检查](https://github.com/duckdb/duckdb/blob/main/src/storage/compression/string_uncompressed.cpp)。PostgreSQL 按空值、变长格式及属性描述取值；SQLite 将字段类型／长度存入记录头并检查记录范围；查阅时 DuckDB 的实现对字典边界、重叠和偏移顺序报损坏异常。本项目采用目录边界检查的思路，保留自己的格式；上述源码不证明三个数据库具有相同的损坏处理策略。

<a id="native-lock-fault-review-20261108"></a>
## 2026-11-08：原生锁升级与两阶段锁交接复核

对冻结的 `LockManager` 原生代码运行既有锁升级失败测试，实际退出码为 1：866 项测试断言中 258 项失败，涉及等待时和失败后旧锁、重入深度及保存点恢复等检查。这是原生测试断言统计，不是 258 条 SQL 失败，也不代表已对 PostgreSQL 做并发对照。原生分配注入的 43 对单一共享表锁场景中，原实现的第 37 点由 alarm 终止（实际退出 `-14`），第 38、39 点退出 `3`；隔离候选没有进入产品，不能据此称问题已修复。

另一组真实进程锁抽样覆盖新取共享、排他、意向和元数据锁及部分升级：在所测部分新取锁分配失败后，测试记录普通线程和外部进程未能取得锁。外部 `flock` 结论基于夹具的 `PROCESS_FREE` 布尔结果和源码路径复核，未保留该子探针的 syscall、errno 或原始 wait 状态；不得把它扩展为全部模式或 SQL 故障结果。43 对注入与这组跨进程抽样是不同测试。

原生两阶段交接诊断在真实私有锁文件上注入一次分配失败，调用栈定位到 `suspendCurrentLocksForPrepared` 的 prepared 映射分配；原有共享锁计数从 2 变为 0，另一表锁仍在。普通 `unlockAll` 后独立进程的排他锁请求仍在约 100 ms 超时；测试显式调用 `releasePreparedLocks` 后该请求成功。这只是原生接口与进程锁结果，未执行 SQL PREPARE、WAL 或冷重启，不能由此断言数据库持久化损坏。构建、哈希、退出及私有进程组终态记录只在本机忽略的 `build/`，不随 Git 分发。

这些缺口尚未修复；需要继续验证锁升级失败和中断时的所有权、死锁处理与两阶段事务交接和恢复。既有功能账目保持开放，用户暂缓的 R51 DROP DATABASE/cache contention 等范围不变。

<a id="ordinary-scalar-effects-20261108"></a>
## 2026-11-08：普通标量参数副作用对照

最新扩展对照使用 PostgreSQL 18.6 和含页 pin 修复的冻结程序 `7d484f5a…`：84 条热态及 5 条冷重启动作中有 13 条语义差异，其中 12 条是目标参数用例，1 条是动态 DDL 重入观察。以同一未修复程序充当候选的负向验证按预期拒绝通过；独立审核重解码了 267 组请求／回复，六次私有服务记录的退出码均为 0。对非 NULL 行执行 `replace(t,nextval('arg_non_null')::text,nextval('arg_non_null')::text)` 后，下一序列值在 PostgreSQL 为 `3`、项目为 `1`。在所测事务内，函数通过动态 DDL 替换 schema 中的 `upper` 后，同一 SELECT 在 PostgreSQL 返回 `done, shadow-reentrant`，项目返回 `26000` 且事务进入错误状态；本观察不确定其内部原因，也不改变用户暂缓范围。所测 INSERT SELECT 值、回滚以及冷重启值两边一致，不能据此推断持久化错误。

扩展套件还在两个项目私有实例中暂时隐藏各自真实 TOAST 文件：所测选中列返回 `XX000`，未选该列的 `upper(t)` 返回 `ABC`；恢复后再次读出 20,480 字节值并与健康控制一致。运行时记录的文件设备号、inode、大小与哈希恢复前后一致；成功清理后文件不再保留，无法独立重新读取其物理哈希。该单一夹具不证明所有表达式都按需读取 TOAST，PostgreSQL 文件未作同类故障注入。

较早私有 PostgreSQL 18.6 与冻结程序 `70f941e0…` 的 55 条 simple-query 动作中，15 条的结果、类型 OID、SQLSTATE、命令标签或事务状态存在差异。冻结程序的二进制、源码快照与原构建收据相符；这轮测试针对当时的冻结基线。两个服务和监督进程实际退出码均为 0。独立审核重新解码了 110 组请求及回复，核对动作顺序、响应字段和记录的进程终态；这不是独立重新执行。完整帧及本地测试脚本仅在被忽略的 `build/` 中，不进入 Git。

对 `t` 为 NULL 的真实表行，`replace(t,nextval('arg_a')::text,nextval('arg_a')::text)` 在两边都返回 NULL，但紧接着读取序列时 PostgreSQL 返回 `3`、项目返回 `1`；将 `t` 放到第二、第三个参数，所测结果相同。另一真实行上，`replace(t,nextval('err_a')::text,(1/(id-1))::text)` 在 PostgreSQL 返回 `22012`，下一序列值为 `2`；项目返回 `Abc`，下一序列值为 `1`。这些结果证明上述调用路径遗漏了实测参数的副作用和错误，不能单凭结果推断内部漏算机制。将查询放入所测 `WITH` 外层后，差异仍存在；该 SQL 形式本身不能证明使用了哪条内部执行路径。

作为范围控制，所测 `VOLATILE STRICT` PL/pgSQL 双参数函数在 NULL 行调用另一个递增序列的函数时，两边下一序列值均为 `2`；因此上述差异**不能泛化为所有严格 UDF**。所测 CASE、COALESCE 和零行序列控制一致。`CASE WHEN id=0 THEN 1/0 ELSE 19 END` 在两边均报 `22012`，不能把常量错误是否被屏蔽当作一般短路结论。转义字符串、拼接和含逗号引用标识符的三个旧错误值也再次复现。PostgreSQL 18.6 的 [ExecInitFunc](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/execExpr.c) 对非常量参数生成求值步骤，再插入严格函数的 NULL 检查；实际规划可能提前化简常量。

这些缺陷尚未修复，现有功能账目状态不变；分析与内存原型不能替代真实 SQL、存储、恢复和并发验证。

<a id="active-relation-ddl-20261108"></a>
## 2026-11-08：活动查询中的表结构修改

在私有 PostgreSQL 18.6 与较早冻结程序 `70f941e0…` 的 26 条普通 SQL 对照中，所测函数在同一 SELECT 扫描目标表时执行 ALTER、TRUNCATE 或 DROP：PostgreSQL 均返回 `55006`，项目均成功；后续查询分别证实新列出现、数据被清空或表不再存在。对同表 UPDATE、未触发的 CASE 分支及修改另一张表的控制不应被统一拦截：所测两边均成功。独立审核重新解码了 52 组请求／回复并核对进程退出；此次不是对最新程序或并发、冷重启的验证，也未研究用户暂缓的 DROP DATABASE 问题。

随后针对冻结程序 `7d484f5a…` 的 100 个 SQL 动作进行对照并复核保留的 200 组请求／回复：94 项具备可比前置条件，另外 6 项因项目不支持测试所需的 `PARTITION OF` 而不可比。共 20 项响应字段存在差异，其中包括两项分区子表创建、两项 EXPLAIN 计划文本和一项除零错误的 RowDescription 差异，不能把所有差异归因于活动关系。别名、CTE、JOIN 和所测子查询内的同表 ALTER 仍表现不同；空表、LIMIT 0、未执行的 CASE、修改其他表和同表 UPDATE 等控制在所测条件下相符。含函数的主键查询在项目中的计划为 TableScan，未通过运行时追踪证明索引扫描路径已覆盖。

另有 79 个动作检查 DDL 检查顺序和多目标操作，独立解码 158 组请求／回复发现 26 项响应字段差异。例如，活动表上的重复列或缺失列 ALTER 在 PostgreSQL 返回 `55006`，项目分别返回 `42701` 或 `42703`；多表 TRUNCATE 和 DROP 遇到活动表时，PostgreSQL 拒绝操作并保留各表，项目在所测例中成功修改各表。外键依赖的 DROP RESTRICT 两边均返回 `2BP01`，该项仍计入字段差异，因为错误响应前的 RowDescription 不同。以上结果来自私有单连接测试及保留记录的复核，并非对并发 DDL、崩溃恢复或所有分区行为的验证；PostgreSQL 版本由日志和配置记录，版本查询响应帧未单独保留。

另用私有 PostgreSQL 对照活动关系的生命周期：外层 SELECT 经普通视图或派生表读取时，所测函数修改底表均返回 `55006`；独立函数内的 SELECT 已结束后，再修改该表则成功。这 11 项仅在 PostgreSQL 执行并经独立解码，不能证明项目或隔离原型已有相同行为。隔离组件的引用转移与失败清理只经手工夹具测试，尚未连接真实 SQL 调度。

独立原型观察到，同线程持有的表共享锁可以升级，ALTER 后 physicalRelationId 不变；事务回滚后声明还可能恢复成原样，简单比较声明或 ID 无法识别中间变化。原型尚未接入产品，也未证明全局类型、排序规则或 TOAST 的生命周期。PostgreSQL 的 [CheckTableNotInUse](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/tablecmds.c) 会检查当前会话的活动关系引用和待处理触发器；项目对应缺口仍未修复。

<a id="row-variable-directory-corruption-20261108"></a>
## 2026-11-08：变长字段目录损坏的私有对照

在私有项目数据库中，先写入 `(42, 'alpha', '')` 并正常关闭，再分别修改变长字段目录的偏移、长度和重叠位置，重算项目页校验和后重新启动。三种损坏页的 Fletcher-16 校验和均经独立重算有效：前两种 `SELECT payload` 成功返回非 NULL 空串，重叠位置返回 `*`；原始值是 `alpha`。恢复原页后，三种查询均重新返回 `alpha`，同一夹具的 `id` 与空字符串控制正常。隔离的按需读取草稿在这三个样例中也返回了相同错误值。

独立审核检查了保留的修改前后页、23 组 SQL 请求／回复和退出记录；六次私有修改／恢复助手及七次服务进程均正常退出。这个故障注入只针对项目自身磁盘格式的一行和三种变长目录损坏，**不涉及 PostgreSQL 磁盘格式比较**；正常校验和不能证明元组内部目录合法。当前损坏检测缺口尚未修复，也不能由此推断所有损坏类型的行为。

<a id="row-read-page-pin-20261108"></a>
## 2026-11-08：按行号读取的页帧生命周期复核

旧版 `readRowByRid` 在复制行数据前解除页帧 pin；若解除后该帧被淘汰或重新使用，指向页内数据的指针存在失效风险。本次未动态复现该失效窗口，不能称已证明发生过数据错误。旧版原生测试使用实际 16 帧、400 行和 18 个页，普通读取及可见行读取在分配失败时均出现目标页 pin 从 0 留在 1；测试显式解除遗留 pin，两个独立进程分别以退出码 1 记录缺陷。早期一次两帧配置请求未生效，实际使用了默认 256 帧；该记录保留，不作为 16 帧结果。

修复使普通读取在复制结束前保持 pin，两个读取方法均在异常退出时释放 pin。同一原生测试中，NULL 位图、普通行和可见行的三个分配失败点均保持 pin 0→0，随后重试成功；测试已加入 `tests/`。私有 PostgreSQL 18.6、基线和候选的 31 条查询／事务／冷重启动作中，基线与候选完整响应帧 31/31 相同；除两条 EXPLAIN 计划文本外，其余 29 条与 PostgreSQL 的所测结果、类型、SQLSTATE、标签和事务状态一致。检查包含 NULL、空串、索引查询、更新、删除、回滚和 20 KB 大字段；独立审核重新解码了完整帧并核验实际退出及输入哈希。成功路径已清理私有 TOAST 文件，无法事后重新读取其文件哈希。

另在三个私有实例分别提交数据、执行未提交的 UPDATE／DELETE／INSERT 后，终止自有服务并冷重启：三次被终止服务的实际等待状态均为 `-9`，重启后正常退出；已提交行、NULL 和 20 KB 大字段保留，未提交变更消失。23 条动作中基线／候选完整响应帧相同，除一条 EXPLAIN 计划文本外，另 22 条与 PostgreSQL 的所测语义一致；独立审核重新解码了全部帧。该测试只覆盖一个进程终止时点，不能代表断电或全部 WAL 崩溃位置；PostgreSQL 后台进程未逐个记录崩溃等待状态。

pin 防止缓冲帧在复制时被淘汰，并不单独提供页内容读写锁或 MVCC 可见性；并发写入、页池关闭和损坏元组的 NULL 位图校验等仍需分别验证。故障注入只覆盖所测分配点，不代表整库异常安全。此记录不改变功能账目状态。

## 2026-11-08：普通表达式按需读取隔离原型

隔离内存原型采用单次求值会话传递列读取，尚未接入产品。原生正向测试及 14 项反例测试通过；其中 3000 次求值样例的求值器长期生成列缓存条目保持为零，不能据此推断所有工作负载的内存行为。生成列绑定曾改变求值器引擎引用，成功、异常和重入三个反例先失败、修订后通过；但测试的初始引擎引用均为空。独立复核了源码与测试产物哈希、实际退出记录和补丁快照，未独立重建二进制。原型仍拒绝真实引擎和生成列函数，尚未验证真实 SQL、存储、TOAST、并发、元数据变化、sanitizer 或 PostgreSQL 对照，不代表相关缺陷已修复。隔离源码与测试材料只保存在本机忽略的 `build/`。

<a id="zero-projection-argument-audit-20261108"></a>
## 2026-11-08：零宽查询与函数参数差距复核

以 PostgreSQL 18.6 和完整 CMake 基线 `70f941e0…` 运行 52 项普通 simple-query：45 项具备可比夹具，其中 12 项语义一致、33 项有差异；另 7 项因本项目零列表 INSERT 前置失败而不可比。普通表的 `SELECT FROM` 在命令标签分别为 `SELECT 3` 和 `SELECT 2` 时没有发送对应的零字段 DataRow；PG 分别实际发送 3 行和 2 行。空零列表的 `SELECT DISTINCT *` 和尾逗号错误在 PG 为 `42601`，项目却成功。零列表的写入、撤销和恢复问题仍未修复。

私有大字段故障控制中，先确认选中 TOAST 列可读取，再暂时隐藏自有 TOAST 文件：选中列查询和不选该列的空投影均返回 `XX000`，说明当前空投影受未选字段影响。文件已恢复到相同设备、inode、大小与 SHA；没有在恢复后再次执行 SQL 读取。此测试只涉及私有目录，不证明所有列需求路径。独立审核重新解码全部请求／帧，参考 PG 与候选服务、监督进程正常退出。

普通表 49 条查询对照还发现两个错误成功值：`upper('we''ird')` 和 `length('we''ird')` 返回 `WE`、`2`，PG 返回 `WE'IRD`、`6`；`upper('prefix:' || t)` 在所测各行均返回 `PREFIX:`，未保留行值与 NULL。另两条空结果中的常量除零在 PG 报 `22012`，项目返回空结果。所测 INSERT SELECT、UPDATE 和 DEFAULT 及冷重启后的数据与 PG 一致；这不证明所有写路径均无类似缺陷。

两个隔离候选均未进入产品：单次查询语法缓存的 49 条完整协议帧与基线一致，193 个单点 C++ 分配故障检查通过，但未证明性能收益；单独修正参数切分则引入 `replace('a' || '', 'x', 'y')` 回退，PG／旧版返回 `a`，候选返回错误值。候选的首轮驱动源码后来被覆盖，原 SHA、请求、完整回复帧和退出收据仍保留；独立审核依据这些帧确认反例，不能把现有驱动冒称当时源码。语法缓存故障测试限于单线程辅助对象，不代表整库内存故障；完整测试套件未运行。原始数据只在忽略的本机 `build/`，不随 Git 分发。

<a id="numeric-log-decimal-20261108"></a>
## 2026-11-08：numeric 十进制对数验证

`ln(numeric)`、`log(numeric)`、`log10(numeric)` 和双参数 `log` 使用十进制算法，浮点重载保留浮点路径；旧版高精度默认值曾写入错误成功值。完整 CMake 单线程构建禁用 OpenSSL 以匹配本次旧版配置，实际退出 0；源码快照和 3,491 个输入哈希已核对，程序 SHA-256 `70f941e01de551dcaa0ea58586d4c8ecb5661de71f9b611c9a395564d5228b0c`。根目录默认程序未替换。

算法参考 [PostgreSQL 18.6 numeric.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/numeric.c) 的十进制对数实现。[SQLite 数学函数](https://www.sqlite.org/lang_mathfunc.html)使用近似结果并在域错误时返回 NULL；[DuckDB 对数实现](https://github.com/duckdb/duckdb/blob/v1.5.6/extension/core_functions/scalar/math/numeric.cpp)使用 double，因此二者不能作为 PostgreSQL numeric 的逐位参考。

最终程序对此前真实 PostgreSQL 18.6 捕获的 318 条 SQL 逐条重放：316 条的值和 scale、OID、SQLSTATE、命令标签与 Ready 状态一致，其中 285 条完整响应帧相同，31 条错误诊断帧不同；`ln('1e-999'::numeric)` 与 `log10('1e-999'::numeric)` 因项目 Numeric 容量限制返回 `22003`，PG 可返回结果。该轮复用已捕获的 PG 帧，并未重新启动 PG。最终程序的 54 条整数、浮点、numeric 混合重载与 PG 逐项语义字段一致，42 条完整帧相同，另 12 条错误诊断字段不同。最终程序另通过 52 项普通 SQL、DEFAULT 写入、事务回滚和冷重启检查；私有 PostgreSQL 参考测试通过 49 项，成功响应未逐条留存。

另用 v2 混合链接候选与真实 PG 对照 20 条舍入边界、8 条最高 997 位输入以及 12 条内部半位构造，完整响应分别为 20/20、8/8、12/12 一致；后二组只作正确性检查，不作性能结论。原生侧选定的 8 个测试均通过，其中数值超越函数范围入口检查 57 项；它们使用混合链接候选，格式整理前后非注释 token 相同。最终源码的除法、平方根等原语以独立整数参考重算 1,528 项，普通及 ASan/UBSan 构建各通过 1,528 项；插桩范围限于隔离原语，不代表整库 sanitizer。上述主要结果均经独立审计；完整回归套件未运行。

首次协议测试因就绪检查漏认通配地址而退出，服务实际正常退出；新增消息断言的两次测试也因项目错误文本有时附加 SQLSTATE 后缀而失败，修正测试后通过。失败记录均保留。部分现有错误传递路径在消息正文附加 SQLSTATE，PG 的消息和其他诊断字段仍不同。PostgreSQL 的长除数近似除法与本实现的精确除法在内部模型上可以不同，所测 SQL 一致不证明全输入末位一致；精度和 scale 仍各限 1000 位。零列缺陷仍未修复。数据、原始帧及审计只保存在本机忽略的 `build/`，不随 Git 分发。

<a id="d43-signature-cache-20261108"></a>
## 2026-11-08：函数签名缓存验证

D43 冻结程序 SHA-256 `f183e701bf15d8ebb7f5f5f700455ebb4da939974d60cc9c1a8f1043265c1997`，按正常 CMake 单线程构建、实际退出 0；62 个冻结对象重新链接后与候选 SHA 一致。64 项选定原生入口均实际退出 0，包含 48 线程首次函数解析、不同参数类型与本地回调隔离；完整套件未运行。D40 与 D43 的 27 组既有协议对照共 1,105 对查询；双解码及独立原始帧逐字节复核均未发现差异，96 个所记录的服务进程均正常退出。 四项端到端回归分别为 130、15、951 和 18 项，均无失败；测试助手对内部服务的末次退出/日志记录仍有限制。 64 行真实列值工作集的 7 条查询在 D40 与 D43 之间完整帧一致，独立 PostgreSQL 18.6 功能预检的 7 条查询也与 D43 完整帧一致；首次 PostgreSQL 预检因客户端不支持 trust 握手而在查询前失败，改为临时 SCRAM 后通过。

函数签名提取组件的分配失败注入中，较早 D42 草稿的 14 个失败点中有 13 个遗留分配，D43 的 18 个失败点均清理干净并可在同一进程成功重试；这是组件测试，不能表示全库 OOM 行为。D41 构建因宿主内存压力主动终止，实际退出 -15；D42 的 27 组内部收集完成但外层包装进程中止，相关记录保留，不作为 D43 的退出证据。

<a id="numeric-log-gap-20261108"></a>
## 2026-11-08：numeric 对数错误成功值（D40/D43 历史）

PostgreSQL 18.6 与 D40 实测：`ln('2.0000000000000000000000000000000001'::numeric)` 分别返回 `0.6931471805599453094172321214581766` 和 `0.6931471805599453`。D40 将其作为 DEFAULT 提交后，错误结果在冷重启后仍存在；`ln`、`log`、`log10` 和双参数 `log` 的高精度输入均有差距。测试及压缩证据（可选本地文件 `benchmarks/numeric-log-gap-evidence-20261108.tar.gz`）包含 PostgreSQL／D40 原始帧、事务和冷重启，以及首次鉴权失败和因内存压力终止 profiling 的记录。

独立 C++ 十进制草稿的 293 项测试中 289 项与 PG 一致、4 项触及项目输入或输出的 1000 位限制；110 项特殊值及边界对照一致。另 112 项半位与进位样本的 C++ 草稿重放实际退出 0，均与捕获的 PG 帧一致；这仍是组件级有限样本，组件二进制与草稿源码的链接关系未独立重建。草稿尚未完成所有舍入、错误映射、数据库协议、持久化和性能验证，**尚未进入生产代码**；PG 近似除法的末位边界仍需审计。D42 与 D43 各自和真实 PG 的 20 项动作复测确认该 DEFAULT 错误跨冷重启保留；驱动均实际退出 0，两轮各自的两个候选服务均 wait 0，PG 分别正常停止。

<a id="zero-column-gap-20261108"></a>
## 2026-11-08：零列查询与零列表的未解决差距

针对 PostgreSQL 18.6 和 D40 的零列生命周期对照中，`SELECT FROM generate_series(1,3)` 在 PostgreSQL 返回三条零字段 DataRow，D40 虽报告 `SELECT 3` 却未发送这些行。对合法的 `CREATE TABLE t()`，D40 的 `INSERT ... DEFAULT VALUES` 失败，`COUNT(*)`／`DELETE` 又把零列表当成不存在。43 项热态、事务和冷重启对照及 11 项初始查询、6 项 UPDATE 控制的原始结果保存在压缩证据（可选本地文件 `benchmarks/zero-column-gap-evidence-20261108.tar.gz`）。

隔离草稿仅证明某些空元组可以写入、扫描和删除；插入回滚曾失败，缺列数据损坏检查、元组身份复用、SAVEPOINT、崩溃恢复及完整 SQL 尚未验证。零宽投影的 DISTINCT 顺序和读取未选中 TOAST 列也存在待验证风险。为避免数据错误，草稿未进入产品；D42 与 D43 各自和真实 PG 的 15 项补充复核确认相同主要缺口，双方服务实际 wait 均为 0；两次因测试 socket 路径和 PG 尚未 ready 而失败的启动也保留，未执行 SQL。不能把局部存储通过当作零列表支持完成。

<a id="d40-numeric-exp-20261108"></a>
## 2026-11-08：numeric 指数与浮点重载验证

D40 冻结程序 SHA-256 `25dad0eef51b8af34e628ead2a9633696cf6847ba026a66954ba3b1c1d53fc7e`。62 个冻结对象依 CMake 的链接顺序重新链接，生成程序 SHA 与候选一致。实际运行的 PostgreSQL 18.6 简单查询文本协议矩阵共 317 条：293 条可表示结果的数值、scale 和类型一致，23 条因本项目 1000 位限制返回 `22003`，1 条两端均为 `22003`；非预期差异 0。原始帧分别以两个解码器核验，服务返回码由 `Popen.wait()` 获得且为 0；该采集未保留服务日志或进程组记录，驱动和独立审计的外层 wait 收据也未保存，历史审计中的 `driver_exit` 字段是硬编码，不作为退出码证据。

浮点签名与持久化的独立协议对照包含 272、18、36、15 项动作，检查数据、SQLSTATE、命令标签、事务状态和类型描述，结果与 PG 一致；四项端到端回归分别为 float 重载 130/130、float 默认值重启 15/15、既有 float8 签名 951/951、numeric exp 默认值重启 18/18，覆盖 `DEFAULT`、回滚和冷重启。端到端控制器记录测试子进程退出，部分内部服务输出由旧测试助手丢弃；独立协议对照另行保存服务日志、退出码和原始帧。高精度组件的 1,018 条随机样本中，994 条可表示结果与 PG 文本及 scale 一致，24 条因项目表示限制返回范围错误；组件级 ASan/UBSan 测试通过，不代表整库插桩。 317 项 numeric exp 的原始帧、1,018 个组件样本及 sanitizer 证据经内容去重后保存在压缩证据包（可选本地文件 `benchmarks/numeric-exp-evidence-20261108.tar.gz`），约 0.94 MiB；解压后运行 `python3 verify_bundle.py`。

27 组既有协议族对照包含 D39 与 D40 各 1,105 条配对简单查询，双解码独立审计未发现变化。63 项选定原生入口全部实际退出 0，涵盖 61 项既有入口及新增的浮点重载所有权和 decimal exp 组件测试；完整测试套件未运行。 本轮协议帧、原生测试终端记录及性能收据另存于附加压缩证据包（可选本地文件 `benchmarks/numeric-exp-validation-performance-20261108.tar.gz`），约 2.41 MiB；包内说明其对既有归档和指定 Git 历史对象的依赖与校验方式。

这些测试是有限样本，numeric 精度/scale 上限仍为 1000；例如 PostgreSQL 可计算 `exp(2303::numeric)`，本项目返回 `22003`。D40 的 numeric `ln`／`log` 高精度计算存在差距，当前状态见本报告首节。D39 曾将一个包含 251 个 base-10000 数字组的合法 binary Numeric 输入拒绝为 `22P03`；D40 尚未重新验证该例，该路径与暂缓的协议范围有交叉。

<a id="d39-numeric-20261108"></a>
## 2026-11-08：定点格式、除法和超越函数验证

D39 冻结程序 SHA-256 `3bf6a4eabb9d6ee0a2a3236ec80d69f1a455ad7af7924ba6a5e30e16fd8e31df`，正常构建实际退出 0。61 个选定原生入口和独立审计实际 0；新增 `numeric_division_inplace_test`、`numeric_tochar_fixed_exact_test`、`numeric_transcendental_range_test`，另按真实新语义调整一项既有默认值测试。完整测试套件未运行。

定点格式的 PG18.6／D37／D38／D39 四方对照每版本 1,365 个动作：1,286 个直接格式、51 个既有默认值事务和 28 个 FMPL 写入／回滚／冷重启动作。D38 在 FMPL 等符号边界曾真实产生错误成功值并持久化；D39 的 28 项目标语义与 PG 一致，修正扩展矩阵中 D38 的 11 项错误。51 项中仍有 6 项既有 PG 差异，复杂模板并非全面兼容；历史 D38 结果和原生失败记录均保留，未被新结果覆盖。

超越函数的 8 组 246 项候选对照、32 项额外类型／默认值对照及 5,997 次直接 evaluator 回放已完成。这 5,997 项回放是 1,999 个 10 的整数次幂分别测试 `log`、`log10` 和二参数 `log`；其中 5,991 项可表示结果的整数值与 PG scale 一致，6 项因当前精度限制明确报错；不等于通用精度兼容。早期经 ExprHelper 绑定的回放真实退出 1，376 项出现 `55P03`，原因尚未归因；随后直接 evaluator 回放范围不同，不能替代那次失败。新增旧版／原型中的无穷符号及模板反例和真实失败均保留。

**D37／D39 历史数据正确性问题：** D37 和 D39 均会对部分合法输入产生错误成功值，例如 `exp(100::numeric)` 与 PG 的数值不同，部分 integer／float8 的极小 `exp` 错误地变为零；这些结果可经 DEFAULT 在热态和冷重启后持久化。部分 numeric 输入已改为明确 `22003`，但 PG 可计算非零结果；不能称为已完整修复。PG 的实际数值范围见[数值文档](https://www.postgresql.org/docs/18/datatype-numeric.html)，格式规则见[格式函数](https://www.postgresql.org/docs/18/functions-formatting.html)。

除法原生旧／新对照及独立 Fraction 样本包含长位数、符号、scale、NaN、无穷和零；正式组件回归 10,045 项通过，Numeric 组件完整 ASan／UBSan 插桩通过，不代表整个数据库经插桩。D37→D39 的 27 个既有协议族独立双解码无未解释变化；不代表全部 SQL／客户端兼容。

本轮压缩证据（可选本地文件 `benchmarks/numeric-func-division-evidence-20261108.tar.gz`）为 17,457,096 字节，SHA-256 `e38a030b4f165f530d1807942790652be18b135635542a31deaefc6f18c71b01`；按内容摘要去重，保留真实回复、失败、源码和测量，不含数据库镜像或 ELF。恢复引用内容需要此前 D34（可选本地文件 `benchmarks/numeric-attribution-evidence-20261108.json.gz`）、D35（可选本地文件 `benchmarks/numeric-tochar-performance-increment-20261108.json.gz`） 和 D37（可选本地文件 `benchmarks/numeric-catalog-evidence-20261108.json.gz`） 三档。三个来源清单未保存历史文件模式，本包只记录生成时的模式；一份旧清单误带且字节已变的无关脚本按包内说明剔除，现行脚本另存。

在仓库根目录校验证据（默认只读，不恢复文件）：

```sh
verify_script=$(mktemp)
tar -xOzf docs/benchmarks/numeric-func-division-evidence-20261108.tar.gz blobs/7f520b3a4e59427d64bcf529a4717f0554793c48d3f40a603f7b8371cba29fb2 > "$verify_script"
python3 "$verify_script" docs/benchmarks/numeric-func-division-evidence-20261108.tar.gz --repo-root .
rm -- "$verify_script"
```



<a id="d37-numeric-catalog-20261108"></a>
## 2026-11-08：NaN、科学数字格式和目录扫描验证

D37 冻结程序 SHA-256 `7b57e3acdfd9b3f3cce8bfbb2ccccdbfa1943ad43c3c9dbd81e09854f8ca0a4f`，正常 CMake 构建实际 0。58 个选定标准原生入口及独立审核实际 0；新 `catalog_publication_scan_test`／`numeric_tochar_scientific_exact_test`／`numeric_div_nan_precedence_test` 分别通过 34／35／25 项。D36 的 56 个选定入口也实际 0；D35 上新增 NaN 回归曾真实退出 1，25 项中 4 项失败；科学格式回归曾真实退出 1，35 项中 23 项失败。未运行完整测试套件。

`div` 专项在 PG18.6、D35、D36、D37 上各执行 82 个 simple-query 动作，D37 与 PG 的行、SQLSTATE、命令标签及 Ready 状态一致；D35 的 9 个差异动作包括 2 个 NaN／±0 与 7 个默认值／事务／冷态派生结果。表行描述存在 10 项原始 T7 差异，不是全协议等价。另有 11 项失败事务／保存点控制以及 12 项有效格式的扩展协议与二进制数值控制；NaN 的 numeric OID1700、格式1和载荷与 PG 一致，3 项已存在的除零错误描述／帧仍不同，不推断其内部阶段。

简单科学格式按类型选择精确十进制：numeric、decimal、bigint／int8 使用精确舍入，int2／int4 和浮点保持原路径。PG、D35、D36、D37 各 210 项简单模板查询中，D37 与 PG 的行、状态、标签、Ready、T7 均一致；相对 D36 有 151 项行为变化。另有各 47 项默认值、UPDATE、批量插入、ADD 回填、保存点和冷重启控制，D37 目标场景匹配 PG；4 项非目标复杂模板仍有 PG 差异，表描述仍有 17 项原始 T7 差异。G1–G7 的额外研究以及 D35 旧失败均保留。

目录扫描只在 Linux 的 `O_PATH` 可用时使用单次打开目录、逐文件 `fstatat(..., AT_SYMLINK_NOFOLLOW)` 与结束前路径身份核对；打开或 `fstat` 失败回退旧扫描。D35 与 D37 原生控制分别 34／34，D37 的定向扫描切换控制 40／40；涵盖跨进程文件发布、dirty 冲突、缺失／软链／目录替换、可遍历但不可读目录和描述符耗尽。初始原型在描述符耗尽时真实失败，回退方案修正后通过。一次扫描过程中换出又换回目录，以及某文件被检查为不存在后才出现，仍可能本轮未察觉，后续扫描才发现；它不是线性一致的证明。两服务同时打开同一隔离库的尝试被原有独占锁拒绝，单服务三连接、DDL提交／回滚及冷重启对照共34份回复一致。

27 个既有协议族的 D36→D37 1,105 对回复逐动作核对，无未解释变化；记录不等于所有客户端和 SQL 路径均兼容。1000 位内部上限、普通 `to_char` 的超长定点模板、复杂科学模板以及无结构 DEFAULT 回退仍是差距。数值范围参照 [PostgreSQL 18 数值文档](https://www.postgresql.org/docs/18/datatype-numeric.html)，格式语义参照 [PostgreSQL 18 格式函数](https://www.postgresql.org/docs/18/functions-formatting.html)。

本轮压缩证据（可选本地文件 `benchmarks/numeric-catalog-evidence-20261108.json.gz`）为 12,424,374 字节，SHA-256 `788ec9321d87e402eedc569ebed4fc59dbe495a0aeaea1716660ff8bf65eea11`；包含专项原始帧、失败、源码差异、性能与审计，不含数据库镜像和 ELF。部分内容按摘要引用此前 D34、D35 两档，完整恢复需同时保留 D34 基准（可选本地文件 `benchmarks/numeric-attribution-evidence-20261108.json.gz`）和 D35 增量（可选本地文件 `benchmarks/numeric-tochar-performance-increment-20261108.json.gz`）。



<a id="d35-numeric-tochar-20261108"></a>
## 2026-11-08：数值范围、余数和 to_char 默认值验证

D35 候选 SHA-256 `057eec81c559a80e0d416a84a578bdedfeab5c849b0bf7866b91c9f01a7b3006`，正常 CMake 构建和外层协调进程实际退出 0。55 个选定标准原生入口、一个独立二进制入口实际退出 0；包括新 `numeric_tochar_default_range_error_test` 的 13 项、`numeric_div_range_error_test` 的 89 项和 `numeric_arithmetic_range_error_test` 的 55 项检查。未运行完整套件。

11 组协议计划包含原九组每版本 232 个 SQL 动作，外加 `to_char` 14／35 动作；对照、原始回复和独立双解码审计均完成。下表的差异是测试动作数，不是独立缺陷数；多条查询可能因同一根本原因而不同。

| 协议组 | 每版本 SQL 动作数 | D35 相对 PG 的差异动作数 |
|---|---:|---:|
| 数值范围与 DEFAULT | 51 | 28 |
| ADD COLUMN 原子性 | 21 | 4 |
| div/mod 默认值 | 14 | 5 |
| 条件求值 | 25 | 8 |
| 条件求值 EXPLAIN | 7 | 5 |
| 截断商与余数边界 | 58 | 6 |
| 合法 1000／1001 位 | 15 | 5 |
| 带符号余数与事务 | 21 | 0 |
| NaN、无穷和零 | 20 | 1 |
| to_char 原始专项 | 14 | 9 |
| to_char 扩展专项 | 35 | 21 |

D30 与 D35 的 26 项既有 numeric 入口各自构建及运行，52 次程序实际退出均为 0，独立审计完成。原生 DEFAULT 的 11 类对照仅 `to_char` 范围错误由写入原文改为 `22003`；已测的 `(`、`[]`、`[1,2)` 和 `[one, two` 等旧式字面量字节保持。

原九组 D35→D34 动作无变化；原 27 族对照完整 T7、错误与通知无未解释变化。`to_char` 两组 D35→D34 分别有 9／21 项预期变化，包含事务状态和后续读取的派生变化。D34 对 `to_char('1e999'::numeric,'9.9')` 的默认值插入及 ADD 回填会成功保存 SQL 源文，热态、保存点和冷重启复现；新增原生测试在 D34 真实退出 1，13 项中 6 项失败。D35 对已测输入返回 `22003`，直接查询、INSERT／UPDATE DEFAULT、多行插入、失败事务与保存点均核对；ALTER ADD 失败后列未发布、两条旧行不变，冷重启仍保持。PG 对此输入返回 ` #.#`，并非等价行为。

原九组仍有既有差异：项目可能先在输入或中间计算触及 1000 位限制，令后续错误优先级、ADD 列存在与否及查询结果和 PG 不同；EXPLAIN 的 5 项差异为计划输出比较，不能推断运行阶段或性能。合法 1000 位 `div`、`mod`、`%` 的选定结果和冷热 DEFAULT 现可与 PG 对齐；1001 位 `div` PG 成功、D35 返回 `22003`。`div(NaN, 0)` PG 为 `NaN`，D35 为 `22012`。

数值解析存在其他吞掉范围异常或无结构失败回退路径；本轮不声称所有 numeric 函数或所有默认值安全。对 native 中恰似越界 SQL 表达式的旧原文字面量，处理结果会从写入原文变为报错。普通和 SERIALIZABLE 活动事务退出、恢复与 SSI 仅为选定控制；ASan 为运行时库拦截，不是全核心插桩。

D31 原始启动器遗漏测试曾失败，单项补测不代替原始失败；D32 的大商测试曾真实失败；D33 的大数余数／默认值错误及 D34 的 `to_char` 原文写入均保留。D34 自包含压缩证据为 numeric-attribution-evidence-20261108.json.gz（可选本地文件 `benchmarks/numeric-attribution-evidence-20261108.json.gz`），19,503,240 字节，SHA-256 `f58db3aec6bcbb5a0da4309bc8c99010c4b87a444153d715c989fe0d44ef0828`；包含真实协议帧、失败和源差异，不含 ELF／数据库镜像。D35 结果、正式性能窗口和后续回归另存于增量证据（可选本地文件 `benchmarks/numeric-tochar-performance-increment-20261108.json.gz`），6,771,752 字节，SHA-256 `6b8fa6d5369bc5ec130af882955de5696b2db6ffb98d0be6800dd4558a3154c5`；其中相同内容按 SHA 引用前述 D34 档，完整恢复需要两档。为控制体积，3,822 条旧版已通过测试的逐项日志／fixture 明细未入包，汇总与精确省略清单保留；首次过大归档尝试真实失败也有记录。



## D30 目录投影及事务退出验证（2026-11-08；full suite=false）

本轮对普通 `pg_attrdef`／`pg_depend` SELECT 使用实际目录字段的按需快照，声明与数据在同一把锁下复制；公共完整快照仍保留默认表达式和依赖。`pg_attrdef.adbin` 的 PostgreSQL 节点树仍未实现，不声称目录完整等价。活动事务与 SSI 登记改为进程寿命，避免全局数据库对象退出时访问已析构的登记容器。

D30 实际通过 52 个选定标准原生入口及 1 个独立二进制入口，合计 53；23 组协议与 D28 的 994 条成对回复在行、tag、SQLSTATE、Ready、T7 和错误／通知上相同。针对投影的 PG／D27／D28／D30 热冷对照共 76 条回复，按实际 OID 对应后选定行、tag、SQLSTATE、Ready 无差异，但 PG 原始行描述符 T7 仍有 4 项差异。完整发现审计的 2,023 条回复包含历史和参照，不能说成 2,023 条新测试；未运行完整测试套件。

D28 新投影测试因误以为 ALTER 后目录行顺序不变而断言失败，原进程真实退出 `-11`；隔离 ASan 运行时拦截重放实际退出 `-6`（未强杀），报告活动事务登记容器在退出时二次释放。D29 修正测试的列身份断言，但未修生产退出问题；D30 对普通及 SERIALIZABLE 活动事务正常退出、重启后未提交行不可见／已提交前缀保留、后续写入提交作了实际控制，普通和 ASan runtime-only 均实际退出 0。后者仅加载 ASan 运行时库，不是全核心插桩；记录保留原有失败。

已知缺陷仍未修复：合法默认表达式 `(('1e500'::numeric * '1e500'::numeric)::text)` 在 PostgreSQL 18.6 得到 1001 位结果，而 D27／D28／D30 会把 SQL 源文写入 TEXT；现有无约束 numeric、持久默认值来源和无 SQLSTATE 回退均需继续修复。并发对照中 PostgreSQL 可在另一会话 DDL 未提交时立即读取旧目录，D27／D28／D30 的同类查询等待超过 5 秒，ROLLBACK 后才返回；未采集线程等待栈，库级锁路径只是依据代码和时间顺序的推断。

D28／D30 的 SSI 三个同源入口各实际退出 0，6 份输出及退出状态一致；这是选定对照，不证明全部并发语义。两份正式协议脚本实际退出 0。完整恢复、异常、协议和性能记录见[性能报告](performance-report-2026-10-30.md#d30-catalog-transaction-evidence-20261108)。


## D27 DEFAULT 运行时错误与快照验证（2026-11-08；full suite=false）

本轮修正 INSERT 遗漏列 DEFAULT 求值中已有 SQLSTATE 或有效结尾 SQLSTATE 标记的错误传播，并对 substring 负长度保留 `22011`；目录声明读取按需省去 DEFAULT 对象图复制，公共默认快照仍完整。没有 SQLSTATE 且没有有效标记的旧/native 求值失败仍可退回原始字面量；域、旧数据及全部错误类型不构成全面兼容承诺。

D24 的 `catalog_default_snapshot_demand_test` 实际退出 `-11`，诊断指向进程退出时内置目录声明生命周期问题；D27 的 ASan 只注入运行时库而非全核心插桩，该控制正常退出 0，不能称为完整 ASan 构建。D26 的 `regexp_replace('abc','[','z')` DEFAULT 在热态和冷重启后均错误写入表达式文本；D27 返回 PostgreSQL 的 `2201B` 且未写入行。历史失败记录保留。

最终 D27 49 个常规原生入口加 1 个二进制控制，共 50 个实际退出 0；22 个协议族独立审计覆盖 1,998 SQL、76 个正常结束 Main。五族 DEFAULT 错误独立对照记录 342 SQL、48 个正常结束 Main，所比较语义字段没有候选差异；两个族的原始行描述符 T7 仍有差异，不能称为完整 wire 或 PostgreSQL 等价。未运行完整测试套件，功能仍为未接受草稿。

五族的 D23/D24 标记错误、substring 类型、无结构错误没有捕获 trace，归档明确记作 `NO_TRACE_CAPTURED`，不能推断运行或通过。测试涉及真实插入、事务/保存点、失败后的可见行和重启；各边界以压缩包内原始脚本、请求、响应、退出回执为准。性能另见[完整报告](performance-report-2026-10-30.md#default-runtime-snapshot-evidence-20261108)，不将个别样本推及整体。


## 最新 P43/D23 草稿验证（2026-11-07；full suite=false）

46个选定原生入口实际0；新回归目录36、绑定26、生命周期61项均通过，包括真实DDL／DML、所属表保留、失败TEMP创建清理、SERIAL默认依赖、savepoint／外层回滚及冷目录读。身份35条、目录9条正式协议脚本通过。

主50条对照取得选定语义闭环，扩展32条保留两处差异；15族既有模型和所有原失败继续记录。新审计核对214份SQL回复观察（含先前D20主对照50份）、10个正常Main及4534个闭库文件，实际OID和完整T7差异单列。真实旧数组控制61条观察、594行／4018单元，21条操作链的全T7在真实表OID对应后相同。新系统目录增加的21条本地声明依赖独立核对，旧依赖完整保留；未宣称该系统图与PG相同。全套未运行、性能NOTRUN。[完整证据与边界](performance-report-2026-10-30.md#attribute-default-objects-draft-20261107)。


<!-- P43-D20 unaccepted draft checkpoint -->
## 最新 P43/D20 草稿验证（2026-11-07；full suite=false）

42个选定原生入口全部actual0。新增数组回归的56项及新进程首次启动通过，覆盖完整预检、已发布冲突拒绝、存储／排序规则和typmod保留、已有依赖及逻辑类型声明。身份35条、目录9条正式协议脚本实际0。

真实闭库D16映像经完整字节／模式／SHA核对后升级；专项共61条SQL观察、594行／4018单元／6NULL、41398请求回复字节和9个正常Main，两个独立解码器核对。旧库21条完整操作链在实际表OID对应后全7字段T7及值／状态／tag／Ready与PG一致；原D19在第10条断连的93B不完整回复原样保留，不补造Ready。宽范围15族对照仍保留既有差异，不能称为全部PG兼容。详见[本轮报告](performance-report-2026-10-30.md#physical-array-catalog-upgrade-draft-20261107)；性能NOTRUN。


<!-- P43-D18 unaccepted draft checkpoint -->
## 最新 P43/D18 草稿验证（2026-11-07；full suite=false）

39个不同原生入口（38个常规入口加 REGCLASS 二进制回归）均实际退出0；新增目录排序规则测试60项、类型物理属性测试50项均通过。两份正式简单查询脚本分别验证35条身份冲突和9条目录协议用例，均实际0。已登记686个唯一驱动／666份独立E2E，库存检查通过；这不是全套686项或全部C++测试通过的声明。

主对照保留1701条 PG／D18 SQL观察、48个正常Main、1979行／11185单元／208NULL及411247请求回复字节。专项 PG／旧D17／新D18／真实旧库升级另有67条SQL观察和1条版本控制；当前17条及升级后16条的完整7字段T7／值／状态／tag／Ready匹配实际PG参照，旧D17的10处差异（含2次CTE错误）保留。宽范围对照仍有未映射真实OID和实际功能差异，不称全部PG模型相同。

测试文件现2422份：原2405份中2399份逐字节不变，6份按真实PG契约调整（身份START10预期、4份旧冷目录夹具、34普通字段与6系统字段的分别检查），原失败记录保留；新增14份CPP、2份Python、1份头文件。P43/D14编译失败、D15错误候选、D16／D17夹具失败及审计器首轮假设错误均保留，未改写为成功。完整范围和数据见[草稿报告](performance-report-2026-10-30.md#identity-sequence-catalog-draft-20261107)与压缩证据（可选本地文件 `benchmarks/identity-sequence-catalog-draft-20261107.json.gz`）。


## P42 最新有限验证（2026-11-07）

新正式`read_transaction_publication_test.cpp`：旧版38项中15个新增优化要求失败、冷开数据成功；C1的38项全部成功。独立核对两版16份完整原生观测与55条WAL记录。另32个既有原生入口旧／新各30个0，TOAST在604行断言-6、WAL追加故障测试300s未完成并由本任务执行器终止-15，两版全部保留且不计通过。加新回归后C1的33个不同入口为31个0、2项未通过，full suite=false。

实际PG18.6／旧／新各95条SQL完整模型通过，旧新95份请求回复逐字节相同；六份产品闭库WAL共475条CRC／prev链核对0。TEMP提交动作、序列、事务DDL、读后写、失败事务、RR并发与崩溃后完整值／NULL均保留严格检查。性能前还有10个不计时场景、366条完整SQL、20个Main0通过。621次新正常成本及全部135行比较在[报告](performance-report-2026-10-30.md#no-effect-transaction-publication-20261107)，任何负向不排除、不重采。

原2404测试字节不变、新增1后2405（顶层CPP1138／全部CPP1143）。源251、Markdown754及725保护记录保持；原273状态及291条历史不变，只追加partial第292条，Goal active。

<!-- P41 large-varchar-toast-search begin -->
<a id="large-varchar-toast-search-current"></a>
## 2026-11-07：长VARCHAR全值、冷开及性能验证

两个新正式原生测试验证完整数据。large_varchar_toast_test在C6三种模式各setup／cold1／cold2，共9个新进程子阶段全部0，覆盖更新返回完整行、回滚、越界原子拒绝、删除、VACUUM／FULL、提交后DML、TRUNCATE、索引RID与NULL位图。独立解码C6的29份完整观测及旧版2份成功部分观测，共31份／140行／420单元／106 NULL／11,200,242字节；旧版三种setup为1、六次冷开NOTRUN，属于不支持准入，未写成数据损坏。新TOAST布局测试在旧bf9与C6均0，两版各2个实际新子进程，180个数据页／544个存活分块的全部二进制值、头、Fletcher-16、页槽RID与朴素串行first-fit模型相等，短末块复用早期空洞，删除及冷开正确。其原始二进制API输入包含NUL，不能理解为SQL TEXT允许NUL。

相关48个原有正式测试按当前要求：46个actual0；toast_test仍在604行 !corruptScan.open() 断言actual -6、后续NOTRUN；declared_type_cold_binding_test旧bf9与C6都actual1，12项中2个初始catalog条件失败，保留OPEN。全套未运行，full suite=false。原type_registry_test把65536声明当错误，旧源码在C6实际-6的首次观察保留；依据已冻结PG声明范围，扩充为65536／10485760成功及10485761／非法修饰符拒绝，更新源码在旧版-6、C6为0。没有删除负向或改掉其余要求。加上两个新测试，共50个不同正式测试，当前48个0和上述两个失败；另外的历史原断言失败单列保存。旧26项对照来自相同bf9正常核心／源码的P40实测，另外22项旧版在本批真实重跑；70个原源码终态及全库文件／模式独立核对。

新141条VARCHAR SQL在独立PG18.6、旧bf9、候选C6均完整执行，三角色共423条核心SQL、305行／1249单元／132 NULL、228,538,937字节；10个正常Main0及2个预期SIGKILL -9。全值、顺序、重复数、NULL、状态／tag／Ready和全部七个描述字段与实际PG核对，关系OID只用各自真实探针映射。C6普通数据要求通过，仍有17个pg_class来源描述差异，以及3个大常量ADD DEFAULT的PG实现限制差异：PG对70000字节常量报54000，随后读取／DROP列报42703，项目接受并正确存取；这些差异保持开放，strict PG=false。137条V1参考模型的4个假设被真实PG否定，首次actual1及全部原始数据保留；141条V2在任何候选执行前冻结，另加4097字节默认值成功控制，没有按候选结果降低要求。

原72条TEXT／VARCHAR SQL另用本批新PG参照核对，三角色216条核心SQL、600行／2292单元／96 NULL、96,178,911字节；7个正常Main0与2个预期-9。原3个VARCHAR声明／写入差距修复，4个catalog来源描述差距仍在。22／62／72／28及wide OID控制共185条新完整请求，2588行／11613单元／969 NULL、45,817,003字节、8个正常Main0与1个预期-9。相对P40只有上述3条VARCHAR成功结果改变，其余请求回复逐字节一致；62条原9项与28条原3项PG差距保留。以上计数按各队列给出，72条复用于185桥接，没有重复计算成新的SQL执行。

六种既有FULL专属有限故障计划在C6均通过，保留同一真实透传钩子字节：120条完整SQL、4条中断FULL、10400行／48966单元／3696 NULL、64,393,755字节、4个预期-9与8个正常Main0、6285映像文件。6087条／47,972,984字节WAL的CRC32C／prev链及COMMIT／ABORT／未提交边界独立核对。它们没有重开用户已暂缓的通用IO／分配器／保存点矩阵。

所有正常窗口关闭后，项目DUMP扩展补测15条完整SQL、3个正常Main0：有界／无界VARCHAR及两类数组的真实逻辑导出，在全新数据库恢复并再次冷开，全值／NULL／全部七个描述字段正确。只验收这组类型声明与值，不声称项目DUMP已完整保留所有PG对象／约束。

诊断不计正常成本：C2和C3在141条计划中均完成60条，第61条ALTER请求已发且60秒无回复，其后80条未发、全部冷开NOTRUN；两个Main均正常关闭0。C2／C3闭库失败映像和全wire分别保留；映像在超时后正常关闭保存，不能当作故障瞬间或零检查点崩溃证明。C3只对自己的Main允许父进程后代GDB附加，四次真实栈显示writeToast反复访问并校验缓存页，不能写成死锁证明。系统sudo非交互安装失败，GDB由下载包解到自有临时目录，未更改系统ptrace设置。C6全部141条在无调试器下完成。另一次相同原生数据的strace，两版toast.dt pread64都是364次，请求2,981,888字节、实际读1,499,136字节；没有磁盘读调用数减少的证据。新正式测试最初日志写CRC，窗口后仅修正为实际Fletcher16并重新运行两版；断言、模型、生产程序和1680个时钟未改变，原日志保留。

本轮1680次新正常计时：159个实际正常Main0、3288条完整SQL、467859行／1695258单元／121014 NULL、1,090,713,993请求回复字节、22881个闭库文件。五组采集器和五组独立归约器实际0；窗口内没有本任务编译、调试、插桩、压缩或其他SQL，自然缓存、不绑定CPU，不声称宿主机全空闲。墙钟／客户端CPU覆盖发包到ReadyI，服务器CPU为整个Main并包括后台线程。全部样本一次执行、0排除／0重采；P40原1545次观察保持关闭和原样。本轮是新的程序／时间窗口，不能替代旧负向。

负向保持OPEN：新长值DELETE RETURNING墙钟43.553088ms，+9.954%／+23.830%，服务器CPU+67.962%／+39.067%；首次VACUUM墙钟25.753918ms，+22.574%／+1.265%；FULL墙钟218.226818ms，+32.204%／+21.215%，服务器CPU+7.756%／+1.774%。新增完整值读取墙钟+2.080%／-2.014%，结果混合。通用270项矩阵的30个区域，墙钟6低于两旧／18高于两旧／6居间；服务器CPU10／8／12，客户端CPU6／14／10。没有排除、重采或把漂移归因为已证实的产品原因；全部负向与原因继续开放。

正式库存2404个文件（1137顶层／1142总C++），2401个旧测试逐字节不变、一个类型注册测试按上述范围扩充、另加两个新测试；注册E2E684／独立E2E664不变。生产251个文件，改6个cpp并加CharacterColumn.h，正常CMake C++17/O2程序C6为587f898d，旧bf9ad7ae、更早c8ca0215，61核心对象冻结；没有靠升级C++23宣称提速。默认425165af不变。754个Markdown路径／725份保护记录保留，仅已有8文档插入说明；原273项及22 complete／166 partial／70 unverified／15 deferred_by_user不变，290条旧历史原样，新增partial第291条，IDX-07仍unverified/evidence[]。全部用户暂缓范围不变。Goal active，直接master提交，未推送或替换默认程序。下方各批均为各自历史版本。 [全部225行性能及原始证据](performance-report-2026-10-30.md#large-varchar-toast-search-20261107)。
<!-- P41 large-varchar-toast-search end -->

<!-- P40 vacuum-full-relation-snapshot begin -->
<a id="vacuum-full-relation-snapshot-current"></a>
## 2026-11-07：FULL关系快照、回退与全部成本验证

新增vacuum_full_relation_snapshot_test，以完全相同源码链接旧c8ca与正常C1核心。两个模式各有256行目标表／删除后16行、12行无关表、真实脏缓存、主键／二级／hash／bloom索引、70003字节以上TEXT、UTF8、空串／字面NULL／SQL NULL。旧版父actual1：6个优化断言失败，两次setup为1、四次冷开均0；所有热态／冷态完整值和RID NULL位图已正确，不是旧版数据损坏复现。C1父actual0且6子阶段全部0：成功模式实际10份同inode／nlink≥2物理备份；故障钩子对10次硬链接实际返回EXDEV，随后完整复制回退并通过热态及两次新进程冷开。无关堆同步2→0，无关脏页保留给自身owner，之后自身flush正常完成。所有拦截实际调用真实系统同步，未跳过fsync；EXDEV是显式故障控制，不冒充真实跨文件系统部署验证。

C1本批重新执行26项相关原生测试：12项WAL／身份／出生／退休／redo／UNLOGGED／TRUNCATE、10项长字段／VACUUM／投影／视图、热态vacuum_full_test及既有两个FULL恢复测试通过，合计25项actual0。既有toast_test仍在604行 `!corruptScan.open()` 断言actual -6，后续NOTRUN；未修改预期，full suite=false。旧c8ca的26个对照终态来自P39已认证实际运行，逐项核对相同冻结源码、250生产文件、61核心对象和正常ELF后复用，没有声称本批重跑旧版26项。

既有vacuum_full_cold_recovery_test在C1的6子阶段实际为0／0／0／-9／0／0：包括一次预期强杀，不能写成6次全0。堆352256字节（43页）压到32768字节（4页），随后提交1MiB TEXT更新、插入、删除，关闭／强杀后连续两次新进程冷开全值通过。两种模式各597条／4986712字节WAL，共1194条CRC32C与prev链核对；旧代次578镜像、最大块42，新代次7镜像，0个checkpoint。既有generation测试10子阶段均0，覆盖首次堆WAL前有效空09 schema、三次FULL／重命名、空表两次FULL／后续DML；最终身份2／4／3、0x23提交边界及计数器核对通过，0个checkpoint。

原22、62、72、28条SQL及实际wide OID控制全部重新执行，共185完整请求／2588行／11613单元／969NULL／45,817,233请求回复字节，8个正常Main0和1个预期SIGKILL -9，2315个闭库文件核对。全部完整请求与回复逐字节等于P39真实c8ca观察，再由独立解码器检查完整值／NULL／全部T7／错误／NOTICE／tag／Ready。原PG18.6参照逐字节复用，未声称本批新跑PG；62条中的9项、72条中的7项、28条中的3项SQL子分区差异继续开放，strict PG=false。

六种FULL专属有限故障控制在C1均首次尝试通过：schema发布前强杀、发布后提交前强杀、COMMIT后清理前强杀、发布报错后回滚schema期间再次强杀、提交后清理失败、回滚后清理失败。共120条完整SQL与4条真实中断FULL、10400行／48966单元／3696NULL／64,393,755字节，4个预期SIGKILL -9、8个正常Main0，6285个实际映像文件，一个预期XX000。6087条／47,972,984字节WAL独立核对CRC／prev链、COMMIT／ABORT／未提交边界和代次不可复用；两种清理失败后的后续DML与显式CHECKPOINT顺序及冷开全值正确。使用P39已接受V2钩子的同一字节，仅初开进程插桩，冷开正常C1；P39原V1漏拦截remove的失败属于历史保留，不是本批重试，未覆盖原记录。

在正常计时前，使用真实fsync／fdatasync／msync透传钩子核对FULL工作量。旧c8ca固定／单长／多长各58次同步，其中8／6／6次属于无关关系；C1为50／52／52次，无关关系均0。逐路径原始日志、实际PID／线程、返回值、完整SQL与闭库映像保留。C1诊断墙钟438.800／458.549／502.184ms，明显高于旧版；诊断时可能与本任务编译重叠，明确不能作为正常成本或线程独占成本证据。只从这些诊断接受实际工作量减少，不能用减少的系统调用数代替运行时间结果。

全部正常计时1545项／129个正常Main0／3003完整SQL／467094行／1693173单元／120549NULL／911,125,308请求回复字节／19656闭库文件，四个采集器及四个独立归约器实际0。窗口没有本任务编译、压缩、插桩、剖析或其他SQL；自然缓存、不绑定CPU，不声称宿主机全空闲。墙钟和客户端CPU覆盖socket至ReadyI；服务器CPU为整个Main并包含后台线程。所有原始计时一次执行，0排除／0重采。

明显负向保持OPEN：首次普通VACUUM多长字段墙钟C1中位67.010218ms，旧c8ca首测30.951480ms／回测25.722458ms，即+116.501%／+160.512%。原三次样本分别为C1 [71.510152,26.679347,67.010218]ms、旧首测[35.992784,30.951480,25.821788]ms、旧回测[25.722458,23.927581,29.365169]ms，全部保留。单长字段大批重新插入的墙钟也+51.70%／+58.00%。未因负向重跑、排除样本或更改预期。

正常窗口全部关闭后，因上述负向另做一次有界同步诊断：同一after-DELETE种子、旧c8ca与C1各单长／多长普通VACUUM一次，共28条完整SQL、3576行／14874单元／1098NULL／8,603,834字节、2个正常Main0、652个闭库文件。四次维护各实际两次同步，均为目标.toast.dt与.toast.idx，无额外无关同步。多长诊断旧版墙钟39.305ms（同步24.028ms），C1墙钟11.593ms（同步3.656ms）；这个反向诊断结果只说明本次同步延迟不同，不证明原正常负向的原因，不替换原样本，也不用于宣称提速。诊断和独立校验均actual0，无编译重叠。

本批仍只验收未来普通关系FULL的有限重写／关闭／强杀／回滚恢复。不能修复已被旧FULL写坏且没有新pending／退休授权的旧集群；P38实际损坏库继续保留。跨FULL的PITR／物理基备未验收；数据库排他锁范围仍大于PG表级锁，未验收分区FULL冷开或高并发等价。硬链接需要文件系统支持，实际失败会走复制回退；不宣称所有平台／文件系统已验证。原15项安全／TDE及额外用户暂缓范围保持，这六种FULL故障不是已暂缓的通用IO／分配器／保存点矩阵，也未恢复执行图／child-provider或retained planning／portal工作。

正式测试库存2402个文件（1135顶层／1140总C++），2401个旧测试逐字节不变；注册E2E684／独立E2E664不变。250个生产文件只改TableManage.cpp，正常CMake C++17/O2 C1为bf9ad7ae，旧c8ca0215、更早84bbadb4；61核心对象冻结。默认425165af不变。754个Markdown路径、725份保护记录保持，只在已有8文档插入说明。原273项和22 complete／166 partial／70 unverified／15 deferred_by_user不变；289条历史保持，新增partial第290条；IDX-07仍unverified/evidence[]。Goal active，直接master提交，未推送或替换默认程序。下方各批是各自历史程序的记录。 [完整性能与归档](performance-report-2026-10-30.md#vacuum-full-relation-snapshot-20261107)。
<!-- P40 vacuum-full-relation-snapshot end -->

<!-- P39 vacuum-full-wal-generation begin -->
<a id="vacuum-full-wal-generation-current"></a>
## 2026-11-07：FULL新代次、恢复与完整性能验证

新增vacuum_full_cold_recovery_test使用同一源码对照：旧84bb两种情形均在首次冷开出现重复可见主键（父actual1，4个子阶段已执行／2失败／2后续NOTRUN）；C1为6个子阶段全部符合实际终态，正常关闭或主动SIGKILL -9后连续两次新进程冷开全值／NULL／主键通过。原堆352256字节（43页）压到32768字节（4页），之后真实UPDATE到1MiB TEXT、INSERT、DELETE并提交。两份WAL共1194条独立CRC32C核对，旧代次578个页镜像最大块42、新代次7个镜像，退休完成及提交顺序正确，0个检查点，不能归因于checkpoint绕过。

新增vacuum_full_generation_recovery_test进一步覆盖有效空09 schema在首次堆WAL前加载、三次FULL／中途重命名、空表两次FULL及后续插入。C1实际10个子阶段均0，完整值／NULL与两次冷开通过；旧版在3次新代次检查失败（父actual1，4阶段执行／3失败／6冷开NOTRUN），不把未运行阶段计为冷开复现。09 fixture保留format2堆格式，不代表所有历史堆版本已迁移。独立解码验证legacy的0x23提交边界、重复重写退休、最终身份2／4／3及不可回退计数器，0个检查点。

12项恢复相关原生测试在旧84bb与C1均本批实际0，覆盖身份codec／保护、退休完成、出生与崩溃、redo、基础WAL／全页／截断、UNLOGGED强杀／恢复和TRUNCATE恢复。C1另10项长字段／VACUUM／投影／视图相关测试实际0，既有vacuum_full_test的4组热态、分区、备份失败与中途失败恢复也实际0。既有toast_test仍在第604行 `!corruptScan.open()` 断言中止（actual -6），后续NOTRUN；保留P38同位失败，未修改预期，full suite=false。该计数为有限相关原生测试，未宣称全套通过或原生分区FULL冷开已验收。

原先因冷开退出-6中断的同一旧库22条SQL计划，现在C1完整执行22条／2130行／9813单元／738NULL／12,433,869请求回复字节，两个正常Main实际0，944个闭库文件核对。初开16条完整请求和回复与P38旧84bb原观察逐字节一致，新增完成原先未运行的6条冷开。原始旧库种子334个名字／模式／字节不变；P38原失败、被旧观察器遮蔽的旧终态及缺失阶段保持原记录。

六种FULL专属有限故障控制全部经独立完整值／NULL／全部T7／错误／tag／Ready核对：schema发布前强杀、发布后提交前强杀、COMMIT后清理前强杀、发布报错后回滚schema期间再次强杀、提交后清理失败、回滚后清理失败。共120条完整SQL、4条确实发出后被中断的FULL、10400行／48966单元／3696NULL／64,393,755字节，4个预期SIGKILL -9和8个正常Main0，6285个实际映像文件；一个预期XX000保留。6087条WAL／47,972,984字节独立CRC与prev链核对，并核实各自COMMIT／ABORT／未提交及不可复用代次。两种清理失败后继续UPDATE，再显式CHECKPOINT，检查点确实晚于该DML提交；冷开值未被旧备份覆盖。只在初开Main加载版本化syscall钩子，冷开使用未插桩的同一正常C1。

首轮测试钩子漏拦截libstdc++实际使用的libc remove：after_commit未按预期断开，采集器actual1且未启动冷开；两种cleanup_fail虽各完成24条SQL，独立故障核对均actual1。所有首轮源码、日志、计划、完整wire、实际终态及NOTRUN保留。V2只增加remove拦截，三个原计划字节不变，正常生产ELF不变，再实际完成相应故障控制；没有改成忽略故障的预期，也没有重采或筛选任何正常性能样本。

原62、72、28条SQL及一条实际wide OID控制在本批C1重新执行，共163请求、458行／1800单元／231NULL／33,383,364字节，6个正常Main0和1个预期SIGKILL -9。所有完整请求与回复均与P38实际84bb逐字节一致，独立第二解码器核对全部值、NULL、T7、错误、NOTICE、tag和Ready；复用的是已认证归档中的真实PG18.6参照，没有本批新跑PG的声明。原62的9项差异、72的7项差异及28的3项SQL子分区差异继续保留，strict PG=false。

全部正常计时共1545项／129个实际正常Main0／3003完整SQL／467094行／1693173单元／120549NULL／911,125,308请求回复字节／19656闭库文件。窗口无本任务编译、压缩、插桩、剖析或其他SQL；自然缓存、不绑定CPU，不声称宿主机全空闲。墙钟／客户端CPU覆盖socket至ReadyI；服务器CPU为整个Main并含后台线程。

本批验收只覆盖未来普通关系FULL的有限重写／关闭／强杀／回滚恢复情形。不能修复已被旧FULL写坏且没有新pending／退休授权的旧集群；P38实际失败库仍保留。跨FULL的PITR／物理基备重建未验收，不能仅凭新生命周期记录重建重写后的完整堆。内部事务暂时使用数据库排他锁，再取表锁和cache锁，并非PG表级并发等价；热态分区既有回归通过，不宣称分区冷开闭环。原始15项安全／TDE暂缓与额外用户暂缓记录保持；这六项不是已暂缓的通用IO／分配器／保存点故障矩阵，也未修改执行图／child-provider、retained planning／portal或CPU-sort interrupt。

正式库存2401个测试文件（1134顶层／1139总C++），2399个旧测试逐字节不变，注册E2E684／独立E2E664不变。源码250个文件，正常CMake C++17/O2候选C1为c8ca0215，旧84bbadb4、更早56aad0c0；默认425165af不变。754个Markdown路径和725份保护记录保持，只在8份已有文档插入本批说明。原273项和22 complete／166 partial／70 unverified／15 deferred_by_user不变，既有288条历史保持、新增partial第289条；IDX-07仍unverified/evidence[]。Goal active，直接master分批提交，未推送或替换默认程序。下方各批均为对应历史程序的记录。 [完整性能与归档](performance-report-2026-10-30.md#vacuum-full-wal-generation-20261107)。
<!-- P39 vacuum-full-wal-generation end -->

<!-- P38 scalar-text-logical-limit begin -->
<a id="scalar-text-logical-limit-current"></a>
## 2026-11-07：TEXT全值、普通恢复与1500项完整成本

新增large_text_toast_test在同一冻结源码上形成33检查：旧版21失败／actual1，当前0失败／actual0。全字节覆盖65535、65536、70003、UTF-8 90012字节／30006字符、可压缩131076字节、随机1MiB、NULL、空串、文本NULL及类似marker的普通串；同时验证第二长字段100003字节、旧式和optional更新的完整返回值、事务回滚／提交、普通VACUUM、热态VACUUM FULL、TRUNCATE后重插1MiB，以及有界VARCHAR的字符数和尾空格约束。

10项相关原生测试在旧版与当前均actual0：vacuum_toast_test、parallel_vacuum_test、vacuum_page_lock_test、vacuum_full_test、bytea_large_toast_test、ordinary_project_toast_demand_test、sorted_projection_null_bitmap_cost_test、view_column_dependency_test、vacuum_toast_physical_reference_test、alter_column_toast_generation_test。旧版前9项使用既有相同源码／对象／ELF的真实终态，第10项重新执行；当前10项均本批执行。既有toast_test两版仍在第604行 `!corruptScan.open()` 断言中止（actual -6），后续检查NOTRUN，未改预期，full suite=false。

新增72条SQL分别在真实PG18.6、旧版56aad0c0和当前程序执行：216条核心请求、507行、1935单元、80NULL、73,199,459请求／回复字节，7个Main正常0、2个主动SIGKILL终态-9。独立解码器核对完整值、NULL、全部T7、tag、Ready、错误及NOTICE，当前目标TEXT用例通过；严格PG比较仍有7项差异：4项系统目录列来源OID元数据，以及大声明／无界／多字节大VARCHAR的3项差异。初轮强杀前已删除1MiB行，其最大存活TEXT为200006字节；该组1MiB重写后的冷开来自正常关闭，不把它计作1MiB崩溃恢复证据。

实际旧版30412452创建、正常闭库并冷开验证的after-DELETE种子，334个名字／模式／字节在本批前后完全一致，固定／单长／多长字段人口241／241／113。另做不含FULL的18条普通维护控制，每角色18条，共36请求／3070行／14670单元／1110NULL／16,337,172字节，4个Main正常0；当前在旧模式中写入70003字节及1MiB TEXT后，普通VACUUM和新Main冷开全值／T7通过，未改动固定／单长字段完整wire。再以相同原始种子做独立18条崩溃控制，共36请求及相同全值工作量，2个主动SIGKILL -9、2个新Main正常0，确认提交后完整1MiB TEXT存活并读回。两项控制各有1600个闭库／崩溃映像文件核对；它们不替代下面失败的原始FULL测试。

新发现的普通VACUUM FULL／WAL恢复缺陷仍OPEN，属于下一项必须处理的非暂缓工作。原始旧模式22条矩阵，两版各完成初开16条并正常退出0；当前16条全值正确，但之后冷启动失败，两版各6条冷开SQL未运行。旧版首次观察器的cleanup断言遮蔽了已退出子进程的wait状态，因此其冷开actual exit未记录；修正观察器后只继续原先未启动的当前角色，实际冷开退出-6，未重跑或补写旧版状态。提前运行的独立审计也因缺少完整receipt实际退出1，失败保留。独立CRC32C核对定位LSN6784528的有效8192字节页镜像：cv_sig、物理身份3、旧块22；FULL后同身份的堆仅18页，重放拒绝跨越4页的缺口。CRC为0xd116b066，名义行宽0是合法format2而非本次原因。本批未修改恢复代码；需要正确的重写物理代次／恢复边界，不能靠忽略错误或无界补页关闭。实际失败快照和正常旧模式混合长值种子均保留在本地。

原62条SQL重新用旧版和当前执行，共124请求、4个Main正常0，完整请求与回复逐字节一致，真实PG18.6原始oracle和既有9项差异保持。P37原28条维护SQL计划字节不变，另在真实PG／旧版／最终C2重新执行，共84核心请求、144行、553单元、129NULL、2,873,498字节、6个Main正常0；额外4条实际OID／PG版本控制单列。原TEXT70003插入、立即读回、冷开读回3个确切缺口现与PG完整值和T7相符，其余25条旧／新完整回复字节不变。28条中24项值／状态／tag匹配、1项按各库真实正OID核对，仍有SQL子分区创建0A000、子表目录身份及冷开身份缺行3项差异，strict PG=false。

正式1500计时／114正常Main，全部162项比较、完整raw20/raw3、首轮、长尾及负向保留。源码、进程终态、原始wire和闭库文件可按SHA重建，C2与实际功能测试C1的61对象／完整ELF逐字节相同。

正式库存2399个测试文件（1132顶层／1137总C++），注册E2E684／独立E2E664不变。754个Markdown路径、725份保护记录保持，8份既有文档只插入本批说明。原273项、22 complete／166 partial／70 unverified／15 deferred_by_user不变，既有287条历史保持并新增partial第288条，IDX-07仍unverified/evidence[]。 [完整性能与压缩证据](performance-report-2026-10-30.md#scalar-text-logical-limit-20261107)。
<!-- P38 scalar-text-logical-limit end -->

<!-- P37 vacuum-physical-references begin -->
<a id="vacuum-physical-references-current"></a>
## 2026-11-07：物理TOAST引用、完整协议与1500项实际成本

新增 vacuum_toast_physical_reference_test 在同一冻结源码上执行全部16种情形：旧版160检查／48失败／actual1，C1为160／0／actual0。覆盖普通表与原生范围分区存储、vacuumToast及外层vacuum、真实同库／跨库同名表调用者、跨位图字节的NULL、空串、文本NULL、UTF-8、类似marker的普通字符串、TEXT11003／40003及BYTEA70003／11009。独立完整值物理解码核对16种情形中的80个存活对象全部原始字节，以及每个真实孤儿的回收、调用者绑定恢复和第二次维护幂等。

独立oracle使用实际DataFileHeader及校验、PageAllocator、PageWrapper和既有完整值解码器；不复用新引用收集算法，也不使用前置失败的逻辑分区读取来判定通过。物理长字段NULL/空串/二进制/全长值及真实调用者绑定均核对。

8项相关原生测试在旧版和C1均actual0：vacuum_toast_test、parallel_vacuum_test、vacuum_page_lock_test、vacuum_full_test、bytea_large_toast_test、ordinary_project_toast_demand_test、sorted_projection_null_bitmap_cost_test、view_column_dependency_test。其中旧版后3项复用相同源码／冻结对象／ELF的既有正常终态，其余均本批实际执行。既有toast_test在旧版与C1仍于第604行 `!corruptScan.open()` 断言中止（actual -6），保留失败，后续断言未运行；未修改测试预期，full suite=false。

原62条SQL在旧版30412452和C1全部执行，4个初开／冷开Main正常0，完整请求／回复逐字节相同，沿用并核对真实PostgreSQL18.6原始oracle，既有9条协议差异仍在。另以真实PG18.6、旧版和C1各执行28条新维护SQL及冷开读取，共84请求／142行／549单元／129NULL／2733516字节，6个Main正常0；第二套解码器核对全值、NULL、完整T7、tag、Ready和原始错误／NOTICE。28条中旧／新完整wire一致；21项值／状态／tag与PG相符，1项用各库真实正OID分别核对，另6项PG差异保留，strict PG=false。

本批实际PG缺口包括：TEXT70003字符INSERT在产品返回22023而PG成功，立即读取与冷开读取缺该行；SQL `CREATE TABLE ... PARTITION OF` 返回0A000，子表目录身份及冷开身份读取缺行。原生分区物理存储控制不代表这项SQL功能已实现，也不代表逻辑分区扫描已通过。

正式1500计时／114正常Main／2733完整回复，另1536诊断查询／3正常Main；不剔除首轮、尾部、CPU回退或既有失败，所有冻结源码、原始wire、三时钟、完整闭库文件和首错可从压缩证据重建。

全部试验失败原样保留：v1因不存在的makeByteaColumn编译actual1；v2/v3用70003字符TEXT建原生数据时拒绝，目标情形NOTRUN；v4较大原生列声明在首行遇IO_ERROR、回滚未完成，退出actual -6／double free，原因OPEN，未调查或修改用户暂缓模块；v5真实SQL声明下原生写TEXT70003仍拒绝。v6的前8个“绑定”控制实际未绑定，不能作为NULL安全证据，后续SQL子分区DDL为0A000并停止。v7改用真实正RID后普通表复现失败，但逻辑分区读取的初始全值前置检查失败，余8种情形NOTRUN；未将其改称通过。v8调用私有路径／行头接口而编译actual1。最终v9改用受控默认路径与既有完整物理解码器作为独立oracle，才完整执行16种情形并形成160／48旧版红测；全部初稿源码、编译／运行终态和首错均留存。

正式库存2398个测试文件（1131顶层／1136总C++），注册E2E684／独立E2E664不变。754个Markdown路径和725份保护记录保持，8份既有文档仅插入本批说明。原273项及22 complete／166 partial／70 unverified／15 deferred_by_user不变，历史增至287，IDX-07仍unverified/evidence[]。 [完整正负结果与归档](performance-report-2026-10-30.md#vacuum-physical-references-20261107)。
<!-- P37 vacuum-physical-references end -->

<!-- P36 physical-row-null-reuse begin -->
<a id="physical-row-null-reuse-current"></a>
## 2026-11-06：物理位图复用、完整协议及真实成本

新增 sorted_projection_null_bitmap_cost_test 在相同冻结测试源码上，旧版 1156 检查／8 失败／exit1，C1 1156／0／exit0；每 64 行排序后投影的文件校验从 353 降到 0，两个回退控制仍为 233／353。覆盖内存与真实落盘排序、升降序、NULL/空串/文本 NULL、UTF-8、二进制、长字段、过滤来源、schema 不匹配、子类虚调用、LIMIT 与虚拟列错误。

7项旧／新均exit0的既有程序为 ordinary_project_toast_demand_test、sort_null_binding_lifetime_test、typed_sort_operator_test、bounded_sort_runs_test、sort_final_merge_demand_test、view_column_dependency_test、scalar_result_null_bitmap_test。两代均使用各自冻结249源码、61core及stub。

另外 7 项相关原生回归在旧版和 C1 均实际 exit0。既有 toast_test 在两版均于第 604 行 `!corruptScan.open()` 断言中止（actual -6）；保留为失败，后续断言未运行，未修改测试预期或调查／修复该故障模块。首个原生控制器因此停止，后续仅执行原计划尚未启动的 11 个运行，没有重跑已完成项，也没有宣称全套通过。

原 62 条 SQL 在旧版与 C1 全部重新运行，初开／冷开 4 个 Main 正常0，完整请求／回复逐字节一致；全部 T7、值／NULL、SQLSTATE、诊断、tag 与 Ready 保留。既有 9 条 PostgreSQL 差异仍在，strict PG=false；沿用并核对真实 PostgreSQL18.6 的固定原始 oracle，没有运行 PG 速度对比。

新正式性能270+720操作共72个Main正常0，1926完整回复逐值核对；定位阶段另720操作／18Main正常0，采样另1536查询／3Main正常0，全部原始错误、尾部、时钟括号与文件核对保留。

正式库存 2397 个测试文件（1130 顶层／1135 总 C++），注册 E2E 684／独立 E2E 664 不变；754 个 Markdown 路径与 725 份保护记录保持。原 273 项及 22 complete／166 partial／70 unverified／15 deferred_by_user 不变，历史增至 286，IDX-07 仍 unverified/evidence[]。 [正负结果与归档](performance-report-2026-10-30.md#physical-row-null-reuse-20261106)。
<!-- P36 physical-row-null-reuse end -->

<!-- P35 view-column-dependencies begin -->
<a id="view-column-dependencies-current"></a>
## 2026-11-06：视图依赖、旧库与事务回归

最终 C4 正常 CMake C++17/O2（程序 SHA 前缀 77418e9c）通过新增原生 242 项检查；同一最终测试旧版实际 120 项失败，中间 C3 实际 2 项失败。7 个既有相关原生测试在旧版及最终 C4 均实际 exit0。28 条真实依赖删除返回 2BP01；29 条普通事务／保存点对照的值、tag、SQLSTATE 和 Ready 与 PostgreSQL18.6 一致，保留先前写入、失败 CASCADE、恢复与冷重启均有记录。

新增 `view_column_dependency_test.cpp` 覆盖真实列／别名／表达式／谓词／排序／分组／窗口／CTE／派生／相关子查询／USING／NATURAL／自连接／CASE/ARRAY/ROW/FILTER/UNION、星号后加列、兼容替换、编译目录列身份、保留注释、列重映射、递归 CASCADE、多子命令失败恢复、真实旧式关系依赖及纯元数据校验。旧版最终 242/120失败/exit1，C3 242/2失败/exit1，C4 242/0/exit0；初版六次 abort 未改为通过。

7 项既有程序为 view_test、view_replacement_contract_test、serial_owned_sequence_test、ddl_rollback_restart_test、temp_alter_column_catalog_test、with_dml_native_command_view_test、catalog_snapshot_test。旧版记录实际复用并核对，最终 C4 全部重新运行 actual0，61 core 与各代冻结源码／stub 对应。

新 201 条协议矩阵仍有 3 条别名运行查询差异，真实旧库升级 42 条仍有 5 条嵌套／字面量运行差异；整表／视图删除 13 条的值与状态匹配 PG，编译目录视图 12 条仍有 1 条别名执行 0A000。全部请求、完整 T7、NULL、诊断和 Ready 原样保留，完整 PG 协议等价仍为 false。原 62 条 SQL 完整回复在旧／C4 间逐字节相同，41 条目录查询及 7 条控制回复相同，原有 9／24 条 PG 差异保持。另保留完整 pg_attribute 89 条调研。

普通事务补充 29 条在 PG／旧版／C4 全部运行，初始及冷开 Main 均实际正常0；C4 的所有值/tag/SQLSTATE/Ready 匹配 PG。前期 C1 首查询超时的 42 条 NOTRUN 保持失败；两个单独 source-first 诊断、旧版完整 42、后续候选完整 42 均有原始记录，未归因为已修复。

正常 270+27 操作的 72 个 Main 正常0，1197 完整回复独立核对。正式库存为 2396 个测试文件（1129 顶层／1134 总 C++），注册 E2E 驱动 684／其中独立 E2E 664 不变；本批没有声称全套通过。原 273 项、22 complete／166 partial／70 unverified／15 deferred_by_user 保持，历史记录增至 285，IDX-07 仍 unverified/evidence[]。754 个 Markdown 路径、725 份保护记录及旧 35 个压缩包保持。 [全部原始值、负向与压缩证据](performance-report-2026-10-30.md#view-column-dependencies-20261106)。
<!-- P35 view-column-dependencies end -->

<!-- P34 projected-toast-demand begin -->
<a id="projected-toast-demand-current"></a>
## 2026-11-06：按需求读取 TOAST 验证

新增原生回归 14 组、83 项检查全部通过；同一测试在旧版实际失败 6 项。另 5 个既有相关原生测试在旧版及最终 C2 均通过，包含多 LOB 正常冷开/恢复生命周期、缺文件错误、BYTEA 和目录声明 550 项检查。原有 62 条完整协议回复、额外 41 条目录查询及 7 条控制回复，在旧版/C1/C2 间逐字节一致；原 9 项与新 24 条 PostgreSQL 查询差异继续保留。

新测试 `ordinary_project_toast_demand_test.cpp` 在旧版为 14 组/83 检查/6 失败/实际 exit1，C1 与最终 C2 均 83/0/exit0。检查真实正常值、NULL/空串/字面 marker、10KiB BYTEA、缺失未选中 LOB 的正向读取、选中 LOB 行输出前失败、Filter/错误 schema 回退、恢复原 LOB 文件和虚拟依赖；没有放宽旧断言或将 NOTRUN 算通过。5 项既有测试为 ordinary_project_bitmap、multilob_storage_lifecycle、shared_toast_runtime_missing_file、pg_class_declaration、bytea_large_toast。

最终 C2 的 6 个原生程序均实际 exit0，完整匹配 61 core/本代 stub/冻结源码。原协议仍 strict CLI exit1；新 41 查询 PG 参考 41/41，产品 17/41、24 条差异不变。最终 C2 新运行 4 个初始/冷 Main 正常 exit0，旧版原始完整记录复用并独立重新解码。

C1/C2 各 270 个正常计时/54 Main、1098 完整响应/96624 行/386388 单元/27864 NULL 独立校验；CPU 诊断另有 2100 回复和 4 个正常 Main。正式库存 2395 文件、1128 顶层/1133 总 C++；注册 684/664 standalone E2E 保持，本批不代表整套通过。[全部原始值与边界](performance-report-2026-10-30.md#projected-toast-demand-20261106)。
<!-- P34 projected-toast-demand end -->

<!-- P33 pg-class-declaration begin -->
<a id="pg-class-declaration-current"></a>
## 2026-11-06 pg_class declaration validation

Final C2 normal CMake C++17/O2 builds successfully. `pg_class_declaration_test.cpp` passes all six groups/550 checks with the exact61-core objects and own test stub. It covers real34 fields/type closure, ordinary DDL, cold CSV/read-only absence, startup idempotence, qualified user type shadows, unavailable selected inputs (NULL included), and finite class/type/attribute/dependency bootstrap conflicts before mutation. It compares complete declared class/type/attribute values, rather than relying on abbreviated `toString()` summaries. The old generation executes the same final test with48 checks,31 failures and one unavailable-capability NOTRUN; it is not counted as a pass.

`catalog_snapshot_test`, `declared_type_physical_catalog_test` and `pg_stats_histogram_extent_test` each pass on old and C2. Earlier C1/native fixtures and both launcher failures remain in the archive. Initial quoted raw-native DDL fixtures retain four failed checks each; the normalMain41-query run confirms the corresponding quoted CREATE/ALTER/index operations succeed. C1's wrong12-byte type and TEXT25 NULL output are rejected, not accepted fixes.

The original58 targets plus four cold controls run unfiltered on before/C1/C2: all strict drivers exit1 with the same9 PG difference IDs. Before/C2 has60 full replies byteexact; only two self-identity replies gain the actual row. C1/C2 all62 replies are exact. Every frame/NULL/T7/error/tag/Ready is independently decoded from the saved bytes.

The additional frozen41-query matrix runs on real PostgreSQL18.6 and each product generation. PG passes41/41; before and C2 each match17/41, while24 queries differ (258 before/246 C2 failed field/shape checks, not246 different queries). Another seven controls per product preserve `pg_attribute`42P01 and type input limitations. Five returned type rows match PG on nine fields; independently decoded catalog files match all34 attribute rows on six fields. Input/result codec completeness is not asserted.

The actual old-image upgrade and cold restart run20 requests with fresh-C2 byteexact replies and normal actualwait0 for both servers. Old class/type/attribute rows and the original old image stay unchanged. Full ordinary performance validation runs270 timed requests/1098 replies, independently checks96624 rows/386388 cells/27864 NULLs, and closes54 Main processes normally. All raw values, tails and regressions are retained in the [report](performance-report-2026-10-30.md#pg-class-declaration-20261106).

Inventory:2394 test files,1127 top-level/1132 total C++ files;684 registered drivers/664 standalone E2E drivers unchanged. The full inventory is not represented as an all-tests pass.
<!-- P33 pg-class-declaration end -->

<!-- CURRENT_LEGACY_LOG_APPEND_20261106 -->

## 普通日志追加与实际查询成本（2026-11-06，P32-C1）

正式只新增1个CPP回归，最终源码 `d2669466…`。完整61对象/core加本代stub的实际配对中，before为6组普通行为通过、7组新机制能力缺失NOTRUN；C1共13组、105项检查通过，测试作用域内观察38次writev封装层命中（含控制返回，非物理写盘次数），两代最终测试编译stderr均0字节。覆盖NUL/UTF8/空串/换行/长值、追加、轮转、权限、symlink、实际打开错误和有限EINTR/短写/零进展/EIO控制；128条并发短记录比较完整无序集合，不宣称任意大记录跨短写原子性。两项既有TRUNCATE测试两代均正常0，完整stdout/stderr逐字节一致，实际分别记录5次/3次TRUNCATE日志。

原58个严格协议目标及4个额外冷锚控制全部保留，两代产品各完成62请求、目标各53/58，原9项普通差异未关闭，strict CLI仍为1；全部62请求/响应原始字节一致。每代实际207行/848单元/156个NULL，4个初始/冷Main均取得真实子进程wait0、PID/组消失。复用此前真实PG18.6参考，没有重跑PG，没有把目录来源元数据或选中错误的TEZ/EZ差异改成PASS或暂缓。

本批固定成本另有270计时/54正常Main，不能与native检查数相加冒充全仓测试。正式测试2393，源248/头177；仅新增1CPP，不宣称全套重跑。[本批完整报告](performance-report-2026-10-30.md#legacy-log-append-20261106)与可逆原始证据（可选本地文件 `benchmarks/legacy-log-append-20261106.json.gz`）。以下为各代历史。

<!-- CURRENT_FIXED_ID_PROFILE_20261106 -->

## 主键查询路径的独立诊断（2026-11-06，P31）

复用P30真实三表闭库状态，P29-C2→P30-C2→同一P29-C2三个实际正常进程，各512次原样主键查询。外层及3个Main实际wait0、PID/组退出，零重试。独立重解1,575个完整响应：1,536个目标的394,752个ID、18次前后完整三表模型、1,560次物理SELECT的全部7项描述字段/标签/ReadyI均相符；合计400,179行观测、412,533单元、1,230个NULL。6次EXPLAIN旁观均为TableScan→Project；6次pprof渲染和独立原始PC解码全部真实退出0。

这是CPU8/libprofiler诊断，不是新增正常性能矩阵或功能验收。原P30正常负例、58个严格协议目标和9项差异保持，回退原因仍OPEN。pg_class完整34列声明及来源补全只有依赖审计和未运行方案，未应用生产补丁，不计功能通过。正式测试仍2392、源248/头177、原功能273项状态与history281不变；没有新正式测试或重跑既有native/PG参考。完整定义见[本批报告](performance-report-2026-10-30.md#fixed-id-query-profile-20261106)与压缩原始证据（可选本地文件 `benchmarks/fixed-id-query-profile-20261106.json.gz`）。

<!-- CURRENT_ORDINARY_PROJECT_BITMAP_20261106 -->

## 当前行NULL位图复用与按需求捕获（2026-11-06，P30-C2）

普通物理表的直接投影现在按选中列需求，复用当前行已有的完整NULL位图；局部作用域在正常或异常退出时恢复，只选非空物理列时不再复制。既有值提取、虚拟列求值顺序、TOAST处理、文件身份校验函数和暂停范围保持。

正常CMake C++17/O2的最终C2，在固定/单BYTEA/多LOB三类直接完整查询中，墙钟中位数对前后两次旧版降低约9%–32%，服务器CPU降低约11%–35%；这是本机各三样本的有限结果。主键投影负控仍有回退，固定表服务器CPU为+26.19%/+25.84%，单BYTEA主键查询墙钟为+8.03%/+3.65%。跳过多余复制没有消除这些回退，其根因未证，不能据此归因频率、负载或噪声，也不能称全库提速。

本批仅优化真实物理 TableScan→Project 的当前行 NULL 读取：选中列需要 NULL 信息时，借用该行已拥有的完整位图，并在局部作用域退出或异常时恢复；C2 在既未选中可空物理列、也未选中虚拟列时不复制位图。新增的普通 native 回归在 before 执行13组并明确保留6组能力缺失 NOTRUN，C1、最终C2各19组全部真实通过。NULL、空串、文本 NULL、TOAST及标记字面量、相反位图、虚拟列需求、嵌套和异常恢复均保留完整值与真实RID观察。外部 current-file lookup 的有限机制计数不等于 SQL 耗时或整体提速。

真实 PostgreSQL 18.6 参考完成全部58个固定目标及7个控制，观测205行、846个单元、156个NULL；before、C1、C2分别完成全部58个目标及4个冷锚控制，各为53/58，观测204行、845个单元、156个NULL。三代产品全部62组查询的请求、响应及差异保持原始字节一致。既有9项普通差异未关闭：4个初始目录源锚和4个冷目录锚涉及 pg_class 自身行及目录RowDescription源OID/列号，另一个是选中虚拟列除零时PG发送TEZ而产品发送EZ。它们不是PASS，也不是用户暂缓；没有删例或放宽期望。PG启用fsync，所有初始及冷上下文都取得实际子进程wait0，PID和进程组消失。

六个既有native测试在before、C1、C2三代共18次全部真实通过，stdout/stderr逐字节一致；C1与C2新增native的完整stdout也一致。正式只新增1个CPP测试，完整58条portable协议草稿仍以known_gap形式保留，未运行、未注册，不能计为通过。本批不改参数帧、provider生命周期、retained规划或暂停的输出format2边界，不宣称目录/协议错误帧已与PG等价，也不据这些功能回归单独宣称全查询性能提升、默认程序晋升或服务重启。

原273项仍为22 complete/166 partial/70 unverified/15 deferred_by_user，`IDX-07`保持`unverified`且`evidence=[]`。完整PG参考58/58；三代产品均完成全58目标与4冷控制，目标各53/58，另4冷锚控制仍失败。原9项目录/普通错误帧差异完整保留、没有改为PASS或新增暂缓。仅新增1个正式CPP回归；whole58 known-gap协议草稿未运行、未登记。Goal active，默认程序未替换，无服务重启、分支切换或推送。

本批实际清单：源248/头177；测试2392，原2391逐字保持，新增1个CPP；顶层CPP1125/全部CPP1130（nested5）；E2E登记684 driver/664 standalone保持；Markdown754/保护725保持；旧29份gzip保持，新归档后总30。原280条历史原文保持，追加第281条partial。

完整原始分布、既有差异、许可和验收边界见[本批性能报告](performance-report-2026-10-30.md#ordinary-project-bitmap-20261106)与可逆压缩证据（可选本地文件 `benchmarks/ordinary-project-bitmap-20261106.json.gz`）。以下P29及更早内容为对应代别的历史记录。

<!-- CURRENT_MULTILOB_20261106 -->

## 多个大字段存储与真实命令结果（2026-11-06，P29-C2）

原七列建表同时含两个 BYTEA 和两个 TEXT 时，类型逻辑上限之和会超过堆头的 uint32 字段。现在仅对 format 2 中这种确实含变长列且最小实际元组可装页的溢出定义，将堆头名义宽度记为 0；可表示的旧宽度、列上限、实际变长元组和 TOAST 格式保持。UNLOGGED 初始化分叉在原有维护入口校验自身宽度，不匹配时重建。执行结果另显式标记无 RETURNING 的计数命令，普通 WITH DML 也据实际执行结果发送命令帧。

原19项定义/排序规则诊断在C1及C2均完整通过：每代父进程2154项检查，两个工作进程合计604项（452+152）。原失败发生在旧七列建表前置阶段，不能把它写成19项全已执行。另C1/旧P28C4双代12工作进程共2662项检查、6组真实冷重启RID对照全过；C2正式物理生命周期用例为10工作进程2483项、父进程17809项，覆盖真实大值/NULL/RID、备份恢复、分区、UNLOGGED及维护路径。以上是不同固定夹具和进程角色，不能相加冒充全仓测试数。

固定SimpleQuery文本格式0的37+3条、七列全值/NULL/十六进制BYTEA/列描述七项/命令与Ready检查，PG18.6首次40/40，C1为38/40，C2为40/40；C2自包含正式协议用例另一次同40控制也通过。C1保留的两项是DELETE多发空RowDescription帧，行值/NULL/状态/命令原本正确。C2按真实DML命令角色修正该帧形状，并保留SELECT/RETURNING结果形状；没有靠搜索字符串中的RETURNING判断。PG与本地分别读取真实关系OID进行来源锚定，不要求两个数据库的原始OID或整段原始协议字节相同。

原constraint154协议回归在C2完整通过。独立结果形状mini21参考PG21/21，C1为13/21，C2为19/21，仍保留两个普通缺口：零列一行SELECT缺少一个零列DataRow；零列表RETURNING *的错误代码实际42P01而PG为42601。它们未被改为PASS。

两个产品专属普通WAL角色共实际12条动作：COMMIT确认后强杀与未提交UPDATE确认后强杀，各自退出为预期-9，随后正常Main首先打开同一目录完成恢复并正常退出0；没有由原生读取器抢先恢复，也没有PG崩溃参考。后续在4份正常关闭后的独立副本上运行真实公共HeapScanCursor，34行/238单元/44个NULL和全部真实RID身份检查通过，四观察者均正常0且PID/进程组消失。初始到冷开共同键1..7不变，未提交角色全部8个旧键恢复，提交角色7个未修改旧键保持，实际修改键3及新键使用真实唯一RID。原WAL回执不回写；独立sidecar补充可见性证据，绝不把LP_NORMAL或虚构SQL ctid当可见行。

正常 CMake C++17/O2 固定成本对照已完整执行：旧版—C2—同一旧版各三轮，固定表及单 BYTEA 表各256行，另有C2专属多LOB表128行。168句实际计时、546个响应、24个正常Main及42个只读副本观察者均完成；全值/NULL/来源/冷开与真实RID验证通过。结果不支持本批普遍提速：16个可比操作中，墙钟与服务端CPU各为1项低于两旧、12项高于两旧、3项介于两旧；固定表查询在原双表流程中服务端CPU中位数15.667→30.044→18.536ms，C2增加91.76%/62.08%。全部变慢项、原三样本、长尾及漂移保留。DELETE旧版还多一个空T帧，不能当完全相同协议工作量；新多LOB因旧版建表失败，仅描述新版成本，不计算提速比。

后续固定CPU8的单表、三代正常构建窄诊断另有12次目标观测与24个正常Main；C2查询墙钟中位数22.086ms，对两旧24.181/26.336ms，服务端CPU14.326ms，对两旧15.406/16.605ms；C2相对C1的CPU中位数仍增加4.40%。这个更窄条件未重现原流程的大幅差异，但不能冲销原负向结果、归因于CPU亲和性或证明稳定收益，回退原因仍未确认。四角色用户态CPU采样的512次完整查询均通过，显示共同热点在逐列NULL检查及文件身份核验；采样计时没有混入正常性能表。

最终验证对象是候选C2（SHA `b53f853b356e9c8cf1e8fedf9832201d9bf705cf81d817b35314b4660e334594`），默认安装程序未替换，未推送或切换分支。原53.45MiB性能JSON当前保留为5.70MiB gzip，未压缩基准文件已被Git忽略；本批完整证据同样采用可逐字节还原的gzip归档。只对核验可恢复的生成副本安排清理，Git历史不重写。性能目标继续，不能把功能修复称为全库优化完成。

实际清单：源248/头177；测试2391，原2388逐字保持，新增2个CPP和1个协议测试；顶层CPP1124/全部CPP1129（nested5），登记684 driver/664 standalone E2E；Markdown754/保护725保持；旧28份gzip不变，总计29。原279条历史原文保持，新增第280条partial。

这是有限的 partial 进展，Goal active。原 273 项状态保持 22 complete/166 partial/70 unverified/15 deferred_by_user；`IDX-07` 仍为 `unverified`、`evidence=[]`。零列行集、零列表和原 TOAST 损坏夹具的普通缺口仍保留，用户暂缓边界保持。本批不证明完整 PostgreSQL 数据类型上限、文件格式、WAL 或协议等价。

完整定义、首轮失败与测试边界见[本批报告](performance-report-2026-10-30.md#multilob-storage-20261106)和可逆压缩证据（可选本地文件 `benchmarks/multilob-storage-20261106.json.gz`）。下方 P28 及更早段落是对应代别的历史记录。

<!-- CURRENT_CONSTRAINT_VALIDATION_20261106 -->

## CHECK/FK NOT VALID 与真实历史验证（2026-11-06，P28-C4）

`ADD CONSTRAINT ... NOT VALID` 仍在真实元数据锁下绑定 CHECK/FK 定义，只跳历史行验证；后续写入仍由原真实约束执行链检查。`VALIDATE CONSTRAINT` 对未验证的 CHECK/FK 扫描真实可见历史行，仅在成功后发布 validated 标记；失败保留定义和未验证状态，已验证约束不重复求值。真实 SQL NULL 与非 NULL 空字符串分别处理。CHECK 的 volatile 函数不因 volatility 标记被一律禁止；定义绑定不执行用户函数，实际验证仍按到达的行求值。

正常候选 C4 为 CMake C++17/O2，程序 SHA256 `5cd5fd9e15f72257d13f2622e180d643e5cc93fffe8a128cbf8e820f64caf539`；before 为 P27-C2 `dd5d9b85…`。原生父测试 4,896 checks/0 failures（含文件快照断言），其中真实 lifecycle worker 779/cold worker 158；两 worker 正常退出，捕获两份当时文件映像，不能将这些层级的计数相加。真实目标 PageAllocator fetch 38 个观测中 18 个为零、20 个为非零 fetch，另有真实读取正对照证明 hook 确实生效；这只说明历史验证访问路径，不代表整句无 I/O。原协议 142 主流程+12 冷控制共154条全部匹配 SQLSTATE、完整值/NULL、真实本地 relation OID 的 T7、tag 和 Ready；58份参数快照、5项保留标志检查通过。修改前同源用例实际83条（56匹配/27差异）及71条 NOTRUN；C3全部154条中有4处不匹配，原失败保留，C4修复后原断言未放宽。独立 append10 全10条通过：已经 validated 的带 nextval CHECK 不再求值，PK/UNIQUE VALIDATE为42809、缺失名42704。新增真实 PostgreSQL18.6 mini20 全20条符合首预测；产品对应19条+6条冷控制全25条、13项真实 `.params` 检查通过，ASCII collation 只比较大小写语义角色，PG使用真实 ICU、本地使用 nocase，不声称 provider/OID/Unicode 等价。mini19 v1接收器在启动前缺少 Path import：0SQL/0owner，原首错误保留后仅修接收器，不称产品重试或产品失败。原159参考的3个预期行快照错误及 reserved RESET 首预测错误都保留，后续 oracle 仅从已记录 PG 离线更正。完整原始帧不是逐字节 PG 等价：mini19主流程12组 raw 相同、7组不同（错误文本字段或真实本地 OID），不抹掉差异。

普通 ADD CHECK/FK 与同样有效数据的旧实现可比较所需验证成本。NOT VALID 可描述去掉不需要的历史扫描，但旧版多做扫描，不能称同扫描工作量提速；旧 VALIDATE 仅改标记，其耗时不是正确验证的性能基线。整个普通 ALTER TABLE 仍有 DDL 物理回滚快照、文件复制/摘要/同步成本，目标 PA fetch=0 不表示整句 O(1)、无堆文件读取或无 I/O。

固定0/128/1024行×三轮before-first/candidate/before-return，27个独立主流程+27次同数据冷重启均正常0；216个ADD/VALIDATE计时点、3024份响应、2384640个完整单元格（157950个NULL）、18360份物理文件映像（其中2700份堆映像）全部独立核对。1,024行时，CHECK NOT VALID wall中位数对前/后旧版分别−63.939%/−61.677%、server CPU −71.962%/−72.695%；FK NOT VALID wall −40.047%/−39.400%、CPU −15.482%/−13.469%，这两项是消除旧版多做的历史扫描。普通ADD CHECK wall −18.399%/−14.808%，CPU +0.124%/−4.360%；普通ADD FK wall −38.575%/−35.423%，CPU −4.131%/−0.022%。补上真实历史检查的CHECK VALIDATE wall +52.762%/+43.290%、CPU +260.317%/+251.282%，属于新增正确性工作成本，不能与旧假验证比较成等价速度。

负结果也保留：空表普通ADD CHECK CPU +29.920%/+35.993%；1,024行已验证FK重复VALIDATE wall对首/返旧版为−36.791%/+51.673%。全样本最长单点17.977415s，旧版wall中位数往返漂移−58.325%～+60.792%，server CPU漂移−28.186%～+4.689%。完整72行metric统计含各角色raw3/min/p50/max、双基准差值与旧版漂移，没有剔除负值/长尾，没有稳定倍数、因果结论或PG速度等价声明。计时是普通socket请求至Ready，包含DDL物理快照成本；process CPU与proc快照非原子、含所有服务线程，不是独占算子成本。

本批四个生产文件、两份新增回归、原登记入口和既有文档在 master 做本地版本管理，使用 `Palpitate-xus <palpitate.xus@outlook.com>`。正常验收程序独立冻结，根目录运行程序仍为 `425165af…`。原始证据只以完整可逆 gzip 入库；53.45MB 的旧 analyze/reindex JSON 已由约5.7MiB gzip替换，旧提交历史未重写。恢复步骤和逐流SHA保存在归档内，构建目录中的原始文件不再直接进入Git。

当前实际清单：源248/头177；测试2388（原2386逐字保持，新增1 CPP与1协议）；顶层CPP1122（含链接桩）/全部CPP1127，其中nested 5；登记683 driver/663 standalone E2E。Markdown 754/保护725保持，旧27份gzip保持，归档总计28。原278条历史原文保持，prepend第279条partial。

本批为有限普通约束生命周期的 partial 进展，Goal active。原 273 项状态保持（22 complete/166 partial/70 unverified/15 deferred_by_user）；`IDX-07` 仍是 `unverified`、`evidence=[]`。本批 GiST 页树资料只是未实现研究，不能将 P27 的 batch partial 记录当作 IDX-07 已部分验证。原用户暂缓边界保持。

完整代别、首轮失败、普通成本全表与范围见[本批报告](performance-report-2026-10-30.md#constraint-validation-20261106)和压缩原始证据（可选本地文件 `benchmarks/constraint-validation-20261106.json.gz`）。下方 P27 及更早段落保持历史口径。

<!-- CURRENT_GIST_PROBE_READER_20261106 -->

## GiST 读取开销与空文件回退（2026-11-06，P27-C2）

同一机制源码在旧版和C2各实际52219检查/20探测/4096真实行通过；每端六个真实分配、read和目标堆阳性控制命中，20个完整RID集合都精确且目标堆fetch为0。独立归约16614项通过，只核既有输出，没有重跑机制程序。正式reader夹具保留八组110探测：旧版2217检查中4项失败均属零字节文件的两API合同，C2为2215检查/0失败，110次目标堆fetch均0，另有10次真实堆fetch阳性。检查数随实际分支有所不同，未改同源SQL或预期。

新82命令/93动作协议旧版12项失败仅在零字节文件的等值、范围、LIKE结果与路径；C2全部通过。完整原始帧、三列七字段描述符、实际关系OID、NULL/空串/UTF8/DEL、命令tag和Ready均保留。坏CRC/部分头各自从同一健康代损坏并恢复，不是失败后重试；合法非空旧文本与24字节V3零记录是独立正例。原120命令C2通过，PG75复用对照保留73/75原始帧一致及两处DELETE严格差异。

C1生命周期原source_unchanged=false回执原样保留：独立核对正式源、冻结源、计划和基线SHA完全相同；旧运行器source变量被循环中的数据文件遮蔽。C2改用captured_source。该离线核验没有重跑原生测试或改写旧回执。首次归约脚本的诊断参数名冲突也单独保存，修正只影响离线脚本。

这些是有限专项与相关入口，不是全部测试文件重跑，不是PG全功能验收。最终注册数量、完整相关名单、所有首次失败和成本分布见[本轮报告](performance-report-2026-10-30.md#gist-probe-reader-20261106)；下方历史完整保留。

生产解析器7个原文字节片段及4个完整依赖（含真实Numeric实现）单独使用ASan/UBSan编译并实际运行一次：12,355检查/8组/2021帧/4042次解析全部通过，含217个非零截断、1744个单bit变异、CRC合法结构异常、1MiB两侧、数值及两个visitor形状；开启泄漏检测，stderr为空。它是解析器组件检查，完整SQL/堆行为由上述真实引擎夹具分别验证。

6个旧协议入口全部正常0；生命周期all2879检查及独立冷启183检查通过，各阶段统计文件在冷启前逐字捕获。

当前为2386个测试文件：原2383份逐字保持，新增两份CPP和一份协议测试；顶层CPP1121（含链接桩）、全部CPP1126/子目录5，登记682个driver/662个独立E2E。源248/头177，754份Markdown路径和725份保护记录保持；26份旧gzip逐字保持，新增第27份。原273项及22complete/166partial/70unverified/15用户暂缓状态、277条历史保持，仅增加第278条partial。

<!-- CURRENT_GIST_EMPTY_BOUNDS_20261106 -->

## GiST 空字符串、边界与 LIKE 检索（2026-11-06，P26-C4）

最终C4普通CMake C++17/O2构建退出0。格式组件O2与ASan/UBSan（含泄漏检测）各8434检查；现有API生命周期2879检查及独立进程冷启183检查通过；typed接口806检查/9组/68探测全部目标堆fetch为0，并有4次真实堆fetch正控制；文件归属冲突四个语义探针通过。17个有限native入口（其中2个新、15个旧）和6个旧协议入口全部正常0；迁移后的all-AM native实际12168检查/17场景，恢复实际3579检查/18workers（5次预期SIGKILL、13次正常0，36份完整输出及真实marker/event保留）。新协议120条命令、15个索引图像记录及六阶段真实heap RID/NULL见证通过，含正常双进程冷启。

真实PG18.6安装并加载btree_gist后重放75条共享SQL；最终C4逐项比较3231行/19382单元（661个NULL）、248列描述符的1736个元数据字段。75/75语义及描述符一致，73/75完整帧一致；两次DELETE本项目额外发送零字段T，PG只发CZ，严格全协议通过仍为false。两端关系OID各由真实目录锚定；PG正常退出、PID已回收，未比较PG访问方法格式、路径或速度。

首次结果全部保留：旧版现有API夹具183检查中21失败；文件名冲突v1先因设置顺序失败，v2真实执行后旧版1失败、C1四失败，C2及最终C4修正通过。C2首次协议的117个RID见证解析错误与1个真实全宽索引浪费分开记录；C3旧LIKE严格性能路径失败由C4独立LIKE角色修复。PG首次启动角色设置错误时0条共享SQL；修正initdb角色后才执行首次75条SQL。三个生命周期冷启前.runtime_stats只有原始长度/SHA清单、未保当时字节，明确标为metadata-only；所有成本459图像、实际捕获流与其余可用阶段字节完整归档，不凭冷启后统计伪造早期图像。

当前清单为2383个测试文件：原2378中2374份逐字保持，4份只迁移当前GiST格式断言并加强V2兼容/空串真实RID检查，另增4个CPP及1个协议测试。顶层CPP1119（含链接桩）、全部CPP1124/其中子目录5；登记681个driver/661个独立E2E。源248/头177，754份Markdown、725份保护记录保持；25份旧gzip逐字保持，新增第26份。原273项及22complete/166partial/70unverified/15用户暂缓状态、原276条历史逐字保持，仅prepend第277条partial。

这些是有限专项检查，不是2383文件全量重跑或PG全功能验收。未运行R151原341/103矩阵，未扩展已有用户暂缓范围。完整源码、计划、首轮失败与性能证据见[本轮报告](performance-report-2026-10-30.md#gist-empty-bounds-20261106)。以下 P25 及更早段落均保留当时的实现、计数和未完事项；本轮状态以本节为准。


<!-- CURRENT_REINDEX_ACCESS_METHODS_20261106 -->

## 普通访问方法 REINDEX 与空文本生命周期（2026-11-06）

本批补入普通 `REINDEX TABLE` 的 B-tree/Hash/Bloom/FTI/GIN/GiST/SPGiST/BRIN 重建、物理 Bloom NULL/空文本维护与 UPPER/LOWER 表达式索引 SQL 桥接。有效旧头的 BRIN `pages_per_range` 与 Bloom `k` 保留；`m` 随真实新键集按既有 loader 默认128 bitsPerEntry重算。旧头损坏/缺失时回退宽64/k7。默认程序仍为 `425165…`，没有服务重启或默认升级。

当前统计：2378 测试文件（原2375字节不变加2 CPP及1协议），顶层CPP1115（含链接桩）、总CPP1120/其中子目录5；登记680个driver/660个独立E2E。源248/头177，754 Markdown、725保护记录；24份旧gzip逐字保持，归档总计25份。原273项22/166/70/15及275条历史保持，仅新增partial第276条。

C2 主 native 实际 12096 检查/17 场景通过，恢复 native 实际 3183 检查、8 控制/18 workers （5 次预期 SIGKILL、13 次正常退出）；24 条 PG18.6 共享命令比较 1343 行/8058 单元及全部字段声明。31 个既有 native 与 5 个相关协议及 auto3 通过；另一个相关协议仍失败，旧版配对尚未到相同阶段。

GiST 空文本编码、native-only 丢失定义、虚拟 Bloom NEW NULL 和上述失败均保留普通未完；不关闭原273项。

本批本地专项协议实际69条命令/40个文件图像通过；PG共享24条、7次完整人口结果共1,343行/8,058单元（441个NULL），42个列描述符逐项检查7个字段。这些是6个数据列的描述元数据，并非7个数据列；两端tableOID各自真实锚定。PG保留4个实际BRIN metapage，损坏/丢失时本地宽64与PG intact宽1明确分列；事务内1个ReadyT图像没有CHECKPOINT或物理页读取。

恢复控制覆盖真实V2持久标记、首次辅助文件旧代备份/新代发布、全部rename及目录fsync后的进程中断，并保留V1兼容控制、私有文件fsync/发布rename的定点EIO与正常控制。5次预期SIGKILL不是普通退出；18个worker的36份完整stdout/stderr均保留。该控制不覆盖发布标记前崩溃的临时文件回收，也不等价完整PG事务WAL/并发恢复；原有混合reindex_atomicity回归仅原样观察一次，不扩展安全/TDE范围。

完整代别、首轮失败、工作量差异与全部成本表见[本批报告](performance-report-2026-10-30.md#普通访问方法-reindex-与空文本生命周期2026-11-06)。以下P24及更早段落保留当时历史，不把旧数字当作当前统计。

下方 P24 及更早段落为历史记录。


<!-- CURRENT_BRIN_OPERATOR_CONSISTENCY_20261106 -->

最新P24-C2将普通BRIN摘要比较改为运算符专用路径；仍校验min<=max、所有文件/CRC/identity/dirty/snapshot fences与完整heap Filter复核，NULL flags不新增求值，legacy timestamp原路径保留。实际功能门禁及全部首次结果、固定9进程的完整维护/查询wall、CPU、长尾、负数与漂移见[性能报告](performance-report-2026-10-30.md)和压缩证据（可选本地文件 `benchmarks/brin-operator-pruning-20261106.json.gz`）；不称完整PG BRIN、所有SQL提速或因果已隔离。性能外层runner因label覆盖退出1、九backend正常0，二者分开记录；原source/plan/complete=false回执保留，后续仅离线finalize，不重采样、不当产品故障。默认425保持，partial、Goal active。旧2374测试逐字不变+1 native=2375；679/659 roster、源248/头177、顶层CPP1113/全部1118、754MD/725保护记录保持，历史275，旧23gzip保持+新24。下方P23及更早段落为历史记录。


<!-- CURRENT_BRIN_MAINTENANCE_OPTIONS_20261106 -->

最新P23-C1修复BRIN在VACUUM FULL、加列、删列和TRUNCATE/reset重建时丢失pages_per_range：健康V3配置在重建前保存，宽度1和SQL默认128均保持；缺失/旧格式/损坏仍沿用64回退。生产只改TableManage.cpp四处捕获/四次传参。新协议42命令/12镜像/4路径检查、25相关原生、6专项协议、3自动统计均正常0；PG18.6共享35SQL/19,327字段、11真实metapage一致；恢复7worker/2477检查。

固定9个正常socket进程测量完整VACUUM FULL及其后7类查询，所有wall/CPU、尾部、控制项和负数见[性能报告](performance-report-2026-10-30.md)及压缩证据（可选本地文件 `benchmarks/brin-maintenance-options-20261106.json.gz`）。该修复保留用户配置，不代表所有操作均提速；REINDEX仅Btree、完整PG BRIN AM/增量维护/WAL、规划重复验证开销、Bloom/GiST非NULL空文本覆盖和P22数组ROLLBACK间歇超时仍开放。前批密集CPU回测+6.64%负面记录保持。本批partial、Goal active。源248/头177，2373旧测试字节不变+1=2374，679注册/659独立E2E；原273item状态不变、历史274；754MD/725保护记录/22旧gzip保持。下方P22及更早段落为历史记录。


<!-- CURRENT_BRIN_TYPED_SCAN_20261105 -->

最新 P22-C10 修复 BRIN 数值边界/NULL 状态并接入真实候选页扫描，支持普通 SQL/绑定后五类比较和 NULL 谓词、反向常量及 pages_per_range。23页 extent 的原生候选 cursor 实际只 fetch1页；完整谓词仍复核。格式组件 O2/ASan各5588、新原生1834、新协议101命令/25路径断言、真实PG18.6共享73SQL/10490字段及7恢复worker/2477检查通过。有限81原生80正常0+1父代TOAST中止；旧32协议29正常0+3父代相同失败，全部保留。

固定9个正常socket进程、4096×4、七类查询，服务器CPU减少（首轮／同旧回测）：高端窄范围 41.19%／42.36%；低端窄范围 48.68%／49.29%；非主键等值 25.78%／20.13%；不存在的范围 94.22%／94.12%；密集范围控制 -7.44%／-6.64%；主键控制 30.31%／28.42%；无对应索引列控制 7.79%／4.38%。所有wall/CPU/控制负数、原始帧和尾部见[性能报告](performance-report-2026-10-30.md)与压缩证据（可选本地文件 `benchmarks/brin-typed-scan-20261105.json.gz`）。新BRIN文件V3，旧1/2只作兼容读取/安全回堆，旧二进制不能读取V3；当前物理heap2保持，暂缓format2仍指wire/output，NetworkServer.cpp未改。REINDEX尚仅Btree、部分重写配置保存、PG opclass/revmap/增量维护/WAL/concurrent、SSI及特殊类型/来源路径等仍开放，不声称全部数据库操作或PG性能等价。本批partial，Goal active。源248/头177、测试2373，678注册/658独立E2E；原273对象状态不变、历史273，754MD/725保护记录和21旧gzip保持。下方P21及更早段落为历史记录。


<!-- CURRENT_INDEX_BUILD_NULL_OWNER_20261105 -->

最新 P21-C1 让索引构建/REINDEX 复用当前行物理 NULL 位图，并修复 BRIN 将真实 SQL 全 NULL 整数摘要写成 `[0,0]` 的问题；空字符串范围保留。新原生13366检查、新协议40命令、真实PG18.6共享35命令/2250字段及7恢复worker/2477检查通过。有限相关79原生78正常0+1父代相同TOAST中止；旧32协议29正常0+3父代相同失败，全部保留。

正常2048行维护9进程、368640字段核对；相对同旧回测CPU中位数减少：Btree 19.30%，复合 19.02%，写事务 45.10%，回滚 47.67%；BRIN -154.76%（退步）。wall波动大且多项变慢，另9进程完整socket查询控制也没有一致提升，详见[性能报告](performance-report-2026-10-30.md)与压缩证据（可选本地文件 `benchmarks/index-build-scan-owner-20261105.json.gz`）。BRIN数值排序及SQL执行算子等仍为普通开放缺口，本批partial，Goal active。当前物理heap版本2保持；暂缓format2指协议/输出所有权。源248/头177、测试2370，677注册/657独立E2E；原273状态不变、历史272，754MD/725保护记录与20旧gzip保持。下方P20及更早段落为历史记录。


<!-- CURRENT_INDEX_CRC32C_20261105 -->

最新P20-C2为共享索引CRC32C加入通用查表和受CPU能力保护的SSE4.2路径，保留完整校验、所有文件字节与C++17基本构建要求。组件44392检查及ASan/UBSan、无SSE4.2模拟35520检查通过；12真实存储worker/3840字段与六类索引旧新双向读写、重建文件逐字节一致。最终68原生67正常0及1项父代相同TOAST中止、旧32协议29正常0及3项父代相同失败全部保留。9正常socket进程wall中位数减少（首轮／回测）：热点前缀 18.62%／-0.48%；热点等值 3.68%／0.90%；低频前缀 11.82%／14.98%；低频等值 -12.80%／-22.17%；不存在等值 6.78%／0.48%；主键控制 -1.07%／-6.83%；控制差异不作因果归因。源248/头177、测试2368，676注册/656独立E2E；原273状态不变、历史271，754MD/725保护记录与19旧gzip保持。Goal active；下方P19及更早段落为历史记录。详见[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_GIST_READ_OWNER_20261105 -->

最新P19-C3修正普通当前读事务被GiST误判为重叠写者的问题，并复用可见元组NULL位图。专项1214检查、新39协议、PG18.6共享37命令/1287字段对照通过；候选检索阶段目标堆页fetch由旧版2021降至0，返回行仍正常回表校验。历史/重叠/自身写入/缺失损坏/空文本范围及SERIALIZABLE保留堆后备。旧60原生59正常0和1项父代相同TOAST中止、旧32协议29正常0和3项父代相同严格失败全部保留。9正常socket进程wall中位数减少（首轮／回测）：热点前缀 2.54%／3.42%；热点等值 -0.42%／-1.44%；低频前缀 56.62%／52.19%；低频等值 -12.42%／-10.29%；不存在等值 2.15%／1.44%；主键控制 -6.09%／-3.54%，负数与控制波动也报告。旧GiSTScan/idx_scan只代表算子，不能单独证明底层物理索引访问；下方旧章节按此限制理解。源248/头177、测试2367，676注册/656独立E2E；原273状态不变，历史270，754MD/725保护记录保持，18旧gzip保留并新增1。仍partial，Goal active；详见[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_TYPED_PLANNER_STATISTICS_20261105 -->

最新批次 P18-C6 将普通等值与二进制文本 LIKE 扫描成本接入拥有真实身份的 typed ANALYZE 统计，并修正事务内显式分析的两份统计文件回滚。新65条协议/15次扫描、原LIKE123/27、最终原生专项及cold exec、组件O2/ASan/UBSan、7个恢复worker和真实PG18.6共享33命令/6732字段对照通过；相关旧协议29通过、3项父代完全相同严格失败，首次自动数组/range回滚超时及后续原deadline固定旧/新/旧六次通过均保留。完整socket性能中位数减少（首轮／回测）：热点前缀 73.72%／73.25%；热点等值 51.25%／15.72%；低频前缀 58.53%／-8.58%；低频等值 44.30%／38.10%；不存在等值 24.66%／24.31%；主键控制 30.77%／29.00%，负数也保留，只代表该负载。原273与22/166/70/15状态保持，历史269，测试2365，源248/头177，675注册/655独立E2E，754MD/725保护记录、17旧压缩包保持。仍是partial，Goal active；事务内ANALYZE需要临时数据库物理备份，旧统计格式、未转换消费者和用户暂缓模块未整体完成。详见[性能报告](performance-report-2026-10-30.md)与压缩原始证据。


<!-- CURRENT_LITERAL_LIKE_SCAN_20261105 -->

## LIKE前缀的真实索引读取与密集前缀成本（2026-11-05，P17-C6）

普通无参数SELECT中的TEXT/VARCHAR字面量LIKE现在可实际使用已有GiST侧索引。已绑定AST提供真实关系/列身份；前缀范围只筛选候选，原类型化LIKE、AND剩余条件和NULL语义继续复查。转义百分号/下划线、多字节ESCAPE、UTF8上界以及打开前删除索引均有回归。可用ANALYZE的MCV与完整覆盖直方图桶识别密集前缀，按真实表extent和成本参数保留堆扫描；未知探测至少计1行成本。

同一4096行×3列、父代／最终C6／同一父代回测各3进程交错运行；wall中位数变化（正数为减少，相对首轮／回测）：热点前缀（3200行）6.88%／-3.83%；低频前缀（1行）68.01%／67.77%；不存在前缀（0行）69.40%／68.72%；显式ESCAPE前缀（1行）71.84%／71.06%。仅代表本负载；C3热点超过3倍耗时的失败尝试完整保留。

最终C6通过新原生310检查、原13068与独立冷读105检查、37个相关原生入口；纯前缀组件O2和ASan/UBSan各72567检查，启用泄漏检测。新123条协议命令含27次实际扫描断言；32个相关旧协议入口29正常0、3项严格失败与父代完整观察相同。真实PG18.6以两个会话重放共享65条SQL，935个完整字段/NULL/OID逐项相同，3个EXPLAIN ANALYZE实际行数相同；未比较PG的AM路径、成本或速度。

详见[性能报告](performance-report-2026-10-30.md)与压缩原始证据（可选本地文件 `benchmarks/like-prefix-scan-20261105.json.gz`）。本批仍为partial，Goal active。


<!-- CURRENT_ORDINARY_QUERY_PROFILE_20261105 -->

## 普通查询的规划元数据与解析开销（2026-11-05，P16-C2）

本批在一次普通查询规划内复用关系schema，并按需各读取一次B-tree/Hash/Bloom/GiST列清单；主键与不可能使用位图/GiST的条件省去无用发现。普通表达式先排除不可能的类型常量前缀，避免对数字、表达式分隔符前的列名做推测类型解析及异常处理。真正的限定/引号/修饰/复合类型常量与显式CAST仍使用原声明语法。

交错的8倍调用补测，父代／最终C2／同一父代回测各3进程；wall中位数变化（正数为减少，相对首轮／回测）为：热点等值（3200行）12.15%／-1.12%；低频等值（1行）15.17%／18.32%；不存在等值（0行）17.94%／14.60%；主键等值（1行）25.52%／19.68%。全部短时及补测的正负结果保留，不能推广为全部操作或PG速度。

最终C2通过原生13068与独立冷读105检查、32个规划/存储入口和14个语法入口；另2个冷目录原生入口保留父代完全相同的严格失败。新65条协议命令含10个实际扫描断言，原有73条等值协议通过；另30个相关旧协议入口27通过、3个严格失败与父代完整观察相同。实际PG18.6的共享25条SQL仍为7331/7331逻辑单元格、9/9行估计一致，8/9路径相同；不是完整兼容验收。

详见[性能报告](performance-report-2026-10-30.md)与压缩原始证据（可选本地文件 `benchmarks/ordinary-query-profile-20261105.json.gz`）。本批是普通读取partial进展，Goal active。


<!-- CURRENT_EQUALITY_SCAN_COST_20261105 -->

## 普通等值查询的实际扫描选择与完整网络成本（2026-11-05，P15-C7）

本批让普通原生标量堆表查询实际消费基于ANALYZE频率的扫描选择：热点等值可选顺序扫描，低频等值和主键保留索引路径。此前只修改规划器时，无ORDER BY的普通查询仍绕开物理计划；现在结构化输出已接通，并修复无可用索引OR的显示文本合并，以保留重复行、NULL、空串和换行。B-tree、位图AND/OR与GiST在探测前取得语句快照，并在快照不适合当前索引或探测期间提交边界改变时读取完整可见堆候选，再做原条件检查。

这是普通读取的partial进展，使用现有原生 `.stats` 中的MCV/计数；尚未让优化器全面消费新的typed `.analyzed_statistics` 槽。普通EXPLAIN的文本缓存键现包含精确的实时成本设置；没有修改已暂缓的retained/custom/generic规划、参数、portal/format2或mutation/provider生命周期。当前私有普通后端P15-C7 SHA256为 `3ff5f16a9cc3a223d6618db87d8a5bbe85240dfd4c08cfdceafeb6ba49c03d3a`。默认根程序425与既有服务保持；Goal active，PostgreSQL整体功能仍partial。
同一4096行×3列、同一真实SQL与完整网络返回，父代／最终C7／同一父代回测各3进程，固定9个完成测量的进程，不重试计时查询；另两次在基线装载/启动阶段失败、没有计时样本，完整保留。wall中位数相对首轮父代／回测的变化（正值为减少、负值为增加）：热点等值（3200行）12.51%／2.45%；低频等值（1行）-5.57%／-5.47%；不存在等值（0行）77.31%／77.36%；主键等值（1行）-11.11%／9.44%。正负变化均如实保留；仅代表这四个普通读取负载，不能推广为全部操作、通用吞吐或PG速度。

新原生13068检查与独立冷读105检查通过，纯频率组件O2及ASan/UBSan各17检查（启用泄漏检测）。24个相关原生入口实际正常0，含完整7worker恢复1974检查和14份子进程完整输出；这些入口实际链接最终C7的61个核心对象。新73条普通协议命令正常0，验证实际扫描计数、全部行值/NULL及旧快照；26个旧协议入口中23正常0，另3保留与父代一致的严格失败，未称全套通过。真实PG18.6对共享25条命令的7331个逻辑单元格全部逐字相同，9个EXPLAIN行数相同，8个扫描选择相同、1处成本边界扫描选择不同。

原始测量、失败与独立复核见压缩机器归档（可选本地文件 `benchmarks/equality-scan-cost-20261105.json.gz`）和[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_PG_STATS_SOURCE_20261105 -->

## pg_stats 完整基础查询与读取成本（2026-11-05，P14-C6）

本批接通实际 `pg_catalog.pg_stats` 的17列基础SQL查询、参数为零的纯Describe、文本/二进制/混合结果格式，以及实际 owning anyarray 输出；支持已测的过滤、投影、排序、LIMIT、别名、CTE和JOIN。每次查询只读取当前数据库的真实统计与目录，重用本次查询内的关系/属性索引和每关系一次的物理模式校验，减少逐列重复扫描、模式复制和文件散列。

本节更新下方P13当时“17列SQL尚未接通”的结论；下方历史批次保留当时事实，当前状态以本节为准。当前私有普通后端P14-C6为 `9362b02aba17d45038702211ba42a6a1d797ef3580d97edb8ffabce80e0f63b6`。默认根程序425和既有服务未切换，Goal保持active；整体PostgreSQL功能仍为partial。
同功能未索引C2／最终C6／同一C2回测各3进程，每进程先1次不计时完整查询，再计时3次完整17列SQL查询API。16张表×30列×32行，实际512行/15360存储单元格/1696个NULL；每次查询返回480行×17列。wall中位数384.334／245.178／379.841ms，降低36.21%／35.45%；process CPU中位数392.205／254.151／387.508ms，降低35.20%／34.41%。这是该负载的完整原生SQL API对照，不是任意查询、网络端到端、已发布父代或PostgreSQL速度对照。所有9个样本及长尾保留。

新原生109检查与独立冷读90检查、投影组件O2和ASan/UBSan各548检查通过；34个相关原生入口全部正常0，包含完整7worker恢复1974检查及14份完整子进程输出。普通后端新协议26条SQL及旧标量25/数组19/范围19条自动统计协议通过。真实PG18.6同26条协议命令991个字段值：938逐字相同、53处严格差异保留；17列二进制153项中145相同、8处为实际存储宽度差异，六种范围的24个直接二进制值全部相同。另510个有限float4输入的文本和二进制身份均与PG一致。没有全库SAN或整体PG等价结论。

原始测量、失败及逐字复核见压缩机器归档（可选本地文件 `benchmarks/pg-stats-source-20261105.json.gz`）与[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_RANGE_COLLECTOR_20261105 -->

## 范围统计采集与完整 ANALYZE 成本（2026-11-05，P13-C2）

本批新增六种内置范围类型int4range/int8range/numrange/daterange/tsrange/tstzrange及实际范围域的ANALYZE采集：独立排序下界和上界、保存范围边界与长度直方图、空范围比例、NULL比例和实际存储宽度。边界保留包含关系、无界端点和子类型语义；本次样本局部缓存解析键，避免排序时反复解析。

最终私有普通后端P13-C2为 `3593b23e5e142605fe71149daec3347015003fb7a84d8a2505c80578e4e77a5d`，默认根程序425保持。完整pg_stats目录/17列SQL及output-only anyarray尚未接通，两条实际探针仍严格失败0A000。多范围尚缺真实目录/存储拥有者；本批仍为采集阶段的partial进展，Goal active。
本批同功能未缓存C1／缓存C2／同一C1回测，各3进程；每进程2048行、1个整数和六种范围，先1次不计时预检，再计时3次完整ANALYZE API。process CPU中位数388.224／201.363／391.997ms，降低48.13%／48.63%；wall中位数441.019／250.031／456.067ms，降低43.31%／45.18%。所有9次原始测量均保留；这不是任意SELECT、父代已发布后端或PG速度对照。

原生范围845检查及冷读262通过；组件O2和ASan/UBSan各43检查（启用泄漏检测，非全库SAN）。21个相关原生入口含完整7worker恢复1574检查全部正常0。原数组786+冷读191、标量641+冷读2、范围19条/数组19条/标量25条协议SQL通过。真实PG18.6同一520条SQL对照18列243项：229相同，14处严格差异保留（13处存储宽度、1处numeric NaN长度排序）。没有PG整体通过结论。

原始测量、失败和复核见压缩机器归档（可选本地文件 `benchmarks/analyzed-range-collector-20261105.json.gz`）及[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_ARRAY_COLLECTOR_20261105 -->

## 数组统计采集与完整 ANALYZE 成本（2026-11-05，P12-C3）

本批把真实数组存储输入接到同一次ANALYZE采样：保存整数组MCV/直方图/相关性，以及常见元素、按非NULL数组计算的成员频率和不同元素数直方图。实际验证integer/text/numeric/boolean/float8数组的NULL、空数组、字面NULL、重复值、多维/下界和冷读；其他元素类型及全部PostgreSQL功能仍需分别验收。

最终私有普通后端P12-C3为 `7d3cc2a1353c105ecac62579a919ec96d088cd325fa74234bc47252061e1029a`，默认根程序425保持。完整pg_stats目录/17列SQL与output-only anyarray尚未接通，两条实际探针仍严格失败0A000；本批是采集阶段的partial进展，Goal active。
完整ANALYZE对照为本批同功能未缓存C2／缓存C3／同一C2回测，各3进程；每进程2048行、1个整数和3类数组，先1次不计时预检，再计时3次完整API。process CPU中位数1243.545／164.180／1296.411ms，降低86.80%／87.34%；wall降低85.86%／86.87%。最终版685.690ms wall长尾保留。此结果限于该负载，未测任意SELECT或PG速度。

原生数组786检查及冷读191通过；计算组件O2和ASan/UBSan各32检查（启用泄漏检测，非全库SAN）；17个相关原生入口含完整7worker恢复1574检查全部正常0。原标量641+冷读2、数组19条协议SQL和原标量25条协议SQL通过。真实PG18.6同一554条SQL对照14列325项：301相同，24处严格差异保留（11宽度、11collation OID、2处相同频率元素的截断选择）。没有PG整体通过结论。

原始测量、失败和复核见压缩机器归档（可选本地文件 `benchmarks/analyzed-array-collector-20261105.json.gz`）及[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_SCALAR_COLLECTOR_20261105 -->

## 标量统计采集与 ANALYZE 优化（2026-11-05，P11-C4）

本批把真实ANALYZE扫描接入标量统计快照：保留NULL、实际存储宽度和原始扫描顺序，按统计目标维护有界样本，生成signed float4 distinct、MCV、排除MCV后的直方图及相关性。数组/范围专用采集、完整继承/表达式索引统计、真实pg_stats目录源和anyarray输出仍待完成；不是完整17列SQL功能验收。

最终私有普通后端为P11-C4 `e1cab6f6…`；默认根程序425保持。原生641检查及fresh冷读2检查通过，统计计算组件O2与ASan/UBSan各22检查通过（启用泄漏检测，非全库SAN），16个相关原生入口全部正常0。普通Main的CREATE/INSERT/ANALYZE/SELECT四条命令通过，464B实际快照独立解码核对两列；两条完整pg_stats SQL探针仍严格失败0A000，服务器正常0。

相同输入、匹配en_US默认排序规则的真实PG18.6对照共189项：184相同，保留3处实际存储宽度差异和文本槽2处collation OID差异（本地0/PG100）。样本边界按独立样本检查有效范围，不要求随机结果相同。原C-locale参考的文本相关性差异也保留；没有修改SQL语料以取得通过。

本批新增三个永久入口：analyzed_scalar_statistics_test（真实DDL/heap/ANALYZE，641检查和独立cold2）与analyzed_scalar_collector_test（22个计算/边界控制）。最终16相关入口均0；计算组件启用leaks的ASan/UBSan通过，非整个Capture/StorageEngine/数据库的SAN。Main4条SQL成功，pg_stats2条0A000仍失败；PG189中保留5个差异。旧2342测试与725保护记录保持，当前2345/顶层1094/去桩1093/子目录5。

原始输入、正常和失败构建/执行、PG两种locale及完整9进程成本见[报告](performance-report-2026-10-30.md)和归档（可选本地文件 `benchmarks/analyzed-scalar-collector-20261105.json.gz`）。

最终C4还修复默认自动分析的可见性：旧C3在批量INSERT达到阈值时生成空列快照；现在普通事务成功COMMIT、清理原命令视图与锁后，按已有改动表集合触发分析。失败/回滚不提前发布；只有分析成功才扣除已统计修改数，保留并发新增计数。维护错误不把已持久提交伪装成失败。25条普通协议SQL验证INSERT55、显式事务提交110、UPDATE110、DELETE50、回滚不改快照、ALTER SYSTEM+reload关闭自动分析后显式分析105，并核对5次EXPLAIN基数；服务器正常0。原恢复入口1574检查及7个worker全部正常0。共享IO/分配失败、两阶段提交的自动统计调度及显式事务内ANALYZE的全部回滚语义仍未完整验收。

按用户清理要求删除93209个旧编译文件，实际空闲增加约658GiB；分区85%→72%、空闲约1.5TiB，项目约111→28GiB。源码、原2342测试、全部已跟踪文件、当前C4/default425及PG18参考工具已核对保持。后续以现有CMake缓存和当前core复用构建，证据归档后清理可重建产物。删除在9样本全部结束后进行；只读du盘点与最终对照的回测段重叠，全部结果保留，不称为无并发负载测量。

第三入口为tests/analyzed_scalar_auto_protocol_e2e_test.py，25条SQL原始wire和5份真实快照均留存。两个配置控制草稿误用SET/SET GLOBAL，被当前SQL模式拒绝，错误原样保留；最终沿用既有ALTER SYSTEM及pg_reload_conf，未改配置策略。


<!-- CURRENT_ANALYZED_STATS_SNAPSHOT_20261105 -->

## 完整统计视图的快照基础（2026-11-05，P10）

P10新增3个生产头文件及 `analyzed_statistics_snapshot_test.cpp`，源234/头164，原2341测试文件保持，新增后2342；顶层1092 CPP含test_stubs（排除后1091入口），另5个子目录CPP。正常CMake没有重编译，62旧对象与后端 `30c57ca8…` 保持；正式原生测试采用冻结的新头文件、经哈希核对的61个旧core对象和实际新编译stub794，既有161头/231源码字节不变。

同一组件测试在未优化O2、最终O2、最终ASan/UBSan和正式core入口分别通过：每次父进程568次检查，3个fresh子进程各1次检查。包含独立Python220B固定字节夹具、逐字节损坏/全截断前缀、修复校验后的非法字段、拥有数组/记录/域身份、冷读、实际OFD阻塞的并发写入、关系整组替换和代别耗尽；这些是组件检查，不是568个SQL场景。SAN覆盖组件与StatsPersistence，启用泄漏检测，不是全库SAN。

真实PG18.6参考核对17字段并采集8列的全部7类槽及实际数组元素OID；仅声明/类型契约对应，不宣称产品统计数值已与PG逐项一致。当前普通Main的两条全17列查询均0A000、server正常0；记录为2严格失败。9次组件成本测试全部正常0且完整payload相同。[完整报告](performance-report-2026-10-30.md#完整统计视图的快照基础2026-11-05p10)及归档（可选本地文件 `benchmarks/analyzed-statistics-snapshot-20261105.json.gz`）。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧节保留各批次当时结果。


<!-- CURRENT_STORAGE_LIFETIME_20261105 -->

## 存储退出期生命周期（2026-11-05，P9-C2）

最终P9-C2正常程序 `30c57ca8…`，源231/头161；C1、C2各按正常CMake重编TableManage一个单元，另61对象依赖及fresh62血缘核对，fresh stub794。新永久 `relation_list_lifetime_test.cpp` 的9控制在main和更早owner析构各执行一次，最终18次检查通过；旧O2在第11次检查失败，旧ASan报use-after-free，新ASan/UBSan通过。原562项名称/列表/WAL组件草稿在旧/新O2与ASan/UBSan四次均通过，仅作兼容性控制，不验收R218功能族。

最终23个相关入口、新生命周期入口及原七阶段恢复均通过；完整七阶段Memcheck父进程+7worker均0错误，未启用泄漏检查。P9-C1的advance134和Memcheck冷进程维护表错误完整保留。正常Main固定22对象与P8-C3/P9-C1相同，含轮询计数为C1 219/224、最终C2 212/217，各5项元数据失败，两个进程上下文各正常0。

原2340测试文件逐字保持，新增1文件后2341；顶层1091 CPP含test_stubs，排除后1090入口，另5个子目录CPP。完整恢复9次/108检查通过，无重采；详见[报告](performance-report-2026-10-30.md#存储退出期生命周期2026-11-05p9-c2)及归档（可选本地文件 `benchmarks/relation-list-engine-lifetime-20261105.json.gz`）。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user，Goal active。下方旧节保留当时版本的结果。


<!-- CURRENT_RESTORE_TRANSACTION_HORIZON_20261105 -->

## 物理恢复事务号水位（2026-11-05，P8-C3）

本批最终P8-C3 `d778afbd…`，源231/头161；C++17、O2、ICU、zlib及TLS stub保持。正常CMake在C1按真实依赖重编16个单元，C2重编WAL一个、C3重编TableManage一个，追溯原fresh62构建；测试桩794重新编译并冻结。相关21个旧入口通过；独立文件读取19控制、事务分配器8个顶层及3个fresh-peer控制、WAL32控制通过。并发测试的3种语义由5个实际工作进程执行，全部正常退出。

同一七阶段原生控制：P7-C1在较早目标上数据失败；P8-C1七阶段全部通过；P8-C2/C3均在advance阶段出现wait状态134，后四阶段NOTRUN。已有实际写入循环，精确异常位置/原因未知，必须保留严格FAIL。独立低目标数据探针及正确目标计时通过不替代此入口。

正常Main原始固定22操作在C1/C2/C3的完整固定响应逐字节一致，首次探针为4/4；各版均有5项探针描述符/缺目录身份失败，其他值、命令、NULL和冷启动检查通过。含实际轮询的检查计数分别为C1 249/254、C2 234/239、C3 209/214；这三个检查总数不同不代表改变固定SQL。旧版首次1/4及冷启动−6完整保留，未作原因推断或重试。

新增3个原生入口和1个同源夹具头，测试文件2336→2340，原2336字节保持；顶层1090个CPP含test_stubs，排除该桩后1089个入口，另5个子目录CPP。注册时缺夹具头、缺wrap注解的首轮构建失败保留；补齐同源夹具头和首行wrap注解后，并发入口93335实际编译运行exit0；最终夹具头及七阶段入口仅清理原型注释，控制体和断言保持。新入口不能概括为全部通过。两轮公开恢复各12次、各144检查全部通过，所有长尾及完整失败均见[完整报告](performance-report-2026-10-30.md#物理恢复事务号水位2026-11-05p8-c3)和机器归档（可选本地文件 `benchmarks/restore-transaction-horizon-20261105.json.gz`）。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧节保留各批次当时结果，不能覆盖本节的失败与范围。


<!-- CURRENT_BACKUP_ANALYZE_20261104 -->

## 备份摘要与 virtual 列统计（2026-11-04，P7-C1）

本批 P7-C1 `724c1147…`，0.2.0 / C++17 / GCC13.3 / O2 / ICU74.2 / zlib1.3 / TLS stub。ANALYZE永久20控制由旧14通过/6失败变为20/0；摘要永久864/0，独立固定预期全部保留；相关12个原有入口全部通过。原2334测试源字节保持，现2336；顶层1087 CPP（排除桩1086），另5个子目录CPP，未全仓重跑。

PG18.6原34参考全部保留，两处最初帧顺序预测错误亦保留；当前正常Main同34为17匹配/17差异，零服务故障、正常退出。差异为11个qualified pg_stats入口、REINDEX INDEX与ANALYZE列列表各1、错误帧2、DELETE空描述帧2。实际停止后的.stats证实普通/STORED统计存在、virtual统计缺席；不代替缺失的SQL视图。

两代创建备份清单与所有载荷摘要完全一致。首轮旧→旧恢复探针行数失败保留；独立既有探针目标的四组交叉恢复均通过完整1000+4行/真实RID/NULL。完整API9与正常SQL DDL72均成功，wall p50为2快/2慢，ALTER与TRUNCATE慢于两次旧版。预检和所有原始失败见[完整报告](performance-report-2026-10-30.md)及机器归档（可选本地文件 `benchmarks/backup-analyze-20261104.json.gz`）。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方按批次保留当时记录，当前状态以本节为准。


<!-- CURRENT_GENERATED_DEMAND_20261104 -->

## 虚拟生成列读值需求与完整 SQL 实测（2026-11-04）

本批源码 P6-C8，对应正常程序 SHA256 `45264111…`，保持 0.2.0 / C++17 / GCC 13.3 / O2 / ICU 74.2 / zlib 1.3 / TLS stub。根目录默认程序仍为 `425165af…`，现有服务未重启。

新永久原生入口[读取需求36控制](../tests/generated_column_demand_test.cpp)与[真实spill身份10控制](../tests/generated_context_spill_validation_test.cpp)通过；实际运行C6的61个核心对象/159头文件与当前C8一致。相关25原入口在C5为24/1，旧C2同一quoted extract普通控制也失败，本批未修，未新增deferred；不称当前全仓重跑。

C8固定258为238通过/20差异（13/3/2/2），较C2新增82整项通过、零整项回退，另有6项描述符组件损失。原767观察仍129差异，完整记录对旧版/C6一致、九enum原始记录一致。所有原始SQL、预期、失败与raw保留。

当前Main15个实际上下文正常退出；72个正常基准请求、7200行/25200单元零失败。原2332测试源字节不变，现2334；顶层1085 CPP（排除桩1084），另5子目录CPP。首批编译/包装器/接收器缺口与拒绝C7见[详细报告](performance-report-2026-10-30.md)及机器归档（可选本地文件 `benchmarks/generated-column-demand-20261104.json.gz`）。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧批次保留当时事实，当前状态以本节为准。


## ANALYZE 与 REINDEX 成本优化（2026-11-04）

本批源码 P5-C2（a3baaca6…）优化同次 ANALYZE 的类型/locale 比较和真实行 NULL 位图，以及 REINDEX 逻辑 map 保留。C++17、原维护频率/持久化和用户暂缓边界保持。ANALYZE p50 369–403→74 ms，范围 UPDATE 600–650→389 ms；完整 35 操作为4快/28慢/3居间，36组查询为4快/6慢/26居间，整体性能提升尚未验收，默认仍为425，现有服务未重启。

原相关22入口全过；新比较8规模/240完整字段、bitmap caller新旧各12/0、REINDEX拥有值/RID8/0，私有观察器8/3344与完整PUBLIC60阶段分开。新增3个永久入口，原2329测试字节不变，现2332文件/顶层1083 CPP（排除桩后1082顶层入口，另有5子目录CPP）；未宣称全套重跑。55资源、630动作、1638查询均完整收集；首次预检/编译/观察失败保留。

详见[完整性能报告](performance-report-2026-10-30.md)和本批机器归档（可选本地文件 `benchmarks/analyze-reindex-main-costs-20261104.json.gz`）。下方旧批次保留其当时结论，当前以本节与报告首节为准。

## C4功能与完整成本记录（2026-11-04）

当前源码C4（9d85c688…）补入RETURNING纯声明与真实来源元数据、虚拟列拥有值及部分MERGE来源输出。新增123原生控制122通过/1个严格元数据差异，新132协议对照全过，原56项为55通过/1差异；273项状态未变，Goal继续active。

实际35操作为7快/19慢/9居间，范围UPDATE及REINDEX仍慢；36组完整查询25快/2慢/9居间。未通过本机默认程序的整体性能提升验收，默认仍为425（425165af…），现有服务未重启。独立API512行的分配/CPU收益及空表/单行负值分开报告，不能概括为所有数据库操作已提速。

新增5入口：[声明46](../tests/returning_declaration_api_test.cpp)45/1，[虚拟列28](../tests/virtual_generated_owning_test.cpp)、[语法14](../tests/local_expression_syntax_test.cpp)、[REINDEX16](../tests/reindex_single_pass_test.cpp)、[MERGE19](../tests/merge_returning_source_owning_test.cpp)全部通过。quoted stored extract严格integer预期保留。原2324测试字节未改，现2329文件、1080个cpp文件（排除链接桩test_stubs.cpp后为1079原生入口），未宣称全部重跑。C4选取57原生56/1、31协议24/7，10533共同观察无旧正确变错；严格56旧48/8→55/1，新132旧9/123→132/0，新33生成语义仍27/6。原579为451匹配/128差异，全部raw/canonical/原正确组件连续；暂停enum九项保持，原341/103未执行。

详见[本批报告](performance-report-2026-10-30.md#returning-声明真实行值与操作热路径2026-11-04)及机器归档（可选本地文件 `benchmarks/returning-declarations-hotpaths-20261104.json.gz`）。以下为历史记录。

## 2026-11-03 内置函数注册与复合 RETURNING

最终功能程序C3 `425165af…` 与初期C2宽回归按不同代别记录，不能将所有通过项拼成C3全量验收：

| 实际程序与入口 | 完整终态 | 范围说明 |
| --- | --- | --- |
| C2宽原生147 | 145通过/2既有失败 | 原math、routine失败保留 |
| C2宽协议95 | 86通过/8完整语义失败/1启动失败 | 启动超时103、0 SQL；原记录保留 |
| C8宽协议95参考 | 85通过/10未通过 | 其中2入口未完整观察，不能称10个已完成语义矩阵 |
| C3原生选取56+新增RETURNING1 | 57入口、56通过/1既有math失败 | 新注册14控制与新RETURNING20控制通过 |
| C3协议选取31+CASE准备1 | 32入口、24通过/8既有失败 | CASE准备额外入口保留3项旧差异 |
| 固定301类型协议 | 188匹配/113差异 | 仅第254项INSERT binary RETURNING新增通过，其他300项完整实际响应保持 |
| 固定83/111/68/16 | 278项实际响应与C8相同 | 既有配置、REGTYPE等差异没有改预期 |
| 新PG参考RETURNING56 | C8为36通过/20差异，C3为48/8 | DELETE42内容和事务后状态正确，仍缺描述符 |

新增成本夹具分别记录默认分配器与累计new观察，不把单次局部CPU当作完整SQL分布：

| 局部夹具 | C8→C3实际观察 | 解释范围 |
| --- | --- | --- |
| extra helper，512行×3，捕获关闭 | 1,218,134→213,590次请求；88,677,248→14,491,520B | 累计C++new请求字节，不是live/峰值/RSS |
| 同夹具，捕获开启 | 1,450,070→445,526次请求；126,917,504→52,731,776B | 拥有值捕获的原功能校验保留 |
| 默认分配器extra helper，捕获关闭 | 单次进程CPU466.525→56.841ms | 独立辅助循环，不是整SQL或PG速度 |
| 公开游标512行 | 分配不变；单次冷CPU20.750→35.862ms | 慢值保留，不以helper收益替代 |
| 独立构造器 | 首次多1次请求/96B；预热后64次构造及25,216项检查中29,760→8,704次请求 | 共享表冷成本与常驻成本仍存在 |

原2322测试文件保持，新两个原生文件共34控制接入后为2324文件、1074个原生入口；本批头文件157个，ExprEvaluator实际sizeof保持248B；文件数、入口数与控制数不混算。新RETURNING20同源C8为17失败、C3全部通过。首轮原生20/2源于测试在另一序列会话读取currval，源码/失败保留，仅将原setval/currval观察置于同一真实Session；SQL与20项预期不变。C2公共回调重入诊断的2处回退由C3修复，原3项完整实际观察恢复到C8。

新56项剩余22/23/24/40/41/42只缺OLD/NEW别名描述符，47为虚拟数组载荷，53为历史数组NULL位图；均为普通未完项，未新增暂缓。完整查询S3/C3/S3的1638请求（1296计时、324预热、18 SET/SHOW（9+9））全终态，36组p50均低于两次S3，每预算12/0/0；不是与PG新速度比较。35类操作C8/C3/C8为630样本、每标签210项完整校验、3进程正常结束，18快/2慢/15居间；update_range和reindex为慢项，基线漂移全部保留。

全部证据、旧启动失败及性能结果见[本批报告](performance-report-2026-10-30.md#内置函数注册与复合-returning2026-11-03)；独立机器归档见全部原始统计（可选本地文件 `benchmarks/function-registry-returning-20261103.json.gz`）。以下C8及更早记录保留原始代别、失败和时钟数据。

## 2026-11-03 原始类型行物化分配优化

C8 `91d339db…` 仅重编实际修改的ExecCPP，沿用有效的新ABI对象谱系。选取的26原生入口全部通过；31协议入口仍为24通过/7既有失败入口。固定301观察仍有114差异，83/111各两项配置差异，REGTYPE68仍为57通过/11普通差异，参数16全部通过；这些入口的完整实际响应与C7相同，没有以修改预期刷绿。2322个既有测试文件均未改，不能将选取集称为完整106/95重跑。

实际512行公开游标的累计分配比C7少1536次请求/393216B，比S3少1024次请求、冷次少188256B/重启少188416B。完整查询唯一窗口的36组为5快/18慢/13居间，基线漂移和全部慢项保留；分配减少不是RSS或普遍速度结论。完整收据及原始样本见[本批报告](performance-report-2026-10-30.md#原始类型行物化分配优化2026-11-03)和机器归档（可选本地文件 `benchmarks/owned-datum-primitive-20261103.json.gz`）。下列C7及更早记录按其当时输入保留，原273与既有暂缓不变。

## 2026-11-03 拥有值复合输出与引用类型存储

本节对应基线 `6c27d232` 与最终功能程序 C7 `f135335e…` 的 226 源码输入。先以完整 fresh 构建建立新 ABI，再按真实依赖重编；C7 两个实际 CPP 重编、60 个对象经谱系核对复用，不能称每代均重编全部 62 TU。原始失败、首轮构建/配置问题与后续正确谱系分别保留。

| 批次/入口 | 实际结果 | 解释 |
| --- | --- | --- |
| C5 完整原生 106 | 105 通过/1 既有失败 | 例程数组签名入口的原退出失败保留 |
| C5 完整协议 95 | 86 通过/9 既有失败入口 | 5011 明细比较无新增错误结果，220 完整相等；不是全部通过 |
| C7 最终选取原生 26 | 全部通过 | 包含三个新增入口 38 控制；不是全库全量 |
| C7 最终选取协议 31 | 24 通过/7 既有失败入口 | bit、name、Bind、CTE、nested、interval、143 的完整失败继续保留 |
| 固定 PG18.6 301 观察 | C7 114 差异 | 比 AA52 的 201 差异改善 87，原正例未改成错误预期 |
| 数组位图/CAST 83 | 81 通过/2 差异 | 原 SHOW/UTC 配置预期不放宽 |
| 多态数组 111 | 109 通过/2 差异 | 原 SHOW/UTC 配置预期不放宽 |
| REGTYPE 真实物理 68 | 57 通过/11 差异 | 同数据目录两次进程 epoch，索引/COPY/冷读控制保留 |
| REGTYPE 正常参数 16 | 全部通过 | 文本/二进制、NULL、空值、边界及原错误检查 |

三个新增入口为 `owned_compound_spill_codec_test.cpp`（16）、`owned_compound_physical_spill_test.cpp`（12）、`numeric_special_binary_output_test.cpp`（10），共 38 控制。原 2319 测试逐文件保持，新增后为 2322 文件；文件、入口和观察不是同一种计数，不相加称已覆盖功能数。

C4→C6 的 REGTYPE68 从 29 差异降至 13，C7 再修正 CAST 输入数组声明与实际 PreparedReturningExecution 的行物化，成为 11；C6/C7 完整实际响应仅第 10/42 项改变，其余 66 项逐帧一致。C7 原参数16仍全过；自有两个同目录物理进程和参数进程均正常退出。SHOW、链式 CAST、域重命名/删除/重建连锁及 USING 的 11 差异全部按真实普通依赖列在[本批功能与成本报告](performance-report-2026-10-30.md#拥有值的复合类型输出与引用类型存储2026-11-03)，不笼统称暂停项。

原 273 完整对象/状态及 251 条历史原文须保持；本批新增近期记录只为 partial。资源55与1638请求执行完整通过值校验；四个公开成本进程完整3例/0失败。36组速度为4快/23慢/9居间，默认分配器和计数器分开。原 35 类操作的历史停机失败不重写，也不以本轮正确退出控制代替该验收。

## 2026-11-03 窗口暂存候选与类型协议调研

基线 `3d576b8b` 的S3与scratch1/scratch2严格隔离。两候选均完成既有71原生入口+永久10控制入口（72个不同入口）全部通过，额外cold6每代6/0；没有新增永久测试或修改既有2319文件。
两候选各自完整既有61协议入口为56通过/5保留失败，加220入口为62个不同入口57/5。五失败完整日志或JSON逐字保持S3；原四1641及143/8保持，不调查暂停模块。scratch2首轮220完整值/日志0失败但未持久化外层退出码；同源独立终态复核220/0、exit0且完整响应一致，首轮未覆盖。
固定301新调研观察的独立PG18.6参考完整通过预期阶段/字节/来源锚校验；三版实际响应逐项相同，各有201处PG差异，候选driver退出1但自有服务器正常退出0，不计PG兼容验收通过。误连17.2的版本guard失败和本任务UUID库清理结果保留。
每候选独立55资源请求/40000行118000单元、5正常context及预期只读kill/reopen通过；作用为普通最终Sort与这些实际读取控制，不证明窗口预算/写恢复/R15已修。每候选另一次完整1638请求/1620000行4725000单元、9正常context通过，全部1296计时保留。两版均因未获稳定跨预算收益而拒绝，未安装。
类型输出提案仅有ExprEvaluator/TypedSortSpill/PostgresProtocol统一树syntax-only通过；首轮混头63重定义错误、metadata静态阻断与全部依赖仍保留，未称全生产编译/运行通过。安装程序及生产源码恢复/保持S3，Goal active；数据与复现输入见[本批报告](performance-report-2026-10-30.md#窗口暂存分配与类型输出调研2026-11-03)。

## 2026-11-03 窗口结果共享与排序投影

S3 `aa52b5fe…` 使用冻结216源/62生产对象，147头、Main、网络协议、实际frame/context/provider及原共享比较/R177/R185保持；只在ExecCPP增加私有状态/接线，并扩展私有窗口宿主。
原71个原生入口全部通过，加新10控制入口，共72个不同入口通过。新10直接读取512行物理表，经真实绑定/公开cursor验证重复纯LAG/SUM、完整嵌套record/OID数组、同图重启/提前close、独立命名窗口、LIMIT0及22012。
独立审查后加强错误前缀为完整integer行[[1,0,0],[2,-1,-1]]、3列真实OID23及两次重启一致；冻结a585bd7a源在C1/S1/S2/S3均10/0。先前较弱源和所有日志保留。
新固定220观察包括53条窗口查询及DDL/序列seed/post，独立PG18.6为220/0，原C1为220/27，S1/S2/S3均220/0；三版全部case/fullactual/raw/normalized/match一致。
原155观察完整保留，归一化仅由真实id LIMIT0锚定该物理表OID，不放宽类型/格式/消息次序/返回行/错误/计数。稳定/不可变/易变函数、命名声明、wrapper/CASE、hidden排序及双保留别名反例均严格保留。
S3旧61协议首次56pass/5fail，合并新220后62不同入口57pass/5fail；190项本轮首个完整入口通过。Bind463/40、CTE747/29、nested325/42全文log与S2字节一致；interval106/10及原143/8完整JSON字节一致，原四1641观察不变。
190的全部server observation/rawFrames/compatibility oracle及状态与S2成功独立观察相同；随机自有schema UUID导致请求SQL/frontend packet不同，没有归一化任何服务响应，也不声称整log byteequal。
S2旧61首轮55pass/6fail的连接103、planned190/recorded0/attempted0日志保留；终态后同源同S2独立一次完整190/0（53项预期SQL错误、cleanup/序列基线健康），按实际完成观察合并新220为62入口57/5；不改写S2首轮56/6。
原143恰42/43/85/86/128/129六项改善14→8，余137 fullactual/normalized/match不变。剩余catalog2/record-json错误需求3/compound format1 binary3仍开放，不笼统归入暂缓。

同源512行warm实际cursor的new请求：单LAG226489→226489、双LAG419723→232335、四LAG806565→244394、双SUM44542→43514；接收行和restart/close在计量内，初次建图在外。
单份请求字节只差-3B，是微小对象布局记账，不宣称内存收益。S2单份多1请求/8B的原始结果保留。S3仅对完整stage两套策略均无共享/无外来别名且原producer顺序成立时使用原类型化循环，隐藏重排或共享需求仍走完整通用路径。
请求字节、S2完整36项慢值、另行68/48进程诊断、S3资源和全量时钟分别见[本批报告](performance-report-2026-10-30.md#窗口结果共享与排序投影2026-11-03)，不混用累计请求/时钟/live/truepeak/RSS。
原2316测试文件均未改，新3资产按标准发现/单注册行接入；原273完整对象、249历史、725保护记录保留，新增近期记录为partial。文档后最终检查另按真实收据记录，整体Goal active。

S3实际Main资源55请求/40000行118000单元全部通过；完整1638请求/1296计时、三预算和9正常context成功。重复两/四份LAG(length)降低37.94%–40.83%/63.65%–66.34%，但原common8仍16/24组p50高于两次C1；全部36组11快/19慢/6夹两旧，不能宣称全面提速。完整原始12样本、min/max、漂移与S2历史均保留。

## 2026-11-03 窗口整数键缓存与实际执行成本

C1 `56d06bca…` 使用冻结216源输入、62生产对象；公共147头、Main、ExecCPP/共享Set/R185/R177/真实frame-context-provider均与b04基线字节一致。
原70原生全过，加新实际游标36控制入口全过，合计71个不同原生入口。相同新36在C9有3个成本断言失败，33个语义控制均通过；不是三个新SQL bug。
成本控制的warm实际cursor累计C++new 450603→73176，请求bytes19260574→10314925；原Input外层容量在warm已分配，不能当cold/activepeak/live/RSS下降。
新143在独立PG18.6全过、原C9和C1均14处差异且fullactual逐项相同；首次fixture错误要求用户OID>16383导致多3项失败，已仅改为实际pg_type正OID，原源/143-17/PG原预期保留。
14分类为catalog来源2、secondary record-json的rank错误前缀3、重复volatile window调用共享6、compound binary结果3；没有改期望、删SQL或将它们泛称暂停项。
原60协议仍56/4，Bind463/40、CTE747/29、nested325/42整log及interval106/10完整观察与C9一致；加新143入口后61unique/56pass5fail。没有把观察数当独立缺陷数。

公共API诊断分clocks/allocations/evidence独立进程；before与C1各48进程、144次1000行结果全部0，各144000行/396000单元。
16组owning行/descriptor/typeOID/来源/公开context及图rows/loops完全相同，仅排除跨进程指针数值和运行时间；同一图的重启身份由实际public cursor核对。
CPU p50降低41.85%–61.90%、C++分配请求53.89%–63.96%，未将allocation版时间混入clocks，也未将inclusive节点耗时相减后命名为exclusive comparator CPU。
初次建图每case仅一次、仍约11.6–38.2ms；解析/规划/建图与执行分别报告。C++请求字节不是malloc全量、live、peak或RSS。

实际Main普通finalSort匿名FD/EOF、owned强杀和同目录重开等43请求通过，原1072fixture不变，全部自有进程退出。该资源范围不包括Window partition/stage/cache的预算治理。
本批不重跑C9已有35操作shutdown失败，不把新的只读资源正常退出当作修复该停机问题。
额外allocator18进程/162采样/12006行24012单元全0；整数首/末行uordblks +68160B，文本+22544B，单行+48B，close保留容量/RSS不一致原样报告，不声称truepeak降低。
整SQL864计时+216预热+18SET-SHOW完整1098请求全值通过；4/16MB全部16项p50较两旧低22.3%–39.1%，64kB保留3个较两旧慢项及全部漂移，无重跑。
源码范围pre-doc29项、原273完整对象/raw数组与248历史、2313既有测试、725专属记录和754 Markdown路径均保持；最终版本及完整24项SQL样本见[本批报告](performance-report-2026-10-30.md#窗口整数键缓存与实际执行成本2026-11-03)。整体Goal仍active。

## 2026-11-03 类型化窗口宿主与有序输出复用

最终 C9 `c696d3b2…` 使用同代 61 core、Main 及已重编译且与当前公共头匹配的 stub；主回归 68 原生入口全部通过，另两个独立原生入口也通过，合计 70 个不同入口。
新 kernel 223、宿主 37、异常清理 3、interval 除法 39、计数绑定 30、有序输出执行 16，共 348 个控制全部通过。
覆盖真实行源/过滤、derived/CTE/相关作用域、真实 SourceContexts JOIN、三种参数 origin 同图重启、完整 typed owning 值/描述符、NULL/枚举/OID、提前关闭/EOF、运行错误前缀与 WITH TIES。
通用 Core JOIN lowering 未实现；上述 JOIN 结果不能推广为所有 Core 计划支持。

新永久协议四入口分别为窗口宿主 226、输入键 241、interval 除法 106、有序输出需求 201；完整 SQL/PG18.6 预期/错误阶段和原始响应均保留。
窗口宿主 226、有序输出 201 在最终 C9 全过；键 241 与 interval 106另用同一最终C9完整运行，按实际结果与主58合并为60个不同协议入口。
interval 106 的 10 个差异仅为常量错误的 Bind/Execute 阶段；值、类型、SQLSTATE 和 20 个 AVG 控制均符合严格预期。未改预期、未过滤失败、未恢复 R303 等暂缓调查。
最终同代60个不同协议入口为56通过/4失败；其中主协议回归58入口为55通过/3失败，另键241通过、interval106/10失败。新宿主与有序输出已包含，重复运行的同一 201 不重复累计。
既有失败是 Bind/portal 完整 463/40（日志字节与旧版相同）、CTE/window-owner 747/29 和 nested-window-projection 325/42；后两旧版分别 747/453、325/77。剩余差异仍开放，不能泛称全部属于参数阶段暂缓。
前批 binding58 的实际窗口子查询宿主缺口在当前入口已通过。前批完整910原始观察与其中独立SELECT的9处差异仍保留，本批没有把该历史完整矩阵描述为重新全量验收。

最终运行资源三阶段全过：共同查询旧/新/旧 24 次完整 1,000 行核对；不兼容最终排序在 64kB 实采最多2个匿名排序 FD，16MB 未采到，均验证1,000行/3,000单元且 EOF 清理；
自有进程匿名文件打开时 SIGKILL，原目录重开核对基表1,000行/4,000单元及再次1,000行排序。共43个工作负载请求，28个完成全值读取、1个强杀读取、14个SET/SHOW。
1072个源夹具文件不变。另35类操作每类一次全部数据校验成功，但server context退出时抛std::system_error: Invalid argument，返回-6，driver exit1且complete=false；该入口不能记为通过。原始失败保留，R15既有暂缓保持，未调查/修改停机链或重复挑选通过样本。它不是并发/任意写入崩溃或真实TLS验收。

C1..C9 的不可变二进制、源/对象输入、全部失败及修复前后观察保留；初次 native JOIN 夹具/错误阶段、序列脚本和 metadata 检查器问题单独标记，没有算作产品修复。
实际修复包括窗口子查询宿主、窗口键求值顺序/复用、真实 WindowClause 身份、继承/放置规则、FILTER 与参数需求、ntile 首行NULL、interval 分数级联、异常清理及最终输出排序需求。
C7相对旧58的14个新增回退曾被总失败数减少掩盖；逐观察对比后全部修复，原失败和C7已完成的64kB性能窗口完整保留。
新增原生16控制首轮15/1来自两个新增控制未显式启用低层Core常量规划，真实C8 Main同源PG14/0；只对新增15/16使用真实公开规划owner后16/0，原14逐字保留，没有改变通用Core默认规划策略。
C9仅在私有窗口read减少一次整行上下文复制、删除重复分区/排序键datum；三种origin/真实owner/已有输出快照不变，边界检查时点保留。
原2302个测试、原始273项完整对象/原子条款和725个保护文件字节不变；受保护共享排序助手、原聚合执行与真实参数帧/provider/portal边界审计通过。
方法、研究来源、所有慢值、开放项和本批证据路径见[窗口宿主报告](performance-report-2026-10-30.md#类型化窗口宿主与有序输出复用2026-11-03)。整体Goal保持active。

## 2026-11-02 全局计数表达式与动态排序上限

最终相同61 core的48原生入口全过；新执行33、绑定34、语法840控制全过，同最终源旧ABI分别33/31/572处不符。
新协议75与900在PG18.6/最终58c94e05…全过，旧版61/604处不符；binding58的PG全过、旧53、新1处差异仍是实际窗口子查询宿主缺口。
最终36协议按各入口完整结果34/2，既有Bind完整463/40日志与前版逐字节相同；首候选原33为31/2，普通SELECT超时失败保留，最终同源原超时入口通过，不改超时/连接/重试策略。
原完整910观察最终前900全过，4个独立SELECT的10观察中9处差异未删；没有把接受子集充作全部SELECT计数支持。

旧860中仅2个PG合法括号参数FETCH断言迁移为严格AST/源位置验证，其余858字节/顺序保持；原860/2日志保留。
旧VALUES63中1个K1强制落盘前提修正为动态NULL，保留真实落盘/22012/终止；新增K1控制由PG核定精确源/键计数25/24，最终64/0、旧64仅新成本契约1失败。
新执行最初32中2个同类资源前提错误与所有原源/日志保留，最终33严格验证有限无I/O→NULL落盘→有限不新增I/O、真实异常身份、原图/参数来源、取消/超时/EIO与有限FD。

12项实际Main资源全过，4161行/12483单元及精确描述符全值正确；最多2个排序FD、EOF清理，1072夹具文件未变。
35类操作每类一次是有限正确性检查；匿名动态计数排序文件内强杀、重开1000行/4000单元及再次2000行排序通过，不是任意写入恢复或真实TLS验收。
1620整SQL样本/405预热/36SET-SHOW与独立432执行样本/108预热分层保留，后者10800行/32400单元全部owning及同图校验通过，仍无普遍加速结论。
同代构建、全部失败、完整性能样本及开放项见[全局计数报告](performance-report-2026-10-30.md#全局计数表达式与动态排序上限2026-11-02)，证据目录 `build/pg-feature-global-count-xqbytfo5/`；原273/725与所有用户暂缓不变，整体Goal active。


## 2026-11-02 集合整数键与比较参数优化

原44原生全过，加19真实算子metadata/restart/spool/成本控制，最终45全过；旧19仅6项成本预算失败，功能metadata/resource全过。
新66在PG18.6/前版/新版同源码全过，33协议32/1；原112/143/33/59/52及既有463/40观察不变。
真实integer spool独立arity与公共源宽度错误的XX000/FD0/terminal next均核验，不称私有header破坏测试。
uppercase OID前提共47失败（2个DDL错误、45个级联）及harness错误保留；仅fixture物理BIGINT+真实o::oid，预期不变，未修该DDL。
所有累计分配（非RSS/CPU）、输入顺序计数、1980延迟及慢值、Main资源/35类有限操作/强杀重开见[集合键缓存报告](performance-report-2026-10-30.md#集合整数键缓存与比较参数复制2026-11-02)；原273/725及所有暂停边界不变。

## 2026-11-02 集合排序预算与 VALUES 查询子句

最终相同 61 core 的 44 个原生入口全部通过。新 SetSort 算子 26、语法 860、VALUES 执行 63 共 949 控制全过；同最终源码旧 ABI 基线分别 20/694/63 失败，均为观察数，不是独立 bug 数。
集合 112、VALUES 143、枚举 33、显式类型计数 59 四份固定协议矩阵在严格 PG18.6/最终程序全过，旧版分别 8/127/20/51 处不符；新增类型修饰/语法阶段 52 严格观察全过。
覆盖真共同类型/绑定范围、参数三 origin 同图重启、相关与 shadow 子查询、volatile 求值次数/顺序、NULL/负数/溢出、隐藏键同行、完整递归 metadata、实际匿名文件与有限 FD、提前关闭、取消和超时。

完整 31 协议首轮为 29/2；暂缓 Bind/portal 的 463 项完整日志及其中 40 处差异与前版逐字节相同。
另一失败在 start_ours 的 sock.connect、尚未发送 SQL；同一测试源码和程序独立重跑通过。最终逐入口复核为 30/1，首轮整批结果与原失败日志保留，未改连接策略、超时或重试逻辑。

新增52永久协议入口独立验证通过；全部32个已运行入口按各自最终结果复核为31/1。
首轮 C3 原生两失败源为夹具 collation C/c 及 grammar child/span 预期；C4 的 :: AST 形状与参数槽位四项预期也仅按真实绑定修正。
原失败、源、PG 预期和同源码旧版均留存，不把它们算作生产修复。真实 TIES cancel/timeout 清理、CASE Parse 异常逃逸、缺操作数、类型修饰截断已修复。
该前批全局集合表达式计数曾静默被忽略，随后部分已解析形状明确报 0A000，当时仍列未实现；本批普通 UNION ALL 接入另见最新记录。原始 52 探针的 C4 实际只完成 49 项即 SIGABRT，剩余三项未观察；最终相同严格预期完整 52/0。

实际 Main 全值 4,107 行/12,321 单元的七项资源核验全过；首轮采样曾把 mkstemp→unlink 的瞬时命名 FD 误判，后按同 FD/路径转为 deleted 或确实关闭且路径消失验证，原观察保留、生产文件 helper 未改。
35 类操作有限检查、匿名文件打开时 SIGKILL/重开 1,000 行/4,000 单元及再次集合排序通过，不是任意写入崩溃或真实 TLS 验收。
同代构建/哈希、所有失败与性能数据见[集合排序与 VALUES 报告](performance-report-2026-10-30.md#集合排序预算与-values-查询子句2026-11-02)，证据目录 `build/pg-feature-set-sort-wi9y4qzg/`；原始总账与专属暂缓未变。

## 2026-11-02 普通类型化排序预算与比较缓存

最终 39 个原生入口全部通过；新增 codec 51、通用有界段 17、真实算子 17，共 85 控制。
覆盖完整 owning ExprValue/四类 RowContext 容器、递归 record/OID/NULL、实际低预算文件、TopN/超大行、有限 FD、
多轮归并、短 I/O/EINTR/ENOSPC/EMFILE/损坏输入、主异常清理和 volatile 需求。
真实 SourceContexts 的 JOIN 绑定测试不等于实现新的 JOIN 算法；公共 buildPreparedQueryPlan 的 CTE/derived VALUES 与实际堆另独立执行。

所选完整 26 协议入口为 25 通过/1 失败；新增固定 108 在严格 PG 18.6/最终程序全过，旧版仅 NaN WITH TIES 两处失败。
原 80 控制顺序和预期不变，追加 28 个无 ARRAY/无 LIMIT 的普通入口控制；Simple 与正常 text/binary 不限额 Execute 均保留。
旧 Bind/portal 完整 463/40 观察逐字节相同，不排除失败、不恢复 R197/R303/R304/R305；旧会话 470 等完整回归通过。

首批 38 原生有一入口失败：新普通回调使用 std::sort，使原 80 中 16 项 stable_sort 分配观察未发生；
恢复原稳定批次回调后最终 80 全过，共享模板和全部原断言字节不变。没有恢复 R185 的旧专项调查。
原 TopN 八项的 SQL/排序/NULL/行数/需求断言保留；旧四项要求保留全输入的资源契约被新的严格预算要求替代，
64 KiB 下全部八项要求输入结束活跃分配小于 1 MiB，旧版四项失败、新版全过。原源码/初次四失败与相同默认预算观测均保留。
codec 初次编译缺少实际中断声明、初版 NAME 声明/SHOW 标签/重复二进制 statement 名、
算子夹具的树观察/序列目录/Offset open 时点错误及全部候选失败也保留，不把夹具失败写成已修生产能力。

实际 Main 同一 9 项旧版 6 项资源失败、最终全过：低预算 6 项有匿名文件，高预算 3 项无文件，完整值正确、最多 2 FD 且全部释放。
35 类操作有限检查和类型化匿名文件内 SIGKILL 后重开 1,000 行/4,000 单元格通过；不是全库恢复或真实 TLS 验收。
构建、全部原始记录、1,584 个完整延迟样本及限制见[类型化排序报告](performance-report-2026-10-30.md#普通类型化排序的预算与比较缓存2026-11-02)。
证据目录为 `build/pg-feature-typed-sort-zm9x0zst/`；原始 273 项与专属暂缓保持原文。

## 2026-11-02 会话 work_mem 与实际排序预算

36 个原生入口全部通过；新增解析/状态 136、真实 Sort/Config 27 控制，共 163，覆盖会话/启动默认、事务/保存点及预算打开时快照。
现有搜索路径/时区语法、扫描/MVCC、TOAST/索引、恢复和排序错误控制重跑通过。
实际 Main 所选完整 25 协议为 24 通过/1 失败；新增完整 470 在严格 PG 18.6/最终程序全过，旧版完整 404 处不符。
保留原 391/448 所有断言与步骤，追加缓存、语法和警告/错误顺序，验证 Simple 与正常格式不限额 Execute、OID/text/binary、原始帧和 Ready。

首候选完整 448 有 8 处不符；b190 虽通过 448，新增 22 个错误边界仍有 4 处不符，随后移动警告至值校验前再完成 470。
第一版 PG 检查器的 143 个 bytes/str 字段名差异也保留原源码/日志；没有将错误前缀/帧断言改为多个可接受值。
最终唯一失败仍为暂缓 Bind/portal 的完整 463/40，与前版全部观察逐字节相同；b190 的完整 25 另有部分 COPY 帧关闭失败，
最终入口通过不改写原失败，也不恢复或关闭 R197/R303/R304/R305。

实际 Main 的 31 个会话预算/FD/重载控制全过：低预算落盘、高预算不建文件、两个连接互不改值，最终核对 4,000 单元格。
35 类操作有限 smoke、匿名文件内 SIGKILL/重开及再次排序通过；不作全库、完整例程 GUC 或真实 TLS 验收。
CMake 21 次成功编译/16 个不同 CPP、链接 62 对象；最终 Main-only 变化后核对相同 61 core，复用不链接 Main 的 36 原生，实际 Main 另完整重跑。
504 个独立 SQL 样本与 600 次 CPU 诊断均保留，未证明全面提速或无回退，见[会话工作内存报告](performance-report-2026-10-30.md#会话工作内存与实际排序预算2026-11-02)。
证据目录 `build/pg-feature-workmem-_a2m0fg2/` 的 `native-final.json`、`protocol-final.json`、470 旧/PG/各候选完整日志与 `final-provenance.json` 记录来源。

## 2026-11-02 最终归并按需输出与排序绑定恢复

最终 31 个原生入口全部通过；新增 8 个归并控制和 32 个真实堆 NULL 绑定生命周期控制，旧版分别 6/26 不符，新版全过。
现有 I/O 16 个控制也通过；输出 EIO 从旧 0 行改为精确 1 行后报 58030，原失败日志保留，未放宽为任意前缀。
错误后立即清理，重复 next 无额外 I/O；失败 open 的诊断、主异常身份、嵌套/析构和后来绑定均检查。

实际 Main 所选完整 22 协议为 21 通过/1 失败；同一排序完整 47 条在严格 PG 18.6/新版全过。
既有暂缓 Bind/portal 的完整 463 中 40 处差异与前版逐字节相同；完整 timeout 入口通过，不据此恢复或关闭 R197。
实际匿名文件内 SIGKILL、重开 1,000 行/4,000 单元格、再次排序及 35 类操作有限 smoke 通过。

本批只改 ExecutionPlan.cpp，CMake 两次成功重编该单元并链接 62 对象；原生链接 61 core，未称 fresh 全部编译。
新测试的首版旧程序未捕获预期 EIO 而中止、原输出 EIO 断言失败均保存，之后完整重跑所有控制，没有删除失败观察。
宽行原生 15 样本约快 1.78–1.86 倍；504 个 SQL 样本未证明稳定提速，960 混合诊断另存，详见[最终归并报告](performance-report-2026-10-30.md#最终归并按需输出与排序绑定恢复2026-11-02)。
原始 273 项、原子条款、专属问题/暂缓记录及共享排序中断 helper 未改；这不是整库或完整 PG 验收。
证据目录 `build/pg-feature-query-oxy0qhou/` 的 `native-final.json`、`protocol-final.json`、新旧完整日志与 `final-provenance.json` 记录实际来源。

## 2026-11-02 普通物理排序预算与外部归并

该历史批次的 61 个 core 对象通过 29 个原生入口：前批 24、三个新排序入口及 redo_crash_recovery/recovery_integrity。
新入口完整 39 控制（17 内存、16 I/O/清理、6 真堆/TOAST），冻结旧版分别 13/15/4 不符，新版均全过；不是 32 个独立缺陷。
逐输入调用边界及 EOF 的分配观察分开记录，完整消费所有结果；另验单个 256 KiB 超大行。
真实 syscall 包装只用于测试程序，验证短传输、EINTR、ENOSPC、EMFILE、EOF/EIO、截断失败、取消及原异常/清理次数；没有生产故障注入 hook。
新测试的链接注解由整批、单项及 sanitizer 构建脚本共同读取，现有构建路由检查通过；本批没有运行整库 sanitizer。

随后只改 Main 接入和函数拆分，全部 61 个 core 哈希仍相同，因此复用这些明确不链接 Main 的原生结果；
最终 O2 实际 Main 另完整跑 22 个协议入口，结果 **21 通过、1 失败**。
新增完整 **47 条**在严格 PG 18.6/最终程序全过，冻结旧版 1 条 ORDER BY 1 整数按文本排序失败。
覆盖实例配置 64 KiB、NULL 顺序/空串/文本 NULL、浮点特殊值、日期/时间、BIGINT、参数/二进制结果、投影重排、别名/序号、AND 及未扩大的其他分流。
NUMERIC >2^53 控制继续走原比较所有者，没有因接入物理 Sort 变成 double 比较。

未通过的是既有暂缓的 Bind/portal 完整 463：旧/最终 40 处差异、全部观察逐字节相同。
前一 `1865fddb` 候选的完整 22 为 20/2，另一项为 statement_timeout 的部分 COPY 帧关闭；
隔离旧/新/旧也均未通过，失败点不同，不能认定同一根因。最终该完整入口通过，不擦除前述失败、不声明 R197 修复。
专属 R197 记录中的既有 COPY 响应/中断暂缓补入[中央索引](deferred-scope.md)，没有继续相关实现；NetworkServer 和共享 CPU 排序中断 helper 保持原字节。

初始 37 条的 `pg_settings.setting::integer` 在 PG 全过、本项目一处 0A000；该目录表达式缺口保持未修复。
配置核验改成原生 text 列后保留全部其他 36 控制，并扩展为 47；最初失败和 PG 对照没有删除。
首个仅算子候选虽然通过原生/协议控制，却在真实 SQL 文件观察中失败；补 SQL 接入后才捕获实际匿名文件。
O0 Main 的快速功能构建只作预验，最终声明使用另行 O2 构建的真实 Main。

最终 Main 的实际匿名文件内 SIGKILL、无命名残留、重开 1,000 行/4,000 单元格及再次全值排序通过。
35 类操作也完成预热和一轮现有检查，只作有限 smoke；后续独立最终延迟 504 样本、前期 756 样本及 900 查询 CPU 诊断分开保存。
收益与较慢控制完整列于[报告](performance-report-2026-10-30.md#普通物理排序的内存预算与外部归并2026-11-02)；不据此通过全仓库、真实 TLS、全故障恢复或完整 PG 行为验收。
证据目录 `build/pg-feature-sort-1be1avdu/` 的 `native-final.json`、`protocol-final.json`、旧/PG/新日志、
`timeout-isolated.json`、`main-sort-crash-outlined.json`、`operations-outlined/operations.json` 和 `final-provenance.json` 保存范围与来源。

## 2026-11-02 普通 LIMIT 需求与运行错误前缀

最终 24 个原生入口通过。新增结果前缀原生完整 16 项验证 SQL 错误、hasError、open/close 错误、非 SQL 异常身份、
close 恰好一次、NULL/空串/文本 NULL/UTF-8，以及保留前缀与默认 checked 清空之间的独立契约。
原 23 项扫描、MVCC/快照、TOAST、索引、聚合、排序取消等入口全部重跑通过。

完整所选 21 个协议入口 **20 通过、1 失败**。新增 `physical_limit_demand_protocol_e2e_test` 完整 62 条在严格 PostgreSQL 18.6/候选全过，冻结旧版有 9 条失败。
覆盖 Simple/text Bind/binary Bind 与结果、LIMIT 0/超量/OFFSET、未请求行中的错误、序列及存储写函数调用次数、DISTINCT/聚合、
FETCH、负数/负零计数、单列星号/NULL，以及 OID/typmod/属性来源。另检查错误前 5 行的原始帧与二进制值，随后 22012 且没有 CommandComplete。
首份 54 条夹具误用“错误前无行”的通用 helper，实际 PG 在第 6 行失败前发送 5 行；保留该失败，改成明确校验 PG 原始行前缀及帧顺序后扩展为 62 条，没有放宽断言。
仅执行分流的中间候选仍有错误前缀和负数 LIMIT 2 条失败，后续才补生产修复。

未通过项为既有 `bind_portal_lifecycle_protocol_e2e_test`：完整 463 中旧/新各 40 处不符，全部观察逐字节一致；不算 40 个独立 bug。
专属 [R304](issue-review-r304-bind-portal-lifecycle.md)/[R305](issue-review-r305-result-format-demand.md)已记录用户暂缓，此次只保留未过滤回归，不修改 Bind/格式策略或恢复调查。
额外有限行数 portal 观察共 6 条，PG 全过，旧/新各有相同 2 条差异；没有接受该观察为修复，不以本轮不限额 Execute 的前缀通过关闭共享 portal 能力。
R303/R304/R305 既有范围补入[中央索引](deferred-scope.md)，专属暂缓记录与原始 273 项状态保留。

35 类操作完成预热及一轮现有检查；该 smoke 与相邻编译/回归重叠，不用于性能结论。
另有隔离的六类 216 个旧/新/旧延迟样本，见[性能报告](performance-report-2026-10-30.md#普通-limit-按需执行与错误前缀2026-11-02)。
本批不是全仓库、恢复/故障注入、真实 TLS 或完整行为兼容验收；默认持久化和 15 秒协议期限未放宽。
证据目录 `build/pg-feature-limit-20261102-tafn38be/` 的 `native-final.json`、`protocol-final.json` 包含实际退出码，
`portal-existing-failure-comparison.json`、`protocol-before-reference-62.json` 和 `final-provenance.json` 保留未通过范围、参考版本及当前源/对象/程序核对。

## 2026-11-02 按页物理扫描与固定快照

最终候选通过 23 个原生入口、17 个协议入口，既有 35 类操作也完成预热及一轮现有 oracle 检查。
新增物理扫描原生 6 项：冻结旧版有 1 项 open 内存失败，其余值/NULL/重开控制通过；新版完整 6 项通过。
新增跨后端快照 8 项：autocommit、READ COMMITTED、REPEATABLE READ、SERIALIZABLE 四种场景中，
游标打开后另一后端提交 UPDATE/DELETE/INSERT，原扫描仍得到 192 行旧值及旧 TOAST，随后新扫描得到新值。
统计专项验证提前停止、EOF、重复 close、析构和未打开子算子的 NULL 绑定；增加真实 allocator 关闭故障后验证 58030、页/表锁释放与重开。

新增写函数排序协议完整 13 条，在 PostgreSQL 18.6、冻结旧版和最终候选均全过；
覆盖同一语句中的后续页修改、显式事务及回滚。前批完整 48 条 Top-N、80 项原生排序取消、190 条聚合排序和 69 条参数索引均再次通过，
相邻原生还包括并行执行、MVCC、索引快照/损坏、丢失堆文件、TOAST 失败、VACUUM/快照导入导出；协议包括事务特征、完整值和 NULL 谓词。

公共头变化触发 CMake 13 个生产单元重编，另明确重编两次 ExecutionPlan 以核验资源释放及统计锁边界，当前 62 个对象链接；不是重新 fresh 编译全部 62。
PG 参考首次在监听后、就绪前收到 57P03，未执行任何专项 SQL；保留日志，并用 pg_isready 在原 15 秒启动期限内判断就绪后才完成参考检查。
初次四类 144 样本、扩展六类 216 样本及最后统计锁边界修正后的六类测量分开保存，不合并择优；实际数值、内存口径及剩余普通 LIMIT 分流见[性能报告](performance-report-2026-10-30.md#按页物理扫描2026-11-02)。
证据目录 `build/pg-feature-scan-20261102-i7t0b3bo/` 中，最终原生汇总为 `native-release.json`（前阶段及统计夹具增补原始运行另存），
协议为 `protocol-release.json`，生产输入/实际对象/当前测试源与二进制核对为 `final-provenance.json`。
这不是完整 PG 兼容、全仓库、真实 TLS、长期并发或崩溃恢复验收；分区/RLS 和其他用户暂缓范围未改，273 项原状态不变。

## 2026-11-02 有界排序与类型化执行

- 最终候选通过 11 个原生入口和 11 个真实协议入口；新增 `prepared_topn_memory_test` 完整 8 项，
  `prepared_topn_protocol_e2e_test` 完整 48 条在本项目及 PostgreSQL 18.6 均全过。冻结旧程序跑同一份 48 条有 11 条失败，原始日志保留。
- 协议覆盖 Simple、text Bind、binary 整数 Bind 和 binary 结果、多键/NULL/OFFSET、0/超量 LIMIT、DISTINCT/WITH TIES、
  排序键与投影的序列调用次数、丢弃候选中的除零错误、OID、超出 2^53 的 numeric/bigint、整数最小/最大值、NaN/Infinity 与虚拟生成列的有效值。
- 10 个相邻协议入口包括完整 190 条普通聚合排序检查、69 条参数索引检查、取消、子查询上下文、存储函数与 FETCH 边界；
  10 个其他原生入口包括完整 80 项排序中断及扩展后的 NULL 位图逐列/按行等价与嵌套绑定恢复检查。
- 既有操作驱动的 35 类全部完成一轮检查（另有预热）；该轮与原生编译有重叠，只作为有限正确性回归，不用于提速结论。
  独立旧/新/旧延迟测量含 144 个已校验样本，详见[性能记录](performance-report-2026-10-30.md#有界排序-top-n2026-11-02)。
- 额外三方探针确认既有缺口：虚拟生成列 `b AS (1/a)` 在 `a=0` 且读取 b 时，旧版和新版均返回 NULL，PG 18.6 报 22012；
  只读 a 的控制三方均成功。本批没有关闭生成列表达式错误传播，也不把上述有效值检查视为全部虚拟列兼容。

本批仅重编译有变化的生产单元并链接当前 CMake 对象，不是重新 fresh 编译全部 62 单元。
NULL 位图测试初次编译缺少 DbError 头文件、早期候选的延迟回退、浮点排序失败及 10,000 行夹具加载超时均留有原始记录。
未修改原始 273 项状态、用户暂缓项、R244 Bind 规划、默认持久化或协议期限；未执行完整仓库/TLS/并发/恢复矩阵。
证据目录为 `build/pg-feature-topn-20261102-bc582b_s/`，最终入口退出码见 `native-keycache.json`、`protocol-keycache.json`，
测试源、生产源、对象及程序哈希见 `final-provenance.json`。

## 2026-11-02 PostgreSQL 差距评估与操作对照

复核 273 项总账及功能族分布，没有改变 complete/partial/unverified/deferred 状态。
使用 `scripts/benchmark_postgresql.py` 在两个私有的本机进程执行既有 35 类操作：双方各预热 35 类，
再各完成 210 个计时样本，既有驱动的协议/结果检查均通过，范围及只读事务/序列的有限 oracle 见[性能报告](performance-report-2026-10-30.md#postgresql-186-同机对照2026-11-02)。
参考进程严格核验为 PostgreSQL 18.6；已有默认 `pgref` 实际为 17.2，没有用于本次对照或被修改。
这不是新一轮全仓库回归、完整 PostgreSQL 差分、真实 TLS、长期/并发或生产验收；[差距评估](postgresql-comparison.md)说明尚缺的能力和证据。

## 2026-10-30 数据库性能批次

2026-10-30 数据库性能批次：最终候选通过 **123 项原生入口和 45 项真实协议入口**，WAL 追加故障矩阵完整 246 项检查全 0。
35 类操作的新版本完整完成 315 个样本；前后对照采用双方均完成的前两轮各 210 个样本，旧版第三轮 CHECKPOINT 错误保留。
测量方法、完整耗时表、故障回归、早期失败、构建/源码哈希和复现命令见[性能测试报告](performance-report-2026-10-30.md)。
默认持久化设置与协议 15 秒期限未放宽；这是本轮指定范围，不是全仓库兼容性或历史 R164 根因关闭。下文历史批次继续按原范围解释。

## 2026-11-01 文档与系统一致性核查

版本一致性、兼容性契约、数据目录端到端、`version()` 协议及原生配置测试共 5 个指定入口通过。
另在私有服务执行 23 条协议观察，包括 README 原文 9 条 SQL、保存点回滚结果、配置默认值/重载/重启和模式门控，全部通过。
实际缓冲池参数来源和后台 checkpoint 间隔只经源码追踪确认；首次 CMake 配置因未安装工具退出 127，该失败记录保留。
同日已补装 CMake 4.4.4，按 README 完成配置和完整编译，新编译 62 个单元；增量构建没有重复编译。
对新产物 `build/cmake/dbms_main` 运行数据目录、`version()` 协议两个入口及上述 23 条文档观察，全部通过。
根目录原程序未替换；本次构建采用 TLS stub，没有重跑真实 TLS、完整 CTest、性能或兼容性矩阵。
修订项目、原始证据与其他覆盖限制见[文档核查记录](documentation-inventory.md#文档与系统一致性核查2026-11-01)。

近期修复与历次验证集中见[审查索引](review-index.md#近期修复与验证记录)，当前实现和性能范围见[状态说明](production-status.md#当前状态与验证范围)。

## 历史记录（以下状态与数量均指记录当时）

## 当前版本：R176有限排序修复，完整矩阵仍有失败

[R176](issue-review-r176-ordinary-aggregate-order-keys.md)的有限源码修复已本地提交`a35658c0`：
保留全局SUM/COUNT真实ORDER键，纯绑定及错误阶段、FILTER/NULL需求、缓存typed排序、
共享参数/重复键和方向身份有native61及完整协议190回归，两侧专项均0。
完整PG190对照0；旧版本190次完整观察59通过131失败，不把这些重复模式称为131个独立缺陷。
工作区完整72入口实际全0；干净版两次完整72均外层1，各71项通过，均在R164入口socket超时，
无完整956成功footer，清理出现延迟响应错位。超时根因未确定，也未证明与R176无关；
保留全部失败，不提高超时、不改持久化、不移除断言，不宣称双侧完整矩阵通过。
每侧35native/34协议/3真实Main顺序及原70断言保留；每侧97对象/39程序及源文件独立核对。
三个Main采用accepted测试体的路径投影及显式私有数据目录，不夹带工作区session测试改动。
Root fresh58、normal wrapper复用当前58链接、repeat零编译/零链接；clean fresh2+56当前header
donor源自独立fresh58 V3，未伪造clean常规build/repeat。提交的2198源文件与tested clean逐blob一致。
跟踪597份MD、917native+564注册Python+3Main=1484入口；工作区600份MD、923+570+3=1496，
原12个未纳入版本的测试入口和三份文档分离保留。这不是1484全量、TLS或整库sanitizer通过声明。
R176的GROUP BY排序、更多类型/算子/参数/query owner边界仍开放；R164超时仍普通未解决，非用户暂缓。
原273仍22 complete /166 partial /70 unverified /15 deferred，总目标未完成。
R173、R156及其他用户暂缓分支不恢复；不push，Actions禁用。

## 历史记录（以下状态与数量均指记录当时）

## 当前接受状态：R170/R171/R172/R174/R175

五项有限修复已分别本地提交，版本对应见[审查索引](review-index.md)。
最终组合在工作区及accepted-only clean各完整70入口（34native、33wire、3真实Main）实际全0；
完整791/314/331/906及原956均0，已接受源2195文件与实际测试clean逐字节对应。
首次Root70实际1的清理超时仍保留、原因未确定，复测0不等于超时根因已修；
源码、58生产对象、server及每端96对象/38程序均独立核对，详见[R170最终证据](issue-review-r170-quoted-unqualified-scalar-projection.md)。
已接受596MD、916native+563已注册Python+3Main=1482；工作区599MD、922+569+3=1494，
原12个未接受入口和三份文档保持分离。这不是1482全量、TLS/整库sanitizer或各中间提交独立全矩阵。
原273仍22 complete /166 partial /70 unverified /15 deferred，总目标未完成。
R173函数签名类型依赖及R176真实聚合排序仍独立开放；R156等用户暂缓模块不恢复，不push，Actions禁用。

## 历史记录（以下状态与数量均指记录当时）

当前接受版本包含[R169组合记录](issue-review-r169-simple-query-raw-syntax.md)所列九项有限修复，已逐项本地提交。
Root和accepted-only clean各完整58入口（26native、29协议、三个真实Main）实际全0；
这是组合定向回归，不是每个中间提交独立全矩阵，也不是全部1478入口运行或TLS/整库sanitizer验收。
已接受589MD、916native+559已注册Python+三个Main=1478；工作区592MD、922+565+3=1490，
原12个未接受入口及三份文档仍分开保留。原273仍22 complete /166 partial /70 unverified /15 deferred，
总目标未完成；R156、R151、R106及其他用户暂缓模块不恢复，不push，Actions保持禁用。
用法与限制见[手册](MANUAL.md)，实际错误、修改方法及日志见各专属报告。
以下旧轮次按原输入保留，历史计数、失败和待办不被本次改写。

[R157 普通SET点号目标](issue-review-r157-dotted-set-target.md)完成独立有限修复：
完整目标先于内建分派，LOCAL/SESSION/transaction相似前缀不改变身份；
引用分量保留原拼写，ColId分类、缺失分量、分隔符与FROM CURRENT边界均验证。
PG1538全0；修复前两端各1538/226失败，native Root3075/474、clean3075/3072失败。
修复后两端3075/1538全0，Root52及accepted-only clean37完整入口、严格输入/归属复核全0。
原search_path_values_grammar夹具未改，两端完整跑到254；R152的5197/1675也重新全过，
R152历史clean30终态1及旧R155断言134仍保留，不把旧输入失败改写成通过。
仅一个Parser CPP：Root正常1O2/repeat0，clean fresh1+57已核对R152生产对象；
六Main源/全头flags/对象核对后复用R152三个真实Main对象，新链接当前57库并隔离运行，
不冒fresh TU。未实现全部通用SET值语法或自定义GUC存储，R140/R151等暂缓项不重开。
[R156 REGTYPE物理常量](issue-review-r156-regtype-physical-constants.md)与R150仍开放；
已接受577MD、909native+548Python+3Main=1460；工作区580MD、915+554+3=1472，
仍有12个未接受入口，无新1460全量/TLS/整库sanitizer。
原273仍22/166/70/15，总清单未完成，不push、Actions禁用。

[R152 REGTYPE 最小引用](issue-review-r152-regtype-identifier-output.md)完成独立显示修复：
精确查找串与显示串分离，quote_ident/format %I/路径列表共用既有 ASCII、关键字引用规则。
PG1675全0；修复前两端各1675/66失败、native5197/1718失败；修复后两端5197/1675全0。
Root45完整相邻入口全0；accepted-only clean30完成29通过、1个旧搜索路径AST断言134，
整批终态1，不称全绿。冻结R155旧版同一未改夹具也在第86行退出134，
另列[R157 普通SET目标](issue-review-r157-dotted-set-target.md)开放，原失败/严格检查器终态1保留。
[R156 REGTYPE物理常量](issue-review-r156-regtype-physical-constants.md)另列开放：
PG/旧已接受版/新clean各34条观察，后两者各14条语义差异，不是34项通过；
OID往返身份丢失、内置呈现与JSON语法身份仍需独立修复，不能用本项显示通过关闭。
共享头改变后两端各正常fresh58生产O2/repeat0、各三个真实Main TU fresh，
新链接各自57当前O2库并隔离执行；输入/提交边界审计通过不代表相邻整批通过。
只接受本项五处源/注册、两个完整夹具及文档；R151继续用户暂缓，不重开R140等专项。
已接受577MD、908native+547Python+3Main=1458；工作区580MD、914+553+3=1470，
仍有12个未接受入口。无新1458全量/TLS/整库sanitizer；原273仍22/166/70/15，
总清单未完成，不push、Actions禁用。

以下各轮摘要保留该轮输入和历史计数；后续状态以本段及各问题文档为准。

[R155 枚举辅助名称归属](issue-review-r155-enum-namespace-owner.md)完成有限修复：
DROP SCHEMA 按真实目录 OID 关联识别解码后的标量枚举名，不再重解析/折叠大小写；
public 带点名称保留实际 owner，竞争身份先报 XX001，旧 native fallback 不变。
PG87全过，旧两端各87/25失败、旧native142/42失败；最终两端142/87全0，
Root24完整入口、独立clean10及归属复核通过。仅私有DDL正常1CPP O2/repeat0；
六Main源/全头flags/对象核对后复用R148三Main对象，新链接57当前O2库并隔离执行，
不冒fresh TU。clean fresh1DDL+56已核对R148对象+当前R149 evaluator，无server stubs。
[R152 REGTYPE显示](issue-review-r152-regtype-identifier-output.md)另记开放：
PG1675全过，旧两端各69失败、native5197/1718失败，夹具/注册未接受，尚无生产修复。
R151按用户要求暂缓、不恢复；R150仍开放。已接受575MD、907+546+3=1456，
工作区578MD、914+553+3=1470仍含14未接受入口；无新1456全量/TLS/整库sanitizer。
原273仍22/166/70/15，总清单未完成，不push、Actions禁用。

[R150阈值重载](issue-review-r150-threshold-width-bucket.md)仍开放；实际PG/Root各完成61条
广泛类型观察，保留非数字类型与XML参考构建能力差异，不冒充61项通过。
[R151枚举数组目录](issue-review-r151-enum-array-catalog.md)按用户“skip it”暂缓：
PG103全过，旧Root/accepted clean各103/86失败，旧native341/280失败；
候选两端native341全0，但完整103协议各仍60失败、四入口终态1。
候选源码、两个夹具及注册原样保留，未验收、未提交；普通继续不恢复本项。
独立REGTYPE名称显示问题可另行复现、修复与提交，不以此重开枚举数组或宣称R150完成。
已接受573MD、906native+545Python+3Main=1454不变；工作区576MD、912+551+3=1466，
另含R151两个未接受入口。原273仍22/166/70/15；无新1454全量/TLS/整库sanitizer。
用户暂缓不重开、不push、Actions禁用。

[R149 WIDTH_BUCKET边界与精度](issue-review-r149-width-bucket-boundaries.md)完成四参数路径修复：
按方向处理包含端点，NUMERIC保持十进制精确商，float8保留自身舍入，
count+1超过int4报22003；已声明重载只归实际builtin，原生同名替换不受影响。
PG804+5全过，冻结旧Root/accepted各804/200失败、旧native515/78失败保留；
最终515/804、Root36、accepted-only clean11及定向执行器UBSan515全0。
首轮Root36两处、clean10一处旧SQLSTATE预期错误均保留终态1；按PG核验
校正预期并补quoted超范围22003断言，没有删断言。生产仅1Evaluator O2/repeat0，
fixture修正后正常0/repeat0；核对六Main源输入、全部头/flags及对象字节后
复用R148真实Main对象，新链接当前57 O2库并隔离执行，不冒本轮fresh TU。
已接受571MD、906native+545Python+3Main=1454；工作区574MD、911+550+3=1464。
原273仍22/166/70/15；二参数阈值重载、完整numeric域与其余缺口仍开放。
无新1454全注册/TLS/整库sanitizer证明；用户暂缓不重开、不push、Actions禁用。

[R147日历间隔顺序](issue-review-r147-interval-calendar-order.md)完成有限修复：
日期先按月份调整并截断到月底，再加天，最后处理微秒与跨日；修正月底、闰年、
混合正负间隔及DML中的错值。PG943全过，旧各507/原生890失败与首版11失败保留。
类型依赖先独立提交f59a1098；当前同一Source625帧的Root48完整入口全部通过，
本项核对后复用该证据，不冒再次运行或重编Main。新accepted-base clean4native+
6wire=10全过，原生2043/协议943及类型552全0。clean为fresh1Evaluator O2+
57个核对全头/flags/CPP/原58receipt及前后字节的R148生产donor，server无stubs。
已接受570MD、905native+544Python+3Main=1452；工作区573MD、910+549+3=1462。
原十个未接受入口及R146等用户暂缓不混入；无新1452全注册/TLS/整库sanitizer证明。
完整civil/DST/其它时间算术仍开放，原273的22/166/70/15不变，不push、Actions禁用。

[R148日期算术结果类型](issue-review-r148-temporal-arithmetic-result-types.md)完成有限修复：
共享的已声明运算签名统一DATE/时间戳与INTERVAL、差值及int4天数的结果类型，
不再继承操作数OID或在NULL/零行时猜值。PG552全过，两旧版各259断言失败、
旧147原生70失败均保留；最终147/552、Root48完整入口与独立clean14通过。
共享头改变后双方均58 fresh生产O2/repeat0，三真实Main本轮fresh O0/O2/O0，
新链接57当前O2库、不用stubs并隔离实际执行。相同对象SHA不改写fresh编译事实。
仅本项类型修复独立提交；R147日历字段顺序候选和两夹具/注册不混入本项。
已接受569MD、904native+543Python+3Main=1450；工作区572MD、910+549+3=1462，
仍含原十个及R147两个未接受入口。无新1450全注册/TLS/整库sanitizer证明。
R146/R137/R140及其它用户暂缓保持；原273的22/166/70/15不变，不push、Actions禁用。

[R145搜索路径参数语义](issue-review-r145-search-path-value-identity.md)完成有限修复：
SQL SET按raw AST的独立参数逐项list-quote，不再把Sconst当已格式化列表；
保留逗号、大小写、单双引号、空名字与关键字语义。FROM CURRENT走真实
persistent/LOCAL赋值栈，避免把LOCAL可见值错误当无操作。PG3573全过，
原版各1098断言失败；初稿各4个事务断言失败，均保留原日志。最终Root/clean3573、
Root18完整入口/clean10及独立复核通过，native254和真实session320全过。
共享头改动后首轮Root/clean均fresh58 O2；最后Main修正再正常1Main O2/repeat0，
核对57库未变，fresh三Main TU O0/O2/O0后新链接当前57 O2库、隔离实际运行。
新clean为fresh Main O2+57逐一核对头/flags/CPP/原receipt/字节的生产donor，无stubs。
已接受567MD、903native+542Python+3Main=1448；工作区570MD、908+547+3=1458。
R144/R140/R137与其它用户暂缓保持未修复，原273的22/166/70/15不变；
无新1448全注册/TLS/整库sanitizer证明，不push，Actions禁用。

[R142 quoted SET参数名](issue-review-r142-quoted-set-parameter.md)完成有限修复：
完整单token双引号名称按GUC大小写无关规则解码，LOCAL搜索路径按规范名判断，
原始@变量分支仍用原语法，quoted @目标不改已有变量。PG530全过，原旧版各544/322
失败，初稿136项SHOW metadata失败/14与clean5终态1保留；SHOW先由R143独立提交，
仅含该修复的干净版本仍544/216失败。最终Root/clean544均0，15完整入口/clean6/
独立核对全0。最新Main/头未变，核对后复用R143两fresh前端对象、新编session298
O0，再链接当前57匹配O2库并隔离运行；不冒全3本轮fresh或生产重编。
R140/R137及其它用户暂缓项保留；R144/R145普通开放不关闭，原273的22/166/70/15
不变，无新1446全量/TLS/整库sanitizer证明。不push，Actions禁用。

[R143 普通SHOW协议结果](issue-review-r143-show-guc-protocol-results.md)完成有限修复：
application_name/client_encoding/search_path/lc_monetary固定text描述不再误报NoData；
结构化单行与显式非NULL位保留空串、字面量NULL及路径中的换行/UTF8，不从stdout猜行。
PG150全过，冻结Root/已接受干净旧版各150/111失败；最终Root14完整入口、clean5
与独立核对均0。正常2CPP O2/repeat0，三fresh真实Main TU/当前57库新链接/隔离运行，
session原290控制；未运行R142待定8或R140待定21扩展。检查器跨行标签/转义错误的
首次1保留，独立v2按完整150控制核对0，未改产品/夹具。R142仍为独立待定候选，
R144应用名规范化、R145路径Sconst列表语义另记普通开放及方法，不冒已修复。
原273的22/166/70/15不变，无新1445全量/TLS/整库sanitizer证明；不push，Actions禁用。

[R141 SET参数身份与分隔符](issue-review-r141-set-parameter-identity.md)完成有限修复：
普通值不再误入compatibility_mode分支，相似/带点名称不改模式，TO值内等号不作分隔符；
独立Extended模式设置不再把未执行的规划事务误当BEGIN，真实事务限制保留。
PG普通参数160全过；冻结旧版各389/235失败。初稿81失败、第二稿21失败及各自
Root13/clean4/独立终态1保留，未删断言；最终389、13完整入口、clean4与独立核对均0。
最终仅Network重编O2/repeat0；三个v2fresh真实Main TU经源/头/依赖核对后新链接当前
57库并隔离执行，session为已接受夹具290，不冒最终fresh或重跑R140待定311。
R140/R137继续用户暂缓、候选不混入；原273的22/166/70/15不变，无新1444全量/
TLS/整库sanitizer证明。不push，Actions禁用。

[R140 TimeZone设置语法](issue-review-r140-timezone-set-grammar.md)按用户“skip it”暂缓。
候选实现、4090原生/6403协议夹具及注册保留，但未提交、未验收，不计已修复。
跳过前专项、Root43和clean7完整入口实际全0；独立终态验收未运行，不补作通过。
本次只提交暂缓记录，已接受902native+538Python+3Main=1443不变；工作区另有
未接受候选。R137及其它暂缓项不重开，原273的22/166/70/15不变，不push、Actions禁用。

[R139 TimeZone默认恢复](issue-review-r139-timezone-default.md)完成有限修复：
DEFAULT和专用TIME ZONE LOCAL复用默认值解析，RESET恢复名称及真实offset，
通用TO LOCAL与带引号关键字仍22023。PG280全过，旧Root/干净旧版各280/48失败；
旧Main五观察确认非UTC默认被清成0，原生扩大到290次命令且原17控制保留。
正常1Main/repeat0、三fresh真实Main TU/当前57库新链接/隔离执行、最终12native+
16wire+3Main=31完整入口与clean5均0，独立核对0，原224协议全过。
R140另有22条实际语法观察：合法紧贴等号仍被拒，缺分隔符/专用等号误接受；
新增DEFAULT还放行两条非法语法，明确保留未修复，不冒完整语法/GUC完成。
R137仍用户暂缓；无新1443全量/TLS/整库sanitizer证明，原273的22/166/70/15不变。
不push，Actions禁用。

[R138 SESSION TimeZone](issue-review-r138-session-timezone.md)已有限修复：
显式SESSION仅在时区路由复用既有值解析，其余SET分派不改。PG224全过，
冻结旧Root/干净旧版各224完整/56失败；原112断言保留并增加112个SESSION控制。
正常1Main/repeat0、三个fresh真实Main TU及当前57库新链接/隔离执行通过；
最终12native+15wire+3Main=30完整入口全0、clean4全0，独立核对0。
首次三Main因wrapper日志路径错误未运行、整批1的记录保留，不冒产品失败或通过。
R137仍用户暂缓/未接受；DEFAULT另有13条观察与方法记录、仍普通未修复。
无新1442全量/TLS/整库sanitizer证明，原273的22/166/70/15不变，不push，Actions禁用。

[R137 外键完整父值核对](issue-review-r137-foreign-key-full-value.md)按用户“skip it”暂缓：
候选实现、测试及原始失败保留，未提交、未验收，不计已修复。初稿57/208及clean7
通过不足以验收；扩展后PG364全过，旧版364/96失败，初稿364/48失败、native89/4失败，
暴露合法网络类型被误拒绝。修订版仅正常1TM/repeat0构建通过，未运行其最终门禁。
旧Root45保留44通过/1末尾DROP断言134、终态1；不改R51断言、不重试覆盖。
候选代码/两测试/注册不进入接受版本；只记录暂缓。通用长文本/复合PK写入及
其它普通问题仍未完成，原273的22/166/70/15不变，不push，Actions禁用。

后续[R136 标量微秒持久化](issue-review-r136-temporal-microsecond-storage.md)已有限修复：
新SQL标量TIME/TIMESTAMP/TIMESTAMPTZ采用显式schema-D微秒codec，旧9/A/B/C与native
工厂保持秒；同类型ALTER重写，索引/排序/NULL/epoch0/时区与实际冷重启纳入验证。
PG412全过、冻结旧408/281失败；新916 codec、92布局（同夹具补齐前四处截断失败）、
408协议与4真实CLI全过。两份fresh58 O2后各补两次私有TM/repeat0，干净6入口0；
最终28native+28Python+3本轮新ABI后fresh、核对未变TU再链接当前57库的Main=59
完整入口全0、独立核对0。未跑新1442全量，旧1436的两失败/1TLSskip保留；
原273仍22/166/70/15，不扩大为TIMETZ/数组precision/全部civil范围或安全专项完成。
不push，Actions禁用。

后续[R135 RETURNING时区呈现](issue-review-r135-timestamptz-returning-timezone.md)已有限修复：
PG最终134全过，冻结旧Root/干净旧版各134中73失败，当前134及原微秒266全过。
只在每条语句的协议结果中按OID1184/日期/会话时区呈现，不改DmlResult或heap，
同一Simple Query批次后续SET不能重画先前结果。正常1/repeat0，干净两O2 CPP+
56生产donor含Main的真实server两入口0；最终18native+24Python（23协议/1CLI）+
3核对R132未变TU后新链接当前库并执行的Main=45完整入口全0，独立0，不冒fresh TU。
新1438未全量，旧1436两失败/1TLSskip保留。新表三标量微秒INSERT/UPDATE22023
已另记为普通未修复，不由呈现修复关闭；原273仍22/166/70/15，不push、Actions禁用。

后续[R134 原生回调签名归属](issue-review-r134-core-native-callback-ownership.md)已有限修复：
干净旧实现1320控制/922失败，同一夹具修正后1320全过；只给实际ABS/LENGTH/UPPER/
LOWER builtin callback声明所有权，opaque替换保留原始参数和caller结果提示。
实际PG原132协议全过，干净一CPP+56匹配库的4完整入口0；Root正常1/repeat0，
22native+12wire+3核对R132未变测试TU后新链接当前库并执行的Main=37完整入口全0，
独立核对0。不是fresh Main TU或新1437全量；旧1436的两真实失败/1TLSskip保留。
原273仍22/166/70/15；通用其他callback和微秒heap仍普通开放，不push、Actions禁用。

2026-10-17[干净ecf632e9完整注册回归](issue-full-registered-ecf632e9.md)已结束：
1436个入口各一次、有序唯一、无漏跑/重复；1433实际通过、2失败、1 TLS环境skip。
标准runner/summary/driver及独立终态检查器均实际1；输入与产物独立核对0，不冒全绿。
全部58生产TU fresh O2/repeat0、3真实Main fresh测试TU/新链接57匹配库/无stubs并实际
隔离执行通过。两失败为background_worker的55P03异常和stored_function_engine_owner
末尾DROP断言，分别按原后台专项/R51范围只登记，不删除断言或猜同根因。
R129/R130原完整失败入口本轮通过，R129/R131/R132/R133新增协议入口也全过；
不由此关闭旧R51、普通其余缺口或原273。原状态22/166/70/15保持；下方各轮
“未全量”等保留当时历史，以本节新终态为准。不push，Actions仍禁用。

后续[R133 timestamp binary微秒](issue-review-r133-timestamp-binary-microseconds.md)修复合法
1114/1184非整秒被22P03拒绝：PG266全过、冻结旧版266/204失败，整数解码及负epoch
向下拆分保留精确微秒与R131 UTC标记。正常1/repeat0、核心266和12native+16wire+
3核对R132未变TU后新链接当前库并执行的真实Main=31完整入口全0，独立核对0。
不冒fresh Main、整秒heap升级或完整时间输入域/错误阶段完成；新1436尚未全量。

后续[R132 SET TimeZone TO](issue-review-r132-timezone-set-to.md)已修复旧Main把TO当值：
PG112全过、冻结旧版112/44失败。只消费通用SET的独立TO，不改引号内数据或专用
TIME ZONE值。正常1/repeat0、三个真实Main fresh TU编译/新链接/隔离运行，以及
12native+15wire+3Main=30完整入口均0，独立核对0；旧R124对象此次未复用。
新1435未全量；LOCAL/SESSION/DEFAULT及完整GUC事务/严格语法仍普通开放。

后续[R131 TIMESTAMPTZ binary时区](issue-review-r131-timestamptz-binary-timezone.md)已修复UTC
时刻被再次按会话本地解释：实际PG158全过、干净旧版和冻结Root各158/45失败。
仅在有限1184中间文本明确+00，1114与infinity/NULL不变；正常1/repeat0及12native+
14wire+3核对后新链接当前库并执行的真实Main=29完整入口全0，独立核对0。
新1434未全量；合法SET TimeZone TO拒绝另列普通R132继续，不冒本项已修。

后续[R130 原生函数结果声明](issue-review-r130-native-result-hint-precedence.md)已修复：干净旧库
实际705/1/smallint、168矩阵53失败，当前168全过；PG56参考全过。显式声明不被
UNKNOWN覆盖，真实callee优先，同名原生替换保留自身所有权。正常2/repeat0及
20native+12wire+3核对后新链接当前库并执行的真实Main=35完整入口全0，独立复核0。
F8F-03本项有限修复，旧完整三失败不改写；新1433未全量，R51仍暂缓。

后续[R129 原生数组投影声明](issue-review-r129-native-array-projection-types.md)修复四处类型提示
丢失isArray，并校正旧TEXT夹具；最终typed夹具在干净旧库实际134，PG/干净旧程序
51协议均全过，证明SQL契约与原生API缺陷须分开。正常1/repeat0及14native+
9wire+3核对后新链接当前库并执行的真实Main=26完整入口全0，独立复核0，不冒Main
TU重编或新1432全量。原8f完整三失败保留，F8F-03继续处理，R51不重开。

2026-10-17[干净8f0465fc完整注册回归](issue-full-registered-8f0465fc.md)已结束，实际退出1：
1431入口各一次、按预期顺序无漏跑/重复，1427实际通过、3失败、1 TLS环境skip。
全58 fresh O2/重复0、三真实Main fresh编译执行及输入/产物终态核对完成；
不把环境skip或运行完整当作通过。三项失败及修改方法已登记，普通数组投影
夹具与pg_typeof声明问题分别处理/提交，DROP DATABASE路径按原R51暂缓。
原273状态/勾选不提升，不push，Actions仍禁用；下方各轮保留其历史范围。

新增[R128 TIME二进制参数](issue-review-r128-time-binary-parameter.md)158协议控制全过，实际PG158
全过、旧完整132失败；初版旧fixture112项后缺RowDescription中断保留、强化失败
收集后按同一158期望重跑。NetworkServer实际normal1/repeat0均0，最终24native+
28协议+3核对后新链接当前57库并实际执行的真实Main=55不同完整入口全0。
原122核心匹配（原始2条PG内部来源字段不同保留）、原97精确匹配；本项接受
898native+530Python+3Main=1431，工作区901+533+3=1437含原六候选，554AST/13Bash
另检。无新完整注册/TLS/整库sanitizer证明，不冒重编Main或旧heap升级。

新增[R127 直接TIME过高精度](issue-review-r127-time-excess-precision.md)252原生/276协议全过，
PG276全过、固定旧原生225/协议246失败；2/0及复查1/0增量normal/repeat均0。
最终24native+27协议+3核对后重新链接当前57库并执行的真实Main=54不同完整入口
全0，不冒重编Main。原122核心行为全匹配，原始两条PG内部诊断来源不同不伪造；
原97仍精确匹配。本项接受898native+529Python+3Main=1430，工作区901+532+3=1436
含原六候选；553AST/13Bash另检。以下轮次保留各自历史输入，无新完整注册/TLS/
整库sanitizer证明。

新增[R126 TIME关键字精度语法](issue-review-r126-time-precision-grammar.md)严格549原生/326协议，
实际PG326全过，固定旧原生217/协议296失败。首版协议只完成19项后重置的实际1
保留并修正；末版23native+26协议+3核对后重连的真实Main=52不同完整入口全0。
原122现120匹配/2条TIME(7)通知及typmod差异，原97全匹配。本项接受897native+
528Python+3Main=1428，工作区900+531+3=1434含原六候选；552AST/13Bash另检，
无新完整注册/TLS/整库sanitizer证明，Main对象复用不冒称新TU编译。

新增[R125 合法TIME显式精度](issue-review-r125-time-precision-value.md)339原生/830协议，
PG830全过、旧830协议/317原生失败；最终22native+25协议+3真实Main=50完整入口
均0。未改Main TU核对后重连当前57库并执行，不冒重新编译。原97全匹配、122
仍12边界差异；本项接受896native+527Python+3Main=1426，工作区899+530+3=1432
含原候选，无新完整注册/TLS/整库sanitizer证明。

新增[R123 时间列precision](issue-review-r123-temporal-column-precision.md)782项原生/104项协议，
实际PG104全过、旧218原生/94协议失败；首版原生全过、协议六个R124路由失败保留。
最终匹配产物的有限回归见记录；不把metadata检查当微秒heap、CAST值舍入或新全量证明。

新增[R124 TIMETZ后缀表投影](issue-review-r124-timetz-postfix-projection.md)96项协议控制，
实际PG96全过、旧24失败，原schema回归暴露的六个42703断言保留。
本项重编与匹配产物的有限终态见记录，不冒新完整注册、真实TLS或整库sanitizer证明。

新增[R121 默认TIME文本输入](issue-review-r121-time-input-normalization.md)90native/177协议，
实际PG177全过、旧130协议/65native失败。最终18native+22协议+3真实Main=43入口
均0，Main是核对未改单元后重连当前57库并实际执行，未重编TU。原32对照全匹配，
97仍21显式精度差异。本项接受894native+3Main+524Python=1421，工作区897+3+527
=1427含原候选，无新完整注册回归、真实TLS或整库sanitizer证明。

新增[R119 时间戳binary](issue-review-r119-timestamp-binary-result.md)235低层/232协议，
实际PG232全过、旧144协议/192低层失败；保留首版完整36终态1及8个上游输入失败，
R120独立修复后共享41完整入口全0，原64条观察当前全匹配。本项接受组合
893native+3Main+523Python=1419，工作区896+3+526=1425含保留候选；无新完整注册
回归、真实TLS或整库sanitizer证明，不捏造R120之后又一轮编译/41重跑。

新增[R120 日期逆换算](issue-review-r120-date-cycle-inverse.md)独立枚举3652059天
及152协议控制，实际PG152全过、旧48失败；匹配58单元/三真实Main及有限终态见记录。
本项独立提交后892native+3Main+522Python=1417，工作区896+3+526=1425含尚未提交
R119及原候选；无新完整注册、真实TLS或整库sanitizer证明，组件sanitizer不冒充整库。

新增[R117 TIME binary](issue-review-r117-time-binary-result.md)121低层/144协议
控制，实际PG144全过、旧114协议/58低层失败。最终有限匹配终态见记录；本项
提交后891native+3Main+521Python=1415，工作区894+3+524=1421含保留候选，无新
完整注册、真实TLS或整库sanitizer证明，R118真实Main结果不冒充本项重跑。

新增[R118 时态物化布局](issue-review-r118-temporal-view-materialization.md)186条
协议控制，实际PG全过、固定旧60失败、首版仍36失败均保留。最终匹配回归/真实
Main终态见记录。本项提交后的接受组合为890native+3Main+520Python=1413，
工作区893+3+523=1419含保留候选；未获新完整注册回归、真实TLS或整库sanitizer证明。

新增[R116 视图完成tag](issue-review-r116-view-command-tags.md)54条完整协议
控制，实际180006全过、旧14tag失败。有限匹配回归与真实终态见记录；接受
890native+3Main+519Python=1412，工作区893+3+522=1418含保留候选，无新完整注册、
真实TLS或整库sanitizer证明。原273统计不提升。

新增[R113 INTERVAL binary结果](issue-review-r113-interval-binary-result.md)117低层
及171协议控制，实际PG171全过、依赖修复后旧149失败/原生78失败。最终有限回归
与失败原文见记录；接受890native+3Main+518Python=1411，工作区893+3+521=1417
含未接受R98/R109候选，无新完整注册运行，不证明真实TLS或整库sanitizer。

新增[R114 逻辑长度](issue-review-r114-physical-attribute-length.md)102原生及28协议
控制，PG28全过、旧22失败；候选曾剩5个既有CHAR失败，R115另项修复。最终有限
匹配回归见记录。本项接受889native+3Main+517Python=1409，工作区892+3+521=1416，
含保留候选和未接受R113；尚无新完整注册回归，原273统计不提升。

新增[R115 标量别名投影](issue-review-r115-scalar-alias-projection-metadata.md)228条
完整协议控制：实际180006全过、旧13失败。有限匹配产物回归及逐版失败见记录；
本项接受组合888native+3Main+516Python=1407，R114另项。工作区892+3+521=1416
含未接受候选及R113，未跑新完整注册集合，不证明真实TLS或整库sanitizer。

新增[R110 数组二进制结果](issue-review-r110-array-binary-result.md)110条
协议和182条低层控制，实际180006参考110零失败、旧75失败。有限回归和逐版
失败见记录；本项提交后接受组合888native+3Main+515Python=1406，含保留候选
的工作区891+3+518=1412，均无新完整注册回归。未证明真实TLS或整库sanitizer。

新增[R112 具名数组输入](issue-review-r112-unconstrained-named-array-input.md)
57条协议控制，实际参考180006零失败、固定旧程序44失败；逐版失败、正常/repeat
与有限门禁见记录。只新增一个协议注册入口，本项提交后的注册组合为887native+3Main+
514Python=1404，无新完整注册回归；R109/R110 候选另计，不复用旧全量证明。

当前运行结果、失败明细和复现方法见[代码审查索引](review-index.md)。验证入口为 `bash scripts/build_tests.sh`，只运行其已注册集合；未注册测试需单独运行或补入入口。

后续[R109](issue-review-r109-array-positions.md)已按用户要求跳过，候选源码与
测试保留未提交。其388/248和66有限完整入口全0是候选历史证据，不计已修复
或接受manifest；旧248/160、388/290、242/154与228参考日志均保留。

后续[R108数组元素相等](issue-review-r108-array-position-typed-equality.md)新增
354原生与258协议控制，最终参考258零失败、固定旧版本158失败；旧原生320/183
和早期239/153保留，逐版失败与原生夹具校正细节见记录。第五版
32native/29协议/3真实Main共64完整入口及28补充JSONB均0，核心/最终
fence0。两个新增注册入口对应接受manifest887native+3Main+513Python=1403，
含未接受R98工作区889+3+515=1407；本项有限验收不是新完整注册回归。

后续[R107长度函数签名](issue-review-r107-string-length-signatures.md)增加607
原生及654协议控制；最终参考654零失败、旧程序392失败，旧原生607/440失败。
首两版实际失败和逐版artifact核对分别保留；实际候选与邻接验收以本项记录
为准。新增两个注册入口，本项提交后的组合886native+3真实Main+512Python=1401；
工作区含R98时为888+3+514=1405。两套新组合均无全量终态，R106草稿未注册。

后续[R104 网络值转换](issue-review-r104-network-value-conversions.md)实际PG/
Root649、原738及完整SDK、21native/17协议/3真实Main通过；五CPP实际4/3/2
增量normal/repeat及核心/最终独立核对均0，按单项本地提交。旧649/419、SDK134、
首版582/3与649/58、第二版649/1保留；无新1399/1403全量，原273不变。

2026-10-16后续[R105 限定NAME→INET/CIDR](issue-review-r105-network-name-input.md)
PG/Root738、完整SDK及21native/16协议/3真实Main全过，自身真实全58/
normal/repeat、核心与最终独立核对均0，按单项本地提交。旧738/34、SDK134
保留；无新1398/1402全量；R104后续独立修复，原273未关闭。

2026-10-16后续[R103 NAME→MAC](issue-review-r103-macaddr-name-input.md)实际参考
最终PG/Root366、完整SDK及21native/16协议/3真实Main通过，真实全58/normal/
repeat和独立终态fence全0，单项本地提交。旧366/30、SDK134及首版358/6与
366/12失败保留；75纯组件sanitizer不冒整库证明，无新1398/1402全量。

2026-10-16后续[R102 INET/CIDR输入](issue-review-r102-network-input-notation.md)
最终旧188组件72失败、672协议389失败；当前188/纯组件ASan/UBSan、PG/Root672、
2558对照及21native/16协议/3真实Main通过；最终真实全58/normal/repeat、匹配
核心及最终fence全0，单项本地提交。首版672/23保留；无新1398/1402全量，
R103/R104/R105后续各独立修复，不把本项写成所有网络类型问题均已完成。

2026-10-16后续[R101 MAC输入](issue-review-r101-macaddr-input-and-widths.md)核心75、
PG/Root328及236额外codec差分通过，纯头ASan/UBSan75通过；二十完整native和
十五协议及三个真实Main全0。头依赖修正后全58真实重编0，所有对象/程序字节
相同，新匹配核心2/328及最终fence全0；单项本地提交，不冒新1396或工作区1400
全量。旧75/38、328/186及首版314/5失败保留，不挪用旧1391全量结果。

2026-10-16后续[R100几何输入](issue-review-r100-geometric-coordinate-delimiters.md)
最终232组件、PG/Root230、十个完整native及七个完整协议通过，纯组件ASan/UBSan
通过；旧232/104、230/135、首轮准备超时与旧夹具134均保留。只证明本项范围，
不把下列1391历史全量用于本项后的1394或含未提交R98的1398组合。

2026-10-15最新[冻结5091b39c完整回归](issue-full-registered-5091b39c.md)实际退出0：
881 native、3真实Main及507 Python共1391入口各一次，1390实际通过、1 TLS环境
skip、0失败。源码/生产产物及全部58 receipt、57 test库字节独立终态核对0。
不证明真实TLS、原273全部完成或旧间歇性失败根因已修复；下方保留旧输入结果。

2026-10-13 审查输入为 `a502ffa3`。本轮完整注册回归已终态：普通native 783通过/2失败，真实Main入口3通过，Python驱动436返回成功/8失败，共1232项、10项失败；Python成功数包含一个无OpenSSL的TLS显式跳过。驱动实际exit1，源码及冻结生产二进制终态校验通过，注册集合无遗漏/重复。完整失败清单、实际SQL差分、正常API错值和修复建议见[审查报告](code-review-2026-10-13.md)。前次同源码专项60/60 native、45/46 wire中的ALTER ARRAY超时仍保留，不能用本次单次通过消除；下方历史全绿记录不代表当前HEAD。

## 历史记录：2026-08-14

以下仅保存当时 137 个 C++ 测试和 2 个 E2E 的 PASS=139、FAIL=0 记录，不代表当前 HEAD、当前测试总数或所有功能已验证。
当前功能状态见[原总账](postgresql-18-gap-audit.md)，原始旧依据为[命令手册](commandsList.md)和[历史 TODO](all-gaps-todo.md)。

本轮完成性能与并发硬化轮次的全面复验（13 个提交后）：

- **全量回归**：`./scripts/build_tests.sh` 完整重跑，137 个 C++ 测试 + 协议 E2E + 窗口函数 E2E（13/13）全部通过，0 失败。
- **并发与事务专项独立复跑**：`concurrency_test`（含 INSERT/UPDATE/DELETE 回滚恢复二级/复合/Hash/TOAST、checkpoint 持久化、MVCC 快照隔离、性能子项）、`wal_truncate_test`、`redo_crash_recovery_test`、`prepared_transaction_test` 在独立临时目录中单独复跑，全部通过。
- **线程安全压测（新增，本轮引入）**：针对 `BufferPool` 新的两阶段加载/孤儿帧失效语义，构造多线程压力（8 读者 + 改写者 + 失效器 + 4 帧小池强制驱逐/重载），分别在 TSAN 与 ASAN+UBSan 下运行：**0 数据竞争报告、0 内存错误、0 内容损坏**。修复前同一压测报告约 1 万次内容损坏（`invalidatePage` 在并发读者下重置被 pin 帧导致帧复用污染）。
- **确定性失效语义回归（新增）**：pin → invalidate → unpin → 重取必须返回新鲜正确数据；invalidateAll 后重取同样正确；同线程持 pin 期间重取同页返回不可用而非死锁。
- **性能基准复测**（与优化前同机对比）：事务内带 PK 插入 200 行 86–114ms（1760–2370 行/秒，优化前 11.7 行/秒），commit 12–15ms（优化前 267ms）；并发套件 500 行插入 + COUNT/SUM/MAX/GROUP BY 聚合 1.4ms、插入 265–305ms（优化前约 106s）；索引等值查找 14 次查找 1–2ms。
- **回归发现的缺陷均在当步修复**：removeMulti 二分改造后等值键 RID 扫描、schema 缓存引入期捕获的 RLS/fillfactor/catalog 快照隔离、hashidx 失效误指 secidx 等，均由现有测试套件（gin_brin_index_test、policy_test、fillfactor_test、catalog_snapshot_test 等）拦截后修复，最终保持全绿。

本轮新增 Volcano 执行失败契约回归：合成 `open()` 失败必须由
`executePlanChecked()` 返回 `ok=false` 和错误信息，不能与合法空结果混淆；所有测试调用
方均使用 checked contract，旧空结果兼容入口已删除。

本轮新增运行时统计持久化回归：数据库/表统计可从当前格式快照重载，损坏 magic/版本或
尾随数据会 fail-closed；关系删除标记不会在下一次持久化时恢复旧表统计。

本轮新增协议配置边界回归：不同 backend 的 `statement_timeout` 会话值互不污染，非管理员
不能执行 `SET GLOBAL`，进程级参数不能通过普通 `SET` 修改；planner 参数变化后旧 `EXPLAIN`
缓存不会被复用。

本轮新增配置文件回归：非法键和值、非有限浮点、越界整数和损坏配置加载均拒绝；候选配置
失败不会污染当前对象，成功保存使用临时文件、`fsync` 和原子替换。

本轮新增 typed DDL 类型边界回归：未知列类型和非法类型修饰符在物理变更前拒绝，不再
静默降级为 `varchar`；合法 `SERIAL`、`NCHAR`、`BINARY` 映射及自动序列仍保持可用。

本轮新增 DDL 路由收敛回归：`CREATE/DROP DOMAIN`、`CREATE/DROP SEQUENCE`、
`CREATE/DROP SCHEMA`、角色/用户和数据库均通过 typed bridge 执行，确认标准入口不再依赖
`main.cpp` 旧字符串分支。
同时验证 `ALTER SEQUENCE ... RESTART ... INCREMENT BY ...` 和 `RENAME TO` 由 typed bridge
执行；重命名保持 catalog OID、更新 `nextval` 默认表达式，重启后新名称可用且冲突不会破坏原对象。
存储完整性回归还验证了损坏 heap page、损坏/截断边界被拒绝，以及 page compaction 不覆盖 special space。

本轮新增序列持久化回归：`sequence_full_test` 验证失败的 `ALTER SEQUENCE` 不会遗留启动恢复快照，
重启后 `nextval` 保持已提交状态；序列文件拒绝尾随字段、非法 cycle/范围/cache，写入使用原子发布，
`INT64` 正负边界、`currval`、`setval` 和非循环耗尽行为均通过。

本轮新增 DDL 会话生命周期回归：typed `DdlExecutor` 执行结束后恢复原线程局部会话指针，避免局部
`Session` 销毁后后续嵌入式存储调用解引用悬空指针；完整序列专项测试通过。

本轮新增数据库生命周期持久化回归：新建数据库的 `tlist.lst`、字符集和 public schema 标记必须完整，
checkpoint 文件必须是精确的当前三字段格式且拒绝尾随数据；完整回归继续验证物理备份标记和恢复路径。

本轮新增数据库 DDL 错误传播回归：构造数据库目录创建失败时，`CREATE DATABASE` 返回错误并输出
SQLSTATE，不会伪报成功；`DROP DATABASE` 也只接受 `StorageEngine::OK` 为成功结果。

本轮新增 DDL 路径与重复对象回归：重复 `CREATE SCHEMA` 必须失败，`IF NOT EXISTS` 才跳过；损坏
domain sidecar、非法数据库路径、序列/视图/关系名称路径组件均 fail-closed，schema marker 和
domain 创建使用原子发布。

本轮新增辅助对象 sidecar 回归：view、function、TVF、procedure 的元数据父路径被普通文件占用时
返回 `IO_ERROR`，正常定义使用原子发布，重复定义返回 already-exists 且不会覆盖旧内容。

本轮补强 `DROP TABLE ... CASCADE` 回归：构造 catalog 依赖表及其索引，验证 CASCADE 同时清理
依赖表的物理关系和索引 sidecar，再删除依赖 catalog 行；RESTRICT 预检仍不产生任何修改。

本轮新增索引 sidecar 故障回归：`.secidx` 或 `.idxnames` 被目录占用时，`CREATE INDEX` 返回错误，
不会留下物理索引或名称映射；索引元数据改写采用原子发布，复合索引删除保留剩余元数据选项。

本轮修复 `planner_runtime_stats_test` 的后台 writer 清理竞态：测试 teardown 通过 `dropDatabase()` 释放
数据库缓存与 WAL 所有权后再清理目录；专项测试和完整回归均通过，最终基线保持 `PASS=139 FAIL=0`。

---

## 测试环境

- OS: Linux 6.8.0-117-generic
- Compiler: g++ -std=c++17 -O2 -pthread
- 标准编译命令：`./scripts/build.sh`；规范测试入口：`./scripts/build_tests.sh`；`./scripts/run_all_tests_fast.sh` 是只改变输出形式的安静外壳
- `build_tests.sh` 会在测试前检查 `dbms_main` 的源码、头文件、manifest 和构建配置指纹；二进制缺失或过期时自动原子重建，因此完整回归不依赖调用者预先执行 `build.sh`。
- 生产二进制构建失败时，`build_tests.sh` 会立即 fail-closed，拒绝运行任何可能过期的 E2E 二进制。
- 每个 C++ 测试在独立临时工作目录中运行，回收目录后才进入下一项；顺序回归不再共享 `.txnid`、WAL、catalog 或后台 worker 的相对路径状态。
- 本轮构建质量检查：主程序在 `-Wall -Wextra` 下无编译警告；修复 join cost 参数错误及 parser/测试中的未使用代码。
- schema 格式回归：当前格式 `0x44420009`，长表名、列名和约束名可完整写入/读取；旧格式仍按 fail-closed 策略拒绝。
- 窗口函数端到端测试：由统一回归入口执行 `tests/window_e2e_test.py`（13 用例，包含 Volcano `WindowAgg` 排名/偏移、窗口聚合、`ROWS/RANGE/GROUPS` frame/exclusion、OFFSET 和命名窗口路径，使用隔离临时目录和临时管理员账号）
- Volcano 窗口单元测试：`tests/volcano_select_phase51_test.cpp` 覆盖多窗口独立排序、排名并列、分区边界、`lag`、默认 frame、`EXCLUDE CURRENT ROW`、count/first/last/ntile/percent_rank/cume_dist 及文本/JSON EXPLAIN。
- Volcano 分组单元测试：同一测试覆盖 `GroupAggregateOp` 的常见聚合、`HAVING`、`ROLLUP`/多个 grouping set、省略分组列的 `NULL` 输出及文本/JSON EXPLAIN；主 SQL 手工回归同时验证普通 GROUP BY 与 ROLLUP。
- Semi/Anti/Existence/Quantified/Scalar 回归：`tests/volcano_select_phase51_test.cpp` 验证未关联 `IN`/`NOT IN`、inner 过滤、重复键、NULL 语义、`QuantifiedSubqueryFilterOp` 的 `ANY`/`ALL` 空集与 NULL 语义、`ExistenceFilterOp` 的 `EXISTS`/`NOT EXISTS` 及 `ScalarSubqueryProjectOp` 的 0/1 行与 cardinality error；协议 E2E 验证真实 PostgreSQL 查询路径及 NULL 结果。
- 普通聚合结构化回归：`tests/volcano_select_phase51_test.cpp` 验证带过滤条件的隐式空 grouping set；协议 E2E 验证主 SQL `COUNT(*)`/`SUM()` 已走统一聚合计划路径。
- TOAST 回归：`tests/toast_test.cpp` 验证大值插入/更新/删除、zlib 压缩后的物理体积和解压读取。
- 事务回滚回归：`tests/concurrency_test.cpp` 验证 INSERT、UPDATE、DELETE 的普通 `ROLLBACK` 与 `ROLLBACK TO SAVEPOINT` 一致恢复主键、单列二级、复合、Hash 索引和 TOAST；DELETE savepoint 回滚后在同一事务提交仍可读，UPDATE/DELETE 场景在新 `StorageEngine` 重启后再次校验堆、Hash 与大字段内容。
- 锁管理器并发回归：`tests/lock_manager_concurrency_test.cpp` 使用真实双线程验证等待图死锁只释放一个受害者、表锁重入与共享锁升级、row/page token 归属、等待期间资源生命周期和批量清理；`tests/cross_backend_lock_test.cpp` 验证独立 `StorageEngine` backend 共享同一锁注册表、隔离不同数据库的同名表、backend-local 超时策略，以及独立 fork 进程对同一数据库的表/row/page 锁阻塞、释放后获取和 gap 锁阻塞；重复运行 10 次通过。
- 保存点锁资源回归：`tests/lock_manager_concurrency_test.cpp` 验证保存点后新增的表/row/page/gap token 会在检查点回滚时释放，并验证保存点前共享表锁在后续升级后恢复为共享模式。
- 复制管理器并发回归：`tests/replication_concurrency_test.cpp` 验证复制槽激活/停用、值快照读取、standby/conninfo/sync 状态并发写入及 slot 生命周期；复制槽定义校验和 active slot 禁止删除也有覆盖。
- 锁失败传播回归：`tests/lock_failure_propagation_test.cpp` 持有真实表级排他锁并在另一线程执行建索引，验证调用方收到 `LOCK_CONFLICT`、等待图清理且不会继续发布索引。
- DDL 辅助对象回滚回归：`ddl_transaction_skeleton_test` 验证失败时撤销 view、materialized view、UDF/TVF、procedure、trigger、RLS policy 和 collation；collation 最后一个条目删除后物理文件也会清理。
- DDL 外层事务回归：`ddl_transaction_skeleton_test` 验证多个 CREATE 跨语句由外层 `ROLLBACK` 逆序撤销，并验证 `ROLLBACK TO SAVEPOINT` 只撤销保存点后的 CREATE；含该内存 undo 队列的事务会拒绝 `PREPARE TRANSACTION`。
- 2PC 跨 backend/跨进程重启回归：`prepared_transaction_test` 由独立子进程创建 prepared 事务，验证 PREPARE 前刷出 heap/index 缓存、prepared 文件 durable 原子发布、表/row/page/gap 锁在另一进程完成前持续阻塞、重启后锁 ownership 重建、`COMMIT PREPARED` 后数据可见、`ROLLBACK PREPARED` 后数据不可见、顺序/索引条件查询均不可见，以及事务内拒绝完成 prepared transaction。
- DDL DROP/REPLACE 回归：`ddl_transaction_skeleton_test` 验证 DROP TABLE 与 CREATE OR REPLACE VIEW 的变更前快照恢复，外层回滚先恢复对象再撤销同一事务中的新增行；快照已被 DDL 修改后另一条快照型 DDL 及 SAVEPOINT 会 fail-closed。
- CMake 与脚本构建共同读取 `cmake/dbms_sources.txt`，并在配置阶段校验源文件存在性、重复项和 `src/main.cpp` 入口，避免生产源文件列表漂移。
- CMake 现在提供 `check`/CTest 标准入口；它们调用唯一的 `scripts/build_tests.sh` 完整测试编排器，不再复制 standalone test 的链接、stub 选择或 E2E 调度逻辑。
- `snapshot_export_import_test` 现在覆盖 v2 快照数据库绑定、版本/长度/尾随字节/XID 校验、重复导入、跨数据库导入，以及首次读写后拒绝替换快照；`DELETE`/`TRUNCATE` 也会阻止事务中途导入快照。
- `scripts/build.sh`、`build_tests.sh`、`build_one_test.sh` 和 `run_all_tests_fast.sh` 共同读取 `scripts/build_common.sh`；本轮验证了配置指纹失效与增量单测入口。
- `./scripts/build_tests.sh` 是唯一的完整测试编排实现，会先通过 `build_common.sh` 的共享逻辑检查并构建 `dbms_main`，再缓存生产对象、逐个测试独立链接运行，并自动执行两个 E2E；因此不依赖调用者预先运行 `build.sh`。`run_all_tests_fast.sh` 仅捕获其成功输出并显示 PASS 计数，失败时原样打印完整编译/链接/运行日志。
- 文档一致性检查确认根目录旧版 `MANUAL.md` 已移除，`docs/MANUAL.md` 是唯一受 README 与测试/部署文档引用的完整手册。
- PostgreSQL 协议 E2E 覆盖普通顶层多行 DML 和写 CTE 的 statement atomicity：批量 INSERT 或 CTE INSERT 后续行冲突时，前序行不会残留；成功的 `WITH ... RETURNING` 结果也能通过协议返回。
- PostgreSQL 协议 E2E 覆盖 typed `CREATE TEMP/TEMPORARY TABLE`：临时表在所属会话内持续可用、遮蔽同名永久表，不同 backend 可创建同名临时表，其他会话不可见，断开连接后对象被清理。
- PostgreSQL 协议 E2E 额外覆盖 `ON COMMIT DELETE ROWS` 与 `ON COMMIT DROP`：提交后分别保留空表和删除临时表；parser 单测覆盖 `PRESERVE/DELETE/DROP` AST 状态。
- 新增临时表重启生命周期回归：`tests/temp_restart_cleanup_test.cpp` 模拟临时 relation 已落盘后重启，确认 `tlist.lst`、schema/data/index/TOAST 等残留全部清理；完整测试入口通过。
- 同一回归还覆盖 `CREATE TABLE` 在损坏 heap 初始化失败时的清理：schema、heap 和 `tlist.lst` 均不残留；唯一/索引初始化失败路径同时释放元数据锁。
- `tests/ddl_transaction_skeleton_test.cpp` 新增后处理失败回归：强制约束 metadata 写入失败，验证 DDL executor 的物理表创建会通过事务记录完整回滚。
- 同一测试覆盖 CatalogManager 只读 drop plan 与延后应用边界；`DROP TABLE` 的物理 relation 仍独立于 catalog plan，executor 只在物理删除成功后应用计划。
- 同一测试新增 `DROP SCHEMA` 的 namespace drop plan 延后应用回归，确认 catalog 计划与物理 schema 删除相互独立。
- 新增数据库生命周期回归：`DROP DATABASE` 释放数据库级缓存后，同名重建不会继承旧 CLOG/WAL/page/index 状态。
- 新增 typed `ALTER TABLE` 路由回归：验证 AST bridge 的 ADD COLUMN、DEFAULT、NOT NULL、RENAME COLUMN/TABLE，以及 `RENAME TO` 不再误判为列重命名。
- 新增 ALTER statement atomicity 回归：`ddl_transaction_skeleton_test` 验证多子命令中前一动作已落盘、后一动作失败时，整句 schema 会从 DDL 快照恢复，且事务快照不会残留。
- 新增外置 tablespace 事务快照回归：表移动到自定义 tablespace 后，在事务内移回默认目录并回滚，快照恢复 schema 与外置 heap 数据；同一场景覆盖全新引擎读取。
- 新增 DDL 提交失败回归：延迟 CHECK 使 `StorageEngine::commitTransaction()` 失败时，`DdlTransaction` 返回失败、清理事务状态，并恢复已执行的物理 `ALTER TABLE` 快照，调用方不会输出成功结果。
- DDL 隐式提交错误传播回归：`deferrable_test` 在延迟 CHECK 失败的显式事务中执行 `CREATE DATABASE`，确认隐式提交失败后 DDL 不继续执行、事务清理且目标数据库不存在。
- DDL 隐式提交入口审计：规范执行器和 legacy DDL 分发共用失败即停止的提交守卫，避免兼容路径吞掉 `commitTransaction()` 错误。
- 新增事务资源生命周期回归：普通 COMMIT/ROLLBACK 不会遗留整库快照；DDL 快照在 COMMIT/ROLLBACK 后清理；前一事务的 deferred CHECK 导致隐式提交失败时，后续 `BEGIN` 返回错误且不会开启新事务。
- 新增 DDL 快照并发回归：文件级 DDL 快照使用事务 ID 独立命名，并持有数据库级排他锁；另一 backend 在快照事务结束前阻塞，避免物理恢复覆盖并发提交。
- 新增 DDL 快照重启回归：模拟未完成的 `.txn_backup.<xid>`，新 `StorageEngine` 按无 COMMIT 证据恢复 DDL 前的 schema 并清理快照目录。
- WAL 提交回归继续覆盖 COMMIT 记录与刷盘路径；`WALManager::XLogFlush()` 现在返回 segment 同步结果，提交在 WAL 不可用/刷盘失败时不会先发布 CLOG 可见性。
- CLOG 持久化回归继续验证段重载；段写入现在完整写入临时文件、`fsync` 后原子替换并同步 `pg_xact` 目录，写失败不会提前清除 dirty 状态。
- CLOG 提交故障回归：删除事务的 `pg_xact` 目录后，提交返回 `IO_ERROR`，行级修改被撤销并追加 ABORT WAL；新 `StorageEngine` 重启恢复也不会重新出现该行。
- CLOG 跨 backend 回归验证两个独立 `CommitLog` 实例对同一段的按位合并，以及已打开读缓存发现外部段替换后刷新，不会丢失或永久读取旧事务状态。
- CLOG 截断回归验证旧段正常回收，并在文件锁导致持久化失败时保留最后一个 durable segment image。
- WAL 截断回归验证未归档段不会删除、已归档且早于保留 LSN 的段会回收、最早保留段可被恢复扫描发现，并且后续 checkpoint 不会为已删除段重建 archive marker。
- WAL 归档故障回归验证归档目录不可用时源段和 `.ready` 均保留，修复目录后可重试完成原子归档与 `.done` 发布。
- Catalog 持久化回归验证临时文件替换/目录同步失败会返回错误，并保留原有 catalog 目标文件，不会把部分写入报告为成功。
- WAL 崩溃恢复回归覆盖首个合法 LSN 0、未初始化页 LSN、已提交插入/删除重做和未提交事务回滚；`redo_crash_recovery_test` 同时校验 heap 与主键 B+Tree，并覆盖同一事务连续插入两行后 before-image 逆序 undo 不会留下中间状态；多实例 WAL writer 的尾部刷新/锁也由 catalog snapshot 场景覆盖。
- 新增 `recovery_integrity_test`：构造 CRC 正确但 payload 损坏或路径越界的索引 WAL 记录，验证恢复拒绝非法镜像、不会写出数据库目录外文件，并以异常中止启动；heap/index 应用失败均不再静默忽略。
- `recovery_integrity_test` 同时覆盖损坏 WAL 长度/`xl_prev` 链、非法 heap page image 和非连续 page ID 输入；WAL 截断回归使用完整有效记录构造 segment，并验证记录所在完整 segment 的刷盘边界。
- Checkpoint 回归现在断言 checkpoint 真实成功；页刷盘、checkpoint WAL、checkpoint 文件 fsync 和 archive status 任一失败都会返回失败，不再伪报 `CHECKPOINT completed`。
- Checkpoint 活动事务回归验证：活动事务期间不会推进数据库恢复起点；事务结束后 checkpoint 才刷已加载 heap/index 缓存并成功持久化 checkpoint WAL/LSN。
- SSI 回归：`phase5_remaining_test` 验证非空索引谓词按 heap page 登记 SIREAD，单列主键/二级 B+Tree 比较谓词登记逻辑索引范围并与写键进行重叠检测，非相交页事务可同时提交；跨页读写危险结构仍有一个事务返回 `SERIALIZATION_FAILURE`，空谓词关系级保护继续生效。复合/表达式/部分索引、其他访问方法的物理 predicate lock 和完整 SSI 规则仍未完成。
- BufferPool 回归覆盖一帧缓存的脏页淘汰/重载与 pinned 页保护，确认缓存不足时返回失败而不是强制淘汰活动页。
- GIN/BRIN/B+Tree 回归继续通过，并覆盖 root split/search/range、长键固定宽度规范化、重开索引后的查找和精确删除；B+Tree 的 header/node I/O 失败现在 fail-closed，避免底层页读取失败升级为空指针崩溃。
- B+Tree 多值索引回归补充 `(key, RID)` 精确删除：删除 6000 条跨叶重复键中的单个 RID 后，其余相邻 RID 仍可检索，重复删除会正确失败；生产 DML/事务回滚路径直接使用 `StorageEngine` 的 B+Tree 实现。

- 索引架构清理回归：删除未接入生产执行链的 `IIndexAM`、B+Tree/Hash 适配器及独立 GIN/BRIN 类；保留并增强测试以验证 `StorageEngine` canonical GIN/BRIN 创建、查询和损坏 sidecar fail-closed。
- CREATE INDEX 事务回归：B-tree、Hash、GIN、BRIN 在显式外层事务回滚后均清理物理文件、名称映射和 catalog；重复名称拒绝，`IF NOT EXISTS` 安全跳过。
- 事务索引 WAL 回归通过：B+Tree/Hash `flush()` 和事务边界统一缓存刷盘路径已接入；脏索引刷盘会记录完整文件 before/after image，提交前 WAL 先刷盘；`wal_basic_test` 检查索引 WAL 记录，`redo_crash_recovery_test` 验证未提交插入回滚、已提交插入恢复和已提交删除恢复。
- Hash/GIN/BRIN 持久化回归覆盖 `HashIndex` 二进制格式重载、StorageEngine GIN/BRIN sidecar 原子发布和损坏文件拒绝；Hash dirty 状态在关闭失败时保留。
- StorageEngine GIN/BRIN 回归通过真实 DDL 构建后查询索引内容，验证 `forEachRow()` 页面失败契约、GIN 原子发布和 BRIN 二进制范围文件路径。
- 索引/执行器错误传播回归通过：B-tree/复合/全文/GiST/SP-GiST/Hash/GIN/BRIN 构建与 `REINDEX` 不再吞掉 heap 扫描失败，Volcano 顺序扫描和索引扫描遇到页面读取失败时 fail-closed；`volcano_select_phase51_test` 通过。
- 执行器架构清理回归：删除未使用的 `IExpr`/`PlanNode`/`IExecutionPlanner` 接口后，Volcano 算子仍统一实现 `IOperator`；`IndexScanOp` 改用带 page shared lock 和 MVCC 检查的直接 RID 回表，`volcano_select_phase51_test` 的索引、bitmap 和组合算子路径全部通过。
- RLS 访问路径回归：带 `owner` 索引的启用 RLS 表，Volcano 计划强制选择策略感知 `TableScanOp`，只返回当前用户可见行；索引绕过测试通过。
- 访问方法安全回归：移除不具备 MVCC/NULL 位图证明的伪 `IndexOnlyScanOp`；Bitmap AND/OR 改用受保护 RID 回表并保持原谓词 recheck，Volcano 专项和全量回归通过。
- 统计/查询/DML 错误传播回归通过：`ANALYZE` 失败不发布半成品统计文件，过滤器、聚合、JOIN、FK/EXCLUDE 检查、ON CONFLICT 目标扫描、表重写和 TOAST/page 写入路径不再把页面 I/O 失败当成空结果或成功；Volcano 并行 page-range scan 会向算子返回失败。
- 新增 schema 格式完整性回归：截断 schema 与错误 magic 均 fail-closed，不会返回可写的部分 schema。
- Docker 镜像构建：`docker build -t dbms-cpp:verification .` ✅（OpenSSL 真实 TLS）；Compose 配置检查 ✅。
- CMake：当前验证环境未安装 `cmake`，配置/编译未执行。
- 网络启动安全边界：无 TLS 证书/私钥时默认拒绝启动；明文仅由显式 `--insecure` 开关启用。
- 网络服务生命周期回归：`tests/network_server_lifecycle_test.cpp` 验证端口占用时 `startServer()` 返回失败、显式 shutdown 停止监听并等待客户端 worker，活动连接槽位最终归零。
- 网络连接容量回归：accept 路径原子预留 `maxConnections` 槽位，认证失败和 TLS 握手失败均释放槽位。
- 网络输出并发回归：`tests/network_security_test.cpp` 并发创建 4 个线程局部输出捕获，每个线程写入 32 行并验证没有其他会话内容串入。
- 角色 DDL 回归：`CREATE/ALTER/DROP ROLE`、`CREATE/ALTER/DROP USER` 统一走 Catalog/AST 路径，混合大小写密码通过真实验证。
- PostgreSQL 协议 E2E：`python3 tests/postgres_protocol_test.py` 验证 SSLRequest/plaintext 协商、StartupMessage、真实 SCRAM-SHA-256 握手、simple query 和 Parse/Bind/Execute/Sync，并覆盖 NUMERIC 精确存储、1700 binary 参数/结果与 PostgreSQL INSERT command tag。
- 协议扩展查询回归：验证 `ParameterDescription`、文本和 binary `int4` 参数 `$1`、binary `int4` 结果、statement `Describe`、portal/statement `Close` 的真实 Parse/Bind/Execute 生命周期，以及 OID/长度/属性号/表 OID RowDescription 元数据。
- 协议日期时间/UUID binary 回归：验证 `date`、`time`、秒精度 `timestamp`/`timestamptz` 和 `uuid` 的 binary 参数、binary 结果、OID/格式元数据及单列空格值不被拆分。
- 协议 portal 分页回归：验证 `Execute maxRows` 分批返回、未耗尽时发送 `PortalSuspended`、耗尽时发送 `CommandComplete`，以及耗尽 portal 再执行不重复返回数据。
- 协议 ACL 回归：alice 将 `SELECT` 授予 `analyst`，bob 通过角色继承读取表数据；bob 未获得 `INSERT` 时真实协议返回权限错误。
- 角色继承边界回归：`NOINHERIT` 用户保留原始成员关系但不获得父角色 ACL/RLS 权限，切换为 `INHERIT` 后两条权限路径同时生效；`pg_hba.conf` 角色匹配仍使用原始成员关系。
- 对象授权回归：非所有者即使带有管理员标志也不能执行 `ALTER TABLE ... OWNER TO`；所有者仅可转移给能够 `SET ROLE` 到的目标角色；`SET ROLE` 后超级用户绕过和表 ACL/RLS 有效角色均正确收敛。
- owner ACL 回归：无显式 `.permissions` 记录时，表 owner 仍拥有表级/列级权限、`GRANT OPTION` 和 `all` 权限视图。
- 表级 GRANT OPTION 回归：验证表级 grant marker 仍提供基础列权限，独立撤销只移除 grant option，存在下游授权时 RESTRICT 拒绝、CASCADE 递归撤销并清理 grant chain。
- 角色 ADMIN OPTION 回归：验证角色关系的 ADMIN OPTION 授予、重复授权升级、只撤销 ADMIN OPTION，以及撤销后成员关系仍保留。
- 默认权限回归：验证 typed `ALTER DEFAULT PRIVILEGES` 的表级多权限 GRANT、幂等写入、REVOKE 只影响后续新表，以及不支持对象类型/grant option 时 fail-closed。
- TRUNCATE 回归：验证 typed 多表 AST、`RESTRICT` 在 FK 依赖下不产生部分修改、递归 `CASCADE`、多表截断、`ONLY` 字段及 `RESTART IDENTITY`。
- ALTER TABLE typed 回归：`tests/alter_table_only_test.cpp` 验证 RLS enable/disable/force/no-force、ATTACH/DETACH PARTITION 的 AST 规格传递，以及 trigger enable/disable 的持久化状态。
- EXCLUDE typed 回归：`tests/exclude_test.cpp` 通过 DDL bridge 验证 `ALTER TABLE ... ADD/DROP CONSTRAINT ... EXCLUDE` 的约束名保留、冲突拒绝、删除后放行和持久化清理；CREATE TABLE 的等值与范围排斥回归继续覆盖 INSERT/UPDATE。
- ALTER TABLE typed 清理回归：`tests/ddl_bridge_routing_test.cpp` 验证基础动作在删除 `main.cpp` 重复分支后仍由 typed bridge 执行；`tests/ddl_ast_bridge_test.cpp` 进一步验证 CLUSTER、REPLICA IDENTITY、VALIDATE/ALTER CONSTRAINT 的 catalog、索引归属和持久化结果。
- 协议错误恢复回归：验证扩展查询错误后的 Bind/Execute 被忽略至 Sync、Sync 后连接可继续查询，以及事务外错误返回 `ReadyForQuery('I')`。
- 协议失败事务回归：验证显式事务中的语句错误返回 `ReadyForQuery('E')`/`25P02`、普通命令被拒绝、失败状态下 `COMMIT` 不提交先前 INSERT 而执行完整回滚，以及 `ROLLBACK TO SAVEPOINT` 可恢复外层事务。
- 协议两阶段命令边界回归：验证失败事务中的 `COMMIT PREPARED`/`ROLLBACK PREPARED` 不会被改写为本地恢复命令，未知 prepared transaction 返回错误。
- 协议 backend 隔离回归：双连接验证事务 ID/快照不串线，未提交行不可见，ROLLBACK 后状态清理，连接断开回滚未完成事务，COMMIT 后新事务可见；另覆盖 `START TRANSACTION` 选项与 `SAVEPOINT`/`ROLLBACK TO SAVEPOINT` 路由。
- SQL 统计模块回归：`tests/sql_stats_test.cpp` 验证字符串/数字常量归一化、引号标识符区分、调用次数与耗时聚合、数据库过滤、`pg_stat_statements.max` 确定性淘汰、当前格式快照重载、尾随数据损坏拒绝及 reset。
- 运行时统计模块回归：`tests/runtime_stats_test.cpp` 验证并发 SQL 计数、失败/提交/回滚计数、表扫描和实际 DML 行数统计及 reset。
- 协议运行时统计回归：`postgres_protocol_test.py` 通过真实 PostgreSQL wire 查询验证 `pg_stat_database`/`pg_stat_tables` 返回数据库查询、表 DML 及 Volcano 索引扫描/取行计数，而非固定零值。
- Planner 统计反馈回归：`tests/planner_runtime_stats_test.cpp` 验证完整扫描 live-row 估计参与 Hash/Nested Loop Join 选型和 EXPLAIN，未取得精确证据时回退物理行数，重建同名表后旧估计不会泄漏。
- DML AST 回归：`parser_phase1_test.cpp`、`dml_semantics_test.cpp` 与 `dml_returning_test.cpp` 验证 `INSERT` 多行 AST、混合 `DEFAULT` 位置、`DEFAULT VALUES` 区分、简单单表 INSERT SELECT（含 WHERE/列表达式）、无 target 或显式匹配主键/唯一约束 target 的 `ON CONFLICT DO NOTHING`、单列及复合主键/唯一约束冲突的常量或只引用 `excluded` 的 evaluator 标量 `DO UPDATE`、目标行/`excluded` 受限 `WHERE`、复合 UNIQUE 中 NULL 不互相冲突、不同唯一约束冲突不会被错误忽略、以当前目标行列值为输入的受限标量表达式 UPDATE、单源表 `UPDATE ... FROM`、单源表 `DELETE ... USING`、INNER/CROSS JOIN 的 UPDATE FROM/DELETE USING、窄版 MERGE、INSERT/UPDATE/DELETE 的受限标量表达式 RETURNING 以及 DML 尾随垃圾 fail-closed；协议 E2E 验证普通单表 INSERT 的多行/表达式/DEFAULT、显式 NULL 不被默认值覆盖、字符串大小写保留、常量及行级表达式 UPDATE、简单谓词 DELETE、INSERT/UPDATE/DELETE 的列投影和受限标量表达式 `RETURNING`、INSERT SELECT/冲突处理返回结果和 `INSERT 0 N` command tag。

---

## 测试汇总

| 功能域 | 测试项数 | 通过 | 失败 | 备注 |
|--------|----------|------|------|------|
| 认证与连接 | 2 | 2 | 0 | — |
| DDL - 数据库 | 3 | 3 | 0 | — |
| DDL - 表 | 14 | 14 | 0 | 含 CTAS / RENAME / POINT / INET/CIDR 类型 |
| DDL - 索引 | 7 | 7 | 0 | B+Tree/Hash/FullText/GIN/GiST/BRIN/SP-GiST 基础路径通过；专用访问方法仍是简化实现，不宣称 PostgreSQL 完整 opclass 兼容；B+Tree 分裂/重复键/范围回归见 `gin_brin_index_test.cpp` |
| DDL - 视图 | 5 | 5 | 0 | 含 ALTER VIEW RENAME TO / SET SCHEMA |
| DDL - 触发器 | 2 | 2 | 0 | — |
| DDL - 用户/角色 | 4 | 4 | 0 | — |
| DML - INSERT | 5 | 5 | 0 | 含 AST bridge 的多行/DEFAULT/显式 NULL 回归 |
| DML - UPDATE/DELETE | 2 | 2 | 0 | — |
| DQL - SELECT | 5 | 5 | 0 | — |
| DQL - JOIN | 3 | 3 | 0 | — |
| DQL - UNION | 1 | 1 | 0 | — |
| TCL - 事务 | 3 | 3 | 0 | — |
| DCL - 权限 | 4 | 4 | 0 | 含 SHOW USERS/SHOW ROLES |
| 工具命令 | 8 | 8 | 0 | — |
| 分区管理 | 3 | 3 | 0 | Range/List/Hash + ATTACH/DETACH |
| 高级特性 | 2 | 2 | 0 | NOTIFY/LISTEN, RLS |
| **合计** | **73** | **73** | **0** | 新增跨 backend 2PC prepared 事务回归 |

---

## 1. 认证与连接

### 1.1 登录（admin）

**输入**
```
admin admin
```

**预期输出**
```
successfully login
```

**实际结果** ✅ `successfully login`

---

### 1.2 退出

**输入**
```
exit
```

**预期输出**
```
Quitting...
```

**实际结果** ✅ `Quitting...`

---

## 2. DDL - 数据库管理

### 2.1 CREATE DATABASE

**输入**
```sql
CREATE DATABASE testdb;
```

**实际结果** ✅ `Create Database succeeded (charset=utf8)`

---

### 2.2 USE DATABASE

**输入**
```sql
USE DATABASE testdb;
```

**实际结果** ✅ `set Database to testdb`

---

### 2.3 DROP DATABASE

**输入**
```sql
DROP DATABASE testdb;
```

**实际结果** ✅ `Database dropped`

---

## 3. DDL - 表管理

### 3.1 CREATE TABLE（圆括号 + 冒号格式）

**输入**
```sql
CREATE TABLE t1 (id:int:0:1, name:varchar(50):0);
```

**实际结果** ✅ `Table create succeeded`
> 注：此前 `(col:type:flags)` 格式因 tokenize 不分割冒号导致零列解析，已修复。

---

### 3.2 CREATE TABLE（花括号格式）

**输入**
```sql
CREATE TABLE t2 {id:int pk, age:int:0};
```

**实际结果** ✅ `Table create succeeded`

---

### 3.3 INSERT + SELECT 验证

**输入**
```sql
INSERT INTO t1 (id, name) VALUES (1, 'alice');
INSERT INTO t1 (id, name) VALUES (2, 'bob');
SELECT * FROM t1;
```

**实际结果** ✅
```
1 row(s) inserted
1 row(s) inserted
id name
1 alice
2 bob
```

---

### 3.4 ALTER TABLE ADD COLUMN

**输入**
```sql
ALTER TABLE t1 ADD COLUMN email varchar(100);
```

**实际结果** ✅ `Column added`

---

### 3.5 ALTER TABLE DROP COLUMN

**输入**
```sql
ALTER TABLE t1 DROP COLUMN email;
```

**实际结果** ✅ `Column dropped`

---

### 3.6 DROP TABLE

**输入**
```sql
DROP TABLE t2;
```

**实际结果** ✅ `Table dropped`

---

### 3.7 ALTER TABLE RENAME COLUMN

**输入**
```sql
ALTER TABLE t1 RENAME COLUMN name TO username;
```

**实际结果** ✅ `Column renamed`

---

### 3.8 ALTER TABLE RENAME TO

**输入**
```sql
ALTER TABLE t1 RENAME TO t1_new;
```

**实际结果** ✅ `Table renamed`

---

### 3.9 CREATE TABLE AS SELECT (CTAS)

**输入**
```sql
CREATE TABLE t3 AS SELECT * FROM t1;
CREATE TABLE t4 AS SELECT id, name FROM t1 WHERE id = 1;
```

**实际结果** ✅ `Table created with N rows`，数据正确复制

---

### 3.10 CREATE TABLE with POINT type

**输入**
```sql
CREATE TABLE geo_points (id:int:0:1, loc:point:0);
```

**实际结果** ✅ `Table create succeeded`

---

### 3.11 INSERT POINT values

**输入**
```sql
INSERT INTO geo_points (id, loc) VALUES (1, '0.0,0.0');
INSERT INTO geo_points (id, loc) VALUES (2, '10.0,10.0');
INSERT INTO geo_points (id, loc) VALUES (3, '20.0,5.0');
INSERT INTO geo_points (id, loc) VALUES (4, '-5.0,15.0');
```

**实际结果** ✅ 全部 `1 row(s) inserted`

---

### 3.12 SELECT with spatial operators

**输入**
```sql
SELECT * FROM geo_points WHERE loc << '15.0,15.0';   -- left of
SELECT * FROM geo_points WHERE loc >> '15.0,15.0';   -- right of
SELECT * FROM geo_points WHERE loc <^ '15.0,15.0';   -- below
SELECT * FROM geo_points WHERE loc >^ '15.0,15.0';   -- above
SELECT * FROM geo_points WHERE loc <@ '10.0,10.0,5.0'; -- within circle
```

**实际结果** ✅
- `<<` 返回 x < 15 的点（id 1, 2, 4）
- `>>` 返回 x > 15 的点（id 3）
- `<^` 返回 y < 15 的点（id 1, 2, 3）
- `>^` 返回 y > 15 的点（无）
- `<@` 返回圆内点（id 2，中心 (10,10) 半径 5）

---

### 3.13 CREATE TABLE with INET / CIDR type

**输入**
```sql
CREATE TABLE networks (id INT, name VARCHAR(50), ip INET, net CIDR);
```

**实际结果** ✅ `Table create succeeded`

---

### 3.14 INSERT and query INET/CIDR with operators

**输入**
```sql
INSERT INTO networks (id, name, ip, net) VALUES (1, 'host1', '192.168.1.1', '192.168.1.0/24');
INSERT INTO networks (id, name, ip, net) VALUES (2, 'host2', '192.168.2.1', '192.168.2.0/24');
INSERT INTO networks (id, name, ip, net) VALUES (3, 'host3', '10.0.0.1', '10.0.0.0/16');
-- subnet-of: MATCH rows 1,2
SELECT * FROM networks WHERE net << '192.168.0.0/16';
-- contains: MATCH row 1
SELECT * FROM networks WHERE net >> '192.168.1.1';
-- overlap: MATCH rows 1,2
SELECT * FROM networks WHERE net && '192.168.0.0/16';
-- equality
SELECT * FROM networks WHERE ip = '192.168.1.1';
```

**实际结果** ✅ 所有操作符返回正确结果：`<<` 子网判断、`>>` 包含判断、`&&` 重叠判断、`=` 精确匹配

---

## 4. DDL - 索引管理

### 4.1 CREATE INDEX (B+Tree)

**输入**
```sql
CREATE INDEX idx_name ON t1(name);
```

**实际结果** ✅ `Index created`

边界回归同时验证了 250 条跨叶唯一键、6000 条跨叶及内部节点重复键的精确查找，以及范围扫描不重复返回。

---

### 4.2 CREATE INDEX USING HASH

**输入**
```sql
CREATE INDEX idx_hash ON t1 USING HASH (id);
```

**实际结果** ✅ `Index created`

---

### 4.3 CREATE FULLTEXT INDEX

**输入**
```sql
CREATE FULLTEXT INDEX idx_ft ON t1(name);
```

**实际结果** ✅ `Fulltext index created`

该命令是项目扩展语法，不属于 PostgreSQL `CREATE INDEX ... USING ...` 标准路径。

---

### 4.4 CREATE GIN INDEX

**输入**
```sql
CREATE INDEX idx_gin ON t1 USING GIN (name);
```

**实际结果** ✅ 创建真实 GIN 文件索引并登记 typed catalog 元数据。

---

### 4.5 CREATE GiST INDEX

**输入**
```sql
CREATE INDEX idx_gist ON t1 USING GiST (id);
```

**实际结果** ✅ 通过 typed DDL 创建 GiST 风格物理索引；当前实现仍是简化 range/spatial 结构，不宣称 PostgreSQL 通用 GiST opclass 兼容。

---

### 4.6 CREATE BRIN INDEX

**输入**
```sql
CREATE INDEX idx_brin ON t1 USING BRIN (id);
```

**实际结果** ✅ `Index created`

---

### 4.7 CREATE SPGIST INDEX

**输入**
```sql
CREATE TABLE locations (id INT PRIMARY KEY, pos CHAR(20));
INSERT INTO locations VALUES (1, '0.0,0.0');
INSERT INTO locations VALUES (2, '10.0,10.0');
INSERT INTO locations VALUES (3, '20.0,20.0');
INSERT INTO locations VALUES (4, '-5.0,5.0');
CREATE INDEX idx_spg ON locations USING SPGIST (pos);
SELECT * FROM locations;
```

**实际结果** ✅ 四叉树空间索引创建成功，查询返回 4 行坐标数据

---

### 4.8 DROP INDEX

**输入**
```sql
DROP INDEX idx_name;
DROP INDEX idx_hash;
DROP INDEX idx_name, idx_hash CASCADE;
DROP INDEX IF EXISTS missing_index;
DROP INDEX idx_name ON t1; -- migration-only legacy suffix
```

**实际结果** ✅ 标准 name-only、多名称、`IF EXISTS`、`CASCADE/RESTRICT` 以及 B-tree/Hash/GIN/GiST/BRIN/SP-GiST 物理索引均通过；物理索引、`pg_class` 和依赖元数据同步清理。

---

## 5. DDL - 视图

### 5.1 CREATE VIEW

**输入**
```sql
CREATE VIEW v1 AS SELECT id, name FROM t1 WHERE id > 0;
```

**实际结果** ✅ `View created`

---

### 5.2 SELECT FROM VIEW

**输入**
```sql
SELECT * FROM v1;
```

**实际结果** ✅ 返回视图数据

---

### 5.3 DROP VIEW

**输入**
```sql
DROP VIEW v1;
```

**实际结果** ✅ `View dropped`

---

### 5.4 ALTER VIEW RENAME TO

**输入**
```sql
CREATE VIEW v1 AS SELECT id, name FROM t1 WHERE id > 0;
ALTER VIEW v1 RENAME TO v2;
SELECT * FROM v2;
```

**实际结果** ✅ `View renamed`，v2 可正常查询并返回数据

---

### 5.5 ALTER VIEW SET SCHEMA

**输入**
```sql
ALTER VIEW v2 SET SCHEMA other_db;
USE DATABASE other_db;
SELECT * FROM v2;
```

**实际结果** ✅ `View schema changed`
> 注：视图定义中的基表名未带数据库限定，若目标数据库中不存在同名基表，查询会报错。此为已知限制。

---

## 6. DDL - 触发器

### 6.1 CREATE TRIGGER

**输入**
```sql
CREATE TRIGGER trg1 AFTER INSERT ON t1 PRINT inserted;
```

**实际结果** ✅ `Trigger created`

---

### 6.2 DROP TRIGGER

**输入**
```sql
DROP TRIGGER trg1;
```

**实际结果** ✅ `Trigger dropped`

---

## 7. DML

### 7.1 INSERT INTO（显式列名）

**输入**
```sql
INSERT INTO t1 (id, name) VALUES (3, 'charlie');
```

**实际结果** ✅ `1 row(s) inserted`

---

### 7.2 INSERT INTO（省略列名）

**输入**
```sql
INSERT INTO t1 VALUES (4, 'dave', 28);
```

**实际结果** ✅ `1 row(s) inserted`

---

### 7.3 UPDATE

**输入**
```sql
UPDATE t1 SET name = 'alice2' WHERE id = 1;
```

**实际结果** ✅ `1 row(s) updated`

---

### 7.4 DELETE

**输入**
```sql
DELETE FROM t1 WHERE id = 3;
```

**实际结果** ✅ `1 row(s) deleted`

---

## 8. DQL

### 8.1 基本 SELECT

**输入**
```sql
SELECT * FROM t1;
SELECT id, name FROM t1 WHERE id = 1;
SELECT COUNT(*) FROM t1;
```

**实际结果** ✅ 均返回正确结果

---

### 8.2 JOIN

**输入**
```sql
CREATE TABLE t2 {fid:int pk, info:varchar(20)};
INSERT INTO t2 (fid, info) VALUES (1, 'info1');
SELECT * FROM t1 JOIN t2 ON t1.id = t2.fid;
```

**实际结果** ✅ 返回 JOIN 结果

---

### 8.3 UNION

**输入**
```sql
SELECT id FROM t1 UNION SELECT fid FROM t2;
```

**实际结果** ✅ 返回 UNION 结果

`tests/set_operation_volcano_test.cpp` 进一步验证了 Volcano `SetOperationOp` 的
UNION/INTERSECT/EXCEPT 及 ALL 多重集结果，并覆盖投影后的 DISTINCT 去重；协议 E2E
另验证了聚合 legacy producer 经 `MaterializedRowsOp` 进入同一集合节点。

---

### 8.4 GROUP BY + HAVING

**输入**
```sql
SELECT name, COUNT(*) FROM t1 GROUP BY name HAVING COUNT(*) > 0;
```

**实际结果** ✅ 返回分组结果

---

## 9. TCL - 事务控制

### 9.1 BEGIN / COMMIT

**输入**
```sql
BEGIN;
INSERT INTO t1 (id, name) VALUES (4, 'dave');
COMMIT;
```

**实际结果** ✅ `Transaction committed`

---

### 9.2 BEGIN / ROLLBACK

**输入**
```sql
BEGIN;
INSERT INTO t1 (id, name) VALUES (5, 'eve');
ROLLBACK;
SELECT * FROM t1 WHERE id = 5;
```

**实际结果** ✅ `Transaction rolled back`，后续 SELECT 无 id=5 行

---

### 9.3 SAVEPOINT

**输入**
```sql
BEGIN;
INSERT INTO t1 (id, name) VALUES (6, 'frank');
SAVEPOINT sp1;
INSERT INTO t1 (id, name) VALUES (7, 'grace');
ROLLBACK TO SAVEPOINT sp1;
COMMIT;
```

**实际结果** ✅ id=6 保留，id=7 回滚

协议回归还验证 `BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY`、`START TRANSACTION READ COMMITTED READ WRITE`，以及无固定字符串偏移依赖的保存点创建和 `ROLLBACK TO SAVEPOINT`。

---

## 10. DCL - 权限控制

### 10.1 CREATE USER + GRANT

**输入**
```sql
CREATE USER testuser WITH PASSWORD 'testpass' NOSUPERUSER;
GRANT SELECT ON t1 TO testuser;
```

**实际结果** ✅ `User created`, `GRANT succeeded`

---

### 10.2 REVOKE

**输入**
```sql
REVOKE SELECT ON t1 FROM testuser;
```

**实际结果** ✅ `REVOKE succeeded`

---

### 10.3 SHOW USERS

**输入**
```sql
SHOW USERS;
```

**实际结果** ✅ 列出所有用户及其权限级别（admin 1，普通用户 0 等）

---

### 10.4 SHOW ROLES

**输入**
```sql
CREATE ROLE auditor;
SHOW ROLES;
GRANT auditor TO testuser;
SHOW ROLES;
```

**实际结果** ✅ 列出所有角色名称，仅显示角色不显示已授予用户

---

## 11. 工具与诊断

### 11.1 SHOW TABLES

**输入**
```sql
SHOW TABLES;
```

**实际结果** ✅ 列出当前数据库所有表

---

### 11.2 SHOW COLUMNS

**输入**
```sql
SHOW COLUMNS FROM t1;
```

**实际结果** ✅ 显示列定义

---

### 11.3 SHOW INDEXES

**输入**
```sql
SHOW INDEXES FROM t1;
```

**实际结果** ✅ 显示索引列表

---

### 11.4 DESC

**输入**
```sql
DESC t1;
```

**实际结果** ✅ 显示表结构

---

### 11.5 EXPLAIN

**输入**
```sql
EXPLAIN SELECT * FROM t1 WHERE id = 1;
```

**实际结果** ✅ 返回执行计划

---

### 11.6 ANALYZE TABLE

**输入**
```sql
ANALYZE TABLE t1;
```

**实际结果** ✅ `ANALYZE completed`

---

### 11.7 VACUUM

**输入**
```sql
VACUUM t1;
```

**实际结果** ✅ `VACUUM completed, 0 pages freed`

---

### 11.8 CHECKPOINT

**输入**
```sql
CHECKPOINT;
```

**实际结果** ✅ `CHECKPOINT completed`（checkpoint WAL、LSN 文件和关系页刷盘均成功）

---

## 12. 分区管理

### 12.1 CREATE TABLE ... PARTITION BY RANGE

**输入**
```sql
CREATE TABLE sales (id:int:0:1, amount:int:0) PARTITION BY RANGE(id) (
    PARTITION p_low VALUES LESS THAN (100),
    PARTITION p_high VALUES LESS THAN (200)
);
```

**实际结果** ✅ `Table create succeeded`

---

### 12.2 ATTACH PARTITION

**输入**
```sql
CREATE TABLE p_mid {id:int pk, amount:int:0};
ALTER TABLE sales ATTACH PARTITION p_mid FOR VALUES FROM (100) TO (150);
```

**实际结果** ✅ `Partition attached`

---

### 12.3 DETACH PARTITION

**输入**
```sql
ALTER TABLE sales DETACH PARTITION p_mid;
```

**实际结果** ✅ `Partition detached`

---

## 13. 高级特性

### 13.1 NOTIFY / LISTEN

**输入**
```sql
LISTEN mychannel;
NOTIFY mychannel, 'hello';
```

**实际结果** ✅ `LISTEN mychannel`, `NOTIFY mychannel`

---

### 13.2 行级安全 (RLS)

**输入**
```sql
CREATE TABLE users (id:int:0:1, name:varchar(50):0, role:varchar(20):0);
INSERT INTO users (id, name, role) VALUES (1, 'alice', 'admin');
INSERT INTO users (id, name, role) VALUES (2, 'bob', 'user');
CREATE POLICY p1 ON users FOR SELECT USING (role = 'admin');
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
```

**实际结果** ✅ `Policy created`, `RLS enabled`

运行时回归还验证了：`USING (owner = current_user)` 只返回当前用户行；显式 `TO public` 按 PUBLIC 生效；多个默认 permissive 策略按 OR 组合，`AS RESTRICTIVE` 按 AND 组合；`FOR ALL` 省略 `WITH CHECK` 时继承 `USING`，阻止越权写入；`NOINHERIT` 用户不匹配父角色策略；表 owner、`pg_authid.rolsuper`/`rolbypassrls` 绕过普通 RLS，`FORCE ROW LEVEL SECURITY` 重新执行策略；owner 同步持久化到正式 schema 和 `pg_class.relowner`，`ALTER TABLE ... OWNER TO` 会立即改变 RLS 行为。普通查询、删除以及 `UPDATE ... FROM` 的来源扫描均不再暴露其他用户行；启用 RLS 但没有适用策略时默认返回零行。策略解析/求值失败会安全拒绝该扫描。

---

## 14. 已知问题与限制

### 14.0 TABLESPACE 物理路由

`CREATE TABLE ... TABLESPACE`、`ALTER TABLE ... SET TABLESPACE` 已验证真实关系文件迁移：
heap、FSM/VM、索引、分区和 TOAST 文件位于 `<LOCATION>/<DATABASE>/`，目标目录不存在或
表空间 marker 缺失时拒绝操作，不会回退到默认目录。测试还使用全新 `StorageEngine`
重启读取迁移后的行，覆盖了此前“重启打开空文件”的数据损失风险。

剩余限制：表空间权限/owner、`ALTER TABLESPACE` 完整语义和 PostgreSQL OID/符号链接布局。

### 14.1 ALTER TABLE SET SCHEMA

**当前行为**：命令已由 AST bridge 转发到现有存储实现；当前实现的目标参数仍按数据库级迁移处理，不等价于 PostgreSQL 的 namespace-only 语义。

**级别**：部分实现（语义与 catalog namespace 迁移仍待补齐）

---

## 15. 结论

本次测试覆盖的历史手册场景、137 个独立 C++ 回归测试、PostgreSQL 协议 E2E 和窗口函数 E2E 均通过；分组 Volcano 单元与主 SQL 手工回归也通过。系统在以下方面表现稳定：

- ✅ 基本 CRUD（CREATE/INSERT/SELECT/UPDATE/DELETE/DROP）
- ✅ **POINT 数据类型**与空间运算符（`<<` / `>>` / `<^` / `>^` / `<@`）
- ✅ **INET/CIDR 网络类型**与网络运算符（`<<` / `>>` / `&&`）
- ✅ 索引系统基础路径（B+Tree/Hash/FullText/GIN/BRIN/SP-GiST）；GiST 仍列为缺失
- ✅ 视图与触发器（含 ALTER VIEW RENAME TO / SET SCHEMA，以及简单单表视图多行 `INSTEAD OF` INSERT/UPDATE/DELETE 协议回归）
- ✅ 事务控制（BEGIN/COMMIT/ROLLBACK/SAVEPOINT）
- ✅ 权限管理（GRANT/REVOKE）
- ✅ 分区管理（Range/List/Hash + ATTACH/DETACH）
- ✅ 查询能力（JOIN/UNION/INTERSECT/EXCEPT 含 ALL 语义、基础 GROUP BY/窗口函数/CTE/LATERAL、未关联单列 IN/NOT IN、未关联单表 EXISTS/NOT EXISTS、单列未关联 ANY/ALL、单个未关联标量目标）
- ✅ Volcano 并行 heap scan（page-range workers、确定性 Gather、串行结果等价性）
- ✅ 工具命令（SHOW/EXPLAIN/ANALYZE/VACUUM/CHECKPOINT）
- ✅ INSERT 省略列名 / ALTER RENAME / CREATE TABLE AS SELECT

**未发现阻塞性 bug**。

---

*报告生成时间：2026-08-14*
*测试执行人：自动化测试脚本 + 人工验证*
- Parser 边界回归：`parser_phase1_test` 新增非法 `LIMIT/OFFSET/FETCH`、负数、缺失计数以及省略 FETCH count 的测试；非法计数 fail-closed，省略 count 默认 1。
- Parser 数值选项回归：同一测试新增非法函数 `COST/ROWS`、角色 `CONNECTION LIMIT` 和列统计目标测试，确认解析失败不会产生默认 AST。
- Sequence DDL 边界回归：`sequence_full_test` 新增非法 `CREATE SEQUENCE START`、`ALTER SEQUENCE INCREMENT/CACHE` 测试，确认不会创建或改变序列，合法序列、缓存、重命名、依赖和重启行为保持通过。
- DDL 清理回归：`ddl_bridge_routing_test` 验证 `ALTER ROLE/ALTER USER` 继续由 typed bridge 执行；删除 main.cpp 中不可达的重复角色执行器后路径保持一致。

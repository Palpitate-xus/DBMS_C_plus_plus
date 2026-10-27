# R290：runtime 统计快照加载保留旧表及失败后发布混合状态

状态：现有 C++ 快照加载 API 的有限修复；MON-03 和原 273 条总清单仍未整体完成。

| 缺陷 | 可观察影响 | 修改方法 |
|---|---|---|
| load 只覆盖快照中出现的表 | 旧磁盘表及仅内存表继续出现在统计中；旧有效行数仍能影响 planner；下一次持久化又把旧表写回 | 成功加载时替换目标数据库的全部 live/baseline 表记录，不影响其他数据库 |
| 只清除快照内表名的 reset 标记 | 旧的“已删除但本次快照未包含”标记继续存在，后续 peer 新增同名表的统计可能被丢弃 | 完整加载同时清除目标数据库的旧 reset 标记，保留其他数据库的标记 |
| 先修改 baseline/live，再逐项分配复制 | 实际 bad_alloc 可能留下新数据库计数、旧表计数和部分新表组成的混合状态；重试/持久化可再次错算 | 所有分配、键和值复制成功后，在原互斥区删除目标旧状态并转移已准备节点 |
| exists(path, ec) 返回 false 时总返回 true | 实际 ELOOP、ENAMETOOLONG 等状态错误被当作“没有快照”，上层可能继续启动 | 保留真正缺失文件的成功/no-op 语义，但有 error_code 时返回 false |

只修改 `src/process/RuntimeStats.cpp` 的 `loadRuntimeStats()`；没有修改持久化函数、
统计合并公式、文件格式、头文件、类布局、Main、DDL 或全局 StorageEngine 缓存。
加载完整文件并校验既有格式后，在原 file-lock → runtime-mutex 顺序内先取得全部状态容器，
准备两个单数据库 map 和两套目标表节点，再 erase/merge。固定 std::allocator 相等，
键为标准 string/pair，节点转移与比较不再分配；异常前仍是完整旧状态，成功后为完整新状态。
不存在的文件仍不清空当前统计；有效的零表快照则清空目标旧表。这两种情况明确区分。

## 固定夹具与真实失败对照

`tests/runtime_stats_snapshot_load_test.cpp` 从独立模型编码项目当前格式，逐字段核对
全部数据库计数、表计数、有效行数、其他数据库及后续真实持久化字节，不只检查 load 返回值。

同一最终夹具两侧旧版各完整 **6292 控制、2036 失败、exit 1**；修复后两侧各完整
**6292 控制、0 失败、exit 0**。2036 是重复模式下的失败控制数，不是独立 bug 数。
没有删失败案例、增加 180 秒 deadline、关闭同步持久化或放宽计数/完整字节断言。

四个健康快照轮廓包括重叠、零表、完全不同表集及未知/零行估计；检查重复加载幂等、
没有旧 planner 估计、源文件不被加载重写、peer-only 表及清空内存后精确重读。
五个错误内容（空、短头、末尾截断、尾随字节、错误数据库）、真实状态错误、锁文件为目录、
快照为目录、真正缺失文件及空数据库参数均检查状态、描述符和后续 checkpoint。
reset 标记与 baseline 的私有状态通过下一次真实 persist 的完整字节和外部新表行为验证，
没有测试专用生产接口，也没有把内存 reset 冒充实际进程/系统崩溃重启。

分配矩阵预先固定 **512 个位置**，只在调用 load 的当前线程拦截真实 C++ new/new[]，
含 aligned/nothrow 形式；该矩阵先准备既有统计状态，不冒称已单独注入冷进程首次 singleton 初始化。
旧版实际 56 个分配点、新版实际 68 个，分别逐点确实触发一次；
其余位置仍作为健康控制保留，不算作注入成功。每个位置检查结果、完整全局状态、源字节、
FD、目标和其他数据库的真实 checkpoint、健康重试。不是所有 malloc、内核 I/O 或任意 OOM 证明。

## MON-03 原范围与继续计划

原要求仍为：统计子系统补 shared/persistent counters、snapshot semantics、reset、
track_* GUC、function/SLRU/WAL/checkpointer/bgwriter/I/O 统计。下表不重新定义该范围。
[PostgreSQL 18 累计统计文档](https://www.postgresql.org/docs/18/monitoring-stats.html)
区分共享累计值、正常/非正常重启和事务内统计缓存；本项目的文件加载修复不等于这些语义均已实现。

| 子要求 | 当前可用证据 / 后续验收方法 | 状态 |
|---|---|---|
| 多后端 shared counters / flush 时机 | 现有进程内 map 与磁盘 delta 合并不能代替共享内存、跨后端并发观察和精确汇总 | 未充分证明 |
| persistent counters、正常与非正常重启 | R289 文件锁及本项加载可支持部分现有路径；仍需真实进程 clean/crash/recovery 对照 | 未整体完成 |
| 完整文件加载与失败不发布半状态 | 本项 6292 控制与后续 checkpoint/重试证明上述边界 | 本项有限完成 |
| none/cache/snapshot、事务缓存及 clear_snapshot | 下述真实接口观察只能证明各次响应；需跨事务、跨连接并发语义和类型验证 | 未完成 |
| database/table/function/shared reset | 本项只处理加载时旧标记，不等于实现全部 reset 对象、并发时序及持久化 | 未完成 |
| track_* GUC 实际作用 | 需逐 GUC 检查 SHOW/SET、更新是否真的受控及重连/事务行为，不以接受 SET 为实现证明 | 未完成 |
| function/SLRU/WAL/checkpointer/bgwriter/I/O | 需逐视图/函数对齐 PG18 列名、OID、行身份和真实工作负载增量 | 未完成 |
| persist 的故障与重试 | 文件发布后仍有分配式内存更新，需另行复现；本项未改 persist，不把该源码疑点算作已修复 | 待独立验证 |

## 23 条实际接口观察（非通过矩阵）

使用独立新 PostgreSQL 18.6 实例及两侧当前真实服务器，各按相同顺序发送 23 条 Simple SQL，
原始 rows、SQLSTATE、列名、OID 和 tag 均保存。观察脚本 exit 0 仅表示采集完成，
不是产品与参考相等或这些统计特性已经通过。每侧的数据库名和自然计数不同，未伪造数值相等。
产品每侧有 19 次非零 SQLSTATE；另外 4 次未报错也不能算作正确支持：

| 另行记录的问题 | 实际响应与 PG18 对照 | 后续修改方向（未实施） |
|---|---|---|
| 三次 SHOW stats_fetch_consistency 返回错误形状 | 产品均 0 行、四列 `table: / _fetch_consistency / rows: / 0`，全 TEXT；PG 是一列 TEXT、一行，依次 cache/snapshot/none | 检查完整 GUC 目标的分派，避免前缀命令混淆；根因仍需独立复现，不在本项恢复 Main 暂缓草稿 |
| pg_stat_database 忽略三列投影且类型全错 | 产品返回七列且 OID 均 25；PG 的这三列为 datname/name、xact_commit/int8、xact_rollback/int8，即 19/20/20 | 由 typed 虚拟关系参与绑定、投影及排序，不靠显示文本猜字段；须独立划清与已暂缓目录身份模块的边界 |

“无错误”仅指 SQLSTATE 为空，不是特性通过。原始元数据和响应均保留，未删除这些观察。

| SQL（发送顺序） | PG18.6 SQLSTATE | Root SQLSTATE | 纯提交 SQLSTATE |
|---|---|---|---|
| `SHOW track_counts;` | 无错误 | XX000 | XX000 |
| `SHOW track_activities;` | 无错误 | XX000 | XX000 |
| `SHOW track_functions;` | 无错误 | XX000 | XX000 |
| `SHOW track_io_timing;` | 无错误 | XX000 | XX000 |
| `SHOW track_wal_io_timing;` | 无错误 | XX000 | XX000 |
| `SHOW stats_fetch_consistency;` | 无错误 | 无错误 | 无错误 |
| `SELECT pg_stat_clear_snapshot();` | 无错误 | 42883 | 42883 |
| `SELECT pg_stat_get_snapshot_timestamp();` | 无错误 | 42883 | 42883 |
| `SELECT datname,xact_commit,xact_rollback FROM pg_stat_database ORDER BY datname;` | 无错误 | 无错误 | 无错误 |
| `SELECT * FROM pg_stat_slru;` | 无错误 | 42P01 | 42P01 |
| `SELECT * FROM pg_stat_wal;` | 无错误 | 42P01 | 42P01 |
| `SELECT * FROM pg_stat_checkpointer;` | 无错误 | 42P01 | 42P01 |
| `SELECT * FROM pg_stat_bgwriter;` | 无错误 | 42P01 | 42P01 |
| `SELECT * FROM pg_stat_io;` | 无错误 | 42P01 | 42P01 |
| `SELECT * FROM pg_stat_user_functions;` | 无错误 | 42P01 | 42P01 |
| `SELECT * FROM pg_stat_xact_user_functions;` | 无错误 | 42P01 | 42P01 |
| `SELECT * FROM pg_stat_xact_user_tables;` | 无错误 | 42P01 | 42P01 |
| `SET stats_fetch_consistency='snapshot';` | 无错误 | XX000 | XX000 |
| `SHOW stats_fetch_consistency;` | 无错误 | 无错误 | 无错误 |
| `SELECT pg_stat_get_snapshot_timestamp();` | 无错误 | 42883 | 42883 |
| `SELECT pg_stat_clear_snapshot();` | 无错误 | 42883 | 42883 |
| `SET stats_fetch_consistency='none';` | 无错误 | XX000 | XX000 |
| `SHOW stats_fetch_consistency;` | 无错误 | 无错误 | 无错误 |

## 构建、回归与版本边界

每侧只 fresh 编译一份 RuntimeStats.cpp；其余 120 生产对象、两份 stub、134/131 个项目头、
MMD 依赖和编译器/对象哈希均核对后复用，保留 R283 私有布局，不冒称全冷编译。
新夹具两份及 8 个统计邻接 TU fresh；既有 98 个 test/Main 对象匹配复用，包括六个真实 Main。
108 个 test/Main 程序及两个服务器全部重新链接当前库。
完整有限正常矩阵 146 条（102 native、6 Main、38 wire）实际全 0；另两侧分别新编译
RuntimeStats.cpp 与专项夹具执行 scoped UBSan，各完整 6292 控制全 0，共 148 条当前运行记录全 0。
没有整库 ASan/LSan/TSan、全部注册测试或 PG 完整统计语义的通过声明。

提交仅含本 CPP、专项原生测试及本文；2311 个已接受源码与测试投影逐文件相同。
981 native + 598 注册 Python + 3 Main = 1582 仅为库存。五项静态门两侧提交前后实际 0，
require-complete 两侧仍实际 1；原 1492 冻结矩阵和原 273 条状态不重写。
R288/R99 用户暂缓、R287 dependency held、R281 held 及其他所有暂缓项保留；
没有恢复 R51 全局缓存/DROP/后台清理、R222 或其他候选，没有 push，没有启用 Actions。

证据目录：`/tmp/dbms-r290-stats-load.54q50Rld`。临时目录不保证长期保存。

| 证据 | SHA-256 |
|---|---|
| R289匹配基线及七源隔离 | `458481f95455ca0a7e087437476557a2fd71268bb45a3357de9b116c268fd387` |
| 最终同一6292项旧两侧完整失败 | `3195e371a623c2cea842382311402866d6a2f2a9d58b45546c5c16019117116f` |
| 完整替换/预分配节点增量与工作区保留 | `cbdcb722ff0e86c3c8dc32c95810e39aaf138a0edebbed13336f8559aefc4e7e` |
| 两个生产及两个新原生TU构建 | `c8b4b6f2251a33605110d8f1093f06f2364ccc61690bb089765a914f9b874224` |
| 108新链接程序及8个邻接新编译TU | `82a47c480102dd2b8d80b08999ad7562ab8c9c4085b83f9dcab9cb98af365e42` |
| 最终同一6292项新两侧通过 | `2d17f730339f879b40bd7f8199a1820b264cceed4e51417e53c27301e06ea9f2` |
| 146条当前有限运行 | `544a8a251776f8013ec35db842d27213350b91928b22fd583db2e37f14af592d` |
| 两个scoped UBSan完整6292项 | `bd6a9ababf80f219afe25ef42353ea82a2e01776137a3278b057c973af3745d0` |
| MON03三侧各23条接口观察，仅采集 | `0c61179bdbb3a5f5c3c027267e20c520e117f472b32b48226aa07f431bd9e30d` |
| 源码/头/MMD/对象/程序与工作区复核 | `c4c234fdd142217933ce0a0461b5a3d883b90945fa5a222412a16b0b05181be7` |

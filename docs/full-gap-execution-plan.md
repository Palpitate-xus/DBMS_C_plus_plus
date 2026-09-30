# 总差距清单执行计划

当前范围是 `postgresql-18-gap-audit.md` 全部 273 项，不再用局部收尾计划替代总任务。`review-closeout-plan.md` 仅记录上批第 191–196 项修复；它的完成不表示总清单完成。安全 / TDE 专项沿用用户先前的跳过要求，单独计为 deferred，不计完成。不 push，不启用 GitHub Actions。

| 批次 | 工作 | 验收与状态 |
| --- | --- | --- |
| B0 | 273 项与代码、测试、提交建立映射，纠正文档完成口径 | 已建立 `gap-progress.json`；未逐项核实的保留 unverified，局部修复仅为 partial |
| B1 | SQL 结果正确性与类型 / NULL 保真；推进 P0-01/02、QRY-01/04/07/10、FUNC-02 | 进行中；先复现具体错误，再修复并增加 C++ / 完整 SQL 回归 |
| B2 | catalog / 事务化 DDL / 依赖与回滚 | 未完成；新增 UNLOGGED 表 DROP 回滚数据保真回归，并修复 13 个表级 DDL／维护入口及枚举更新的缓存锁与表锁顺序、自动及显式事务的数据库级 DDL 等待忽略 `lock_timeout`、活动事务与空闲连接期间误删数据库、连接占用期间重命名数据库、选项 sidecar 原地截断及数据库名与快照／归档路径碰撞；同进程 CREATE/DROP/RENAME 名字锁及选项 read-modify-write 已串行化，跨进程、多个嵌入式存储引擎实例及双目录移动的崩溃原子性仍待处理，继续按蓝图拆成可复现的原子子任务 |
| B3 | 存储 / WAL / MVCC / 索引 / 资源治理 | 未完成；已补单目录跨进程互斥、一处排序平方复杂度，以及归档段不覆盖发布／幂等重试。第 822 项让查询消费 B-tree 的明确读取失败状态；第 823 项拆开运行时已有树加载与 CREATE／DDL 重写／恢复的显式建索引，缺失文件不再补为空树。P0-06 已核实仍为整文件镜像与恢复重建；下一步核实 Hash/Bloom 相邻加载与原始索引损坏时的 rollback incomplete，再推进 page/logical WAL、故障注入、多 backend 隔离和持久化验证 |
| B4 | 其余类型 / 函数 / 查询 / 优化器 / 监控 / 工具兼容 | 未完成；不能将语法支持当作运行时完成 |
| B5 | 备份 / 复制 / 恢复 / 扩展生态 / 升级 | 未完成；需要端到端验收，外部环境或破坏性迁移另行说明 |

每个具体问题独立修复、测试、commit，并在 `code-review-progress.md` 记录关联的总清单 ID。完成某个案例不会勾掉整个功能族。只有整个条目满足蓝图的验收要求，才可同时更新原 Markdown 复选框和 JSON 状态为 complete，并附测试、证据、提交。

使用 `python3 scripts/check_gap_progress.py` 检查 273 项是否遗漏、重复、证据失效或完成状态矛盾。`--require-complete` 在任意条目未完成时返回非零，包括用户跳过项；不能把跳过或部分实现包装为全部完成。

## 后续优先队列

2026-09-30 第 851 项进展：EXPLAIN 的单 INNER 等值 JOIN 真实计划／执行已独立提交，新增重链／正式协议、原 CLI 加强后的 8/8 断言、6 个 C++、8 组相邻协议、重链／正式完整协议及正式构建均退出码 0；bag、NULL、quoted／schema／alias、self join、空表与绑定错误已覆盖。旧 c1409254 全脚本唯一 JOIN 失败记录保留，真实 374 差分通过不替代整套。第 852 项 COUNT(DISTINCT) 类型等价已定向通过新 C++／协议和真实专项差分，尚待相邻验收记录；第 853 项 GROUP BY 类型／NULL／复合键问题已复现，继续独立修复。总账仍 273 项：24 complete、136 partial、98 unverified、15 deferred_by_user，未 push，Actions 禁用。

2026-09-30 第 850 项进展：NUMERIC 索引／约束尺度等价已独立提交，最终 10 个 C++、新重链／正式协议、8 组相邻协议、完整协议、正式构建与专项真实 PostgreSQL 18.6 差分 cases=1 failed=0 均退出码 0；旧差分 failed=1 保留。独立实测 B-tree 超过 20 字节键被截断而误报重复、负零显示尺度差异、SQL COUNT(DISTINCT)／GROUP BY 等价仍未修，旧索引／重复 heap 不擅自迁移或删除。根冻结 c1409254 正式全脚本已结束退出码 1：378/378 C++、95/96 E2E，唯一 EXPLAIN ANALYZE JOIN 报 0A000；继承清理已通过，原失败记录保留。同一二进制真实 PostgreSQL 18.6 全差分 374/374、failed=0、退出码 0，不含隔离第 847–850 项。接着修复真实 JOIN 计划／执行，不削弱原断言；总账仍 273 项：24 complete、136 partial、98 unverified、15 deferred_by_user，未 push，Actions 禁用。

2026-09-30 第 849 项进展：ANALYZE distinct／MCV 的 SQL 列类型等价和 equality MCV 匹配已独立提交，最终 8 个 C++、新重链／正式协议、8 组相邻协议、完整协议、正式构建均退出码 0；numeric 0.50／0.500 合为 cardinality=2、hot count=35，三个等价数值字面量与浮点尾零的 Filter 估算均为 35。参考 PostgreSQL 18.6 TEMP 表 count distinct=2、matching=35、MCV 频率 0.875，仅数据／频率校准。真实 SQL COUNT(DISTINCT) 和 GROUP BY 却仍将两种显示尺度分开，另实测 NUMERIC PRIMARY KEY 接受相等值；这些已列未修，先修数值键，再修聚合和分组，不用统计通过关闭 TYPE／IDX／QRY 族。根冻结 c1409254 的正式 378 C++／96 E2E 与同一二进制真实 374 差分仍运行，不含第 847–849 项；仍 273 项：24 complete、136 partial、98 unverified、15 deferred_by_user，未 push，Actions 禁用。

2026-09-30 第 848 项进展：pg_stats 直方图 final upper bound 已独立提交，旧 C++／协议末端 19.25 失败，新目标／正式专项、5 个 C++、6 组相邻协议、完整协议和正式构建均退出码 0，20.25 上界与空 histogram 分支通过。参考 PostgreSQL 18.6 自销毁 TEMP 表边界为 1.25／20.25，只是端点校准，不宣称项目 10 桶／8 列 view 完全兼容 PG。第 849 项类型 MCV／distinct 已通过新定向及相邻验收，正式专项待最终确认；另实测 numeric PRIMARY KEY 接受相等的 0.50／0.500，以及 SQL COUNT(DISTINCT)／GROUP BY 分开相等值，继续独立修复。根冻结 HEAD c1409254 正式全脚本与 374 组真实差分仍运行（本次 alter_inherit_test 已通过，但不替代整套退出码），不含第 847–849 项；总账仍 273 项：24 complete、136 partial、98 unverified、15 deferred_by_user，未 push，Actions 禁用。

2026-09-30 第 847 项进展：普通 SELECT 的 quoted comma FROM 改写已独立修复，旧正式二进制协议失败且真实差分 cases=1 failed=1，新重链／正式专项、8 组相邻协议、完整协议、正式构建和新真实 PostgreSQL 18.6 差分 cases=1 failed=0 均退出码 0。根冻结第 845/846 项 HEAD c1409254 的正式全脚本（预计 378 C++／96 E2E）与同一已正式构建二进制的真实差分（374 cases）仍运行，不含本项新增第 375 组。第 848 项 catalog histogram 丢 final upper bound 已复现并隔离修复，尚待正式／相邻验收；pg_stats 投影／WHERE、SQL-type MCV／distinct、quoted 空白 alias、阈值事务时机及其余总清单继续未完成。仍 273 项：24 complete、136 partial、98 unverified、15 deferred_by_user，未 push，Actions 禁用。

2026-09-30 第 845 项进展：类型化 ANALYZE extrema／histogram 已独立提交，最终 8 个 C++、新重链／正式专项、10 组相邻协议、完整协议、正式构建和 5 组逐一真实 PostgreSQL 18.6 差分均退出码 0；1.25…20.25 不再 lex max=9.25／边界截整数，超 2^53 BIGINT 与 30 位整数部分 numeric 保留原值。继承 fixture 12 次完整复验亦通过，原第 842 项全脚本失败保留。准备主分支冻结最新合并后重跑预计 378 C++／96 E2E 与 374 组真实差分；不包含仍隔离的第 847 项 quoted comma FROM。SQL-type distinct／MCV、虚拟列／TOAST、pg_stats 投影／WHERE／边界字段、统计编码／失效／采样／correlation 和其他总清单仍未完成；总账仍 273 项：24 complete、136 partial、98 unverified、15 deferred_by_user，未 push，Actions 禁用。

2026-09-30 第 846 项进展：继承测试清理竞态已独立提交，先调用 dropDatabase 关闭 catalog／allocator／extent worker，再清理残留，保留全部原继承断言与清理错误断言；第 844 项正式生产对象重链的新完整用例在 12 个独立目录连续退出码 0。第 842 项冻结脚本原失败（375/376 C++、93/93 E2E、退出码 1）保留，真实差分 374/374 通过也不冒充整套通过。第 845 项类型统计最终 8 个 C++、新重链／正式协议、10 组相邻协议、完整协议、正式构建及 5 组逐一真实 PostgreSQL 18.6 差分均已退出码 0，详细证据另记；第 847 项 quoted comma FROM 修复仍隔离进行，不包含在上述轮次。准备快进最新主分支再全量验收；总清单仍 273 项：24 complete、136 partial、98 unverified、15 deferred_by_user，未 push，Actions 禁用。

2026-09-30 第 844 项进展：ANALYZE 的真实 NULL／空串统计已独立提交，8 个 C++、重链／正式专项协议、8 组相邻协议、完整协议及正式构建均退出码 0；真实 PostgreSQL 18.6 临时表核对 NULL fraction 和 MCV 频率一致，但非全差分验收。根冻结第 842 项 HEAD 88358d80 全脚本已退出码 1：375/376 C++、93/93 E2E；唯一失败是 alter_inherit_test 在活跃存储仍写 extent 临时文件时直接 remove_all 清理，已有具体异常路径，待独立修复，不抹去本轮失败。该轮同一正式二进制真实 PostgreSQL 18.6 差分 374/374、failed=0、退出码 0，不含第 843–844 项。继续浮点／精确 numeric 统计边界、pg_stats 投影／WHERE、阈值事务时机及其余总清单；仍 273 项：24 complete、136 partial、98 unverified、15 deferred_by_user，未 push，Actions 禁用。

2026-09-30 第 843 项进展：EXPLAIN 关系绑定已独立修复，最终重链／正式专项、12 组相邻协议、完整协议与最终正式构建均退出码 0；quoted comma／关键字／schema／search_path／点号空白、WHERE/GROUP BY 输入边界及错误后连接复用通过。普通 FROM quoted comma、统计 NULL／浮点、开启阈值自动分析的事务时机及其他总清单仍未完成。根冻结第 842 项 HEAD 88358d80 的完整正式脚本与同一二进制 PostgreSQL 18.6 差分仍运行，不含本项；总账仍 273 项：24 complete、136 partial、98 unverified、15 deferred_by_user，未 push，Actions 禁用。

2026-09-30 第 842 项进展：typed／legacy DML 十三处强制 ANALYZE 旁路已独立修复，8 个 C++、新重链／正式协议、14 组正式相邻协议、完整正式协议与最终正式构建均退出码 0；第 841 项正式原生 ANALYZE 协议亦通过。根冻结第 838 项 HEAD 2c6d4f70 的完整正式脚本 374/374 C++、89/89 E2E 和同一二进制真实 PostgreSQL 18.6 差分 373/373、failed=0，均已确认退出码 0。该轮不含第 839–842 项；原第 831 项脚本退出码 1 记录不抹去。准备主分支快进最新修复再全量验收，继续处理 EXPLAIN／普通 FROM quoted 名称、阈值自动分析事务时机、浮点／NULL 统计、旧索引迁移等。总账仍 273 项：24 complete、136 partial、98 unverified、15 deferred_by_user，未 push，Actions 禁用。

2026-09-30 第 841 项进展：原生 ANALYZE 目标列表已独立提交，新协议、10 组相邻协议及专项真实 PostgreSQL 18.6 差分 cases=1 failed=0 均退出码 0；隔离正式构建／完整协议运行中。根第 838 项的 374 个 C++ 已全部通过，89 组 E2E 和 373 组真实差分仍运行，不含隔离第 839–841 项。继续处理写语句内强制 ANALYZE 的错误时机与配置旁路、EXPLAIN／普通 FROM 的 quoted relation 解析、浮点直方图截断，以及旧索引迁移等未完成条目。总账仍 24 complete、136 partial、98 unverified、15 deferred_by_user，未 push，Actions 禁用。

2026-09-30 第 840 项进展：未知与已分析零行基数已独立修复，7 个 C++、新增重链／正式协议、10 组相邻协议、完整协议、隔离正式构建均退出码 0；第 839 项最终完整协议亦已退出码 0。统计／成本现有子集实测后 OPT-04／05 改为 partial，总账 273 项：24 complete、136 partial、98 unverified、15 deferred_by_user。新发现 legacy 写语句内自动 ANALYZE 得零，以及原生 ANALYZE relation 被拒绝，继续独立修复；未据此降低成功期望。根冻结第 838 项正式全脚本和 373 组真实 PostgreSQL 18.6 差分仍运行，后续隔离修复不计该轮；未 push，Actions 禁用。

2026-09-30 第 839 项进展：EXPLAIN 的有界 OR／AND／括号分组已独立提交，新 C++、ASan+UBSan、新／11 组相邻协议、隔离正式生产构建及正式专项协议均退出码 0，最终完整协议仍运行。第 837 项完整协议、正式构建及正式 BETWEEN 专项协议均已退出码 0。根主分支冻结第 838 项 HEAD 2c6d4f70 重跑正式全脚本（374 C++／89 E2E）和同一正式二进制真实 PostgreSQL 18.6 差分（373 cases），仍运行，不含隔离第 839 项。接着处理未 ANALYZE 的零行估算、旧索引迁移、REAL/numeric 混合语义等未完成总清单；仍 24 complete、134 partial、100 unverified、15 deferred_by_user，未 push，Actions 禁用。

2026-09-30 第 838 项进展：差分工具超时单测已独立修复并提交，38 项在默认／15／37／120 秒四种环境均退出码 0。第 837 项的新协议及 10 组相邻协议均退出码 0，完整协议和隔离正式构建仍运行。根第 831 项旧整套失败记录保持，准备快进最新修复后正式重跑；总账仍 24 complete、134 partial、100 unverified、15 deferred_by_user，273 项未完成，未 push，Actions 禁用。

2026-09-30 第 837 项进展：EXPLAIN 的 BETWEEN／NOT BETWEEN 与字面量 and 分隔修复已提交，新 C++ 和真实协议退出码 0，相邻／完整协议及隔离正式构建进行中。第 836 项正式生产构建及正式专项协议均已退出码 0。第 831 项 HEAD 7f34371f 的完整脚本已结束，退出码 1：368/368 C++、82/83 协议/E2E 通过；pg_diff_runner_test 的两个 mock 断言写死 timeout=15，而实际遵守 DBMS_PROTOCOL_TEST_TIMEOUT=120，单独修正测试隔离后再验。该轮真实 PostgreSQL 18.6 差分 371/371、failed=0、退出码 0；不把后续隔离修复计入该轮。主分支已快进至第 836 项，旧错误索引迁移、REAL/numeric 类型提升、EXPLAIN 其余 lowering 及其他总清单仍未完成；未 push，Actions 禁用。

2026-09-30 第 836 项进展：整数索引写入规范化已独立提交，10 个 C++、新增及 8 组相邻协议、完整 PostgreSQL 协议和专项真实 PostgreSQL 18.6 差分均退出码 0；隔离正式构建运行中。第 835 项相邻／完整协议、正式生产构建与正式专项协议均已退出码 0。根冻结第 831 项的真实差分已确认 371/371、failed=0、退出码 0；其完整脚本仍运行，后续隔离修复不计入该轮。SQL EXPLAIN 丢弃 BETWEEN、旧错误索引迁移、REAL/numeric 类型提升及其他未完成条目继续处理。总账仍 273 项：24 complete、134 partial、100 unverified、15 deferred_by_user；未 push，GitHub Actions 禁用。

第 835 项已修复整数与有限 numeric 的精确比较及 BIGINT 小数 BETWEEN 精度丢失；6 个 C++、新增协议及新增真实 PostgreSQL 18.6 差分 cases=1 failed=0 均退出码 0，相邻／完整协议和隔离正式构建运行中。第 834 项正式构建／正式类型键协议及完整／8 组相邻协议均通过。新的隔离实测表明写入端 id=0001 与 id=1 被当作不同主键，heap 可出现重复数值，优先修复整数写入键及旧索引一致性；SQL EXPLAIN 的 WHERE parser 丢弃 BETWEEN、REAL/numeric 类型提升也仍在队列中。根冻结第 831 项整套／371 组真实差分尚未结束，总清单未完成。

第 834 项复用存储端的类型索引键，修复二级 IndexScan／Bitmap/DNF 按原始浮点、money、UUID、char 字面量漏行；9 个 C++／新增真实协议退出码 0，相邻／完整协议与隔离正式构建运行中。第 832 项正式构建／正式整数协议已退出码 0；第 833 项 8 组相邻协议通过。参考 PostgreSQL 18.6 又确认本地 REAL 与 numeric 的混合比较不兼容，不把该差异算通过；继续类型提升、整数与 decimal literal 比较、缺失统计估算及 Hash/Bloom 加载／恢复。根冻结轮次仍仅到第 831 项，整套尚未结束。

当前冻结轮次为第 831 项 HEAD `7f34371f`：正式生产构建已退出码 0，全脚本和同一正式二进制的 371 组真实 PostgreSQL 18.6 差分运行中，不计入第 832 项以后的隔离修复。第 832 项整数键的 6 个 C++／新增和 8 组相邻协议通过，隔离正式构建运行中；第 833 项文本 TIMING FALSE 的 5 个 C++／新增协议通过，相邻协议及正式整套待结束／复验。已确认浮点二级索引 query key 不规范使 EXPLAIN ANALYZE 漏行，接着统一现有存储键的类型转换；原整数列与 decimal literal 比较、缺失 ANALYZE 默认基数、Hash/Bloom loader／恢复和其他未完成清单仍在队列中。

最新整套验收（2026-09-30）：第 823–824 项冻结 HEAD `56782f45` 的正式生产构建和 371/371 真实 PostgreSQL 18.6 差分退出码 0，但完整脚本退出码 1，363/364 C++、75/77 协议/E2E 通过。第 831 项修复 VACUUM FULL 无主键夹具，定向完整用例退出码 0；两项超时在原二进制提高有界等待时间后复跑通过，但不抹去原整套失败或宣称定位根因。第 830 项 5 个 C++、新增 JSON ANALYZE 协议、9 组相邻协议、完整 PostgreSQL 协议、隔离正式构建／正式专项协议均退出码 0。主分支冻结第 831 项 HEAD `7f34371f` 重跑 build_tests（预计 368 C++／83 协议/E2E）。隔离第 832 项整数键归一化的新增协议与 6 个 C++ 已退出码 0，相邻／正式构建运行中，不属于冻结轮次。总清单尚未完成；接着处理文本 TIMING FALSE、其他类型索引键、缺失 ANALYZE 默认基数、整数与 decimal literal 比较。

1. P0-02：继续迁移 SELECT 的结构化结果输出。quoted alias、无 FROM 普通投影、有限 scalar SQL/PLpgSQL UDF、独立 `VALUES` 和已验证的无相关标量子查询投影已迁移；第 728–729 项修复了标量子查询中 NULL、空串、字面量 `NULL` 和带空格文本的线协议失真。CTE / set operation、相关与复杂标量子查询、SRF、完整 SQL function query body 和剩余 utility/function 分支仍需统一 typed rows / NULL bitmap，不能靠显示文本反推数据。
2. P0-01 / SQL-01 / QRY-10：删除剩余改变语义的字符串路径；第 679 项只覆盖已验证的 LIMIT/OFFSET 常量整数表达式，仍需处理无 FROM 查询其他形态、顶层 WITH TIES、复杂表达式与执行短路，不能用部分行切片测试关闭整项。
3. P0-16：参考端已通过 PostgreSQL wire protocol 在同一 session 无损读取 rows / NULL / headers / type OID / SQLSTATE / command tag，并要求精确 PostgreSQL 18.6 版本。已在临时目录从官方源码构建 18.6 实例并校准 `en_US.utf8` 排序/货币区域设置。第 821–822 项冻结源码的正式生产构建与完整脚本均退出码 0，覆盖 362 个 C++ 与 75 个协议/E2E 测试；同一生产二进制的 370 组真实 PostgreSQL 18.6 差分 `failed=0`、退出码 0。第 823–824 项随后才合入主分支，不计入这份整套验收；最新主分支整套待复验。此前完整脚本曾因序列快照回退、`ddl_ast_bridge_test` 清理竞态、prepared 旧期望、collation、view 和 CREATE TABLE options 测试清理竞态而失败，另有旧源码两轮因发现数据库路径缺陷主动中止；这些运行都不计通过。较早 `alter_inherit_test` 间歇失败隔离复跑 12 次后及后续全套通过，原因未定位。差分用例数不是总清单完成数，本地端按 case 重建 session。下一步仍需扩充 SQL、并发 schedule、catalog、crash point 和零 allowlist 发布门；这些用例不能替代总清单验收。
4. 然后按 B2–B5 推进 catalog / 事务化 DDL、持久性、资源治理及剩余功能族，逐项补实测证据。用户跳过的安全 / TDE 专项仍不计完成。
5. CAT-15：第 652 项 quoted schema 名含点号的序列路径，以及第 672 项 `public."a.b"` 与 `a.b` 的旧式物理键碰撞，均已改用可逆编码并纳入正式差分。后者在启动时按 catalog 归属迁移旧 public 带点号序列文件，重启测试验证计数值延续；归属歧义时拒绝猜测。序列 namespace 的全部依赖、并发、崩溃和旧格式组合仍未系统验收，CAT-15 仍为 partial。
6. IDX-03 / IDX-14 / OPT-15 / OPT-16：第 823 项 B-tree 加载／创建分离与第 824 项主键编码／复合成员规划已定向验证；上述最新整套仍有 VACUUM 夹具与超时失败，先处理并复跑。第 825 项缓存代际失效、第 826 项 Hash/Bloom Bitmap/DNF 失败检查、第 827 项 Bitmap EXPLAIN 节点、第 828 项 JSON 可选逗号、第 829 项 JSON cache-hit framing 均有旧版本失败与新版本定向通过证据，各隔离正式生产构建／正式专项协议已退出码 0；825／829 完整协议也退出码 0。第 830 项 JSON ANALYZE 单文档与真实计数的新增协议及 5 个 C++ 通过，相邻／完整协议与隔离正式构建进行中。下一步处理 VACUUM 夹具、文本 TIMING FALSE、缺失 ANALYZE 时的默认基数；整数条件 0001 在普通 SELECT 返回原行而 EXPLAIN ANALYZE 实际为零已复现，类型归一化需修复。继续 Hash/Bloom 运行时加载／显式构建和索引 rollback incomplete 的真实持久化状态。功能族均保持 partial，总清单未完成，用户跳过项不重新开启。

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

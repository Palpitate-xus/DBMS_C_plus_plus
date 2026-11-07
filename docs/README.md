# 文档索引

<!-- P37 vacuum-physical-references begin -->
<a id="vacuum-physical-references-current"></a>
## 当前批次：VACUUM引用安全与完整成本

本批1500项完整正常计时没有证明全面提速。在共同的真实after-DELETE种子上，单长字段首次VACUUM服务器CPU比旧版及旧版回测低8.36%／7.73%；多长字段首次VACUUM墙钟高9.83%／27.97%，CPU分别低1.92%／高35.29%。在共同after-UPDATE种子上，仅选ID的CPU高8.43%／8.39%，墙钟高9.77%／14.77%；三个全列查询墙钟也高于两次旧版，完整正负结果保留，原因OPEN。

新测试160／0通过，旧版160／48复现；8项相关回归旧／新均actual0，既有TOAST断言仍actual -6。[测试报告](test-report.md#vacuum-physical-references-current)、[1500操作与完整压缩证据](performance-report-2026-10-30.md#vacuum-physical-references-20261107)。

正式库存2398个测试文件（1131顶层／1136总C++），注册E2E684／独立E2E664不变。754个Markdown路径和725份保护记录保持，8份既有文档仅插入本批说明。原273项及22 complete／166 partial／70 unverified／15 deferred_by_user不变，历史增至287，IDX-07仍unverified/evidence[]。
<!-- P37 vacuum-physical-references end -->

<!-- P36 physical-row-null-reuse begin -->
<a id="physical-row-null-reuse-current"></a>
## 当前批次：扫描与排序投影执行优化

固定数据下的 720 次完整查询对照中，固定字段排序、单长字段排序、多长字段全列无排序的墙钟中位数分别下降约 27%～29%、23%～24%、10%～11%。只选 ID 的服务器 CPU 高于两次旧版约 12.37%／5.00%，墙钟居间；不能称所有查询或数据库操作都更快。

原样 270 操作矩阵的 30 个区域，墙钟 19 低于两旧／2 高于两旧／9 居间，服务器 CPU 为 12／9／9，客户端 CPU 为 9／16／5。单长字段 VACUUM 墙钟高约 22.90%／28.87%，多长字段 VACUUM 高约 19.10%／4.85%；另有 9 个区域服务器 CPU 高于两旧，全部保留，原因仍 OPEN。

新增原生1156检查通过，既有TOAST断言在旧／新两版均失败，未称全套通过。[测试报告](test-report.md#physical-row-null-reuse-current)、[性能与压缩证据](performance-report-2026-10-30.md#physical-row-null-reuse-20261106)。正式库存 2397 个测试文件（1130 顶层／1135 总 C++），注册 E2E 684／独立 E2E 664 不变；754 个 Markdown 路径与 725 份保护记录保持。原 273 项及 22 complete／166 partial／70 unverified／15 deferred_by_user 不变，历史增至 286，IDX-07 仍 unverified/evidence[]。
<!-- P36 physical-row-null-reuse end -->

<!-- P35 view-column-dependencies begin -->
## 最新验证：视图列依赖与 DDL 开销（2026-11-06）

[性能报告](performance-report-2026-10-30.md#view-column-dependencies-20261106)提供 270 个普通操作和 27 个视图 DDL 操作的全部样本、负向及压缩恢复方法；[测试报告](test-report.md#view-column-dependencies-current)记录 242 新检查、7 既有专项及原始 PG 差异；[契约](compatibility-contract.md#view-column-dependencies-current)列出删除保护、旧库和边界。正式库存 2396，Markdown 路径仍 754，历史 285，Goal active。下方为各自版本的历史结果。
<!-- P35 view-column-dependencies end -->

<!-- P34 projected-toast-demand begin -->
## 最新验证：部分列 TOAST 需求读取（2026-11-06）

[性能报告](performance-report-2026-10-30.md#projected-toast-demand-20261106)记录完整 C1/C2 两轮矩阵、收益与全部回退；[测试报告](test-report.md#projected-toast-demand-current)列出 83 项新检查、5 项既有专项和完整协议复核；[契约](compatibility-contract.md#projected-toast-demand-current)说明物理部分列适用范围。正式测试库存 2395，Markdown 路径仍 754，Goal active。后续各批条目保留各自版本的历史结果。
<!-- P34 projected-toast-demand end -->

<!-- P33 pg-class-declaration begin -->
## Latest: 2026-11-06 pg_class declaration

The [comparison](postgresql-comparison.md#pg-class-declaration-current) and [compatibility contract](compatibility-contract.md#pg-class-declaration-current) distinguish the actual34-column declaration from its seven executable values. The [test report](test-report.md#pg-class-declaration-current) records550 native checks, PG41 strict pass/product17of41, and actual legacy upgrade/cold results. The [performance report](performance-report-2026-10-30.md#pg-class-declaration-20261106) retains all270 timings, negative cases and both old medians. The new compressed evidence is 15.92 MiB; no extra Markdown file was added. The earlier P31 source-only proposal and rejected P33-C1 runs remain historical evidence, superseded only for the accepted declaration scope.
<!-- P33 pg-class-declaration end -->

<!-- CURRENT_LEGACY_LOG_APPEND_20261106 -->

## 最新验证入口（P32-C1）

普通日志改为逐次O_APPEND/writev追加，新增13组原生检查；当前正式测试2393，本批没有全套重跑。30案例服务器CPU为16低/6高/8居间，多LOB负向保留，未宣称一致提速。完整协议仍有原9项普通差异、默认425未换；见[本批完整报告](performance-report-2026-10-30.md#legacy-log-append-20261106)与[可逆原始证据](benchmarks/legacy-log-append-20261106.json.gz)。下方索引和历史保持。

使用项目从 [README](../README.md)、[使用手册](MANUAL.md)和[打包与部署](PACKAGING.md)开始。
接口范围以[兼容性契约](compatibility-contract.md)为准；命令示例不代表所有组合均已实现。

当前实现、已验证范围及剩余瓶颈集中见[状态说明](production-status.md#当前状态与验证范围)。
[与 PostgreSQL 的差距评估](postgresql-comparison.md)汇总功能总账、实际能力边界及 18.6 同机小表性能对照。
文档清单覆盖仓库 Markdown；历史问题和发布快照保留对应批次结论，不作为当前版本的全部能力声明。

## 审查、问题与验证

- [代码审查索引](review-index.md)：当前审查报告、复现、修复方法与验证边界。
- [逐项修复进度](review-fixes-2026-10-13.md)：审查问题的修复、独立提交和验收结果。
- [全部文档检查清单](documentation-inventory.md)：逐文件分类、本地链接和文档与系统一致性核查范围。
- [测试报告](test-report.md)：当前验证入口与按版本保留的结果。
- [数据库性能测试报告](performance-report-2026-10-30.md)：35 类操作及 PG 18.6 对照，含有界排序、按页扫描、普通 LIMIT、外部排序、回归范围与剩余瓶颈。
- [PostgreSQL 18 总清单](postgresql-18-gap-audit.md)与[机器可读状态](gap-progress.json)：唯一的功能完成状态来源。
- [实现蓝图](postgresql-18-implementation-blueprint.md)与[复查计划](review-closeout-plan.md)：工作顺序和验收方法。
- [用户暂缓范围](deferred-scope.md)：明确暂缓的模块及保留的未修复状态。

## 开发参考

- [SQL 命令参考](commandsList.md)
- [解析器](../src/parser/README.md)、[执行器](../src/executor/README.md)、[系统目录](../src/catalog/README.md)、[复制模块](../src/replication/README.md)
- [变更记录](../CHANGELOG.md)与[生产化状态](production-status.md)
- 文档维护检查：在仓库根目录运行 `python3 scripts/check_docs.py`；新增、移动或删除文档后用 `--update-inventory` 更新[清单](documentation-inventory.md)。

## 如何阅读历史文档

`issue-*.md` 保存对应提交、失败和局部验证证据；其中的 PASS 只适用于文档明确标注的输入与范围。
历史计划、旧 TODO、`archive/`、旧版本发布说明和按日期追加的批次记录用于追溯，不是当前版本的能力声明。
后续审查可以推翻旧结论，最新报告会说明依据，原始失败记录继续保留。

完整回归、静态审查、差分测试、sanitizer 和并发测试覆盖不同问题。任何单项通过都不能证明没有 bug。

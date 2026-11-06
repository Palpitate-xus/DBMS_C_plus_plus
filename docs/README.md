# 文档索引

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

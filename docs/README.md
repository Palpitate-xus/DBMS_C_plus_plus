# 文档索引

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

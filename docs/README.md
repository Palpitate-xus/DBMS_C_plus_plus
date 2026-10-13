# 文档索引

使用项目从 [README](../README.md)、[使用手册](MANUAL.md)和[打包与部署](PACKAGING.md)开始。
接口范围以[兼容性契约](compatibility-contract.md)为准；命令示例不代表所有组合均已实现。

## 审查、问题与验证

- [代码审查索引](review-index.md)：当前审查报告、复现、修复方法与验证边界。
- [逐项修复进度](review-fixes-2026-10-13.md)：审查问题的修复、独立提交和验收结果。
- [全部文档检查清单](documentation-inventory.md)：逐文件分类和本地链接检查。
- [测试报告](test-report.md)：当前验证入口与按版本保留的结果。
- [PostgreSQL 18 总清单](postgresql-18-gap-audit.md)与[机器可读状态](gap-progress.json)：唯一的功能完成状态来源。
- [实现蓝图](postgresql-18-implementation-blueprint.md)与[复查计划](review-closeout-plan.md)：工作顺序和验收方法。
- [用户暂缓范围](deferred-scope.md)：明确暂缓的模块及保留的未修复状态。

## 开发参考

- [SQL 命令参考](commandsList.md)
- [解析器](../src/parser/README.md)、[执行器](../src/executor/README.md)、[系统目录](../src/catalog/README.md)、[复制模块](../src/replication/README.md)
- [变更记录](../CHANGELOG.md)与[生产化状态](production-status.md)

## 如何阅读历史文档

`issue-*.md` 保存对应提交、失败和局部验证证据；其中的 PASS 只适用于文档明确标注的输入与范围。
`archive/`、旧版本发布说明和按日期追加的批次记录用于追溯，不是当前版本的能力声明。
后续审查可以推翻旧结论，最新报告会说明依据，原始失败记录继续保留。

完整回归、静态审查、差分测试、sanitizer 和并发测试覆盖不同问题。任何单项通过都不能证明没有 bug。

# 总差距清单执行计划

当前范围是 `postgresql-18-gap-audit.md` 全部 273 项，不再用局部收尾计划替代总任务。`review-closeout-plan.md` 仅记录上批第 191–196 项修复；它的完成不表示总清单完成。安全 / TDE 专项沿用用户先前的跳过要求，单独计为 deferred，不计完成。不 push，不启用 GitHub Actions。

| 批次 | 工作 | 验收与状态 |
| --- | --- | --- |
| B0 | 273 项与代码、测试、提交建立映射，纠正文档完成口径 | 已建立 `gap-progress.json`；未逐项核实的保留 unverified，局部修复仅为 partial |
| B1 | SQL 结果正确性与类型 / NULL 保真；推进 P0-01/02、QRY-01/04/07/10、FUNC-02 | 进行中；先复现具体错误，再修复并增加 C++ / 完整 SQL 回归 |
| B2 | catalog / 事务化 DDL / 依赖与回滚 | 未完成；按蓝图拆成可复现的原子子任务 |
| B3 | 存储 / WAL / MVCC / 索引 / 资源治理 | 未完成；需要故障注入、隔离和持久化验证 |
| B4 | 其余类型 / 函数 / 查询 / 优化器 / 监控 / 工具兼容 | 未完成；不能将语法支持当作运行时完成 |
| B5 | 备份 / 复制 / 恢复 / 扩展生态 / 升级 | 未完成；需要端到端验收，外部环境或破坏性迁移另行说明 |

每个具体问题独立修复、测试、commit，并在 `code-review-progress.md` 记录关联的总清单 ID。完成某个案例不会勾掉整个功能族。只有整个条目满足蓝图的验收要求，才可同时更新原 Markdown 复选框和 JSON 状态为 complete，并附测试、证据、提交。

使用 `python3 scripts/check_gap_progress.py` 检查 273 项是否遗漏、重复、证据失效或完成状态矛盾。`--require-complete` 在任意条目未完成时返回非零，包括用户跳过项；不能把跳过或部分实现包装为全部完成。

## 后续优先队列

1. P0-02：继续迁移 SELECT 的结构化结果输出。quoted alias、无 FROM 普通投影、有限 scalar SQL/PLpgSQL UDF、独立 `VALUES` 和普通标量子查询投影已迁移；CTE / set operation、相关及 legacy scalar subquery、SRF、完整 SQL function query body 和剩余 utility/function 分支仍需统一 typed rows / NULL bitmap，不能靠显示文本反推数据。
2. P0-01 / SQL-01 / QRY-10：删除剩余改变语义的字符串路径；继续处理无 FROM 查询、顶层 WITH TIES、LIMIT/OFFSET 表达式和执行短路，不能用部分行切片测试关闭整项。
3. P0-16：参考端已通过 PostgreSQL wire protocol 在同一 session 无损读取 rows / NULL / headers / type OID / SQLSTATE / command tag，并要求精确 PostgreSQL 18.6 版本。已在临时目录从官方源码构建 18.6 实例，校准 `en_US.utf8` 排序/货币区域设置后，当前 160 个用例文件的差分为 `failed=0`。下一步仍需扩充 SQL、并发 schedule、catalog、crash point 和零 allowlist 发布门；这 160 组不能替代总清单验收。
4. 然后按 B2–B5 推进 catalog / 事务化 DDL、持久性、资源治理及剩余功能族，逐项补实测证据。用户跳过的安全 / TDE 专项仍不计完成。

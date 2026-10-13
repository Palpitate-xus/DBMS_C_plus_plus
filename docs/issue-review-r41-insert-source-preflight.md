# R41：恢复 INSERT SELECT 来源的执行前检查

## 问题与修复

复查 [R39](issue-review-r39-insert-select-owner.md) 时发现，旧 source 接收器
同时承担来源表和实际引用列的检查；切换到通用 QueryPlanner 不能假定这些
检查也由 planner 承担。普通物理、标量/派生子查询甚至 WHERE FALSE 来源
都必须保持原有权限语义。这是本轮改动产生的回归，不从清单中隐去。

现在在读取任何 source 行前，遍历真正 prepared SELECT bodies、source range
和 ColumnBinding 的 occurrence/ordinal。投影/star、WHERE、ORDER BY、JOIN、
group/window 表达式及保留子查询使用同一声明图；不通过值或 SQL 字符串猜列。
NATURAL/USING 的隐含键来自 binder 已解析的 merged descriptor，限定于对应
JOIN 子树，不能遗漏未投影的连接键，也不误要求无关来源同名列。

普通 INSERT bridge 和 prepared mutation constructor 共用检查入口。TEMP
保留其会话所有权路径，CTE/merged 逻辑范围不被误当成物理表，target 与
EXCLUDED 也不获得虚假的 SELECT source 身份。合法完整表/列授权及 source
NULL、冲突处理、子查询、排序、事务与 RETURNING 的执行规则保留。

## 真实验证

Root 初始输入 `e282aae2`，证据目录 `/tmp/dbms-review-fix-donor.N9sdas6f/`。

| 检查 | 实际结果 |
| --- | --- |
| 原始 38 项原生控制 | 13 失败，exit 1 |
| 初步 source 检查，扩展至50项（含隐含JOIN键） | NATURAL 两项失败，exit 1 |
| NATURAL/USING 修正后50项 | 全通过，exit 0 |
| 最终53项，新增prepared constructor三控制 | 全通过，exit 0 |
| 新驱动及六完整原生邻居 | 七者exit 0 |
| 八完整协议邻居（含原完整postgres_protocol） | 八者exit 0 |

每次源变化后真实重编 DmlExecutor.cpp 并标准链接成功；未改变公共头文件。
原生链接前验证全部58份当前源/头/flags receipts，结束核对输入未变；不把
中间失败时的旧对象/二进制标成候选。新驱动由原生发现机制注册，使用独立
隔离 cwd 的自有测试数据库，未操作任何真实用户表或外部账号。

原始和中间失败保留为 `r41-original-native.log`、`r41-initial-native.log`；
最终 `root-r41-complete-owner-build.log`、`r41-complete-owner-native.log`、
八份 `r41-*_test.py.log` 记录实际终态。邻居覆盖完整旧 R39、NULL bitmap、
BOOLEAN/INTERVAL 输入、冲突、RETURNING、PREPARE lifecycle 及完整协议。
此项恢复已存在的数据库使用检查，不展开用户暂缓的安全/TDE专项。

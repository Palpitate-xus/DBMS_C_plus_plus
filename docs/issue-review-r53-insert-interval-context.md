# R53：INSERT SELECT 的 UNKNOWN INTERVAL 输入上下文

## 问题与修改方法

原完整 `interval_column_modifier_protocol_e2e_test.py` 的27项中，INSERT SELECT
写入 YEAR/DAY TO MINUTE 列时，把裸 UNKNOWN `'4'` 提前按无范围 INTERVAL 转成
4秒，再由存储按目标范围截断成0；实际 PostgreSQL 18.6 分别得到4年/4分钟。

纯 assignment preflight 已有正确目标描述符。现在把该描述符的标量 INTERVAL
typmod传递给直接 UNKNOWN 输出根的真实 Cast AST，首次解析即使用目标字段范围，
并同步准备后 source descriptor 的 typmod；保持原始 projection provenance。
不重解析SQL、不提前执行函数，不给派生表/CTE/集合操作中已定型的TEXT强加上下文。
已有INTERVAL表达式保留自己的输入解释，再由原存储赋值应用目标范围。

数组必须保留不同阶段：实际参考将数组中的裸数字先按秒解析，再应用元素范围，
所以 `'{4,NULL,-2}'` 写入 INTERVAL YEAR[] 得到 `{00:00:00,NULL,00:00:00}`，
并非4年/-2年。修复只对标量传递首次解析范围，不误改数组。

## 验证证据

目录 `/tmp/dbms-review-r46.ifu6SWLp/`：

| 检查 | 实际结果 |
| --- | --- |
| 新133协议控制的实际PG18.6 | 最终133全0，r53-reference-corrected.log；同日志含原驱动参考28项全0 |
| 主分支未含R53的真实二进制 | 新133中9项失败，r53-root-before-fix133.log；冻结副本SHA1489f219ab7943f4996f99143bad830149b1785ee6fa34fc81f62fb70ccf69ad |
| 最终候选新133与原27 | 全0，r53-first133-candidate.log、r53-first-original27-candidate.log及最终协议批次 |
| 新纯binder控制 | 78项全0，真实Cast修饰符、source descriptor、NULL和空来源，无数据库读写 |
| 最终完整native组合 | 六个完整入口全0，r53-final-native-corrected.log，当前全部58份receipts及终态输入一致 |
| 最终完整协议组合 | 七个完整入口全0，r53-final-protocols-corrected.log；含源检查优先级、冲突owner、NULL bitmap和BIT描述符 |
| 标准构建 | query_binding.cpp真实O2重编、链接0，r53-first-standard-build.log |

新控制保留13种字段范围、UNKNOWN/已类型化INTERVAL/NULL、FROM和空来源、TEXT
拒绝、派生/CTE/UNION拒绝、无行也检查坏输入，以及数组解释差异。

第一次新参考错误地期待数组按年解析，参考实际失败1项；按真实结果修正后PG133
全过。最初候选日志含9个真实标量错误和该1个错误期望，不能全称10个产品bug。
`r53-original-corrected.log` 的启动发生在候选已重建后，实际上是候选通过证据，
不能当成修复前基线；因此另用不含R53的主分支二进制冻结副本获得上述9失败。
两批首次门禁因拼错不存在的驱动名未执行，日志保留；正确文件名预校验后重跑，
不计不存在的文件为通过。

本项不关闭其他聚合、窗口、协议错误恢复和完整INTERVAL功能族问题。主分支集成
及下一轮全注册验收独立进行，原273状态不因本项自动完成。

# R45：窗口聚合的表达式参数、类型和实际需求

旧 legacy 窗口聚合把完整参数当作物理列名。`sum(v+1)`、
`sum(nextval(...))` 等语句因此返回 NULL，COUNT 表达式返回 0，结果声明
还可能退成 TEXT。正常 SQL 没有报告能力错误，却给出错误数据；SUM 的
VOLATILE 参数根本没有执行。O05 复查发现并单独登记此项。

## 修复方法

参数保留实际 prepared AST，由已有窗口参数 receiver 绑定和求值，不再
通过字符串列名查值。元数据使用真正 query-host 的窗口重载声明，SUM/
COUNT/AVG/MIN/MAX 的输出类型不从参数文本猜测。SQL NULL、空串和文本
`NULL` 分别保留；MIN/MAX 使用声明类型和实际 collation 比较。

同一 parsed window 的聚合按实际成员行推进，然后再推进下一成员；多个
不同输出的参数保留各自求值站点。O05 已证明等价的排序输出 owner 才共享
聚合值。子查询参数继续使用真实 child cursor/相关行，非相关 InitPlan 的
memo 由原执行 carrier 管理，不重新解析 SQL 或另建一个执行者。

固定 frame head 可以追加新成员并保留状态。head 移动或存在 EXCLUDE 时
重新计算当前成员，不能把 VOLATILE 参数按原始行号永久缓存，否则移动
窗口的值和调用次数会错误。peer 的相同 frame 可以复用结果。NULL 输入
不参与 SUM/AVG/MIN/MAX，COUNT(*) 则计入实际成员。

parent LIMIT/OFFSET 按需要消费窗口输出前缀；没有额外排序，或排序确实
保留当前窗口顺序时，不执行后续输出的参数。需要跟随 frame 的未来成员
仍按需求读取。LIMIT 0 保留纯准备检查，但不打开输入。额外排序/去重的
完整输入需求与这些前缀优化分开，不能仅截断源数据破坏 FOLLOWING。

整数/NUMERIC 使用精确累加，REAL/DOUBLE 使用其浮点声明及转换格式；
REAL 参数扩展到 AVG 的 DOUBLE 累加器前恢复实际 float4 宽度，不能把
用于显示的最短十进制字符串直接当成原来的 double 输入。

```sql
SELECT id, sum(v + 1) OVER (
  ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
) AS total FROM example ORDER BY id;
```

## 验证记录

证据目录 `/tmp/dbms-review-fix-donor.N9sdas6f/`。原版本为 `c5d10cc3`，
冻结程序 `root-before-r45.frozen` SHA-256：
`5c990c603c0a3cdc2c326499928cf83087dbe19df46be778ed8ce1392cfc258d`。

| 检查 | 实际结果 |
| --- | --- |
| 原四个 SUM VOLATILE 控制 | 原版本全失败，PG18.6 全通过；`r45-original4-protocol.log` / `r45-original4-postgres-reference.log` |
| 原版本最终300协议控制 | 287 失败，exit 1；`r45-original300-protocol.log` |
| PostgreSQL 18.6 / 180006，300控制 | 全通过，exit 0；`r45-final300-postgres-reference.log` |
| 首候选294控制，O0 Main诊断 | 全通过，exit 0；`r45-first-candidate294-protocol.log` |
| 扩展300首候选，O0 Main诊断 | 1项REAL AVG宽度失败，exit 1；`r45-first-candidate300-protocol.log`，原失败保留 |
| 修正后300控制及十六完整协议邻居，O0诊断 | 十七完整驱动全通过，exit 0；`r45-second-candidate300-protocol.log` / `r45-second-o0-diagnostic-protocols.log`，Main输入hash一致，57库对象按当前标准flags/header/source核对 |
| Root最终标准O2构建 | exit 0；`root-r45-first-standard-build.log`，Main实际重编及标准链接 |
| Root最终300控制及十六完整协议邻居 | 十七完整驱动全0；`r45-final-protocols.log` / `r45-final-*_test.py.log`，含完整postgres_protocol、O05、R04 307/78、R07/R33/R44、EXPLAIN JOIN及逻辑解码 |
| Root十四完整native邻居 | 全0；`r45-final-native.log`，含O05 native74/R04 native349/R07 native122，全部58当前receipts与终态生产输入一致 |

300 控制包括三种 frame 模式、十二组双向边界、四种 EXCLUDE、五种聚合、
NULL/空串/字面量 NULL、参数子查询、独立/共用输出、多个分区、浮点类型、
LIMIT/OFFSET、错误阶段及值/标签/OID/命令计数。副作用控制直接核对序列
次数，不把累计值正确误当作执行次数正确。

初始294参考只有一项夹具失败：错误地要求执行期除零前没有任何已发送行。
PG已发送正确的 `5`、`15` 前缀后返回22012，无成功命令标签。最终测试对
分析错误仍严格要求零行；对该运行期错误只允许正确前缀且必须保留原错误，
不要求缓冲/发送时机完全相同。失败保留于
`r45-first294-postgres-reference.log`，没有改变成功值、SQLSTATE 或调用次数。

这是表达式参数的正确性修复，不宣称全部 PostgreSQL window/浮点行为
兼容或已具备所有 inverse-transition 优化。移动窗口目前仍会重算成员；
特殊 NaN/Infinity、全部复合类型以及不同 window 的完整执行图/需求组合
未在此专项关闭。最终完整注册回归和273功能总账另行验收。

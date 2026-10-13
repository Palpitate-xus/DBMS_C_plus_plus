# R44：窗口临时结果不能占用用户源列名称

R07相邻复查发现legacy把窗口结果写到源行map的 `_win_0/_win_1/...` 键。
这些都是合法用户列名，ROW_NUMBER/RANK等会覆盖本应读取的源值；后续
nth_value、LAG、NTILE参数、物理列投影和最终排序读到的便是临时结果。
原生入口正确，强制legacy后正常两行 `(1,10),(2,20)` 却可返回第一值1/30
而不是10。真正SQL NULL也可能变成非NULL空串或数字。

## 修改方法

每个源行另持有按真实窗口ordinal索引的windowValues，与源列map和窗口NULL
集合分别存储。装载时初始化完整槽宽；窗口计算/前行rank复用/结果投影只消费
对应槽位，不再创建任何SQL可见临时列名。排序、参数求值、nth_value及物理
投影继续读取未被修改的实际源map，且随行索引移动完整values/NULL身份。
不保留一组所谓禁用列名，用户不需要重命名已有表，也不通过禁止合法窗口
SQL回避问题。

## 实际验证

证据目录 `/tmp/dbms-review-fix-donor.N9sdas6f/`。初始3控制的实际PG/原程序
对照见 `r44-legacy-source-collision-original-and-pg.log`：原程序2失败/1阳性。
该对照读取冻结 `41d45c4a` 程序。最终39控制原版本为 `c94a8714`，冻结
`root-before-r44.frozen` SHA-256为
`d2ec3993c6d75e903cc8ee1a631b71c5d04e925bee633a9e97441ceb7374862b`。

| 检查 | 实际终态 |
| --- | --- |
| 原版本，同一完整39控制 | 29失败，exit1；`r44-original-protocol.log` |
| 实际PostgreSQL18.6 / 180006，同一39控制 | 全通过，exit0；`r44-postgres-reference.log` |
| Root标准构建 | 真实重编Main并标准链接，exit0；`root-r44-build.log` |
| Root同一完整39控制 | 全通过，exit0；`r44-final-window_internal_namespace_protocol_e2e_test.py.log` |
| Root十五完整原生邻居 | 全部exit0；`r44-final-native.log`，全部58当前生产receipts及最终源签名不变 |
| Root二十完整协议/CLI驱动 | 全部exit0；`r44-final-*_test.py.log`，含R07/R06/R33/声明比较/NULL/frame/EXECUTE及完整postgres_protocol |
| 文档与注册 | exit0；476唯一注册/459 standalone e2e，全部Markdown本地链接/inventory另验 |

保留原3条SQL及全部精确断言，另有BIGINT极值、NUMERIC高精度/scale、TEXT
C排序（空串/字面量NULL/空格/真正NULL），各表同时有三个碰撞列。覆盖原生
阳性、三个窗口/物理投影、最终物理列排序、NULL位置、分页、LIMIT0、NTH自身
碰撞及NULL传播、LAG按原值求值、NTILE按原参数初始化、DISTINCT及SQL普通
源值保持。NUMERIC比较按其已断言OID1700进行；TEXT绝不因内容像数字而归一。
同时断言标签、列名和全部OID，错误数字编码计作失败，不隐藏原控制。

普通源值保持控制在原实现也通过，未观察到这些SELECT改变持久行；不能把
本项描述为已实测heap写入损坏。修复不关闭frame R04、逻辑预览R13、EXPLAIN
O03或重复别名O05，不宣称全注册回归已经通过。

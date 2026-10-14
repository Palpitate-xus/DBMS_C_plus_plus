# R63：DEFAULT不能成为普通值或同名列

## 原因与修改方法

未引用的DEFAULT被解析成ColumnRef，普通`SELECT DEFAULT`错误报42703；存在
同名`"default"`列时，非法表达式甚至能借用该列，空输入/LIMIT 0也返回成功。
真实PG18.6的DEFAULT只能由合法赋值/INSERT VALUES等语法角色消费，普通表达式
报42601，不能根据名称或显示文本执行。

修复为未引用DEFAULT建立真正Literal语法节点，由普通值binder拒绝。INSERT
VALUES与UPDATE赋值的现有DEFAULT消费者仍先处理自己的合法角色；引用列、
限定列、字符串和AS输出标签保持原身份。第一轮只改parser/binder后，native
通过，但legacy SELECT还有9条失败，部分返回文本DEFAULT或错误22P02。
继续在原AST中识别DEFAULT site，复用R60的原作用域纯预绑定路径，先绑定再让
legacy接收器执行，保留FROM/目标表达式的实际错误优先级与空来源检查。

## 实际验证

外部oracle/基线目录`/tmp/dbms-review-orphan-controls.vQleNfjM/`，候选证据目录
`/tmp/dbms-review-r46.ifu6SWLp/`：

| 检查 | 实际结果 |
| --- | --- |
| 实际PG18.6原30/扩展33 | 全0，default-role-reference30.log、default-role-reference33.log |
| 冻结主分支原30/扩展33 | 16/17失败，default-role-original30.log、default-role-original33.log |
| 新native原主分支 | 首个SELECT DEFAULT得到42703而不是42601，exit134，r63-root-original-native.log |
| 第一parser/binder候选 | 完整19native通过，原30协议仍9失败，r63-first-native.log、r63-default_value_role_protocol_e2e_test.log；组合五协议中只有四个邻居通过 |
| 最终新控制 | 33协议与强化22native全0，r63-final-default_value_role_protocol_e2e_test.log及r63-final-native.log |
| 最终完整native门禁 | 六入口全0，r63-final-native.log；含DEFAULT sequence、INSERT source preflight、prepared execution及CREATE/background I/O相邻控制，当前58份receipts/结束输入一致 |
| 最终完整协议门禁 | 两组共十入口全0，r63-final-protocols.log及r63-default-neighbor-protocols.log；后五个为INSERT默认值prepared/规划、UPDATE域默认值、domain default事务和长默认表达式完整入口 |
| 标准生产构建 | 第一轮parser/query_binding、第二轮Main真实O2重编并链接0，r63-first/second-standard-build.log，无公共头变化 |

新控制保留合法INSERT/UPDATE DEFAULT、真实RETURNING值/NULL/OID、引用和限定
同名列、字符串、CASE未选分支、空输入、LIMIT 0与VALUES。缺失表42P01先于目标
DEFAULT；目标依次绑定时，DEFAULT/缺失列的先后仍分别得到42601/42703，不能
为得到固定42601而在parser中无条件拒绝并改变原作用域错误优先级。

本项先独立私树提交，主分支1339回归继续保持`18d85f4e`冻结输入。原失败与
中间9失败均保留，不冒主分支当前完整注册回归全绿或原273功能族完成。

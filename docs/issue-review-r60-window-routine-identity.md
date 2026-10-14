# R60：窗口调用必须保留真实函数身份及角色

## 复现与修改方法

普通标量`upper(t) OVER()`被当作普通表达式执行，空来源/LIMIT 0甚至成功。
search_path前置schema中的真实SQL函数`row_number`、`lag`、`array_agg`也被
窗口builtin规则覆盖。显式`pg_catalog.array_agg(v)`值看似正确，但legacy
重建调用只保留规范短名，Describe再次绑定到用户标量函数，结果OID错误为23。

修复应在实际参数绑定后、任何行或常量执行前，拒绝真实标量函数的OVER角色
（42809），保留缺失列42703和缺失routine42883的优先级；不通过运行函数推断
角色。legacy窗口的类型/参数query必须保留原调用限定名、引号及FILTER，不能
从规范调度短名重建另一处不同身份的调用。

本项只处理已存在标量routine的窗口角色与窗口调用身份。通用标量重载的完整
签名校验另列R62，不能把该缺口写成已完成。

## 验证记录

证据目录`/tmp/dbms-review-r46.ifu6SWLp/`：

| 冻结/检查 | 实际结果 |
| --- | --- |
| 实际PG18.6初始64控制 | 0失败，r60-reference-first.log |
| 原R56候选64控制 | 44失败，r60-original.log |
| 实际PG18.6扩展73控制 | 0失败，r60-reference73.log |
| 同一未修正二进制扩展73 | 53失败，r60-original73.log；SHA与冻结Root相同，为3f1c6502e0502758af9fdc11e8c324738465de3b19ea098ed4b32bd74f9f11f4 |
| 原主分支原生新控制 | 首个upper OVER纯绑定未抛错，exit134，r60-root-original-native.log |
| 第一候选73控制 | 仍有8失败，r60-first73-candidate.log；顶层预检查漏过CASE/WHERE嵌套site，未提交为完成 |

第二轮补齐原AST的表达式/来源/CTE/集合分支site遍历，只据真实hasOver标志
触发纯绑定，不搜索SQL字符串，也不运行未选分支。最终73控制全0，日志
`r60-final-window_routine_identity_protocol_e2e_test.log`。

候选第一轮真实O2编译Main/TableManage、第二轮真实O2编译Main并链接0，
`r60-first-standard-build.log`与`r60-second-standard-build.log`。最终六完整
native全0，`r60-final-native.log`，包含新59纯metadata控制、真实volatile序列
未消费证明和原函数namespace/window/参数执行邻居；开始/结束当前58份生产
receipts及缓存输入一致。最终十完整协议全0，`r60-final-protocols.log`，保留
原窗口类型、45数组、300参数、frame初始化、152偏移、unordered、内部列名、
qualified routine及117重复别名完整断言。原失败与第一轮8失败均保留。

主分支1339完整注册回归另冻结在`18d85f4e`；本项先独立私树commit，期间不修改
其任何tracked输入或二进制，也不提前合入。不能将私树结果冒充主分支复验。

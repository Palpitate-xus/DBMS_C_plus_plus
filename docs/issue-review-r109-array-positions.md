# R109：缺失ARRAY_POSITIONS

状态：用户要求跳过当前项；候选代码、测试与记录保留，未提交，不计已修复。
跳过前自身正常/repeat构建、388原生/248协议控制及33native/30协议/3真实Main
共66个不重复完整入口实际退出均0；这是候选快照的有限证据，不是接受版本或总清单完成。
本项继续原TYPE-16/FUNC-01/SQL-09普通功能集，不恢复R106等用户跳过专项。

## 问题与实际参考

原生注册表及SQL入口没有ARRAY_POSITIONS，合法调用统一报42883。实际180006
参考与固定75e8旧程序各29条完整观察有20条语义差异；观察器退出0仅表示
完成采集，不是行为通过。参考源码pg_proc.dat的OID3279声明为
`array_positions(anycompatiblearray,anycompatible) -> integer[]`，非STRICT。

| 调用 | PostgreSQL18.6 | 固定旧程序 |
| --- | --- | --- |
| INTEGER数组{1,NULL,1,2}查1 | {1,3} | 42883 |
| 同数组查SQL NULL | {2} | 42883 |
| 同数组查99 | 空INTEGER数组{} | 42883 |
| 下界-2的同数组查1 | {-2,0} | 42883 |
| 空数组查1或NULL | {}，不是SQL NULL | 42883 |
| SQL NULL数组查1 | INTEGER[] SQL NULL | 42883 |
| NUMERIC{1.00,2,1.0}查NUMERIC 1 | {1,3} | 42883 |

这是收集全部匹配下标，不是把ARRAY_POSITION首个下标包装成数组。结果容器
是INTEGER[]，元素是输入真实下标；缺失元素/空输入返回空数组，与NULL输入
返回SQL NULL不同。元素相等、NULL bitmap、无默认相等能力、未知字面量输入
及调用声明必须各自遵循正确阶段，不能先求值参数、函数或子查询选类型。

## 修复计划

1. 固化多匹配/无匹配、上下界、NULL/empty、真实typed相等及声明/错误优先级。
   三种原生模式（parsed、prepared、真实常量planning）和真实wire参数OID、
   PREPARE、数据行、RETURNING、volatile参数次数与用户重载分别覆盖。
2. 与[R108](issue-review-r108-array-position-typed-equality.md)共享真正的typed
   数组查找实现，保留R86/R108所有旧断言；新增两参数anycompatible声明及
   INTEGER[]返回元数据，不复制任意文本相等或新建全局TEXT→数组隐式边。
3. 仅真实builtin取得签名/结果/immutable规划规则。同名用户函数保留具体声明；
   registerFunction替换保留原始cell和unknown结果，不继承INTEGER[]或被预执行。
4. Main/Parse/Describe使用同一真实绑定，错误签名不得被空来源或未选分支隐藏。
   不修改公共头、协议字节或provider生命周期，不恢复R65/R106等暂缓模块。
5. 本项自身正常/repeat构建、匹配全部58 receipt/57库字节/冻结输入，运行新增
   完整入口、数组/声明/数值邻居及真实Main前端，保存所有失败后独立本地commit。

## 当前证据

证据目录`/tmp/dbms-review-r46.ifu6SWLp/`：
`r109-array-positions-observer.py`、`r109-array-positions-reference-observations.log`
与`r109-array-positions-old-observations.log`保存原29条采集。
固定旧程序`r109-pre-fix-dbms-main`的SHA256为
`75e8e272167aaaacc1f3a013ad34c9c6892d022bc7776d1b947b407947a62bf5`。

永久协议`tests/array_positions_typed_equality_protocol_e2e_test.py`先为242控制，
实际180006零失败/实际0，固定旧程序154失败/实际1；初版228零失败另保留。
补入真实text Bind/Execute后最终248，实际180006零失败/实际0、旧160失败/实际1；
日志`r109-reference248.log`、`r109-old-protocol248.log`，242版日志也保留。协议注册一次。
永久原生`tests/array_positions_typed_equality_test.cpp`有101条核心×三模式，
加声明/回调/参数/用户callee/原生raw载体等，完整388控制，旧库290失败/实际1，
日志`r109-old-native388.log`。候选五CPP已实现，未改公共头；四份自有O2库加
53份来源/receipt/字节匹配的Root库、fresh stubs与冻结输入的开发验证中，新增388、
原R108的354、R86的208及完整array_functions四入口均实际0，日志
`r109-development-candidate1.log`。这不是正常生产Main或全部58 receipt验收；
正常/repeat构建均实际0，实际重编5/0个CPP；新的匹配cache/fresh stubs核心
四个完整原生入口388/原354/原208/原array库及四个完整协议248/原258/原80/
原654均零失败。33native/30协议/3真实Main的66个不重复完整入口均实际退出0；
不借用R108的64入口作为本项证明。候选binary为
`c1f5ce345ef26132e466aac2a2928f95183a1f230f94d44a1338bc45dd97704c`，
cache为`768757c2a589b7a52ddcc47cfa1361871a1d73bdc0dbc569092a9f456079bb42`，
构建前冻结src/tests/scripts seal为
`ea5b22a4f1e8367c45061b004f2fad059dd003e48ff35e6345cf4d5a235b9bcf`。

另确认独立R110边界，尚未修复：实际180006与固定75e8各十条参数/格式观察有
两条差异，非NULL NUMERIC[]和INTEGER[]请求binary结果时旧程序在RowDescription
之后报0A000、没有DataRow，参考正常返回数组二进制；SQL NULL数组的binary返回
仍正常。相同参数的文本格式及NUMERIC[]/INTEGER或INTEGER[]/NUMERIC needle
调用原array_position均正常，不能将其写成普遍参数类型丢失。
证据`r110-array-parameter-observer.py`、`r110-array-parameter-reference-observations.log`
和`r110-array-parameter-old-observations.log`保留；两个观察器实际0只是采集完成。
修改方法是为数组形状、lower bounds、NULL bitmap和真实元素OID实现二进制
输出并复用真实元素codec，而非把文本字节标成binary；binary输入和record的
完整能力仍需各自验证。这是待做的普通TYPE-16/PROTO-04工作，不是用户暂缓项，
不会用本项ARRAY_POSITIONS文本/native验收代替它。

独立R111也已确认、未修复：实际180006/固定75e8各16条ARRAY构造器与相同
array-literal CAST的常量规划观察有4差异。`array_position(ARRAY[point],point)`
和JSON构造器形式在WHERE false/LIMIT 0下参考报42883、旧程序误返回空结果；
相同CAST-array形式正确报错，未选COALESCE后臂两边均返回7。
证据`r111-array-constructor-planning-observer.py`、
`r111-constructor-reference-observations.log`、`r111-constructor-old-observations.log`
保留，两次观察器0不是行为通过。修改方法是绑定后、按真实需求剪枝后折叠
纯结构ARRAY构造器，再规划被明确选中的immutable builtin；不得执行行、参数、
SQL child或用户回调发现常量，也不得据此重开R65/R106。该通用规划缺口另项
修复，不把它隐藏为ARRAY_POSITIONS或R108已经通过的范围。

该参考构建的XML支持不可用，不据其XML环境错误关闭Root XML。任意复合/域、
一般多态、数组comparison/hash、assignment与完整record/array binary I/O
不因新增一个函数而完成；原273仍22 complete/166 partial/70 unverified/
15 deferred，items与checkbox不提升。R98/R99候选和R106未注册草稿保留、
排除本项提交；不push，Actions禁用。

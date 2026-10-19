# R158：角度换算的中间溢出与次正规数

状态：角度数值边界的有限修复已通过两端组合验收；原失败与依赖证据保留。
本项不恢复用户暂缓的 R156 完整类型化常量、内置类型呈现或 JSON 语法身份。

## 已复现的错误与修改方法

| SQL 表达式 | PostgreSQL 18.6 | 修复前 Root / accepted-only |
| --- | --- | --- |
| radians('1e308'::double precision) | 有限值约 1.7453292519943295e306 | Infinity |
| degrees('1e306'::double precision) | 有限值约 5.729577951308232e307 | Infinity |
| degrees('1e308'::double precision) | 22003，真实溢出 | Infinity |
| radians('5e-324'::double precision) | 22003，下溢归零 | 0 |
| radians('1e-320'::double precision) | 非零次正规数 | 0 |
| degrees('5e-324'::double precision) | 非零次正规数 | 0 |

[原实现](../src/expression/ExprEvaluator.cpp)先计算 x*pi 或 x*180，
中间结果可能溢出，尽管最终角度仍可表示；没有检查实际溢出或下溢归零。
另外 ExprValue::asDouble 使用 stod，遇到可表示的非零次正规数也可能异常后返回 0。

候选只改这两个已有 builtin 的回调：复用既有严格 float8 输入解析器，
以固定 binary64 常数 0.0174532925199432957692 做一次除法或乘法，
复用既有浮点范围检查。保留 NaN、正负 Infinity、正负零、非零次正规数和 SQL NULL。
不全局修改 asDouble，不改变原生替换回调的签名、参数、返回值或求值归属。
参考：[PG float.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/float.c)、
[PG float.h](https://github.com/postgres/postgres/blob/REL_18_6/src/include/utils/float.h)。

## 永久夹具与修复前证据

[原生夹具](../tests/angle_conversion_range_test.cpp)完整 603 项：
30 个输入、两函数、原生参数/绑定/解析字面量/标量 helper/行 helper/纯描述六条路径，
全部数值 NULL 类型、原生 opaque 替换所有权及非法签名；比较实际 binary64 位。
[协议夹具](../tests/angle_conversion_range_protocol_e2e_test.py)完整 1449 项：
54 个输入，两函数，Simple/Extended text/binary、numeric 输入、嵌套调用、
保留 statement 的正反向重执行、text/binary Bind、Parse/Bind Describe、
真实表与 NULL、零行、变量 CASE/COALESCE、事务失败及保存点恢复。
描述符固定 OID701、长度8、typmod -1；零和结果的 binary 位不能被近似比较掩盖。

实际 PG 18.6 / 180006 的完整协议夹具 1449 项、0 失败。
未改夹具在旧 Root 与冻结的已接受 R157 上各完整执行：原生 603 项各 140 失败，
协议 1449 项各 587 失败，均实际退出 1。不是从 SQL 样例推算失败数量。
旧收集驱动末尾在错误 cwd 校验相对路径清单而退出 2；
所有测试实际终态及原日志保留，随后在正确旧目录独立校验全部 tracked 字节成功。
这项驱动错误不改写为产品失败，也不把旧测试改写为通过。

## 仍开放的要求

首轮修复候选完整门禁实际失败：Root 23 个入口及 accepted-only clean 23 个入口
均退出 1，合计44通过、2失败。新原生两端603项全过；新协议两端1449项各18失败。
这18项都是实际表的限定函数调用被旧适配器错误拼成单个标识符而报42883，
已另立 R161 处理；不是浮点范围断言可以删除的理由。
首轮 clean 构建包装器也曾在开始编译前因相对清单 cwd 错误退出1；原日志保留，
修正包装器后实际 fresh Evaluator O2 构建通过，不将未开始的编译当作通过。
所有既有 clean/donor 已跟踪文件永久冻结；后续使用新的独立目录。

[R160 常量规划时机](issue-review-r160-angle-constant-demand.md)仍未修复；
不能把 reached-value 的范围修复称为角度函数所有规划语义完成。
[R159 编码差异](issue-review-r159-encoding-boundaries.md)现已分别完成hex、escape、base64三个本地修复提交；
该独立编码验收不替代本项与R161等依赖的完整回归。
原始 20 条诊断观察完整保留；通过夹具不替代其中十条常量时机或四条编码观察。
后续须完整复测、逐项修复，而不是删除失败或给 PostgreSQL 与本产品设置不同预期。

本地证据目录：/tmp/dbms-review-r46.ifu6SWLp/r158-*。
原 273 总账未完成；不 push，GitHub Actions 保持禁用。

## 最终候选的完整运行检查点

R169 V4工作区完整58入口实际全部通过；本项native603、协议1449均0失败，
原18条真实表限定调用失败通过R161及其签名/准备依赖消除，没有删除控制。
非零次正规数、正负零及所有binary位断言保留，范围修复不关闭R160常量规划时机。
独立干净版本亦完整58实际退出0；两端各26native、29协议、三个真实Main全部通过。
组合构建、范围及原始日志索引见[R169](issue-review-r169-simple-query-raw-syntax.md)；
该结果不表示本项一个中间提交单独通过依赖全部到齐后的矩阵，也不是整库全量通过。

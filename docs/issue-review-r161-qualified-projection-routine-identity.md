# R161：真实表投影保留限定函数身份

状态：限定标量表投影身份的有限修复已通过两端组合验收；未限定quoted相邻问题仍开放。

## 已复现的问题

~~~sql
CREATE TEMP TABLE angle_rows(x double precision);
INSERT INTO angle_rows VALUES (1), (NULL);
SELECT pg_catalog.degrees(x) FROM angle_rows;
~~~

PG 18.6 正常执行；旧 Root 和已接受 R157 的实际表调用报42883。
同一调用的零行、纯描述或被跳过 CASE 不能代替 reached-row 验证。
此问题由 R158 的完整1449协议夹具暴露：数值候选两端各18失败，完整门禁均为1。

[Main](../src/main.cpp)的旧字符串投影把 pg_catalog.degrees 整体放入 SelectExpr.funcName，
[旧行适配器](../src/commands/TableManage.cpp)按“已解码的单个标识符”正确引用该字段，
结果成为 "pg_catalog.degrees"(x)，而不是 pg_catalog.degrees(x)。
该字段的生产者与消费者身份契约不匹配，参数列的真实类型和 NULL 位并没有丢失。
不能通过剥去 pg_catalog 或拆分任意点号修补，否则会错误绑定用户同名函数或带点的 quoted 名称。

候选扩展现有完整查询绑定入口到有表来源的、具有真实限定语法来源的标量调用，
保留整个 AST、schema/name、别名、参数、搜索路径、实际列及求值所有权。
仍由既有 shape gate 判定支持范围；aggregate/window/SRF 的原接收者保持，
SRF 资格使用已有纯元数据 owner 查询，不依赖尚未绑定的 setReturning 标记。
不修改原生 SelectExpr 已解码名字、TableManage 适配器、共享头或存储格式。

没有采用最初未编译的 expreval 投影草案：静态复核发现旧谓词捷径会扫描字符串或
嵌套 CASE 中的 IS NULL，且手工列名路径可能漏掉隐式别名。
最终通过完整 AST 绑定，追加9项原始字符串/CASE/隐式别名控制，
再追加16项原有无FROM的限定聚合/DISTINCT/FILTER控制；原612项没有删除。

## 专项证据

[协议夹具](../tests/qualified_projection_routine_identity_protocol_e2e_test.py)固定637项，
与角度极值修复独立：只用0、正负1、NULL等普通数值。
覆盖真实表、限定 builtin、引用的 schema/function、带点/双引号/混合大小写用户名字、
同签名用户函数遮蔽与显式 pg_catalog、多参数表达式、带点表别名、NULL/空串/字面NULL、
Simple/Extended text/binary、原始 OID/长度/typmod/格式、Parse/Bind Describe、
保留 statement 的 text/binary 参数重执行、42883 与错误后恢复。

实际 PG18.6/180006 完整637项0失败；旧Root实际跑过的是621项、216失败，
冻结已接受版重新跑完整637项、216失败；均实际退出1，不能推算未跑的旧Root637结果。
V4候选两端完整637各30失败，其中12项为过严role gate新引入的旧无FROM聚合回归；
V5保留原无FROM分支后两端637各18失败，12项回归已消除。
余下九个执行签名和九个Parse元数据错误分别由R162、R163处理，原失败日志全部保留。
没有使用当前数值候选的通过代替旧版复现。

## 当时的范围与后续

R158数值候选仍须保留原603/1449全部控制，待本依赖独立验收后再运行、单独提交。
常量规划时机、编码边界及原273清单的其余缺口不由本项关闭。
quoted 未限定 builtin 的旧字符串路径已有独立观察：
`SELECT "degrees"(x) FROM ...`在PG执行正常，V4 Root/clean报42883，
见r161-candidate-probes.py及三端完整8条观察；本项只修限定身份，不宣称修复此相邻问题。
用户已暂缓的 [R156 类型化常量](issue-review-r156-regtype-physical-constants.md)、
R151自动枚举数组及其它暂缓项不恢复。

本地证据：/tmp/dbms-review-r46.ifu6SWLp/r161-*。
原273仍为22 complete /166 partial /70 unverified /15 deferred，全部完成的目标未缩小。
不 push，GitHub Actions 保持禁用。

## 最终候选的完整运行检查点

R169 V4工作区完整58入口实际全部通过；本项完整637、共享角度1449均0失败。
V5后的18条执行/元数据错误分别经R162、R163消除，原637全部保留；
无FROM聚合/DISTINCT/FILTER等原所有权控制也通过，没有退回被否决的字符串投影方案。
quoted未限定callee仍属相邻开放问题，不由本项关闭。
独立干净版本亦完整58实际退出0；两端各26native、29协议、三个真实Main全部通过。
组合构建及日志索引见[R169](issue-review-r169-simple-query-raw-syntax.md)；
不把后续依赖到齐的组合通过描述为每个中间提交的独立全量通过。

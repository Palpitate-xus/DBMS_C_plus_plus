# R124：表查询把TIMETZ后缀转换误判为列名

状态：本项NULL后缀路由已修复，96项协议控制和共享48个完整入口均已通过。
本项按独立本地commit管理，不push，不恢复已明确跳过的模块。

## 发现和修改方法

`SELECT NULL::timetz AS z FROM t`在真实普通/TEMP表及WHERE false下应返回
TIMETZ声明（OID1266、逻辑长度12、typmod -1）；无输入行不影响声明合法性。
旧程序却报42703、column "null::timetz" does not exist。Parse/Describe可以
正确绑定，Simple及Extended执行却仍走不同的旧投影路由。

原因是Main的表投影后缀转换白名单已有TIME/TIMESTAMP/TIMESTAMPTZ，却漏了
TIMETZ。只在该既有名单增加TIMETZ，使其沿用现有CAST表达式求值，不把完整
表达式当物理字段。此路由修复不实现完整CAST源类型资格校验或非NULL时间
输入，不额外执行无需求分支、不改heap/binary codec，也不声称完整TIMETZ支持。

## 实际证据

永久`tests/timetz_postfix_projection_protocol_e2e_test.py`96项控制在实际
PostgreSQL18.6（180006）全部通过，固定旧产物完整96项中24失败、实际1。
普通/TEMP、空/NULL、Simple/Describe/Extended text与binary NULL、大小写的
`::timetz`及CAST和TIME/TIMESTAMPTZ邻居均保留；元数据、真实NULL、来源、
Ready状态、完成标签和协议确认分别检查。非NULL TIMETZ binary发送不在本项。

原始日志位于`/tmp/dbms-review-r46.ifu6SWLp/`的`r124-reference96.log`和
`r124-old96.log`。旧程序SHA256为
`dd282eeaf175aea11453fc3fa224cb72e6d906a7a038425bd060e747dd73512f`，
与R121接受的组合产物相同，仍含保留候选，不是clean HEAD全量证明。

该问题由R123原104项schema/协议回归暴露：其第一版782项native全过，而
104项协议仍有六个此类42703。六个断言和失败记录不删除；两项源码分别提交，
共同匹配产物的最终回归需明确标出，不虚构每个commit之后都重跑同一轮。

## 验证要求和未完成边界

Main发生修改，不能复用R120/R121的旧同TU对象。重新编译当前真实Main测试
单元，explain/session的测试TU为O0、window及正常生产库O2；连接57份当前库，
无stubs，再逐个经初始化前隔离器执行。普通生产构建及重复构建、完整专项
协议和相邻入口、双缓存/58 receipt/57 test库字节及源码封印均需真实核对。

R123列精度元数据与R122显式TIME precision值/AST声明分别记录，不能用本项
替代它们。完整TIMETZ输入/源类型资格/时区及存储、原273其余缺口仍普通开放；
原273仍22complete/166partial/70unverified/15deferred，Actions禁用。

## 最终有限验收与版本范围

生产normal/repeat实际重编1/0个CPP且均0；本项Main的真实三个前端TU全部
重新编译并连接57份当前正常库（explain/session测试TU O0，window与生产O2，
无stubs），初始化前隔离执行均0。本次没有借用R120/R121的旧TU对象。
共享R123/R124的21native、24协议、3真实Main共48个不同完整入口全部0，
包括原六个失败断言、104项列精度协议和本项96项；双缓存/58 receipt/57份
test库字节及源码封印的独立终态复核0。它不是新完整注册/TLS/整库sanitizer。

日志为`r124-normal-build.log`、`r124-repeat-build.log`、
`r124-real-main-object-compile.log`、`r124-real-main-link.log`、
`r123-final-{driver,native,core,neighbor}.log`、三个`r123-*-frontend.log`和
`r123-r124-shared-independent-check.log`。此轮同时包含尚待独立提交的R123
四CPP元数据补丁及原保留候选，不能称clean HEAD，也不虚构每个commit后
又编译或跑48次。本项只暂存所属Main一行、96项驱动/登记、记录与状态入口。

冻结程序SHA256为
`e593e92a7ea662f97e8d47f5c277d7deb6e58a54c3d5ee2030c25630077b194b`，
缓存为`26b6ac52029eec3ec463279d16ade9a78e57110697b7d326eeca4125e3246796`，
源/测试/脚本封印为`2c34b2a40cd35c01f1c606d4a506268cb0ecbe44309936c9e7fb96fbaa014478`。
公共头/生产flags签名仍为
`739bf42b71ae53b6f0cfe5e817aec5e81edb16ef16893f6616d93f026f9594c8`。

本项接受544MD、894native+525Python+3Main=1422；工作区548MD、
898+529+3=1430含R123两个待接受运行入口及原六个保留入口。
550Python AST/13Bash、文档本地链接/清单及原273一致性另作静态复核；
原56933字节候选补丁和三个草稿保持字节不变，require-complete仍实际1。

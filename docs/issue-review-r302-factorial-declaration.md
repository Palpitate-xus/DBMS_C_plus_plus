# R302：FACTORIAL 的真实声明与既有 primitive 常量规划

状态：本次修复声明、分析、返回类型、调用角色及既有 primitive 常量规划。
**不是 FACTORIAL 全部问题或完整协议阶段已解决。** 完整夹具仍失败：大数范围
属于用户暂缓的 R271；12 个 Bind 阶段差异仍开放，单列 [R303](issue-review-r303-prepared-constant-planning-phase.md)。
不缩小原总目标，不将未修复问题计为完成。

## 问题、影响与修改方法

| 问题 | 使用影响 | 本次修改与状态 |
|---|---|---|
| 缺少真实声明与回调所有权 | 零参数返回 NULL，多参数忽略尾部；numeric/real/float8/TEXT/boolean/date/array 及 typed NULL 可错误成功 | actual builtin 接入 DeclaredCore，声明 bigint→numeric，复用既有 implicit int2/int4→int8；已修复 |
| UNKNOWN 输入与角色优先级 | `'bad'`、`'5.0'`、`'NaN'`、`'Infinity'` 的 22P02 可被死分支掩盖；DISTINCT/FILTER/ORDER 被忽略 | 整 query 分析及实际签名、codec、scalar 角色检查；保留 FILTER 名称/boolean 优先级；已修复 |
| 前端/无 DB 类型与返回 OID 不一致 | qualified 调用可错误描述为 TEXT，CASE 的 numeric 返回域亦可丢失 | 扩展现有 Main/Network/binder/ExprHelper 声明入口，按实际 callee 推断；已修复 |
| genuine builtin 的不可变性与零行需求 | 负数常量可被 WHERE false/LIMIT 0 掩盖 | 仅实际 builtin+i+numeric 返回者进入已有 primitive planner；输入转换为 bigint，而非 numeric 结果域；已修复 |
| opaque/stored owner 与副作用需求 | 不能因同名就强加 bigint/numeric、arity 或 i，亦不能提前调用序列和 routine | 标记实际 callee，完整 native opaque i/v、stored、序列/变量及 lazy 控制通过；不恢复用户暂缓的 routine identity 模块 |
| 大阶乘 runtime precision | 1661/1662、450/451 在 PG 合法，产品仍 22003 | R271 暂缓，不扩大/旁路 Numeric 域；原生和完整协议保留失败，不算修复 |
| 扩展协议 Bind planning owner | 首次 custom 参数及 literal 坏 Bind 未提前 22003/E，产品却 BindComplete | [R303](issue-review-r303-prepared-constant-planning-phase.md) OPEN；此次不伪造 generic/custom cache 选择或删除阶段断言 |

声明依据：[PostgreSQL 18 数学函数](https://www.postgresql.org/docs/18/functions-math.html)。
实际参考进程为 PostgreSQL 18.6/180006；整数结果另用 Python 大整数阶乘独立
构造，binary NUMERIC 使用独立 base-10000 编码。没有从产品输出构造 oracle。

## 完整固定夹具与当前结果

| 夹具 | PostgreSQL 18.6 | 旧 Root / 旧 pure | 当前 Root / 当前 pure |
|---|---:|---:|---:|
| 最终原生 114 | 不适用 | 各 91 失败、exit 1 | 各 4 失败、exit 1；全部为 R271 大数范围 |
| 完整协议 704 | 0 失败、exit 0 | 各 591 失败、exit 1 | 各 36 失败、exit 1；24 个 R271 场景、12 个 R303 阶段场景 |
| 原同序 69 SQL 重放 | 原参考结果保留 | 各 48 处 rows/state/OID/tag 差异 | 各 2 处，只有 1661/1662 的 R271；其他 67 条相符 |

重复控制不当作独立 bug 数。全部原 69 条 SQL 逐字保留，完整 704 始终保留
大数、空表/真实 typed 行、严格 NULL、三查询模式、text/binary 参数和结果、
未知参数、精确元数据/数据帧、序列纯准备、真实 stored routine、首次 custom
与 warm generic 的不同失败阶段。只说明行为与参考相符的 generic 控制，
不据此推断产品已经有真实 generic-plan cache。

114 包含原 69 条、448..451 边界、typed row、opaque i/v 替换、实参需求、
不可变性、stored owner、序列及 variable planning；当前 110 条通过，4 条域
差异仍完整失败。协议当前 668 条匹配，36 条仍失败，不称全项通过。

生产修改前，协议 V1 因生成 Python `null` 而 NameError；V2 完整 676 有 41
个 oracle 错误：int4 binary 的 42 是字符串、忽略显式事务 Ready 状态，以及
对已变 generic 的参数仍错误期望 Bind 失败。V3 保留原 SQL，修正类型/状态，
同时覆盖 fresh custom Bind 22003/E 与 warm generic Execute 22003/E；完整
704 在 PG 全 0 后才冻结。复查另发现命令 validator 在 notices=False 时也丢弃 Notice；
保留原版，仅加强此断言并通过四个合成正负控制，704 请求/值/错误阶段完全不变。
再完整重跑 PG/旧两侧/新两侧，仍为 0/591/591/36/36；不是按产品结果放宽参考期望。

原生第一版 114 在旧侧各 93 失败、候选各 6 失败，其中两处是测试调用方未按
API 契约先 `prepareExpression` 就 evaluate，真实返回 XX000；不是产品 bug。
仅为 stored/sequence 两处补纯注册步骤，全部 SQL/114 个断言保留；旧版/候选
各重新编译并完整重跑，得到上表 91/4。原版及失败日志继续保留，未隐藏 errata。
早期 wrapper 日志 label 冲突和静态 gate 的错误 CLI 调用也保留为驱动错误，
不计产品缺陷；正确静态与完成 gate 已另有实测。

## 当前匹配构建、整合与仍开放的门禁

六 CPP×两侧共 12 个生产 TU fresh；其余 110 个生产对象及两个 stub 按源码、
全依赖头、MMD、flags、工具链/hash 核对复用。两侧生产 Main 均 O2，六个真实
Main 测试 TU 全部 fresh（window O2、既有 cleanup/session O0），并均运行通过。
新两个 native 测试对象 fresh、其余 42 个匹配 native 对象复用；50 测试程序及
两个 server 新链接。最终接受的 2328 个源文件必须与 tested pure 逐 blob 对应。

正常完整 80 入口实际 74 exit 0 / 6 exit 1：两侧原 MIN602（各 2 个 R271）及
新 114、704 仍失败。原 R300 的 76 个入口完整保留；除上述 MIN602 外，原
native/Main、5244、2133、416 等完整回归均 0，没有缩小原 SQL/断言/期限。

专项 UBSan 为 Evaluator、Planner 和三个夹具共 10 个 fresh instrumented TU，
六个当前程序完整运行，无 UBSan 诊断；其中四个旧 GCD 程序 exit 0，新两个
114 因同四处 R271 功能断言仍 exit 1。不是 sanitizer 全绿功能验收，也不是
整库 sanitizer；其他库使用正常匹配对象，Main 未 instrument。合计 86 个有限
运行记录为 78 exit 0 / 8 exit 1，匹配构建门 0、完整运行门 1。

990 native + 604 registered Python + 3 Main = 1597 只是当前登记数量，不是
1597 全量运行。原冻结 1492 的 1481/11 结果不改写；原 273 条仍为
22 complete / 166 partial / 70 unverified / 15 deferred，总目标未完成。

只修改六 CPP、注册、两个夹具和本次记录；没有 header、Numeric/PostgresNumeric、
parser grammar、Catalog/TableManage、generic/cache-policy 或角度函数修改。
保留 Root 外来草稿、11 个隔离映射及十八处暂存差异。R301/R160、R298、R271、
R294、R291、R288/R99、R222、R51、R267、R156 完整常量、R205、R217 及所有
其他用户暂缓范围不恢复。仅独立本地 commit，不 push，Actions 禁用。

临时证据目录 `/tmp/dbms-r302-factorial-declaration.4Cyw35mU` 不保证长期保存。

| 完成收据 | SHA-256 |
|---|---|
| completed-preflight-receipt.json | `40596d7bce64910ad351dbe3eb7ff8dee52526d351db1a40eb7b1ab7cc7aaa79` |
| completed-reference-v3-receipt.json | `ac427f72674a2bdcf68f1c9193ad097369ddd0174039a1c8a12a7e83d5818f7a` |
| completed-fixtures-frozen-receipt.json | `0f603b0175733f678df2decb40bff5a16e7870ec42436f062aba181a8375c463` |
| completed-native-fixture-api-correction-receipt.json | `6f370275997c6d367973ef3c727e2ce1766b12ed986fbdf08fed91450b211bd0` |
| completed-corrected-native-current-build-receipt.json | `21e7930d2a975f0aa72ccee80344f295f59a3b9833d5832066bac88cdba1ca36` |
| completed-protocol-framing-correction-receipt.json | `dcfc16b52edf9ca67913973336bfea44e899c2b90721d10dbd3e50a33151a5cd` |
| completed-strict-notice-full704-receipt.json | `6b31786225c7cb3809408dc5d219e41a7c44ce6cbd1fc0edecb21f12e2461f79` |
| completed-framing-corrected-runtime-build-receipt.json | `5f20e6de5eaef64265b9a82671707c14256dffe0cb0c29ef4bb5d5b1dbabdf82` |
| completed-framing-corrected-candidate-receipt.json | `4f40d20a097a7676de14df84f7ac4c2b407425dca68f66af97c0cc5b86a310db` |
| completed-six-main-test-objects-receipt.json | `89a326cd60bc2c8c78825947eecb02f9ce750e408d9e141bc8d44c5e53ea714e` |
| completed-regression-build-receipt.json | `2524450e257f796938cd3642793a1555f86a7c7257ac5a708729ac1b73df4a43` |
| completed-candidate-receipt.json | `5e5082152633a4ff5632f94b3b2af4bd65e6447e9cbffb582d553fda6d57e19c` |
| completed-scoped-ubsan-receipt.json | `e54ca5c70e7f7a710474927f7bb6afad160253915f0ea177af61f4febc4b936a` |
| completed-post-observations-receipt.json | `a8547d9f99e316049bb1b8f63a21ac91c793d983fa1129800a8b80d357ba86b0` |

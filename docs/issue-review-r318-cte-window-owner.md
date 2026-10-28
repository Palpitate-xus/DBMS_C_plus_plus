# R318：CTE 来源的窗口执行丢失关系所有者

状态：R318 窗口来源作用域修复已通过以下有限验收，独立本地提交；R320、R319 与原 273 总清单仍未完成。

下文按审查、V1/V2 候选及 V3 修正的顺序保留历史证据；旧段中的
“尚未验收”等描述记录当时状态，当前有限结论以末尾 V3 验收为准。

R317 的完整 800 项保留了两种函数在 Simple/Extended/二进制输出下的六个
CTE 请求。PostgreSQL 18.6 成功返回六行，旧 Root/accepted-only 均返回
42P01。例如：

```sql
WITH w AS (SELECT id,p,v,n,d FROM r317_w)
SELECT lag(v,1,2147483648) OVER(PARTITION BY p ORDER BY id) AS v
FROM w ORDER BY id;
```

初次记录的源码线索：Main 的 `windowProtocolType` 为取得 LAG/LEAD 共同结果类型，
拼出独立 `SELECT <window> FROM <tnameOrig>` 并重新绑定。逻辑 CTE 名离开
当前 query 的来源/继承绑定后可能被当作实际 catalog 表；最初只视为源码推测。
后续独立运行、部件验证及最终生产修复见下文。

修改方向：让窗口接收端使用当前实际 bound query 的输出和 sourceRange，
或在合法的继承/物化映射下取声明，不孤立重解析原逻辑 FROM 名；保留真实
CTE 遮蔽、作用域、类型和 NULL 行。不得用把名字改成物理字符串、忽略 42P01
或固定返回空结果来绕过。若需公共头/owner 结构修改，必须完整重建受影响 TU。
不恢复用户已暂缓的通用 CTE Describe/CASE label 或全局缓存生命周期改造。

证据：`/tmp/dbms-r317-window-offset-audit.vcBaloAt/`，完整 800 项的索引
773、778、783、788、793、798。原始输入、PG 帧和严格失败全部保留；
没有生产修改、验收或通过声明。须独立复现、修复、验证和 commit。
原 273 总清单未完成，不 push，不启用 Actions。

## 扩展独立核实

完整参考矩阵使用独立 PostgreSQL 18.6、Root 源码投影和纯提交源码投影，
各运行 673 请求；随后从参考端生成冻结 oracle，并由同一严格协议夹具
重新跑完整三侧。参考 673/0 失败，两个旧产品各 673/545 失败。检查包含
Simple、Extended、二进制输出的完整帧顺序、ParameterDescription、
RowDescription 原始字节、DataRow 字节、命令标签、SQLSTATE 和 Ready。
错误消息要求有效 ERROR 字段和 UTF-8，不要求不同实现的错误文字相同。

覆盖 LAG/LEAD、FIRST_VALUE、NTH_VALUE、SUM、COUNT、ROW_NUMBER、
NTILE；物理基表、普通 CTE、别名、列别名、依赖前一 CTE、遮蔽物理表的
同名 CTE、quoted 名称、derived 来源；正常行、WHERE false、LIMIT 0、
named window、多窗口输出，以及 CTE 不泄漏到下一条语句的控制。

545 个差异不能全部称作本项 bug，也不是 545 个独立 bug：

| 每侧断言数 | 实际差异 | 状态/归属 |
| --- | --- | --- |
| 420 | 参考成功，产品报 42P01；每种协议模式各 140 | 本项 CTE 窗口执行所有者，OPEN |
| 12 | 基表 LAG/LEAD 共同结果类型在 Describe 中由 BIGINT 错为 INTEGER | R317，用户已暂缓 |
| 54 | 同名 CTE 查询已有正确行，但两种 Describe 返回 NoData | R217 通用 CTE Describe，用户已暂缓 |
| 54 | derived 查询已有正确行，但两种 Describe 返回 NoData | R156 完整 presentation，用户已暂缓 |
| 3 | 物理列描述中的来源身份不同，值和类型等其余字段相同 | R226，用户已暂缓 |
| 2 | 作用域外 CTE 的 42P01 时机不同，产品多发布 Parse/参数描述 | R294 通用 Parse，用户已暂缓 |

完整 673 项全部保留，没有删掉这些 125 项、改 oracle、跳过断言或把它们
算作通过。R318 与 R319 不随 R317 的 skip 自动暂缓。

## 绑定链路的部件证据

新增 OUT-only 原生控制夹具在旧两侧各 47/0 失败。它不是修复后的通过证据，
而是区别当前全树绑定器与错误调用方式：

1. 完整 `WITH c AS(...) SELECT <window> FROM c` 对上述九种调用均能得到
   正确类型/OID，并保留指向真实 CTE statement 的 sourceRange 和六列描述。
2. 相同主体被孤立成 `SELECT <window> FROM c` 后，没有 CTE 声明，确实报
   42P01；不能从调用者的执行期 QueryCteFrame 自动获得 relation metadata。
3. 物理 `w.v` 为 TEXT、同名 CTE `w.v` 为 INTEGER 时，全树 LAG/LEAD 仍正确
   为 BIGINT；孤立主体却选择物理 `w`，报 42883。仅“同名表存在就不报错”
   不能证明窗口绑定正确，也不能按物理表存在与否猜作用域。

源码中不仅 `windowProtocolType` 会生成脱离所有者的 SELECT，窗口参数准备
及窗口标量排序准备也有同一方式；三处都直接调用 catalog-backed
`prepareBoundQuery`。已有 `resolveTableName` 消费执行期 CTE frame，但该
frame 不会自动进入独立 SQL 的纯 metadata relation callback。部件控制与
实际失败共同确认了这条所有权丢失链路；尚未记录逐调用点运行跟踪，也未
证明三个调用点在每一条失败中都会执行，不能把所有差异只归到第一处。

## 修复与验收计划

1. 让查询/CTE 执行边界保留实际 `PreparedQuery`、当前 statement 和来源描述；
   参数、输出 metadata、窗口键及外层排序消费这些实际绑定节点与 sourceOrdinal，
   不从逻辑关系名重新创建一个没有父 CTE 的 SELECT。
2. 保留真正的继承/遮蔽边界、quoted 名称、列别名、共同结果类型、NULL、空行
   及多窗口位置；不能按名字替换为物理字符串、吞掉 42P01 或伪造空结果。
3. 覆盖当前扩展矩阵，并补充窗口位于依赖前一 CTE 的子 CTE 内等继承 owner
   场景。当前 673 项含“来源依赖前一 CTE”，不等于已测试所有嵌套窗口 owner。
4. 若 Main 改动，必须重新编译真实 Main 和六个相关 Main 测试 TU；公共头
   若改动则按完整依赖/ABI 重建，不能用旧对象假称验证新实现。
5. 跑原已接受回归、窗口族和 scoped sanitizer；逐项比较失败身份，冻结
   oracle 不动。修复独立 commit，不夹带用户暂缓 R317 或其他工作区草稿。

## 证据与版本边界

新证据目录：`/tmp/dbms-r318-cte-window-owner.uiI6VnsY/`。
`completed-observation-comparison-final-receipt.json` 保存全部原始差异，
`completed-observation-component-classification-receipt-v2.json` 保存逐项归属，
`completed-independent-golden-frozen-receipt.json` 保存参考来源及 SHA，
`completed-observations-receipt-fixture.json` 与原始日志保存复验退出码。
`completed-old-native-receipt.json` 及日志保存两侧 47 项控制的实际执行。
首次比较在纯副本日志尚未结束时被完整性断言拒绝，未写比较结果；等待其
673 项确实结束后才生成最终比较，未更改输入或放宽完整性检查。
分类 V2 进一步把 108 个 NoData 分成 54 个 CTE 与 54 个 derived，分别明确
其既有暂缓范围；只改分类说明，没有改任何请求、期望、差异或退出码。

生产仍为 R316；本次仅更新审查证据和修复方法，没有生产/头/永久测试/注册
修改，OUT 夹具尚未接入。没有修复验收或 allgreen 声明。R317 用户暂缓候选
仍保留在其临时目录，未恢复。原 273 总清单状态未改，总目标仍未完成。

## 实施候选（未验收）

补充嵌套 CTE、同名物理 TEXT 来源、继承/遮蔽和窗口标量排序后，独立参考
747 项已冻结；原 673 项的请求和预期字节完全不变。当前候选给纯绑定入口
增加显式、query-local、只复制 relation metadata 的 provider。窗口的三处
准备入口从当前 Session/database/继承边界内的真实 CTE materialization
读取描述，逻辑 SQL/别名不改写，未安装全局解析器、未执行查询来猜类型。
普通准备仍走原 catalog 规则，schema-qualified 来源不被 CTE 遮蔽。

修改涉及 TableManage 公共头、实现和 Main，故必须完整重建 122 个生产 TU、
stubs、全部相关原生和六个真实 Main 测试 TU，不复用旧 ABI 对象。尚未完成
构建或修复后运行验证；R317 和其他用户暂缓项均未恢复。候选没有验收/修复
提交，不能把本段或部件测试的通过当作数据库运行问题已解决。

## 实施复查与 V2 候选（未验收）

V1 直接把 query-local lookup 合并到通用 catalog relation 回调。复查发现
视图的独立绑定会复制这个回调；14 项独立部件模型在两侧各有 6 项明确
视图边界失败。它是 V1 回调组合的控制模型，不是已运行的新服务器结果。
已主动终止 V1 的未完成构建（143），保存其源码和部分对象，不作验收。

V2 在 QueryBindingMetadata 中新增独立 queryRelation 角色：真正的局部
WITH 先解析，再允许当前查询继承 execution-owned materialization；
进入存储视图时复制 metadata 并显式清除这个查询局部角色，catalog 规则
和其他类型/函数声明照常保留。Main 的三处适配和 747 个请求/期望不变。
新增冻结控制夹具默认包含 14 个纯绑定和 5 个真实 StorageEngine/存储视图
检查；修复后还未运行，不可据此宣称 19 项通过。

初次模型还包含一个不相关的裸整数字面量 OID 假设；构造器未提供对应声明
时它实际为 0。后续把局部 WITH 控制加强为读取真实 public.c TEXT 描述，
与继承的 INTEGER 明确区分；模型共 14 项、6 项视图边界失败，两侧一致。
初始 14/7 日志、修订后的 14/6 日志和源码均保留；未修改 747 项参考 oracle。

现在涉及 Main、TableManage、query_binding 三个 CPP 和两个公共头；必须
重新全量构建 122 个生产 TU、两个 stubs、所有相关 native 和六个真实 Main
测试 TU。不能使用 V1 的部分对象或旧 header ABI 作为 V2 通过证据。

## V2 运行检查与 V3 修正（未验收）

V2 已实际完成 122 个生产 TU、两个 stubs、86 个 native 和六个真实 Main
测试 TU 的全新构建；两侧早期 native 47/27/19 均全 0，严格 747 项各有
465 项失败、无夹具异常，未通过验收。Root 完整原始观察 747 项结束；
纯副本原始观察在 510 项后一次 15 秒 socket timeout 中止，不能称它完成。
其后同一默认超时的严格夹具两侧完整 747 项、均无 exception，全部日志保留；
V3 仍须完整重新观察与回归，不能扩大超时或把超时算成 metadata 暂缓。

运行发现继承上层 CTE 的窗口子 CTE，在到达三个窗口适配器之前还会经
前置声明绑定；该处孤立 catalog 准备同样丢失来源。V3 把真实窗口 AST
的前置准备统一到同一个 query-local 入口，普通 scalar/DEFAULT 路径不变；
没有恢复 R317 输入推断、通用分析器或常量规划改造。

另有独立 derived-WITH 分派问题：不含任何窗口的 14 个控制中，参考无
SQL 错误，两个产品各六个 42P01。故不能把它归为窗口 metadata 修复已完成
或用户暂缓项，详见 R320。747 中相关 12 个严格失败仍保留为 OPEN。

V3 只再改 Main，两个公共头与 V2 逐字节相同。允许按完整源码/头/依赖/
编译器/flags 匹配复用 V2 的 120 个生产对象、两个 stubs 和 86 个 native
对象；重新编译两侧 Main 和六个真实 Main 测试，重新链接。不能称这一步
又重新编译了全部 122 个生产 TU，也不能混入 V1 的旧 ABI 对象。

## V3 最终有限验收

普通矩阵完整 208 条运行记录：194 条退出 0、
14 条非零；scoped UBSan 完整 92 条，
82 条退出 0、10 条非零，未出现
UBSan 诊断。保留此前全部 196 条输入，原 20 条非零的失败身份逐项不变，
仅对 factorial 测试自己生成的 schema UUID 在 SQL 标签内作等价命名，
不改任何实际/期望报文字节。普通及最终完整 sanitizer 复跑中，旧窗口
36 条 wire 基线均完整全 0；native 47/27/19 在普通和 sanitizer、两侧
均完整全 0。该表述不把首次运行的超时改写成通过。

首次 scoped UBSan 的 92 条记录另行原样保留：
80 条退出 0、12 条非零。除既有失败
与本次保留的严格协议差异外，Root 的 window_internal_namespace 和纯副本
的 window_frame_boundaries 各在普通 CREATE TEMP TABLE 处发生一次默认
socket 超时，均没有 UBSan 诊断。随后两侧这两个程序共
4 次相同二进制/夹具/默认超时的针对性复跑全通过，
又保留完整 92 条相同输入、参数、顺序的 sanitizer 全组复跑。
本文开头 92 条验收主矩阵指这次完整复跑，不覆盖首次失败日志。
全部实际运行含普通、两组 sanitizer 及四次针对性复跑，共
396 条记录。没有修改产品、夹具、断言或超时来
取得复跑结果；不声称已确定或修复超时根因，也不声称全套稳定性已获证。
R51 全局存储缓存/生命周期/后台清理仍按用户要求暂缓。

两个公共头在 V2 修改后，122 个生产 TU（Root 62、纯副本 60）、两个
stubs、86 个 native 和六个真实 Main 测试 TU 均已全新构建。V3 头文件
逐字节不变，仅 Main 再改：两侧 Main 重新编译，120 个生产对象、两个
stubs 和 86 个 native 对象按源码/头/编译器/flags/MMD 完整匹配复用；
六个真实 Main 测试再次编译，92 个程序和两台服务器重新链接。窗口 Main
保持 O2，cleanup/session 保持既有 O0。没有复用 V1 的旧 ABI 对象，也
不把 V3 的 2 个新编译伪称为另一次 122 个全部重新编译。

scoped UBSan 当前全新编译 56 个 TU：每侧九个核心含本次 Main、
TableManage、QueryBinder，以及 Parser/Output/DDL/Network/Evaluator/DML；
另有 Planner 和 18 个 native。真实 wire 服务器的 Main 已插桩；其他库
和 stub 为当前匹配的普通 ABI 对象，不宣称全库或六个 Main 测试都插桩。
92 条 sanitizer 运行保留旧 40 条全部输入，没有复用旧头插桩对象。

747 项严格协议夹具保留全部请求与独立 PG18.6 预期。源代码投影的当前
完整观察结果如下，数量均来自实际报文：

| 投影 | 请求 | 严格差异 | 原执行错误已恢复为参考状态和完整行值 | 独立 OPEN R320 |
| --- | --- | --- | --- | --- |
| root | 747 | 457 | 477 | 12 |
| accepted | 747 | 457 | 477 | 12 |

除明确记录的 R320 请求外，所有请求的 SQLSTATE、DataRow 原始字节、
NULL、命令标签及 Ready 与参考一致。R320 是未修复、未被用户暂缓的
derived-WITH 执行问题，不能把全部非零归为暂缓，下一步须独立修复。
剩余严格差异逐项保留如下：

| 投影 | 归属 | 当前差异数 |
| --- | --- | --- |
| root | R156_user_held_full_derived_presentation | 54 |
| root | R217_user_held_full_CTE_Describe | 374 |
| root | R226_user_held_physical_origin_identity | 3 |
| root | R294_user_held_general_Parse_phase | 2 |
| root | R317_user_held_LAG_LEAD_common_Describe | 12 |
| root | R320_OPEN_NOT_HELD_derived_WITH_dispatch | 12 |
| accepted | R156_user_held_full_derived_presentation | 54 |
| accepted | R217_user_held_full_CTE_Describe | 374 |
| accepted | R226_user_held_physical_origin_identity | 3 |
| accepted | R294_user_held_general_Parse_phase | 2 |
| accepted | R317_user_held_LAG_LEAD_common_Describe | 12 |
| accepted | R320_OPEN_NOT_HELD_derived_WITH_dispatch | 12 |

普通及 sanitizer 的完整 747 项日志还逐帧对照当前已分类观察，不能删除
R320 或将缺少 Describe、来源身份/Parse 阶段差异算作通过。跨私有数据库
物理来源 OID 只在明确的 R226 范围分类，原始日志、帧和测试期望不重写。
执行恢复后出现的 NoData 描述差异不能伪称与旧失败完整报文完全相同。
R317 继续按用户 skip 暂缓，R319 外层标量窗口接收器仍 OPEN；本项不等于
完整 QRY-08、通用分析器、constant planning 或完整视图物理身份已完成。

Root 与纯提交副本的版本、文档、兼容契约、总账、测试注册检查均通过；
`--require-complete` 仍失败。原 273 为 22 complete /166 partial /70 unverified /
15 deferred_by_user，不改写冻结历史 1492 的 1481 PASS /11 FAIL 为当前全绿。

当前证据目录：`/tmp/dbms-r318-window-scope-v3.b9U5ViEz/`。关键文件为
`completed-runtime-build-receipt.json`、`completed-regression-build-receipt.json`、
`completed-candidate-receipt.json`、原始 `completed-scoped-ubsan-receipt.json`、
`completed-unchanged-window-timeout-recheck-receipt.json`、
最终 `completed-scoped-ubsan-full-runtime-recheck-receipt.json`、
`completed-current747-component-classification-receipt.json`、
`completed-current747-normal-san-wire-proof-receipt.json`、
`completed-current747-normal-san-full-recheck-wire-proof-receipt.json`、
`completed-classified-candidate-receipt.json`；V2 的全量公共头 ABI 构建证据在
前一目录保留并按 SHA 引用。独立提交只接受纯副本的本项差异，保留全部
外来工作区及原 18 项暂存变更。不 push，不启用 Actions，总目标仍未完成。

## 提交前的一字节格式规范化

Git 差异格式检查指出新增的 `prepareWindowSourceQuery(s,` 调用行末有
一个多余 ASCII 空格。提交源仅移除这一个行尾空格，不改行数、表达式、
字面量、SQL、断言或任何公共头。为保持原始编译证据不变，实际构建和
运行使用的两个冻结源码投影、对象、MMD、服务器、测试及日志全部原样
保留。提交中的 Main 因此不声称与原运行源逐字节相同。

真实 GCC 使用原参数分别预处理两侧生产 Main、两侧 UBSan Main 和六个
真实 Main 测试 TU，共十个编译变体；前后各一次，二十个命令退出 0，
每对完整预处理 TU 的 SHA-256 完全相同。仅将临时克隆文件的宏路径映射
回原文件名，并去掉预处理行标记用于比较；未增加语义宏、改变优化等级
或删除程序令牌。原文件的行数与各令牌坐标不变，编译证据按该一字节
词法等价关系复用，不把它说成又重新编译或重新运行了完整矩阵。

`completed-one-trailing-space-preprocessor-equivalence-receipt.json` 保存
全部实际命令、前后输出与源码 SHA。提交源映射和原运行源映射分别保存，
只有 Main 的这一字节不同；其余 2350 个 src/tests/scripts/cmake 文件与
运行证据逐字节一致。最初被格式检查拒绝的候选索引与日志也保留，随后
对规范化候选重新执行 Git 差异格式检查及既有文档/总账门禁。

# R340：普通 VALUES 的共同声明修饰符

状态：下列普通 VALUES 协议声明适配问题已修复并完成有限验证。
不是整个 VALUES、所有协议类型或原 273 项总清单全部完成。

| 问题 | 原因 | 修改方法 |
| --- | --- | --- |
| BIT(n)、VARBIT(n)、VARCHAR(n)、CHAR(n)、NUMERIC(p,s)、时间精度等共同声明被描述成无 typmod | 协议 VALUES 分支只保留共同类型字符串，丢弃绑定器已经计算出的共同 typeMod；另一处投影 CAST 扫描不拥有 VALUES 共同声明 | 复用现有纯类型绑定输出，让 Parse/Describe 和执行结果描述共享真正的共同声明 |
| 相同修饰符应保留、不同修饰符应退化为 -1 | 按单个 CAST 或结果字符串宽度推测无法表达共同规则 | 只复制真实 OID 匹配的绑定字段 typeMod，包含有效的 -1；不借其他字段身份 |

实现只在 NetworkServer.cpp 的普通 VALUES 描述路径。
不运行查询计划或值来选长度；不改变 OID、列名、来源、typeSize、格式或参数。
仅接受现有标量角色，CASE、子查询、CTE、窗口、排序、集合操作等原有拥有者不变。
类型探测的 22/42 类、3F000、0A000 错误回到原有消费者处理，不移动旧错误阶段；
I/O、中断、损坏等其他错误继续传播，不使用通用异常吞掉它们。
不修改 Main、公共头、binder/evaluator、存储或规划器，不恢复用户已跳过的模块。

## 实际验证

在本机实际 PostgreSQL 18.6 全新私有 cluster 先冻结 1428 请求参考，再严格
重放，实际 exit0。原 1239 请求及其 golden 是精确前缀；更早 1133/965/777/304
也从实际完整日志核验，不把前缀算成额外独立运行。
新增 189 请求覆盖 simple、extended、binary、Describe-only，等／不同 typmod、
NULL／UNKNOWN、负 NUMERIC scale、引用物理类型、多列及 typed 参数。
协议逐字段对比消息顺序、SQLSTATE、RowDescription、ParameterDescription、
文本／二进制 DataRow、command 和 ReadyForQuery，不只比较输出文本。

| 检查 | 修改前 | 最终结果 |
| --- | --- | --- |
| Root projection 完整 1428 | 222 个请求有差异 | 121；101 个原失败消除 |
| accepted 完整 1428 | 264 个请求有差异 | 163；相同 101 个失败消除 |
| 原 1239 前缀 | Root89 / accepted131 | Root74 / accepted116；其中 3 处 CHAR 现在只剩数据错误 |
| 原 1133 / 965 前缀 | Root65/61，accepted107/65 | 完整签名不变；777/304 仍各 55 差异 |
| 新增 Describe-only | 包括原来错误的声明 | 全部 47 请求匹配真实 PG，包括独立执行尚不支持的类型 |
| 实际签名变更范围 | — | 两侧相同 133 请求，仅 RowDescription 的 typeModifier 字节改变 |

没有新增失败索引，所有先前正确的完整签名不变。133 个改变请求中并非每个
都完全转绿：部分只修正描述，独立的值／执行差异保留。所有数据、错误码、
参数描述、格式和协议消息顺序在本项前后精确相同。
序列先 nextval 得 1，Describe-only 的 VALUES(nextval(...)) 后 currval 仍为 1；
三次真正执行后分别为 2/3/4。不据此宣称所有错误阶段和回滚语义都正确。

原始配对回归实际 200 次，176 次 exit0、24 次非零。其中 Root common168
当前版出现 socket 超时，后续响应错位；最终严格签名门禁实际发现并拒绝了
这份异常日志。保留原日志，以相同二进制、驱动、请求、golden 和默认超时
重新运行该组前后各一次，168 条完整签名与原先 before 完全一致；这两次也
因原有功能差异 exit1。原始异常不改写成通过；共实际 202 次配对／重放观察，
最终选取的完整 200 格配对矩阵为 176 次 exit0、24 次已知非零。
74 个当前 native/Main
fixture 对象逐源／头匹配复用，但全部重新链接当前 Network 并实际运行；
8 个真实 Main fixture 不链接 test_stubs。R332 数组函数声明和 R339 CHAR
值的原生失败完整保留；13 个非前缀协议族前后完整期望行一致。
旧 630/CHAR131/common168 的差异不变，没有删断言或改参考使它们变绿。

局部 UBSan 实跑 30 次，16 次 exit0、14 次已知非零，无项目 UB 诊断；
完整 1428 及其他协议输出与普通版一致。同一 libubsan 的故意溢出 canary
实测持久 log_path 捕获有效，不是以空日志推断检测器工作，也不是全工程 UB 证明。
正常生产编译保持 O2。局部 Main 和包含 Main 的测试沿用明确 O0，其他插桩
对象 O1；每个对象实际 flags 单独核验，不伪称统一 O1。

普通版新编 Network 2 个生产 TU，匹配复用其他 120 个 TU 和 2 个 stub；
74 个测试对象匹配复用，74 个当前原生链接新建。局部新编 Network 2、匹配
38 个插桩对象，共 40 对象用于 18 原生及 2 服务器链接。实际 238 份 MMD、
源／头／对象、编译器与完整 argv 均核验；源码清单数量不等于运行覆盖。

## 保留的独立缺陷与后续方法

新增 189 请求仍有相同 47 处差异，全部属于执行，不再有 Describe-only 差异：

| 事项 | 实际证据 | 后续修改方法 |
| --- | --- | --- |
| R339 CHAR 共同值转换 | 新增 6 请求只差 data；原 1239 中另有 3 个，原生仍 211 断言 1 失败 | 单独修正普通 VALUES 中 keyword CHAR 默认 CHAR(1) 的截断，使用真实无 typmod 共同输入，保留内层显式 CHAR(n) |
| R327 时间输入／转换 | 新增 11 请求 42846，非空 timestamp/timestamptz 和 typed 参数受影响 | 独立追踪输入 codec 与共同值转换；不与本项描述修复混合 |
| R341 TIME/TIMETZ/INTERVAL 的 VALUES 执行未支持 | 新增 12 请求 0A000；包括 NULL::time；只描述已正确 | 独立核验并扩展普通 VALUES 执行的类型准入及值表示，不借此恢复暂缓规划模块 |
| R342 ARRAY 的 VALUES 执行未支持 | 新增 18 请求 0A000，含字符／位串／NUMERIC 和 typed NULL 数组；只描述已正确 | 独立核验数组共同类型及行结果载体，添加真实执行／协议用例，保持用户暂缓的数组列写入和数组函数不动 |
| 既有报错阶段 | 原 1239 中 6 请求仍有消息／参数描述差异，SQLSTATE 正确 | R244/R303 等依用户暂缓，不借此恢复 |

R332/R334 草稿及全部更早跳过项哈希不变。本项仅提交 network、测试注册、
协议驱动、冻结 PG 参考和本文共 5 路径；保留其他工作区修改和原暂存 18 路径。
没有 git push，没有启用 GitHub Actions。原 273 清单仍未完成，完成门禁实际
exit1；历史 1492 的 1481/11 不是当前 HEAD 重跑结果。

运行：python3 -B tests/values_declaration_protocol_e2e_test.py（设置 DBMS_MAIN
为目标服务器）。严格驱动仍完整报告保留差异，不把本项接受条件冒充全绿。
有限验证证据：/tmp/dbms-r340-values-declaration.xrab6dV3/。

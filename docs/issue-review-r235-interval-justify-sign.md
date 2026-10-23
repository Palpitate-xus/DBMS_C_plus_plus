# R235：INTERVAL justify 的混合符号、进位与表行一致性

状态：本项完整985原生、997协议及126回调身份检查在Root与纯提交投影均通过；总清单未完成。
对应TYPE-06/FUNC-01。本项不恢复用户暂缓的R238后续或其他模块。

## 复现、原因与修改方法

| 缺陷 | 影响 | 修复 |
| --- | --- | --- |
| `justify_interval`取days/time绝对值再套months符号 | `1 mon -1 day`错误成为`1 mon 1 day`，不守恒 | 逐字段进位与异号借位，保留原间隔值；不通过整体重贴符号实现 |
| hours/days缺少相邻字段借位 | `1 day -1 hour`不能规范化为23小时；`-1 mon 1 day`不能变为-29天 | hours仅规范day/time，days仅规范month/day，interval规范全部三字段 |
| calendar字段按int64校验，且跳过中间溢出 | 超出实际int32字段的结果被当正常值返回 | 宽中间量检查int32范围后窄化；真实进位溢出报22008，再做符号借位，不能重排掩盖溢出 |
| 极值/infinity与本地有符号取反 | infinity变NULL；INT64_MIN微秒渲染可能溢出 | infinity原样返回；使用共享无符号幅度formatter，保留微秒与所需正号 |
| 表查询另有旧文本解析/归一化 | 丢微秒、重复处理负号、NULL变零，表行与常量/参数结果不同 | 删除该重复实现，带原始字段类型和heap NULL信息委托同一表达式计算器；保留SQLSTATE |
| 三个函数缺少静态返回声明 | prepared Describe/空表元数据把结果报为TEXT OID25 | ExprHelper仅补充三个justify函数固定INTERVAL声明；不执行参数，不更改常量载体/CASE规划 |
| 返回声明不能仅看同名拼写 | 原生替换可能返回TEXT/integer/BYTEA且有自己的arity/volatility | 私有回调标记只赋给真实内置实现；替换仍为unknown声明/native身份，分析不执行callback，不强转其结果 |

`justify_interval`在day/time同号时先把整30天进month，避免随后day进位发生本可避免
的int32溢出。month进位必须先校验再借位，例如最大month加30day减1微秒仍应报22008，
不能用最终数学值可表示作为理由接受它。days模式不修改time字段，包括INT64_MIN。

依据本地PostgreSQL18.6实际完整997协议结果，以及同版本timestamp.c中三个
`interval_justify_*`函数。新实现没有改动AST/公开头、磁盘布局或wire编码格式。

## 计划与实际验证

| 步骤 | 执行结果 |
| --- | --- |
| 保存独立回归夹具并核实参考答案 | 同一997协议对私有真实PG180006全部通过，默认fsync，正常停止 |
| 在最新父提交重新取得旧基线 | Root/纯投影各985中269失败、997中855失败，全部断言均达到 |
| 修复计算器、表行委托与返回声明 | V3两侧各fresh编译三个CPP；V4再fresh evaluator与两项native，复用核对过的V3其他对象，新链接服务器 |
| 执行未删减新夹具 | Root/纯投影各985/0、997/0和126/0，退出0 |
| 相关回归与暂缓差异完整保留 | 最终62次完整程序60退出0；两次原R238完整2863仍各114失败，失败标签集合完全相同 |
| 文档与版本管理 | 独立index仅提交本项九路径；提交前后核对源码投影、其他工作区与原暂存18不变；不push |

原生矩阵为315组month/day/time乘三函数，独立守恒正规形oracle和幂等性，另有严格
NULL、infinity、int32/int64完整端点、中间溢出、预进位、参数单次求值与native替换。
协议同一315组覆盖Simple及真正OID1186的text/binary Bind，检查精确16字节字段、
结果OID/长度/typmod/格式、Describe不执行、NULL/infinity/端点/错误及真实表行和空结果。

最初参考验证的两个月边界期望有误：实际PG先报22008再借位。已在旧基线前仅更正
两条错误期望，SQL和985/997控制不变；V1源码、失败日志、V2修正与完整PG证据保留。
最新父提交旧基线仍复现269/855，不能把此参考答案修正计为产品修复。

回归每侧9个fresh原生：R238完整696、field literal、assignment、typed range、
arith、binary result、calendar order、column modifier、simple CASE constant demand；
另19个完整协议含原R2382863、R237360、R236143以及136 cast input、字段/列/范围/
SQLSTATE/存储/INSERT SELECT、CASE、binary array及TIME/CHAR/BIT。R237360和R236143
两侧仍全部通过。60个通过的程序不代表整个测试清单通过，更不是原273项全部完成。

V3已完成原985/997及完整60次程序（58退出0），但独立复查126回调检查两侧均126失败，
显示还缺少内置结果声明的实际callback身份边界，未将V3提交或称为最终验收。V4保留
旧检查与原985/997不变，仅补充标记与回调身份保护；126包含三个函数、普通/quoted/
catalog限定拼写、NULL、TEXT/integer/BYTEA opaque返回、两种自有arity和volatility，
断言分析/绑定零次执行与实际恰好一次执行，最终两侧126/0。不是R156全载体重做。

## 用户暂缓与验收边界

最新用户要求“skip it”已应用于上项剩余R239/R240/R241：37个CAST分析/错误阶段、
75个常量CASE planned typmod、2个NAME参数差异均保留且不恢复，合计不是114个独立bug。
本轮仍执行原完整2863以检查回归，两个完整驱动均如实退出1；不删断言、不更改gold，
未借本项恢复R156完整载体、R217标签、R214 NAME物理DDL、R106地址CAST或其他暂缓。

Root62生产对象/134头与纯投影60/131分别匹配，Root Main仍使用pre-R229匹配donor，
不编译或运行当前跳过的Main草稿、默认旧dbms_main。不是normal/repeat、所有CPP
fresh、全工作区源码、全套、TLS或sanitizer验收。2264纯源码输入与实际测试投影逐字节
一致；946 native+594 Python+3 Main=1543仅清单计数，本轮没有全跑1543。
原冻结1492和原273历史未重写，`--require-complete`仍应退出1。Actions保持禁用。

## 本地证据

初始/PG证据：`/tmp/dbms-r235-interval-justify.tDCn43bJ`。
首次重建及回调缺陷证据：`/tmp/dbms-r235-v3-justify.Q4xujoDQ`。
最终重建/完整验收：`/tmp/dbms-r235-v4-justify.ymfmoNU7`。

| 记录 | SHA256 |
| --- | --- |
| PG首轮含两条错误期望 | `d4df99a8633991da4b84df70f447febf060823db8285a4dcd5978a3e8d80a289` |
| 固定夹具修正 | `8093e8b946d9a5b2ed3d7139037182b0685fd44af7e529f928a259b21bc46a52` |
| 完整真实PG997 | `d7568d88d5ccf6fbc7af7d647d738123238f4b33a7bb70de2291c4745e236566` |
| 原旧基线 | `3b49d61d941ee4f3d1718ae7fd9b34b491563d744de6b5d5cce7cf374b358f4e` |
| 最新父提交旧基线 | `7fba4e051eb5ef5e595f14c88bcd47ba52513bab4d292555e118c6445832cf2d` |
| V3完整60次运行 | `d6d2688bbdd0a019865abac9914534ab43125b8da455849dbe0d358bff429fc2` |
| V3完整126回调失败 | `d742e17233038acc6bd6520318aad58ceed88e0ca1147c45ccf88b1127eb3d69` |
| 最终实际构建 | `56f84c445380304ddd1090cb18c86ddb1e084a540cc273461c54953fbfa70010` |
| 新完整985/997/126 | `1dcd86977a1e01a1ee67ea8d3dfdb4f870a62b43274c2fcdb5f9649e57113244` |
| 最终完整62次运行 | `f9a193b447163a6440cc1af736a793bc6d95f36915ee00e24891d93bb6c92808` |
| 暂缓完整2863未新增失败标签 | `860ee2a4444038905ea4970a4156d9dec26b4184537208b2827329cbc5952b7c` |

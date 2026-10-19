# R159：hex、escape 与 base64 编码边界

状态：三类普通编码边界完成有限修复；仅含本项改动的独立版本和工作区各11入口全部通过。
本文后半的三项类型目录静态线索仍未核实，不随编码修复关闭。

| 实测 SQL | PG 18.6 | 旧 Root / accepted-only | 修改方法 |
| --- | --- | --- | --- |
| SELECT encode(decode('6 1','hex'),'hex') | 22023 | 61 | 只在完整 byte pair 前跳过允许的空白，两个 nibble 必须相邻 |
| SELECT encode(decode('61'\|\|chr(11)\|\|'62','hex'),'hex') | 22023 | 6162 | 仅允许空格、tab、LF、CR；不能用 isspace 放行 VT/FF |
| SELECT ascii(encode(decode('07','hex'),'escape')) | 7 | 92 | escape 仅对 NUL 与大于等于128的字节输出八进制，反斜杠加倍；保留非零 ASCII 控制符及 DEL |
| SELECT length(encode(decode(repeat('61',58),'hex'),'base64')) | 81 | 80 | 按 PG 规则每76列换行，验证恰好边界及尾部规则 |

三端分别完整执行原 20 条诊断中的这四条，结果见
/tmp/dbms-review-r46.ifu6SWLp/r158-{reference,old-root,old-clean}-discovery.log。
这是观察记录，不是四项通过。临时表只用于同一连接的私有诊断，没有业务数据修改。

修复前，[实现](../src/expression/ExprEvaluator.cpp)的 hexDecode 先删除全部 isspace；
base64Encode 不换行；escape 分支对所有小于0x20或大于0x7e的字节转义。
[既有测试](../tests/encoding_functions_test.cpp)只检查 byte pair 之间的空白，
且 BEL 的预期本身锁定了错误的八进制输出，需要按实际参考纠正并保留该用例。

新增验证覆盖全字节 escape 往返、合法/非法空白位置、空串/NULL、混合大小写、
奇数 nibble、base64 的57/58字节等行边界及多行、padding、真实表、参数与协议原始 datum。
修复前后保留完整参考和失败证据，逐根因提交，不通过全局放宽 bytea 校验绕过。
对照 [PG encode.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/encode.c)。

## 仅登记的类型目录静态线索

以下尚未运行专门对照，均未修复，不能视为已验证 bug；记录以避免丢失审查发现：

- `_record` OID2287 的 bootstrap category 为 P、typelem2249、storage x；
  regtype_output 只按 category A 判断数组可能漏掉真实 record[]。
  需核实 PG 的真实数组/storage 判定，再验证目录、呈现及数组往返，不混入 R151 自动枚举数组。
- parser 的 TIME/TIMESTAMP WITH TIME ZONE 声明语法归约为通用 timetz/timestamptz，
  可能丢失 SQL 标准语法的内置身份，被同名用户类型遮蔽。
  需分别验证标准语法、普通别名、引用与限定声明；不能通过全局固定通用别名消除正常遮蔽。
  这不是用户暂缓的 R140 TimeZone 设置值语法。
- TypeRegistry 宣告 oid[] 为1028，但 builtinTypeRows 缺少对应实际目录行。
  需核实普通声明、pg_type 双向链接、参数/NULL/数组 codec 及持久化身份，不能编造 OID 替代。

这些线索不构成恢复用户暂缓的 R156 类型呈现/载体专项的授权。
总清单仍未完成；不 push，Actions 禁用。

## 普通 codec 的修复与验证

候选仅修改hex pair扫描、escape字节规则及base64每76列换行，保留原BEL控制并
纠正其字节预期；不扩展base64解码策略、一般BYTEA输入或用户暂缓的类型呈现模块。
PG18.6的完整三字节组达到76列时即使EOF也加LF，因此57字节输出77、114字节输出154；
padding在循环后输出，不额外换行，55/56字节输出76、58字节输出81。

固定676项native及原800项wire已准备；原800在PG18.6为0失败、冻结旧Root为164失败。
V2保留全部800，加入PG接受的`TQ=A`与`TQ==AAAA`各三模式，共806项；
使用夹具局部的严格DataRow长度和完整消息序列检查，不修改共享协议解析器。
V2在PG18.6实际806/0，冻结旧版本806/164失败；修复不改变非规范padding的既有接受策略。
原始与V2证据在`/tmp/dbms-review-r46.ifu6SWLp/r159-v{1,2}-{reference,old}-protocol.log`。

独立验证从已接受提交`a831c47f`投影，仅加入本项四个Evaluator hunk、
原BEL断言修正、完整676/806夹具与协议注册，不依赖其他工作区候选。
新编译一个Evaluator生产O2对象，其余57个对象逐一核对源码、全部头、编译器与flags、
收据及对象字节；两项native新编译并链接当前生产对象。
三个真实Main TU核对原始Main、已接受函数投影、全部头、flags和对象字节后复用，
重新链接当前57库并隔离运行，不宣称它们是本轮fresh TU。
实际11入口全部0：新native676、新协议806、原编码native、五个相邻协议和三个Main。

独立源码`/tmp/dbms-r159-independent-source.uMLBl42t`冻结不再修改；
生产binary SHA256为`e6caa09245d4c240e06f7f676f7d6cd39c29a1ab4160570a3d98453484d9843e`。
完整收据在`/tmp/dbms-r159-independent-receipts.SKM82DEa/final-receipt.json`，
运行记录为`/tmp/dbms-review-r46.ifu6SWLp/r159-independent-runtime.log`。

Root在原52入口结束后修正三个CPP并重新编译O2，重复构建0项，核对当前全部58生产对象；
两项native新编译，三个真实Main沿用相同头/输入/flags的R168V2真实TU并重新链接当前57库。
Root生产binary SHA256为`a7867ec6e967cf30d84b19194aa82b45fb9fbd267f744509a9ce91721555bba2`。
Root本项11入口实际全0，包括native676、wire806、既有编码native、五个相邻协议
（typed NULL完整1000项）和三个真实Main；完整运行前后源码fence均核对0。
收据`/tmp/dbms-r159-root-compiler-receipts.bcJbbqQi/final-receipt.json`，
日志`/tmp/dbms-review-r46.ifu6SWLp/r159-v1-root-runtime.log`。

按hex、escape、base64三个根因分别本地提交。完整676/806共享回归在最后一个提交加入，
运行对象包含三类修复；不声称仅含第一或第二个中间提交的版本也已通过完整矩阵。
前两项提交为`e92f0656`（hex）和`e20f8702`（escape及原BEL断言）；
两次提交均逐hunk复核，没有混入Evaluator的其他候选或用户原有改动。
完整夹具见[原生测试](../tests/encoding_boundaries_test.cpp)与
[协议测试](../tests/encoding_boundaries_protocol_e2e_test.py)。
不扩展R156、不更改原273状态；无整库全量/TLS/sanitizer的新通过声明，不push，Actions禁用。

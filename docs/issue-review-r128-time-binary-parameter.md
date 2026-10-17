# R128：TIME二进制参数拒绝合法微秒和24时

状态：本范围已修复，158项协议及55个不同完整入口全部通过；
按本项独立本地提交，不push。

## 实际缺陷与修改方法

`binaryProtocolParameterLiteral`已有1083接收分支，但要求微秒数能整除1000000，
且严格小于24时。它不是完全没有接收器，也不是先丢小数再接受：合法的微秒
及24:00:00会被拒绝为22P03。负值/过大值及不足八字节也落入同一泛化错误，
与实际PostgreSQL18.6（180006）的阶段/SQLSTATE不符。

| 路径 | 修改方法及边界 |
| --- | --- |
| 真实OID1083的binary Bind | 八字节大端整数微秒，接受0至86400000000（含24时），保留全部微秒并生成规范clock；不经过double、主机有符号转换或整秒heap解码 |
| 错误传播 | 不足八字节08P01；先读取八字节并校验范围22008，再检查剩余字节22P03；私有接收接口带结构化inputSqlState，不靠消息猜TIME错误 |
| 相邻协议与类型 | Parse/Describe无datum、Bind/Execute/重绑、NULL、结果text/binary、显式0–6舍入及BOOL/INT4/FLOAT8邻居；外层wire framing不在本项范围 |

接收codec修复不迁移旧4字节整秒TIME heap；也不冒称TIME[]/TIMETZ或
TIMESTAMP/TIMESTAMPTZ的微秒binary接收已支持。结果发送和普通文本输入分见
[R117](issue-review-r117-time-binary-result.md)、[R121](issue-review-r121-time-input-normalization.md)，
显式precision见[R125](issue-review-r125-time-precision-value.md)和
[R127](issue-review-r127-time-excess-precision.md)。这些普通剩余项不改记用户暂缓。

## 永久测试与复现

新增`tests/time_binary_parameter_protocol_e2e_test.py`158项，无新增原生入口。
固定整数向量验证从1微秒到24时的真正1083输入，保留text/8字节binary结果、
NULL、OID/长度/typmod、命令及Ready；独立precision向量覆盖0–6及CAST/::。
named Parse/Describe后八次Bind/Execute重绑，坏codec参数后必须保持连接可用，
每次附带SELECT 1恢复检查，不把恢复断言另数完整入口或独立bug。
所有外层消息和参数长度封套均合法，只改变TIME接收函数取得的datum字节。

实际PG18.6完整158项全过，参考日志
`/tmp/dbms-review-r46.ifu6SWLp/r128-reference158.log`；旧红灯及修复后完整结果另记。
同一最终158项在固定旧程序完整132失败、实际1，见`r128-old-protocol158.log`；
不是132个独立bug。初版旧验证完成112项后，在第113个预期成功的named重绑
遇到真实旧错误，fixture未先判断RowDescription存在而抛AssertionError，未完成
158项。`r128-old-protocol158-incomplete-initial.log`保留该轮，不冒其为完整红灯。
仅强化缺失描述符的失败收集、不改预期向量，再完整重跑PG158全过和旧158/132。
初版PG158全过日志亦保留为`r128-reference158-initial.log`。
固定旧程序`r128-pre-fix-dbms-main` SHA256
`bd35ec36787917ff93b5d612edef31bd4ff8f4a7b13b35c07b42e6f187f79a89`，
为R127已验收组合，仍有原保留候选，不是clean HEAD完整注册证明。

只修改NetworkServer私有接收器及本项注册，不改公共头、Main或heap；
normal/repeat、158专项、相邻完整入口、核对未改真实Main TU对象后新连接
57份当前O2库并隔离执行、最终封印及独立暂存/提交前后复核均按实际结果登记。
原273统计/勾选不提升，已跳过模块和原候选不混入，Actions禁用，不push。

## 专项验证

`r128-normal-build.log`实际只重编NetworkServer 1个CPP、repeat 0，均实际0。
`r128-current-protocol158.log`完整158项全过；私有binary接收函数仅增加结构化
状态传递，其他codec和原有text路径保持各自行为，无公共头/Main/heap变化。
未改Main测试TU仍须核对依赖/对象字节，再新连接当前57库并实际隔离运行，
不能把旧库二进制或未完成的入口算作本项终态。

## 最终有限验收

`r128-final-driver.log`和各结果目录记录24native+28协议+3真实Main共55个
不同完整入口全0；`r128-final-independent-check.log`独立核对0。
包括时间精度/语法/默认输入、日期3652059天、时间戳/interval/数组binary、
schema、逻辑长度、类型及会话等相邻入口，不把158个内部控制说成158个完整入口。

Main/公共头/测试TU与flags未变，核对R124对象的源码、头、编译器/flags、
正常Main receipt及对象字节后，重新连接57份当前O2库，无stubs，再经初始化前
隔离器实际执行explain/window/session三个真实Main前端。
这是复用未改TU对象、新链接及新执行，不是本项重新编译Main或复用旧库二进制。
双缓存、全部58个当前receipt、57份test库字节及源/测试/脚本封印末端复核0。

程序SHA256 `b25a7eb42ab2ccbd8927177f9a944e8dbf1f9c61484bbe307285c2ed30299441`，
缓存`b6c2916ab67c3ff0da953101b691c2d0103064d882f55b57b7b30c4c22da3c8e`，
封印`921fdfe6b04c332c67bbcd180cb777ba7f66cb5eef7807a818dd213a52d80353`，
公共头/生产flags签名仍`739bf42b71ae53b6f0cfe5e817aec5e81edb16ef16893f6616d93f026f9594c8`。

再次完整重采原122及原97：122的值/NULL/OID/长度/precision/来源/完成状态及
通知S/V/C/M全部匹配，原始两条TIME(7)仍只差PG可选诊断来源P/F/L/R，不
伪造其实现文件/行号、不说成所有原始字段同一字节；四个表列保持精确匹配。
原97基本clock/合法precision仍精确匹配。
`r128-time-precision-after122.log`/comparison及`r128-time-input-after97.log`/
comparison保存采集/比较实际0和全部原始字段。

本项接受投影549MD、898native+530Python+3Main=1431；工作区552MD、
901+533+3=1437含原六个保留入口。554Python AST/13Bash及文档本地链接/清单
另作静态复核，没有新完整注册回归、真实TLS或整库sanitizer证明。
原56933字节候选补丁及三个草稿字节不变，原273勾选/统计仍为
22complete/166partial/70unverified/15deferred，require-complete仍实际1。
旧heap与其他receiver/时态语义普通开放，不混入旧候选、不开Actions、不push；
独立暂存与提交前后只读复核另作实际记录。

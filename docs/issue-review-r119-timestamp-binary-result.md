# R119：时间戳微秒binary结果被错误编码为固定远古时间

状态：发送器及独立上游R120日期修复后的232协议控制、235低层控制和共享
41入口有限验收实际全部通过。R120先独立提交，再按R119独立提交，不push。

## 问题与影响

PostgresProtocol的TIMESTAMP/TIMESTAMPTZ发送器把完整小数秒文本交给旧整秒
parseTimestampToSeconds；后者失败返回0，却被发送器当成有效内部时间，减去
2000年wire epoch后输出固定的错误8字节值。客户端可能正常解码成远古日期，
而不是收到错误；TIMESTAMP[]/TIMESTAMPTZ[]通过真实元素sender继承同一错误。
这不等于1970时间戳都会失败：1970整秒原本正常，通过控制保留。

最初实际PG180006/固定0579474b程序各64条观察，12个binary差异；text控制
保留。永久232协议控制PG全部通过，固定旧144失败、实际1。完整235低层控制
旧192失败、实际1。正常1CPP/repeat0构建后，首版235低层已全过，但完整36入口
回归真实退出1：232协议尚有8个既有22008跨日输入失败，其余19协议/16native
完整入口通过。全部原断言、首版红灯和固定旧日志保留。

8个输入失败来自DateType.h的DISCONV，在闰年年初返回非法月份；由
[R120](issue-review-r120-date-cycle-inverse.md)单独修复。R119不靠删除跨日案例、
放宽SQLSTATE或把8条预期失败算通过来验收。

## 修改方法

只改PostgresProtocol.cpp内部helper及真实1114/1184调用：把完整小数秒与时区
分离，复用既有checked整数解析得到微秒/进位，然后验证旧整秒输入，明确拒绝
0错误哨兵。以__int128完成减2000年epoch、乘1000000及小数累加，检查int64范围；
全程不以double转换完整时间。负epoch及-1微秒、半偶舍入/秒/日/年进位均保留。

TIMESTAMP忽略已校验的显式offset；TIMESTAMPTZ按offset转UTC，不能把两种类型
用同一错误时区解释。支持现有1–9999年ISO carrier、日期单值/T分隔/外部空白及
正负infinity的真实max/min；不把非法日期、时钟或offset包装成看似有效的日期。
真实1115/1185封套继续使用原元素OID及同一scalar codec。失败不发送部分DataRow。

本项不改公共头、heap秒格式、输入/typmod/会话时区规则、域/用户OID、binary
Bind或所有PG的BC/大年份输入范围。上游R120的公共头改变是另一个独立版本，
当前共同产物包含该修复，不冒称只有R119一项改变或clean HEAD全量证明。

## 永久控制及证据

`tests/timestamp_binary_result_protocol_e2e_test.py`完整232条：22组独立期望时间，
涵盖前后epoch、-1微秒、早/晚年份、闰日、舍入/进位/infinity；双类型分别通过
text Bind、带NULL数组、SQL常量和ARRAY构造器。另测6组offset、NULL/空/全NULL、
负下界、2D/6D、无行、真实Parse/Describe、未改text/混合格式及真实表的
INSERT RETURNING/SELECT。表内scalar用既有秒heap可存的整秒，数组保留微秒，
不掩盖scalar物理精度边界。OID/size/typmod/format/字段名/行/command tag/Ready
逐项严格检查，独立期望值经实际PG18确认，不从被测parser计算。

`tests/timestamp_binary_result_test.cpp`完整235条：22组时间及6组offset的
scalar/真实数组字节、基本ISO carrier、NULL/形状/下界、20组非法carrier、
相邻TIME/INTERVAL/数值及混合行全成或全不发送；Engine链接入口使用初始化前
隔离器、O2及fresh stubs。原235入口完整旧红灯和首版全过均保留。

证据目录`/tmp/dbms-review-r46.ifu6SWLp/`：最初64观察为
`r119-timestamp-{reference,current}64.log`；
`r119-reference232-initial.log`、`r119-old232-initial.log`、
`r119-old-native235-initial.log`为永久控制参考及旧红灯；
`r119-final-{driver,native,core,neighbor}.log`为首版完整36入口的真实失败终态。
固定旧`r119-pre-fix-dbms-main` SHA256：
`0579474b74a5f4386563066b0b21e4f9d255d1a1a94c6ab6143fc8e93dd95429`。
首版生产`c1b020432223b229c49ff950db9a2295db7020271b9d06a334e0a01e70900809`
也固定保留，并非clean HEAD。

## 最终共同验收及范围

上游R120触发全部58个生产TU的实际fresh O2重编、正常link/repeat和官方
normal/repeat，均实际0；之后不改源/测试/脚本，按同一冻结产物验收。共享
17native+21协议+3真实Main=41个不同完整入口实际全部退出0。终态保留于
`r120-final-{driver,native,core,neighbor}.log`及各结果目录；本项不重命名或覆盖
R119首版失败日志。R120仅改日期header，发送器和本项两份测试字节未改动。
三个Main TU直接编译当前真实源，explain/session测试TU O0、window/生产库O2，
匹配57正常库、无stubs；不是复制测试体、只跑编译或调用原CLI入口。

生产SHA256 `24b23d5a5b39ae4717c64a810d4c37d4e0dce14ea5a9f3b3c0a7495ec5a6e524`，
缓存签名 `ba206d2af2b3c31427ccc6347eb8ce14f9e5d9f787c43beb174b1af20ccf419b`，
源/测试/脚本封印 `1c6e6e034d731bb9d627a1b2048220ac9058ab8a1d0ec1397962b91436763436`。
双缓存、全部58receipt、57test-library字节及冻结输入复核0。本项验收与R120
复用同一未改产物，不捏造又一轮CPP编译或41入口重跑。

额外完整重采原64条观察，精确对比同一180006参考，原12个差异全部消失，
当前64条全部匹配rows/metadata/NULL/SQLSTATE/tag；
`r119-timestamp-after64.log`及`r119-timestamp-after64-comparison.log`实际均0，
区别于最初仅采集0但存在12差异的日志。

本项接受组合为541MD、893native+3Main+523Python=1419；工作区544MD、
896+3+526=1425含原保留候选。无新完整注册回归、真实TLS或整库sanitizer证明；
R120纯日期组件sanitizer不作为整个数据库或本sender的sanitizer证明。
全部工作区544MD/547Python AST/13Bash及链接/清单一致性另作静态核对，原273
一致性保持0、require-complete仍实际1。本项只暂存15个路径和所属hunk，不纳入
原56933字节候选补丁及三个保留草稿，提交前后均复核。

TIME默认基本clock输入/text Bind后由[R121](issue-review-r121-time-input-normalization.md)
单独修复；完整grammar/cast eligibility、显式精度、scalar微秒heap、binary Bind、TIMETZ及其他codec、
BC/大年份、目录SQL/view origin及R111规划仍普通开放，按各自方法继续复现修复。
原273仍22complete/166partial/70unverified/15deferred，不提升勾选；R98/R99/R109
候选及R106草稿保留且不混入提交，用户已明确暂缓的范围不恢复。Actions保持禁用。

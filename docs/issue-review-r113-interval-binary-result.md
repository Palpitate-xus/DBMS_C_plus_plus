# R113：INTERVAL及数组缺失真实二进制结果编码

状态：本范围已修复并完成有限验收，按本项独立本地提交；不提升原273项状态。

## 问题与复现

标量INTERVAL和含非NULL元素的INTERVAL[]请求二进制结果时报0A000；
文本结果、SQL NULL、空/全NULL数组可正常返回。SELECT、实际表RETURNING
和UPDATE等客户端都受影响。错误发生在发送结果阶段，写语句还会回滚，不能
将之后SELECT0行误诊为独立存储丢失，也不能用文本字节冒充binary。

最初实际180006参考与固定旧595869a9各48条观察，22条二进制差异；两个观察器
退出0仅代表完整采集。永久171协议控制实际参考全部通过，旧152失败、实际1。
严格物理INTERVAL size16断言另外暴露[R114](issue-review-r114-physical-attribute-length.md)，
其独立修复及[R115](issue-review-r115-scalar-alias-projection-metadata.md)先行提交，未放宽断言。

随后固定b4128ced版本重新完整运行：117低层控制78失败、171协议149失败，均
实际退出1。只去除了metadata缺陷，二进制发送缺失仍可独立复现。

## 修改方法

真实PostgreSQL18.6源码timestamp.c的interval_send逐字段发送：
int64时间微秒、int32天、int32月，均网络字节序，共16字节。
timestamp.h中正/负无限为这三个字段各自全部最大值/最小值。

只在PostgresProtocol.cpp内新增真实1186发送分支；有限值复用既有纯函数
parseIntervalInput及其checked状态，不经过SQL求值、存储、会话或total-seconds
换算，不把月/天折入时钟，不通过double转换int64微秒。无限标记独立处理，
检查完整token、大小写和外部空白；非法/溢出值拒绝，发送失败不输出部分DataRow。
既有真实1187数组封套调用同一标量codec，自然保留元素、NULL、维度和下界。

未更改公共头内容、物理行布局、解析/赋值规则、二进制Bind输入或其他标量codec。

## 永久验证与历史失败

`tests/interval_binary_result_protocol_e2e_test.py`：171控制。35组独立期望
微秒/天/月tuple，各经真实text Bind的标量/带NULL数组、SQL INTERVAL常量及
ARRAY构造器四种模式；另测NULL/空/全NULL、负下界、2D/6D、无行结果、文本
保留、Parse/Describe、实际表INSERT/UPDATE/SELECT RETURNING、typmod精度、
错误输入与Ready状态。包含最大/最小int64微秒、int32月/天和双向无限。
分数单位与时钟小数使用参考各自真实舍入结果，不修改参考值。

`tests/interval_binary_result_test.cpp`：117低层控制。独立构造16字节和数组
封套，遍历同35个datum及NULL/维度、原始infinity拼写、非法/溢出/错误形状、
既有数值/BOOL和文本输出、混合列全成或全不发送；其他尚缺codec仍拒绝，
不让测试把假文本二进制当成功。完整入口在隔离器中运行，先切目录后初始化Engine。

初次171协议夹具误把Bind值字符串当值列表，实际参考159控制后报错；修正
夹具构造后PG171全过，旧171/152失败。原日志未删除，不把中断轮算成全量参考。
源证据安装在`/tmp/dbms-pg18-reference.23EK0zXN/postgresql-18.6/`；参考实际
server_version_num=180006，不使用默认Docker中其他版本充当18.6。

日志目录 `/tmp/dbms-review-r46.ifu6SWLp/`：`r113-interval-*-observations.log`、
`r113-reference171-{initial,fixture2}.log`、`r113-old171-{initial,fixture2}.log`、
`r113-post-metadata-old-native117.log`、`r113-post-metadata-old171.log`。
最初旧程序SHA256：595869a944bc409cb04e37597a0aa74c9734a07c11a8c813b0aa81473001a6e5；
依赖提交后的固定旧程序：b4128ced2d381c48f7c783550cb69753ce99d7924f851a4f801f576cfce393dc。
它们均含用户保留候选，不冒充clean HEAD二进制。

## 最终验收及提交边界

normal/repeat实际重编1/0CPP、均退出0；既有SqlArrayText.h缩进warning保留，
不是本项编译失败。生产和库为O2，fresh stubs/隔离native。最终冻结输入下
16个完整native、20个完整协议，共36个不同入口全部实际退出0，含本项117/171、
原182/110数组binary、数值binary、interval赋值/输入/范围/修饰符/精度、真实
表RETURNING、R114的102/28、R115的228、会话及默认完整协议；未改原断言和超时。
日志`r113-final-{driver,core,native,neighbor}.log`和逐协议目录保留。

最终binary：
`2c8c1b64fb11188bbd65fc471c7d75cfaba94633b844dcc3290058f0e5d9aa58`；cache：
`048df4063447c12612d5ea7848d45f9342c7f5832244772b3523bc752e164c31`；
src/tests/scripts seal：
`2c37e60e2063e946790bd949fe4195e3efd8e74261563ea7855e4a1cd7090217`。
双stamp、全部58个生产receipt、57测试库字节、冻结输入/公共头终态核对0。
没有把一份增量编译说成新编58份，也没有借用旧完整注册回归。

工作区539份MD、542个Python AST、13个Bash语法/本地内联目标与状态/注册
检查0；原273一致性0、require-complete实际1。近期记录同时纠正R114/R115
与时间TYPE-06/数组TYPE-16、字符TYPE-04的关联，原items/勾选/统计不变。
本项精确14路径：PostgresProtocol.cpp内部29行、两个测试、单个注册和文档；
接受536MD/890native/518Python，注册组合1411；工作区893+3+521=1417含保留候选。
原56933字节保留补丁重建后源码及三个暂缓草稿逐字节检查仍使用独立门禁。
这不是clean HEAD全量、真实TLS或整库sanitizer证明。

补充原90条数组观察完整采集退出0，实际参考差异由10降为9：INTERVAL输出
差异消失，BOX输入2、JSONB/TSVECTOR规范文本2与其他缺失标量binary5保留。
日志`r113-array-matrix90-current.log`，与原实际180006参考逐条比对；不是90项全过。
另对TIME/TIME[]做32条实际参考/当前观察，两侧采集均0但有16个差异：
微秒/24:00:00二进制12次0A000，以及大于6位小数的文本舍入4次不符。
`r117-time-{reference,current}32.log`完整保留。检查确认旧sender复用了仅支持
0–23小时、整秒的物理parseTimeToSeconds；高精度输入文本规范化独立待查。
这不是INTERVAL修复失败，也不能写成TIME或整个时态支持已经完成。

## 后续状态及仍未完成范围

R113原32条TIME观察中的binary问题后由[R117](issue-review-r117-time-binary-result.md)
单项实现真实微秒sender；输入/文本规范化和旧heap精度仍另项开放，不改写
本页原始16差异或INTERVAL验收证据。后续终态以R117记录为准。

| 范围 | 后续修改方法 |
| --- | --- |
| TIMETZ、JSON/JSONB、TSVECTOR、range标量及非NULL数组binary | 逐类型实现真实发送格式并做实际参考字节对照，不猜用户OID |
| INTERVAL/数组binary Bind输入 | 由真实接收codec校验长度、类型身份、维度/元素，而非复用文本Bind |
| BOX数组文本输入和JSONB/TSVECTOR规范文本 | 分别修真实分隔符和元素输出，不归于INTERVAL发送 |
| 通用具名复合/域/用户数组 | 由真实目录身份和所属codec控制，不通过内置OID表假装全支持 |
| R117 TIME微秒/24时binary及超微秒规范文本 | 采用真实微秒TIME发送、检查0至24时边界；输入舍入另按真实datum契约修复，不能粗改4字节存储或静默丢精度 |

R111规划及目录SQL/来源问题仍普通开放；时态NULL视图物化布局的后续实现
和验收见[R118](issue-review-r118-temporal-view-materialization.md)。非物化TEMP VIEW tag后由
[R116](issue-review-r116-view-command-tags.md)独立修复。未完问题不是用户暂缓。
原273仍22complete/166partial/70unverified/15deferred；未获得新
完整注册回归、真实TLS或整库sanitizer证明。R98/R99/R109候选和R106草稿不混入；
不push，Actions禁用。

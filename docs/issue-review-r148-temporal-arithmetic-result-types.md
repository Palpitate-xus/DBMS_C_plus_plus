# R148：日期算术的结果类型归属

状态：完成有限修复与独立终态核对（实际0），仅按本项20路径独立提交。
147原生/552协议、Root48完整入口和独立clean14均实际通过；
两份全58生产O2与三真实Main的新编译、链接和隔离执行均已完成。
本项先处理类型声明依赖；本项验收时R147日历字段顺序尚未独立验收，不混入本次提交。

## 问题与修改方法

| 问题 | 旧版实际表现 | 修改方法 |
| --- | --- | --- |
| DATE与INTERVAL结果仍声明为DATE | text Bind返回时间戳数据却发1082/4字节描述；binary结果报0A000 | 共享结果签名明确DATE加/减INTERVAL为TIMESTAMP/1114，不继承左输入OID |
| 交换加法操作数丢失类型 | INTERVAL加DATE/TIMESTAMP/TIMESTAMPTZ误声明1186 | 使用完整的已声明操作数对，结果属于日期/时间戳运算，不属于左侧interval |
| 日期/时间戳相减仍继承输入 | DATE差值、timestamp/timestamptz差值声明错误 | DATE-DATE为integer，时间戳差值为interval，结果由运算签名拥有 |
| NULL改变结果类型 | 直接native严格NULL返回左类型；SQL文本后缀又可能把整个表达式声明为interval | 在NULL短路前确定结果类型，metadata按完整AST而非最后一个cast猜类型 |
| binary发送选择错误编码器 | Portal描述可为timestamp，执行结果却被声明为interval，合法数据因此0A000 | 静态、绑定、实际执行共同使用既有共享签名入口，发送端无需改为猜值或宽松解析 |

修改位于[arithmetic_type.h](../src/expression/arithmetic_type.h)的既有resultType，
在UNKNOWN数值推导前处理有限、明确声明的日期签名。
覆盖DATE/时间戳与INTERVAL的正反加法和减法、DATE与int4天数、
DATE/时间戳差值及interval加减；保留原数值宽度、提升和错误规则。
只统一既有有限执行能力的类型，不通过采样一行、求值函数或伪造参数发现类型。
两个CPP仅将numericResultType/numericOperator改为一般算术命名，语义入口不另建副本。

规则与运算返回类型见[PostgreSQL 18日期/时间运算说明](https://www.postgresql.org/docs/18/functions-datetime.html)；
实际预期另由本机180006参考逐控制验证，不把文档说明当运行证明。

## 参考、旧红与待验收

| 完整输入 | 实际记录 |
| --- | --- |
| 实际PG18.6/180006，552协议 | 全部通过，实际退出0 |
| 冻结Root c1befd58，同552 | 259断言失败，完整执行，实际退出1 |
| 冻结已接受ef39c0b7，同552 | 259断言失败，完整执行，实际退出1 |
| 旧147原生 | 70断言失败，完整执行，实际退出1 |
| 最终147/552、Root48与clean14 | 完整执行，全部通过，实际退出0 |
| 独立终态复核 | 完整控制、来源、原工作区28路径及冻结输入核对均通过，实际退出0 |

259/70包含同一类型根因的多模式、NULL、描述与后续数据检查，
不是相同数量的独立bug。reference、旧红及最终候选均保留完整输入。
新专项中的interval都只有clock部分，不依赖R147月份/天数顺序修正。

## 永久回归设计

[temporal_arithmetic_result_type_test.cpp](../tests/temporal_arithmetic_result_type_test.cpp)
共147控制：19个签名逐项检查共享规则、文本/AST声明、左右及双NULL和非NULL值，
另有4个别名、5个不猜测签名和5个原数值规则控制。

[temporal_arithmetic_result_type_protocol_e2e_test.py](../tests/temporal_arithmetic_result_type_protocol_e2e_test.py)
共552控制：Simple与Extended text/binary结果、明确OID的text Bind、
左右/双NULL、WHERE FALSE零行、独立Statement/Portal Describe。
严格检查值或原始binary字节、类型OID/长度/typmod/format、NULL、tag和Ready；
Describe核对参数OID并不得返回执行结果。不是新增binary参数receiver。

原生SHA256：
662cfce1e15d3e0f259edb141ce7749c102e89a45beb0664510e12c7fe5b48df。
协议SHA256：
f39dd83ea905d8457f9f6b214b8f1ce8813f3d6eb8a1944e8853810c9b811d1f。

## 构建和候选边界

共享头逻辑改变，Root和干净版本均正常fresh编译全部58生产O2对象，
repeat零重编和58个当前receipt，均实际0；未使用旧头下的对象。
三真实Main TU另行fresh编译（explain/session原有O0、window O2），
随后新链接当前57生产O2库、不用test stubs，并经既有隔离机制实际执行，全部实际0。
复用的只是未改变的已接受320控制输入文件，不复用R145/R147对象；
新编译得到相同对象SHA也不能改写实际编译记录。

干净版本基于0f1949a1，仅本项四个partial源/注册路径及两个新夹具；
不含R147字段顺序修复、两夹具或注册，也不含R98/R99/R109/R137/R140候选。
其全部tracked文件及src/tests/scripts封印从构建起永久保持不变。
Root较宽的组合不能代替这份独立接受组合。

Root源封印：
6257082a5e7b215ce1511b701720a822dd7483f89f9f6f88694b97c29c171f86。
Root头/编译配置：
041162b15cd54581ec85e7706bbc4fd50c12b6342c25a6ac29b37b973248644a。
共享arith头SHA256：
21efd56ae1e8cf8e0a2644e03ab85ebd7d456516baf37ed71cffc55399795536。
Root binary：
474c64cb3793d5590db618b293681a0502e2cb3472290911da22cb36641a47b1。
干净 binary：
1420a863f46ac73e1b90180d77f31f5b03b0f8151de3fa65507c4657ea4641e0。

## 与R147的分项关系及未完成边界

R147值候选最初原生2043通过，但完整943仍11失败；
Root35为34通过/1失败、clean9为8通过/1失败，整批均实际1。
这11个断言全部保留，不能因日历值算对就接受整个夹具。
本项先独立修类型依赖；R147仍须独立干净组合、完整943及单项commit后才能验收。

本机/tmp/dbms-review-r46.ifu6SWLp/保存reference/旧红/build/三Main/
逐入口结果/源边界及独立核对日志。2175份前置源、测试和脚本逐字节撤回本项后
重建R147原d467帧；更早R145的055帧与用户草稿由前项来源链保留。
不修改旧候选、失败日志或donor，不把保留的其它候选混入接受版本。

本项接受数量为569MD、904native+543Python+3Main=1450；
工作区572MD、910native+549Python+3Main=1462还包含原十个及R147两个待接受入口。
本项没有新1450全注册、真实TLS或整库sanitizer证明。
TIME/TIMETZ算术、UNKNOWN重载、完整cast图、civil/BC/infinity/DST及整个TYPE-06仍开放。
R146主键、R137外键、R140时区语法及其它用户暂缓不重开。
原273及22complete/166partial/70unverified/15deferred不变，不push，Actions保持禁用。

# R127：直接TIME投影的过高精度未截断且缺少warning

状态：本范围已修复，252项原生/276项协议和54个不同完整入口全部通过；
按本项独立本地提交，不push。
来自[R122](issue-review-r122-time-explicit-precision.md)在R126后的两条剩余观察。

后续实际1083的scalar binary Bind微秒/24时及错误状态见[R128](issue-review-r128-time-binary-parameter.md)。
158/55和原122核心、原97匹配另作验收；本项仍为直接投影精度/通知，旧heap、
其他表达式角色及其他receiver保持各自所有权。

## 实际缺陷与修改计划

实际PostgreSQL18.6（180006）将直接TIME(7)声明的precision截断为6，并发送
severity WARNING、SQLSTATE 22023、消息`TIME(7) precision reduced to maximum allowed, 6`。
项目旧结果typmod为-1、没有通知；超过微秒的小数还可能保留未经规范化的text。
消息中的整数是实际modifier值，0x7等不能原样当作消息里的精度数。

| 路径 | 方法与边界 |
| --- | --- |
| 值转换 | 显式非负int32 TIME精度超过6时按6走既有规范化；不放宽持久化TypeRegistry的0–6守卫 |
| 声明描述 | 真实CAST/::/typed literal AST、当前session目录确认1083/1183后返回有效6，不执行datum或猜UDT |
| 直接SELECT声明通知 | Simple Q一次、Extended Parse在ParseComplete前一次；Describe/Bind/Execute/多行结果不重复；从实际声明构造结构化通知，不扫描SQL字符串 |

仅直接SELECT投影声明的通知时机，不把DDL、SQL PREPARE、嵌套/其他表达式、
WITH/子查询、其他时间类型、消息过滤GUC和完整声明分析冒充完成。
不依赖待确认R98诊断channel，不重开已跳过模块；Actions禁用，不push。

## 永久测试及原始证据

`tests/time_excess_precision_protocol_e2e_test.py`276项在实际PG18.6全部通过：
七种高精度整数写法、scalar/array/NULL/空/下界、限定/quoted/具名数组、
Simple/Describe/Extended text与binary、Parse一次及两次Bind/Execute不重复、
两行表投影只通知一次，及合法0–6/字符串注释/非法关键字语法邻居。
通知比较S/V/C/M和顺序，不伪造PG实现文件F/L/R及内部诊断位置。

`tests/time_excess_precision_test.cpp`252项使用固定text期望，覆盖半偶输入、
微秒进位、scalar/array/维度/下界、具名声明、坏clock SQLSTATE及严格registry邻居。
链接引擎的测试经初始化前隔离器运行。

`/tmp/dbms-review-r46.ifu6SWLp/r127-reference276.log`为完整参考通过记录。
初版fixture误用既有array辅助函数参数、在第一个控制前报TypeError，实际1，
日志`r127-reference276-initial.log`保留；修正fixture后重新完整验证，不算产品失败。
固定旧程序`r127-pre-fix-dbms-main` SHA256
`97890f1d8edfe73d8984c02a38f7c4e85374820d6d0ebda800f61df7ef4e892f`。
它为R126已验收工作区组合，仍有原保留候选，不是clean HEAD全量证明。

固定旧252项原生225失败、完整276项协议246失败，均实际1；不是225/246个
独立bug。对应`r127-old-native252.log`及`r127-old-protocol276.log`保留整轮控制，
原生在修改产品前核对原58 receipt并用57份当时O2库与fresh stubs隔离运行。

逐版normal/repeat、252/276专项、相邻完整入口、匹配57库的真实Main
新链接/实际隔离执行、封印及独立暂存/提交前后复核继续按真实结果登记。
原273统计/勾选不变；完整heap/微秒行存储、binary Bind等普通未完项继续开放。

## 逐版验证

首版normal重编NetworkServer/ExprEvaluator共2 CPP、repeat 0，均实际0；未运行
首版252/276，不冒首版专项通过。复查后将通知收集入口限定SELECT，避免额外
解析无关DDL，再实际重编NetworkServer 1 CPP、repeat 0，均实际0。
此修正版252原生/276协议完整通过，`r127-c2-native252.log`及
`r127-c2-protocol276.log`保留结果。没有公共头、Main或heap变更。

## 最终有限验收

`r127-final-driver.log`及结果目录记录24native+27协议+3真实Main共54个不同
完整入口全0；`r127-final-independent-check.log`独立核对0。包含R126严格549、
R125值转换339/830、R123列精度782/104、日期3652059天及数组/interval/时间戳/
逻辑长度/schema/类型/会话协议等邻居，没有把内部断言数量冒作完整测试入口。

Main、公共头、测试TU和flags未改，核对R124对象的源码/头/编译器/flags、
正常Main receipt及对象字节，再新连接57份当前O2库、不用stubs，并经初始化前
隔离器实际执行explain/window/session三个真实Main前端。复用未改TU对象、
新链接及新执行，不是本项重新编译Main或复用旧库二进制。
双缓存、全部58个当前receipt、57份test库字节及源码/测试/脚本封印末端复核0。

程序SHA256 `bd35ec36787917ff93b5d612edef31bd4ff8f4a7b13b35c07b42e6f187f79a89`，
缓存`32e6751381ab24a18313391e38ecd4bb9c8bcbbfaaed7723124ea746bb29bdef`，
封印`c73a69e5ab68471f34e16a77743cd8b2b8e0b5ae2ced8d9f6dfe4a20a3234679`，
公共头/生产flags签名仍`739bf42b71ae53b6f0cfe5e817aec5e81edb16ef16893f6616d93f026f9594c8`。

完整重采原122条，值/NULL/OID/长度/precision/来源/完成状态及通知S/V/C/M
全部匹配；原始比较的两条TIME(7)只差PG可选诊断来源P/F/L/R，不伪造其
实现文件和行号，也不声称原始字节等同。原四个表列及原97基本clock/合法
precision保持精确匹配。`r127-time-precision-after122.log`/comparison及
`r127-time-input-after97.log`/comparison保存采集/比较实际0和全部原始字段。

本项接受投影548MD、898native+529Python+3Main=1430；工作区551MD、
901+532+3=1436含原六个保留入口。553Python AST/13Bash及本地文档链接/清单
另作静态复核，没有新完整注册回归、真实TLS或整库sanitizer证明。
原56933字节候选补丁及三个草稿保持字节不变，原273勾选/统计仍为
22complete/166partial/70unverified/15deferred，require-complete仍实际1。
DDL/其他表达式角色、消息GUC、完整转换资格等普通未完项不改记用户暂缓。
独立暂存和提交前后只读复核分别记录，不混入旧候选，不push或启用Actions。

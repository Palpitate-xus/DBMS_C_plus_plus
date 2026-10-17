# R126：TIME关键字的非法精度到执行阶段才被处理

状态：本范围TIME关键字精度语法已修复，549项严格原生、326项协议及52个
不同完整入口全部通过；按本项独立本地提交，不push。
本项从[R122](issue-review-r122-time-explicit-precision.md)拆出关键字语法和整数
token范围；合法值转换/声明描述已由[R125](issue-review-r125-time-precision-value.md)单独修复。

## 实际发现与方法

未加引号、未限定schema的TIME关键字精度只接受一个不带符号的32位整数
字面量。旧程序接受负数/负零、小数、多参数、引号和NULL等，有些直到输出
描述符后才报错；即使NULL或WHERE false也应在语法阶段报42601。

实际PG18.6还接受0x/0b/0o进制及数字分隔符，不能把它们误拒绝：0x3、0b11、
0o3及0_3的precision均为3；2147483647是合法整数语法（随后warning/clamp），
2147483648在TIME精度位置则是42601。

| 路径 | 修改方法与边界 |
| --- | --- |
| consumeDeclaredType | 仅未限定/未引号的TIME关键字要求单个无符号int32 token；复用既有整数codec验证合法进制/分隔符和范围，AST保存其十进制整数值 |
| CAST/::/常量/DDL | 共同类型语法入口拒绝不合法声明，避免产生结果描述符或执行DDL；不靠是否有datum决定声明合法性 |
| 高precision及具名类型 | 7、30、INT_MAX仍是合法语法，clamp/warning属于后续语义问题；qualified/quoted泛型名字仍保留其不同语法，不能猜其负precision SQLSTATE |

不修改公共头、Main、heap、发送器或全局TypeRegistry的持久化范围守卫。
AST整数字面量规范化会让合法非十进制TIME声明得到真实值3，而不是原文0x3。

## 复现及永久控制

永久`tests/time_precision_grammar_protocol_e2e_test.py`326项在实际PostgreSQL
18.6（180006）全部通过，固定旧程序完整326项296失败、实际1。
覆盖十二类非法modifier的Simple/Describe/Extended、NULL/array/WHERE false及
TEMP DDL；要求42601出现在ParseComplete、BindComplete、RowDescription、
DataRow和CommandComplete之前，Ready保持空闲。
合法precision/进制/分隔符检查NULL声明及真实text舍入。四个高precision
Parse-only控制只证明其语法允许，明确不检查或冒充尚缺失的warning/typmod能力。

永久`tests/time_precision_grammar_test.cpp`549项：parse/parseForBinding双入口、
八类SELECT/DDL类型角色、直接type envelope、合法整数规范值、array/zone后缀、
timestamp/numeric/char/bit邻居，以及qualified/quoted负值的纯语法区别。
完整旧549项217失败、实际1；不执行引擎语义，不把具名负值的语法接受当转换合法。

日志目录`/tmp/dbms-review-r46.ifu6SWLp/`内`r126-reference326.log`、
`r126-old-native549.log`、`r126-old-protocol326.log`保留完整证据。
`r126-precision-lexical-reference14.log`保存实际十四条边界观察，采集0不等于
所有这些语义已修复。初版185协议/260原生的PG全过、旧162/136失败日志也
保留；补充合法整数词法后再按最终326/549完整验证，不改写初轮结果。
固定旧程序SHA256为
`1a1d677ead64f7507f382ee9a3214709f941a39bc0cb40a66adf22d68b89786d`，
对应R125已验收组合，仍有原保留候选，不是clean HEAD全量证明。

## 验证范围及仍未完成

normal/repeat、549/326专项、相邻解析/类型入口、重新连接当前57库后的真实
Main运行和最终输入/缓存/58 receipt/库字节封印都已分别验证，具体见最终终态。
Main未改时，必须核对原真实TU对象的源码/头/flags/编译器/正常Main receipt
和对象字节，不能把旧库二进制复用当当前运行，也不能冒称本项重编Main。

TIME高precision的clamp/warning、具名类型modifier完整语义、其他类型的
关键字精度语法、完整常量分析、TIME输入/转换资格、旧heap与binary Bind仍
普通开放。原273统计/勾选不提升，明确暂缓项不重开，原候选不混入；
Actions保持禁用，不push。

## 首版失败与修正

首版normal/repeat为1/0 CPP、native549实际0，但协议只完成19项，第20项
非法TEMP DDL的Extended Parse发生ConnectionReset，未完成326项，实际1。
`r126-c1-protocol326.log`保留完整输出和异常，不以native通过代替协议验收。
新TIME检查的DbError直接从DDL类型读取冒出；Network的Parse入口按结构化
ParseResult响应语法错误，因而未能保持连接。修正复用既有声明语法错误桥：
完整语句返回42601 ParseResult，单独type-envelope调用仍按契约抛DbError。

永久C++控制进一步要求parse/parseForBinding不得抛异常，以检查API边界。
同一最终549项又连接已冻结R125的57份O2旧库，按原cache日志逐字节核对、
复制到独占目录，当前相同头/flags/编译器下fresh stubs并初始化前隔离执行，
仍完整217失败、实际1。`r126-old-native549-strict.log`保留该强化后的旧红灯；
不是把当前已修改parser库当旧程序，也不声称旧库匹配当前全部58 receipt。
修正后的normal/repeat重编parser的1/0个CPP，严格549项原生和完整326项
协议均实际0。首版和修正版各自的normal/repeat均为1/0，不冒充全58重编。

## 最终有限验收

`r126-final-driver.log`及各结果目录记录23native+26协议+3真实Main共52个
不同完整入口全部0，`r126-final-independent-check.log`独立核对0。
包含R125值转换、R123列精度、日期3652059天全域、数组及时间/时间戳/interval
binary、物理逻辑长度、schema及协议相邻入口，不是只数内部断言作为完整测试。

Main、公共头、测试TU与flags未改；核对R124对象的源码、头、编译器、flags、
正常Main receipt及对象字节后，重新连接57份当前O2库，不使用stubs，再经
初始化前隔离器实际执行explain/window/session三个真实Main前端。
这是复用未改TU对象、新链接及新执行，不是本项重编Main或复用旧库二进制。
双缓存、全部58个当前receipt、57份test库字节及源/测试/脚本封印末端复核0。

程序SHA256 `97890f1d8edfe73d8984c02a38f7c4e85374820d6d0ebda800f61df7ef4e892f`，
缓存`320efc2440f7650364c9084554b45d82e3e929154197318dfa8e26e9bbdeaced`，
封印`d2fdf5836dd5b8bb92b70037da02fe6da08c0f8a993403a0dc613dfa326a3c2c`，
公共头/生产flags签名仍`739bf42b71ae53b6f0cfe5e817aec5e81edb16ef16893f6616d93f026f9594c8`。

额外完整重采原122条，120条精确匹配，仅TIME(7)的text/binary两条在warning
及typmod上仍不同；原四个TEMP列观察保持匹配，原97条基本clock/合法precision
观察仍全部精确匹配。`r126-time-precision-after122.log`/comparison及
`r126-time-input-after97.log`/comparison保存采集/比较0，不说成122全过。
TIME(7)及具名类型等普通未完问题继续开放，不改为用户暂缓。

本项独立接受投影547MD、897native+528Python+3Main=1428；工作区550MD、
900+531+3=1434含原六个保留入口。552Python AST/13Bash及文档本地链接/清单
另作静态复核，没有新完整注册回归、真实TLS或整库sanitizer证明。
原56933字节候选补丁及三个草稿保持字节不变，原273勾选/统计仍为
22complete/166partial/70unverified/15deferred，require-complete仍实际1。

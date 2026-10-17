# R125：合法TIME精度的值未转换，表达式typmod丢失

状态：本范围合法TIME(0–6)的转换与声明描述已修复，339项原生/830项协议
以及50个不同完整入口全部通过；按本项独立本地提交，不push。
本项从[R122](issue-review-r122-time-explicit-precision.md)拆出合法0–6的值与
真实声明描述；R123列schema元数据、R124表投影路由已有独立提交。

后续关键字精度语法修复见[R126](issue-review-r126-time-precision-grammar.md)，严格549/326及
52入口全0。再次重采原122仅剩TIME(7)两条warning/typmod差异，原97仍全匹配。
下方保留本项当时12个剩余差异及其范围，不把后续语法修复等同高精度通知完成。

## 缺陷及修改方法

TIME(p)被通用透传，结果文本保留未舍入输入；即使p=6的部分binary datum
正确，RowDescription仍为typmod -1。NULL、空结果和Describe不应靠首行猜precision。

| 路径 | 方法及边界 |
| --- | --- |
| 类型输入/转换 | 复用基本clock的范围校验及整数微秒半偶舍入，再独立执行合法p=0–5的正数半向上舍入；p=6不再量化，保留24时及规范文本 |
| scalar/array | 真实CAST、::、typed literal、text Bind后的TIME/TIME[]转换复用同一输入，不在sender补偿；保留NULL、数组维度和下界 |
| 声明元数据 | 从原AST的CAST/::/typed literal读取声明，使用当前session的只读目录解析真实1083/1183身份和合法modifier，不猜用户OID或执行datum |
| 来源和相邻类型 | 只保留实际TIME声明的precision，类型/长度及物理来源守卫保持；同名表达式别名不能借表列精度，char/bit的既有规则不改 |

默认六位输入舍入与显式precision不是一回事：0.0004996先成为500微秒，
再以p=3成为1000微秒；0.500000在p=0成为1秒，而默认输入半偶规则仍不变。

## 实际复现

永久`tests/time_precision_value_protocol_e2e_test.py`830项在实际PostgreSQL18.6
（180006）全部通过；固定旧程序完整830项全部失败、实际1，不代表830个独立bug。
期望为独立整数向量，检查规范text及真实8字节binary、数组封套/维度/下界、
真实SQL NULL、OID/逻辑长度/precision/来源、完成标签、Ready与协议确认。
覆盖0–6、CAST/::/常量、限定/quoted/具名数组/without-zone、Describe、空结果、
实际1083/1183text Bind、已声明TIME再次缩精度、普通Simple及同名表投影别名。
七类所选坏值校验SQLSTATE，但不以此证明其全部分析/报错阶段正确。

永久`tests/time_precision_value_test.cpp`339项使用真实表达式AST，固定整数
期望及同类scalar/array/NULL/限定声明/维度和boolean/integer/date邻居；完整旧
339项317失败、实际1。Evaluator在main/bootstrap后构造，链接引擎的程序均经
初始化前隔离器，不直接在用户数据目录启动。

日志目录`/tmp/dbms-review-r46.ifu6SWLp/`：`r125-reference830-initial.log`、
`r125-old-native339.log`及`r125-old-protocol830.log`。旧程序
`r125-pre-fix-dbms-main` SHA256：
`e593e92a7ea662f97e8d47f5c277d7deb6e58a54c3d5ee2030c25630077b194b`。
它是R123/R124共享48入口接受的工作区组合，仍有原保留候选，不是clean HEAD。

## 验证计划和仍未完成

第一版只修改ExprEvaluator.cpp、NetworkServer.cpp和本项登记，无公共头/Main
或heap变更。复查时还要求元数据适配器不吞I/O、取消或目录损坏错误；声明
不合法仍由既有分析所有者处理，不以返回-1隐藏真实底层故障。
normal/repeat、339/830完整专项、相邻入口、匹配57库和真实Main运行均已实际
完成，具体终态见下节；独立暂存范围与提交前后只读复核仍分别执行。

R122的负数/小数/多参数/+号语法及报错阶段、过高precision的clamp/warning、
完整TIME输入与转换资格、所有组合表达式typmod、binary Bind接收、旧4字节
整秒TIME heap及完整微秒行存储仍普通开放，不能用本项测试代替这些能力。
原273统计与勾选不提升，明确暂缓项不重开，R98/R99/R109/R106候选不混入；
Actions保持禁用，不push。

## 最终有限验收及复查

首版normal/repeat实际重编2/0个CPP，339/830专项均0；随后收窄声明适配器的
异常捕获，只返回既有声明错误的-1，I/O/取消/目录损坏继续传播，末版
normal/repeat再重编NetworkServer的1/0个CPP，均实际0。
最终22native+25协议+3真实Main=50个不同完整入口全0，包括日期全域、物理
逻辑长度、schema/interval列modifier、数组binary、默认TIME和时间戳等邻居。
Main/测试TU/公共头与flags未变，严格核对R124对象的来源/字节和正常Main
receipt后重新连接57份当前O2库，无stubs，再隔离实际运行三个真实前端。
这是复用未改TU对象及重新连接/执行，不是本项重新编译Main或复用旧库二进制。

`r125-normal-build.log`/repeat、`r125-final-normal-build.log`/repeat、
`r125-c1-native339.log`、`r125-c1-protocol830.log`、`r125-final-*`日志及结果目录、
`r125-real-main-link.log`和`r125-final-independent-check.log`保存完整终态。
双缓存、全部58个当前receipt、57份test库字节及源码封印独立复核0。
程序SHA256 `1a1d677ead64f7507f382ee9a3214709f941a39bc0cb40a66adf22d68b89786d`，
缓存`017c0e1894a5ea8d32ccf1fd5d5d9fe3259c23a1ea6b412c5818d2de7c8d39b3`，
源/测试/脚本封印`9c8ce865cc24ce8da4ce4ea8f65deeab1536061214c5f4f1c20ea1b02791a10b`，
公共头/生产flags签名仍`739bf42b71ae53b6f0cfe5e817aec5e81edb16ef16893f6616d93f026f9594c8`。

额外完整重采原122条观察，110条现在精确匹配，剩余12条仅为precision边界
及NULL坏precision；四个表列观察保持匹配。原97条默认基本clock及合法显式
precision观察全部精确匹配，原21条精度差异消失。
`r125-time-precision-after122.log`/comparison和`r125-time-input-after97.log`/
comparison保存采集/比较实际0；不将前者说成122条全过或全部TIME已完成。

本项接受546MD、896native+527Python+3Main=1426；工作区549MD、
899+530+3=1432含原六个保留入口。551Python AST/13Bash和本地链接/清单另作
静态复核，没有新完整注册回归、真实TLS或整库sanitizer证明。
原56933字节候选补丁和三个草稿保持字节不变，原273勾选/统计不变，
require-complete仍实际1，不把普通剩余问题改记为用户暂缓。

# R121：TIME输入透传导致非法值被接受、文本不规范

状态：默认基本clock文本输入已实现，90个C++/177个协议控制及43个不同完整
入口的有限回归实际全部通过；按本项独立本地提交，不push。
本项只解决默认输入；后续列精度元数据见[R123](issue-review-r123-temporal-column-precision.md)，
合法显式TIME(0–6)的值/声明描述见[R125](issue-review-r125-time-precision-value.md)。后续完整
重采原97条已全匹配，原21条合法精度差异消失；下方旧观察保留其原始输入，
完整语法/资格、物理格式及其他时态能力仍普通开放。

## 实际发现

实际PostgreSQL18.6（180006）与固定0579474b旧程序各97条观察，后在R119/R120
修复并提交后的24b23d5a产物上完整重采同97条：两个项目版本均有89个精确差异，
其中28条应报输入错误却被接受，21条涉及显式精度的值或typmod。采集及比较
退出0只是日志收集成功，不是这些数据库行为通过。

`bad`、越界小时/分钟/秒、负时钟、infinity及超出24时的值可以作为TIME返回，
数组元素同样未校验；短时钟、空白、时区及超微秒文本也未规范。合法binary
输出已由R117实现，但输出器不能修复输入透传，或把拒绝坏binary值当输入正确。

证据目录`/tmp/dbms-review-r46.ifu6SWLp/`：
`r121-time-input-{reference,old,current}97.log`保存全部观察；
`r121-time-input97-comparison.log`及`r121-time-input-current97-comparison.log`
保存精确比较。固定当前`r121-pre-fix-dbms-main` SHA256：
`24b23d5a5b39ae4717c64a810d4c37d4e0dce14ea5a9f3b3c0a7495ec5a6e524`。
该产物与R119/R120共同41入口验收一致，仍包含原保留候选，不是clean HEAD。

## 修改方法及验证计划

| 普通问题 | 方法及边界 |
| --- | --- |
| 默认TIME基本clock输入透传 | 在真实类型输入中校验字段及范围、整数微秒舍入/进位并输出canonical文本；测试常量、CAST、scalar/array和text Bind，不在sender修文本 |
| Bind阶段的真实1083/1183输入 | 复用真实输入coercion，正确传播SQLSTATE和NULL；不能仅在Execute输出时发现坏值，也不猜用户OID |
| 显式TIME精度和描述符 | 对照实际PG的precision舍入及RowDescription typmod，分阶段实现；21条差异另保留，不冒称默认输入修复已完成精度能力 |
| 其他输入语法及cast eligibility | 实际参考审查合法clock/时区和源类型，记录尚未实现的形式，不能用通用原文透传伪装支持 |
| 标量TIME微秒heap和binary Bind | 现有4字节整秒格式与wire8字节不同；物理版本、迁移/重载和真实接收codec另项，不静默改旧行或放宽元数据 |

永久`tests/time_input_normalization_protocol_e2e_test.py`177条控制实际180006
全部通过，固定24b23d5a旧程序完整运行130失败、实际1；包含R117的30组独立
期望微秒（本项检查规范text）、两种offset、七类错误SQLSTATE、scalar/array、
常量/CAST/text Bind、NULL/空/下界/2D/6D、Describe、无行和真实TEMP表短时钟
INSERT RETURNING/SELECT。错误Bind要求在portal description前拒绝，包含
WHERE false不执行结果表达式的控制；不声称这些检查证明所有常量的Parse时机。

永久`tests/time_input_normalization_test.cpp`90控制：同独立期望、错误状态、
NULL/维度/下界/限定别名及boolean/integer/date邻居。完整旧65失败、实际1。
预期时间从已实测参考的整数值独立格式化，不用被测parser计算。测试Evaluator
在main/bootstrap后按函数静态初始化，Engine链接入口经初始化前隔离器运行。
对应日志为`r121-{reference177,old177}-initial.log`与`r121-old-native90-initial.log`。

仅修改ExprEvaluator内部默认基本clock的文本/已声明TIME输入和NetworkServer
真实1083/1183的text Bind：字段/范围、整数半偶舍入/进位、规范文本及真实
输入错误传播。数组沿用原有维度/NULL结构转换；不改公共头、Main、heap或
binary Bind。其他源类型cast eligibility、日期前缀/AM-PM/命名时区等完整输入
形式和常量分析时机尚未完成；这些不是用户暂缓，不冒充完整PG TIME输入支持。

normal/repeat实际各重编2/0个CPP且均0，库和生产均O2。初轮当前native90/
wire177实际全过，`r121-initial-current-native90.log`和`r121-initial-current177.log`
保留输出。邻近TIME/时间戳/INTERVAL/数组/物理长度/视图及默认协议的18native/
22协议/3真实Main共43个不同完整入口实际全部0；使用匹配O2产物、fresh stubs、
初始化前隔离器。`r121-final-{driver,native,core,neighbor}.log`及每个结果目录
保存终态。没有新完整注册回归、真实TLS或整库sanitizer证明。
Main源码/测试/header未变时，可核对预先记录的真实TU对象依赖并重新连接57份
当前正常O2库后隔离执行；这不是重编Main，也不允许复用旧库连接的旧程序。

本项三个Main已核对R120记录的真实TU源码、公共头/flags、正常Main receipt及
对象字节，确认未改，另连接57份当前正常库；explain/session测试TU原为O0、
window及生产库O2，无stubs。三个真实Main入口隔离执行实际均0，连接日志
`r121-real-main-link.log`和三个frontend日志区分对象复用、重新连接与真实执行，
未重编TU或调用原CLI入口。全部58个当前receipt/双缓存/57test-library字节及
最终封印复核0，不复用与旧库连接的旧二进制程序。

冻结产物SHA256 `dd282eeaf175aea11453fc3fa224cb72e6d906a7a038425bd060e747dd73512f`，
正常Bash配置所得缓存 `b6e2f64adc799776444633d6b8eeb5734d0a87121ec9c53b86e862ddae6f636b`，
源/测试/脚本封印 `b0604aa1619c347bab5be0f19501a6e301b1a4420d2f2b3edd1a7dffdfc8f6c6`。
公共头/生产flags签名仍为`739bf42b71ae53b6f0cfe5e817aec5e81edb16ef16893f6616d93f026f9594c8`。
一次只读检查误从zsh加载Bash专用配置，产生的无效签名不用作证据，已用正常
Bash重取；生产构建和所有测试入口始终用Bash，未将该检查错误当产品红灯。

## 复查与仍未完成

额外完整重采原97条观察，默认输入76条差异为0，剩余21条全部为显式typmod；
原28条错误接受不再出现。原R113/R117的32条TIME text/binary观察现全匹配，
原16差异先由R117解决12个binary，本项再解决4个text，历史日志不改写。
`r121-time-input-after97.log`、`r121-time-after-text32.log`和
`r121-final-observations-comparison.log`保留采集/比较真实0；这不表示97条全过。

工作区545MD/548Python AST/13Bash及本地链接/清单另作静态复核；本项接受组合
542MD、894native+3Main+524Python=1421，工作区897+3+527=1427含原保留候选。
只暂存本项17个路径和所属hunk；原56933字节候选源补丁及三个草稿另作字节
复核。原273一致性保持0、require-complete实际1，仍不是总清单完成。

原273仍22complete/166partial/70unverified/15deferred，不提升勾选。R109及原
明确暂缓模块不恢复，R98/R99候选与R106草稿不混入本项；Actions保持禁用。

# R120：闰年年初日期逆换算产生非法月份

状态：已修复；17native/21协议/3真实Main共41个不同完整入口实际全部通过。
先于依赖它的R119时间戳发送候选独立本地提交，不push。

## 问题与影响

Date的天数逆转换DISCONV在闰年起始日期上算错年内天数，月份循环可越过1
并访问DAYS负下标。永久native按日期顺序首次在ordinal1096、0004-01-01失败，
并非仅400年边界。纯Date算子中0399/1599/1999/2399年12月31日加1天
不能返回相应闰年1月1日，日期转换及时间戳跨日规范化受影响。这是普通日期
换算错误，不涉及用户已暂缓的安全/认证、CTAS或对象生命周期模块。

真实180006参考的R119永久232控制全过，旧程序144失败；首版只修发送器后
仍有8个相同的22008输入失败，均涉及1999年末小数秒进位，235低层字节控制
已全过。保留全部断言与首版红灯，不把该问题误称为新增发送器回归。

`/tmp/dbms-review-r46.ifu6SWLp/r120-date-cycle-observations.log`记录11条纯组件
观察，4个400年边界返回非法月份；采集退出0并不意味着这些结果正确。
SQL的DATE+整数路径使用另一套正确转换，原有通过控制保留，不将其误报为
相同的SQL失败；依赖旧DISCONV的时间戳跨日输入才在协议中重现红灯。
后续永久回归、最终构建与匹配产物终态在完成后登记。

## 修改方法与计划验证

以有界的400/100/4/1年Gregorian周期分解替代不安全的年/月反向扫描，最后
从1月开始按真实月长求月日。保留既有1–9999年的民用日期域、越界返回无效
Date及现有heap格式，不把当前四位年份域冒充完整PostgreSQL的BC/大年份范围。

永久C++控制独立枚举1至9999年的全部3652059天，以正向年月日循环作为
期望，而非用被测转换算期望，并覆盖22个端点、400年/普通世纪闰日、正负
移位和int64极值控制。单独以标准库链接的纯组件O2及ASan/UBSan均实际0，
完整覆盖所有天和22控制；这不是整库sanitizer或Engine链接入口已通过的声明。
`r120-pure-{exhaustive,sanitized}.log`保留输出。

152协议控制实际PG180006全过，旧c1b02043程序完整运行有48个22008失败，
实际1；96个SQL日期控制走另一条正确计算路径，均保留原断言。native旧入口
在第1096天实际退出1，未声称旧程序完成全域枚举。日志为
`r120-{old-native-initial,reference152-initial,old152-initial}.log`。
旧固定产物SHA256 `c1b020432223b229c49ff950db9a2295db7020271b9d06a334e0a01e70900809`，
包含未提交的R119 sender和原保留候选，不冒充clean HEAD。

修改公共DateType.h后，必须重建并核实全部58个生产编译单元，O2及实际flags
保持一致；测试使用fresh stubs及初始化前隔离器。三个真实Main TU另行编译、
隔离执行，编译退出0不作测试通过证明。不恢复用户暂缓项，不push或启用Actions。

本项已完成全部58个生产TU的本地fresh O2重建（最多4个编译器并行），逐个真实
编译退出0并按正常生产者格式原子发布receipt；正常生产link/repeat均0，之后
`scripts/build.sh` normal/repeat均实际0且无需再编CPP，不把fresh58误记为后两次
命令编译了58个CPP。`r120-fresh58-build.log`及normal/repeat日志保留实际输出。
三个真实Main源TU直接编译为本项自有对象，explain/session测试TU O0、window
测试TU及全部生产库O2；它们的编译与生产构建并行，连接只在58份生产receipt
匹配后进行，使用57份正常O2库、无stubs。三个真实入口初始化前隔离执行实际
均0，不调用重命名的原程序入口，不把编译/连接0当作执行0。

冻结生产SHA256 `24b23d5a5b39ae4717c64a810d4c37d4e0dce14ea5a9f3b3c0a7495ec5a6e524`，
缓存签名 `ba206d2af2b3c31427ccc6347eb8ce14f9e5d9f787c43beb174b1af20ccf419b`，
源/测试/脚本封印 `1c6e6e034d731bb9d627a1b2048220ac9058ab8a1d0ec1397962b91436763436`。
生产/头/flags签名 `739bf42b71ae53b6f0cfe5e817aec5e81edb16ef16893f6616d93f026f9594c8`。
核心完整协议中日期152/时间戳232/TIME144/INTERVAL171均实际0：R119原8个跨日
失败在保留全部断言后消失。17个native（含全域Date、235时间戳、121TIME、117
INTERVAL及102物理长度）、21个协议及3个真实Main共41个不同完整入口实际
全部0；`r120-final-{driver,native,core,neighbor}.log`及每个结果目录保存终态。
`r120-real-main-{object-compile,link}.log`区分编译/连接，三个frontend日志记录
真实执行。双缓存、58receipt、57test-library字节和最终封印复核均0。

工作区全部543MD/547Python AST/13Bash、本地链接及注册清单静态检查0，原273
一致性0但require-complete实际1，表示总账仍未完成。提交前后另作范围/字节
复核，不把这些静态检查当完整运行测试。

本项单独接受组合540MD、892native+3Main+522Python=1417；工作区543MD、
896+3+526=1425含尚未提交R119及R98/R109候选。只暂存本项13个路径；R119
三个源/测试文件另以SHA256保留，原56933字节候选补丁及三草稿也独立复核。
无新完整注册回归、真实TLS或整库sanitizer证明，原总清单并未全完。

原273清单仍22complete/166partial/70unverified/15deferred；本项不提升勾选。
全范围时间精度/输入/typmod/BC、R111规划、目录SQL及视图origin等仍普通开放。

本页记录的R119首版候选及依赖关系保留；后续sender另由
[R119](issue-review-r119-timestamp-binary-result.md)独立验收、提交，不改写本项原终态及接受计数。

## 本轮同时记录的后续普通问题

TIME输入默认落入evalCast的原文透传，binary sender严格解析不等于输入已校验。
额外实际PG180006/固定0579474b旧程序各97条观察，精确比较89个差异，其中28条
PG拒绝而旧程序成功接受，21条涉及显式精度修饰；采集/比较实际0不是结果全过。
`r121-time-input-{reference,old}97.log`及`r121-time-input97-comparison.log`保留
全部观察。该旧产物是已接受R117组合，非本项新日期产物，不更换输入来源。
后续方法：实现所属类型真实输入校验、整数微秒舍入/进位和typmod阶段的规范化，
分别测试scalar/array/常量/text Bind；4字节标量heap升级仍另项，不能依赖输出器
拒绝坏值。用户暂缓项不扩大到这些普通问题，也不与R120日期提交混合处理。
后续默认基本clock/text Bind由[R121](issue-review-r121-time-input-normalization.md)
单独修复，97重采仅余21个显式精度差异；完整grammar/cast eligibility和物理升级仍普通开放。

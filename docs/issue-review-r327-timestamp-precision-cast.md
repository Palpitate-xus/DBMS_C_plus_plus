# R327：TIMESTAMP 精度值转换未分派到真实 codec

状态：TIMESTAMP(p)/TIMESTAMPTZ(p) 的值分派、精度舍入和规范 datum 类型已修复。
仅完成下述有限验证；共享 SELECT 描述、TIME/TIMETZ/INTERVAL 的普通 VALUES
准入、TIMETZ codec/二进制输出及原 273 总清单均不能据此记为全部完成。

| 缺陷 | 原因 | 修改方法 |
| --- | --- | --- |
| 精度被忽略，SELECT 原样返回未舍入文本 | 带修饰符的 timestamp(p) 字符串与裸 timestamp 比较，未进入已有 codec | 使用真实已解析内建身份分派，单独解析、应用精度 |
| VALUES 外层共同转换报 42846 | 上一步把 timestamp(p) 当作 datum 类型名，下一步不把它认作 timestamp 来源 | 返回真实 codec 的规范 timestamp/timestamptz datum，不把声明修饰符塞进类型身份 |
| 精度调整必须保留日期、时区和 NULL | 不可按字符串删小数或通过 double 四舍五入 | 复用已有整数微秒 parse/round/format；正常化后按 PG epoch 舍入；SQL 精度大于 6 沿既有 TIME 规则归一到 6，注册表仍严格 |

只修改 ExprEvaluator.cpp 和测试注册，包含既有 TemporalStorageCodec.h。
没有修改公共头、Main、协议、binder、存储、规划器或常量载体；没有修复
暂缓的 CASE/EXTRACT 等角色。普通 bare timestamp 路径和原有 NULL 路径保留。
本项不放行新的 VALUES 类型，R341/R342 的原有拒绝仍明确报告。

## 实际验证

实际 PostgreSQL 18.6 全新私有 cluster 采集冻结 1806 请求，另一个全新
cluster 严格重放实际 exit0。原 1549 请求和 golden 为精确前缀，更早
1428/1239/1133/965/777/304 从实际完整日志核验，不算成独立重跑程序。
新增 257 请求覆盖 SELECT 和 VALUES、相同／不同声明、typed NULL、参数、
TIME/TIMETZ/INTERVAL/TIMESTAMP、simple/extended/binary/Describe-only 及序列
观察。包含真实错误参考（例如命名物理 interval(3)）而不是强制它们成功。
协议逐字段核对消息阶段、SQLSTATE、描述、参数、DataRow、command、Ready。

| 检查 | 修改前 | 最终结果 |
| --- | --- | --- |
| Root projection 完整 1806 | 272 个请求差异 | 255；17 个原失败消除 |
| accepted 完整 1806 | 317 个请求差异 | 300；相同 17 个原失败消除 |
| 实际改变的完整签名 | — | 每侧相同 23 请求改善；另 6 个 SELECT 执行只修正数据，既有描述差异保留 |
| 原 1549 / 1428 前缀 | Root112 / accepted154 | Root101 / accepted143；原先 11 个 42846 消除 |
| 原 1239 / 1133 / 965 / 777 / 304 | Root71/65/61/55/55，accepted113/107/65/55/55 | 完整签名不变 |
| 新增原生实际 PG 六个标量值／NULL | 每侧 18 断言、8 失败 | 每侧相同 18 断言、0 失败 |
| 旧冻结的 630 数组构造器协议族 | 每侧 110 请求差异 | 每侧 58；52 请求完整匹配 PG，另 6 请求数据修正后只剩描述差异 |

没有新增失败索引，所有先前正确的完整签名不变。普通 SELECT 的六个新
执行只有 DataRow 改变且数据全部匹配 PG；没有把未修复的描述隐藏掉。
原生 fixture 分别断言 datum 类型、NULL 和完整值，覆盖 TIMESTAMP(2)、
TIMESTAMPTZ(2) 及两个显式 offset。它不是 0..6 各精度、所有日期范围、
全部 source 类型或全部时区规则的穷尽证明。参考、断言均未按候选结果改写。

实际配对 204 次：182 次 exit0，22 次已知非零。76 个当前 native/Main
fixture 全部实际运行，8 个真实 Main 对象逐依赖匹配复用、不链接 stub；
74 个旧测试对象匹配，新测试两个对象先用修复前库编译链接，同一对象再
链接当前库。全部 76 个当前原生链接新建；不是只跑新测试。
原有 32 SQL 的 Main 测试仍 211 断言、0 失败；R332 的原生失败保留；
12 个其他非前缀协议族完整期望行不变，包括 CHAR131/common168 差异。
630 族中，既有数组元素 CAST 同样调用本项 codec，因而有 58 个签名正确
改善（含精度 1/3、混合声明、嵌套数组及参数）；没有修改数组函数、数组列
写入或新增查询角色。全部 52 个完整改善逐字段匹配其既有冻结 PG 期望，
其余 6 个数据匹配而只剩 R330 描述问题；所有原来正确的完整签名不变。
最初“所有旧族签名均不变”的核验实际退出，原检查及日志保留；最终门禁
逐条证明这些变化正确，并非移除检查、改 golden 或恢复 R332/R334。

局部 UBSan 实跑 32 次：20 次 exit0，12 次已知非零，无项目 UB 诊断；
完整 1806 及其余协议输出与普通版相同。相同 libubsan 故意溢出 canary
实测持久 log_path 捕获有效，不是全工程 UB 证明。Main 及包含 Main 的
fixture 保留历史明确 O0，其他插桩对象 O1，正常生产依然 O2。
普通版新编 evaluator 两个生产 TU，匹配其他 120 个及两个 stub；局部
新编四、匹配 38 个对象，共 42 对象用于 20 原生和两个服务器链接。
实际 242 份 MMD、源／头／对象、编译器及完整 argv 全部核验。源码快照
数量不代表每个文件都执行过；没有重跑整套历史 1492 或全部原生测试。

## 本次发现／保留的问题与后续方法

| 事项 | 实际证据 | 后续修改方法 |
| --- | --- | --- |
| R330 共享 SELECT 时间描述修饰符 | 新 SELECT timestamp(p)/timestamptz(p) 数据已正确，直接描述及执行描述仍丢精度；TIMETZ 也受影响 | 让真实 CAST 声明及 OID 拥有修饰符；独立修复描述，不借此恢复 CASE/完整计划拥有者 |
| R343 TIMETZ 标量输入／精度 | 非空 timetz(p) 仍原样输出未舍入文本，短时间字段也未正常化 | 独立实现真实 TIMETZ scalar codec 和规范 datum 类型，验证固定 offset、舍入、24h、NULL 与错误输入 |
| R344 TIMETZ 二进制输出 | 非空 TIMETZ 的 binary SELECT 仍 0A000，OID1266 没有编码分支 | 在真实二进制类型支持及编码处补充、核对 12 字节 clock/zone 表示，不伪造 text format |
| R341 TIME/TIMETZ/INTERVAL 的 VALUES 准入 | 原 12 请求及新增 TIME 等控制仍 0A000；本项未修改 Main | 验证各真实 codec 后扩展共同执行准入；描述正确不等于值执行正确 |
| R342 ARRAY 的 VALUES 执行 | constructor 的表达式拒绝和 typed NULL 的目标类型拒绝仍存在 | 独立处理普通 datum-array 两个入口，保持 R332 数组函数／R334 数组列写入暂缓 |
| 命名 interval(3) 的已有阶段差异 | 真实 PG 的非法声明报 22023；部分 Describe 或扩展阶段不同 | 属已有 R244/R303 等规划／阶段暂缓，不改 golden、不借本项恢复 |
| R331 及其余旧差异 | 所有非本项完整签名保留 | 按原记录继续独立修复 |

本项仅提交 evaluator、测试注册、新 native、协议驱动、冻结参考和本文共
六路径。Main 和全部公共头不变，其他工作区文件及原暂存 18 路径保留。
Root 的 R322 formatter/DOUBLEABS 等暂缓区域通过本项精确逆补丁验证，未
编译／提交整份物理 Root；R332/R334 草稿哈希不变，不恢复任何早先跳过项。
不 git push，不启用 Actions。原 273 仍未完成，完成门禁实际 exit1；
历史 1492 的 1481/11 不是当前 HEAD 重跑结果。

原生：bash scripts/build_one_test.sh timestamp_precision_cast_test。
协议：python3 -B tests/temporal_value_cast_protocol_e2e_test.py（DBMS_MAIN 指向
目标服务器）。完整严格驱动仍报告保留差异，不把本项接受当成全部通过。
证据：/tmp/dbms-r341-values-temporal.Q0fDyh2N/。

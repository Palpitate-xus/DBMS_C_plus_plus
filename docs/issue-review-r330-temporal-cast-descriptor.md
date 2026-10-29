# R330：真实时间 CAST 声明精度丢失于协议描述

状态：下列普通直接 CAST／类型字面量的时间修饰符适配已修复，完成有限验证。
不是所有表达式载体、全部时间类型执行／二进制功能或原 273 总清单完成。

| 缺陷 | 原因 | 修改方法 |
| --- | --- | --- |
| TIMESTAMP/TIMESTAMPTZ/TIMETZ 的 CAST(p) 描述为 -1 | 旧声明适配只识别 TIME，结果类型白名单也遗漏其他时间家族 | 用实际内建 scalar/array OID 选择精度 codec，描述与准备结果均接收真实声明 |
| 精度 0 和外层无精度 CAST 需要明确表示 | 不能把 0 当作缺失，也不能把 -1 当成未找到声明 | 返回有值 optional，包括真实的 -1；独立传递声明 OID，只有匹配结果真实身份才赋值 |
| 扩充元数据不可误改 TIME 警告 | 原 TIME helper 同时服务警告和描述 | 警告保留原 TIME-only wrapper；通用元数据 helper 单独使用其他时间家族，不把 TIMESTAMP 警告错误命名为 TIME |

只修改 NetworkServer.cpp 与注册。没有新公共头、Main、binder、evaluator、
存储或规划器修改。描述不执行值／函数体，不从数据长度推测，不缓存或
覆写 portal OID；仅直接真实 CAST 和类型字面量，CASE、函数／查询子角色、
完整常量／规划载体继续由原拥有者处理，不恢复用户跳过模块。
声明语义错误仍交给原分析路径；I/O、中断、损坏错误继续传播。

## 实际验证

实际 PostgreSQL 18.6 全新私有 cluster 冻结最终 2362 请求；再用另一个全新
cluster 严格重放实际 exit0。原 1806 请求及 golden 为精确前缀，更早
1549/1428/1239/1133/965/777/304 从完整实际日志核验，不算额外独立运行。
新增 556 控制包括四类时间的 0..6、精度 7、NULL／CAST／类型字面量、schema
与引号、物理数组别名、标准长拼写、参数、外层嵌套 CAST、simple/extended/
binary/Describe-only 和序列。参考实测确认：外层显式无精度 CAST 输出 -1。
初轮 2298 原始采集后、产品修改和 golden 冻结前补充嵌套声明控制；初轮日志
全部保留。最终 2362 重新初始化私有 cluster，不复用已运行的序列／表状态。
两轮 raw collector 的 exit0 只是完整采集，不是数据库语义全部通过。

| 检查 | 修改前 | 最终结果 |
| --- | --- | --- |
| Root projection 完整 2362 | 619 请求差异 | 282；337 原失败消除 |
| accepted 完整 2362 | 664 请求差异 | 327；337 相同原失败消除 |
| 实际签名变化 | — | 两侧各 385；只改变 RowDescription 的真实时间 typeModifier 字节 |
| 修改后仍未完全匹配的改变请求 | — | 两侧各 48；修正元数据后保留独立值、binary、警告问题 |
| 旧 630 构造器协议族 | 58 差异 | 52；6 消除 |

没有新增失败索引，所有原来正确的完整签名不变。逐列解码证明列名、table/
attribute、OID、typeSize、format 均不变，改变的修饰符匹配真实 PG；数据、
SQLSTATE、参数描述、消息顺序、command、Ready 不变。新增有效控制中不再
有只差 descriptions 的请求；不能据此把有数据／binary／警告差异的请求
也记为通过。序列控制同时保留实际引用值与 Describe-only 不执行观察。

原生／协议配对实际 204 次：184 次 exit0、20 次已知非零。76 个当前
native/Main fixture 全部实际执行，包含 8 个不链接 stub 的真实 Main；
76 个测试对象逐源／头匹配复用，76 个当前链接都新建并消费新 Network。
原有真实 Main 仍 211 断言 0 失败，TIMESTAMP 原生仍 18 断言 0 失败；
R332 的数组函数声明失败完整保留。其他非前缀协议族的签名逐项核验，
630 族的元数据改善单独对照其冻结 PG 期望，不改测试／golden 迎合结果。

局部 UBSan 实跑 32 次：20 次 exit0、12 次已知非零，无项目 UB 诊断，
完整协议输出与普通版一致。相同 libubsan 的故意溢出 canary 实测持久
log_path 捕获有效，不是全工程 UB 证明。正常生产仍 O2；局部 Main 和
包含 Main 的旧 fixture 保持明确 O0，其他插桩对象 O1，按对象保存 flags。
普通版新编 Network 两个 TU，匹配其他 120 个及两 stub；局部新编两、
匹配 40 个，共 42 插桩对象用于 20 原生与两个服务器链接。实际 242 份
MMD、源／头／对象、编译器与完整 argv 核验，源码快照不等于运行覆盖。

## 仍保留的问题与后续方法

| 事项 | 实际剩余 | 后续修改方法 |
| --- | --- | --- |
| R343 TIMETZ 标量值／精度 | 类型描述修正后，部分非空 timetz(p) 仍原样输出未舍入时间 | 独立实现实际 TIMETZ codec、规范类型、固定 offset 与微秒舍入，保留时间范围／NULL／错误输入验证 |
| R344 TIMETZ 非空 binary 输出 | 仍有 OID1266 的 0A000 | 补真实 12 字节 clock/seconds-west 编码与支持检查，不能回退成文本格式掩盖 |
| R350 非 TIME 声明精度 >6 的警告 | TIMESTAMP/TIMESTAMPTZ/TIMETZ 的真实 typmod 已归一为 6，但部分 N 警告帧仍缺失 | 在现有直接声明警告拥有者范围内核验准确类型名、阶段和次数，不恢复暂缓的完整规划／阶段架构 |
| R341 普通 VALUES 时间类型准入 | TIME/TIMETZ/INTERVAL 仍 0A000 | 先完成底层值／binary，再独立扩充准入并验证一次求值 |
| R342 普通 datum ARRAY VALUES | constructor 表达式拒绝和 array 目标拒绝都还存在 | 独立处理普通数组 datum 入口，不恢复 R332 数组函数或 R334 数组列写入 |
| R331 及更早差异 | 本项不改变其实现 | 依原记录继续复现修复 |
| 已有错误阶段、CASE／完整 carrier 等 | 原暂缓项完整保留；物理 interval(3) 的真实 22023 不改写成成功 | 不借元数据修复恢复 R244/R303/R156/R238 等用户跳过内容 |

本项仅提交 network、注册、协议驱动、冻结参考和本文共五路径。其他工作区
文件、原暂存 18 路径及 R332/R334 草稿哈希保留；R322 值格式化区域不变。
只使用已接受快照及完整 Root projection，不编译／提交整份物理 Root。
不 push，不启用 Actions。原 273 未完成，完成门禁实际 exit1；历史 1492
的 1481/11 不代表当前 HEAD 重跑。

运行：python3 -B tests/temporal_cast_descriptor_protocol_e2e_test.py，DBMS_MAIN
指向目标服务器。完整严格驱动仍报告保留差异，不把本项接受当成全绿。
证据：/tmp/dbms-r330-temporal-descriptor.ir0ERXvg/。

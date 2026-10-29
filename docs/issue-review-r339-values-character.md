# R339：普通 VALUES 的 CHAR 共同值截断

状态：普通 VALUES 的无长度限制 CHAR 共同值转换已修复，完成下列有限验证。
不是整个 VALUES、所有执行类型或原 273 总清单全部完成。

| 缺陷 | 原因 | 修改方法 |
| --- | --- | --- |
| 已转换为 CHAR(n) 的值仍被截成一个字符 | 普通 VALUES 为共同类型额外包 CAST AS character；关键词省略长度等于 CHAR(1)，不是共同类型的无 typmod 转换 | 在这个共同值转换点使用真实物理内建 pg_catalog.bpchar，与已修复的 BIT 无 typmod 处理一致 |
| 原值的显式长度与空格不能被重写 | CHAR(n) 的截断／padding 已由内层真实 CAST 决定 | 保留内层 CAST 和现有 codec，仅取消外层错误的默认长度 |

只修改 Main 的普通 VALUES 共同转换及测试注册。不修改 evaluator、binder、
协议、公共头、存储、赋值规则或规划器。R340 的真实共同 typeMod 描述保持，
不按数据长度猜类型，不用调整协议 OID 掩盖值错误；不恢复用户跳过项。

## 实际验证

实际 PostgreSQL 18.6 全新私有 cluster 先采集并冻结 1549 请求，另一个全新
cluster 严格重放实际 exit0。原 1428 请求及 golden 为精确前缀，更早
1239/1133/965/777/304 从实际完整日志核验，未改写既有参考。
新增 121 请求覆盖 simple、extended、binary、Describe-only：
相同／不同 CHAR(n)、关键词 CHAR 默认长度、引号／schema bpchar、NULL、
空串、保留空格、UTF-8 多字节／emoji、UNKNOWN 与 VARCHAR 顺序、整数／
BOOL 显式 CAST、多列、typed 参数及成功输入一次求值。
逐字段检查消息顺序、SQLSTATE、列／参数描述、文本／二进制 DataRow、
command 和 ReadyForQuery，不只比较打印文本。

| 检查 | 修改前 | 最终结果 |
| --- | --- | --- |
| Root projection 完整 1549 | 184 个请求差异 | 112；72 个原失败消除 |
| accepted 完整 1549 | 226 个请求差异 | 154；相同 72 个原失败消除 |
| 原 1428 前缀 | Root121 / accepted163 | Root112 / accepted154；9 个 CHAR 值差异消除 |
| 原 1239 前缀 | Root74 / accepted116 | Root71 / accepted113；3 个 CHAR 值差异消除 |
| 原 1133 / 965 / 777 / 304 | Root65/61/55/55，accepted107/65/55/55 | 完整签名不变 |
| 新增 121 请求 | 63 个差异 | 全部匹配实际 PG，包括 30 个新增 Describe-only |
| 真实 Main 原有 32 个实际 PG SQL 及 API 断言 | 每侧 211 断言、1 个 CHAR complete values 失败 | 每侧相同 211 断言、0 失败，无删减或改期望 |

没有新增失败索引，所有原来正确的完整签名不变。72 个改变的请求只改变
DataRow 值，且全部转为匹配 PG；描述、类型、参数、错误码、格式和消息阶段
均不变。真正的 keyword CHAR 默认 CHAR(1) 和内部 CHAR(n) CAST 仍按 PG
执行。Describe-only 后序列 currval 仍为 1，三次真正执行后为 2/3/4。
这只是本组成功输入的一次求值证据，不是所有错误阶段／回滚语义证明。

实际配对 200 次：178 次 exit0、22 次已知非零。74 个当前 native/Main
fixture 全部实际运行，其中 8 个真实 Main 测试重新编译、链接，不链接 stub；
其余 66 个非 Main 测试逐源／头／对象／完整链接 argv 匹配，复用完全相同的
当前二进制，不伪称重新链接。R332 的数组函数声明失败保持，13 个非前缀
协议族前后完整期望行不变，包括 630/CHAR131/common168 的既有差异。

局部 UBSan 实跑 30 次：18 次 exit0、12 次已知非零，无项目 UB 诊断；
普通版与插桩版完整协议签名一致。相同 libubsan 故意溢出 canary 实测
持久 log_path 捕获有效，不是全工程 UB 证明。正常生产仍 O2；局部 Main
及包含 Main 的 fixture 明确使用 O0，其余插桩对象 O1，每个对象真实 flags
保存并单独核验，不把混合配置说成统一 O1。

普通版新编 Main 2 个生产 TU，匹配复用其他 120 个及 2 stub。8 个真实
Main fixture 新编／新链接，其余 66 个对象及二进制匹配复用；局部新编
4、匹配 36 个插桩对象，共 40 对象用于 18 原生和 2 服务器链接。
实际 238 份 MMD、源／头／对象、编译器及完整 argv 全部核验，源码清单数量
不冒充运行覆盖。早期一次采集调用因预检副本尚未完成、receipt 未发布而
立即退出，未启动参考或任何运行；预检终态后原样重新调用并完整完成。

## 保留的独立缺陷与后续修改方法

| 事项 | 本次仍保留的证据 | 后续方法 |
| --- | --- | --- |
| R327 时间输入／转换 | 原 R340 新增组中 11 请求仍为 42846 | 单独追踪 timestamp/timestamptz 输入 codec 与共同转换，不动已跳过的 EXTRACT 模块 |
| R341 TIME/TIMETZ/INTERVAL 的 VALUES 执行准入 | 12 请求仍为 0A000，只描述正确 | 独立核验普通 VALUES 准入及真实值表示，补执行与协议验证 |
| R342 ARRAY 的 VALUES 执行 | 18 请求仍为 0A000，只描述正确 | 独立核验共同数组类型及结果载体；不恢复 R332 数组函数或 R334 数组列写入 |
| 既有规划报错阶段 | 原 R338 组剩余 6 请求仍有消息／参数描述差异，SQLSTATE 正确 | 依用户 R244/R303 等暂缓，不借本项恢复 |
| R330/R331 及更早不属于本项的故障 | 本项不修改其实现或宣称完成 | 按原记录独立复现、测试和提交 |

所有更早跳过项及 R332/R334 草稿哈希不变。本项仅提交 Main、测试注册、
协议驱动、冻结参考和本文共 5 路径；保留其他工作区文件及原暂存 18 路径。
不 git push，不启用 GitHub Actions。原 273 仍未完成，完成门禁实际 exit1；
历史 1492 的 1481/11 不代表本次当前 HEAD 重跑。

运行：python3 -B tests/values_character_result_protocol_e2e_test.py（DBMS_MAIN
指向目标服务器）。完整严格驱动仍报告未修复差异，不把接受本项当成全绿。
证据：/tmp/dbms-r339-values-character.LnihEEI2/。

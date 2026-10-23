# R238：INTERVAL 输入上下文与已声明值转换

状态：原生/普通值转换已修复并验证；完整协议矩阵仍有独立差异，不能宣称全绿。

## 已复现的缺陷和修改方法

R237 原完整 360 项暴露 `CAST(CAST(nextval(...) AS text) AS interval DAY)`
错返回一天；实际 PG 将已有 TEXT '1' 先解析为一秒，再被 DAY 截成零。旧 interval
回调对所有裸数字一律附加目标最小单位，没有区分 input 与已有类型的转换。

修复保留真实角色，而不是修改数据的类型名：CAST/:: 将原输入 AST 传给已有私有
转换器；只有真正 UNKNOWN datum 或原始无类型 quoted leaf 才使用目标 input 字段。
已声明/已规划值不被重新当成 UNKNOWN，函数/列/参数也不通过内容猜测角色。
typed interval literal 原有 UNKNOWN 输入规则保留；没有重做 R156 完整常量载体。

数组与标量不同：UNKNOWN 数组文本中的 `1` 也先按普通 interval 输入为一秒，再
应用 element typmod。因此 interval 数组先执行无修饰元素输入，再执行目标字段/
precision 转换，保留实际 source element、维度、非一 下界和 NULL。静态 binder 与
native evaluator 同时拒绝缺失的 integer/bool/array cast，包括 NULL/空数组，
不能靠没有数据绕过类型资格；其他类型的数组规则和文件格式不变。

依据：[PostgreSQL 类型转换规则](https://www.postgresql.org/docs/18/typeconv-overview.html)、
[interval 输入与字段](https://www.postgresql.org/docs/18/datatype-datetime.html)及实际18.6。

## 全部实际结果，不删减失败

| 完整检查 | 旧版 Root / 纯提交 | 当前 Root / 纯提交 |
| --- | --- | --- |
| 新原生 696 | 各 311 个断言失败，退出1 | 各零失败，退出0 |
| 新协议 2863 | 各 1029 个断言失败，退出1 | 各剩114个差异，退出1 |
| 原 R237 360、R236 143 | R237当时各剩一个值错误 | 两侧均全部通过 |
| 每侧既有8原生/16协议 | 本轮不作旧版全回归声明 | 全部48次通过 |
| 同一完整新协议，对真实PG18.6 | 修正夹具后2863/0 | 私有实例正常停止，默认fsync |

最终 56 次完整程序/驱动中54退出0、两个完整2863各退出1；不报56全绿。
696值/错误 corpus 来自真实 PG180006，保存完整 text/binary/Describe 数据、OID、
modifier、ParseComplete与SQLSTATE，native嵌入同一不可变值记录；不是手写产品期望。
311/1029/114是重复控制失败数，不是独立缺陷数量，也不等于整个类型族完成。

首轮协议验证有12个夹具错误：合法的 reached TEXT 输入错误会先发送 RowDescription
再报错，旧 predicate 却禁止一切描述符。只将断言改成要求已捕获的精确 PG metadata，
全部SQL、gold/native数据、SQLSTATE与2863控制均未变；V1源码、日志与失败收据保留。
修正后再取得完整PG2863/0及旧版两侧1029失败，未通过修改gold迎合产品。

覆盖13组字段、正负/小数、CAST/::、真实UNKNOWN和TEXT/VARCHAR/BPCHAR/NAME/
interval/time、数组、CASE/文本函数、精度/infinity/错误和不支持的声明。协议另有
Simple/Extended text/binary/Describe、typed参数、实际表行/NULL/空输入/LIMIT0、
序列元数据不执行与实际单次执行。NAME源通过CAST/参数验证，不恢复R214物理NAME DDL。

## 未修复差异及后续修改方向

两侧114个剩余控制的标签均已在旧1029失败中出现，没有新增失败标签；完整保留：

| 后续记录 | 两侧各控制数 | 证据与需要的修改 |
| --- | --- | --- |
| R239 分析/错误阶段 | 37 | 无FROM描述器未执行真实CAST分析：UNKNOWN坏输入与不兼容类型在Parse时被错误接受；另有Simple runtime错误遗漏已知RowDescription。应复用并保留实际查询分析/结果声明，不能用SQL字符串特判或吞错误替代。 |
| R240 CASE planned typmod | 75 | 返回值字节均正确，但常量CASE在Simple/执行阶段丢失折叠分支的typmod；PG statement Describe反而应为-1。必须分别管理静态与实际规划输出，不能把分支修饰符一律塞给Parse。不得借此恢复R156/R217暂缓模块。 |
| R241 NAME参数 | 2 | supplied OID19在两种Execute结果格式下报42846，PG应返回零interval。实际参数类型/绑定路径仍待诊断，不能猜根因或靠NAME物理DDL绕过。 |

这不是将失败排除出完成门；整个新2863仍判失败，上述问题不能计作修复或将TYPE-06/
SQL/PROTO整项关闭。R106显式地址CAST专项、R156完整常量载体及其他暂缓不恢复。

## 构建和版本记录

两侧各fresh ExprEvaluator/query_binding、新native及8既有native；其他自己的对象/
编译器/头/stub核对后复用并新链接。Root62生产对象/134头，纯提交60/131，布局不混。
Root Main仍用明确pre-R229匹配donor，不编译/运行跳过草稿或默认旧dbms_main。
不是normal/repeat、全部fresh、全工作区源码、全套、TLS或sanitizer验收。

回归包括原完整136 cast input、field literal、column modifier、typed range、输入
错误/存储/赋值/INSERT SELECT、算术/日历、CASE、binary arrays及TIME/CHAR/BIT。
所有旧超时、持久化选项、SQL和断言保留。编译回归与primary并行后，曾仅暂停自有
gate进程，等primary完整收据发布后恢复，避免锁释放与收据发布之间的读取竞态；
没有终止任何编译或runtime，没有改测试源以取得通过，协调收据独立保存。

2261纯源码输入与实际有红验收组合一致；944 native+593 Python+3 Main=1540仅清单。
独立index提交本项九路径，原暂存18/其他工作区以及未实施R235的夹具/注册保留。
原273和冻结1492历史不重写，总目标未完成。R233及其他暂缓不恢复，不push，Actions禁用。

本地证据目录：`/tmp/dbms-r238-interval-input-context.zJpQQPVJ`。

| 记录 | SHA256 |
| --- | --- |
| 真实PG不可变corpus | `74b03079b5d9c2092e1f489e109039f654c4f3b70443982831aa99d3c14d646e` |
| 初次PG协议含12夹具错误 | `1a0574601e168438b86b8b0bec05da7ac16ec435a760019c87fb9fe6ecb284e0` |
| 精确PG错误metadata夹具修正 | `f6e7b5ae2408fccecf4c0ba156059b43d5c2f85e672c474bcc980d352034fbba` |
| 完整PG2863 | `65523a00dd2274203937a4ac0b7e4ac7a70c88c8d8ae3482bce109301ed9ec4c` |
| 完整旧基线 | `3cf3cb5def28739a07885f9bd2aaf8b1452d424265fada9158bf689e7ec238cf` |
| 实际构建 | `74918780618640ab2a81268b9eb17de814a1eeb438aaf9f1ea54f9f25ed18062` |
| 完整primary | `d5d0bc46c67e36b4df42ffeb5be103671dc870a7c85ab99c9e79c7790f501fe8` |
| 剩余114分类及旧标签核实 | `a3cbd24c9688310c1a68a18a43481ac0e9f8225e7286f354d03a9cdcbde365dd` |
| 运行协调 | `f8a429462e404953ac86200ace7154e322d14e977b2f4f8fa885b4c5f51013af` |
| 完整56次运行仍有红 | `7ebe087f4328c2055af8defbd0690aff59a9c1209da9e0c86d9e093e23406418` |

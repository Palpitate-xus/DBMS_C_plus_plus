# R47：BIT 与整数 cast 的类型关系和二进制值

## 问题与修复

旧工作区未登记的完整160条协议在本机实际 PostgreSQL 18.6 全过；主分支
`f4087f5d` 首条 `B''::smallint` 返回22P02，而实际应在类型检查时拒绝为42846。
之前将 bit 内容按十进制解析，也会错误处理空串、二进制数值及满宽符号。

显式 BIT 仅与 INT4/INT8 具有这组直接数值 cast；INT2 或 VARBIT 不因此获得
该关系。两种保留 cast AST 语法在纯绑定阶段检查，包括 typed NULL、空来源及
dead CASE。检查只用 typed NULL 参数，不求值用户 operand/routine。执行器同步
检查直接原生求值路径，NULL 不能绕过缺失的类型关系。

允许的 BIT→INTEGER/BIGINT 按二进制累积，空 bit 为0；只有恰好32/64位才
按相应有符号宽度解释。超过目标位宽即22003，包括全零前缀；转换避免有符号
溢出及实现定义的 unsigned→signed 窄化。BYTEA、文本、数值舍入及BOOLEAN
相邻 cast 仍保留各自语义。

## 独立验证

证据目录 `/tmp/dbms-review-r46.ifu6SWLp/`：

| 检查 | 实际结果 |
| --- | --- |
| 原完整160协议，实际PG18.6 | 全0，legacy-bit-postgres-reference.log |
| 原160/扩展172输入，主分支 | 首条B''::smallint的22P02与42846不符，未声称跑完全部控制 |
| 新原生 | 174条值/宽度/符号/空串/NULL/绑定与相邻控制全0，r47-first-native.log |
| 同一160协议，候选 | 全0，r47-final160-protocol.log |
| 最终完整原生组合 | 十个不同完整驱动全0，r47-first-native.log / r47-additional-native.log |
| 八个完整协议邻居 | 全0，r47-final-protocols.log；另有新160协议，共九个完整入口 |
| 标准生产构建 | 两个变化CPP实际O2重编/链接全0，r47-first-standard-build.log；全部58份当前源/头/flags receipts核对 |

邻居包括 BIT 输入/参数/BETWEEN/ARRAY、BOOLEAN→character、NUMERIC cast 和
REGTYPE cast 图，以及真实 prepared execution/constant planning/sort、BYTEA
和内部char整数关系。没有跨公共头 epoch 借对象，也不是新一轮 fresh58。

## 后续 R50：协议描述符仍需修复

额外增加12个合法 integer/bigint→bit(5/65) 控制，完整172条在实际PG18.6全0。
候选通过类型/值检查后，在首个合法 bit(5) 的 RowDescription 中实际返回
`(OID1560, size=-1, typmod=-1)`，而不是长度5。失败未改成宽松预期，也未
宣称172条通过；完整输入/全部断言永久保存在
`tests/compat/bit_integer_cast_descriptor_known_gap.py`，另列R50继续修复。
它是显式未登记的known-gap探针，不冒充标准注册回归已覆盖并通过。
普通160回归保留最初全部SQL和断言；174原生仍包含合法整数→BIT值控制。

原失败日志为r47-first172-protocol.log及r50-first172-known-gap.log，实际参考
日志为r47-postgres-reference172.log。独立原生描述符探针确认binder实际保留
长度5（r50-typed-descriptor-probe.log），不能用其通过冒充wire元数据正确。
本项关闭直接cast类型图/数值缺陷，不宣称整个BIT类型或协议描述符完成；原273
功能总账状态不变。主分支仍冻结运行完整回归，候选尚待独立合入及组合验证。

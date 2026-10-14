# R50：BIT/VARBIT 的真实协议长度修饰符

## 问题与修改

R47 保留的 172 条完整转换控制，在合法 `integer::bit(5)` 的结果描述符失败：
值和 OID 正确，typmod 却是 -1。独立纯 binder 探针有正确的 5，丢失发生在
协议元数据适配器。此外，物理 BIT(5) 目录误用字符类型的长度加4规则，返回9；
原 bit_protocol 测试也错误地固化了9。

实际 PostgreSQL 18.6 的新60控制全部通过，候选修改前60条全部失败。修复：

- 原始 Cast AST 的声明长度传递到 Simple/Extended 的实际结果描述符，支持
  BIT/VARBIT 标量、数组、多列和 pg_catalog 限定名；隐式 cast 不凭空获得长度。
- 目录生成使用位数，不加字符头长度；已有声明修饰符映射同时处理 BIT/VARBIT，
  覆盖 CREATE/ALTER 的标量及数组，明确区分无界 VARBIT 与显式最大容量长度。
- 无关 ALTER 同步保留真实最大容量 VARBIT 的已声明目录修饰符。
- 原 bit_protocol 的固定预期由9改成实际参考的5，不放宽成任选值。
- 保留 compat 文件内全部原172条 SQL/断言，增加正式注册入口；新60控制也正式注册。

不根据返回字符串的长度推测类型，不执行表达式来 Describe，不修改存储文件格式。
本项不宣称所有历史目录自动迁移，也不关闭完整类型功能族。

## 实际验证

全部证据位于 `/tmp/dbms-review-r46.ifu6SWLp/`。

| 检查 | 结果及日志 |
| --- | --- |
| 实际 PG18.6 新协议60控制 | 全0，r50-modifier-reference.log |
| 未修正候选新协议60控制 | 60失败，r50-modifier-original.log |
| 最终新协议60与原完整172 | 全0，r50-final-bit_type_modifier_protocol_e2e_test.py.log、r50-final-bit_integer_cast_descriptor_protocol_e2e_test.py.log |
| 新native目录控制 | 42项全0，含冷独立engine、eviction重载、无关ALTER及保存点/完整回滚 |
| 完整native组合 | 七个入口全0，r50-final-native.log；全部58份当前生产对象receipts及终态输入检查通过 |
| 完整协议通过项 | 八个实际入口全0，r50-final-protocols.log和r50-neighbor-protocols.log；包含三个字符类型邻居、原160、BIT及NUMERIC字段修饰符 |
| 标准生产构建 | 两个变化CPP真实O2重编/链接0，r50-first-standard-build.log；不是另一轮fresh58 |

60控制逐项检查值、NULL、OID、长度、typmod、format和行数；Extended同查 statement
和 portal 两份 RowDescription，覆盖空表、WHERE false、65位及数组。

组合批次另有两个未通过驱动，不能称整批全绿：INTERVAL 原完整驱动仍有R53的
UNKNOWN来源上下文错误；物化视图原完整target驱动在第60个序列哨兵后发生超时，
尚待独立重现/归因。两个拼错的字符邻居文件名未运行，正确文件名预校验后四个
完整入口全部通过，日志分别保留；不把不存在的驱动计入通过数。

主分支冻结的f4087f5d完整回归不含此修复。主分支独立集成验证、剩余失败和
下一轮完整注册验收仍需实际执行。

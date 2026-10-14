# R80：未绑定列保护晚于参数类型推断

状态：本范围已修复并通过主工作区门禁。

冻结b5def9ba的1360入口完整回归中，原window_legacy_result_order协议44项有
一项失败：ORDER BY abs(id-5)被误报42883、TEXT减INTEGER；其余43项正常。
不可通过放宽TEXT算术规则或改动排序期待来消除这个错误。

R62已有legacy未绑定列分支，但resolver在判断是否走该分支前，仍对每个参数
调用inferParsedInputType。结构指纹阶段的id没有binding/typed RowContext，
被默认成TEXT，故保护尚未生效就抛出算术类型错误。简单abs(id)没有二元运算，
因此原100项控制中的简单列阳性没有覆盖这一顺序问题。

修改方法：先完整检查是否存在未绑定列，只有没有时才推断声明参数并解析签名。
实际whole-query binder仍以真实列声明严格校验，legacy执行仍使用真正typed
RowContext；不求值参数来选择签名，不将TEXT按数值内容转换，也不更改窗口排序。

新增unbound_scalar_signature_preparation_test保留完整17控制：复杂/嵌套ABS
指纹、来源限定等价、实际typed legacy求值，以及空来源/LIMIT 0下对TEXT算术、
ABS(TEXT)、upper(integer)、length(integer)的严格拒绝。原实现实际8失败，
`/tmp/dbms-review-r46.ifu6SWLp/r80-old-native.log`，未在第一失败后截断。
本项不代表R62全部回调/UDF通用签名范围已完成。

主工作区ExprEvaluator实际O2重编、链接与repeat均0，
`r80-standard-build.log`；binary SHA256为
07520570fa7870aa14e57058afc0339dbe5886f7e0949ac4190b9fee443331bb。
新17控制8失败→0，十一完整native全0，`r80-final-native.log`。
原完整窗口44（含真实CLI）和七个邻居协议全0，`r80-final-protocols.log`，
包括132签名、67 ABS、普通排序、内部窗口namespace、prepared规划、窗口routine
及重复别名排序；生产输入/binary/58份对象receipt首尾一致。
实际PostgreSQL18.6原44控制也全0，`r80-reference44.log`。
这些日志都位于`/tmp/dbms-review-r46.ifu6SWLp/`。

本项验收时只关闭原冻结的窗口失败，不改写其1360入口/2失败历史终态；当时
另一项LENGTH原生VARBINARY失败尚未修复，随后由[R79](issue-review-r79-native-binary-length.md)
独立验收。两项定向通过也不能宣称该历史全量或修复后的新组合已经完整全绿。

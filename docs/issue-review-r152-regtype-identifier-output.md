# R152：REGTYPE 最小引用输出（开放，未修复）

状态：独立问题已复现，尚未实施、验收或提交生产修复。
用户暂缓的 [R151 枚举自动数组](issue-review-r151-enum-array-catalog.md)不恢复。

## 已复现的问题

~~~sql
CREATE SCHEMA type_display;
CREATE TYPE type_display.plain AS ENUM ('z','a');
SELECT pg_typeof(NULL::type_display.plain)::text;
~~~

PG 输出 type_display.plain，原程序输出 "type_display"."plain"；
类型在搜索路径可见时，原程序仍输出不必要的 "plain"。
特殊名称本来需要引号，不能通过删除所有双引号解决。

| 待修改位置 | 方法 |
| --- | --- |
| regtype_input.h 的用户类型输出 | 保留始终引用的精确查找串判断可见性；输出串另按 SQL 规则进行最小引用 |
| quote_ident / format %I / 搜索路径的重复规则 | 提取已有正确的 parser 标识符呈现逻辑为单一 owner，共用 ASCII 与关键字分类，不按区域设置猜字符 |
| schema 与数组呈现 | schema 同样最小引用；已有真实数组行递归呈现元素名，不创建或伪造数组目录身份 |
| 回归门禁 | 共享头改变后，正常 fresh58 O2 与真实前端 fresh TU、独立 accepted-only clean 和完整邻接入口必须验证 |

PG 18.6 ruleutils.c 的 quote_identifier 规则为 ASCII 小写或下划线起始，
后接 ASCII 小写、数字或下划线；非 unreserved 关键字需要引用，
嵌入双引号须成对转义，美元符号不能作为免引用字符。
项目已接受的 R145 parser 规则及其 494 条关键字事实可复用，
不能使用另一套不完整的 isKeyword 表或修改任意数据字面量。

## 当前实际证据

| 输入 | 完整控制 | 实际结果 |
| --- | --- | --- |
| 实际 PG 18.6 | 1675 wire | 0 失败 |
| 冻结旧 Root / 已接受 R149 clean | 各 1675 wire | 各 69 失败、退出 1 |
| 旧实现，最终原生夹具 | 5197 native | 1718 失败、退出 1 |

协议覆盖全部 494 关键字和 22 类特殊标识符的 quote_ident/format %I、
Simple/Extended text/binary 结果、实际标量 enum 的限定/可见/遮蔽名称和清理。
原生直接调用真实 evaluator 与 regtype 输出；数组部分仅提供合法的复制元数据输入，
不调用 R151 自动数组创建器、不使用替代 server，不构成 R151 验收。

两个未接受夹具为 tests/regtype_identifier_output_test.cpp 和
tests/regtype_identifier_output_protocol_e2e_test.py。
原生最初两次编译因测试 AST 字段/Session 命名错误退出 1；第三次 5197/1719 还含
测试错误地要求 legacy format 回调抛 DbError 的一处失败。
只兼容其既有 runtime_error SQLSTATE 合约后，最终旧红为 5197/1718；
协议仍独立检查真实 22004，所有控制和旧日志保留。

69 项旧 wire 失败包含三项 DROP SCHEMA 清理错误，另立
[R155 命名空间归属修复](issue-review-r155-enum-namespace-owner.md)并独立验证。
R155 之后尚未重跑本项完整 1675；不能把 69 减 3 当成实际新结果，
也不能用 R155 的通过宣称本项修复完成。
本项仅 registry/两个夹具保留在未接受工作区，生产提取/呈现方案未应用；
后续仍须完整复现并独立提交，不恢复暂缓模块、不 push、不启用 Actions。

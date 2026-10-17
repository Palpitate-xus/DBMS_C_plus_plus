# R112：无 typmod 的具名 BPCHAR/BIT 数组输入被截断

状态：本范围已修复并验收，按本项独立本地 commit；不 push。首版失败保留。

## 问题与复现

数组 CAST 解析具名类型后，将元素类型归一为 CHARACTER/BIT，再逐元素调用
显式标量 CAST。这样丢失了原始具名声明“没有长度限制”的含义，错误套用
SQL 关键字 CHARACTER/BIT 省略长度时的默认值 1。线协议 Bind 的真实数组
OID 同样会经过该输入路径，因此不是显示格式问题，而是值已经被截断。

| 输入 | PostgreSQL 18.6 | 固定旧程序 |
| --- | --- | --- |
| `CAST('{abcd}' AS pg_catalog.bpchar[])` | `{abcd}` | `{a}` |
| `CAST('{101}' AS pg_catalog.bit[])` | `{101}` | `{1}` |
| OID 1014 Bind `{abc,NULL}` | `{abc,NULL}` | `{a,NULL}` |
| OID 1561 Bind `{101,NULL}` | `{101,NULL}` | `{1,NULL}` |
| 普通 `CHARACTER[]` / `BIT[]` | 仍应用关键字默认长度 1 | 本项必须保留 |
| 显式元素长度、SQL NULL、空数组 | 各自原语义 | 本项必须保留 |

新增完整协议入口 `tests/unconstrained_named_array_input_protocol_e2e_test.py`
含 57 条控制：36 条具名数组输入、7 条关键字/显式长度、2 条已有标量规则及
12 条真实 Parse/Bind/Describe/Execute。包括 NULL、空字符串、字面值 NULL、
负下界、多维数组；不靠二进制输出验证，能独立于 R110 的候选编码器复现。
实际参考版本 180006 全部通过；固定旧程序 44 条失败、实际退出 1。

## 修改方法与范围

在数组元素转换处保留原始声明是否为具名且未带 typmod。仅这种真实 BPCHAR/
BIT 元素采用对应具名标量输入；普通 SQL 关键字及显式长度仍使用原规则。
不引入全局 TEXT 隐式边、不执行表达式探测类型、不修改公共头、provider 或
网络字节布局。R106 未选显式地址 CAST 分析等用户跳过专项不恢复。

## 验证与版本管理

证据目录 `/tmp/dbms-review-r46.ifu6SWLp/`。旧程序
`r110-pre-fix-dbms-main` SHA256 为
`c1f5ce345ef26132e466aac2a2928f95183a1f230f94d44a1338bc45dd97704c`；它包含
未接受的 R109 候选，相关元素输入代码与本项修复前一致，不冒充 clean HEAD。
参考/旧完整日志为 `r112-reference57-initial.log` / `r112-old57-initial.log`。

首版自身正常构建实际重编 1 CPP、repeat 0，均退出 0。首版漏掉 CHARACTER
规范名，BIT 控制改善但 BPCHAR 仍失败；完整协议的 24 条失败及 R110 的 1 条
失败均保留，不以构建成功当作行为通过。第二版补足 CHARACTER 规范名，
自身 normal/repeat 仍重编 1/0 CPP、均退出 0；完整57输入控制、110二进制结果
控制和当版22个完整协议入口均退出0。该版低层新夹具的数组身份计数预测错误
另归R110，不把整批native写为通过。

最终冻结输入下，15个完整native与22个完整协议共37个不重复入口全部实际
退出0，包括原数组库、modifier/descriptor/NULL/constructor、CHAR/BIT、
R108的354、R86的208及R107的607/654等完整入口。新增低层182和协议110、
本项57均实际0；只改新夹具后的 normal/repeat 均重编0个生产CPP、产物不变。
日志 `r110-r112-final3-{driver,native,protocol}.log` 与逐协议日志目录保存。
解释为有限验收，不是完整注册集合、真实TLS或整库sanitizer证明。

生产 binary SHA256：
`595869a944bc409cb04e37597a0aa74c9734a07c11a8c813b0aa81473001a6e5`；cache：
`f119652e76df96b502610bb149f3efbd85704c05821ef66a271465cc31f11086`；最终
src/tests/scripts seal：
`372b318071ba82b2ca588176faec5f2f355c131a1b732d4af923e285144408ba`。
当前cache双stamp、全部58个生产receipt、57份test库字节与冻结输入终态核对0；
不是本项新编58份的声明。库和native均O2、fresh stubs；生产Main正常O2链接。

工作区还含未接受的R98/R99/R109及R110候选，不冒充clean HEAD全量证明。
本项提交只收数组元素输入小hunk、57条独立文本协议测试、一个注册入口与文档；
12路径投影索引核对532份已跟踪Markdown、887native、514Python，原273不变。
全部工作区文档/链接、Python AST、shell语法及注册检查另作静态核对。
本项直接覆盖表达式CAST与参数Bind输入声明，不据此宣布所有数组列赋值或
具名域/复合数组能力完成。

原总账 273 项和勾选不提升：22 complete / 166 partial / 70 unverified /
15 deferred；本项只是 TYPE-04 / TYPE-16 / PROTO-04 的具体修复。
R98/R99 和用户跳过的 R109 候选、R106 未注册草稿均保留并排除本项提交。
R110 数组二进制输出另行验收和提交；不 push，GitHub Actions 保持禁用。

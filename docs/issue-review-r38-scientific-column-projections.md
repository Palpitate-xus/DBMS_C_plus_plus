# R38：表投影不得拆开科学计数法的指数符号

## 原问题与方法

`SELECT a+1e-40 AS n FROM t` 在 NUMERIC 列 a 有数据时错误返回 `42703`，
称列 `1e` 不存在；正指数、乘除余等运算以及从表投影单个指数字面量也受影响。
结构化 parser、纯 metadata 和 typed execution 已能正确处理该 SQL，错误发生
在 Main 旧表投影的两处字符级分词：把 `1e-40` 拆成 `1e - 40` 后重新拼接求值。

两处拆分器在拆 `+/-` 前，调用已有 SQL lexer 检查当前尾数/指数前缀是否仍
是一个数值 token；满足规则且下一字符是数字才保留指数符号。不按 `e/E` 结尾
直接猜测，因此 `rate-1` 等普通列名的减法不会被吞成指数。尾数字符、空格、
括号、既有聚合参数和 cast 表达式继续保留，未替换整条查询的执行路径。
没有修改 parser API、存储、头文件或类型绑定。

## 检查范围与基线

父版本为 R37 `7b84a3d4`，使用其冻结 Main 取得红基线：新协议 60 控制中
18 失败，exit 1；完全相同最终驱动在 PostgreSQL 18.6 全通过，exit 0。
新增 native 36 控制在父版本即全通过，证明正确的 SQL lexer、纯 metadata、
PreparedQueryExecution 及传入正确 token 的 legacy 执行都不是此次错误来源。
协议通过真实 Main 表投影入口覆盖实际缺陷，不用原生替代协议验收。

控制覆盖 E/e、正负指数、前导/尾随小数点、不同运算和优先级、括号/负号、
函数/cast 邻居、列名末尾 e、NULL 行、空表、WHERE false、LIMIT 0、参数、
纯 metadata 不运行 nextval。原 `a+1e-40` 及其他算术的字符串/value/scale/OID
要求不变。单独纯字面量 `SELECT 1e-40 FROM t` 按精确 Decimal tuple（值与
scale）及 OID 检查：R32 已记录原有输出可以保留 `1e-40` 拼写，本项不要求
把该拼写规范化为小数文本。该 SQL 未换成 cast，初始日志亦保留。

## 真实终态与构建证据

| 实际检查 | 父版本 | 候选 / PostgreSQL 18.6 |
| --- | --- | --- |
| 新协议，60 控制 | 18 失败，exit 1 | 候选、PG 均全通过，exit 0 |
| native，36 控制 | 正确 typed 路径已通过，exit 0 | 全通过，exit 0 |
| 完整 `postgres_protocol_test.py` | 本项不以其代替红基线 | 候选完整运行 exit 0 |

八个原有 native 邻居完整通过：numeric_text_arithmetic_binding、
numeric_gcd_lcm_type、scientific_numeric_arithmetic、scientific_numeric_literal、
arithmetic_result_type_inference、numeric_cast_division、numeric_division_projection、
parsed_expression_result_type（后缀 `_test.cpp`）。

十七个完整协议邻居均 exit 0：numeric_text_arithmetic_binding、
numeric_gcd_lcm_type、scientific_numeric_arithmetic、scientific_numeric_literal、
arithmetic_result_type、numeric_cast_division、numeric_division_scale、
numeric_zero_scale、numeric_truncated_quotient、table_arithmetic_typed_bridge、
expression_quoted_row_binding、quoted_arithmetic_describe、aggregate_arithmetic_scope、
aggregate_argument_expression、integer_arithmetic_width、floating_arithmetic_width、
arithmetic_predicate（后缀 `_protocol_e2e_test.py`）。没有截取这些驱动的子集。
新增协议已注册标准 E2E 清单，native 由既有发现机制纳入。

沿 R37 已真实 fresh58 的同一 header epoch，只重编本项唯一变化的生产源
`src/main.cpp` 并重新链接，实际 exit 0；没有借用旧 Main 或为不匹配对象重写签名。
冻结候选 SHA-256：
`d5a233cf0f05adf7941d8a72b8d6b34fc918105156e3bde68e19a03ae9eac0f3`。

全部证据保留于 `/tmp/dbms-review-r38-evidence.al0boB6O/`；父版本 native 的
真实完整记录复制为 `native-parent-controls.log`。最终红基线与 PG 对照为
`protocol-baseline-final.log`、`protocol-pg18-final.log`；候选为
`protocol-candidate.log`、`native-candidate-neighbors.log`，完整协议为
`postgres_protocol_final.log`。本项定向通过不等于全项目无 bug 或完全兼容 PG。

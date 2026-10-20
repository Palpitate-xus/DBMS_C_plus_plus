# R204：浮点转整数必须恢复输入宽度和严格转换结果

状态：本项有限修复已通过两侧完整定向矩阵。R201 及其他用户暂缓范围保持不变。

总账映射：本项提供 `TYPE-02` 的 primitive integer/float 舍入、范围和错误语义的有限证据。
它不关闭该项的全部 binary storage/ordering/implicit cast 要求，也不替代 `SQL-08`
完整 cast graph 或 `SQL-11` 全语句错误语义的实现和验收。

## 已复现的问题

修复前已接受基线为 `2b35a091`；工作区另有已有未接受改动，不由本项批准。

| 表达式 | 修复前 | PostgreSQL 18.6 |
| --- | --- | --- |
| `'1e-310'::double precision::integer` | `22003` | `0` |
| `'-5e-324'::double precision::bigint` | `22003` | `0` |
| `'123456789'::real::bigint` | `123456790` | `123456792` |
| `'2147483647'::real::integer` | `2147483600` | `22003` |
| `'9223372036854775807'::real::bigint` | `9223372000000000000` | `22003` |
| `'-9223372036854775808'::real::bigint` | `-9223372000000000000` | `-9223372036854775808` |
| `'1e999tail'::real::integer` | `22P02` | `22003` |

共用整数 cast 分支把所有浮点文本都交给 `stod`：它既会因可表示的 float8 次正规数报告 ERANGE，
又会把 REAL 的最短显示文本误当作一个独立的 float8 数值，造成错值和错误的整数范围判定。
后者不是简单的字符串显示差异，实际返回的整数以及是否接受溢出都会改变。

## 修改方法

按已经解析的源类型复用现有 `parseRealCastValue` / `parseDoubleCastValue`，
先恢复真实 float4/float8 值，再执行原有半偶舍入、有限性及三个整数宽度的边界检查。
语法/范围错误仍转换为目标整数类型对应的结构化错误，其他错误原样抛出。
保留 NUMERIC、BIT、BYTEA、BOOLEAN 等非浮点分支、NULL、返回类型与范围算法。

同时核实共用浮点输入 codec：PG 在数值前缀溢出时先报 `22003`，再检查后缀语法；
将真实溢出/完全下溢检查放在尾部检查前，非零可表示次正规数仍接受。
尾部检查比较真实字符串末端，不以遇到内嵌 NUL 就认定整个原生输入已消费。
原生中 `"1\0tail"` 到 REAL/float8 的静默截断也改为 `22P02`。
只调整上述三个现有函数；不改头文件、ABI、存储或协议布局，也不恢复完整 typed-carrier 重构。

## 夹具、PG 对照及原失败

[原生夹具](../tests/floating_integer_cast_test.cpp)完整 1,350 项：
2 种源宽度 × 3 种整数 × 42 个输入 × raw/bound/parsed/literal-helper/row-helper 五条路径，
另有原生类型别名、NULL、内嵌 NUL 整数转换及独立 REAL/float8 输入检查。
覆盖半偶舍入、精度恢复、正负边界、次正规数、Infinity/NaN、错误输入与正常结果类型。
原生 `float`/`double` 等别名遵循项目既有映射，不把它们宣称为 PG 同名 SQL 声明。

[协议夹具](../tests/floating_integer_cast_protocol_e2e_test.py)完整 1,190 项：
同一源/目标/输入矩阵的 Simple、Extended text/binary；两种参数输入和输出格式；
保留 statement 的重执行、纯 Parse/Bind Describe、真实表列、零行与有效混合结果，
以及事务失败、`25P02`、保存点恢复。精确检查整数值/字节、NULL、OID 21/23/20、
长度 2/4/8、typmod -1、输入参数 OID 700/701、标签和 Ready 状态。
预期值独立恢复 IEEE 源宽度后做半偶舍入，以 Python 整数检查边界。

首轮 PG 夹具完整运行 1,190 项、18 个预期错误：误将 `1e999tail` 预期为 `22P02`，
PG 实际为 `22003`。没有删除这些输入或检查；原 V1 源码和失败完整保留，
修正独立 oracle 的范围优先级后，相同 1,190 个控制在新建 PG 18.6 实例全部通过。
V1 原生源码只保留了快照，未执行，不冒称它有原生运行结果。

最终未改夹具在旧 accepted-only 与工作区各完整运行：
原生 1,350 项各 143 失败，协议 1,190 项各 194 失败，实际均退出 1。
重复控制不是 143/194 个独立缺陷。原失败不由后续绿色结果重写。

证据目录：`/tmp/dbms-r204-float-integer.SLAHux2X`。
旧两侧回执 `completed-baseline-receipt.json`，
SHA256 `f7120e0ee0e0f3504ab8f026cfc57261a9cb11d33915f0c20262d80e4ca3bbf3`。
V1 PG 失败 `/tmp/dbms-private-pg18-full-fixture.se85qlfq/completed-reference-receipt.json`，
SHA256 `754b554c2d9534b2eaff891d575a2fd2ca3b7459455c996865699afe3f03b8dd`。
最终 PG 全通过 `/tmp/dbms-private-pg18-full-fixture.0m4uw1ez/completed-reference-receipt.json`，
SHA256 `d04635830e0dfa75ca091f6133fc34fe425b794e62a4f8c3b7e3ec73ff26d394`。
PG 使用私有新实例、默认 fsync 并独立停止；没有操作其他实例。

## 最终定向验收

accepted-only 与正常工作区构建各 53 个完整入口全部实际退出 0，共 106 个运行入口。
每侧 29 个原生：新 1,350、floating_value_subnormal、trigonometric_subnormal、math_functions、
math_projection_error、angle_conversion_range、float8_scalar_signature、native_float8_callback_override、
numeric_square_root、integral_rounding_result_type、round_result_type、round_native_input_compatibility、
numeric_sign_null_type、abs_integer_bounds、scalar_signature、native_scalar_signature_ownership、geometric、
floating_arithmetic_width、floating_special_values、floating_subnormal_storage、postgres_numeric、
to_char_exact_rounding、cast_structured_error、cast_projection_validation、bit_integer_cast_value、
internal_char_integer_cast、bytea_functions、integer_numeric_predicate、numeric_cast_division。

每侧 24 个协议：新 1,190、floating_value_subnormal、trigonometric_subnormal、float8_scalar_signature、
angle_conversion_range、floating_arithmetic_width、text_case_locale、scalar_signature、
qualified_projection_routine_identity、integral_rounding_result_type、round_result_type、numeric_zero_scale、
abs_integer_bounds、geometric、to_char_exact_rounding、scientific_numeric_arithmetic、numeric_gcd_lcm_type、
numeric_square_root、to_char_zero、bit_integer_cast、bit_integer_cast_descriptor、numeric_cast_division、
integer_numeric、binary_float_parameter_roundtrip。原夹具的全部断言、输入和期限保留。

每侧 fresh 1 个生产表达式单元，其他 57 个生产对象及 stub 的源、设置和对象字节独立核对，
使用本侧单独核对的 127 个头文件；不是 fresh 全 58 或 fresh Main。
每侧 8 个原生测试单元本轮 fresh（含基线新 1,350 对象），21 个为匹配源/头/编译器/设置的
R203 donor，全部 29 个程序重新链接当前库。工作区正常构建实际 1 CPP 并链接，
重复构建 0 编译、0 链接；干净侧显式编译及链接。运行在共享锁下串行执行。

候选回执 `completed-candidate-receipt.json`，
SHA256 `99e8234d6c1f838afc90bb186d181aee029c9f63646fac324c2c8e0b8c7cb0a1`。
独立提交仅包含本项三个函数、一个协议注册、两个新夹具及本文档；
此前暂存的 R185 18 个文件和其他来源改动不由本项接受。

## 后续已确认、尚未修复的 REAL widening

在本项已验证候选上另行观察发现：REAL 转 double precision 及部分 float8 数学函数仍把
REAL 显示文本作为独立 float8 解析。例如 `'123456789'::real::double precision`
仍返回数值 `123456790`，PG 为 `123456792`；`sin('123456789'::real)`
返回 `0.6529857358218815`，PG 为 `-0.9604127276688877`。
REAL 最小非零值 widening 也有数值差异，不是只有显示形式不同。

10 条观察另有 sqrt、负零和 NaN 对照；不能把观察程序退出 0 当成 10 条兼容通过。
产品日志 `accepted-widen-observation10.log`，
SHA256 `937e5674faa083fc8eddc6949ef68b5b158b41db25ac1d6ad18c73d81a865551`。
PG 对照 `/tmp/dbms-private-pg18-full-fixture.5hefw4nb/completed-reference-receipt.json`，
SHA256 `b7472e01f60869966cc15578077231f9d45c2017c46177f1a49e79ac90f1144c`。
下一项应让共享 float8 转换按源 REAL 恢复物理精度，补充明确/隐式 widening、数学函数、
表列与参数的独立回归；不据整数 cast 通过关闭该问题。本项尚未开始它的实现。

原 1492 冻结结果仍为 1481 PASS / 11 FAIL；原 273 状态仍为
22 complete / 166 partial / 70 unverified / 15 deferred，总目标未完成。
本项不声称全部 cast、domain/UDT、常量规划时机或原总清单已完成。
不恢复 R201/R197/R185/默认协议主测试等暂缓项，不 push，不启用 GitHub Actions。

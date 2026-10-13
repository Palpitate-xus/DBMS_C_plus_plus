# R34：科学计数法 NUMERIC 算术的数值与 scale 保真

## 问题与修改方法

R32 已修复指数字面量的类型，但 `SELECT 1e-40+1e-40 AS n` 仍返回
NUMERIC `0`，而 PostgreSQL 18.6 返回 `0.` 后 39 个零再加 `2`。
这不是浮点误差：内部精确计算已经得到正确结果，随后错误的显示 scale
将它重新舍入为 0。INSERT/UPDATE 中的同类计算也会把错误结果写入表中。

`ExprEvaluator.cpp` 的二元算术和 `mod()` 各自有一个 `textScale` lambda，
直接统计小数点后的字符数，遗漏指数的数学含义：无小数点的 `1e-40` 被当作
scale 0；`1.250e-2` 的指数文本被计入小数位；`1.00e+40` 则增加不应有的尾零。

两处统一复用已有 `numericTextScale(input, parsed)`，以精确解析结果及零值的
指数感知规则求 scale。加、减、取余和 `mod` 取两个输入的最大 scale，乘法
取两者之和。除法继续使用 `Numeric::operator/` 的既有精度/scale 规则，不用
加减规则截断商。删除二元运算多余的字符串补零，由 `withScale().toString()`
保留有限值尾零并保持 NaN/Infinity 的合法拼写；NULL 与除零规则不变。

没有修改 parser、binder、头文件、存储格式、浮点算术或 NUMERIC 精度上限。
剩余按小数点计数的格式化代码操作的是已展开的 `Numeric::toString()` 结果，
不是尚含 e/E 的原始操作数，不属于这两处根因。

## 回归与真实终态

基线提交：`65afb1dbab83345e4d80f605adf13ce405c1ff31`（R32）。
新增原生测试同时覆盖旧 ExprHelper 和真实 PreparedQueryExecution，检查完整
字符串和纯 metadata 的 NUMERIC 类型。新增注册协议测试覆盖 E/e、正负指数、
前导点、巨大/微小值、指数尾零、消去得到的零、带 scale 的零、NULL、非有限值、
NUMERIC 参数、表列、INSERT/UPDATE、显式 cast、空结果、错误 SQLSTATE，以及
Parse/Describe 不执行 nextval 或除零表达式。

有限值加减乘余的期望由独立 Decimal 精确运算生成，并验证完整输出字符串
（包括 scale）；除法和其他控制使用明确的期望字符串。完全相同的协议控制
实际运行 PostgreSQL 18.6 对照，而不是只比较本实现的两条执行路径。

| 检查 | 基线 | 修复候选 / PostgreSQL 18.6 |
| --- | --- | --- |
| 新原生测试，120 控制 | 36 失败，exit 134 | 全通过，exit 0 |
| 新协议测试，123 控制 | 32 失败，exit 1 | 候选及 PG 均全通过，exit 0 |
| R32 原 `scientific_numeric_scale_known_gap.py` | R32 日志 exit 1 | 原 SQL/值/OID 断言未放宽，exit 0 |
| R32 初始完整 111 项快照 | 原候选 110 通过、1 失败 | 原字节快照 111 全通过，exit 0 |

上述失败数是控制数，不是独立 bug 数。初轮探索日志还包含两条测试 SQL
拼接负数时缺少空格形成 `--` / `%-` 的问题，已修正测试构造，再对最终 123 项
驱动重新取得 PG 通过及基线失败记录；没有据此改数据库语法。

原有 12 个 native 邻居整批完成，全部 exit 0：
`scientific_numeric_literal_test`、`numeric_zero_scale_test`、
`numeric_division_scale_test`、`numeric_truncated_quotient_test`、
`numeric_cast_division_test`、`numeric_division_projection_test`、
`numeric_column_modifier_test`、`numeric_storage_test`、`numeric_test`、
`postgres_numeric_test`、`arithmetic_result_type_inference_test`、
`expression_storage_type_identity_test`。

原有 11 个协议邻居完整通过、断言未放宽：`scientific_numeric_literal`、
`numeric_column_modifier`、`numeric_cast_division`、`numeric_division_scale`、
`numeric_zero_scale`、`numeric_truncated_quotient`、`numeric_square_root`、
`prepared_primitive_assignment`、`case_integer_literal_binding`、
`arithmetic_result_type`、`protocol_parameter_type_identity`
（文件名后缀均为 `_protocol_e2e_test.py`）。R32 主驱动只更新了历史说明注释。
完整 `postgres_protocol_test.py` 也实际完成并 exit 0，未截取子集。

## 构建与证据

沿 R32 已验证的完整对象缓存真实重编唯一修改的生产翻译单元
`src/expression/ExprEvaluator.cpp` 并重新链接。未变更任何头文件或借用不同
header epoch 的对象；原生测试链接前核对每个生产对象的源码/编译签名。

冻结基线 Main SHA-256：
`b4a97fca19a28078e572c9d41b4211bd22b7053f14ae6cd9013d6ca4a3deddd3`。
冻结候选 Main SHA-256：
`bea197b99d4f6f0349e4fa463641cfe9b8626760a00b993e40c64b3ba4ea838e`。

新日志、冻结候选保留于 `/tmp/dbms-review-r34-evidence.Qa40cNac/`；
`protocol-*-accepted.log` 为最终驱动的红基线/PG 对照，
`protocol-candidate.log`、`native-candidate-and-neighbors.log` 为候选完整记录。
原 R32 的全部 111 控制失败证据和快照继续保留在
`/tmp/dbms-review-r32-evidence.lmCg5Ovk/`，快照 SHA-256 仍为
`e6aeda049caf5eb55ff041f85e67d4f6bb86e987ba0340b4702370ff00c4f536`。
重跑快照时仅提供本仓库测试目录作为 `__file__` 导入上下文，不编辑源字节或断言；
新通过记录为 `original-111-candidate.log`。

## 独立邻居发现与范围边界

初轮 `protocol-baseline.log` 另保留了三个不同根因的发现：
`gcd/lcm(6e-40,4e-40)` 值正确但返回整数 OID；显式 TEXT 与 NUMERIC 的
加法未被拒绝；表列 `a+1e-40` 的旧投影路径错误把 `1e` 当作列名。
这些不是运算后 scale 舍入问题，本提交未宣称修复，已交给总审查继续跟踪。
新注册测试中 gcd/lcm 及表列混合输入使用显式 numeric cast，覆盖此次算术
值/scale 路径；原始失败 SQL 和证据没有删除，也没有替换 R32 原 111 项断言。
本次定向验证不等于全项目无 bug，也不改变现有 NUMERIC 1000 位精度限制。

后续：[R36](issue-review-r36-gcd-lcm-result-types.md) 已单独修复上述 gcd/lcm
结果类型问题，并按未加 cast 的原 SQL 完成值和 OID 回归。
[R37](issue-review-r37-numeric-text-arithmetic.md) 随后单独修复显式 TEXT/数值
算术的非法接纳，并验证 UNKNOWN 合法输入及纯 metadata 错误阶段。

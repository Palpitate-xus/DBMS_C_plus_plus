# R32：科学计数法字面量保留 NUMERIC 类型

## 影响与根因

合法语句 `INSERT INTO t(n) VALUES(1e40)` 在 `n NUMERIC` 时错误返回
`42804`，称表达式为 TEXT；UPDATE、INSERT SELECT 也受影响。负号形式还可能
报 `42883 operator does not exist: - text`。Parse/Describe 可以把 NUMERIC
结果错误声明为 TEXT（OID 25），空结果和 CASE 的类型分析同样受影响。

词法分析器、AST 字面量解析器和 NUMERIC 求值器已经支持 e/E 指数。
不一致发生在 `src/expression/expr_helper.cpp`：共享 `looksLikeNumber()`
只接受数字和小数点，`inferParsedInputType()` 将指数字面量降为 UNKNOWN，
`inferValuesResultType()` 经普通结果推断又将其降为 TEXT，写入绑定最终拒绝它。

## 修改方法

共享字面量识别器按语法识别一个完整十进制尾数、可选 e/E、可选指数正负号以及
至少一位指数数字；重复指数、小数指数、缺失数字和尾随杂字不成为 NUMERIC。
类型识别不调用浮点转换、NUMERIC 构造、SQL 执行器或用户函数，因此 `1e500` 和
`1e-500` 的类型不会取决于 double 的范围。已有 AST 推断据此返回 NUMERIC；
旧无类型行值 helper 的指数形式也明确返回 NUMERIC，不误归为整数。

字符串字面量仍为 UNKNOWN，显式 TEXT 仍为 TEXT，未放宽 TEXT 向 NUMERIC 的
赋值规则；普通整数的 INT/BIGINT 边界和小数推断不变。没有修改头文件、parser
语法、NUMERIC 存储格式、数值精度上限或 wire codec。

## 验证范围与构建来源

基线为 `aeaa325773953098be3b0bb59381e802f80962ed`。冻结 R26 donor 的完整
构建签名、58 个对象签名、全部头文件、manifest 和编译配置均通过核对；只复制
55 个源码逐字相同的对象，并逐字比较对象。Main、ExecutionPlan、DmlExecutor
三个差异翻译单元真实重编后得到本次基线。候选只重编修改的 expr_helper.cpp
并重新链接。没有覆盖 donor，也没有给不同源码或头文件的对象重新标记签名。

新增 native `scientific_numeric_literal_test.cpp` 的 203 项控制覆盖 19 种合法
指数形式的 parser、AST 类型、VALUES 类型、完整 SELECT/INSERT/UPDATE 绑定、
真实 prepared 标量求值、整数边界、无效 token、字符串类型、错误阶段，以及
不执行 nextval、不写目标表的纯 metadata 检查。基线 141 项失败，候选全部通过。

新增协议测试覆盖同样 19 种指数形式，Simple SELECT、INSERT VALUES、UPDATE、
INSERT SELECT、写入后读回、OID、command tag、空结果、Parse/Describe，以及
纯 metadata 阶段不执行 nextval。数值按 Decimal 精确比较；不把 NUMERIC 显示
scale 或是否使用指数输出纳入此项类型修复的验收。

| 实际检查 | 修改前 | 修改后 / 参考 |
| --- | --- | --- |
| 新 native，203 控制 | 141 失败，exit 134 | 203 通过，exit 0 |
| 新协议最终驱动，110 控制 | 79 失败，exit 1 | 110 通过，exit 0 |
| PostgreSQL 18.6，同一最终协议驱动 | 不适用 | 110 通过，exit 0 |
| 独立 scientific scale 诊断 | 初始完整驱动实测值为 0 | 候选独立诊断仍 exit 1；PG18.6 exit 0 |

失败控制数不代表独立 bug 数；最终 count/readback 失败也包括前面 INSERT 被
拒绝的后果。初始 111 控制的原始记录见下面独立发现说明。

协议测试已加入标准 E2E 清单；native 测试由既有发现机制纳入。
完整原有 `postgres_protocol_test.py` 也已实际 exit 0。

冻结基线 Main SHA-256：
`6ff2db3ac75d223a78a81bbd70c3128b4994993b16a2ec40f0c4801cdde7088e`。
冻结候选 Main SHA-256：
`b4a97fca19a28078e572c9d41b4211bd22b7053f14ae6cd9013d6ca4a3deddd3`。

原有 7 个相邻 native 驱动均完整 exit 0：

- `parsed_expression_result_type_test`
- `case_integer_literal_binding_test`
- `prepared_primitive_assignment_input_test`
- `numeric_column_modifier_test`
- `numeric_storage_test`
- `arithmetic_result_type_inference_test`
- `expression_storage_type_identity_test`

原有 8 个相邻协议驱动均完整 exit 0，未修改断言：

- `numeric_column_modifier_protocol_e2e_test.py`
- `numeric_cast_division_protocol_e2e_test.py`
- `numeric_division_scale_protocol_e2e_test.py`
- `numeric_zero_scale_protocol_e2e_test.py`
- `prepared_primitive_assignment_protocol_e2e_test.py`
- `case_integer_literal_binding_protocol_e2e_test.py`
- `arithmetic_result_type_protocol_e2e_test.py`
- `protocol_parameter_type_identity_protocol_e2e_test.py`

## 独立发现

最初扩展协议控制额外执行 `SELECT 1e-40+1e-40`，PostgreSQL 18.6 返回
`2e-40`，本次基线和类型候选仍返回 0。该表达式的 NUMERIC 类型已正确；错误
来自 ExprEvaluator 数值加法按尾数字符串的小数点计数来确定结果 scale，忽略了
指数后再 `withScale(0)`，属于独立数值运算问题，不是写入类型绑定问题。
可复用该文件现有 `numericTextScale(input, parsed)` 修复指数感知的运算 scale，
另行补充加减乘法及正负指数的精确结果回归；本项没有修改这段算术代码。
原 SQL、精确值和 OID 断言单独保留在
`tests/compat/scientific_numeric_scale_known_gap.py`，明确作为尚未修复的诊断，
没有注册为 R32 的通过项。R32 主协议驱动仍检查该表达式的 NUMERIC Describe
类型。最初 111 控制中候选 110 通过、此数值控制失败的日志和测试快照全部保留，
不以类型修复完成宣称该算术问题完成。

所有原始 PG、baseline、候选和相邻回归日志及初始完整协议驱动快照保留在
`/tmp/dbms-review-r32-evidence.lmCg5Ovk/`。本项定向验证不是全项目无 bug 或
NUMERIC 支持 PostgreSQL 全部范围的声明；现有实现仍有独立的 1000 位精度限制。

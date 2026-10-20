# R203：共用 asDouble 转换丢失可表示的非零极小数

状态：共用转换的有限修复已通过两侧完整定向矩阵。R201 及其他用户暂缓范围保持不变。

## 缺陷和修改方法

R202 修复了三角函数的受检输入路径；继续审查发现其余共用 `ExprValue::asDouble`
的调用者仍会丢失可表示的非零极小数。`std::stod` 在报告 ERANGE 时抛出
`out_of_range`，即使转换结果并未归零，旧 catch-all 也直接返回了 0。

| 表达式 | 旧 accepted-only / 工作区 | PostgreSQL 18.6 |
| --- | --- | --- |
| `ceil('5e-324'::double precision)` | `0` | `1` |
| `floor('-1e-310'::double precision)` | `0` | `-1` |
| `round('-5e-324'::double precision)` | `0` | `-0` |
| `abs('-1e-310'::double precision)` | `0` | `1e-310` |
| `sign('5e-324'::double precision)` | `0` | `1` |

候选修复共用转换入口：只在原 `out_of_range` 分支用 `strtod` 重新取得结果，
保留有限、非零的可表示值；原 NULL/空串/无有效前缀/真溢出/完全下溢归零仍返回 0。
保留原生 API 的宽松前缀解析、特殊值和类型不参与转换的旧约定，不把它改为严格 SQL cast。
其他异常仍走原 catch-all；不改头文件、公共布局、签名、结果类型或调用者的运算和求值规则。

源码调用点还包括 numeric 数学函数、几何缩放、比较及格式化；因此不只修改五个取整函数。
对应既有完整回归会一并执行，但不以有限矩阵声称这些家族已实现全部 PostgreSQL 语义。

## 独立夹具与原失败

[原生夹具](../tests/floating_value_subnormal_test.cpp)完整 942 项：
40 个原生转换输入 × 6 种类型标签，逐项检查非 NULL 及 NULL 转换；
7 个数学函数 × 20 个输入 × 5 条 raw/bound/parsed/literal-helper/row-helper 路径，
以及平台和私有数据库控制。包括普通与十六进制前缀、尾随文本、内嵌 NUL、
无效输入、真溢出、完全下溢、最小非零值、正规数边界、正负零、Infinity 和 NaN。
比较真实 binary64 位，不能用近似比较掩盖零符号或丢值。

[协议夹具](../tests/floating_value_subnormal_protocol_e2e_test.py)完整 639 项：
ceil/ceiling/floor/round/trunc/abs/sign，相同 20 个输入的 Simple/Extended text/binary，
保留 statement 的 text/binary 参数重执行、纯 Parse/Bind Describe、真实表及零行。
使用独立 Python 数值运算和原角度夹具的严格值/类型/NULL/命令标签/Ready 状态检查。
同一完整夹具在私有新建 PostgreSQL 18.6、默认 fsync 下全部通过，随后独立停止。

旧 accepted-only 基线为 `220eb1ba`；工作区另含已有未接受改动，不由本项批准。
旧两侧库各完整执行：原生 942 项各 158 失败；协议 639 项各 131 失败，
实际均退出 1，原失败和日志保留。这不是 158/131 个独立缺陷。

证据目录：`/tmp/dbms-r203-round-subnormal.SEGajH7G`。
原失败回执 `completed-baseline-receipt.json`，
SHA256 `c88d2b825d5cb7d8bec45bf9555ec2d70a38ffc868babc136db6807f4ce7cf19`。
PG 完整对照 `/tmp/dbms-private-pg18-full-fixture.d1859xym/completed-reference-receipt.json`，
SHA256 `9361825dfc64579f3cdcc98aa221a73bfb705d7469661164088ef25562ef1785`。

## 最终定向验收

accepted-only 与正常工作区构建各 39 个完整入口实际全部退出 0，共 78 个运行入口。
每侧 21 个原生：新 942、trigonometric_subnormal、math_functions、math_projection_error、
angle_conversion_range、float8_scalar_signature、native_float8_callback_override、numeric_square_root、
integral_rounding_result_type、round_result_type、round_native_input_compatibility、numeric_sign_null_type、
abs_integer_bounds、scalar_signature、native_scalar_signature_ownership、geometric、floating_arithmetic_width、
floating_special_values、floating_subnormal_storage、postgres_numeric、to_char_exact_rounding。

每侧 18 个协议：新 639、trigonometric_subnormal、float8_scalar_signature、angle_conversion_range、
floating_arithmetic_width、text_case_locale、scalar_signature、qualified_projection_routine_identity、
integral_rounding_result_type、round_result_type、numeric_zero_scale、abs_integer_bounds、geometric、
to_char_exact_rounding、scientific_numeric_arithmetic、numeric_gcd_lcm_type、numeric_square_root、to_char_zero。
全部原断言、输入、超时和持久化设置保留；没有把入口数量当作独立缺陷数量。

每侧 fresh 1 个生产表达式单元，其余 57 个生产对象及 stub 为逐项核对源、设置和对象字节的 donor，
使用本侧单独验证的 127 个头文件。每侧 14 个原生测试单元本轮 fresh（含保留的基线 942 对象），
7 个测试单元为同源/同头/同编译器和设置的 R202 donor，全部 21 个程序重新链接当前库。
工作区实际正常构建 1 CPP 并链接，重复构建 0 编译、0 链接；干净侧显式编译及链接。
不是 fresh 全 58、fresh Main、完整 1500 入口、TLS 或整库 sanitizer 验收。

候选回执 `completed-candidate-receipt.json`，
SHA256 `a0366c2483084fa9f385df4a49a1a2f2300e7cd1301f9123bcb2a8ec9e02e83a`。
独立提交仅包含本项共用转换、一个协议注册、两个新夹具及本文档；
其他来源改动及 R185 原暂存 18 个文件保持原样，不随本项接受。

## 独立未修复项及非缺陷控制

35 条初始观察还确认：`'1e-310'::double precision::smallint/integer/bigint`
在产品报 `22003`，PG 返回对应整数 0。该路径单独使用 `stod`，本项共用入口修复不关闭它；
下一步应复用严格浮点输入转换、保留真实整数范围及舍入错误，并增加独立 cast 回归。
原观察见 `/tmp/dbms-private-pg18-full-fixture.2d4nnuf5/observe.log`。
其完整回执 SHA256 `1d7d1b22c980dd25b775a706d188387c2fea2e17c266cdd6a5055b66cd9889b7`。

`sign('NaN'::double precision)` 在产品和 PG 都为 0，**不是已确认缺陷**；
新原生和协议夹具均保留该控制，不按直觉改写其行为。

原 1492 冻结结果仍为 1481 PASS / 11 FAIL；原 273 状态保持
22 complete / 166 partial / 70 unverified / 15 deferred，总目标尚未完成。
不恢复 R201/R197/R185/默认协议主测试等暂缓项，不 push，不启用 GitHub Actions。

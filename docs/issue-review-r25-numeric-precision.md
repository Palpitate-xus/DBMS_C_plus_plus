# R25：有效数字舍入保留数量级

原 `Numeric::withPrecision` 丢弃尾数后将负 scale 强制变为零，导致 `12345` 三位精度变成 `123`，`999` 两位变成 `100`，负数同样错误。

复用 `withScale` 的整数位舍入：负目标 scale 在舍入后补回整数零位，保留数量级、符号、半向上规则及 carry，并沿用 1000 位精度上限。参数必须在 1–1000 内；非法参数在有限值、零、NaN/Infinity 上均明确抛出 `invalid_argument`。

新增正常 API 驱动 `tests/numeric_significant_precision_test.cpp`：20 个整数/小数/负数/零/特殊值控制，20 个非法参数控制及最大位数 carry。原生标准测试自动发现该驱动。

- 同一驱动修复前退出 134，首个 `12345 -> 12300` 断言实际得到 `123`。
- 修复后 O2、ASan/UBSan 均退出 0，41 控制全部通过。
- 原 `tests/numeric_test.cpp` 新编译本次真实源，完整执行退出 0。

以上为原生 Numeric API 验证，不冒充 SQL 新增功能或全套数据库测试。真实源独立编译产物在 `/tmp/dbms-review-fix-donor.N9sdas6f/numeric_significant_precision_*`。

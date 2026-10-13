# R01：类型化平方根不经过 double/int64 截断

原 NUMERIC 平方根先经过 `asDouble()`，整数形状结果再转换为 `long long`。
`sqrt(1e40::numeric)` 因超出 int64 得到负数；高精度小数、极小数及显示 scale 同样错误。

## 修改

`Numeric::squareRoot()` 使用十进制逐位算法和一位 guard digit 半向上舍入；无浮点或
有限整数中间结果。显示 scale 依输入 scale 及 base-10000 weight 计算，零、NaN、
正 Infinity 保留相应语义，负输入报 2201F。沿用项目 1000 位 Numeric 上限，结果超限
报 22003，不冒充 PostgreSQL 更宽的任意精度支持。

SQRT 按声明的 NUMERIC 或浮点类型执行。浮点使用原有严格宽度解析和 `std::sqrt`，
保留次正规数、负零和完整精度；REAL 先解析为 float 再扩宽，不能把 float 的短往返文本
直接当作 double 的实际值。

参数表达式 Describe 原先从未绑定的 `$1` 推断重载，使 NUMERIC SQRT 的行描述为 701。
明确非零 builtin 参数的 SELECT 现在使用既有 whole-query binder 与真正的 typed
MetadataPlaceholder；不替换成 NULL SQL、不求值或读取行。保留 EXPLAIN、特殊 provider
及尚未明确参数类型的原有入口。

显示 scale 规则核对了 [PostgreSQL 18 源码 numeric_sqrt](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/adt/numeric.c)，
所有保存的 SQL 预期另由本机真实 PostgreSQL 18.6 核验，而不是用本次算法生成预期。

## 验证

- 新原生 `numeric_square_root_test.cpp`：20 个完整文本结果、3 个负值错误、1000 个
  独立整数平方，O2 和 ASan/UBSan 都实际退出 0。
- 新已注册 `numeric_square_root_protocol_e2e_test.py`：最终 46 项 Simple Query、
  物理 NUMERIC/REAL/DOUBLE 列、SQL PREPARE/EXECUTE、typed Parse/Bind/Describe/Execute，
  数量级、相邻大数、28 位小数、1e-1000、1e998、NULL/特殊值/错误及浮点次正规数。
  PostgreSQL 18.6 和最终 Root 候选均实际退出 0，检查完整文本和 OID。
- 冻结旧 Main 上的完整首版 41 控制实际退出 1，保留错误负数、精度损失和参数描述错误；
  扩展的极小浮点控制又复现了此前候选 double 返回零、REAL 宽度错误。最终未放宽断言。
- 首次 typed 候选虽修复数据仍有三项 Describe OID 错误，日志保留；元数据修复后 41 控制
  通过，再追加浮点/物理列控制并完成 46 项。
- binary float 86 控制、parameter type identity、arithmetic result type、完整
  postgres_protocol 原驱动均实际退出 0。
- 原 math_functions、math_projection_error、functions、floating_order_by、
  catalog_snapshot 五个完整 native 在 Root 最终对象集上全部实际退出 0。
  math_projection_error 曾被 R16 cold-directory 兼容错误阻断，该失败保留；合入
  `a0d2efde` 后原驱动原断言全部通过，没有把失败算成数学通过。

公共 Numeric 头变化后的 Root 标准构建真正 fresh58 完成。后续 changed TU 按标准脚本
新编译；native 链接前核对所有58对象的真实源码/头/编译器/flags签名，stub/driver新编译，
每个程序在自己的临时目录执行，终态再次核对输入签名未变。
日志在 `/tmp/dbms-review-fix-donor.N9sdas6f/r01-*`；回归及预期保存到仓库，可独立重跑。
上述是本项和相邻验收，不是整个数据库全回归已通过的声明。

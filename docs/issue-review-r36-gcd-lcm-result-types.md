# R36：gcd/lcm 的 NUMERIC、BIGINT 结果类型

`SELECT gcd(6e-40,4e-40),lcm(6e-40,4e-40)` 原本返回正确的精确值，
但 RowDescription 却把两列标成 INTEGER（OID 23），而不是 NUMERIC（1700）。
BIGINT 重载同样被错报 INTEGER，空表、NULL、Parse/Describe 也受影响。

根因是 `ExprHelper::inferAstResultType()` 把 gcd/lcm 和 length 等固定返回
INTEGER 的函数放在同一分支。现根据参数的静态类型选择：含 NUMERIC 则返回
NUMERIC，否则含 BIGINT 则返回 BIGINT，其余既有整数路径返回 INTEGER。
使用已有数值类型规范化支持别名；不执行表达式、不解析数值来猜类型。
值计算、存储和函数执行代码均未修改。

| 实际终态 | 基线 `3687acb1` | 候选 / PostgreSQL 18.6 |
| --- | --- | --- |
| native 63 控制 | 34 失败，exit 134 | 全通过，exit 0 |
| 注册协议 61 控制 | 48 失败，exit 1 | 候选、PG 均全通过，exit 0 |

控制包括科学计数的巨大/微小值、尾零、零、混合 INTEGER/BIGINT/NUMERIC、
UNKNOWN 字符串、NULL、真实参数、表列、空表和 LIMIT 0，以及 nextval 在
纯 metadata 阶段不被执行。原失败 SQL 按原值和 OID 要求保留，没有加 cast
掩盖结果类型。初轮误假定两 SMALLINT 参数能唯一选出 INTEGER 重载，PG 实测
为 42725；将合法混合 SMALLINT/INTEGER 用作邻居控制，未在本项扩写完整函数
重载解析。该初轮记录也保留，不据此宣称全部 PostgreSQL 函数签名兼容。

原有 native 邻居六个全部 exit 0：scientific_numeric_arithmetic、
scientific_numeric_literal、arithmetic_result_type_inference、
parsed_expression_result_type、expression_storage_type_identity、
numeric_truncated_quotient（文件名后缀 `_test.cpp`）。原有五个完整协议邻居
scientific_numeric_arithmetic、scientific_numeric_literal、arithmetic_result_type、
protocol_parameter_type_identity、numeric_truncated_quotient 均 exit 0。

唯一生产变动 `expr_helper.cpp` 已真实重编并链接，没有头文件变化。
冻结候选 SHA-256：`0422c8a825e34a9d256acd48426c1df325f300cc97a961909cf52400f18c564e`。
日志、基线失败证据、PG 对照、冻结候选保留于
`/tmp/dbms-review-r36-evidence.tOSCQTJP/`；最终对照为 `protocol-*-final.log`，
候选为 `protocol-candidate.log`、`native-candidate-neighbors.log`。

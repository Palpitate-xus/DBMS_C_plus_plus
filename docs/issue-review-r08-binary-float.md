# R08：浮点参数、存储和二进制结果的精确往返

## 问题与修复范围

审查指出 float4/float8 的二进制 Bind 经过 `std::to_string`，其固定六位小数
会改变有效值：例如 float8 `0.1234567890123456` 变为 `0.123457`，极小值变为零。
该桥接还将 Infinity/NaN 拼成 SQL 标识符，且负零经 SQL 数值字面量丢失符号。

严格的 INSERT/UPDATE/回读验收又确认了同一链路的两个已有问题：

- 存储采用 `float` / `double` 编码名，协议却把它们按 SQL 类型名处理。
  REAL 列被描述为 OID 701/8 字节，DOUBLE PRECISION 列成为 OID 25/text。
- `std::stof` / `std::stod` 把可表示、非零的次正规数的 ERANGE 也视为失败，
  影响物理写入和二进制返回编码。

修改仅涉及浮点输入、物理声明适配和对应浮点输出：

1. 二进制参数使用 `to_chars(general, max_digits10)`，显式处理 Infinity/NaN，
   并以带引号的输入交给原有的目标类型 cast，保持类型宽度及负零。
2. 文本格式的 float4/float8 参数也作为带引号的目标类型输入处理。类型转换检查
   完整字符串，不再把负数超界输入误当成一元 SQL 表达式。integer/numeric 路径不变。
3. 协议物理类型适配根据列的编码名和声明宽度，将 4 字节 `float` 映射为 REAL，
   8 字节 `double` 映射为 DOUBLE PRECISION；不根据值或非空行猜测类型。
   元数据阶段和实际结果编码均适配这些物理名称，避免 Describe 声明 4 字节、
   DataRow 却发送 8 字节。只有 AST 确认的直接物理投影参与适配，表达式别名不参与。
4. 存储解析和二进制返回分别使用 `strtof` / `strtod`，要求完整消费且非空。
   ERANGE 的有限非零次正规数保留；真正溢出到 Infinity 或下溢到零拒绝。
   显式 Infinity/NaN 和正负零仍合法，错误尾部仍拒绝。

没有更改 float4/float8 的存储格式，也没有把它们统一扩宽为 double。
NaN 检查使用正的规范 NaN；不承诺 SQL 文本桥接保留任意 NaN payload。

## 回归与实际结果

新增注册驱动 `tests/binary_float_parameter_roundtrip_protocol_e2e_test.py`：

- 30 个 SELECT 控制：每种宽度 14 个 IEEE 位模式加 NULL，包括精度样例、
  正负最大有限值、最小正规/次正规值、最大次正规值、正负零、Infinity/NaN。
- 36 个 INSERT RETURNING、UPDATE RETURNING 和物理回读控制。
- 16 个文本参数范围/错误尾部控制，同时覆盖 SELECT 和 INSERT；
  正负溢出及下溢要求 22003，错误尾部要求 22P02，不能返回数据或完成标签。
- 4 个 Simple Query 描述控制：空源、NULL RETURNING、互换列别名、与物理列名
  冲突的 cast 表达式别名。没有通过非空数据猜测浮点类型。

每个正常往返同时检查原始二进制字节、OID、长度、typmod、格式码、命令标签和
Ready 状态。PostgreSQL 参考使用独立随机 schema、事务及最终回滚；错误控制用
savepoint 恢复。项目端使用独立临时数据目录与随机端口。

| 实际运行 | 66 个往返 | 16 个范围/语法控制 | 4 个描述控制 | 退出码 |
| --- | --- | --- | --- | --- |
| PostgreSQL 18.6 | 全部通过 | 全部通过 | 全部通过 | 0 |
| R30 后、R08 前的冻结基准 | 10 通过、56 失败 | 12 通过、4 失败 | 1 通过、3 失败 | 1 |
| 仅修二进制参数和输出的首次候选 | SELECT 30 通过，存储 36 失败 | 当时未追加 | 当时未追加 | 1 |
| 完整修复的冻结候选 | 全部通过 | 全部通过 | 全部通过 | 0 |

失败数按测试控制计算，包含前序写入失败带来的后续读不到行，不是独立问题数量。
首次候选结果被原样保留，最终验收没有删除存储检查或放宽断言。

基准 SHA-256：
`d9d0bc3c624db08d464dfaaed68a242d270f2be9628e8776aa384abecc91585a`。
最终候选 SHA-256：
`0f93316aa8ca1fa40facb958f372de62968948967ba354040aeaeafbe9b99303`。
完整原始日志保存在本机 `/tmp/dbms-review-r08-evidence.7jKaeoDu/`，其中
`r08-complete-baseline.log`、`r08-complete-pg18.log`、`r08-candidate.log` 和
`r08-accepted-candidate.log` 分别保留完整基准、PG、首次候选和最终结果。
`r08-final-candidate.log` 是补存储解析后的中间失败结果：Describe 已修正，
DataRow 仍沿用旧执行类型名；它不是最终验收日志。可重跑断言均保存到仓库驱动。

命令：`DBMS_MAIN=<binary> DBMS_COMPATIBILITY_MODE=postgresql18 python3 tests/binary_float_parameter_roundtrip_protocol_e2e_test.py`。
参考端使用相同驱动的 `--reference18` 和 `PGREF_*` 连接设置。

新增 `floating_subnormal_storage_test.cpp` 直接调用存储 API，检查最小次正规数
写入/回读、负零、真实超界及坏尾部的 INSERT/UPDATE 拒绝，并确认失败后原行
不变、没有新增行。该驱动及原有 floating_precision、floating_special_values、
floating_key_semantics、floating_zero、floating_order_by 六个 native 驱动均 exit 0。
链接使用本私树已核验签名的生产对象和独立新编译的 test stubs/测试对象。

最终冻结候选上，下列六个未缩减的原协议驱动均 exit 0：

- `floating_arithmetic_width_protocol_e2e_test.py`
- `arithmetic_result_type_protocol_e2e_test.py`
- `protocol_parameter_type_identity_protocol_e2e_test.py`
- `protocol_metadata_session_scope_e2e_test.py`
- `physical_array_element_descriptor_protocol_e2e_test.py`
- `returning_array_metadata_protocol_e2e_test.py`

最终冻结候选的完整 `postgres_protocol_test.py` 也实际 exit 0，日志为
`postgres_protocol_final_test.log`。没有缩减原协议测试；本文件不构成全套回归声明。

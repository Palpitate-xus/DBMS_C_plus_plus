# R212：NAME / 内建 name[] 二进制发送

状态：有限发送分支修复；完整 NAME 类型使用仍未完成。
本项不关闭 TYPE-03/TYPE-05/NET-01/FUNC-01 或原 273 项总清单。

## 问题和方法

扩展协议请求 NAME（OID19）的二进制结果时，编码器没有对应分支，返回 0A000。
例如 `SELECT current_user AS v` 的文本结果正常，二进制结果失败。
内建 name[]（OID1003）的数组编码通过同一元素发送器，因此非 NULL NAME 元素也失败。

PostgreSQL 18.6 `src/backend/utils/adt/name.c:namesend` 发送实际字符串长度的字节，
不包含终止 NUL，不填充到 NAME 的物理宽度 64；数组仍使用原维度、下界、NULL 标志和元素 OID19。
修改只给已有字符串二进制发送分支增加 OID19，复用原数组元素编码流程。
不修改输入解析、NAME 参数接收、存储布局、别名、模式默认值或 current_schema() 的值。

| 边界 | 校验方法 |
| --- | --- |
| NAME 空串 / SQL NULL / 普通字符串 `NULL` | 精确比较 DataRow 的 0 / -1 / 4 字节长度及值 |
| UTF-8、空白、逗号、引号、反斜杠、63 字节值 | 按实际 UTF-8 字节发送，不带 NUL/填充 |
| 内建 name[] | 精确比较元素 OID19、维度、下界、元素长度和 NULL 标志 |
| 混合列格式 | 文本列、NAME 二进制列和 name[] 二进制列同帧核对 |
| 任意用户 OID 伪装 name / _name | 仍拒绝，不写出部分 DataRow |

新原生夹具 `tests/name_binary_result_test.cpp` 为 584 项：
142 个有效原生 NAME 字节串（含全部非 NUL ASCII 字节）×4 条标量/数组、文本/二进制路径，
另含 NULL、空/全 NULL 数组、负/零下界、多维/六维、混合列和未知 OID 控制。
它检验已声明 NAME 值的发送契约，不冒充 NAME 输入验证或任意二进制字符串的 PostgreSQL 文本语义。

旧纯已接受代码和旧工作区各完整 584 项、289 次失败、实际退出 1；
增加发送分支后两侧完整 584 项均实际通过。

## 保留的完整集成失败，不能误报为通过

工作区另有未接受草稿 `tests/name_binary_result_protocol_e2e_test.py`，完整 257 项。
相同源码在独立新建 PostgreSQL 18.6 实例全部通过，默认 fsync 不变，停止/服务退出均 0。
它严格检查 NAME/NAME[] 值、NULL、文本/二进制结果、OID、宽度、typmod、来源、格式、tag 和消息顺序；
保留参数使用 OID25 的文本输入，不冒称已支持 NAME 二进制参数接收。

旧版两侧各完整失败 168 项。首轮发送修复的集成测试仍退出 1、失败 70 项，原日志保留。
70 项均由独立建表问题引起：1 次 `CREATE TABLE (... n name,a name[])` 报未知 name（42704），
其后 69 次对该私有表的操作报 42P01。没有把这些失败删除、降低断言或重写成通过。
复查精确解析完整失败列表，拒绝混入任何其他错误、错误值或错误元数据。

这份 257 项草稿和其工作区注册尚不随本项接受；须连同后续 NAME 建表修复完成验收。
下一步修改方法：在 `DdlExecutor::columnDefToColumn` 接入已经认识 NAME 的类型注册表，
保留固定 64 字节、数组、NULL、修饰符和列约束元数据，补建表/实际行验证，再重跑同一 257 项。
不能用已有 `name_declaration_test.cpp` 的类型注册表/函数检查代替真实建表证据。

## 构建、回归及版本边界

每侧本项 fresh 一个生产 TU（PostgresProtocol），其余 57 个生产对象、stub、127 份各自头文件、
编译器和 flags 均核对。正常工作区构建 1 CPP/1 link，repeat 零编译/零链接。
每侧新编 7 个原生 TU（含基线时编译的新 584 夹具），复用一个匹配的既有 BYTEA 对象；
8 个原生程序全部重新链接当前生产库。Main 未变，不声称本项 fresh Main 或全 58 TU。
最多 2 个编译器，运行时串行；不关闭 fsync，不调整超时。

两侧各 8 个原生入口：name_binary_result、name_declaration、array_binary_result、time_binary_result、
timestamp_binary_result、interval_binary_result、postgres_numeric、bytea_input_grammar。
另各 5 个现有完整协议入口：array_binary_result、binary_builtin_array_parameter、sql_value_keyword、
bytea_input_grammar、binary_float_parameter_roundtrip。
这 26 个入口实际全 0；另两次完整新 257 集成入口实际均 1、各上述 70 次失败，不计为通过。

仅提交编码器、新原生夹具和本文档。不接受 R210 默认值草稿、R211 投影草稿、两条待验收协议注册，
或用户已跳过的 R207/R205/R201/R197/R185 等候选。原有 staged 18 路径差异保持逐字节不变。
本次接受后 930 native +576 Python +3 Main =1,509 入口；不是全 1,509/完整 72/TLS/整库 sanitizer 验收。
原 273 项及冻结原 1,492 的 1,481 PASS/11 FAIL 均不重写。不 push，Actions 保持禁用。

## 本地记录

目录 `/tmp/dbms-r212-name-binary.X7kYBNSc` 为本地诊断记录，不是发布依赖。

| 记录 | SHA256 |
| --- | --- |
| `completed-prepared-receipt.json` | `c8f87c9e9d475d93d3f2ef01dc32b42dec3b0ca221c410e580d6c02874303fb6` |
| `completed-baseline-receipt.json` | `b1f4a1608257521189157f4a7f9bbb8ada174981d3471eb430ff67c1dabcd7b3` |
| `completed-build-receipt.json` | `53afd5f669a9d2c791abcdec6d6df573c84bd670343175a80e01968e15f815dc` |
| `completed-first-candidate-rejection.json` | `48d470dd84f083ade43ead634abc15d2e023fa4306f6831f1f0291d8f77345e3` |
| 参考完整 257 receipt | `/tmp/dbms-private-pg18-full-fixture.16lafsnx/completed-reference-receipt.json`，`696d363dab80d6f7185e0c815cf0f2bdea32bfd93f73b0a35a9c8be80faebf4f` |

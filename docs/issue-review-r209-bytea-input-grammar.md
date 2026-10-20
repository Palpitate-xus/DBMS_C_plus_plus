# R209：BYTEA 输入前缀及十六进制空白

状态：有限修复及下述回归已完成。
范围是 TYPE-05/FUNC-01/SQL-11 的有限输入语法，不是全部类型转换或原 273 项完成。

## 问题和修改方法

`ByteaValue::parse` 把大写 `\X` 当成十六进制前缀，还使用 locale 相关的
`std::isspace` 跳过 VT/FF。PostgreSQL 18.6 的 `varlena.c:byteain` 只识别小写
`\x`；`encode.c:hex_decode_safe` 只跳过空格、TAB、LF、CR，且不能位于半字节中间。
因此原实现会接受参考库拒绝的输入，并可能将其写入数据库。

| 输入 | 正确结果 | 修改方法 |
| --- | --- | --- |
| 大写 `\X`、`\X00` 等 | 传统转义解析失败，22P02 | 仅将小写 `\x` 识别为十六进制前缀 |
| 小写十六进制中的 VT/FF | 22023 | 明确比较四种允许的 ASCII 空白，不使用 `isspace` |
| 字节对前/后/之间的空格、TAB、LF、CR | 接受，规范化为小写十六进制 | 保留既有半字节检查及正确输入 |
| 普通转义格式中的 VT/FF | 普通数据字节 0b/0c | 不修改传统转义分支 |
| 大写 A–F、八进制、反斜杠、NULL、整数转换 | 保持原结果 | 不修改有效数字、数据布局或转换接口 |

修改仅位于共享解析器的前缀及空白判断。解析失败仍不修改输出对象，
原生任意二进制数据（含 NUL）及 `fromBytes`/`toString` 的契约不变。
R208 已接受的 22023/22P02 分类不回退。

## 计划及复现证据

1. 固定同一组输入及断言，先跑旧纯已接受版本、旧工作区和独立 PostgreSQL 18.6。
2. 修改共享解析器；两侧均用各自完整头文件重新编译生产代码及相关原生测试，避免旧 inline 定义混入。
3. 回归原生、存储和协议入口，核对未完成候选/既有暂存内容不被提交。
4. 仅提交本项解析器、两个新夹具、一条注册及本文档；保留未完成事项。

新原生夹具 `tests/bytea_input_grammar_test.cpp` 为 1,957 项：
51 组输入、4 种源类型、raw/bound/parsed/helper 五条路径；整数宽度控制；
直接解析、失败输出保持、全部 256 字节的边界与二进制往返；
实际 INSERT/UPDATE 的成功规范化、错误拒绝、无残留行及原行不变。
原生存储字符串 API 的 NULL 哨兵不冒充 SQL NULL；两者的 SQL 语义在表达式路径单独检查。

新协议夹具 `tests/bytea_input_grammar_protocol_e2e_test.py` 为 951 项。
成功路径严格检查值、NULL、OID17/字段宽度/typmod/来源/格式、消息顺序、tag，
包括 Simple/Extended、文本参数、BYTEA 二进制结果、实际行和空结果。
错误路径沿用 R208 明确的 SQLSTATE-only 断言；不声称 Parse/Bind/Execute 错误时机已修复。
参数成功路径覆盖 OID25/17 的文本输入及两种输出格式；不冒称无效参数阶段已验收。

V1 夹具在两侧旧版完整执行：原生各 259 次断言失败（退出 1），
协议各 88 次断言失败（退出 1）。V1 候选首个纯原生入口完整执行 1,957 项、
仅剩 2 次失败；它们是测试把存储字符串接口的非 NULL 空字符串显示误写为 `\x`。
`TableManage.h` 的接口约定及 `sqlRowFromLegacy` 均保留非 NULL 空字符串，只有精确 `NULL` 是哨兵。
V1 源码、对象、失败日志及实际退出 1 均保留，不冒称首轮已通过。

V2 只将这两条 INSERT/UPDATE 查询结果期望改为接口规定的精确空字符串，
并不接受 `NULL`；未删除输入、减少 1,957 项、修改 SQL/协议或 PostgreSQL 期望。
同一 V2 原生夹具重新对照两侧旧生产对象及各自旧头文件，均完整执行、各 257 次失败，退出 1。
951 项协议源码不变，旧版两侧各 88 次失败及参考库结果保持原始记录。
上述次数是重复路径，不是独立 bug 数量。
同一完整 951 项在新建 PostgreSQL 18.6 实例实际通过，默认 fsync 不变，停止/服务退出均 0。

## 回归及提交边界

纯已接受代码组合和工作区组合各 14 个完整入口实际全 0，共 28 个。
两侧新原生 1,957 项、新协议 951 项均完整通过，没有减少断言或提高 socket 超时。

每侧 8 个原生入口：bytea_input_grammar、bytea_hex_sqlstate、bytea、bytea_functions、
bytea_catalog_identity、bytea_large_toast、cast_structured_error、cast_projection_validation。
每侧 6 个协议入口：bytea_input_grammar、bytea_hex_sqlstate、bytea_declared_identity、
encoding_boundaries、binary_builtin_array_parameter、protocol_parameter_type_identity。
存储原生入口核对 DBStatus 和实际行，不冒称所有 SQL DML 错误码/阶段已经验收。

共享头文件变更后，两侧各自完整 127 份头文件及 58 个生产 TU（含 Main）均核对并重编译，
stub 及每侧 8 个原生 TU 均在本项新编，各程序全部链接当前生产对象。
V2 仅为纠正显示期望重编新原生夹具，其余 7 个复用本项已新编对象，而非旧头文件 donor。
工作区使用正常构建：58 CPP/1 link，随后 repeat 为零编译/零链接；纯代码组合为显式优化编译/链接。
编译器最多 2 个，运行时共用串行锁；不关闭 fsync，不减测试。
Root 与纯代码的 OutputCapture.h 不同，没有混用头文件。

仅接受本项共享解析器、两个新夹具、一条纯协议注册及本文档。
原有 staged 18 路径 binary diff SHA 保持
`a92dc922d20160fca4e2042d6c88fa1ea5672be45500478dce72935303084556`。
已接受版本不含 R207/R205；工作区仍含这些及其他未接受候选，其通过不表示接受这些候选。
接受后入口为 929 native +576 Python +3 Main =1,508；本项不是全 1,508、
完整 72、三个独立 Main frontend、TLS 或整库 sanitizer 验收。
本项不恢复 R207、R205、R201、R197、R185、默认协议主测试及其他用户暂缓范围。
错误阶段、广义常量分析及完整 cast graph 不由此关闭。
原 273 项与冻结原 1,492 入口结果均不改写；不 push，GitHub Actions 保持禁用。

## 本地固定记录

记录目录 `/tmp/dbms-r209-bytea-input.TcLfua0f` 是本地诊断资料，不是发布依赖。
PG 完整参考 receipt：
`/tmp/dbms-private-pg18-full-fixture.2y10cx4f/completed-reference-receipt.json`，
SHA256 `6e60702e004f114f7ef0be947a21c3ba174dd83da3ae431f8044de67d84b65e8`。
旧失败、V1 错误期望和首次候选退出 1 的日志均保留，不重写为通过。

| 本项 receipt | SHA256 |
| --- | --- |
| `completed-prepared-receipt.json` | `2cb21d410a5a9a3a642971c88b88d206bfbf63c565c460b201ea81aee805af3c` |
| `completed-baseline-receipt.json`（V1） | `f4619c32e70f51dd3c707cc5e07d4206c795b9144d165d52d173adf88d7ba890` |
| `completed-v1-rejected-receipt.json` | `1235ab28dd21f24ee7e34dee618e4db2bdd038a6608c1842d24e36925acf2401` |
| `fixture-correction.json` | `9d14528a59bcfcf7845500746cb69e60fd533a61384d10bf329731e74fdaceec` |
| `completed-v2-baseline-receipt.json` | `02251b11b5977fabd14a049d4828751072ece1775e1c4c816ef95cc3710472d0` |
| `completed-build-receipt.json` | `f3585c63abda3f4dc953c7b5fc2f6492f63b1d80766482f286c98780f2316990` |
| 最终 28 入口 `completed-candidate-receipt.json` | `77afe3e057395851555adf4b8ecaba5881f54c9b19b031c99f6ed7a224e314ce` |

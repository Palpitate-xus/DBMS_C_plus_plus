# R208：BYTEA 十六进制输入转换的错误分类

状态：有限修复及下述回归已完成。范围是 TYPE-05/FUNC-01/SQL-11 的有限错误分类，
不是全部 BYTEA 输入语法、错误阶段、cast graph 或原 273 项总清单完成。

## 问题与修改方法

R207 的完整 PostgreSQL 18.6 对照暴露了前置问题：
`CAST('\xGG' AS bytea)` 和奇数位/非法字符的十六进制输入应报 `22023`，
项目统一报 `22P02`；既有 `cast_structured_error_test.cpp` 也将同一 SQL 错记为 22P02。
PostgreSQL 本地 `varlena.c:byteain` 将小写 `\x` 输入交给 `hex_decode_safe`，
后者在 `encode.c` 对非法数字和奇数位使用 invalid parameter value；传统反斜杠/八进制错误为 22P02。

修改只在 `castToBytea` 的已有解析失败分支，按真实小写十六进制前缀选择 22023/22P02。
NULL、正确解码、BYTEA datum、整数的 2/4/8 字节大端转换、类型布局与原生解析接口不变。
既有错误测试保留同一十六进制 SQL、改正期望，并增加传统转义错误仍为 22P02 的控制；原文件备份保留。
R207 的重载候选尚未安装；先独立修复此公共转换错误，再继续其声明验证，用户暂缓项不恢复。

| 输入/控制 | PostgreSQL 18.6 | 旧实现 | 方法 |
| --- | --- | --- | --- |
| `\xq0`、`\xGG`、奇数位、半字节中间空白 | 22023 | 22P02 | 区分十六进制输入的失败类别 |
| `\q0`、尾随反斜杠、非法八进制 | 22P02 | 22P02 | 保留传统语法错误 |
| 空值、空 BYTEA、普通字节、完整字节间空白、有效八进制 | 正确 BYTEA/SQL NULL | 原控制通过 | 不改变成功与 NULL 语义 |
| smallint/integer/bigint 的 0、1、-1 | 对应宽度大端字节 | 原控制通过 | 不改变整数分支 |

## 验证范围与诚实边界

新原生夹具 `tests/bytea_hex_sqlstate_test.cpp` 为 478 项：
23 个输入 ×4 个源类型（unknown/text/varchar/name）×5 条 raw/bound/parsed/helper 路径为 460；
另有 18 个整数宽度控制。错误必须为结构化 DbError 并严格匹配 SQLSTATE。

新协议夹具 `tests/bytea_hex_sqlstate_protocol_e2e_test.py` 为 427 项。
成功路径严格检查 Simple/Extended/binary 的值、NULL、字段/OID17/宽度/typmod/来源/格式、
raw BYTEA 二进制数据、消息顺序与成功 tag；覆盖保存语句的 OID25/17 文本参数、重复执行、
实际表行和空结果、整数转换。
错误路径明确标记 `SQLSTATE_ONLY_SIMPLE`，严格检查错误码、无数据行/成功 tag 和 ReadyForQuery，
如已发结果描述则检查其精确 BYTEA 元数据；不将 RowDescription 是否先出现解释为阶段验收。

首轮 PG 完整 427 项有 52 项 envelope 断言失败：公共 helper 假定所有错误都只有 E/Z，
实际物理表行运行时错误在 PG 为 T/E/Z。原 V1 夹具及完整失败保留，且其源 SHA 已与原 receipt 核对。
本轮分类夹具改用上述明确的语义断言，不伪称错误阶段一致；该真实差异继续记在下节。
最终同一 427 项的 PostgreSQL 参考运行实际 0，未删除任何 SQL/数值/SQLSTATE 控制。

旧纯已接受版本及旧工作区分别完整运行原生 478 项，均 160 项失败、实际 1；
协议 427 项均 64 项失败、实际 1。是同一错误分类的重复路径，不是 160/64 个独立 bug。
原始失败日志未覆盖。

## 最终回归与构建

accepted-only 干净候选和工作区各 35 个完整入口实际全 0，共 70 个。
新 478/427 专项、保留原 SQL 后纠正期望的 cast_structured_error 及传统转义控制均实际通过。

每侧 21 个原生入口：bytea_hex_sqlstate、cast_structured_error、cast_projection_validation、
bytea_functions、bytea、bytea_catalog_identity、bytea_large_toast、bit_integer_cast_value、
internal_char_integer_cast、floating_integer_cast、trim_keyword_identity、trim_projection_semantics、
string_functions、scalar_signature、native_scalar_signature_ownership、string_length_signature、
overlay_projection_semantics、utf8_split_linear、numeric_cast_division、integer_numeric_predicate、postgres_numeric。
每侧 14 个协议入口：bytea_hex_sqlstate、bytea_declared_identity、bit_integer_cast、
bit_integer_cast_descriptor、binary_float_parameter_roundtrip、floating_integer_cast、trim_keyword_identity、
string_length_signature、strpos_signature、utf8_split_linear、scalar_signature、
quoted_unqualified_scalar_projection、overlay_boundary、numeric_cast_division。
错误分类夹具的阶段限制仍如上所述，不因入口退出 0 就扩大其证明范围。

每侧本轮 fresh 一个生产 TU（ExprEvaluator），57 个源/头/编译器/flags 匹配的生产 donor、stub、
127 份各自头文件已核对；Root/clean 的 OutputCapture.h 不同，未混用。
Root 正常 1 CPP 并链接、repeat 零编译/零链接；干净侧为显式优化编译/链接。
每侧 12 个 native TU 本轮新编（本项 baseline 对象在本轮复用），9 个匹配的 R206 donor；
21 个程序全部新链接当前 57 个库对象。Main 未变，未声称 fresh Main、全 58 TU 或三个独立 Main frontend 测试。
运行时串行，编译器最多 2 个；未减断言、提高 socket 超时或关闭 fsync。

仅接受本项 ExprEvaluator 失败分支、旧 CAST 夹具的明确改动、新两个夹具、一条注册和本文档。
不接受 Root R205 及其他未完成候选；staged 18 路径差异逐字节保持。
预期已接受入口为 928 native +575 Python +3 Main =1,506；本轮不是全 1,506/完整 72/TLS/整库 sanitizer 验收。

## 固定证据

证据目录 `/tmp/dbms-r208-bytea-hex-state.ACxSLuP0` 是本地审查记录，不是可移植发布依赖。

| 证据 | SHA256 / 说明 |
| --- | --- |
| `completed-prepared-receipt.json` | `aab88db57f4604bea0659099441a650693097b4a8a146deee5e167584a5ae231` |
| `completed-baseline-receipt.json` | `543946cf4dda1ec95eb2068b1a18ad88c518f50c6b29e67b760756cb33942715`，驱动 0，旧失败预期 |
| 最终 70 入口 `completed-candidate-receipt.json` | `18eed32c41c9d95baad75b07ef90bf2f676bd97064b93eb0f00a3c88f05a3bec`，实际 0 |
| PG V1 427 receipt | `/tmp/dbms-private-pg18-full-fixture.6n4jjfit/completed-reference-receipt.json`，`0414206a1467a0d129aa3723fa3ff913616377c0a1f8ba54373b1cd1d9fe266c`，实际 1 |
| PG 最终 427 receipt | `/tmp/dbms-private-pg18-full-fixture.m3n15ct8/completed-reference-receipt.json`，`f847069ac04ef4758d85ffaa800199b9616bc9cdd8718eb1a12bfb51b74b83b8`，实际 0 |
| PG 38 次普通发现观察 | `/tmp/dbms-private-pg18-full-fixture.xb5wioks/completed-reference-receipt.json`，`6e2172c2eff37afa4445df7517be5581743c45c3078854d92a720afe30d87790`；不是兼容性通过 |
| PG 参数阶段观察 | `/tmp/dbms-private-pg18-full-fixture.2ahr6wlb/completed-reference-receipt.json`，`332b779b6ab9702233d4ebc30fdefb70f56450189444f7125fa822d12ada9529`；不是兼容性通过 |

参考库均本次自建 PostgreSQL 18.6，默认 fsync 未改变，停止及服务退出均 0；未操作已有服务器。

## 仍开放的问题和方法

- R207：btrim/ltrim/rtrim 的三个具体重载及实际 callback 所有权仍待实现/验收。
  其 PG 首轮 3,780 项中 54 条错误码期望曾误写 22P02，V1 全部保留；
  仅更正为 22023 后完整 3,780 已过，但未编译/验收 R207 产品候选。
- BYTEA 输入语法：现有 `ByteaValue::parse` 还接受 PG 拒绝的 `\X00` 和十六进制中的 VT/FF。
  方法是对照 byteain/hex_decode_safe 收紧前缀和四种允许空白，逐个核对所有输入入口及原生 API，
  不由此次错误分类关闭。这是普通开放问题，不是新增用户暂缓。
- 错误阶段：非法常量的 PG Parse 会失败，旧产品 Parse 成功；非法 OID25/17 文本参数在 PG Bind 失败，
  旧产品在后续执行失败；表行转换错误 PG 已发 RowDescription，产品未发。原始观察保留。
  方法是按声明、实参输入和真实执行边界分开验证，避免用一次 runtime 失败代替前置分析，
  并在真实执行错误时保留正确的描述。既有用户暂缓的广义常量分析/整套 carrier 重构不重开；
  此文不称这些阶段差异已解决，未暂缓的部分继续普通待修。

R205、R201、R197、R185、默认协议主测试及其他已跳过范围保持不变。
原 273 项 22/166/70/15 标签、冻结原 1492 的 1481 PASS/11 FAIL 均不重写。
不 push，GitHub Actions 保持禁用。

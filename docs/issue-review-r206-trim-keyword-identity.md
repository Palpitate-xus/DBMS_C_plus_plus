# R206：TRIM 的语法身份与参数顺序

状态：有限修复及下述回归已完成。范围是 SQL-01/SQL-11 的 TRIM 语法及 FUNC-01/TYPE-04
中的有限字符串行为，不是完整 grammar、错误位置、函数族或字符串函数重载表的完成证明。

## 已复现问题与修改方法

| 问题 | 旧已提交版本 | PostgreSQL 18.6 | 修改方法 |
| --- | --- | --- | --- |
| 普通字符串被当成方向标记 | `trim('both')` 返回 SQL NULL；`trim('leading','l')` 返回 `l` | 分别为 `both`、`eading` | 在解析时识别真正的方向关键字，不检查实参值 |
| 省略方向的 FROM 形式参数颠倒 | `trim('x' from 'xxbothxx')` 返回空串 | `both` | 将源表达式与字符集表达式排列为实际 callee 的参数顺序 |
| 合法 FROM 前缀与嵌套表达式被破坏 | `trim(from '  both  ')` 报 42703；嵌套 SUBSTRING 的 FOR 被误当列 | 查询成功并返回 `both` | 单独解析当前 TRIM 的参数，不改写调用者或子表达式的 token |
| 错误阶段及语法被忽略 | 空参数、缺失源、重复 FROM、尾随实参 token 被接受或错报 | 42601，Extended Parse 失败且无结果描述/成功 tag | 必须存在实参并消耗到当前右括号，通过已有结构化语法错误通道拒绝 |
| callee 身份不明确 | 语法被表示为带特殊实参的 `trim` 回调 | 语法对应系统 `btrim/ltrim/rtrim` | 仅将未引用、未限定的 TRIM 关键字降低到 `pg_catalog` 中的实际 callee |

生产修改位于 `src/parser/parser.cpp` 和 `src/main.cpp` 的无表结果标签分支。后者从规范 AST 中取真实 callee 名，
不再因为字符串包含 leading/trailing 而猜错列名。参数子树仅移动一次；字符集 FROM 源的顺序变为源、字符集，
不通过求值猜测语法或类型。原生 `trim` 回调名、同名 opaque 回调和引用/限定函数调用接口保留。
PostgreSQL 18.6 的本地 `gram.y` 中 TRIM/trim_list 规则及真实协议结果共同用于核对。

## 回归与已保存的失败

新原生夹具 `tests/trim_keyword_identity_test.cpp` 有 1,281 项：
244 个表达式各经普通/绑定解析、raw/bound 求值及 helper 五条路径；
另有 16 种错误语法的两种解析入口、24 个 opaque 回调所有权/实参/纯绑定控制、
4 个原生 trim 名称控制及私有数据库 setup。结构与数值检查不是独立缺陷数。

新协议夹具 `tests/trim_keyword_identity_protocol_e2e_test.py` 有 1,218 项：
Simple/Extended/binary 精确值、UTF-8、CHAR/VARCHAR、BYTEA 原始发送字节、SQL NULL 与字面量 NULL、
非别名列标签、OID/宽度/typmod/来源/格式、命令 tag、消息序列、ReadyForQuery；
保存的语句反复绑定 text/binary 参数、Parse/Bind Describe 不执行副作用；
实际表行/WHERE false/LIMIT 0、序列参数求值顺序、显式 search_path 中的同名用户函数、错误阶段。

旧纯已接受库和旧工作区均完整执行原生 1,281 项，失败 1,145 项；协议 1,218 项失败 553 项。
原生多数失败包括旧 AST 非规范系统调用的结构差异；这些数目不能称为 1,145/553 个 bug。
两侧退出均为 1；未删除旧失败断言，也不以新运行覆盖旧日志。

首轮 PostgreSQL 完整 1,218 项有一项夹具期望错误：search_path 的不必要引号会在返回值中去除。
V1 原始夹具和终态失败保留；仅纠正该期望后，完整 V2 1,218 项实际退出 0。
原生夹具在首次编译前已经定稿；未利用候选输出生成期望。

首个 parser-only 产品候选的全部 14 个原生入口通过，但完整 V2 协议 1,218 项仍有 22 项失败，
驱动在完整失败 footer 后退出 1，未完成另 29 个入口，不能称完整 44 通过。
其中 5 项为实际 Main 列名分支仍检查文本内容，本轮继续修复；
4 项为测试准备语句的函数 DEFAULT 参数尚不受支持，导致其后 12 次显式调用也失败；
最后 1 项为默认 RESET search_path 的产品/PG 差异。原 V2 夹具和该完整失败记录保留。

最终 V3 的 TRIM 控制数量仍为 1,218，四个同名 shadow 函数改用实际调用所需的显式参数签名，
清理改用确定的 SET search_path。没有把 DEFAULT 或 RESET 的错误输出改成正确期望，
这两个独立问题仍未修复，原 SQL 与修改方法列在下节。V3 已重新跑完完整 PG 1,218 项并通过；
两侧旧库 V3 各完整失败 536 项、实际退出 1。没有用 V2 的旧失败或 PG 通过冒充 V3 证据。

旧工作区 V3 首次启动失败是驱动备份文件缺少执行权限，未发送 SQL；原日志和终态 1 保存。
只给本次自有备份补齐执行位、核对文件内容 SHA 不变后，才完整执行上述工作区 V3 基线。
没有修改产品超时、断言或把启动失败当成产品语义结果。

## 最终回归和构建范围

最终 V3 在 accepted-only 干净候选与工作区各 22 个完整入口实际全 0，共 44 个。
新原生 1,281 和协议 1,218 项均完整通过；既有夹具、超时和默认持久化规则未弱化。

每侧 14 个原生入口为 trim_keyword_identity、trim_projection_semantics、string_functions、
utf8_split_linear、reverse_projection_utf8、translate_projection_utf8、left_right_projection_utf8、
bytea_functions、scalar_signature、native_scalar_signature_ownership、substring_projection_semantics、
string_length_signature、pad_projection_bounds、overlay_projection_semantics。
每侧 8 个协议入口为 trim_keyword_identity、utf8_split_linear、reverse_linear、strpos_signature、
string_length_signature、quoted_unqualified_scalar_projection、scalar_signature、overlay_boundary，
包括既有完整 329/96/294/654/791/132/94 项。

每侧本轮 fresh 两个生产 TU（Parser、Main），56 个匹配生产 donor、stub 和 127 份各自头文件已核对；
两侧 OutputCapture.h 不同，干净候选没有使用工作区头文件。Root 正常构建先只编 Parser 并链接、
再只编 Main 并链接，两次 repeat 都零编译/零链接；干净侧为显式优化编译/链接，未冒称其常规缓存构建。
14 个 native TU 均在本轮首次候选阶段新编译，14 个程序新链接各自当前 57 个库对象；
最终阶段重用这些同库程序，因为后续只改不参与 native 链接的 Main。不是再次 fresh 14 个 TU。
当前实际 server 含本轮新 Main；未运行三个独立真实 Main frontend 测试或声称全 58 TU fresh。

只接受纯候选 Parser、Main、一条协议注册、两个新夹具与本文档；
工作区 R205 候选和其他未接受改动不进入提交。原 staged 18 路径差异保持逐字节不变。
已接受登记入口预期为 927 native +574 Python +3 Main =1,504；不是 1,504 全量、完整 72、TLS 或整库 sanitizer 验收。

## 验证证据

临时证据目录 `/tmp/dbms-r206-trim-identity.R2NZJAyA` 不作为可移植发布依赖。

| 证据 | SHA256 / 结果 |
| --- | --- |
| `completed-prepared-receipt.json` | `6d309ac786b5dbcf7ee9056d86aebd5c546a88a8544315fb7f8add6dc247f206` |
| `completed-baseline-receipt.json` | `364067930ca9c3faa9494a2dfd51969d12fcf4970e7a4896e8314a94bb41b002`，旧失败符合预期，驱动 0 |
| V3 旧完整协议基线 `completed-v3-wire-baseline-receipt.json` | `73242047fae8d799f2cd861946179fe517d8b7dfa4f07d85c9c22a5c35cb0fcb` |
| parser-only 完整失败 `completed-v1-candidate-failure-receipt.json` | `e922652fb7b4ba611e291933e9f044fa2c7e139c09a47e874e8227f80a61902d` |
| 自有备份启动失败 `completed-v3-baseline-launch-failure-receipt.json` | `52a25ef75071a2c401a66eed759e9d3fa0afbf66f9e0449f26465e3b87c7416b` |
| 最终 44 入口 `completed-candidate-receipt.json` | `dd5594751c921322114969b00aa98a9e567d9e134215c278313b71e6290849da`，实际 0 |
| PG32 次发现观察 receipt | `/tmp/dbms-private-pg18-full-fixture.ijdycwv8/completed-reference-receipt.json`，`dab80f8d1b8179a186eee4c303ae45443887f5fd40d8c80ffbd0285a60096abb`；不是 32 项兼容性通过 |
| PG V1 完整失败 receipt | `/tmp/dbms-private-pg18-full-fixture.wnjxc0mr/completed-reference-receipt.json`，`66438e8980cf03268f6cb0f55b19d20c3885fcd47b062266d5b21668ae80e268` |
| PG V2 完整通过 receipt | `/tmp/dbms-private-pg18-full-fixture.44wcv52t/completed-reference-receipt.json`，`37f886d41069d4f587b6d37863d9e6c9344aead3356951426d00e9fc39301ecc` |
| PG V3 完整通过 receipt | `/tmp/dbms-private-pg18-full-fixture.5p7cjhr_/completed-reference-receipt.json`，`4a5acbccd804ef9854b2c7713574d34db5d00e33996418b6518df417856e8902` |

参考库均为本次新建的私有 PostgreSQL 18.6，保留默认 fsync，停止和服务退出均为 0；未操作已有数据库实例。

## 仍未完成与版本边界

实际 callee 的完整签名是下一独立问题：发现观察中 `trim(1)` 及三参数 TRIM 被产品接受，PG 为 42883；
降低到实际 callee 后也仍需独立验证其完整重载。修改方法是为实际 btrim/ltrim/rtrim callback 增加 TEXT/BYTEA
具体签名及 callback 所有权标记，验证错误阶段、空输入、重载搜索、返回元数据和 opaque 替换，不能只按 SQL 名称强制转换。
本次语法修复不能关闭该重载问题、所有嵌套语法上下文、通用 cast graph 或原 273 项清单。

另两个普通开放问题不按用户暂缓处理：

- `CREATE FUNCTION schema.f(s text,c text DEFAULT '') RETURNS text LANGUAGE SQL IMMUTABLE AS $$SELECT 'shadow'::text$$`
  被产品以 42601 拒绝，PG 成功；需要解析、保留、分析并按实际重载绑定默认表达式，覆盖调用、依赖与副作用语义。
  对应 SQL-09/FUNC-05 的默认参数要求，本次不关闭。
- `RESET search_path` 在产品报告 `public`，本次 PG 实例为 `"$user", public`。
  需要按会话实际默认值验证恢复和 ParameterStatus，不能将两种字符串都放入通过白名单。

R205 及其他用户暂缓项不恢复，候选源码/注册/测试保留为未接受工作区内容。
原 273 项 22 complete /166 partial /70 unverified /15 deferred 标签不变；原 1492 的 1481 PASS/11 FAIL 不重写。
不 push，GitHub Actions 保持禁用。

# 原始 1492 入口全量回归：终态与后续修复

## 结论和范围

冻结版本 `2442e4c77030e3ba9fd9a7ff86ee270245fb1770` 的同一次全量运行已结束，
实际退出码为 1：1492 个唯一注册入口按原顺序全部返回，1481 PASS、11 FAIL、0 pending。
这是该冻结版本的结果，不是当前 HEAD 的修复后全量结果。
后续定向通过不能改写原始失败，也不能据此宣布总清单完成。

| 类别 | 入口 | PASS | FAIL |
| --- | ---: | ---: | ---: |
| 原生测试 | 921 | 916 | 5 |
| 真实 Main TU 检查 | 3 | 3 | 0 |
| Python 注册入口 | 568 | 562 | 6 |
| 合计 | 1492 | 1481 | 11 |

921 个原生入口全部重新编译、链接；3 个 Main TU 检查也实际重新编译。
58 个生产对象及 1 个 stub 使用逐字节核验过的同源 donor，并非本轮 fresh 编译全部生产代码。
冻结源码的 2206 个文件及 donor/server 字节均保持原值。
原入口、SQL、断言、执行顺序、持久化设置及各夹具原有超时均保留。
协议入口具有不同原始超时；不能将它们一概写成 15 秒。

## 11 个原始失败及修改方法

以下状态仅针对所列反例；“定向修复”不表示对应完整需求已验收。

| 原始失败入口 | 现象 / 已核实原因 | 修改方法和当前状态 |
| --- | --- | --- |
| `bit_compact_list_receiver_admission_test` | 夹具把 int4/int8 接收声明混用，原退出 134 | R186 修正声明并保留拒绝边界；已有定向验证。见 [接收声明记录](issue-bit-compact-list-admission.md)。 |
| `correlated_integer_parameter_origin_test` | 原退出 2；根因尚未确定 | R187 只增加失败上下文，不能算根因修复；继续核对参数来源和实际绑定值。见 [未完成记录](issue-correlated-integer-parameter-coverage.md)。 |
| `create_type_shell_test` | 故意把 `.types` 换成目录后，旧夹具只接收 bool，未捕获 checked-read 58030 | R188 精确接收 I/O 错误并增加相邻文件、shell 存活及无事务泄漏检查；不放行 DROP。见 [checked-read 记录](issue-review-r168-composite-namespace-owner.md)。 |
| `drop_cascade_test` | 旧夹具缺失 native owner，实际受检查的 XX001 | R189 补足真实所有者夹具并定向验证；所有者完整性检查保留。见 [所有者记录](issue-review-r168-composite-namespace-owner.md)。 |
| `physical_attribute_length_test` | 102 项检查中 1 项仍把当前 TIME 长度写成旧 4 字节 | R190 分开当前存储与旧格式兼容边界；定向验证已通过。 |
| `tests/postgres_protocol_test.py` | search_path 错误码预期 22023，实际 42601 | 用户明确跳过；不修改、不重试、不冒称通过。 |
| `tests/transaction_begin_grammar_protocol_e2e_test.py` | aborted savepoint 内非法 BEGIN 预期 42601，实际 25P02 | 待核实：分别验证简单/扩展协议及 PostgreSQL 18 语法检查优先级，再定位状态门与解析分支；不能直接翻转预期。 |
| `tests/copy_protocol_e2e_test.py` | 本连接 COPY 等待自身有效 CancelRequest 的 57014，读取超时 | 待核实会话中断状态生命周期和读等待唤醒；保留该测试及其原 15 秒超时，不以调大超时代替修复。 |
| `tests/index_corruption_protocol_e2e_test.py` | COUNT 查询预期 XX001，实际返回精确堆结果 `1` | 待收集全部查询的真实执行计划；区分读取坏索引与独立堆读取，不让未使用该索引的堆查询无条件报错。 |
| `tests/pg_class_query_protocol_e2e_test.py` | pg_class 的普通 COUNT 报 XX000：`mutation source descriptor differs from physical relation` | 待核对逻辑目录列与物理扫描描述，修复正确来源路径；不能全局移除描述检查或伪造查询结果。 |
| `tests/temp_index_catalog_protocol_e2e_test.py` | 临时索引目录 COUNT 同样报上述 XX000 | 与 pg_class 失败一起调查；需保留临时索引发布、撤销、savepoint 回滚和 DISCARD TEMP 的完整验证。 |

R190 的修复提交为 `1eab4c69`。默认协议跳过项记录在提交 `2bf24e16`。
本表保存失败证据和处理方法，不把待调查假说写成已经定位的产品原因。

## 本轮以外的已验证普通反例

- R191 修复浮点 NaN 的 SortOp 排序比较，并扩展原有测试；见 [排序记录](issue-review-r191-floating-executor-order.md)。
- R192 修复 MIN/MAX 猜测字符串类型造成的错值，改用实际列声明比较；见 [聚合记录](issue-review-r192-declared-minmax-comparison.md)。

二者具有独立提交、旧红/新绿和定向构建证据，但冻结全量仍使用旧版本，原结果不被替换。
用户暂缓的 R185 排序中断和其他已跳过分支不恢复，未接受的工作区改动仍不自动计入已完成。

## 总清单证据边界

原 273 项审计标签仍为 22 complete、166 partial、70 unverified、15 deferred。
187 项至少直接引用一个注册入口，86 项没有这种直接引用；前者不等于完整证明，后者也不等于未实现。
本轮未据此关闭任何完整需求。单入口 PASS 不证明其全部条件分支执行过，
也不是完整 TLS、整库 sanitizer、崩溃/恢复矩阵或 PostgreSQL 18 全兼容证明。
所有 Git 提交仅保留在本地；不 push，GitHub Actions 保持禁用。

## 原始本机证据

- 终态 receipt：`/tmp/dbms-accepted2442-full1492.9obmw81r/completed-canonical-receipt.json`。
  SHA256：`4005e670928e604302ca4b9073b24bb87064d3d136feea03a7a962e5ac6650ca`。
- 全量原日志：`/tmp/dbms-accepted2442-full1492.9obmw81r/canonical-full-registered.log`。
  SHA256：`66d5b5ffd1e20f8c1cbce0c827079fc37804f0a951f6db7d2866bb5f313f15ba`。
- 原 273 项到入口映射：`/tmp/dbms-canonical1492-requirement-coverage.rjs0run2/completed-partial-coverage-receipt.json`。
  SHA256：`b3281a521e6084587e281b4e3cef869d989fe46f13253aba892d8508490270b7`。

这些 `/tmp` 文件是本机运行证据，不是可移植发布包；coverage receipt 的 partial 指需求证明仍不完整，
不表示原 1492 入口还在运行。

# R292：标识符中的美元符被误认为参数

状态：本项参数 token 识别已作有限修复；完整协议仍有下述 R293 问题，不能称为全绿。
原 273 条总清单未完成；R291 及其他用户明确暂缓项不恢复。

| 缺陷 | 影响 | 修改方法 |
|---|---|---|
| 未将裸标识符作为完整 token 消费 | `value$1` 被当成参数，PREPARE 错增槽位、EXECUTE 错报参数数量或 Bind 失败 | 从标识符首字符扫描完整字母/数字/下划线/美元符续字符 |
| 标识符中 `$0` 或很长数字串触发参数验证 | 合法查询、别名、LIMIT 0、Describe 也可能报 42P02 | 只对标识符外真正的 `$n` 验证位置和范围 |
| 标识符中的 `$tag$` 被当成字符串起点 | 后面的真实参数被忽略，分析与替换都可能漏掉 | 标识符内部美元符不进入 dollar-quote 状态 |

只修改 `src/utils/prepared_stmts.cpp`；不改 Main、Parser、Network、头文件、
类布局或磁盘格式。UTF-8 标识符字节不依赖 locale 分类；原字面量、引用标识符、
嵌套注释、E 字符串、真正 dollar-quoted 字符串及参数错误检查保留。
不是完整 SQL lexer/语法校验，也未实现其他 PREPARE/EXECUTE 语法缺口。

[PostgreSQL 18 词法规则](https://www.postgresql.org/docs/18/sql-syntax-lexical.html)
允许标识符的后续字符含 `$`；本项据此及独立真实 180006 实例验证，不把 `$n`
在标识符中的字节误当成参数，也不改变真正位置参数的含义。

## 检测与未完成边界

| 检测 | 实际结果 |
|---|---|
| 同一原生专项，17 类标识符及真实参数/引用/注释/错误控制 | 旧两侧各完整 556/192 失败、exit 1；新两侧各完整 556/0、exit 0 |
| PostgreSQL 18.6 完整最终协议夹具 | 226 控制、0 失败、exit 0 |
| 旧产品完整最终协议夹具 | 两侧各 226/82 失败、exit 1 |
| 新产品完整最终协议夹具 | 两侧各 226/14 失败、exit 1；不是整批通过 |
| 原有相邻完整回归 | 两侧合计 26 条实际均 exit 0：12 native、6 真实 Main、8 wire |
| scoped UBSan | 两侧各完整 556/0、exit 0；仅 helper CPP 与专项夹具插桩 |

修复前后使用同一原生夹具。初版协议 226 控制中，14 条错误预期“零参数
EXECUTE 多传参数应报 42601”；真实 PG18 返回原查询结果。最终版保留所有
226 条 SQL 和控制，只将这 14 个预期按参考改为精确行、OID 和 tag；初版源码、
参考及产品原始失败均保留，没有删案例或提高 deadline。

相邻回归覆盖 prepare_pg、dollar_quote、prepared_query_execution、
prepared_source_context、prepared_unknown_input、prepared_dml_plan，三个真实
Main 入口及四个原有完整协议入口。2 个生产 TU/2 新夹具/12 邻居 TU 本轮新编译；
其余 120 个生产对象、2 stub 和 6 Main TU 均核对源码、当前头、依赖及对象后复用，
18 个邻居程序和 2 个服务器新链接。复用不是全新编译；没有使用 R291 候选头/对象。
首次邻居构建在全部 18 次链接通过后发生 argv Path 的 JSON 序列化错误；只修正
记录标签并从既有成功日志、对象与 MMD 恢复元数据，没有重复编译或伪造运行通过。

完整协议原样保存为 `tests/prepared_identifier_parameter_known_gap.py`，可手工
运行并明确失败，不作为已经通过的 E2E 注册。不是删掉 14 个失败后制造 212 项全绿。
本轮合计 32 条有限运行记录中 30 条零退出、2 条协议非零；没有全测试库存、
冻结 1492、整库 sanitizer 或原 273 条验收声明。

## R293：另一个仍需修复的错误

剩余 14 条全部是 `EXECUTE identifier_review_N(1)`：PREPARE 已正确识别为零参数，
项目仍报 42601；PG18.6 返回原常量结果。普通名字也可复现：

```sql
PREPARE zero_args AS SELECT 7 AS plain;
EXECUTE zero_args(1);
```

根因方向是 Main 的 EXECUTE 将所有参数数量都作严格相等比较；候选方法是使零参数
PostgreSQL 形式遵从参考的参数忽略规则，仍保留非零参数 arity、合法语法检查、
错误恢复、旧 USING 形式及不求值被忽略表达式的行为。还须核对 `$0` 等参数、
DECLARE 了未使用类型、非法表达式与额外参数副作用，不简单删除数量检查。
本项未改 Main，R293 未实现；不能把 helper 的 native 通过算作它已修复。

版本管理只提交本 CPP、原生回归、完整 known-gap 夹具和本说明；工作区其他
草稿与原 18 处暂存二进制 diff 保持不变。R290 全通过 donor 仅是历史证据，
不冒充本轮新源码完整协议验收；本轮匹配构建单独标注完整协议 gate 1。
没有 push，没有启用 GitHub Actions。

证据目录 `/tmp/dbms-r292-prepared-identifier.Gcv1koSF` 不保证长期保存。

| 证据 | SHA-256 |
|---|---|
| 隔离与十一源排除 | `58fbd10593639c44e22c14f467ffd86756dc6a26acd85a62f1758551b322d635` |
| 初版完整失败 | `474f7e8b0b4a8324a0aa6733ae46d7fe92a3a4c062e0f5c4d6ea19fceed80c8f` |
| 保留全部 SQL 的 oracle 修正 | `e9555abd763a5869f3ff8b56810d6fcd18fac88e76460a2c5108fe660997f73d` |
| 最终 PG226/0 与旧产品226/82 | `aa9c1a905a68478db445d79b9fa62d45da36d4e182d735e95e700d2c6036fd92` |
| 候选边界 | `75ab5fcb49030ed5e99be89f6933795d5023767c9c248d820bda17fcb3c5091d` |
| 同头匹配构建 | `4a73f6b4c691812bcf1903e864efb972d58ce7975673ddeb09fcee0d7f616663` |
| 新 native556/0、完整 wire226/14 | `6b3ecb1116b2ab56c4c629710cc22ef14978e4b74a5386dd0d9e5fbc5f1ff10c` |
| 相邻构建和元数据修正 | `bdad07092831df3586ee748ccf2dfc0cf4e4d37a8882f3b7e635e19e4f01d1ee` |
| 完整相邻26条 | `cef672e9bcad9238aeca3fb5914d70507e08d3c19b10261d19d7bbb2da945c5f` |
| scoped UBSan | `7be80d862d5ca83edce7da59a1825d7b057f6082a2665d936867d529b0b03f77` |
| 完整失败夹具保存 | `09106a26a79a96dae3e247ae9f25d233638300372a737803daa99abeed0299a4` |
| 源码/对象/工作区复核 | `4cf64918a9e71f8ce69bd090f8b62c3cc4db62b1c5b555a945ca2e6e1ca3e949` |

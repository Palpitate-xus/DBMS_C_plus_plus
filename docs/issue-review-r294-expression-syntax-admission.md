# R294：普通 SELECT/PREPARE 的表达式语法接纳（用户暂缓）

状态：按用户最新“skip it”暂缓，不继续本项实现。此前在 R293 已验收生产代码上
独立复现的问题仍未修复；普通 continue 不恢复本项。
不恢复 R291、R288/R99、R222、R51、R267 或其他明确暂缓范围；尤其不以本项
语法检查重开已暂缓的 CAST/CASE typmod、函数体元数据等语义模块。
原 273 条总清单仍未整体完成。

## 实际影响与对照

相同顺序的 23 条 SQL 分别发送给独立 PostgreSQL 180006、新 Root 集成服务器和
纯提交服务器，保存 rows、SQLSTATE、列名、OID、tag、完整消息。每侧采集脚本
exit 0 仅表示收集完整；按 rows/SQLSTATE 比较，两侧产品各有 17 处差异，不能
称为 23 项通过，也不能将重复模式计作 17 个独立 bug。

| SQL / 场景 | PostgreSQL 18.6 | 当前两侧产品 |
|---|---|---|
| `SELECT 1+`、`SELECT 1*` | 42601 | 42883，错误地进入名称/操作符分析 |
| `SELECT NOT` | 42601 | XX000 |
| `SELECT (1` | 42601 | 错误成功，返回 1 |
| `SELECT abs(1,)`、`SELECT coalesce(1,)` | 42601 | 错误成功，返回 1 |
| `SELECT ARRAY[1,]`、`SELECT ROW(1,)` | 42601 | XX000 |
| CASE 缺少 THEN、缺少 END、简单 CASE 缺少 THEN | 42601 | 错误成功，返回 1 / 1 / 2 |
| `SELECT CASE ELSE 1 END` | 42601 | 42703 |
| `PREPARE r294_bad AS SELECT 1+` | 42601，不能发布语句名 | 错误成功，返回 PREPARE |
| 随后 `EXECUTE r294_bad` | 26000，名称不存在 | 42883，说明已留下坏语句 |
| 随后 `DEALLOCATE r294_bad` | 26000 | 错误成功，坏语句此前确实被保存 |
| PREPARE 包含缺 THEN 的 CASE | 42601 | XX000 |

另外五个健康查询/控制保持可观察：正常 CASE、`1+2`、`abs(-1)`、最后的连接
可用性检查，以及下面不可误判的合法别名。其余未知 prepared 名称控制也保留。

重要：`SELECT 1 AND` 在 PG18.6 中是合法目标别名写法，结果为 1、列名 `and`、
OID 23、tag SELECT 1；本产品却报 XX000。不能将它简单当成“AND 缺右操作数”
的负例，也不能为了拒绝 malformed AST 而破坏目标列的合法裸标签语法。
表达式实参内的 `1 AND` 与 SELECT 目标别名不是同一语法角色。

## 源码线索与待实施方法

前一项原生观察已显示，`parseForBinding("SELECT 1+")` 可以返回有效状态，
却带缺少右操作数的 BinaryOp；普通函数调用和 CASE 还缺必需分隔/关键字检查。
R293 只为 EXECUTE 参数建立了纯语法上下文，没有借此宣布普通 SELECT/PREPARE
已严格解析。当前 Simple SQL 的成功结果证明这不只是原生 API 的表面状态。

1. 在实际到达的原始 SQL 上完成语法接纳，先于 sqlProcessor 改写、名称绑定和求值；
   调查共享 raw-syntax 入口是否已经到达 CLI/Simple/Extended 的正确所有权层。
2. 复用并补全表达式语法检查：括号、函数/数组/ROW 的缺实参、运算符必要节点、
   CASE 的 THEN/END；保持 SELECT 目标标签与嵌套实参的不同角色。不能用执行
   抛出的 42883/XX000 代替语法 42601，也不能只检查 ParseResult.isValid。
3. PREPARE 先检查完整查询体，再发布名称、声明与元数据；失败后 EXECUTE/DEALLOCATE
   必须报告 26000。与合法查询体的后续类型/名称/执行错误阶段区分，不提前求值。
4. 以真实 PG 固定完整正负夹具，包括合法 `AND` 裸标签、未知函数错误优先级、
   PREPARE 失败后的命名空间、错误后的连接恢复；保留 R293 的完整 286/419、
   原 R292 226、旧 USING、真实 Main 及相邻回归。未知/未覆盖语法继续记录，
   不把新入口只对小样本通过当成整个 PostgreSQL grammar 完成。

上述只是方案；尚无 R294 候选生产修改、修复后构建或通过声明。
本提交只保存问题和证据。生产代码仍为 R293 `031b46f2`，其后的 `499f1573`
只纠正文档里的转义名称，src/tests/scripts/cmake 内容完全相同。
已有工作区草稿与原十八处暂存 diff 原样保留，没有 push，Actions 禁用。

证据目录 `/tmp/dbms-r294-expression-syntax.QvhZ32Jd` 不保证长期保存。

| 证据 | SHA-256 |
|---|---|
| 当前源/头/对象/工作区隔离 | `b3677ce8ed1e567e8d567d525fc165ffa1c945ad305cca1507d59012349c8808` |
| 三侧各23条完整观察 | `deee559d6cd187c76cdd607ef2f791621ae05387279f6f14b2e43f389e718db2` |
| reference 原始日志 | `f3022bf7b7561fa02b63010daf938d4adfc92437fefebc212bb5545cf7f87e8e` |
| root 原始日志 | `bc5a33a2303c1f56e067cce85755a9eae1cea33e7cbe2588b7cb9b6cbd5064f2` |
| accepted 原始日志 | `bc5a33a2303c1f56e067cce85755a9eae1cea33e7cbe2588b7cb9b6cbd5064f2` |

## 用户暂缓记录

本次跳过覆盖本项的普通 SELECT/PREPARE 表达式语法接纳、合法裸标签处理及
语法/分析错误阶段区分。未编写候选生产代码，未注册新测试，未宣称修复通过。
在用户跳过前启动的三侧各 65 条扩展观察已自然结束，私有服务器已清理；该批
仍只是采集，不是 65 项通过，也没有在跳过后重跑。
扩展观察收据 SHA-256：`df3acd6ad361f8cabca340016b134073bf95ce9e4752ed40b2e027a0669d2865`。

生产代码仍为 R293；已暂缓的其他模块继续保持暂缓。下一项仅检查独立的二进制
NUMERIC 输入是否保留零的小数位数，不重开 R271 数值运行时域模块。
原 273 条总清单尚未全部完成，不 push，不启用 GitHub Actions。

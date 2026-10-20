# R179：STRPOS声明签名、错误阶段与同名覆盖

状态：Open，已完整复现；候选尚未安装、运行验收或提交为生产修复。
基线为R178源码提交`1430b578`，不恢复R177或其他用户暂缓模块。

## 实际问题

| SQL/场景 | PG18.6 | 当前accepted产品 | 修改方法 |
| --- | --- | --- | --- |
| `strpos(123,1)` | 42883 | 1 | 只给真实builtin绑定TEXT/TEXT→INTEGER签名，禁止把任意值字符串化后接受 |
| `strpos(NULL::uuid,NULL::uuid)` | 42883 | NULL | typed NULL仍参与声明解析，不能以strict NULL绕过无重载错误 |
| BYTEA/BYTEA调用STRPOS | 42883，无该重载 | 0 | 不把合法的BYTEA POSITION重载移植到STRPOS |
| `strpos()`、一个实参、三个实参 | 42883 | NULL或忽略额外实参返回值 | 准备阶段验证精确arity及named arguments |
| 空表、WHERE false、LIMIT0、未选CASE/COALESCE分支 | 错误仍在分析阶段 | 多处错误被抑制 | 共享真实查询绑定，不能执行输入来猜签名 |
| 同名schema的INTEGER/INTEGER重载，catalog优先路径 | 选择真实typed用户重载，103 | 部分走builtin返回0或给TEXT描述 | 解析实际声明/namespace，结果描述与执行使用同一callee |
| 不存在的quoted大写STRPOS，extended/binary | Parse失败42883 | Parse成功后Execute才失败 | 入口可按名称族进入分析，但实际callee身份不折叠、不改写 |

UTF8文本搜索、空needle/空haystack、合法NULL及NAME/VARCHAR/CHAR的控制一并保留。
PG目录实查STRPOS只有TEXT/TEXT→INTEGER；POSITION另有TEXT、BYTEA及BIT重载，见[R180](issue-review-r180-position-call-identity.md)。

## 完整严格294项观察

- 同一冻结夹具在PG18.6实际外层0、294项全通过。
- 旧accepted-only R178产品实际外层1；完整294项中154匹配、140断言差异，全部raw响应到达ReadyI，UUID清理确认。
  这些是多种协议/需求/错误阶段的重复控制，不是140个独立缺陷或154项候选通过。
- 范围包括三协议、错误arity/类型、未选分支、空输入、严格输出字段/NULL/binary datum、typed参数、
  UNKNOWN wire参数真实推断、重Bind/Execute、用户同名重载、quoted身份、真实nextval及metadata纯度。
- 原生76项目前为草稿，覆盖静态绑定、真实evaluator、legacy typed cells及opaque native replacement。
  V1三个生产CPP及原生夹具四次syntax-only均0；没有对象或候选数据库通过。
- 静态清单最初把四个setup漏计为290，source-only检查在启动任何数据库前失败；修正为实际294。
  在第一次PG运行前也校正了既有Wire.command接口的参数形式，未改已运行断言来取得通过。

## 候选与验收计划

1. 真实STRPOS callback拥有具体声明；`registerFunction`替换仍保留自己的arity、输入、metadata与volatility。
2. Main及Parse前置只接入纯声明绑定，保留实际quoted/schema身份；UNKNOWN参数从声明推断，不替换成NULL SQL。
3. 独立V2候选仅改Main、ExprEvaluator、NetworkServer三个CPP，无共享头变更。
   路径`/tmp/dbms-r179-strpos-signature-candidate-v2.THVaotzb`；V1及原输入不再改写。
4. 先完成实际旧native反证、新native76及完整候选294，再运行原80/190/654与相邻原生。
   Main改变后，三个真实Main测试体必须fresh编译、新链接当前库、隔离运行，不复用旧Main对象。
5. 候选通过后才安装Root，按拥有hunk保留原有改动，正常/repeat构建及双侧定向验证后逐项本地commit。

本计划不代表已经通过。R180、完整FUNC-01、R164超时及原273未完成状态继续保留；不push，Actions禁用。

## 固定证据

证据目录`/tmp/dbms-review-r46.ifu6SWLp/`。

- `r179-reference294.log`：`9871aee8ebfb4c02b7f6f648cd03d5ddc22e4aff80f1e10740085151529099a4`，实际0。
- `r179-old-accepted294.log`：`e1c5b2fb9ffa592f42822372e8bf50499b714c1b8140a7343c1004a62a9787f3`，实际1/140断言差异。
- V1 syntax receipt：`/tmp/dbms-r179-syntax76.nx900vgv/completed-syntax-receipt.json`，
  `126679e4989a1b8d5cea591614520a9805f871bb68b9a84b81277c70c501afda`，四命令0，仅语法。
- 初始12条观察及6条目录/调用观察为`r179-string-search-*-discovery.log`、`r179-*-search-declarations.log`。
  产品的目录反射查询实际42P01，不把它写成目录反射成功或本项新增修复。

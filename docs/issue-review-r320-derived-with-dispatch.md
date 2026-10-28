# R320：derived table 的 WITH 查询未被执行分派接收

状态：用户最新指令 `skip it` 后暂缓（deferred_by_user）；候选未验收、未提交生产修复。

```sql
SELECT v FROM (
  WITH c AS (SELECT id,v FROM r320_base)
  SELECT id,v FROM c
) q ORDER BY id;
```

独立 PostgreSQL 18.6 与两个 V2 产品服务器各完整观察 14 请求。参考无
SQL 错误，两侧产品各六个 42P01，消息把 `"(with"` 当作关系名。普通
`(SELECT ...)` 来源和顶层 WITH 控制都无 SQL 错误；其行值、命令、Ready
与参考相符。Simple、Extended、二进制输出均复现；不含任何窗口函数，
因此不是只改 LAG/LEAD 来源绑定就能关闭的问题。

`processDerivedTables` 的 `findDerivedStart` 只查找 `(select` 和 `(values`，
没有接收实际语法中的 WITH 子查询，之后把残留 FROM 字符串当作表名。
修复应按真正的 derived query 语法角色接收 WITH 查询，并保留其局部
CTE/继承/遮蔽边界、列和 NULL；不能全局替换 WITH、吞掉 42P01 或假造
空结果。需保留已有 SELECT/VALUES、LATERAL、标量/谓词子查询区别。

R318 的冻结 747 还保留 12 个含窗口的相同 derived-WITH 失败。它们是
OPEN，不得分类为已暂缓 R217 Describe 或 R156 呈现，也不得从夹具删除。
应独立修复、验证并 commit；此文档没有修复/验收声明。

证据：`/tmp/dbms-r318-window-scope-fix.XhbfSKH9/` 中
`completed-r320-observations-receipt.json`、`*-r320-observation.log`；
本轮私有参考集群已停止，默认超时和全部输入未改。
总 273 清单未完成，不 push，不启用 Actions。

## 扩展冻结基线与实施候选（尚未验收）

新增独立 PG18.6 三侧各 121 请求及同一严格夹具复验：参考 121/0，
Root 和纯副本各 121/108 严格差异，原 14 请求及输入字节完全保留。
两侧各 92 个 SQLSTATE 差异，不能将其都算为独立 bug 或已暂缓项。
覆盖 trivia、quoted CTE/alias/column alias、空行、NULL、继承/遮蔽、
嵌套/同级派生来源、VALUES、LATERAL、标量/谓词对照及显式类型参数。
predicate-IN 对照的三处失败属于用户暂缓 R282，不恢复它的实现。

候选仅改 Main：按当前查询的顶层 FROM 来源角色扫描，完整跳过子表达式，
由真实解析树确认 SELECT/VALUES/WITH 子查询；普通与 LATERAL 分别接收。
别名读取保留 quoted spelling、解码身份与真实游标，列别名包括空白后的
列表；仍使用原结构化行/NULL/类型通道和 CTE 执行所有者，不把名字全局替换。
本段记录构建前状态，随后 V1 实测见下文；尚未验收，不能宣称修复完成；R318 的全部 300 条主矩阵、
96 条附加观察和完整 747 项断言仍须保留。所有公共头未改，Main 与六个
真实 Main 测试仍须新编译。原 273 总账与所有用户暂缓项保持不变。

冻结证据：`/tmp/dbms-r320-derived-with-grammar.5Lq1d4XZ/` 的
`completed-independent-121-fixture-receipt.json`、
`completed-strict-before-producer-receipt.json`；两次私有 PG 集群均停止。
实施目录：`/tmp/dbms-r320-derived-query-fix.IMMontLY/`。未 push，Actions 禁用。

## V1 实际构建与未通过的早期检查

两侧生产 Main 已按 O2 全新编译，120 个非 Main 生产对象和两个 stubs
按当前源码/公共头/编译器/flags/MMD 匹配复用，两台新服务器已链接。
公共头未改。只运行了以下早期检查，尚未运行六个真实 Main 回归、
完整旧 300 条主矩阵或 scoped UBSan，不能视为修复验收。

| 投影 | 完整请求 | 严格差异 | SQLSTATE 差异 | 原执行状态差异已修复 |
| --- | --- | --- | --- | --- |
| root | 121 | 106 | 9 | 83 |
| accepted | 121 | 106 | 9 | 83 |

两侧原 92 处执行状态差异各消除 83 处，全部请求均完成、无 socket
异常，原 SQL/期望/默认超时不变。剩余九处中，三处 predicate-IN 属于
用户暂缓 R282；WITH/VALUES 与递归查询各三处仍错误报告 42P01，必须
继续核实、修复，不能都归为暂缓。其余请求的 SQLSTATE、DataRow 字节、
命令标签和 Ready 与参考一致。描述信息有 104 处差异，参数描述有两处
差异，仍须逐项确定归属，不能自动计作已暂缓或已通过。

新增独立 PG18.6 的十个 FROM 别名/标量对照均成功；旧 R318 两侧均能
执行 `SELECT rows.v FROM(SELECT 1::integer AS v) rows`，V1 两侧却报
42P01，确认引入一条回归：误用通用 isKeyword 集合拒绝了合法的 rows
别名。同一对照还确认 AS 后紧邻 quoted 名称以及无显式别名均是合法
FROM 查询；V1 仍未接收，不能只按相同失败总数宣布正确。补充对照的
scalar comma、substring FROM 参数保持成功，作为角色边界正向控制保留。

纯 Parser API 的两侧诊断也显示：递归的 VALUES/UNION 子查询被普通和
binding 解析均拒绝；WITH/VALUES 普通解析可给出 SelectStmt，binding
解析却拒绝。此诊断不是实际 Main 接收链的完整证明，下一步仍须核对
sqlProcessor 之后真正到达的 SQL/查询角色，不能凭 API 表面状态改错误码。

V1 全部源码、对象、服务器、日志保留，未提交/未验收。原 121 项及独立
oracle 保持逐字节原样。新增十项证据目录为
`/tmp/dbms-r320-alias-role-controls.q5uMTmRG/`；私有 PG 已正常停止。
本目录 `completed-v1-early-diagnostic-receipt.json` 与
`completed-parser-role-diagnostic-receipt.json` 保存实际结果。所有更早
用户暂缓项保持不变；没有恢复 R282 或全局 R294 语法接纳，没有 push。


参数字段复查已确认，两处差异分别是 79/112 的 `SELECT v FROM c`：
CTE 本来已离开作用域，参考在 Parse 阶段直接报 42P01，产品却先发布
空参数描述等消息再报相同状态。这是已暂缓 R294 的既有错误阶段差异，
不是显式 int4 被推断为 UNKNOWN。两侧八个显式 int4 参数控制的参数
描述、完整行字节和状态均与参考一致；不以此复查恢复 R294 实现。

随后已对两侧真实 Main 做 O0 独立诊断，不链接 stubs、未调用改名的
CLI 入口：WITH/VALUES 在 sqlProcessor 后能找到 FROM 查询及别名，故
不能把后续执行失败继续归为简单 finder 漏接；递归 VALUES/UNION 则
在共享解析器报错后被查询角色判断拒绝。rows 案例的来源识别成功、别名
识别失败，与新回归一致。对应实际编译、链接和运行都退出 0，记录在
`completed-actual-Main-role-diagnostic-receipt.json`。这两个诊断程序不是
既有六个 Main 回归，也不是 sanitizer 或修复验收。


## V2 修正候选（尚未验收）

在改生产代码前已冻结 139 项独立参考并用同一严格夹具在全新 PG18.6
集群完成零失败复验，最初 121 项的请求和期望逐字节保留。新增合法
关键词/紧邻 AS 引号/匿名来源及内部名称碰撞对照；内部生成名必须不在
原查询中出现，不能让用户写入的未知 qualifier 意外引用临时物化表。

新原生解析器夹具固定 276 个控制，在旧两侧都完整执行、各 150 失败。
它检查 WITH/VALUES 的命令与 CTE 所有者、VALUES 集合查询、递归种子、
ColId 别名、匿名 AST 不泄漏生成名，以及原始表达式的字节范围。最终
夹具只加强了空指针防护，前一版源码/对象/150 失败结果也保留；两版
都是在生产修正前跑旧实现，未减少输入、断言或改变预期结构。

V2 修改 Main 和 parser.cpp，不改公共头：VALUES 复用现有集合查询
解析入口；WITH/VALUES 保留真实 Values 节点和 CTE；分类器跳过完整
CTE 包装而不是把同名 values 误当主体。FROM 标识符复用既有 PG 关键词
类别，Main 仅用真实来源别名的纯语法角色探针；AS 可紧邻引号，匿名
来源用输入中不存在的私有别名，列/NULL/CTE 继续归原结构化通道所有。
WITH/集合 VALUES 走保留 AST 的现有准备查询运行时，普通 VALUES 的
既有接收器不变；继承来源复用 R318 的查询局部元数据入口。

当前尚未重新编译或验收，不能把方案或参考通过算作产品修复。必须新
编译两侧 Main/Parser、六个真实 Main 回归，保留全部旧 300 条主输入、
747 项窗口协议和此前 96 条附加观察；其他公共头/对象只按实际匹配
证据复用。未恢复 R282/R294/R317 或其他用户暂缓项。新参考证据在
`/tmp/dbms-r320-reference135.y60kgUtp/`（实际计数 139），V2 实施目录为
`/tmp/dbms-r320-derived-query-v2.tFR0tUQo/`。未提交，未 push，Actions 禁用。


## 用户暂缓与 V2 留存状态

用户要求跳过后，停止本项实施和验收，不将它记为已修复。此前段落中的
OPEN/未暂缓是当时状态，以本节最新决定为准；完整源码、夹具和原始日志
均保留，不回滚或覆盖其他工作区改动。

V2 在暂缓前已完成：独立 PG18.6 冻结 139 项，原 121 项前缀逐字节保留；
真实 Parser API 原基线每侧 276 检查/150 失败，候选两侧各 276/0。
Main 与 Parser 的四个生产 TU、六个真实 Main 测试 TU 已重新编译，
94 个原生/前端程序已重新链接。两侧 139 项各有 104 个严格差异，
747 项各有 453 个严格差异，不能称为全绿。组件诊断中，139 项各三处
执行差异属于既有暂缓 R282；747 项执行行字节、状态、命令及 Ready
与参考一致。剩余 Describe/来源元数据/错误阶段差异未全部完成归属审计，
不能自动归为已暂缓，亦不作整项验收结论。

普通 212 条回归与 scoped UBSan 96 条任务在用户跳过时收到 TERM，
均以 143 结束，未产生完整运行验收收据；这不是测试通过或修复提交。
候选目录 `/tmp/dbms-r320-derived-query-v2.tFR0tUQo/` 保持原样。
暂缓快照、已完成收据和日志哈希保存于
`/tmp/dbms-r320-user-skip.FHeLvdB2/preserved-skip-evidence.json`。
本次只提交状态文档，不提交本项候选代码；后续普通 continue 不恢复 R320。
下一独立 OPEN 项为 R319（窗口结果的外层标量接收）。
总清单仍未完成；未 push，GitHub Actions 仍禁用。

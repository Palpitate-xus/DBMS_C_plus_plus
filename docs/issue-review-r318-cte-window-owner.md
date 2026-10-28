# R318：CTE 来源的窗口执行丢失关系所有者

状态：已完成独立 673 请求与 47 部件控制核实；生产未改，尚未修复，OPEN；未被用户暂缓。

R317 的完整 800 项保留了两种函数在 Simple/Extended/二进制输出下的六个
CTE 请求。PostgreSQL 18.6 成功返回六行，旧 Root/accepted-only 均返回
42P01。例如：

```sql
WITH w AS (SELECT id,p,v,n,d FROM r317_w)
SELECT lag(v,1,2147483648) OVER(PARTITION BY p ORDER BY id) AS v
FROM w ORDER BY id;
```

初次记录的源码线索：Main 的 `windowProtocolType` 为取得 LAG/LEAD 共同结果类型，
拼出独立 `SELECT <window> FROM <tnameOrig>` 并重新绑定。逻辑 CTE 名离开
当前 query 的来源/继承绑定后可能被当作实际 catalog 表；最初只视为源码推测。
后续独立运行和部件验证见下文；生产修复仍未实施。

修改方向：让窗口接收端使用当前实际 bound query 的输出和 sourceRange，
或在合法的继承/物化映射下取声明，不孤立重解析原逻辑 FROM 名；保留真实
CTE 遮蔽、作用域、类型和 NULL 行。不得用把名字改成物理字符串、忽略 42P01
或固定返回空结果来绕过。若需公共头/owner 结构修改，必须完整重建受影响 TU。
不恢复用户已暂缓的通用 CTE Describe/CASE label 或全局缓存生命周期改造。

证据：`/tmp/dbms-r317-window-offset-audit.vcBaloAt/`，完整 800 项的索引
773、778、783、788、793、798。原始输入、PG 帧和严格失败全部保留；
没有生产修改、验收或通过声明。须独立复现、修复、验证和 commit。
原 273 总清单未完成，不 push，不启用 Actions。

## 扩展独立核实

完整参考矩阵使用独立 PostgreSQL 18.6、Root 源码投影和纯提交源码投影，
各运行 673 请求；随后从参考端生成冻结 oracle，并由同一严格协议夹具
重新跑完整三侧。参考 673/0 失败，两个旧产品各 673/545 失败。检查包含
Simple、Extended、二进制输出的完整帧顺序、ParameterDescription、
RowDescription 原始字节、DataRow 字节、命令标签、SQLSTATE 和 Ready。
错误消息要求有效 ERROR 字段和 UTF-8，不要求不同实现的错误文字相同。

覆盖 LAG/LEAD、FIRST_VALUE、NTH_VALUE、SUM、COUNT、ROW_NUMBER、
NTILE；物理基表、普通 CTE、别名、列别名、依赖前一 CTE、遮蔽物理表的
同名 CTE、quoted 名称、derived 来源；正常行、WHERE false、LIMIT 0、
named window、多窗口输出，以及 CTE 不泄漏到下一条语句的控制。

545 个差异不能全部称作本项 bug，也不是 545 个独立 bug：

| 每侧断言数 | 实际差异 | 状态/归属 |
| --- | --- | --- |
| 420 | 参考成功，产品报 42P01；每种协议模式各 140 | 本项 CTE 窗口执行所有者，OPEN |
| 12 | 基表 LAG/LEAD 共同结果类型在 Describe 中由 BIGINT 错为 INTEGER | R317，用户已暂缓 |
| 54 | 同名 CTE 查询已有正确行，但两种 Describe 返回 NoData | R217 通用 CTE Describe，用户已暂缓 |
| 54 | derived 查询已有正确行，但两种 Describe 返回 NoData | R156 完整 presentation，用户已暂缓 |
| 3 | 物理列描述中的来源身份不同，值和类型等其余字段相同 | R226，用户已暂缓 |
| 2 | 作用域外 CTE 的 42P01 时机不同，产品多发布 Parse/参数描述 | R294 通用 Parse，用户已暂缓 |

完整 673 项全部保留，没有删掉这些 125 项、改 oracle、跳过断言或把它们
算作通过。R318 与 R319 不随 R317 的 skip 自动暂缓。

## 绑定链路的部件证据

新增 OUT-only 原生控制夹具在旧两侧各 47/0 失败。它不是修复后的通过证据，
而是区别当前全树绑定器与错误调用方式：

1. 完整 `WITH c AS(...) SELECT <window> FROM c` 对上述九种调用均能得到
   正确类型/OID，并保留指向真实 CTE statement 的 sourceRange 和六列描述。
2. 相同主体被孤立成 `SELECT <window> FROM c` 后，没有 CTE 声明，确实报
   42P01；不能从调用者的执行期 QueryCteFrame 自动获得 relation metadata。
3. 物理 `w.v` 为 TEXT、同名 CTE `w.v` 为 INTEGER 时，全树 LAG/LEAD 仍正确
   为 BIGINT；孤立主体却选择物理 `w`，报 42883。仅“同名表存在就不报错”
   不能证明窗口绑定正确，也不能按物理表存在与否猜作用域。

源码中不仅 `windowProtocolType` 会生成脱离所有者的 SELECT，窗口参数准备
及窗口标量排序准备也有同一方式；三处都直接调用 catalog-backed
`prepareBoundQuery`。已有 `resolveTableName` 消费执行期 CTE frame，但该
frame 不会自动进入独立 SQL 的纯 metadata relation callback。部件控制与
实际失败共同确认了这条所有权丢失链路；尚未记录逐调用点运行跟踪，也未
证明三个调用点在每一条失败中都会执行，不能把所有差异只归到第一处。

## 修复与验收计划

1. 让查询/CTE 执行边界保留实际 `PreparedQuery`、当前 statement 和来源描述；
   参数、输出 metadata、窗口键及外层排序消费这些实际绑定节点与 sourceOrdinal，
   不从逻辑关系名重新创建一个没有父 CTE 的 SELECT。
2. 保留真正的继承/遮蔽边界、quoted 名称、列别名、共同结果类型、NULL、空行
   及多窗口位置；不能按名字替换为物理字符串、吞掉 42P01 或伪造空结果。
3. 覆盖当前扩展矩阵，并补充窗口位于依赖前一 CTE 的子 CTE 内等继承 owner
   场景。当前 673 项含“来源依赖前一 CTE”，不等于已测试所有嵌套窗口 owner。
4. 若 Main 改动，必须重新编译真实 Main 和六个相关 Main 测试 TU；公共头
   若改动则按完整依赖/ABI 重建，不能用旧对象假称验证新实现。
5. 跑原已接受回归、窗口族和 scoped sanitizer；逐项比较失败身份，冻结
   oracle 不动。修复独立 commit，不夹带用户暂缓 R317 或其他工作区草稿。

## 证据与版本边界

新证据目录：`/tmp/dbms-r318-cte-window-owner.uiI6VnsY/`。
`completed-observation-comparison-final-receipt.json` 保存全部原始差异，
`completed-observation-component-classification-receipt-v2.json` 保存逐项归属，
`completed-independent-golden-frozen-receipt.json` 保存参考来源及 SHA，
`completed-observations-receipt-fixture.json` 与原始日志保存复验退出码。
`completed-old-native-receipt.json` 及日志保存两侧 47 项控制的实际执行。
首次比较在纯副本日志尚未结束时被完整性断言拒绝，未写比较结果；等待其
673 项确实结束后才生成最终比较，未更改输入或放宽完整性检查。
分类 V2 进一步把 108 个 NoData 分成 54 个 CTE 与 54 个 derived，分别明确
其既有暂缓范围；只改分类说明，没有改任何请求、期望、差异或退出码。

生产仍为 R316；本次仅更新审查证据和修复方法，没有生产/头/永久测试/注册
修改，OUT 夹具尚未接入。没有修复验收或 allgreen 声明。R317 用户暂缓候选
仍保留在其临时目录，未恢复。原 273 总清单状态未改，总目标仍未完成。

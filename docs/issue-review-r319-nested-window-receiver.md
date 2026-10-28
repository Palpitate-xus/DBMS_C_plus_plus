# R319：外层标量表达式没有接收真实窗口结果

状态：用户最新指令 `skip it` 后暂缓（deferred_by_user）；候选未验收、未提交生产修复。以下过程记录保留，阶段性 OPEN 不再表示允许继续实施。

R317 完整 800 项包括 Simple/Extended/二进制输出下的两种函数共六个请求：

```sql
SELECT COALESCE(
  lag(v,1,2147483648) OVER(PARTITION BY p ORDER BY id),7
) AS v FROM r317_w ORDER BY id;
```

PostgreSQL 18.6 正常返回六行且类型为 BIGINT；旧 Root/accepted-only 返回
42883。把 LAG 换为 LEAD 同样实际复现。独立窗口目标与多窗口目标的成功
不能替代这些失败，也不能据此将整个 QRY-08 标作完成。

源码线索：legacy 窗口提取及通用标量求值可能没有把表达式内部的真实
WindowCall 交给窗口 owner；尚未完成调用链定位，不能把该推测记作定案。
修改方向：在 query-owned 作用域中提取并绑定窗口节点，计算其真实 typed
结果槽，再让外层 COALESCE/标量投影消费该槽，保留逐行位置、共同类型、
NULL 和实际需求；不能把 LAG/LEAD 注册成假标量回调，或固定返回 NULL。

证据：`/tmp/dbms-r317-window-offset-audit.vcBaloAt/`，完整 800 项索引
775、780、785、790、795、800。原始请求、参考帧和失败断言全部保留。
没有生产修改、验收或通过声明；须独立修复、验证和 commit。
不恢复完整 presentation/ProjectSet/通用 raw analyzer 用户暂缓模块。
原 273 总清单未完成，不 push，不启用 Actions。


## 独立扩展复现与未验收候选

用户暂缓 R320 后，本项在不含 R320 实施代码的两份独立投影继续。
PG18.6 与旧 Root/accepted-only 各完整观察 139 个请求，保留原 800
集合中六项请求及期望帧原样；参考正常，两侧各 120 项严格差异，
其中各 110 处执行结果差异。参考私有集群已停止。

已确认 Main 的窗口提取只接收独立投影，嵌套调用走了普通标量路径。
候选按真实 AST 收集当前查询的窗口，保留子查询所有者边界，由原窗口
算子计算结果，再填入查询私有、带类型的位置槽，外层表达式读取这些槽。
没有把 LAG/LEAD 注册为标量函数，也不通过空值、错误码替换伪造结果。
最终输出、排序、DISTINCT 按实际需求消费标量目标，保留 NULL 通道。
目前只修改 Main；公共头、Parser 和声明推断模块均未改。

独立组件探针确认绑定器能够给两个嵌套表达式声明 BIGINT，并可通过
真实 execution slot 读取给定结果；探针每侧 10 检查中有两处失败，
是其要求原始标量求值返回 BIGINT 类型名，而 fallback 字面量实际返回
INTEGER（值为 7，输出声明仍 BIGINT）。原始失败日志保留；这不是
实际窗口生产器或真实 Main 的验收，不据此声称已修复或全绿。

证据和候选目录 `/tmp/dbms-r319-window-scalar.fkqobgt9/`。
139 项严格夹具与普通回归、scoped UBSan 尚须按当前代码完整验证。
R320 候选仅留在原工作区，不进入本项可执行投影；所有暂缓项不恢复。
本项生产修复尚未提交，原 273 总清单未完成；未 push，Actions 禁用。


## V1 失败保留与 V2 当前检查（仍未验收）

V1 两侧完整 139 项均为 31 处严格差异、六处执行差异，原每侧 110 处
执行差异消除 104 处。剩余六处是 Simple/Extended/二进制模式下的
`ORDER BY result` 与 `DISTINCT ... ORDER BY result`：旧排序适配器将
输出别名换成表达式文本，却没有保留该表达式真实输出位置，错误报 42703。
这些失败没有删除或放宽；V1 源码、对象、服务器和全部日志保持原样。
V1 两侧旧 747 项也已完整收集，各 457 处严格差异，12 处执行差异
仍是最新已暂缓的 R320。其余遗留元数据差异不等于通过。

V2 将原始 ORDER BY 的未限定输出引用交给已完整绑定的输出位置，再按
真实窗口结果槽排序；不改通用语法分析、窗口声明或参数推断。
两侧 Main 已真实重新编译和链接成功。当前 Root 完整 139 项的
SQLSTATE、DataRow/NULL 字节、命令及 Ready 已全部与参考一致；严格
断言仍有 25 处差异，须完成声明、物理来源与错误阶段的逐字段归属核实。
另一侧和旧 747 全部检查、六个真实 Main TU、完整普通回归及 scoped
UBSan 仍在进行，不能称为整项通过或提交修复。

旧 747 的 Describe 问题不能仅凭历史分类标签自动视为用户暂缓。
例如 R156 文档明确的是 REGTYPE、计划常量及相关内置类型呈现，
不能由此推定一般 derived Describe 也被暂缓。旧严格失败仍保持未完成，
后续须核实各自的真实范围；本项只验证窗口结果的外层执行交接。

原始 typed-slot 探针的两处类型名断言失败也未丢弃。`COALESCE` 返回的
fallback INTEGER 单元与绑定的 BIGINT 声明不同，是否影响真实 typed
流仍需独立核查；已准备 29 条无窗口、无 WITH 的参考对照，不在本项
擅自修改共同类型转换。若实际 typed 消费边界确认错误，修复应对真正
被需求的值执行绑定共同类型转换，不能只改 typeName 或求值未选分支。

V2 证据目录 `/tmp/dbms-r319-window-scalar-v2.E1dnwfqY/`；
独立类型诊断排队于 `/tmp/dbms-r321-coalesce-type-observation.Ly8jjvOy/`，
目前不是新缺陷的完成复现或修复声明。所有用户暂缓项继续保持暂缓。
本项代码未 commit，原总账未完成；不 push，不启用 Actions。


## 当前完整组件结果与独立 OPEN 问题 R321

两侧 139 项现均已完成：SQLSTATE、全部行/NULL 字节、命令与 Ready
均与参考一致；各 25 项严格残余按逐字段证明分为 16 项 R317 共同类型
Describe、3 项 R226 物理来源身份、2 项 R294 分析阶段和4项 R317
窗口签名发布时机，均保留严格失败。两侧旧 747 的全部原始消息体
（包括错误与 notice）与已接受 R318 相同；仅允许跨私有库 T 消息中的
table OID/attribute number 作归一化比较，没有重写任何日志或 oracle。
普通 210 条回归与 scoped UBSan 94 条仍须完整收集、核对，尚未验收。

独立 29 项无窗口/无 WITH 对照已经确认前节的类型疑点确实影响数据库：
PG18.6 全部成功，两个旧 R318 产品各 12 个 XX000，发生于三种协议
模式的 COALESCE、GREATEST、LEAST 的 UNION ALL，以及标量 set 子查询。
因此不是 R319 候选新引入，也不能把它当作已暂缓 R317 的窗口声明问题。

实际复现的查询之一（表定义与插入保持观察脚本原样）：

```sql
CREATE TABLE r321_t(id integer,b bigint,i integer);
INSERT INTO r321_t VALUES(1,NULL,7),(2,9,8);
SELECT COALESCE(b,i) AS v FROM r321_t
UNION ALL SELECT b AS v FROM r321_t ORDER BY 1;
```

当前错误为 `typed append child cell differs from its bound input type`。
同一数据上的显式 BIGINT CAST、CASE、NULLIF 及普通 COALESCE 投影对照
没有这些执行差异，所有原始请求/结果/完整消息都保留；私有参考集群已停止。

原因线索已由真实失败和源码相互印证：普通表达式的绑定共同类型是
BIGINT，但 COALESCE 返回被选中的 INTEGER 单元；GREATEST/LEAST 也有
同类结果载体问题。typed append 按真实声明核查输入，正确拒绝不一致。
修复方向是在真实标量求值边界按绑定共同类型转换被需求的值，保留 NULL、
collation 与 lazy 分支；不要移除 append 的声明检查、仅改 typeName，
或让未选中的 COALESCE 分支提前执行。这是下一独立 R321 修复，不在
本项混改 ExprEvaluator、常量规划或其他用户暂缓模块，尚未修复/提交。

证据：`/tmp/dbms-r321-coalesce-type-observation.Ly8jjvOy/` 下的
`completed-independent-common-type-observations-receipt.json` 与
`completed-independent-common-type-comparison-receipt.json`。
原 273 总清单仍未完成，所有用户暂缓决定保持不变；未 push，Actions 禁用。

## 用户暂缓与最终证据状态

本次 `skip it` 跳过当前 R319，不恢复此前已暂缓的 R320 或其他模块。
生产代码仍以 R318 `c3dabe24` 为已接受基线；R319 的 Main、构建注册与
两个新夹具仅保留为未验收工作区改动，不进入生产修复提交。

V2 的普通 210 条与 scoped UBSan 94 条现已实际完成，分别为
194/16、82/12（退出成功/非零），不是全绿或整项验收。旧失败的断言
身份、完整严格协议失败和原始日志均保留。scoped 运行通过独立故意
溢出 canary 验证 libubsan 的持久 log_path，再逐测试捕获报告；94 条
中没有观察到项目 UB 报告。不把 canary 当项目缺陷，不把较早的未启动
监督进程终止或未初始化 runtime 的 help 探针当成成功的 94 条运行。

扩大输入后候选仍有真实遗漏：

| 检查 | 实际结果 | 状态 |
| --- | --- | --- |
| 独立扩展 253 请求，两侧 V2 | 各 102 严格差异，其中各 69 执行差异 | 未修复；不是 69 个独立 bug |
| 分支需求 74 请求，PG 与新旧两侧 | V2 各 12 个序列计数差异，未选窗口仍求值 | 未修复 |
| 真实 Main AST 探针，两侧 | 各 72 检查、6 失败，遗漏量化比较左右操作数 | 未修复 |
| 合并 325 请求，独立 PG18.6 | 完整严格 325/0，私有集群已停止 | 仅参考验证，不是候选通过 |

74 请求中“相对旧版本新增”的逐点计数不能证明旧版本整条查询链正确：
旧版此前的值查询已失败，随后计数碰巧等于参考。V2 相对 PG 的未选
分支副作用错误仍已独立证实，原始失败和记录不改写。

V3 对 AST 所有者、排序和需求的源码候选已经整合，但尚未编译、运行
或验收；本次暂缓后不继续。原始 V1/V2、V3 源码与逆变换、对象、协议
输入/期望及全部失败均原地保留，不删除、不放宽断言。

证据目录：`/tmp/dbms-r319-window-scalar-v2.E1dnwfqY/`、
`/tmp/dbms-r319-additional-window-roles.r6p6B4ws/`、
`/tmp/dbms-r319-window-demand-controls.UVF7zk8R/`、
`/tmp/dbms-r319-window-owner-v3.Ql3zdmbA/`。
独立无窗口问题 R321 不属于本次暂缓，另项处理并独立提交。
原 273 总清单仍未完成；本次只提交暂缓文档，不 push，Actions 保持禁用。

# R170：quoted 未限定标量函数的表投影身份

状态：Open。已复现，候选尚未安装、编译或验收；不把草稿当作修复完成。

## 已复现的问题

实际接受基线为 `073e47367e18163d5aa1d1707ad1c6253b303614`。
在有真实行的表上，`SELECT "degrees"(x) FROM t` 等合法 quoted 标量调用
会被旧执行路径当成包含 SQL 双引号的原生函数名，再次引用后报 `42883`。
未限定标识符中的大小写、字面点号和双引号是身份的一部分，不能简单删引号或小写化。
空表、WHERE false、LIMIT 0 也必须进行必要的名称/签名分析，不能依靠没有到达行执行来隐藏错误。

完整发现夹具 V2 固定 791 个操作，在 PG18.6/180006 上全部通过；
冻结 accepted-only V4 产品完整执行 791 个操作、343 项断言失败，实际外层退出分别为 0/1。
343 包含重复输入、格式组合及错误状态的后继影响，不代表 343 个独立根因。
其中 Parse/Describe 的独立所有者记录在[R171](issue-review-r171-quoted-scalar-protocol-analysis.md)，
普通未引用 abs 参数引用错误另记[R172](issue-review-r172-qualified-column-scalar-argument.md)。

## 根因与修改方法

[Main](../src/main.cpp) 的完整 AST 入口目前只为真实限定 callee 扩展表投影；
旧文本入口将 SQL quoted callee 原片段拷入 `SelectExpr.funcName`。
原生 API 的该字段本来就是已解码身份，[TableManage](../src/commands/TableManage.cpp)
据此再引用一次是正确的，不能为修 SQL 入口而破坏原生 API。

有限候选仅将真实 SQL 源范围内的“一个 quoted identifier 后紧接左括号”
交给既有完整 AST 绑定/执行，保留原 FROM-less 入口以及 aggregate/window/SRF 的所有者。
字面点号和 doubled quotes 保持单个标识符；不全局 strip、lowercase、拆点号或重写 native callee。
需验证嵌套、别名、shadowing、缺失名称、空输入、保留绑定、binary 格式和 volatile 函数只执行一次。

## 固定证据与验证计划

V2 草稿：`/tmp/dbms-r170-quoted-scalar-v2.UQ1T1QsP`。
原始日志位于 `/tmp/dbms-review-r46.ifu6SWLp/`：

- `r170-v2-reference-full791-protocol.log`，791/0，实际退出 0。
- `r170-v2-old-accepted-clean-full791-protocol.log`，791/343，实际退出 1。

旧 binary SHA256 为 `2daeb9653b5844be53f08e6841e58615b1011bb4ed3a615470faf109f8e9e9ad`。
V3 只改两行 docstring，反向还原后与 V2 逐字节一致，保留全部 SQL、断言、791 数量和顺序。
V3 位于 `/tmp/dbms-r170-quoted-scalar-v3.dTmZKtbg`，
夹具 SHA256 为 `d3a9e71c179912534409700f9542901350a484b87022580fe9ddc1053fd8576f`；
V3 也已严格串行实际完成参考 791/0、旧 accepted-only 产品 791/343，外层退出分别为 0/1；
日志为同证据目录的 `r170-v3-reference-full791-protocol.log` 和
`r170-v3-old-accepted-clean-full791-protocol-retry1.log`，两次进程均已收取消费终态。
这是 V3 本身的完整运行，不以 V2 日志替代；仍不是新候选产品通过。
首次 V3 产品包装器误用不存在的 binary 路径，启动前失败、0 控制/退出 1；
原日志保留为环境错误，不计 SQL 失败，也不修改断言掩盖。

Main 修改后必须在 Root 和新 accepted-base-only clean 各新编译三个真实 Main 测试 TU；
旧 Main 对象不得作为本轮 fresh 证据。共享头未改时可逐一证明其他生产对象来源后复用。
完整 791 及原 58 相邻入口、两个既有原生参数契约检查计划保留，不以编译成功替代运行。
不恢复用户暂缓模块；原 273 项状态不变，不 push，Actions 保持禁用。

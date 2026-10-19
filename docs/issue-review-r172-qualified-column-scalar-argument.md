# R172：普通 abs 参数误读带点 quoted 表别名

状态：有限修复已验收，按独立本地提交纳入。最终五项组合两端各完整70入口通过，含本项完整314。

## 已复现的问题

在[R170](issue-review-r170-quoted-unqualified-scalar-projection.md)完整 V2 发现夹具中，
真实表的别名为 `"r.dot"`、integer 列为 `"n"`，值为 -2、3、NULL、-4。
`SELECT abs("r.dot"."n") ... ORDER BY "r.dot".id` 在 PG18.6 返回 2、3、NULL、4，INTEGER23。
同一输入在冻结 accepted-only V4 上，simple/extended 返回四个字面字符串 `r.dot"."n`，
连 NULL 行也错误变成非 NULL；binary 因这些字节不能编码为 INTEGER 而报 `0A000`。

这是完整 791 中三个到达真实行的失败操作，不是 float 格式差异，也不是 quoted callee；
callee abs 在这些 SQL 中本来就没有双引号。
原始证据为 `/tmp/dbms-review-r46.ifu6SWLp/r170-v2-old-accepted-clean-full791-protocol.log`，
相同完整参考日志 `r170-v2-reference-full791-protocol.log` 为 791/0。
旧 binary SHA256 为 `2daeb9653b5844be53f08e6841e58615b1011bb4ed3a615470faf109f8e9e9ad`。

## 根因范围与拟定方法

确切调用链已定位：Main 将 SQL 参数片段放入 `SelectExpr.funcArgs`；
TableManage 的 getVal 在任何列解析前将首尾双引号当作字符串，直接返回中间字节；
strict-NULL 检查也将它视为 literal，abs 的数值转换失败后原样返回，精确吻合四行错误值。
binary 0A000 是错误字节不能编码 INTEGER 的后果，不是独立格式根因。
应保留 AST 中的列身份或在既有 SQL 适配边界正确解析，而不是删除所有双引号、
按字面点号拆分、强制 CAST 输出，或改变原生已解码字符串参数契约。

有限 Main-only 候选只对单个普通表来源、单个 unquoted/unqualified abs 投影、一个直接 ColumnRef
参数保留完整 AST，要求真实 SQL source/token 证明，并显式排除 CTE/set/group/distinct/window/locking 等形状。
既有 shape probe 和 raw SQL 完整绑定仍是实际所有者；不修改 TableManage 或原生参数契约。
普通未引用列也在此有限入口内；不声称嵌套、多投影、derived/aggregate 或 whole-row abs 已完成。
R170 的新 quoted-callee gate 和 R171 的协议 metadata 所有者均不应被冒称已经覆盖本项。
共享完整 791 中这三条断言继续保留，不能删掉、改为预期字符串或放宽 NULL/binary 检查来通过。
后续需补普通别名、quoted 字面点号、嵌入双引号、NULL、文本字面量、native 参数契约及三种协议模式，
先验证完整参考，再冻结独立候选并逐项提交。

首次报告仅登记实际bug和修改方向；候选编译及运行边界以下文实际记录为准，尚未验收。
原 273 状态不变，不恢复用户暂缓模块，不 push，Actions 保持禁用。

## 补充 314 控制的实际对照

冻结夹具为 `tests/abs_column_identity_protocol_e2e_test.py`，
SHA256 `f2d6d7745b1a24b98a10d1f27a56210d2b3a34dbc69ad07681184e94476e18f6`。
它只复用共享 791 夹具的 Wire/identifier/MODES，使用自己的 Review 和固定 314 计数，
没有调用共享 ScalarReview 或改变原 791 的任何字节/断言。
覆盖 15 种列/alias/大小写/点号/双引号及精确 bigint 身份，三种协议的真实行和空输入，
错误后同连接恢复、字面量边界和实际 stored abs overload 的 search_path 所有权。

严格串行实际完成 PG18.6 全 314/0、旧 accepted-only V4 全 314/71，外层退出分别为 0/1，
两个进程终态均已收取；71 包含重复格式/空输入/恢复操作，不是独立 bug 数量。
邻界原有 abs literal/text 错误控制均保留，没有为了通过减少数量或放宽预期。
日志为 `/tmp/dbms-review-r46.ifu6SWLp/r172-v1-reference-full314-protocol.log`
及 `r172-v1-old-accepted-clean-full314-protocol.log`。
SHA256 分别为 `9b9562c81848e6888b3de6cccd9e6e774b846c553466ae5b4e3d8874f2c8d43c`
和 `aa2d7e92fafb3b96084f822c52db5fff41c6058e96e34ad70338029cbc9e7af2`。
上述71是旧产品反证。后续三项组合V1 clean完整314实际0，28native+3真实Main也实际0，
但完整791仍26项其他失败；不据此冒称完整62通过。最终五项组合V2两端编译/清单校验已0，
三个真实Main TU每端均重新编译，并非复用V1；Root完整314实际0，34native及3Main也均0。
Root首轮70因既有清理操作超时而退出1；clean完整70及Root同源/产物原样retry1最终均实际全0，
34native+33wire+3真实Main及两端源/58生产对象/server/96对象/38程序独立复核均0。
据此验收本项有限列身份入口，不扩大成复杂来源或原生参数API变更。
历史失败、最终日志SHA及联合而非中间提交独立全矩阵的边界，见
[R170最终证据](issue-review-r170-quoted-unqualified-scalar-projection.md)。

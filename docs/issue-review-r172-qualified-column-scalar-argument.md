# R172：普通 abs 参数误读带点 quoted 表别名

状态：Open。实际旧产品错误已确认，独立最小源代码修复仍在定位。

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

当前证据表明旧标量参数文本路径把 quoted 限定列引用当作字面字符串，
没有按实际 relation alias/column 身份取单元格；确切最小 owner/hunk 尚未验收。
应保留 AST 中的列身份或在既有 SQL 适配边界正确解析，而不是删除所有双引号、
按字面点号拆分、强制 CAST 输出，或改变原生已解码字符串参数契约。

R170 的新 quoted-callee gate 和 R171 的协议 metadata 所有者均不应被冒称已经覆盖本项。
共享完整 791 中这三条断言继续保留，不能删掉、改为预期字符串或放宽 NULL/binary 检查来通过。
后续需补普通别名、quoted 字面点号、嵌入双引号、NULL、文本字面量、native 参数契约及三种协议模式，
先验证完整参考，再冻结独立候选并逐项提交。

本报告登记实际 bug 和修改方向，不声称新产品修复、编译或运行完成。
原 273 状态不变，不恢复用户暂缓模块，不 push，Actions 保持禁用。

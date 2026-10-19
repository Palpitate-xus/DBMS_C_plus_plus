# R171：quoted 标量表查询的 Parse 与 Describe 所有权

状态：有限修复已验收，按独立本地提交纳入。V2纯分析入口随五项最终组合在两端各完整70入口通过。

## 影响及实际证据

[R170](issue-review-r170-quoted-unqualified-scalar-projection.md) V2 的 PG18.6 完整 791/0
和冻结 accepted-only V4 完整 791/343 原始日志中，18 个失败操作属于 PARSE/DESCRIBE：

| 实际问题 | 操作数 | 正确行为 |
| --- | --- | --- |
| quoted length(textColumn)、abs(intColumn) 返回 TEXT25/width-1 | 2 | INTEGER23/width4 |
| 缺失 quoted 大小写/字面点号/双引号名称仍 ParseComplete | 8 | Parse 阶段 42883，不发布 statement |
| 上述错误发布造成同名合法重试 42P05 | 8 | 失败 Parse 不占用名称，合法重试成功 |

最后八项是前面错误发布的后果，不是八个独立 duplicate-name 根因。
实际接受基线 `073e47367e18163d5aa1d1707ad1c6253b303614`，
旧 binary 和完整 791 发现证据与 R170 相同。没有缩减原控制或把缺失名称改成产品专用成功预期。

## 根因与修改方法

修复前[NetworkServer](../src/network/NetworkServer.cpp)的 Parse 签名检查只接受限定或已知 stored 名称，
表查询 fallback 直接返回，quoted builtin 或缺失 quoted 名称没有交给真实绑定器。
随后发布 named statement；Describe 的无参数表投影又落入旧文本类型推断，错误猜成 TEXT。
这些操作在 Main Execute 之前发生，[R170](issue-review-r170-quoted-unqualified-scalar-projection.md)
的 Main-only 改动不能修复早先帧或撤回已发布 statement。

有限方案：保留既有 role/shape guards，只在 Parse 和 quoted-only Describe 的真实 SQL token
证明一个未限定 quoted callee 时进入纯完整绑定；使用真实声明和列类型，不执行回调、函数体或扫描行。
R166 source-free 参数推断调用不提供此新 admission 参数，保持原入口，不扩张 wire 参数推断范围。
既有 aggregate/window/SRF/CTE/group/distinct/set/derived 所有者不变。

## 静态审查与后续验证

V1 离线候选 `/tmp/dbms-r171-quoted-protocol-v1.Isnfp0x7` 保留。
独立审查发现其 Describe 使用普通 `parser.parse`，不能提供新 gate 必需的 sourceBegin/end；
因此该分支可能永远不进入。这是静态否决，不是候选已经测试通过。
后续版本应仅为新的 quoted Describe admission 建立 `parseForBinding` provenance AST，
不全局改变 Describe 的既有解析和所有者；候选须另冻版本、审查、编译和运行。

完整保留共享 791 操作，严格检查 RowDescription、ParameterDescription、错误码、帧序列、
命名失败恢复、保留 portal、binary 和 metadata 不执行 volatile 函数的证据。
需同时验证原 R163/R166 和原 58 相邻入口；以下最终联合回归已经包含这些检查。
不恢复用户暂缓模块，原 273 不变；逐项本地提交，不 push，Actions 保持禁用。

最终采用独立的 V2 quoted Describe provenance AST；V1静态否决版本保留，
R166原两参数调用及既有role guards不变。后续联合五项候选已实际完成两端编译及产物校验，
Root共享完整791实际0；Root首轮70有一个既有清理操作超时而退出1，不能冒称整轮通过。
clean完整70已实际全0，Root原样完整70复测最终也实际全0；完整791及原956均0。
两端源/58生产对象/server/96对象/38程序独立复核0，有限协议入口据此验收。
详见[R170最终证据与历史失败](issue-review-r170-quoted-unqualified-scalar-projection.md)。
不将首次超时改写为通过，不声称全部1482入口或每个中间提交全矩阵通过。

# R301：复查 R160 的角度函数常量规划与协议阶段

状态：按用户最新“skip it”暂缓 R301/R160 角度函数常量规划模块，停止实现；
普通 continue 不恢复。保留在 R300 已接受生产版本 `11ec1776` 上的完整观察，
下文方法仅为未来明确恢复后的方案，不是本次继续实施的计划。没有本项生产
代码、头文件、永久测试或注册变更，也没有修复后验收；本次 commit 只记录暂缓。
临时目录已生成协议草稿及 PG 观察派生文件，但尚未跑成永久夹具或冻结验收。
不把采集成功、临时文件存在或本次跳过写成产品问题已修复。

## 实际问题与待实施方法

| 问题 | 已观察的使用影响 | 修改方向/状态 |
|---|---|---|
| 纯角度常量未进入既有规划路径 | 原十条零行查询漏报 PG 22003；空表、WHERE false、LIMIT 0 均涉及 | 复用真实 callee/不可变性及现有 float8 primitive 规划，不执行来源或用户 routine 来猜类型；待实现 |
| 标量子查询与外层零行边界 | `(SELECT degrees('1e308'::float8)) WHERE false` 等在 PG 规划报错，产品错误成功 | 按真实 query-owned 子图及需求规划，保留 lazy CASE/COALESCE；待实现 |
| 未使用的 SELECT CTE 被提前求值 | `WITH q AS(SELECT degrees('1e308'::float8) AS v) SELECT 42 AS v` 在 PG 返回 42，产品却 22003 | 调查实际 Simple 查询执行/规划 owner；不得执行未被需求的 SELECT CTE，也不漏掉必须执行的 DML CTE；待实现 |
| Bind 规划边界缺失 | 常量及 float8 text/binary 参数共 36 场景，PG Bind 为 22003、无 BindComplete；产品却成功 | 在真实 Bind 值准备好后、portal 发布/BindComplete 前做不可变 primitive 规划；不在 Parse 为类型执行函数；待实现 |
| 显式事务的错误状态必须同步 | 同 36 场景，PG Bind 后 ReadyForQuery E，产品仍 T | 复用既有扩展协议 abort 边界；保存点恢复后仍可重绑定并执行；待实现 |

源码显示实际角度回调已有 DeclaredFloat8 所有权；现有常量规划名单不含角度函数。
`volatility()` 默认 v，degrees/radians 没有明确 i 条目，后续应以永久控制核实并补齐。
Callback 数值计算与次正规数曾由 R158 修复，不把本项重新做成数值近似补丁。

## 已保存的完整观察

第一批每侧 249 条操作，保留 R158 原 20 条 SQL（含原四条编码控制）、另 34 条
Simple 边界查询及 Parse/Bind/Execute/重绑定操作；每侧原始采集均 exit 0。
外层脚本只在最后打印不同 JSON 行的 sql 键时 KeyError，已保存完整收据并停止
私有 PG；仅修正打印报告，没有重跑数据库或将此包装错误计作产品 bug。

PG 与两侧产品各有 52 处原始 rows/state/OID/kinds 差异，其中两处只是次正规
浮点文本的长短差异，已经独立转换为 IEEE binary64 位并证实相等，不是数值 bug。
其他差异包含原 10 条零行错误、2 条子查询、2 条未使用 CTE 与 36 个 Bind 场景；
重复场景不计作独立 bug 数。

第一批在事务外 Bind 后发 Sync，会结束隐式事务并销毁 portal；此时连健康 Bind
后的 Execute 也会在 PG 报 34000，不能误判为函数执行 bug。为验证真正的重绑定
和执行，第二批显式 BEGIN/SAVEPOINT/ROLLBACK TO 后保留 named portal：每侧 301
条操作均完整采集。PG 坏 Bind 36 个全为 22003/E，两侧产品却成功/T；恢复后的
24 个健康 Bind/Execute 都成功/T。两批均为采集，不是 249/301 项产品通过。

## 明确恢复后的修复与验收方案（当前不执行）

1. 在生产修改前固定完整原生/协议正负夹具并先跑 PG。保留原 20 条与新增完整
   观察，不删除零行、CTE、阶段或失败恢复来凑绿；浮点值按真正 IEEE 位比较。
2. 验证 genuine builtin 的 i、实际 callee 身份、解析和纯描述不求值；保留 opaque
   原生替换自己的 i/v、arity/域及真实 stored routine 的调用所有权。
3. 只对既有 float8 primitive 的真实不可变内建进入规划；保持负零、NaN/Infinity、
   非零次正规数及合法有限值，不扩大 R271 数值域或 R205 REAL widening。
4. 在实际 Bind 值完成转换后复用真实 planning API，再发布 portal；严格核对
   Parse 成功、坏 Bind 无 BindComplete、事务 E、保存点恢复、健康重绑定/执行。
5. 检查 Simple 子查询/CTE 的实际需求边界，不把真实结果取作规划类型信息。
   R217 泛化 CTE Describe/标签模块保持暂缓，不借此恢复其实现。
6. 保留完整 R158 603/1449、R300 1063/27/5244、R299 4178/2133 等相邻入口，
   匹配当前源码/头/工具链构建并独立 pure 验证；若 Main 变更，六个真实 Main
   TU 必须 fresh。当前尚未编写本项永久夹具或生产候选。

不恢复 R156 完整类型化常量、REGTYPE/呈现/JSON 身份，亦不恢复 R298、R271、
R294、R291、R288/R99、R222、R51、R267、R217 或其他明确暂缓范围。
原 273 条总账仍 22 complete / 166 partial / 70 unverified / 15 deferred，未整体完成。
保留用户工作区及十八处暂存差异，只做独立本地 commit，不 push，Actions 禁用。

临时证据目录 `/tmp/dbms-r301-angle-constant-planning.XydEz0sv` 不保证长期保存。

| 已完成收据 | SHA-256 |
|---|---|
| completed-preflight-receipt.json | `b3c64f917ceba8d7ba9cef785f3dc6d7f768e91be95947cfe36fbcaf402aa2b7` |
| completed-observations-receipt.json | `203c16d886b4414583770d4e000a1b60e7f04520090ba9e0014e3a48fb1aad70` |
| completed-observation-report-recovery-receipt.json | `9365096f5f748ddc977bce8b20b20c9200b3af6dde6c7fe6e72a4384ba3527c1` |
| completed-transaction-phase-observations-receipt.json | `9a6e088988d78636a961ad2b67b8a29bb52ff4e4bb08dbcfea37f2c64f50427e` |

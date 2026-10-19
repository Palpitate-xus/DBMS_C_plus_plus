# R174：普通 derived/CTE/aggregate 的静态协议描述符

状态：Open。独立描述符缺口已从完整参考/旧产品帧确认；修复方案尚在离线审查，未验收。

## 已复现的问题

[R170](issue-review-r170-quoted-unqualified-scalar-projection.md)的完整 791 操作
在 PG18.6 全0，冻结 accepted-only V4 有343项失败；下列四种形态的 extended/binary
各两个操作确认独立 metadata 缺口，不应归入343个独立 bug，也不能假定原三项候选全部解决。

| 形态 | 旧协议行为 | 参考行为 |
| --- | --- | --- |
| derived table 上的 quoted abs(d.n) | statement/portal Describe 都是 NoData | INTEGER23/width4 |
| 从 derived 的 quoted length 常量输出读取 d.v | statement/portal Describe 都是 NoData | INTEGER23/width4 |
| 本地非递归 SELECT CTE 上的 quoted abs(c.n) | Describe NoData，后续执行另有旧 quoted-callee 错误 | INTEGER23/width4 |
| sum(quoted abs(integerColumn)) | Describe TEXT25/width-1 | BIGINT20/width8 |

派生 length 例的旧执行结果其实正确，仍缺描述符；不能把 DataRow 正确当作协议完整。
SUM 的简单协议旧结果也有 TEXT 类型差异，其执行路径修复应与 Network 的早期描述符分开记录。
Main 执行发生在 Parse/Describe 之后，不能追补已经发送的 NoData/TEXT 描述。

## 根因与修改方法

[NetworkServer](../src/network/NetworkServer.cpp)的 Describe S/P 进入 describePreparedResult，
不调用 Main。既有 query-host 描述分支拒绝 FROM；legacy fallback 又拒绝任意 CTE、
derived FROM，聚合文本推断不能从 quoted 标量子调用获得真实输入类型。
既有纯完整绑定器已经可以提供这些输入和聚合结果声明，不需要执行函数或扫描行。

独立有限方案：在既有 Parse 发布前纯验证和 Describe fallback 前，为受支持的
单目标、普通 single TABLE/derived、本地非递归 SELECT CTE 的 computed 输出取得静态绑定描述。
不移除 R163/R171 的 role/shape guards，不扩大 R166 unknown-$n 推断，不全局取消 query-host FROM 限制。
普通无修饰聚合应保留自身角色；join/SRF/window/set/group/having/distinct/locking 等另外处理。
不因 computed 输出可描述就顺手给 derived 直接物理列伪造 tableOid/attribute origin；
typmod、domain/base-wire OID 和真实投影身份也不能丢弃或靠执行值猜测。

## 验证边界

旧证据完整保留于 `/tmp/dbms-review-r46.ifu6SWLp/` 的
`r170-v3-reference-full791-protocol.log` 与
`r170-v3-old-accepted-clean-full791-protocol-retry1.log`。
四形态仍在完整 791 的原始断言中，不能删除、将 NoData 改为允许或放宽 OID/width 来通过。
当前三候选的 V1 两端生产程序已完成编译/链接，真实 Main 回归 TU 仍在编译，
V1 clean 的完整 791 专项还在运行；这些不是整轮通过或本项修复声明。
后续新版本只能在保留旧版输入、产物和实际失败后单独构建验证。

本项预计仅 Network CPP，不改共享头；若 Main 字节/驱动/头/flags/编译器一致，
可逐一证明后复用本轮真实 Main TU，但不得称为下一轮 fresh Main 编译。
完整 791/314、原相邻入口和 metadata 不执行 volatile 函数的检查均须保留。
原 273 项状态不变；总目标未完成，不恢复用户暂缓模块，不 push，Actions 保持禁用。

# R174：普通 derived/CTE/aggregate 的静态协议描述符

状态：有限修复已验收，按独立本地提交纳入。V5随五项最终组合两端各完整70入口通过，含本项完整331。

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
最初三候选V1两端编译完成，V1 clean完整791实际26项失败，314及31个native/Main入口实际0；
这些不是完整62通过或本项修复声明。
后续新版本只能在保留旧版输入、产物和实际失败后单独构建验证。

最终本项仅Network CPP，不改共享头；联合R175修改Main，因此V2两端各三个真实Main TU已重新编译，
不使用早期Network-only设想冒称Main复用。两端编译实际0，完整70入口已串行完成。
完整 791/314、原相邻入口和 metadata 不执行 volatile 函数的检查均须保留。
原 273 项状态不变；总目标未完成，不恢复用户暂缓模块，不 push，Actions 保持禁用。

## 新增完整331与版本边界

独立夹具 `tests/computed_query_host_metadata_protocol_e2e_test.py` 的完整331操作，
实际PG18.6参考全0、未含本项修复的V1 clean为150项失败，外层退出分别0/1。
原791/314及新331的全部断言、计数、SQL和顺序保留；150含重复描述与失败Parse后继操作，
不是150个独立bug。旧物理来源、参数及聚合修饰角色控制没有失败。
另有一条Simple Query的derived computed VARCHAR(3)输出值/1043 OID正确、typmod却由7丢成-1；
不能只修Describe后忽略Simple Query的同一声明。

V1被静态否决：调用内ORDER是第二个synthetic参数，不是普通orderBy字段；V2补args.size()==1。
V3将静态声明保留至成功执行结果，但同行审查拒绝了从expanded SQL判断原始参数的扩大；
V4显式传递portal.parameterOids.empty()，Simple Query默认允许，参数portal保留原owner。
V5只补执行后的moneyLocale上下文刷新，声明不重查catalog；这不是新Money格式功能验收。
最终12个Network owned hunk：有限纯绑定、Parse/Describe、执行前声明及原参数owner传递，
computed origin/typmod保留；derived直接物理列、UDT/domain、复杂角色仍不猜测或全局接管。
新331参考/旧产品日志为证据目录中的`r174-v2-reference-full331-protocol.log`及
`r174-v2-old-v1-clean-full331-protocol.log`；V5联合候选的Root完整331实际0，
clean完整331及完整70也实际全0；但Root首轮70因既有R164清理超时实际退出1，
Root同源/产物原样完整70复测最终实际全0，完整331与原791/314/906/956均0。
两端源/58生产对象/server/96对象/38程序独立复核0，据此验收有限静态描述符入口。
详见[R170最终证据与首次超时边界](issue-review-r170-quoted-unqualified-scalar-projection.md)。
不声称全部1482入口或每个中间提交完整矩阵通过，不接管已排除的类型/查询角色。

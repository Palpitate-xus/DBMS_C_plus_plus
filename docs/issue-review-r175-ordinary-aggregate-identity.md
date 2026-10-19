# R175：普通聚合的执行入口与列参数身份

状态：Open。实际完整协议帧已确认问题；离线候选未安装、未编译、未验收。

## 已复现的问题

[R170 完整检查](issue-review-r170-quoted-unqualified-scalar-projection.md)在
PostgreSQL 18.6 的791个操作全部通过。冻结旧 accepted-only V4 有343项失败；
R170/R171/R172 组合 V1 clean 仍有26项失败，其中18项属于本项，另外8项为
[R174 描述符](issue-review-r174-ordinary-source-descriptor.md)。这些是重复模式的观察数，
不是26或18个独立根因。两个产品版本的18项均保留在原始791中。

| SQL 形态 | 旧版及 V1 的行为 | PostgreSQL 行为 |
| --- | --- | --- |
| 无 FROM 的 `sum(1)`、`count(1)`，各三种协议模式 | 42883；extended 静态 BIGINT 描述正确也不代表执行成功 | 单行 BIGINT 1 |
| `sum(quotedQualifiedColumn)`，三种模式 | 将列拼写当作 native key，实际值0 | 原检查数据之和-3 |
| 上述 SUM 加 DISTINCT / FILTER / 调用内 ORDER，各三种模式 | 实际值0 | 对应-3或-1 |

## 根因与修改方法

以不可变 V1 clean 的源码为输入：

1. `src/main.cpp` 的 source-free SELECT 进入标量表达式执行器，SUM/COUNT 没有进入
   已有的完整查询聚合算子。应有限准入单目标普通聚合，保留后续 shape、真实绑定和
   aggregate/scalar role 判断，不用函数名字符串取代真实重载选择。
2. 直接 ColumnRef 没有保留 typed aggregate target，legacy AggItem.arg 继而携带原始
   SQL 引号和限定名。`src/commands/TableManage.cpp` 的 native 聚合按 decoded 物理列名
   比较，找不到输入后跳过所有值。普通无调用内 ORDER 的有限形态应交给完整 AST 算子；
   仍由 legacy 消费的非 DISTINCT 调用内 ORDER，才在 SQL 适配边界以已证实的 ColumnRef
   降低到真实列 key，不全局去引号、删前缀或更改 native API。
3. AggItem 没有通用 DISTINCT 字段，不能只删去 `DISTINCT` 后求普通 SUM。
   完整查询聚合算子已有 FILTER、NULL 跳过、DISTINCT 及空输入规则，应保留这些所有权。
   真实 stored scalar `sum`、显式 `pg_catalog.sum` 与 search_path 的选择也必须保留。

候选仅两个 Main CPP hunk，不修改共享头、wire unknown-$n 推断、native 参数契约或
R163/R171 Parse guards。新 Main 字节意味着后续每端三个真实 Main 回归 TU 必须重新编译；
不能把 V1 的 Main 对象复用冒充本轮 fresh 编译。

## 验证计划与已知边界

原始791及314夹具字节不变，不删除失败断言。新增独立协议夹具
`tests/ordinary_aggregate_identity_protocol_e2e_test.py` 的离线静态清单为906操作：
8种列身份、普通/重复值 DISTINCT/FILTER/COUNT、三种模式、空表/全NULL/WHERE false/
LIMIT0、重复 Bind、负例后恢复、真实 stored scalar shadowing 及显式内建选择。
新数据故意重复，SUM=-5、DISTINCT SUM=-3，防止原数据无重复掩盖 DISTINCT 错误。
这906尚未运行，必须先获得完整 PostgreSQL 参考，再运行旧产品和最终候选。
native 合约仍需既有原生驱动验证，不能由 wire SQL 代替。

R176 普通调用内 ORDER 的真实排序语义另留开放：parser 用第二个 synthetic 参数承载
ORDER BY；当前 typed aggregate reducer 没有独立排序键 owner。整数 SUM 的正确和只能证明
输入身份，不能证明排序键求值、浮点累加顺序、NULL位置、稳定性、collation 或副作用需求。
本项保留原始 ORDER/FILTER 消费路径，不把这些整数控制冒充完整排序实现或验收。

## 证据及版本管理

证据在 `/tmp/dbms-review-r46.ifu6SWLp/`：

- `r170-v3-reference-full791-protocol.log`：791/0，实际退出0。
- `r170-v3-old-accepted-clean-full791-protocol-retry1.log`：791/343，实际退出1。
- `r170-r172-v1-clean-preliminary-full791-protocol.log`：791/26，实际退出1。
- `r170-r172-v1-clean-preliminary-full314-protocol.log`：314/0，实际退出0。

V1 两端生产及三个真实 Main TU 均已编译完成；完整62入口并未全部运行。
已建立独立、逐文件校验的 V1 Root 恢复副本，后续不得改写 V1 clean 或旧产物。
本次只提交问题和方法，不把未安装的候选算作修复；后续通过后逐项本地 commit。
原273项状态不变，总目标未完成；用户跳过模块不恢复，不 push，Actions 保持禁用。

# R175：普通聚合的执行入口与列参数身份

## 后续状态：R176有限源码已提交，完整矩阵未通过

[R176](issue-review-r176-ordinary-aggregate-order-keys.md)已另以`a35658c0`提交全局SUM/COUNT
真实排序键的有限修复，两侧native61/协议190通过，工作区完整72通过；干净版两次完整72
均因R164超时而失败，各其余71项通过，原因未定，不宣称完整矩阵验收。
GROUP BY等边界仍开放；本页以下R176未实现的表述及旧70项证据描述R170/R175验收当时，
不代表后续修复不存在，也不扩大本页原修复范围。

## 历史验收记录

状态：有限修复已验收，按独立本地提交纳入。最终V2随五项组合两端各完整70入口通过，真实ORDER排序仍属开放R176。

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

初版仅两个Main CPP hunk，同行审查后最终为五个，不修改共享头、wire unknown-$n 推断、native 参数契约或
R163/R171 Parse guards。新 Main 字节意味着后续每端三个真实 Main 回归 TU 必须重新编译；
不能把 V1 的 Main 对象复用冒充本轮 fresh 编译。

## 验证计划与已知边界

原始791及314夹具字节不变，不删除失败断言。新增独立协议夹具
`tests/ordinary_aggregate_identity_protocol_e2e_test.py` 的离线静态清单为906操作：
8种列身份、普通/重复值 DISTINCT/FILTER/COUNT、三种模式、空表/全NULL/WHERE false/
LIMIT0、重复 Bind、负例后恢复、真实 stored scalar shadowing 及显式内建选择。
新数据故意重复，SUM=-5、DISTINCT SUM=-3，防止原数据无重复掩盖 DISTINCT 错误。
这906已实际完成PG18.6全0、未包含R175修复的V1 clean为441项失败，外层分别0/1；
完整906字节未改，最终候选两端完整运行均为906/0。
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
首次报告提交仅包含问题和方法；不把候选安装或编译算作修复验收，完整通过后再逐项本地 commit。
原273项状态不变，总目标未完成；用户跳过模块不恢复，不 push，Actions 保持禁用。

## 最终V2有限候选及当前验收范围

新增whole-query入口仅SUM/COUNT，其他六种聚合仍由原Main owner消费，不能从906推定它们完成。
legacy调用内ORDER只在实际single TABLE/single projection、无window/set/group等角色、
真实可见物理整数ColumnRef及synthetic ORDER参数/源token证据成立时，降低SQL列身份。
FILTER进一步限定为可见整数列在左、单个整数literal在右的简单比较，通过已有source scope
降低到物理列名，不全局改native条件解释器，不猜TEXT/collation/array/domain比较。
同一flag下SUM的完整decoded物理key先精确取类型，避免`n.dot`被再次拆成`dot`而丢声明。

这些补充有真实旧906观察：ORDER+FILTER24项失败；另一个quoted dotted列LIMIT0的Simple Query
错误声明TEXT25，应为BIGINT20。它们是未修V1产品结果，不假装两个离线R175源码版本已经运行。
旧906日志`r175-v1-old-v1-clean-full906-protocol.log`、参考日志
`r175-v1-reference-full906-protocol.log`均完整保留。真实排序键求值、浮点顺序等R176仍Open。

最终五项联合V2两端编译实际0，Root48/clean49命令全部0；每端fresh2生产/核对复用56、
fresh6/复用28原生TU、复用stub、真正fresh3Main TU，96对象/38程序及完整源/产物清单复核0。
定向回归每端70入口=34native+33wire+3Main；Root完整906/791/314/331、34native与3Main实际全0。
Root首轮70仍因既有R164 finally清理socket超时而实际退出1，不能写成整轮通过；
干净版完整70已实际全0，完整906/791/314/331及源/产物独立复核均0；
Root保持同一源/产物原样完整70复测最终也实际全0，原956亦完整0。
两端源/58生产对象/server/96对象/38程序独立复核0，据此验收五项最终组合的有限修复，分别本地提交。
首次超时原因仍未确定，失败证据与最终日志SHA见
[R170最终证据](issue-review-r170-quoted-unqualified-scalar-projection.md)。
这不是完整注册1482入口运行或每个中间提交独立全矩阵；R176排序键求值/排序并未因此完成。

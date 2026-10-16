# R106：COALESCE未选显式CAST的常量输入分析遗漏

状态：用户于2026-10-16明确要求“skip it”，本项暂缓，未修复。
没有R106生产代码修改或已注册回归；测试草稿保留，不提交为已验收修复。
R104已独立提交为`2556a3d8`且提交后核对0；其649/738及41入口证明不扩为
本项验收。普通继续不重开本项，也不把暂缓扩大到全部类型或表达式工作。

## 实际观察

实际PostgreSQL 18.6（核实180006）与固定当前d697程序各完整采集以下四条。
三处差异；采集程序退出0只表示记录成功，不表示行为正确。

| SQL | 实际参考 | 当前旧程序 |
| --- | --- | --- |
| `SELECT COALESCE('192.0.2.129'::inet,'invalid'::inet)` | 22P02，无结果/CommandComplete | 成功返回192.0.2.129，OID869 |
| `SELECT COALESCE('192.0.2.0/24'::cidr,'invalid'::cidr)` | 22P02，无结果/CommandComplete | 成功返回192.0.2.0/24，OID650 |
| `SELECT COALESCE('08:00:2b:01:02:03'::macaddr,'invalid'::macaddr)` | 22P02，无结果/CommandComplete | 成功返回08:00:2b:01:02:03，OID829 |
| `SELECT COALESCE('192.0.2.129'::inet,'invalid'::text::inet)` | 成功返回192.0.2.129，OID869 | 相同；必须保留的typed运行期转换惰性阳性 |

R104补了直接UNKNOWN参数的共同类型/输入检查，但旧标量快捷路径仍忽略未选
CAST内部的UNKNOWN常量。不可为修复前三条而执行第四条的typed TEXT转换，
也不能提前执行函数、参数、行值或查询来发现类型。

## 暂缓前的测试草稿

未注册的`tests/cast_input_analysis_protocol_draft.py`完整执行612控制：
实际180006为0失败，固定d697旧程序为168失败。日志分别为证据目录中的
`r106-initial-reference.log`与`r106-initial-old-protocol.log`。两份日志均有
完整控制数；旧程序的断言失败不删除、不放宽。草稿没有进入已接受的1399
注册入口，也不计为产品修复或全量通过。暂缓后原`*_e2e_test.py`草稿仅改名
为`*_draft.py`，内容未改，避免未注册草稿破坏正式测试入口完整性检查。

## 将来获准恢复时的修改方法与验收计划

1. 检查旧标量路径的真实AST静态类型分析，区分CastExpr、语法`::`节点、
   UNKNOWN字符串常量与已typed表达式。复用真正内置目标的身份/输入校验，
   保持限定名、引号大小写及不存在转换的错误；不任意剥离schema。
2. 只校验真实UNKNOWN字符串常量到合法内置目标的输入，不执行完整CAST树，
   不以返回文本猜类型，不修改provider生命周期，不恢复此前明确暂缓模块。
3. 先加入能在固定旧程序失败的永久回归，实际参考验证四种边界及分析阶段。
   保留typed TEXT阳性、函数/参数惰性、原649/738/366及输入SQLSTATE控制。
4. 按实际改动重编并核对当前cache、全部58receipt及测试对象，运行相邻入口，
   保存所有失败和真实终态后才独立本地commit。不借R104或旧全量结果。

固定旧程序在`/tmp/dbms-review-r46.ifu6SWLp/r106-pre-fix-dbms-main`，SHA256：
`d6970b8546dded1cbdac5dedfcfbd3a7dc8e2d8514e1aeba0d607a8400ae5519`。
证据目录同上：`r106-coalesce-explicit-input-probe.py`、
`r106-coalesce-explicit-input-reference.log`及
`r106-coalesce-explicit-input-old-d697.log`。它及后续Engine工作区含未提交R98，
不能冒称仅含HEAD的完整验收。R98/R99候选保持且不提交，明确暂缓范围不恢复；
原273状态不提升，Actions保持禁用，不push。

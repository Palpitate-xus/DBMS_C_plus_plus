# R244：Bind常量规划与错误阶段（开放，尚未修复）

本项来自[R242后续问题](issue-review-r242-interval-extract.md)，未随R243被用户暂缓。
当前只有复现、参考数据和实施计划；没有生产修复，也不能关闭原273总清单。

## 实际分阶段检测

在真实PG18.6与提交`7922f38b`的独立纯投影服务上，各运行完整15组控制。每组分别
发送Parse+statement Describe、Bind+portal Describe、Execute，各自Sync收集响应，
同时保留SQLSTATE、消息顺序、OID/size/typmod、ReadyForQuery状态、行与command tag。
两侧观察器运行退出0，只表示采集完整，不表示兼容通过；逐字段对照有9组差异。

| 控制 | 数量 | PG18.6 | 当前提交 |
| --- | --- | --- | --- |
| `$1::int/0`、显式integer CAST后除零 | 2 | Bind报22012，无BindComplete/RowDescription | Bind成功并发描述，Execute才报22012 |
| abs最小整数溢出、sqrt负数 | 2 | Bind分别报22003/2201F | Execute才报相同SQLSTATE |
| date_part常量/真实text+interval参数、EXTRACT非法单位 | 3 | Bind报22023 | Execute才报22023 |
| 标量子查询除零，外层LIMIT 0 | 1 | Bind报22012 | Execute才报22012 |
| 派生VALUES源中常量除零，WHERE false | 1 | statement Describe有integer列，Bind报22012 | Describe返回NoData，Bind成功，Execute直接SELECT 0且没有错误 |
| 普通原始除零及WHERE false版本 | 2 | Bind报22012 | 原本正确，保持通过 |
| 恒false CASE死分支、upper成功、volatile writer及被WHERE false抑制的writer | 4 | 描述/绑定无副作用，按实际需求执行 | 本轮控制与PG一致 |

9是差异控制数，不是9个独立缺陷。派生源控制还暴露描述入口和规划需求问题，不能
只把它归为报错晚一步。普通除零已经在Bind报错，不能称为新修复对象。

每组在私有事务中创建临时sequence和volatile SQL writer，再ROLLBACK。statement
Describe之后、成功Bind之后用nextval检查计数并复位；两侧已观察到的这些检查均返回
预期1，writer正常执行返回2，被WHERE false抑制时不调用。不能从有限对照推出所有
routine/CTE都无准备期副作用。没有检查或恢复R243签名/NULL语法、R156显示差异。

## 可复用证据

手工观察器：`tests/compat/bind_planning_phase_observer.py`。
参考记录：`tests/compat/bind_planning_phase_pg18_expected.json`（真实18.6的15组原始字段）。

```sh
# PGREF_*按现有pg_diff_runner约定提供；不在仓库中存储密码。
python3 tests/compat/bind_planning_phase_observer.py --reference18
# DBMS_MAIN须指向实际要检查的独立构建，不应误用旧工作区二进制。
DBMS_MAIN=/path/to/tested/dbms_main python3 tests/compat/bind_planning_phase_observer.py
```

它是手工诊断工具，不是已通过的E2E gate，不加入注册套件掩盖9组差异。参考消息中的
诊断文字仅供排查，兼容比较使用SQLSTATE及其他结构字段，不依赖错误文案。
便携版已再次在同一PG与纯提交服务上完整运行，两侧各15组与最初采集完全相同。

最初观察器有两处工具错误：utility无RowDescription时调用了解码器；字段名bytes
不能直接JSON序列化。修正采集/序列化后完整重跑，未改变SQL、15组控制或阶段顺序。
V1/V2失败日志保留，不把它们算作产品故障。参考服务及产品均为私有测试实例。

## 源码定位与实施计划

当前NetworkServer的Bind只在整棵AST属于`isDatabaseIndependentQuery`或既有封闭
bit BETWEEN路径时直接求值；函数、CAST、子查询、实际来源不在该边界中。Portal
仅保留SQL/参数OID/格式和物化结果，不拥有已规划的表达式图。

已有`PreparedQueryExecution::planStatementConstants`会在执行私有表达式副本上
做纯规划，并保留原AST所有权；但正向内置折叠当前仅有有限已证明callback身份。
不能简单把所有函数标为immutable，也不能以同名/volatility字符猜测native替换或
存储例程的所有权。`constantExpression`的载体边界及R156/R240草稿仍须保持区分。

| 步骤 | 必须实现/验证的结果 | 禁止替代方式 |
| --- | --- | --- |
| 1. 分离分析、纯规划与运行实例 | Portal拥有真正bound AST、类型/输入声明、规划副本和来源身份；Describe仍只发布元数据 | Describe执行表达式或读取样本行 |
| 2. 证明可折叠操作的实际身份 | 根据真实callback、类型、严格性和稳定性资格处理规划；动态值/未知所有者保留正确需求 | SQL字符串特判date_part、函数名字白名单冒充身份、运行writer来发现常量 |
| 3. Bind接入既有纯规划 | 在BindComplete前完成所需规划，错误沿现有事务/Sync边界发布，保留原SQLSTATE | 先BindComplete再在Execute补同一个错误 |
| 4. Execute消费保留的规划产物 | 用原绑定身份与现有prepared provider传递实际计划；每次执行有新运行上下文，稳定时钟/SQL值不能冻结在旧规划器创建时间 | Bind求值探针后丢弃结果，Execute重新解析/重复求值；仅缓存结果字符串 |
| 5. Portal与catalog生命周期 | Close/失败/回滚释放计划资源，重复Bind/Execute不串输入；实际测量Bind后DDL/GUC变化、重规划及描述一致性 | 全局SQL文本缓存、恢复用户跳过的R51全局锁路线 |
| 6. 完整验证并独立提交实现 | 保留15组和9组失败期望，增加text/binary参数、无用/可达子树、LIMIT/CTE需求、native替换、volatile/STABLE单次求值及portal生命周期 | 把差异控制删掉，或仅让最初interval样本通过即宣布R244闭合 |

计划需要使用现有typed cells/AST所有权；不是恢复R156完整载体重构、R240 CASE
typmod或R243输入资格/语法专项。若发现资格不足或新的载体/生命周期缺口，应独立
记录，不以保守跳过路径冒充完整PG planner。原总目标和未完成项范围保持不变。

工作区NetworkServer、prepared_query_execution、ExecutionPlan/Main均有其他暂缓
草稿或暂存内容。实现必须从已提交纯投影出发，逐项保留这些字节与原暂存18路径；
不能把它们混入本项。仅做本地commit，不push，Actions保持禁用。

证据目录：`/tmp/dbms-r244-bind-planning.DnS1MSs5`。

## 证据SHA256

| 记录 | SHA256 |
| --- | --- |
| 完整实际PG/产品15组 | `6110899781417d3a866dab3d0bb0f1822c84787ae59e3c2f30df5905c6d96f02` |
| 逐字段9组差异 | `b998c5bbeb58ef57fb988f1bc02ca52e0dbb7f7df20b2569236160801ef9c3f9` |
| 便携观察器与原采集完全相同 | `b3a340808c0a79b1f255cd776ff11feedc0582d6538bfabb8884cf5aaffd2297` |

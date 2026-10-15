# R93：布尔聚合表达式的 typed 求值与分组结果

状态：本范围已修复；21个不同完整native及18个完整协议邻居通过。
新协议150/152通过、两个独立SHOW失败保留，不称整个聚合/原273完成。

## 原问题及实际复现

R92同一生产输入上，512行真实INTEGER/BOOLEAN来源，每个256行分区均有NULL组。
ParallelGroupAggregateOp将非列名布尔参数当成空文本而不是求值：全真来源的
bool_and(v>5)串行为t、并行为f，全NULL组也错误为f。两个worker确实运行，
不把小输入的串行fallback叫并行验收。串行节点另有只处理带空格参数的文本
替换路径，丢失类型、引号、NULL与错误。SQL GROUP BY实际使用的旧
StorageEngine::groupAggregate只创建getValB却未赋值val，boolResult又没有
最终返回分支。普通全局SQL常先投影参数，不能称所有SQL布尔聚合都曾失败。

| 原R92 Root完整入口 | 控制 | 失败 | 实际证据 |
| --- | ---: | ---: | --- |
| parallel_boolean_aggregate_expression_test.cpp | 408 | 213 | r93-old-permanent-boolean-matrix.log，实际1；96个实际worker标志控制均通过 |
| boolean_aggregate_argument_context_test.cpp | 68 | 36 | r93-old-boolean-context-direct.log，实际1；直接扫描副作用表物理行/NULL位，不用另一聚合计算预期 |
| boolean_aggregate_expression_protocol_e2e_test.py | 152 | 56 | r93-old-boolean-wire-configured.log，实际1；54个分组表达式错误、另2个SHOW不支持 |

同一152控制在真实PostgreSQL 18.6（180006）完整通过，日志
r93-boolean-wire-pg18-reference.log实际0。不是编译失败当产品反例，行/OID断言保留。

## 修改方法与首版失败

共享aggregate_type_detail::BooleanArgument按schema绑定列值、NULL位、类型和
collation；静态parse/input类型推断不执行参数。未知字面量按BOOLEAN上下文转换，
已知非BOOLEAN拒绝；逐行使用真实来源engine/database求值。保留结构化SQLSTATE，
不把除零/非法转换吞成聚合NULL。无结构化SQLSTATE的旧异常仍抛出，不宣称统一
所有旧错误路径。串行及并行节点在child打开后保存scanOrigin，FILTER后求值；
AND/OR结果已确定也不提前停止，保留volatile调用次数。worker仍只分区，求值在
leader归并阶段，不引入worker内UDF执行。旧GROUP BY非列参数复用同一实现，
补齐布尔结果返回；保留原列参数/TOAST/virtual读取，新增异常路径释放本层锁。

首版只改物理节点，408控制仍失败144次：静态BOOLEAN的CASE/COALESCE选中NULL
分支后runtime可能未定型，过严类型检查错误报XX000。保留
r93-physical-core-native.log实际1；当时第二个68控制入口未执行。最终候选让
已准入的静态BOOLEAN域承接NULL，非NULL类型变化仍报错。首版补丁/对象只存档。

## 夹具校正及独立SHOW问题

初始外部夹具误将makeIntColumn的scale4当成四字节INT；实际为BIGINT，前置退出2。
改scale2并检查实际dsize、来源值及每分区NULL人口后，初始120控制实际23失败。
context原误要求resolvedCollation为大写C，实际规范化为c，已改为声明C/规范化c/
binary检查。中间COUNT不是独立副作用oracle，最终改forEachRow/实际NULL位，
上述68/36以最终版本为准；原错夹具及日志保留，不把其失败数算数据库bug。

Root worker设置为process-global：普通SET返回XX000，PG模式SET GLOBAL返回42601。
最终夹具在自有隔离server内ALTER SYSTEM持久化，再用真实pg_reload_conf应用，
断言返回BOOLEAN t；参考库仍用普通SET。没有修改权限校验或暂缓安全模块。
SHOW该参数仍返回Unknown SHOW command，是独立后续问题，152中的两项断言
不删除，不称整个wire已绿。

## 当前验证及边界

r93-own-fresh58.log实际0：本工作区全部58生产单元按标准O2实际编译，最多两路；
逐个compiler exit0、标准链接和重复up-to-date均0，不借历史对象。binary为
`30f85ee7f0369b65b6a5f9b333514cbedb9c361afdd28225738a4aae3ee4f3f2`，生产输入
`9e4d4cad3ac6ce25a5bdd9a47b07efb5d9b11a1f3f371330a855bfa352cf9669`。
注册后只重新链接、重复构建，binary字节不变，不称又实际重编58份。

r93-shared-core-native.log实际0：两个完整入口分别408/0、68/0失败，worker
标志、独立人口oracle、NULL/空串/文本NULL、collation、22012/22P02、FILTER先于
参数错误均通过。三种函数在serial/parallel下各测试全512次及FILTER后4次真实
volatile调用；物理行/NULL位正确、另一engine不可见、原transaction可rollback。
不为通过该测试改provider生命周期。

r93-shared-wire.log实际1：原152控制全部执行、150通过，仅SHOW 0/2两项仍为
XX000；旧54项布尔结果错误已全部通过，非整套wire0。全部58 receipts/输入/
binary首尾一致。r93-test-cache-preparation.log只复制同一Root的57份库、fresh
stubs；独立fence逐字节核对58 receipts/57 test库、双stamp、binary、README和
Actions均0。新879 native+3 Main+505 Python=1387组合未全量运行。

r93-neighbor-native.log另外19个不同完整入口全部0：原布尔聚合、真实并行
执行、DISTINCT/FILTER、集合聚合、参数/作用域、返回类型/根owner、enum绑定、
HAVING/常量布尔、collation、窗口数组、prepared/global及scalar歧义均通过；
R92缺失TOAST计数器与合法重写也保持通过。与核心合计21个不同完整native，
不是用408个控制冒充408个驱动。

r93-protocol-neighbors.log完整18入口全部0，含参数/作用域/算术、FILTER、
分组/类型/根owner、prepared/HAVING/enum、窗口/数组、布尔常量/连接、函数
atomicity、pg_settings及原完整PostgreSQL协议；原默认协议的Simple/Extended/
错误恢复及plaintext协商均通过，不当作真实TLS。输入/binary/全部58 receipt
首尾核对0；加上新协议是19入口中的18个完整0、1个仅SHOW两失败的实际1。
不以该18/19替代全1387。

517份Markdown、525 Python AST、13 shell语法、879 native盘点、505唯一注册/
487 standalone、文档/契约/版本及六个总账单测通过；require-complete实际1。
日志/临时辅助脚本位于`/tmp/dbms-review-r46.ifu6SWLp/`。

SHOW、旧全量末尾DROP/普通DDL间歇超时及其余原要求继续；R15/R51/R65、安全/
TDE、CTAS/后台checkpoint等用户暂缓专项不自行重开。本项不改provider生命周期，
不降低断言或提高socket期限。原273仍22 complete/166 partial/70 unverified/
15 deferred。新组合未全量运行；不push、Actions禁用、README保持长期入口。

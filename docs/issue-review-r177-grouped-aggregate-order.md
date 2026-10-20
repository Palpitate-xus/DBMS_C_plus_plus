# R177：GROUP BY普通聚合排序、DISTINCT及计算目标

状态：用户于2026-10-20明确要求“skip it”，本项暂缓、尚未修复。
原始失败、修改方法及未验收候选保留；普通continue不恢复本项。

## 跳过时的保存状态

- 生产工作区未安装R177候选，也无本项生产修复或新增注册测试提交。
- 独立候选`/tmp/dbms-r177-grouped-aggregate-candidate-v1.YGVMEPLO`保留，
  包含私有执行器草稿、77项原生夹具和142项协议/JSON oracle草稿。
  执行器及原生夹具只完成syntax-only检查；没有候选对象、数据库运行或通过声明。
- 原128操作的V3观察不改写。扩展V4增加14次重复值控制，PG142采集外层0，
  旧accepted产品142采集外层1，原4个字段数缺陷仍失败；两端均142完整响应并清理。
  这是采集事实，不是142项候选验收。
- V4参考日志`r177-v4-reference142-discovery.log`的SHA256为
  `6f468026cd8e5896299d18469c14d0c1c332627851b89bd6dc98676e70df6ef3`；
  产品日志`r177-v4-current-accepted-clean142-discovery.log`为
  `be9a7afc6521704a06a47c64cc9ebb63472c87a73aac90304f82d652a402ba59`，
  均保留在下述本地证据目录。严格142夹具仅为草稿，不作为已通过的注册回归。
- 原273状态及R164两次超时保持；仅此调查暂缓，不扩大为全部分组或普通SQL工作完成。

以下为跳过前的观察及计划，保留历史事实，不代表恢复执行或验收。
基线为[R176](issue-review-r176-ordinary-aggregate-order-keys.md)的有限源码提交`a35658c0`，
文档状态为`48831831`；R164两次完整矩阵超时仍保留，不因本项采集而变为通过或用户暂缓。

## 完整128次双侧观察

数据有两个真实分组，每组`n=(2,-1,NULL)`、`v=(1,-2^53,+2^53)`、`k=(2,3,1)`。
同一冻结清单在PG18.6与当前accepted-only clean串行运行，覆盖Simple、extended-text及binary。
清单包含空表/WHERE false/LIMIT0、FILTER、DISTINCT、缺失键、真实nextval及命名statement生命周期。

| 采集端 | 实际结果 | 证据边界 |
| --- | --- | --- |
| PostgreSQL | 外层0；128次发送、128个完整响应，零协议结构缺陷 | 无预填兼容性oracle，不是候选修复通过 |
| 当前产品 | 外层1；128次发送、128个完整响应，4个字段数不一致结果；无连接中断 | 严格解码失败仍记为失败，不把完整采集当作协议健康 |
| 两侧控制 | 序列初值1、2，相邻观察器自身增量1；各自UUID schema创建及清理均确认 | 只清理本轮确认拥有的schema，不接触用户数据库或长期服务器 |

原始SQL、顺序、协议动作及全部128项操作经UUID归一化逐项核对，V1/V2/V3清单完全相同。
离线比较得到77个匹配、51个不匹配的诊断签名；其中将不稳定正table OID归一化为来源关系角色，
不比较清理NOTICE文本。这不是77项候选验收通过，也不是51个独立缺陷或完整类型身份oracle。

## 实际问题与修改方法

| 问题 | PostgreSQL实际表现 | 当前产品实际表现 | 修改方法 |
| --- | --- | --- | --- |
| 每组`sum(v ORDER BY k ASC)`，三种协议 | 两组均0 | 两组均1；DESC均1只是匹配控制 | 将真实typed排序键交给每组聚合所有者，保留float8逐步累加语义，不能用整数和代替排序证据 |
| 每组`sum(DISTINCT n ORDER BY n)`，三种协议 | 两组均1 | 两组均0 | 保留已绑定参数身份及真正DISTINCT归约；不能把`distinct n`当物理列名或只删除DISTINCT |
| `g,sum(v ORDER BY k)+0`及相反方向SUM的组合表达式 | 输出两列：分组键及计算结果 | Simple直接丢失聚合计算目标，只剩分组键 | 以完整SELECT AST构建typed分组输出，不再从文本函数形状猜目标，也不能填NULL掩盖丢列 |
| 上述两个计算式的extended/binary，共4次 | RowDescription和每个DataRow均为两列 | 描述两列、每行实际只返回一列 | 使描述符与执行共享同一分组投影所有者，精确校验字段数及NULL/OID/格式 |
| 每组ORDER键为真实nextval，含COUNT的NULL输入与SUM的全NULL参数 | 每次全表执行6次键求值 | 零次键求值 | WHERE后、每组FILTER通过后缓存真实参数/键；NULL参数仍需键求值，归约再跳过NULL |
| 保留命名statement的执行及重复Bind/Execute | 每次6次键求值；Parse/Describe、Bind/Describe不执行 | 执行和重执行均0次键求值；元数据阶段同样正确不执行 | 绑定可复用，但每次执行需新建组输入；保留元数据纯度，不缓存前次结果 |
| 分组`sum(n ORDER BY k) FILTER(WHERE false)` | 保留两个分组，各返回NULL、零键副作用 | 0A000 | 复用typed FILTER/NULL需求语义；有组但无合格输入不同于根本无输入组 |

序列效果均扣除了观察器自身增量1。全NULL输入仍产生6次键求值，FILTER false则0次；
不能用COUNT忽略NULL或数学交换性来省略合法ORDER表达式。
当前产品普通SUM/COUNT、float8 DESC、COUNT DISTINCT及正条件FILTER有匹配控制；
缺失键在空/false/LIMIT0仍报42703，DISTINCT非参数键报42P10，输出别名不冒充输入键，
这些已正确行为必须保留，不能因新的分组执行修复而退化。

## 初版中断与完整采集方式

V2参考端完整128次健康采集；产品只完成41个结构正常响应，在第42个extended计算目标
响应中收到完整`1/t/T/2/T/D/D/C/Z`，严格解码拒绝“两列描述、一列数据”，随后86项未发送，
清理也未发送。不能把V2的128条记录说成128条已执行SQL；私有产品测试进程随后关闭。

V3保留原严格解码器及全部操作。完整响应已经到达唯一ReadyI边界时，将字段数错误保留为
`OBSERVED_PROTOCOL_DEFECT`且失败标志不变，继续采集下一项；传输或未完整到达边界时仍停止。
V3产品全部128项实际发送、4个结构失败、无传输中断并成功清理；未降低既有测试断言或超时。

## 已定位路径与验证计划

全局typed聚合和GROUP BY的legacy Group/Parallel/storage消费路径并不相同。
[R176](issue-review-r176-ordinary-aggregate-order-keys.md)的真实键、FILTER、DISTINCT及typed归约
需要由实际分组算子按组消费，不能只在全局聚合拥有排序状态。
Main的文本分组投影丢弃计算目标，而纯metadata仍能给出完整描述，导致真实协议字段数错误。

1. 在已绑定查询图上建立分组键及每组输入，复用typed聚合归约，保留键/参数共享与稳定排序。
2. 保持原输入source occurrence/ordinal和NULL，按完整目标AST生成每组输出，不改写SQL文字。
3. 增加每组重复值控制，区分SUM与DISTINCT SUM、COUNT与DISTINCT COUNT；补空/NULL/FILTER、
   多组/多键、计算目标、元数据、真实副作用及重执行回归。
4. 原128操作及所有已观察到的错误阶段/字段/值必须保留；新严格夹具先完整PG对照再验证候选。
5. 独立候选、工作区及相邻回归按真实结果记录；R164失败不删除，不宣称全部注册测试或原273完成。

以上是修改方法，不是已实现功能。GROUPING SETS、所有分组键类型、全部参数及嵌套owner等
仍需分别验证；不以本项有限证据关闭SQL-04或其他大项。R173、R156及其他用户暂缓不恢复。

## 固定证据

冻结V3采集器：`/tmp/dbms-r177-grouped-aggregate-discovery-v3.0GhjRglz/collect_grouped_order.py`，
SHA256 `22a7942480e079df0e490766d33b7fd0297af1875aabe3571834af256f538af5`。
证据目录：`/tmp/dbms-review-r46.ifu6SWLp/`。

- `r177-v2-reference128-discovery.log`：SHA256 `5ac20bc631e24f6f901eeba94cf91c491bf5f8619afe2fb61e1544facde3ced7`。
- `r177-v2-current-accepted-clean128-discovery.log`：SHA256 `8fcd2a12c2175929c857dc4e366527ff80641c37830f045e15ee919b22d84f77`，41个正常响应、1个完整结构错误、86项未发送。
- `r177-v3-reference128-discovery.log`：SHA256 `8a2ad6cea421106b48979ee7d93619171e24558ab3586361377452a6293b188f`。
- `r177-v3-current-accepted-clean128-discovery.log`：SHA256 `fe7d75e79f7fc9ef027be0a18600a177097d5cb6812abb069485f74bd0d5347a`，全部128响应、4个结构失败。
- `r177-v3-offline-comparison.json`：SHA256 `b9925bacd68bbbef6feae048c2a4662fb9292f724c201a56c43130797b079606`，诊断比较非验收oracle。
- `r177-grouped-order-source-only-inventory-v3.json`：SHA256 `8ebd78414c788b080714a59d1a97ad676fce9a747639daaa132fcbcb837c6341`。

本项暂未增加已注册测试或修改生产源码；原273状态不提升。保持逐项本地commit，不push，Actions禁用。

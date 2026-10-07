2026-10-06 PL scalar/native follow-ups 两项独立source/test commits：`b6683521` 修复真实native SELECT/WHERE/ORDER/INTO表达式错误从22012/22P02/22003被吞为XX000；`8cd860e7` 用canonical AST位置绑定修复quoted "X"/x变量值/NULL/type混淆，保留BIGINT宽度、session特殊值、EXTRACT语法和显式trigger binding，并拒绝不存在的变量/qualifier/function。旧native/wire实际红、独立development新storage/stubs的3native及2protocol绿均有记录。ROOT正式O2只重编变更storage对象，其他54对象严格复核匹配，normal/repeat build、55/55签名及binary stamp通过；最终matching O2的6native及7项专项/相邻protocol通过。完整默认协议本follow-up未重跑；前一615/7bc组合在main3170 INSERT kw_joined timeout exit1，之前三次完整超时也保留，不能宣称完整protocol/suite通过。6项ledger unit/3项文档版本兼容检查通过，require-complete仍exit1。见 `docs/issue-plpgsql-scalar-binding-and-native-errors.md`。query内variable/source-column歧义、函数错误后写入不原子及statement-image写放大仍独立未完成；SQL-04/FUNC-05/06仍partial。总账273保持22 complete、166 partial、70 unverified、15 deferred_by_user；未push、Actions禁用、用户安全/TDE跳过项保持deferred。

2026-10-06 cold recovery / PL/pgSQL 两项独立修复已提交：`615f2c54` 修复真实 SIGKILL 后全新 exec 访问未初始化 database-lock map 的冷启动崩溃；`7bc76204` 保留完整 SELECT INTO 查询、structured typed first-row/NULL、STRICT/FOUND、声明/赋值/返回类型与错误码、stored CTE scope 及 native checked fallback。独立 fixture commit `acaa0bea` 用真实表达式 evaluator 替换旧 native echo host，声明 runaway counter 并精确断言未降低的步数上限；旧 O0 exit137 与 O2 RSS约53.5GiB后主动TERM的失败均保留，纠正fixture后O0 1.39秒/15420KiB通过。最终 development 7项专项/相邻协议及3项native通过；合并正式优化版55对象重编、normal/repeat build、55/55签名及binary stamp通过，10项专项/相邻协议及6项matching native通过。完整默认协议本轮在main3170 INSERT kw_joined处socket timeout exit1；此前两次JOIN完整超时与第三次instrumented CREATE TABLE超时也保留，snapshot写放大未修，不宣称全协议通过。6项ledger unit及3项文档/版本/兼容检查通过；全注册suite/TLS runtime/PG18.6 differential未跑。见 `docs/issue-cold-start-transaction-lock-registry.md` 和 `docs/issue-plpgsql-select-into-typed-query.md`。quoted scalar X/x复合绑定错值与函数P0002/22P02后写CTE仍落盘已独立复现，正在分别修复；FUNC-05/06、SQL-04、WAL-05/08、TXN-05/07仍partial。总账273保持22 complete、166 partial、70 unverified、15 deferred_by_user；未push、Actions禁用、用户跳过安全/TDE项保持deferred。

# 工作区与复查清单收尾计划

## 2026-10-07 当前完整273目标计划（fd46；以下较早记录均历史）

当前source `fd46f1a7`：693auto+frontend/355registered/58TU，84项独立
source/test修复commit；原273总范围不缩，全部未闭环问题继续。

| 阶段 | 原要求 / 下一动作 | 当前实际证据 |
| --- | --- | --- |
| 每根因版本管理 | 真复现、保强SQL、独立commit、用户push | 新fd46 generic dispatch已Root独立commit；原61强红、strict0；两窗口前序独立commits保留 |
| 当前ABI/全对象 | 真源/头/flags/manifest/receipt/stamp/freeze | normal69161 soleMain fresh+57当期4796逐对象证明正常donors，全58当前签名/输入hash/repeat0，非新fresh58/SAN |
| 完整组合验证 | 每整个driver、默认磁盘/期限、pure/NULL/once/type | 52完整native90205/18完整whole39351/post native14714/全18whole63651全0，旧41native真默认磁盘，窗口144/72及wire195/147均保全 |
| 原全量scope | 每原discovery/registry、完整真实终态、保全部旧红 | 原Source80 full42496终1/all1043精确multiset，14失败逐项分类，不14bug/84 verdict；新current84full未启动 |
| 接续CTE/BIT/integer | 真实当前Root全文审/合成，不借private绿审批 | c6bd guard scopedREADY含旧9扩展/COUNT红；integer673a独立链待审；BIT compact member84/284继续；original FETCH scalar-child已另独立owner |
| 全部其它原family | 每原frame/query/catalog/type/storage/recovery/ops要求 | enum aggregate rank/matview58030/所有原未闭环不跳，QRY07/TYPE08等仍partial，不以单dispatch修复冒关闭 |
| 总账交付 | 原273证据/checkbox/commit/未完成gate一致 | 22complete166partial70unverified15deferred，完成gate拒绝；无push/Actions启用/安全TDE恢复 |

下一步先审实证inherited CTE guard所有实际source roles并在当前ABI隔离合成；
integer/quoted真实owner逐commit合入，BIT有限bundle须完整审/compact准入全证，
FETCH与enum rank各独立补齐；再开展确切当前原full，保其它原未闭环要求。

## 2026-10-07 历史完整273目标计划（4796）

当前source `479675f1`：692auto+frontend/354registered/58TU，83项独立
source/test修复commit；原273目标及所有未闭环范围不缩小。

| 阶段 | 原要求 / 下一动作 | 当前实际证据 |
| --- | --- | --- |
| 独立窗口修复 | 原强矩阵、完整consumer、每根因commit | 10989d96 NULL195和479675f1 GROUPS147由真实62/40红变全0，strict198/150全0，原生144/72和完整相邻/提交后gate全0 |
| 当前生产输入 | 实际公共头ABI、全58 receipts/flags/stamp/inputs | NULL normal25277真fresh58；GROUPS normal24221 soleExec+57当前109逐对象证明donors，不冒第二次fresh58/SAN |
| 全量原验收 | 所有原注册/自动native、真实终态、保全部原红 | Source80 full42496实际终1/all1043 receipts精确原multiset；687auto+frontend1+341registered PASS，3auto+11registered FAIL，不算14bugs/83 verdict；逐项分类见integration |
| 递归CTE introduced回归 | 实际可继承frame/session/db/source-role owner | 原第17条42P01，旧7504/82bf同完整driver PASS；private原21/强once及邻居绿，原full另derived/materialized红，要求这两完整consumer补证再import；已有9扩展旧红不削弱 |
| generic/BIT/integer | 完整参数类型、metadata、NULL、once、physical/index owner | generic current82全矩阵61真红、matched严格18同矩阵0；finite BIT及codec私有READY；Root新compact native84控制19162实际1/68红，两个真实owner全member准入继续；UNKNOWN/quoted/index其它边界继续 |
| 其它全部原family | 每原目录/query/storage/recovery/IO/DDL/operations要求 | matview后台58030仍OPEN未修；不以两窗口根因修复冒frame/spill/collation或任何更大family关闭 |
| 总账与版本管理 | 每原条目证据/状态/checkbox/commit一致，用户push | 原273=22complete166partial70unverified15deferred，完成gate拒绝；本地独立commit，无push/Actions启用/安全TDE恢复 |

完整证明见integration与两窗口issue。下一步完成generic当前Root隔离导入/正式
正常构建及全强矩阵合成，接收已实证CTE owner修复并逐问题验证提交，复查有限
BIT bundle及其它原问题；不把 private READY 当 master 已完成。

## 2026-10-07 历史完整273目标计划（7b22）

当前test/source `7b2223cf`：690auto+frontend/352registered/58TU，81项
独立source/test commit；生产逐字同3ffbb516。原273目标/所有未闭环范围不缩。

| 阶段 | 原要求 / 下一动作 | 当前实际证据 |
| --- | --- | --- |
| 独立提交 | 真复现、保原SQL/场景/强断言、每问题commit | 原TRUNCATE完整134；7b2223cf仅fixture身份，原三节+第四拒绝控制/target及11邻接/postcommit全0 |
| 全量原验收 | 所有当前注册/自动native、真实终态、不隐藏旧红 | 冻结Source80 full42496真正LIVE，仍含旧TRUNCATE；非81 testepoch，81 full未启动 |
| 当前matview生产故障 | 真后台consumer、目录publication、dirty/retry边界 | 原首轮0而四轮重复78209为1/两134；真实后台domain目录栈97700为1，独立producer复现/修复继续 |
| BIT / generic / enum aggregate | 完整实际type/owner/NULL/IN/OR/FILTER/rank/once消费者 | BIT新mixed未知左operand误cast已查出，仍held；aggregate真实OR/FALSE及enum绑定独立处理，不import未证source |
| 恢复/CREATE/全部原family | 每原owner/retry/IO/DDL/query/storage/operations要求 | 原scope继续，不以TRUNCATE测试契约修正冒恢复窗口或family关闭 |
| 总账 | 原273逐条证据/状态/checkbox/commit一致 | 22complete166partial70unverified15deferred；完成gate拒绝，无push/Actions/安全TDE恢复 |

完整输入/实际终值/源码映射见integration和独立TRUNCATE issue；当前生产all58
与exact3ffb一致，所有driver/stubs fresh+57正常对象匹配证明，不冒fresh58/SAN。

## 2026-10-07 历史完整273目标计划（3ffb）

当前source `3ffbb516`：690auto+1frontend/352registered/58TU，80项独立
source/test commit。四新Root issue对应原scope保持；最新原full已真实启动。

| 阶段 | 原要求 / 下一动作 | 当前真实证据 |
| --- | --- | --- |
| 逐根因commit | 复现/不弱化SQL/完整consumer/独立提交 | 7ecf、8467、0ffd、3ffb分别commit；原DDL/rank/TYPE/range错误和作者错误均保留 |
| 当前生产输入 | 每公共头ABI正确，全58正常/receipt/flags/freeze | 0ffd真fresh58 normal83683为0；3ffb新soleHelper+57当期proved donors71958为0，非新fresh58/SAN |
| 原完整验收 | 当前全部690+frontend/352原runner/default期限完整终态 | 原full42496 LIVE，独立树原58/57生产层proof迁移；focused15/7与strict0不是全绿 |
| TYPE08/TYPE11所有消费者 | aggregate/rank/OID/DDL事务；全literal/IN/NULL/CAST/descriptor/binary | Rootenum33/6完整0；真实generic/enumaggregate与匹配locale继续，BIT held缺口未虚报闭合 |
| 恢复/CREATE/所有family | 每原owner/retry/IO/catalog/query/storage/ops要求 | temp/CLOG普通owner重试、CREATE与其它原未闭环继续，未证private source不导入 |
| 总账验收 | 每原条目完整证据/状态/checkbox/commit一致 | 22complete166partial70unverified15deferred；完成gate拒绝，无push/Actions/deferred安全恢复 |

旧7504full6359已实际1/all1032标签、旧82bf48181实际1/all1031；不把22失败
当22bug或当前结论。所有helpers/source冻结后启动，无观察到期kill/restart。
完整出处/输入/失败/源码映射见integration。

## 2026-10-07 历史完整273目标计划（7ecf）

当前test/source `7ecfd8eb`：687auto+1frontend/350registered/58TU，77项独立
source/test commit。DDL fixture错误游标单项修复，不改任何生产源/头/格式。

| 阶段 | 原要求 / 下一动作 | 当前真实证据 |
| --- | --- | --- |
| 每问题独立提交 | 不弱化原SQL/19节/断言 | 7ecfd8eb；旧原native134，正确扫描+实际CREATE及同XID COMMIT，完整target30243/邻接十项17482为0 |
| 当前匹配生产 | 全58源头flags/receipts/stamp/freeze | 生产逐字同84e；fresh drivers/stubs+57 proved matching正常objects，非新fresh58/SAN |
| 原全量验收 | 当前687+frontend/350全部完整终态 | 当前full未启动；旧82bf48181实际1/1031标签，75046359仍逐个LIVE；不以旧红批准当前 |
| BIT / ENUM | 完整真实类型、literal/IN/NULL/ORDER、rank/quoted生命周期/OID/ALTER | 2828缺依赖，真实当前33wire32个42883；held literal/Main与ENUM组合继续，不弱化quoted矩阵 |
| 恢复/CREATE/所有family | 原完整owner/retry/IO和全部273要求 | temp/CLOG普通owner、CREATE与其它所有原未闭环继续；不以测试false-negative修正冒恢复闭合 |
| 总账 | 每原条目完整证据/checkbox/commit一致 | 22complete166partial70unverified15deferred；require-complete拒绝，无push/Actions/deferred安全恢复 |

## 2026-10-07 历史完整273目标计划（84e1）

source `84e16a74`：687auto+1frontend/350registered/58TU，76项独立source/
test commit。空enum schema与empty hash/NULL各一个Root issue commit；原273未缩。

| 阶段 | 必须验收的原范围 | 当前证据 / 下一动作 |
| --- | --- | --- |
| 独立修复 | 真反例/完整控制/每问题commit | 89ea→75b；472+105→84e，同issue先补NULL扫描边界再合入，不改master历史 |
| 当前生产输入 | 全58源/头/flags/receipts/repeat/freeze | normal37842=0，fresh soleTM+57当前21b完整证明normal donors；不冒fresh58/SAN |
| 原完整回归 | 当前687+frontend/350、磁盘/default期限、完整终态 | 当前full未启动；final20native82125/4whole4946/两strict18为0，prefix首wire1及repeat0完整保留 |
| TYPE08剩余 | 所有rank/投影/排序/quoted TYPE/OID/ALTER事务要求 | scalar native绿不代实际wire，quoted控制原样留，两个根因独立再组合全部矩阵 |
| TYPE11剩余 | 全typed operands/unknown/IN/CAST/descriptor/binary要求 | literal候选新TEXT/INT假命中暂缓导入，补强前不借原11/4窄绿批准；constructor已Root单项修复 |
| 恢复/CREATE | 原stale-temp+真实WAL/REINDEX/普通owner/CLOG、所有IO层 | 真exec强反例及普通后台栈已记录；未证WIP不导入，不吞错误/弱化old guard |
| 其它family/总账 | 每原条目完整证据/状态/checkbox/commit/验收一致 | 全部目录/查询/存储/恢复/运维继续，22complete166partial70unverified15deferred，require-complete拒绝 |

旧8694full64688已终1/all1014标签完整，不以失败数量当bug数；82bf48181、
75046359原full逐个live。helpers冻结、原deadline不变，无push/Actions启用/
用户deferred安全TDE恢复。完整source映射与范围见integration。

## 2026-10-07 历史完整273目标计划（21b1）

source `21b105dd`：685auto+1frontend/348registered/58TU，74项独立source/
test commit。BIT ARRAY constructor 隐式无typmod保全独立提交，原273未缩。

| 阶段 | 必须验收的原范围 | 当前证据 / 下一动作 |
| --- | --- | --- |
| 独立修复 | 真反例/完整控制/每问题commit | private9cdb→Root21b1；完整值/空/NULL/nested/OID及原显式BIT默认保持 |
| 当前生产输入 | 全58源/头/flags/receipts/repeat/freeze | normal71286=0，fresh soleExprEvaluator+57proved d661 normal donors；不冒fresh58/SAN |
| 原完整回归 | 当前685+frontend/348、磁盘/default期限、完整终态 | 当前full未启动，Root11native82410/3whole57467/strict180006为0；旧full不能当当前绿 |
| CREATE三层 | 精确native类别、已有flush结果、真实DDL cause/SQLSTATE | 四点native错分类/七点false success/普通wire XX000均实际确认，各独立commit，最终组合未READY |
| BIT与ENUM | 原全部值/存储/NULL/operator/OID/extended/DDL事务消费者 | 比较/存储literal/descriptor和空enum schema/hash/投影等继续；不以第一项绿代family |
| 目录/查询/恢复/所有family | 所有原namespace/MVCC/WAL/map/EXPLAIN/存储/运维要求 | 原未闭环要求继续，未证私树不导入；所有旧红保留当前源核实 |
| 总账闭合 | 每原条目完整证据/状态/checkbox/commit/验收一致 | 22complete166partial70unverified15deferred，require-complete继续拒绝 |

旧full48518/93414/27869实际终1、完整标签保留；旧8694full64688、82bf48181、
75046359仍逐个live，不因观察时间kill/restart。新helpers冻结、不改单次deadline，
无push、Actions禁用，用户跳过安全/TDE deferred。完整映射见integration。

## 2026-10-07 历史完整273目标计划（9f7d）

source `9f7d55cc`：684auto+1frontend/347registered/58TU，73项独立source/
test commit。新增真PITR选中stream修复及两原测试契约分别commit，未缩原273。

| 阶段 | 必须验收的原范围 | 当前证据 / 下一动作 |
| --- | --- | --- |
| 独立修复 | 真反例/完整控制/每问题commit | 三新Root映射见integration；weak fault候选先拒绝、最终线程归属实证后才单项合入 |
| 当前生产输入 | 全58 actual源/头/flags/receipts/repeat/freeze | d66110753正常0、fresh soleTM+57proved donors；9f7d生产逐字同d661，7504零CPP/all58迁移38219为0，不冒fresh58 |
| 原完整回归 | 当前全部684+frontend/347、磁盘/default期限、完整终态 | exact7504 full6359与82bf full48181 live，不冒9f7d full；105+front0，旧49whole41pass8fail如实保留 |
| 新强专项 | PITR三历史/原备份、DOMAIN全原driver、精确目录故障线程 | Root15+frontend/原冷backup0；DOMAIN11+强化target/strict18为0；tablelist10native6whole3重复0 |
| CREATE错误分类 | 真native IO与semantic区分、普通DDL原因/SQLSTATE/无effects/retry | 两层实际混淆已确认，分别真实注入/独立源码提交，不全换IO、不删原控制 |
| 目录/查询/恢复/所有family | 全部原namespace/MVCC/WAL/map/EXPLAIN/类型/运维要求 | 现有多段WAL、stale-temp、trigger、shared-engine目录与其它未闭环项不替代/不缩小 |
| 总账闭合 | 每原条目完整证据/状态/checkbox/commit/验收一致 | 22complete166partial70unverified15deferred，require-complete仍拒绝 |

旧full33648/56028/97347已实际1，另外四旧full仍live；每旧红在当前源核实，
不把失败数当bug数。新helpers冻结不改，过去作者EOF/offset/cache-preflight
失败全保，未计PASS。无push、Actions禁用、deferred安全/TDE不恢复。

## 2026-10-07 历史完整273目标计划（82bf）

当前source `82bf3739`：683auto-native+1实际frontend/347registered/58TU，
70项独立source/test commit。数组native物理类型、snapshot exporter传输可见性、
原routine canonical oracle各独立提交；原273目标和所有未闭环要求没有缩小。

| 阶段 | 必须验收的原范围 | 当前证据 / 下一动作 |
| --- | --- | --- |
| 独立修复 | 真反例、完整原控制、每问题commit | 新三项映射/原失败见integration；原snapshot/array不改，routine原SQL/calls全部保留 |
| 最新组合构建 | 全部生产输入一致、normalO2/repeat/58receipts/freeze | exact82bf97039为0；fresh soleTM+57proved90fe donors，不冒fresh58 |
| 原完整回归 | 原683auto+1frontend/347registered、磁盘/default期限、完整终态 | full48181已运行live，105+1native39059/49whole86895 live；三原完整native45204及真实故障68439为0 |
| 原view触发器错误后复用 | 保原23514/行/NULL/OID与下一SQL原15秒期限 | 实际trace定位未恢复outer session的析构指针；异常安全清理/真正完整wire继续 |
| namespace与目录 | 完整物理/dependency/peer/parent/savepoint/old refs | 私有原组与更强domain外列/descendant/NULL/quoted/rollback矩阵继续，未READY、不以单consumer代CAT01/09 |
| 备份/恢复 | timeline fork后的真实backup+archive/PITR/cold restart | 真timeline2新行漏恢复134已复现；按实际stream owner独立修复，不冒WAL family完成 |
| 完整DML/OPT16及所有family | WITH同图/phase/atomic、所有格式/options/真实指标、目录WALMVCC/存储/类型/查询/运维 | 全部原未闭环条目继续，不以当前专项替代原要求 |
| 总账闭合 | 每条原状态/checkbox/证据/commit/全验收一致 | 22complete/166partial/70unverified/15deferred；require-complete继续拒绝 |

上一1d25 focused入口全部完成但wrapper实际127（运行中改外部helper产生Bash
读offset错误），保留99native98pass/1arrayseed和frontend0、48whole39pass/
9timeouts，不算整组绿。新helpers冻结启动后不改。旧36568/79042 full已1，
另七full实际live；每个旧红必须在最新源确认，不能算独立bug数量或当前批准。
不push、Actions保持禁用、安全/TDE deferred；只有总账真实闭合才报告完成。

## 2026-10-07 历史完整273目标计划（1d25）

当前source `1d25a253`：680 auto-native+1实际frontend native/347registered/
58TU，67项独立source/test commit。六source-DML、两INSERT default/source、
primary-cleanup、compound source与EXPLAIN metadata各独立提交；完整273目标
未缩小。SourceDML中间oracle单独绿不代替USING消费者；原UPDATE DEFAULT不回退。

| 阶段 | 必须验收的原范围 | 当前证据 / 下一动作 |
| --- | --- | --- |
| 独立修复 | 真复现、保留完整原SQL/控制、逐问题commit | 十一新ROOT映射/原失败见integration；private最终整组而非选取子集 |
| 最新组合构建 | 新AST/全部headers一致、正常O2/repeat/receipts/freeze | 6aec真正fresh58已0；1d25四CPP fresh+54逐字proved donor门73339已0，不冒fresh58 |
| 原完整回归 | 原680 auto-native+1 frontend/347registered、默认磁盘期限、完整终态 | 最新99+1native91615/48whole12909 live，真实GNU fault38306为0，full仅准备；旧5ca618/319已1且937标签完整；其余九full含8694仍live |
| 剩余DML/EXPLAIN | WITH-final writer/CTE同图/atomic/phase、所有格式/options/真实指标 | ordinary/source/default/descriptor专项不代替全部OPT-16和DML；严格34×4原envelope继续 |
| 已复现native边界 | exact array seed/元素类型、namespace完整依赖/物理/目录/peer owner | 数组原assert不改；schema V1新异常修复和V2 committed-clean snapshot/旧refs/parent/rollback验证继续 |
| 当前原full失败核实 | 每项在当前matching58/O2完整旧fixture重现，不以旧红代新红 | actual原三native50319=2fail/1pass：routine int metadata134、snapshot导入后COMMIT可见性134，vacuum_full所有控制0；导出snapshot外部writer身份继续修复 |
| 目录/存储/全部family | catalog WAL-MVCC、完整FSM/VM重建/恢复、所有原273要求 | 每根因修复不代替CAT-01/CAT-09/STO-05架构验收；其它types/query/recovery/operations逐项继续 |
| 总账闭合 | 每条原checkbox/状态/证据/commit/验收范围一致 | 22complete/166partial/70unverified/15deferred；require-complete仍必须拒绝 |

8694 native89=0、whole43=32pass/11fail，GNU map fault两项0；原DELETE六断言、
九入口timeout和初始connect103保持失败。e083正式fresh58/保DEFAULT的merge10
实际0；6aec正式fresh58实际0；不借这些旧组合批准1d25。私有新phase完整14whole/
13native/六CPP scopedSAN/strict180006均0，/dev/shm与O0范围明确。强cleanup
正常/scoped-main同TU driver0，而原数组seed邻接仍134，继续真实修复。
旧5ca full16482已1：610native/243registered pass，8native/76registered fail；
不推断84bugs，原完整日志保留。另八旧full及8694原full逐个handle still live；
不因观察时间结束而kill或restart。不push、Actions保持禁用、安全/TDE deferred。

## 2026-10-07 历史完整273目标计划（8694）

当前生产/测试source `8694f31e`：672native/342registered/58TU，56项独立
source/test commit。map clean-peer、ordinary DML EXPLAIN、public bootstrap、
旧scalar native契约oracle分别提交；这不是四个完整family关闭。

| 阶段 | 必须验收的原范围 | 当前证据 / 下一动作 |
| --- | --- | --- |
| 独立修复 | 真复现、完整原SQL/控制、每问题独立commit | 四新Root映射见integration；private红/所有timeouts保留 |
| 组合构建 | 最新公共头一致，全58 fresh正常O2/repeat/receipts/freeze | 精确8694 build44782 live，无donors；89native/43whole/fault/full仅准备 |
| 原全量回归 | 原672native/342registered、默认磁盘期限、完整终态 | 最新组合未启动；九更早full逐个核实live，两较新组实际前进，不冒全绿/不restart |
| 剩余DML计划/执行 | 原DELETE/nativeUSING、UPDATE FROM、INSERT default/CASE真实source、WITH-DML/descriptor/phase | actual新强PG18基线/候选继续，不能用oracle改动隐藏真实consumer失败 |
| 目录/存储/所有family | 真namespace完整owner/WAL-MVCC、FSM/VM恢复和所有原273要求 | public/bootstrap及map三个根因修复不替代CAT-01/CAT-09/STO-05架构完成 |
| 总账闭合 | 每个原条目完整证据、状态/checkbox/commit/验收一致 | 22complete/166partial/70unverified/15deferred，require-complete必须继续拒绝 |

旧8f58正常已0、native84=83pass/1旧oracle、whole42=32pass/10fail；
旧a1b58/fault已0、旧d4cad native82=81pass/1fail及whole42=30pass/12fail。
新bootstrap22native/strict180006为0，whole11=8pass/3原timeout。
专项/重复绿不消除失败或转成完整suite/TLS/全部family批准。
不push、不启用Actions、不恢复用户跳过安全/TDE；完成目标没有缩小。

范围：开始时未提交的 `ExecutionPlan.cpp`、`main.cpp`、`grouping_sets_expr.sql`，以及 `code-review-progress.md` 中尚未验证的投影子查询解析。历史 PostgreSQL 全功能路线图不属于本次收尾；此前用户要求跳过的安全专项仍排除。不执行 git push。

| 阶段 | 工作 | 验收标准 | 状态 |
| --- | --- | --- | --- |
| 1 | 完善串行 / 并行 string_agg、array_agg | NULL、空字符串、参数表达式、FILTER、输入排序及串并行一致性有回归；单独提交 | 已完成；第 191、196 项；执行器专项及完整 SQL 入口通过 |
| 2 | 收尾分组表达式与临时调试代码 | GROUPING SETS / ROLLUP / CUBE SQL 有明确结果断言；移除临时调试输出；按问题提交 | 已完成；复查第 192 项；SQL E2E 通过 |
| 3 | 完成投影子查询解析检查 | 引号关键字、空白符、ORDER BY / LIMIT 等组合有回归；修复逐项提交 | 已完成；复查第 193–195 项 |
| 4 | 集成验证、更新清单 | 主程序构建和相关 C++ / SQL 回归通过；工作区干净；未 push | 已完成；31 个 C++ 测试和 3 套 SQL E2E 全部通过；改动全部本地提交 |

执行方式：先复现，再修复和回归，按问题单独提交。本文件记录本次明确范围的完成情况，不宣称整个数据库不存在其他缺陷。

## 完成记录（2026-09-08）

本次新增修复为第 191–196 项，分别提交为 `9d41c9b`、`2c3042d`、`06a6338`、`103c8a7`、`11c76de`、`fc6c1e0`；具体问题、影响和验证见 [复查清单](code-review-progress.md)。

收尾验证覆盖清单对应的 20 个 C++ 测试，以及以下 11 个相邻回归：`text_comparison_type_test`、`expression_evaluator_test`、`boolean_expression_test`、`greatest_least_projection_test`、`group_collection_aggregate_test`、`parser_phase1_test`、`parallel_exec_test`、`volcano_select_phase51_test`、`window_functions_test`、`aggregate_bool_test`、`aggregate_percentile_test`。

SQL 验证使用本次构建的 `build/dbms_review_main`，运行 `review_sql_e2e_test.py`（17 个查询）、`window_e2e_test.py`（13 项）和 `explain_analyze_e2e_test.py`（6 项），均在隔离目录中通过。当前环境使用 zlib 和 TLS stub；未验证 OpenSSL 分支，也未重开已排除的安全专项。Shell / Python 语法检查、`git diff --check` 和调试输出清理检查通过。

全部阶段关闭，未遗留本次范围内待办。仓库 GitHub Actions 工作流保持禁用，不执行 push；历史全功能路线图和这里明确排除的功能不计入完成范围。
## 2026-10-07 历史5ca/c061执行计划

最新执行版本 `c061a38a`（622native/320registered/58TU）：UNKNOWN输入、typed Append、普通UNION ALL入口及跨进程XID分别本地commit；private强矩阵scope见integration文档。新公共ABI的全58 fresh正常O2/repeat/audit/freeze90776实际0，冻结SHA见integration文档；原full622/32036568、matching12native35605及whole9wire48073已实际运行，未称full通过。下面表格记录前一精确5ca验收，不替代c061。5ca156native6042已实际1=154pass/2fail；5ca原full另外复现SETTABLESPACE后backup与BEGIN/drop活跃owner COMMIT两项原断言失败，均独立修复、不放宽。后续顺序：完成不改原断言的整合回归；验证并逐项合入domain/WAL代际、INSERT异常事务owner、限定函数/SRF候选；修复完整模式/domain优先级、集合P/D和剩余273项。原全文37/21与domain链setup强矩阵均保留；模式新增数组envelope拒绝在strict180006通过，当前基础候选全58 privateO047334运行，尚未ROOT合入。没有push、Actions启用或用户跳过专项恢复。

Source `5ca4278e` 已逐项本地提交，618 native / 319 registered / 58 TU；完整总账仍未完成，不沿用以上旧“本次范围关闭”作为273项完成证明。提交映射/实际失败/证据见 `docs/issue-ready-source-integration.md`。

| 下一阶段 | 验收要求 | 当前证据与状态 |
| --- | --- | --- |
| 最新组合正式构建 | 全58 fresh 正常O2、repeat、全部 source/header/flags/stamp、冻结同一binary | 精确5ca快照36316实际exit0，冻结SHA见integration文档 |
| 原完整回归 | 不改原SQL/assertions/deadlines，执行618native/319registered | 5ca full16482与156native6042已运行；75wire93351=70pass/5fail；旧d2 full590/312 96468仍live |
| 存储剩余问题 | 真实rename/drop/recreate代际恢复、正确RR setup的vacuum_toast原值断言通过 | 4c05967b独立修正错误首次读setup且保留所有旧断言/新增lazy控制，PG18双连接与5native通过；恢复生产问题仍独立修复 |
| 查询/类型剩余问题 | 保留whole37 DML/UNION ALL、whole domain/pattern priority、SRF完整known-gap控制 | 新ANY whole19和数组whole12专项绿；typed append、TYPE-19域链、模式、SRF两项仍进行 |
| 总账验收 | 全部原273逐项满足完整证据；用户跳过15保持deferred | 22complete/166partial/70unverified/15deferred；完成gate仍拒绝 |

不push，不启用Actions，不恢复用户跳过安全/TDE。旧e6全58正常构建实际0，但native137=135pass/2fail、wire65=64pass/1fail；旧d2 native120实际0、wire64=60pass/4fail。专项结果不替代新ROOT完整验收。

## 2026-10-07 历史2894完整目标执行计划（最新证据见文末）

生产/测试source `28940634`，642native/330registered/58TU。最新九项修复
已逐项commit，c061 checkpoint后累计19项；详见integration映射与原始证据。
完整273目标未缩小，旧2026-09“本次范围完成”不等于总账完成。

| 阶段 | 必须通过的验收 | 当前证据 / 下一动作 |
| --- | --- | --- |
| 独立修复 | 真复现、保持原SQL/断言、每问题独立commit | set三项/array三项/退休/域FK/cache原子guard均已分别ROOT commit |
| 组合正式构建 | 公共头全58 fresh正常O2，repeat及source/header/flags/object/stamp，冻结同binary | 25ee/42bd/0fdb/ed82全部实际0；最新2894公共头fresh58 60362 live，35native/15wire仅准备 |
| 完整原回归 | 原全部native/registered，默认期限和完整矩阵，不删失败 | d2/5ca/c061/42bd原full96468/16482/36568/33648及新0fdb56028/ed8248518 live；无全量PASS |
| 组合专项 | 新头匹配native、whole protocol、严格180006原强fixtures | 私有set43/18/12wire、array54/8native/4wire、retire19/3SAN、pattern11/48均有实际0；新ROOT尚待正式结果 |
| 已证实剩余缺陷 | Unicode模式、explicit search_path/SET LOCAL、真实磁盘timeout原因 | Unicode/runtime与错误旧fixture独立核实，search-path dependencies强矩阵在修；cache V3原强whole18/scopedSAN4已0并独立合入 |
| 剩余family | 逐项核实和实现全部尚未达到要求的273项 | domain ALTER/casts/revalidation、其它sets、完整类型/存储/查询/运维等仍OPEN，继续逐项修复 |
| 总账闭合 | 每条checkbox/状态/证据/提交/验收范围一致，完成审计实证 | 22complete/166partial/70unverified/15deferred；require-complete仍应拒绝 |

不push，不启用Actions，用户跳过安全/TDE专项保持deferred且不虚报完成。

## 2026-10-07 历史完整目标执行计划（c465）

生产/测试source `c465f898`：648native/334registered/58TU。c061后27项独立
source/test commit，最新八项已分别合入。完整273目标没有缩小。

| 阶段 | 必须通过的验收 | 当前证据 / 下一动作 |
| --- | --- | --- |
| 独立修复 | 真复现、保留原SQL/断言、每问题独立commit | Unicode四项、search_path、声明namespace、统一provider、catalog分别commit |
| 组合正式构建 | 公共头全58 fresh正常O2、repeat/source/header/flags/object/stamp、同binary冻结 | 2894正常58/35native已0；c465新58 build28366实际live；58native/25wire仅准备 |
| 原完整回归 | 原648native/334registered、不改默认期限/矩阵 | 新full仅准备；旧七个full均实际live；2894 wire15=8pass/7fail，0fdb wire13=3pass/10fail |
| 组合专项 | 最新头匹配native、完整protocol、严格180006强fixtures | 私有Unicode13/90/8native/4200、namespace14wire/10native/4SAN、catalog12native/fault/磁盘whole通过，scope见integration |
| 已证实剩余问题 | 实际消费者及物理需求修复，不靠兼容fallback掩盖 | legacy模式16个native断言真红；domain DEFAULT原whole11断言真红；无变化SAVEPOINT重建镜像诊断继续 |
| 剩余family | 每个原273条目完整实现/核实/验证 | routine creation/overloads、完整types/catalog/query/storage/运维等继续，不用专项替代 |
| 总账闭合 | 每条checkbox/状态/证据/commit/验收范围一致，完成审计实证 | 22complete/166partial/70unverified/15deferred；require-complete仍拒绝 |

不push，不启用Actions，用户跳过安全/TDE保持deferred。任何尚在运行或仅准备
的 gate 都不写为通过；历史失败不因新候选专项成功而改绿。

## 2026-10-07 历史完整目标计划（6320）

source6320：658native/338registered/58TU，c061后39项独立source/test提交，
最新12项逐项映射见integration；目标始终是原273完整要求，不借旧小范围关闭。

| 下一阶段 | 验收 | 当前证据与动作 |
| --- | --- | --- |
| 已验证独立修复 | 原失败、真实consumer、原assert/deadline、每问题commit | cold/domain五项/模式三项/map/no-effect两项/creation均已分别commit |
| 新公共头组合 | 全58 fresh正常O2/repeat/source/header/flags/object/stamp/冻结 | 精确9772 657/337 build12522 live；首个错误计数guard编译前失败保留；69native/28wire/full仅准备 |
| 原完整回归 | 不改原648/334及旧全量矩阵、实际终态 | c465正常58/native58已0；wire25=16pass/9fail；full79042与七旧full逐个核实live |
| 最新CPP验证 | 6320创建路径同头新对象、完整原whole/冷启动/撤销 | private core/冷启动/八邻接0；原DDL独立58030失败、V2wire7两实际timeout不删；ROOT新组合待验证 |
| 新已证实问题 | UPDATE DEFAULT/DELETE owner与实际RETURNING/nested aliases/域IO | 三个独立owner继续各自复现、修复、原强验证、独立commit；ROOT域IO保原bool断言 |
| 剩余family | 每个原273完整实现与证据 | generaltypes/catalog/queries/storage/recovery/operations仍继续，专项不能替代 |
| 总账验收 | checkbox/状态/证据/commit和每条需求一致 | 22complete/166partial/70unverified/15deferred；require-complete仍拒绝 |

domain旧来源不猜/不迁移、schemaB/D3旧reader拒绝；必要写入恢复Append53.705秒
原15期限失败仍保留，不因no-effect专项绿就关全部延迟。无push/Actions激活或
用户跳过安全/TDE重启；各私有O0/局部SAN/单CPP增量不冒充最新全量正式通过。

## 2026-10-07 历史完整目标计划（b69b）

sourceb69b：663native/340registered/58TU，c061后45项source/test独立commit。
最新六项映射和完整实际日志见integration；总273目标不缩小。

| 阶段 | 必须满足的验收 | 实际状态与下一动作 |
| --- | --- | --- |
| 独立问题版本管理 | 原复现/原assert/deadline/真实consumer，每问题commit | 新owner/alias/域IO/UPDATE三项分别本地commit；原失败不删除 |
| 新public callback ABI | 全58fresh正常O2/repeat/source/header/flags/object/stamp/freeze | exactb69b23520 live，无donors；76native/32wire/原663/340只准备 |
| 原组合比较 | 对应revision一致，不借旧ABI/私有O0 | 9772 fresh58已0，69native/28wire live；11d7两CPP+56证明donors匹配normal0，其helpers未启动 |
| 原全量回归 | 原所有native/registered及已知未注册红，期限不改 | c465原full和七旧full实证live；c465wire25=16pass/9fail；无全量PASS |
| 已证实继续问题 | 真实DML EXPLAIN/DELETE RETURNING/explicit array bounds/跨owner maps | 三个owner各独立tree修复，强fixture原seed不删、不把cascade数成独立bugs |
| 原剩余families | 每条完整功能/实际证据/提交/原gate都满足 | types/query/catalog/storage/recovery/operations等仍OPEN |
| 总账闭合 | 每项状态/checkbox/证据/commit范围一致且完整验收 | 22complete/166partial/70unverified/15deferred；require-complete仍必须拒绝 |

旧必要恢复timeout保持红；alias原Append两轮绿不等于所有延迟闭合。
UPDATE纯prepare EXPLAIN零effects不冒实际plan consumer。新D3/schemaB旧reader拒绝，
旧模糊default来源不猜测/不自动迁移。无push、Actions启用、安全/TDE跳过项重启。

## 2026-10-07 历史完整目标计划（d4cad）

source `d4cad5b3`：667native/341registered/58TU，累计49项source/test独立commit。
新DOMAIN facts、map peer、array oracle/runtime四项分别commit，完整proof/映射见integration。

| 阶段 | 完整验收要求 | 当前实际状态与下一动作 |
| --- | --- | --- |
| 新组合公共头 | 当前全58正常O2/repeat/source/head/flags/object/stamp/freeze | 62553实际0无donors/SHA cf766684；82native74573/42whole59900/原full93414已启动/live |
| CPP-only DOMAIN整合 | 保留ALTER source identity并匹配当前callback头/全部旧对象证据 | a535正常soleDDL+57 proven b69b对象76324实际0；16native0、11wire7pass/4fail原样保留 |
| 原组合门禁 | 各revision对应全部原SQL/assert/期限 | b69b58/76native0，32wire23pass/9fail（DELETE原红+八入口timeout）/full97347 live；9772原69native0、28wire23pass/5timeout |
| 原full终态 | 每条失败原样留存并与当前修复对应复核 | d2 full96468实际1，589native/247registered pass、1/65fail；其余七旧full逐个live，无全量PASS |
| 独立继续修复 | 真实producer/consumer与所有原seed、NULL/OID/owner/effects | map close134与cleanpeer backup134、实际DELETE RETURNING、DML EXPLAIN分别owner继续 |
| 全部原families | 每条273完整功能/强证据/提交，不以局部兼容替代 | 原namespace/catalog/types/arraygrammar/query/storage/recovery/operations等继续OPEN |
| 总账完成审计 | checkbox/状态/证据/commit范围与逐条原需求全部一致 | 22complete/166partial/70unverified/15deferred，require-complete仍拒绝 |

源码修复和错误oracle独立提交，原红/trace/不trace/deadline不删除。私有O0、
单CPP O2或局部SAN不当最新完整source gate。不push、Actions启用或重开用户deferred专项。

## 2026-10-07 历史完整目标计划（a1b7）

sourcea1b7：668native/341registered/58TU，50项source/test分别commit。

| 阶段 | 完整验收 | 当前证据与下一动作 |
| --- | --- | --- |
| 当前完整优化编译 | exacta1b7全部58/head/flags/objects/stamp/repeat/freeze一致 | 92141 fresh正常O2 live无donors；83native/42whole/原full668341/强pwrite fault准备未启动 |
| 前一组合终态 | 原SQL/assert/时间期限，失败不覆写 | d4cad58正常0，82native/42wire/full各live；a53516native0/whole11为7pass4fail；b69b76native0/whole32为23pass9fail |
| 完整namespace owner | cold/warm DROP、dependency/catalog/存储、原所有路径一致 | 新强baseline17assert真1，保旧compile/test作者错误；须真正修复loaded stale及public RESTRICT/CASCADE，不能只改declaration fallback |
| DML真实消费和资源 | 同一实际plan graph/counters/lifetime/null/owner，原完整wire | DELETE consumer/歧义oracle分别处理；prepared DML foundation因真实未open close provider134暂不合入，实际EXPLAIN仍须实现 |
| Map合作peer与全存储 | 实际合作发布/正确durability/restore/失败边界，不接受任意raw VM | close pending独立已commit并私有强proof0；cleanpeer backup134第三因继续，原raw-edit negatives不弱化 |
| 全273闭合 | 每条原scope/功能/真实proof/commit/全量gate完整 | 22complete/166partial/70unverified/15deferred，完成gate仍拒绝；所有原family继续 |

无push、Actions启用、user-deferred安全/TDE重启。每项真实源码/错误oracle分别commit；
正常省略fault宏的普通native绿不冒真正pwrite注入证明，最新whole/full无提前PASS。

## 2026-10-07 当前完整目标计划（8f69；前文均历史）

source8f69：669native/341registered/58TU，52项独立source/test commit。

| 阶段 | 完整验收要求 | 实际证据与下一动作 |
| --- | --- | --- |
| 最新公共API epoch | 全58当前DML/Operator/PCE/map/array/default头和flags/source/对象一致 | exact8f69 fresh正常O2 61223 live无donors；84native/42whole/full/真fault准备未启动 |
| 最新primitive资源边界 | 同一个真实graph/counter和provider关闭、无二次effects | 两项分别commit；原未open close134保留、强native/七相邻/七whole/scopedSAN0，不借私有绿当Root完整证明 |
| 真正SQL EXPLAIN消费 | 原33×4及额外default/descriptor/Parse+Describe/readonly阶段强控 | 前端仍SELECT-only，consumer独立继续；plain metadata不当ANALYZE真实执行 |
| 其它真实DML消费 | DELETE/UPDATE FROM完整AST/source/namespace/NULL/owner和原矩阵 | 原歧义SQL保42702/no-effects负控、限定target正控；oracle/runtime/UPDATE carrier分别提交，LEFT/CURRENT未实现仍OPEN |
| namespace与map owner | 全真实DROP/依赖/catalog/physical及合作durability/restore/fault | nativeDROP原强17assert基线红继续；cleanpeer backup第三因原134、raw negatives不弱化 |
| 所有原scope与总账 | 每条原273完整功能/proof/commit/gate齐全 | 22complete/166partial/70unverified/15deferred不变，完成gate仍必须拒绝 |

每个源码/错误oracle独立commit；不push，不启用Actions，不重开用户跳过安全/TDE。

2026-10-06 最新全量检查点：冻结 ROOT3bc45e3b/文档fa63 的 canonical75090 仍 live；517 原生已实际跑完，515 PASS / 2 FAIL（group_collection_aggregate、grouping_expression_metadata：原 checked-result 22012 断言遇到 uncaught DbError），不是完整通过。协议阶段原 postgres_protocol_test 已在 line2433 quantified EXPLAIN 失败；新同冻结 server 对照确认带空格 SELECT ... > ANY (...) 正常返回3/4，带空格 EXPLAIN 实际42703(column select)，compact ANY(...) SELECT也42703，不是 plan-node 名称夹具问题。原旧 settings/lock/startup 失败仍保留，未因更早失败未执行就称修好。checked SQLSTATE/result/cleanup 契约、quoted UPDATE/RETURNING canonical 身份、duplicate DEFAULT/FROM/RETURNING priority 分别在独立 private 分支验证；quoted修复3fbdb4e9已独立本地commit，25native/12wire及最终PG17.2专测实际全通过；master source/header/tests/registry 不改，等 full 真正终态后才逐项合入。总账273仍22complete/166partial/70unverified/15deferred，目标未完成；不 push、不启用 Actions、不恢复用户跳过的安全/TDE专项。以下历史记录不冒充当前完整证据。

2026-10-06 新组合真实验证：ROOT3bc45e3b新增独立typed UPDATE提交（private c09f9d5b：24native/11wire/actual PG17.2 matrix均0）；合并前efc全部57正式O2编译86360 terminal0/57 entries及repeat+签名stamp通过，合入后只重编DML CPP83522并复核全部57匹配。新冻结09121ee8的81fresh matching native81137已全部通过；68wire53215已terminal1=67/68通过（1项初始connect errno103/SQL前，原因未明保留日志并captured复跑），新完整517native/257registered runner75090仍live，源/头/测试/registry冻结，不能提前宣全量通过。精确证据见 `docs/issue-typed-query-explain-update-combination.md`；旧完整40539为5native/2wire失败含TLS skip，不覆盖成绿。新增duplicate UPDATE target真实native134/wire12assert red及PG17 priority reference正在独立修，interval parser4afb/querybegin2f85尚只private READY，ordinary scalar/EXPLAINsort/readI/O/完整族继续。总账273仍22complete166partial70unverified15deferred，完成gate仍拒绝；只本地commit，不push/启用Actions/重启用户跳过专项。下方是各历史时点记录。

2026-10-06 当前证据更新：canonical40539已terminal1，原501native=496pass/5fail、244registered E2E标签=242PASSED/2fail（含1个TLS stub intentional skip，非TLS runtime通过）；完整失败及18项独立ROOT commit映射见 `docs/issue-full-registered-canonical-1e0a8c5a.md`。nullable INSERT、binary geometry/BIGINT fixtures、query异常锁释放、CAST/arithmetic SQLSTATE、interval输入/运算边界、LIMIT0、aggregate roots/descriptor、prepared carrier/WITH、decoded API literals、RAISE、array metadata及typed EXPLAIN均已逐项本地提交；最后ROOT source efc501b9开始全部57TU正式O2 fresh构建86360，源/头/测试/registry冻结，新516native/255入口尚未运行，不沿用private/旧组合绿。typed UPDATE final24native/11wire仍在验证；ordinary scalar WHERE/ORDER、EXPLAIN sort SubLink、lock begin55P03、readonly I/O及interval storage仍继续。总账273=22complete166partial70unverified15deferred未完成；不push、Actions禁用、用户跳过项deferred。下方旧条目保留各历史时点及对应旧revision，不表示当前仍live。

2026-10-06 canonical runner40539仍live，原501native/244registered入口输入冻结；已实际发现四个native失败。遗漏NULL INSERT真实修复229b2b19（private 11native+wire通过）、BIGINT overflow fixture da8bb245及binary SP-GiST fixture2575ba98（均原ROOT production上通过）已逐项private commit、尚未合入master。partial_index另有原ROOT新native19506 exit134，API scalar RHS字面值身份修复在fresh56正式O2验证；typed UPDATE/aggregate/EXPLAIN/共享57TU carrier继续。见 `docs/issue-full-registered-canonical-1e0a8c5a.md`，无完整gate/全族完成声明，总账仍273=22complete166partial70unverified15deferred，不push、Actions禁用、用户跳过项deferred。

2026-10-06 新增六项独立本地 source/test commit：`272bc46f` reached-query namespace/typed parameters、`2a2a8d62` canonical quoted range scope、`3983dccb` aggregate ORDER role、`49a3416c` typed EXTRACT、`2ec07854` Describe character typmods、`39844448` live-AST quoted Describe type/attribute identity。ROOT272 fresh56正式O2/repeat/签名stamp及43binder+2sequence controls72137 exit0；后四CPP-only组合正式O2/56签名及50wire53287 exit0。最新ROOT39844448共享headerfresh56正式O2 build93903/repeat/56签名stamp全部exit0，冻结0204 binary的新47fresh native72750、51focused wire47695均terminal0；canonical全部501native/244registered protocol/E2E runner40539正在执行，无完整通过声明。旧f30完整protocol初始connect失败保留，带日志复跑96855后来pg_settings断言失败；新398 full4056原10秒/15秒期限亦terminal1，实际捕获startup321ms下pg_settings57014。fresh九查询22323复现123ms三次cancel、每个只读pg_settings仍264KB/8write syscalls，I/O/事务准备开销继续诊断，不放宽timeout/断言。原clause13344仍精确五项subquery/EXPLAIN/typedUPDATE红、四类aggregate arithmetic、schema-qualified routine CREATE及总清单其他任务继续；见 `docs/issue-prepared-and-projection-combination.md`，不以六项修复及定向绿替代全族实现。总账273仍22 complete、166 partial、70 unverified、15 deferred_by_user；不push、Actions禁用、用户跳过项deferred。

2026-10-06 函数ORDER、routine metadata namespace与普通table arithmetic三项独立本地提交：`98b70e2f`、`3e7ee5f4`、`a00d147a`。真实不执行/重复执行、NULL identity碰撞、pure metadata INTEGER/BIGINT碰撞及普通算术精度/溢出/除零旧红保留，private各专项/相邻验证通过且development/局部O2/sanitizer边界明确；新ROOT a00因header/layout变更fresh55正式O2 build3991、normal repeat及55/55签名/binary stamp均exit0，冻结f30d binary的新44native/44focused protocol均exit0；原诊断精确5red（两项ORDER修复、原控制未删），此版完整默认protocol56926原10秒timeout已terminal1：初始connect errno103、SQL前失败，server输出丢弃故原因未明；不是完整绿，前版b418的41native/38focused/full-default绿不算新组合证明。另原typed_group_key gate旧b418 terminal0、新f30 COUNT42883 terminal1，已确认新aggregate ORDER回归。Quoted source/range三真实红、Prepared Describe OID23/20错误、普通EXTRACT、四类原有aggregate空输入/NULL/filter/cast错误，以及subquery/EXPLAIN/typedUPDATE/完整metadata binder仍继续逐项修复。见 `docs/issue-order-metadata-arithmetic-combination.md`。全部对应family保持partial；总账273仍22 complete、166 partial、70 unverified、15 deferred_by_user。未push、Actions禁用、用户跳过项deferred。

2026-10-06 两项独立修复已本地提交：`5de9c73d` 按完整 AST/operator 推导静态算术结果与协议类型，`666bf0d1` 按 canonical 列身份绑定 quoted/unquoted row values。真实旧 native/wire 红保留：BOOL root 被标成 REAL/OID700（应16），以及 `"F"+f` 得2（应3）。private 专项和相邻验证通过，局部O2/局部sanitizer边界明确；新 ROOT666bf 组合 fresh55 正式O2 build31026、normal repeat及55/55签名/binary stamp已exit0，冻结b418 binary的38focused protocol exit0；原41native组40/41、timezone缺列fixture失败保留，实际PG17核实后独立test修正26d4ea6d保留42883并新增42703，fresh完整41native重跑exit0；默认完整protocol14476原10秒timeout亦exit0（共39protocol入口），已知诊断仍7red，不能沿用前一86dd冻结binary的通过结果。普通 table EXTRACT 空值旧红、legacy arithmetic桥、ORDER/subquery/EXPLAIN/typedUPDATE及完整 procedural metadata binder继续独立修复；所有对应family仍partial。见 `docs/issue-static-arithmetic-and-quoted-row-identity.md`。总账273仍22 complete、166 partial、70 unverified、15 deferred_by_user；不push，Actions禁用，用户跳过项保持deferred。

2026-10-06 新增九项独立ROOT修复：`8fc7eeda` actual-engine事务归属、`e98ce1a1` JOIN COLLATE、`53d33158` EXTRACT语法角色、`b661c7ec` 数值类型宽度、`7063fa38` 整数溢出、`a0da0a77` 浮点精度、`adef1689` SUM/AVG42725、`c78ea073` INTO执行需求、`86ddccf9` indexed residual一次执行。各项真实旧红和中间失败保留、断言未降低；receiver最终private正式O2十native及42控制+8相邻协议通过，index最终private正式O2十八native/九协议通过，其他private证据明确development/局部O2边界。新ROOT组合因header/API变化已完成fresh55正式O2 build96406、normal repeat和55/55签名/binary stamp；39fresh matching native与36protocol均exit0；保留clause诊断仍7red，同一冻结a38完整默认protocol80673按原10秒timeout exit0（37protocol entrypoints=36专项+完整1），不冒称全gate或整个数据库完成。静态projection OID、旧table arith桥、ORDER/subquery/EXPLAIN/typedDML及完整metadata binder继续；I/O写放大未修。详见 `docs/issue-engine-owner-receiver-numeric-followups.md`。总账273仍22 complete、166 partial、70 unverified、15 deferred_by_user；不push，Actions禁用，用户跳过项保持deferred。

2026-10-06 WHERE/index/null-safe JOIN 三项独立提交 `aa149ca8`、`d184d8a1`、`6460a246` 已完成当前组合的正式 O2 构建及55/55签名复核：30个专项/相邻protocol exit0，24项fresh matching native中23项通过、constraint_expr的SUM('abc') 42725断言失败（整组exit1）。原已知函数诊断仍7项red。额外真实native16308复现indexed residual重复副作用：一个匹配行调用写函数两次；独立修复进行中，未修改原一次调用断言，不能把简单index绿扩展为通用安全。上一index-only完整默认protocol仍在prepared ALTER处timeout，此646组合未重跑完整gate；I/O写放大、binder、INTO demand及其他族仍未完成。详见 `docs/issue-where-index-null-safe-join-combination.md`。总账273仍22 complete、166 partial、70 unverified、15 deferred_by_user；不push，Actions禁用，用户跳过项保持deferred。

2026-10-06 五项独立修复已组合提交：`711f9030` COLLATE标签、`0888873f` typed AT TIME ZONE、`d2b7b566` DISTINCT值角色、`b2c4d983` source-query完整null-safe比较与BOOL类型、`5411a7aa` 存储函数写入属于calling statement及CID/volatility/host/internal visibility边界。真实旧红与中间失败保留；正式O2重编5个变更CPP，其余50按当前签名匹配，normal/repeat build、55/55对象与binary stamp通过。最终14项fresh matching native及26个专项/相邻协议exit0；同一正式binary已知函数WHERE/ORDER/subquery/EXPLAIN/typed-DML诊断9项全red exit1。新完整默认protocol2374处idx_scan/idx_tup_fetch为0、exit1（非timeout）：filterRows任何active事务都禁用index，新implicit query owner使孤立read也走heap，独立snapshot-safe索引路径修复继续；不得改计数或削弱断言。此前四次完整timeout及statement-image写放大未修。另实际PG17.2/冻结候选18 INTO+4普通控制复现过度执行VOLATILE投影/错误优先级，独立output demand修复继续。详见 `docs/issue-plpgsql-lexical-and-source-comparisons.md`、`docs/issue-stored-function-statement-atomicity.md`、`docs/issue-plpgsql-select-into-execution-demand.md`。完整query准备/binder、null-safe JOIN ON和完整routine族未完成；总账273仍22 complete、166 partial、70 unverified、15 deferred_by_user。无全注册suite/TLS runtime/PG18.6 differential声明；未push、Actions保持disabled、用户跳过项保持deferred。

2026-10-06 PL scalar/native follow-ups 两项独立source/test commits：`b6683521` 修复真实native SELECT/WHERE/ORDER/INTO表达式错误从22012/22P02/22003被吞为XX000；`8cd860e7` 用canonical AST位置绑定修复quoted "X"/x变量值/NULL/type混淆，保留BIGINT宽度、session特殊值、EXTRACT语法和显式trigger binding，并拒绝不存在的变量/qualifier/function。旧native/wire实际红、独立development新storage/stubs的3native及2protocol绿均有记录。ROOT正式O2只重编变更storage对象，其他54对象严格复核匹配，normal/repeat build、55/55签名及binary stamp通过；最终matching O2的6native及7项专项/相邻protocol通过。完整默认协议本follow-up未重跑；前一615/7bc组合在main3170 INSERT kw_joined timeout exit1，之前三次完整超时也保留，不能宣称完整protocol/suite通过。6项ledger unit/3项文档版本兼容检查通过，require-complete仍exit1。见 `docs/issue-plpgsql-scalar-binding-and-native-errors.md`。query内variable/source-column歧义、函数错误后写入不原子及statement-image写放大仍独立未完成；SQL-04/FUNC-05/06仍partial。总账273保持22 complete、166 partial、70 unverified、15 deferred_by_user；未push、Actions禁用、用户安全/TDE跳过项保持deferred。

2026-10-06 cold recovery / PL/pgSQL 两项独立修复已提交：`615f2c54` 修复真实 SIGKILL 后全新 exec 访问未初始化 database-lock map 的冷启动崩溃；`7bc76204` 保留完整 SELECT INTO 查询、structured typed first-row/NULL、STRICT/FOUND、声明/赋值/返回类型与错误码、stored CTE scope 及 native checked fallback。独立 fixture commit `acaa0bea` 用真实表达式 evaluator 替换旧 native echo host，声明 runaway counter 并精确断言未降低的步数上限；旧 O0 exit137 与 O2 RSS约53.5GiB后主动TERM的失败均保留，纠正fixture后O0 1.39秒/15420KiB通过。最终 development 7项专项/相邻协议及3项native通过；合并正式优化版55对象重编、normal/repeat build、55/55签名及binary stamp通过，10项专项/相邻协议及6项matching native通过。完整默认协议本轮在main3170 INSERT kw_joined处socket timeout exit1；此前两次JOIN完整超时与第三次instrumented CREATE TABLE超时也保留，snapshot写放大未修，不宣称全协议通过。6项ledger unit及3项文档/版本/兼容检查通过；全注册suite/TLS runtime/PG18.6 differential未跑。见 `docs/issue-cold-start-transaction-lock-registry.md` 和 `docs/issue-plpgsql-select-into-typed-query.md`。quoted scalar X/x复合绑定错值与函数P0002/22P02后写CTE仍落盘已独立复现，正在分别修复；FUNC-05/06、SQL-04、WAL-05/08、TXN-05/07仍partial。总账273保持22 complete、166 partial、70 unverified、15 deferred_by_user；未push、Actions禁用、用户跳过安全/TDE项保持deferred。

2026-10-06 repeated-source JOIN source/test commit `d3fcfc0b`：旧优化版自连接返回重复对角组合，WHERE a.id=1 AND b.id=2 错误为空。五种 native JOIN 与 SQL dispatcher 现分别绑定每个 FROM occurrence，单遍词法限定符绑定、injective encoded column keys、projection/WHERE/ON/aggregate/shared stage keys 保留物理扫描锁及 NULL identity；重排限定星号、带空格/双引号/点号列名及 literal/internal-key collision 均有回归。开发及正式优化版各13个专项/相邻 E2E、2个 matching native通过；55个生产对象正式重编、normal/repeat build及全对象签名检查通过。完整默认协议首轮在 prepared boundary 的 ALTER TABLE 处 socket timeout；独立10秒限时完整 boundary通过（ALTER 0.787秒），随后串行完整协议又在 ON COMMIT DELETE ROWS 临时表 INSERT 处 timeout。两次完整失败均保留且原因未定，不宣称完整协议通过；6项ledger unit/3项文档版本兼容声明检查通过。见 `docs/issue-join-range-identity.md`。总账273仍22 complete、166 partial、70 unverified、15 deferred_by_user；SQL-04、QRY-01/02/03/05仍partial，PL/pgSQL SELECT INTO 分割及 no-row NULL bitmap已复现待修；全注册suite/PG18.6 differential未跑，未push，Actions禁用，用户跳过项保持deferred。

2026-10-06 duplicate CTE name source/test commit `b6856ad4`：旧协议和 native parser 都接受同层重复 CTE，隔离库中两个同名 INSERT CTE 还会分别落盘。共享 lexer-based preflight 现按引号/大小写规范化同层名字，独立检查嵌套 WITH 列表，并在 native parse、执行入口和 Extended Parse 发布对象前拒绝 42712；不是执行到第二次绑定时才拒绝。开发8个专项/相邻协议与2个 native 通过；55个 production objects 全部按共享正式优化配置重编且签名复查一致，正常 build/link 与重复 up-to-date 检查通过。最终优化组合的8个专项/相邻 E2E与完整默认协议均通过（共9 entry points，plain TCP非TLS），matching production objects重链的2个 native亦通过；新增25P02失败块优先级、保存点恢复和 batch rollback 控制通过，6项 ledger unit/3项文档版本兼容声明检查通过。见 `docs/issue-cte-duplicate-name-preflight.md`。全注册suite/PG18.6 differential未跑；PG17.2只读诊断不是18.6证据。SQL-01/04、QRY-05、PROTO-02/03仍partial；多行同源JOIN、SELECT INTO及完整CTE snapshot/visibility/SEARCH/CYCLE/materialization仍待修。总账273保持22 complete、166 partial、70 unverified、15 deferred_by_user；未push，Actions禁用，用户跳过项保持deferred。

2026-10-06 CTE/词法 follow-up 四项独立 source/test commits：`8dd62c09` 用逐层关系绑定替代全局 CTE 名替换并区分 JOIN 逻辑/物理键；`b0487280` 修复 compact FROM 因子与 CTE AS( 边界；`77849804` 修复 comment/dollar-RHS WHERE 静默丢失过滤；`5163f0b5` 独立执行已存储 view 后应用外层 typed 查询，阻断 caller CTE 污染且不重复写入 CTE。四个专项及九个相邻开发 E2E 通过；优化构建与重复 up-to-date 检查通过，正式22个专项/相邻 E2E通过，开发完整默认协议在 ON COMMIT DELETE ROWS 临时表 INSERT 处 socket timeout（保留失败），优化完整默认协议随后 exit 0，验证 plaintext negotiation、startup/auth/simple/extended 和 error recovery/ReadyForQuery（非 TLS 验证），最终共23个 protocol/E2E entry points通过，非全注册套件。六项 ledger unit 和文档/版本/兼容声明检查通过。中间 JOIN 1/1、负向 XX000、四次并发 setup timeout、view 回归及独立 PL/pgSQL SELECT INTO 失败均保留。详见 `docs/issue-cte-relation-scope-and-lexical-boundaries.md`。重复 CTE 名静默接受、多行同源 CTE 自连接错误、SELECT INTO 参数分割等已复现待修；全注册套件/native suite/PG18.6 differential 未跑。SQL-01/04、CAT-16、QRY-01/02/03/05 仍 partial；总账273保持22 complete、166 partial、70 unverified、15 deferred_by_user，require-complete 实测 exit 1。未 push，Actions保持关闭，用户跳过项不重启。

2026-10-06 CTE/derived/LATERAL composition 五项独立修复已提交：`98f9c978` 避免活动物化表/用户同名表冲突；`514b4caf` 按查询深度和 AST 界定 WHERE 函数预检查；`f2feaaea` 保留派生表别名与 CTE 限定星号；`4b2ff4f6` 跳过未填充物化视图占用的内部候选名；`5928ff7a` 将类型分析异常纳入协议边界并读取未填充视图的列元数据。最终优化构建与重复 up-to-date 检查通过，16 项专项/相邻/完整默认协议 E2E 通过；额外 DIV-14 两次因 fixture 遗留 1234ms 超时失败，独立 test commit `ff693582` 验证并恢复该设置后完整重跑通过，原失败记录保留（共17个最终通过的 E2E entry points）。六项 ledger unit 与三项文档/版本/兼容声明检查通过；未跑全注册套件、新 native suite 或 PG18.6 differential。详见 `docs/issue-materialized-query-composition.md`。仍有 CTE 名与列/输出别名同名的全局替换错误、compact FROM 因子边界和普通 comment/dollar-RHS WHERE 过滤问题，SQL-04/QRY-01/02/03 仍 partial。总账273：22 complete、166 partial、70 unverified、15 deferred_by_user；不 push，Actions文件保持禁用，用户跳过项仍 deferred。

2026-10-06 SQL-01/DML-03 follow-up source/test commit `9357d433`：两侧来源可解析但普通 JOIN 缺少 ON/USING 时，旧 AST 仍有效，隔离库实测 UPDATE 2 改值、DELETE 2 清空。现在 UPDATE/DELETE 发布 AST 前递归检查来源 JOIN 条件；CROSS/NATURAL 保留隐式连接语义，普通 JOIN 必须有 ON 或非空 USING。旧生产 parser object 的新增 native case 在 `FROM a JOIN b` 上失败；正式构建、与新生产 parser object 链接的 missing-source/P1 原生测试、UPDATE/DELETE 来源和 DML CTE 协议 E2E 通过。包括外层 CROSS 中隐藏的无条件 JOIN，逐错误验证 42601、无成功 command tag、全部目标行不变。此前 dml_semantics 原生套件在 ea7/208 组合通过，本 follow-up 未重跑该整项或完整注册套件/PG18.6 differential。完整语法、表达式分析及复杂来源仍缺，SQL-01/DML-03 仍 partial；总账273保持22 complete、166 partial、70 unverified、15 deferred_by_user。不 push。

2026-10-06 QRY-02/QRY-03 source/test commit `208f34d8`：FROM 的两处解析现保留 NATURAL INNER/LEFT/RIGHT/FULL 类型及可选 OUTER，LATERAL 左输入按实际保留侧/合并键映射。旧 NATURAL LEFT 输入返回 42601，旧解析器的新增 AST 类型断言失败；重建后的 parser_phase1 原生测试、正式构建、derived-type、join-type、SQL-literal 与 MERGE 协议 E2E 通过。覆盖 INNER、LEFT/RIGHT 保留行、FULL 的左右键独立 NULL 与裸键 coalesce、无公共列且一侧为空的 outer 形式及 typed 空输出；另验 UPDATE/DELETE 的 AST 类型、MERGE 外层 ON 保留与非法 NATURAL 语法控制。完整套件与 PG18.6 differential 未跑，任意 scope 和 LATERAL 外层裸列/star 等仍缺，QRY-02/QRY-03 保持 partial。

2026-10-06 SQL-01/DML-03 独立 source/test commit `ea7e46d9`：缺失/解析失败的 FROM/USING 来源曾被丢弃，语句退化为 target-only DML；隔离库旧版本的 `UPDATE ... FROM;` 实测 UPDATE 2 并改值，`DELETE ... USING;` 实测 DELETE 2 并清空。现拒绝发布缺失来源的 AST，DML bridge 显式声明 42601；第一次候选拒绝写入但 wire 为 XX000 的结果亦已修正。dml_missing_source_parser 原生、UPDATE/DELETE 来源协议与 DML CTE 协议、正式构建通过，协议逐错误验证全部目标行和值不变、无成功 command tag、后续合法 DML 正常。见 `docs/issue-dml-03-missing-source-parser.md`。完整 FROM grammar、任意 DML join tree、全套件和 PG18.6 differential 仍缺；SQL-01/DML-03 保持 partial。总账273仍22 complete、166 partial、70 unverified、15 deferred_by_user；不 push，Actions关闭，用户跳过项不重启。

2026-10-06 QRY-03 source/test commit `b42e4405`：普通 JOIN 的右表不写别名时，FROM-chain reader 原把 `USING` 消耗成隐式别名，导致合法 `JOIN table USING (id)` 返回 42601；现将 USING 保留为连接关键字。新增无别名 INNER、FULL、连续 USING 和后接 LATERAL 的协议回归，验证合并列顺序、结果行、列名、整数 OID 与 command tag；修复前 join-type E2E 在新增无别名用例实测失败 42601，修复后正式构建、join-type、derived-type、multijoin E2E 全通过。完整套件和 PG18.6 differential 未跑。外层 LATERAL 裸列/star、NATURAL outer 输入等仍待修复，QRY-02/QRY-03 保持 partial；总账273：22 complete、166 partial、70 unverified、15 deferred_by_user。不 push，Actions保持关闭。

2026-10-06 QRY-02/QRY-03 source/test commit `069889d3`：LATERAL 左输入现接受简单 base-table USING 和普通 NATURAL JOIN。物化查询显式保留每个关系的叶列，另按 JOIN 可见输出构造裸列映射；INNER/LEFT 取左键、RIGHT 取右键、FULL 对来源键按 NULL 位图 coalesce，限定的左右键保持各自的 NULL。旧左输入 USING 返回 42601；允许该语法后，裸相关 `id` 曾返回 42703。正式构建、derived-type、SQL-literal 与 multijoin E2E 通过；覆盖复合 USING/NATURAL、RIGHT/FULL 保留行、NULL 键、连续 FULL USING、裸合并键 LATERAL ON、空左输入、局部列优先级和 USING 后 CROSS 同名列的合法限定引用。完整套件及 PG18.6 differential 未跑。新无别名普通 USING 回归仍失败 42601，正单独修复；外层裸 `SELECT id` 仍 42703，`SELECT *` 暴露内部列/重复键，NATURAL outer + LATERAL 仍 42601。QRY-02/QRY-03 保持 partial，总账273：22 complete、166 partial、70 unverified、15 deferred_by_user，不 push。

2026-10-06 QRY-02/QRY-03 补验外层 LEFT JOIN 树与 `LEFT JOIN LATERAL ... ON` 组合，test-only commit `710671ef`：左树中的右侧行先作 NULL extension，随后 lateral `ON true` 仍返回 `id=6`、右侧 `label=NULL` 和 `next_id=7`；`python3 -m py_compile tests/derived_type_protocol_e2e_test.py` 与 `tests/derived_type_protocol_e2e_test.py` 通过。无源码改动；完整套件和 PostgreSQL 18.6 differential 未运行，总账状态不变，不 push。

2026-10-05 QRY-03 修复普通 `FULL JOIN` 省略 `OUTER` 时错误路由，source/test commit `63b772e2`：`FULL JOIN` 原未被单表对 JOIN 分派器识别，误走 INNER 分支并以 `42P01` 报 missing FROM-clause；现同时识别 `FULL JOIN` 与 `FULL OUTER JOIN`，并按实际 `JOIN` token 定位右表起始处，不再依赖固定关键字长度。新增对无匹配左行及右行的协议回归；正式构建、`tests/join_type_protocol_e2e_test.py`、`tests/derived_type_protocol_e2e_test.py` 和 `tests/sql_literal_preservation_e2e_test.py` 通过。完整套件、PG18.6 differential 与其它 FULL JOIN 复杂语法仍未验；QRY-03 仍 partial，总账 273：22 complete、166 partial、70 unverified、15 deferred_by_user，不 push。

2026-10-05 QRY-02/QRY-03 扩展 LATERAL 左输入到简单 base-table JOIN tree，source/test commit `cf317cd1`：`LEFT/RIGHT/FULL OUTER/INNER JOIN ... ON` 结果后接 LATERAL 原以 `42601` FROM syntax error 失败；现对简单叶子 base tables 解析别名/列映射，用完整查询执行器物化左侧结构化 rows 与 NULL bitmap，再逐行执行 LATERAL。协议回归覆盖 INNER 匹配 fan-out、LEFT 未匹配 NULL extension、RIGHT/FULL OUTER 的保留行、空左输入的 typed RowDescription；构建、derived-type、SQL-literal、join-type 和 multijoin E2E 均通过。USING/NATURAL 合并列映射、复杂/未知 scope、完整套件与 PostgreSQL 18.6 differential 未闭合；QRY-02/QRY-03 仍 partial，总账不变，不 push。

2026-10-05 QRY-02/QRY-03 补验 LATERAL 的 mixed-case quoted outer column，回归测试 commit `1a2e42b3`：在先前 alias 修复 `2080a994` 的基础上，新增 `"OuterAlias"."MixedId"` 同时出现在 LATERAL 标量 target 与外层 WHERE 的真实协议查询，验证值、列名和 INT OID；`python3 -m py_compile tests/derived_type_protocol_e2e_test.py` 与 `tests/derived_type_protocol_e2e_test.py` 通过。没有新增源码改动；仅覆盖该简单 quoted alias/column 路径，复杂 schema-qualified/nested scope、全套件与 PG18.6 differential 仍未验证，QRY-02/QRY-03 仍 partial，不 push。

2026-10-05 QRY-02/QRY-03 支持简单 LATERAL 左输入的双引号关系别名绑定，source/test commit `2080a994`：`FROM lateral_ident_left AS "OuterAlias" CROSS JOIN LATERAL (SELECT "OuterAlias".id + 1 ...)` 修复前以 `42P01` 报 missing FROM-clause entry，因为旧替换器跳过了双引号标识符；现按 SQL 标识符路径扫描并替换相关限定列，同时跳过字符串和注释。协议回归覆盖带引号的外层别名及结果 OID；`bash scripts/build.sh`、`tests/derived_type_protocol_e2e_test.py`、`tests/sql_literal_preservation_e2e_test.py` 通过。仅验证简单 quoted relation alias + unquoted column；quoted column、任意 schema-qualified/嵌套 LATERAL scope、完整套件和 PostgreSQL 18.6 differential 未验证；QRY-02/QRY-03 仍 partial；总账 273：22 complete、166 partial、70 unverified、15 deferred_by_user，不 push。

2026-10-05 QRY-02/QRY-03 修复连续简单 LATERAL 子查询导致的后端 100% CPU 循环及前一层合成列覆盖，source/test commit `6123c5b3`：`processDerivedTables()` 扫描第二个相邻 LATERAL 时反复重访同一括号位置；搜索游标现对每个跳过的子查询单调前进，普通 derived table 替换后重置扫描。链式物化时将前一临时表合成列映射到新组合 schema，避免 `x.first` 被误解析为第二层的 right 输出。修复前两步查询在协议超时前持续占满 CPU，单独推进游标后又观测到 `x.first=3`（应为2）；新增两行链式标量回归验证 `1→2→3`、`2→3→4`。`bash scripts/build.sh`、`tests/derived_type_protocol_e2e_test.py`、`tests/sql_literal_preservation_e2e_test.py` 通过。仅验证简单两步 FROM-less scalar chain；复杂相关链、完整套件和 PostgreSQL 18.6 differential 未完成，QRY-02/QRY-03 仍 partial；总账 273：22 complete、166 partial、70 unverified、15 deferred_by_user，不 push。

2026-10-05 QRY-02/QRY-03 扩展 LATERAL 的左输入为多个简单 base-table CROSS/comma 关系，source/test commit `e7a9d086`：此前两个 CROSS 输入后接 LATERAL 的测试以 `42601` 解析失败；现逐行构造左侧 CROSS 组合，对每个关系的限定列逐一绑定，裸相关列只在组合 schema 中唯一时绑定，并可在 INNER/LEFT LATERAL `ON` 中评估多个左关系引用。回归覆盖 CROSS、comma 和引用双方关系的 ON predicate；`bash scripts/build.sh`、`tests/derived_type_protocol_e2e_test.py`、`tests/sql_literal_preservation_e2e_test.py` 通过。非 CROSS 左侧 JOIN tree、table function、lateral chain、完整套件和 PostgreSQL 18.6 differential 未完成；QRY-02/QRY-03 仍 partial；总账 273：22 complete、166 partial、70 unverified、15 deferred_by_user，不 push。

2026-10-05 QRY-02/QRY-03 扩展 LATERAL 裸外层引用到带简单本地 base-table FROM 的子查询，source/test commit `e63e4e1e`：原 `SELECT r.label || payload FROM typed_lateral_right r WHERE r.id = l.id AND payload <> 'skip'` 以 `42703` 报 bare payload 不存在；现读取本地 base-table schema，保留局部列优先级，只把本地列集合中不存在且在单一左输入唯一的裸引用按类型替换，从而在 target 和 WHERE 中逐行求值。回归同时验证 `r.id`/`r.label` 仍是内层引用、外层 `payload` 正确绑定、NULL 筛选和过滤结果；CTE、derived/function FROM 等无法可靠确定本地列集合的形态不猜测。`bash scripts/build.sh`、`tests/derived_type_protocol_e2e_test.py`、`tests/sql_literal_preservation_e2e_test.py` 通过；多左关系、未知 FROM scope、quoted outer identifier、任意相关表达式/子查询和 LATERAL chain 仍缺，QRY-02/QRY-03 继续 partial；总账 273：22 complete、166 partial、70 unverified、15 deferred_by_user，不 push。

2026-10-05 QRY-02/QRY-03 补无自身 FROM 的 LATERAL 标量 target/WHERE 裸外层引用，source/test commit `40ef739a`：此前 `id + 10` 和 `WHERE id < 3` 均以 `42P01` 失败；现按表达式 AST 收集唯一裸左列引用，在每个外层行内用类型安全 literal 绑定 target scalar 与 WHERE，并只改写顶层 `AS` 别名，保留 CAST、引用符号和列列表分隔。协议回归覆盖数字算术、文本 CAST/拼接、单引号/空串/文本 `NULL`/SQL NULL、true/false/NULL WHERE 结果，以及全部被过滤时的零行 typed RowDescription；`bash scripts/build.sh`、`tests/derived_type_protocol_e2e_test.py`、`tests/sql_literal_preservation_e2e_test.py` 通过。只支持单一左关系且 lateral SELECT 无自身 FROM；多左关系、quoted outer identifier、任意相关子查询/表达式和 LATERAL chain 仍缺，QRY-02/QRY-03 继续 partial；总账 273：22 complete、166 partial、70 unverified、15 deferred_by_user，不 push。

2026-10-05 QRY-02/QRY-03 补 LATERAL 的布尔 `ON` 条件与 LEFT NULL extension，source/test commit `18fc748b`：原 `LEFT JOIN LATERAL (...) x ON true` 未被识别并以 `42P01` 报 relation missing；现对单一简单左表逐行物化右侧结果，按结构化值、类型和 NULL bitmap 求值 evaluator 支持的 boolean ON，INNER 按条件筛选，LEFT 在没有任何匹配（包括条件为 false/NULL）时生成右侧 SQL NULL。协议回归覆盖 LEFT `ON true` 多匹配、条件过滤、false/NULL preservation、INNER 筛选；`bash scripts/build.sh`、`tests/derived_type_protocol_e2e_test.py`、`tests/sql_literal_preservation_e2e_test.py` 通过。完整多左关系/任意 JOIN tree、LATERAL chain、table function 和更一般相关引用仍缺，QRY-02/QRY-03 继续 partial；总账 273：22 complete、166 partial、70 unverified、15 deferred_by_user，不 push。

2026-10-05 QRY-02/QRY-03 补 LATERAL 的裸外层列投影，source/test commit `c7447f26`：`CROSS JOIN LATERAL (SELECT id AS outer_id) x` 中无限定的 `id` 原返回 `42P01`；当子查询无自身 `FROM` 时，直接列目标现在按唯一左侧列绑定，并用显式类型转换保留来源 OID、文本值、SQL NULL 和空左侧输入的 RowDescription。新增整数、NULL、含单引号文本、空左表协议回归；`bash scripts/build.sh`、`tests/derived_type_protocol_e2e_test.py`、`tests/sql_literal_preservation_e2e_test.py` 与 `git diff --check` 通过。此项不代表完整 LATERAL：表达式/WHERE 中裸外层引用、含内层 FROM 的外层解析、多左侧关系和 table function 等仍未完成，QRY-02/QRY-03 继续 partial；总账 273：22 complete、166 partial、70 unverified、15 deferred_by_user，不 push。

2026-10-05 QRY-03 多表 JOIN 支持 alias-qualified `alias.*` 展开，source commit `d7389c77`、多列协议回归 commit `93951ac3`：LEFT JOIN 链的 `SELECT b.*` 原报 `42703 column "b.*" does not exist`；现在按所限定关系的 schema 原序展开所有列，保留重复列名、类型 OID 与 NULL bitmap，不把 USING 合并输出误当成限定表 star。协议回归验证单列 NULL-extension，以及三列 `id/a_id/label` 顺序、INT/TEXT OID 和实际 NULL；`scripts/build.sh`、`tests/join_type_protocol_e2e_test.py`、`tests/multijoin_e2e_test.py` 通过。完整 row expansion、schema-qualified relation stars 与更一般 join-tree star 语义仍未闭合，QRY-01/QRY-03 仍 partial；总账 273：22 complete、166 partial、70 unverified、15 deferred_by_user。

2026-10-05 QRY-03 多表 JOIN 可按 target list 中已投影的同一表达式排序，source/test commit `cd53df9d`：`SELECT a.id + b.val AS total ... ORDER BY a.id + b.val DESC` 原以 0A000 拒绝；现在按投影表达式的大小写不敏感、trim 后文本匹配，复用已计算的投影值与结果类型排序，不重复求值。三表降序计算表达式 regression、`ORDER BY 1`、别名及 NULLS FIRST 现通过；最终 `scripts/build.sh`、`tests/join_type_protocol_e2e_test.py`、`tests/multijoin_e2e_test.py` 通过。排序表达式必须与 target-list expression 文本对应；任意未投影 ORDER BY expression、任意语法等价表达式、collation/stable tie/top-N 仍未闭合，QRY-03/QRY-10 仍 partial；总账 273：22 complete、166 partial、70 unverified、15 deferred_by_user。

2026-10-05 QRY-03 多表 JOIN 投影支持简单 CASE，source/test commit `de98eddb`：`CASE WHEN a.id = 1 THEN b.val ELSE c.id END` 原以 0A000 拒绝；现在校验 switch/WHEN/THEN/ELSE 所有列引用，在 joined row context 中执行、推断合并结果类型并发布协议 OID。三表 CASE 条件命中/未命中分支、alias、排序与 typed result 已覆盖；CASE 的 E2E 在改前报 0A000，改后 `scripts/build.sh`、`tests/join_type_protocol_e2e_test.py`、`tests/multijoin_e2e_test.py` 通过。复杂/任意 CASE、SRF、聚合/窗口、完整 target list 与 PG18.6 differential 仍未完成，QRY-01/QRY-03 仍 partial；总账 273：22 complete、166 partial、70 unverified、15 deferred_by_user。

2026-10-05 QRY-03 多表 JOIN 投影现可求值 evaluator 支持的标量函数调用，source/test commit `1ef35962`：`abs(a.id - b.val) AS distance` 原被所有 FunctionCallExpr 统一拒绝；现在普通函数参数递归绑定并在每个 join row 的 typed/NULL-aware 上下文中求值，函数别名、结果 OID 与 `ORDER BY 1` 保留；常用 COUNT/SUM 等聚合、窗口及 FILTER/within-call ORDER BY 仍显式拒绝而不按逐行标量误算。`scripts/build.sh`、`tests/join_type_protocol_e2e_test.py`、`tests/multijoin_e2e_test.py` 通过，COUNT(*) 负向控制返回 0A000。CASE、SRF、聚合/窗口、完整 QRY-01/QRY-03 与 PG18.6 differential 仍未完成；总账 273：22 complete、166 partial、70 unverified、15 deferred_by_user。

2026-10-05 QRY-03 支持多表 JOIN 简单标量表达式投影，source/test commit `124bc278`：三表链上的 `a.id + b.val AS total` 原以 `0A000` 被拒绝；现在受限 unary/binary/literal/cast 表达式在 join 与 WHERE 之后基于带类型及 NULL bitmap 的行上下文求值，发布表达式 OID/alias/NULL，`ORDER BY` 输出位置使用实际投影结果排序。协议回归覆盖整数算术、第三表投影、NULL-extended 输入的 NULL 传播、`ORDER BY 1`/`ORDER BY ... NULLS FIRST`；`scripts/build.sh`、`tests/join_type_protocol_e2e_test.py`、`tests/multijoin_e2e_test.py` 通过。函数调用、CASE、SRF、聚合、复杂 target list 与完整 PG18 differential 仍未完成，QRY-01/QRY-03 仍 partial；总账 273：22 complete、166 partial、70 unverified、15 deferred_by_user。

2026-10-05 QRY-03 修复 JOIN 哈希键把值格式误当语义，source/test commit `3fef4f7d`：存储层过去按原始显示文本建 INNER/LEFT/RIGHT hash key，`NUMERIC` 的 1.0 与 1.00 因 key 不同漏掉本应相等的 JOIN 行。现在三种 join path 都用类型化 canonical key 做候选匹配，同时保留行原始显示值；协议回归覆盖 INNER/LEFT/RIGHT/FULL USING 与显式 ON、结果值、列名、OID 和行数，关键 USING 用例在修复前实测为 0 行。`scripts/build.sh`、`tests/join_type_protocol_e2e_test.py`、`tests/multijoin_e2e_test.py` 通过。完整套件及 PG18.6 differential 未跑（reference preflight 仍是 17.2）；未证明所有跨类型、collation 或 timestamp JOIN equality，QRY-03 仍 partial。总账 273：22 complete、166 partial、70 unverified、15 deferred_by_user。

2026-10-05 QRY-03 增加 `USING`/`NATURAL` join 输出语义，source/test commit `fd79875a`：单 JOIN 和链式 JOIN 现在为 USING 列生成 SQL 合并输出，保留 `SELECT *` 的列顺序、裸 USING 列引用和限定后的原表列引用；复合 USING 键、LEFT/RIGHT/FULL 链的 NULL extension 与 FULL 合并键 coalesce 有协议回归。NATURAL 有公共列时按共同列合并；无公共列时按笛卡尔连接处理，并验证 outer 形式的保留侧。旧实现的单 USING 曾报 missing ON，链式 USING 曾报 42601。`scripts/build.sh`、`tests/join_type_protocol_e2e_test.py`、`tests/multijoin_e2e_test.py` 均通过；完整套件和 PostgreSQL 18.6 differential 未运行（既有 reference preflight 显示 17.2）。Quoted USING identifier、表达式/任意嵌套/lateral 等完整语义仍未实现，QRY-03 与 OPT-02 仍 partial。总账 273：22 complete、166 partial、70 unverified、15 deferred_by_user；不 push，Actions 保持禁用。

2026-10-05 QRY-03/OPT-02 修复多表 JOIN `ON` conjunction 丢行，source commit `856fe079`、RIGHT/FULL 回归 commit `65718c2b`：三表查询 `ON a.id = b.a_id AND b.val = 1` 原被按首个 `=` 切成单一谓词，残余文本被当作列名并静默返回空集。现在分离 top-level `AND`，用一个跨关系等值条件作 hash key，其余受支持比较保留为 storage `ON` filter；inner 重排只在依赖关系已进入输入后应用，outer 链仍按书写顺序把残余条件作用在对应 join 边、在 NULL extension 前求值；其他表达式显式拒绝。`scripts/build.sh`、`tests/join_type_protocol_e2e_test.py`（inner 重排、延后依赖谓词、LEFT/RIGHT/FULL NULL extension）和 `tests/multijoin_e2e_test.py` 通过。QRY-03/OPT-02 仍 partial，完整套件与 PG18.6 differential 未跑。总账 273：22 complete、166 partial、70 unverified、15 deferred_by_user。

2026-10-05 OPT-05 修复 join cost GUC 被小表启发式覆盖，source/test commit `61c8bc57`：`enable_nestloop=off` 时基础代价先把 Nested Loop 置为不可竞争，但统计选择率修正又将其覆盖为有限代价，随后 `<50` 小表捷径无条件选 NLJ。4×3 两表且均已 ANALYZE 的 planner 回归在修复前因拿到 `NestedLoopJoinOp` 而失败；现在统计修正只作用于仍可用的 NLJ，小表捷径也要求 NLJ 仍是候选，因此可用 Hash Join 被选中。`scripts/build_one_test.sh planner_runtime_stats_test`（红/绿）、`scripts/build_one_test.sh cost_model_test`、`scripts/build.sh` 均通过。未跑完整suite或PG18.6 differential。OPT-05 仍 partial：selectivity support、完整类型/operator/collation aware cost、统计失效等仍未实现。总账 273：22 complete、165 partial、71 unverified、15 deferred_by_user。

2026-10-05 OPT-01 审查：`PlanContext` 加 `buildSelectPlan()` 返回单个 `OpPtr` 计划树，没有可比较的 relation/path 集合或参数化路径；`EquivalenceClass`/`PathKey` 只是轻量 struct。pathkey overload 丢弃 `eqClasses`，识别出可排序 index 后仍保留 `SortOp`（当前实现内有明确 safe-fallback `break`），因此只是未生效的接口而不是 PostgreSQL 式 path planner。`tests/eq_class_pathkey_test.cpp` 只在空表上执行、未断言计划节点或实际有序数据，不能证明索引排序或等价类传播。仅审查、无生产改动；OPT-01 从 unverified 调整为 partial，架构重做仍未实现，详见 `docs/issue-opt-01-path-planner-audit.md`。总账 273：22 complete、165 partial、71 unverified、15 deferred_by_user。

2026-10-05 VAC-03 部分修复，source/test commit `09611ff0`：`VACUUM (ANALYZE)` 原先被接受却走普通 vacuum 路径，不生成/刷新统计信息；`FULL` 选项也可能未触发表重写；`FREEZE` 被静默忽略，事务块内执行亦未拒绝。现在解析 parenthesized/legacy 维护选项，ANALYZE 实际刷新统计、FULL 调用重写路径并显式传播失败，FREEZE 以 `0A000` 拒绝，事务块中的 VACUUM 以 `25001` 拒绝；补充关系不存在、参数和 verbose 摘要处理。`scripts/build.sh`、`python3 tests/legacy_forced_analyze_protocol_e2e_test.py`、`scripts/build_one_test.sh vacuum_full_test`、`git diff --check` 通过；测试覆盖括号及旧式 ANALYZE、`ANALYZE = true`、统计行数/EXPLAIN估算、FREEZE与事务拒绝、FULL 文件收缩及数据保留。完整注册套件和 PG18.6 differential 未跑。VAC-03 仍 partial：FULL 仍非事务性原子 relation swap，crash consistency 未证明；FREEZE 与 VACUUM progress view 未实现，VERBOSE 输出也非完整 PostgreSQL counters。总账 273：22 complete、164 partial、72 unverified、15 deferred_by_user。

2026-10-05 WAL-06 为 SP-GiST sidecar 增加 checksum，source/test commit `068623bc`：新建 `.spgist` 使用固定宽度 RID/IEEE-754 坐标 V2 记录与 CRC32C，在线完整校验后构造查询索引；CRC、格式或记录验证失败时回退 heap，V1 文本仍兼容读取并列为 unchecked。只读 checksum verifier 扫描 cluster/tablespace `.spgist`，报告 checked/unchecked 数，只校验 signature/version/exact count/CRC，不验证点坐标语义或索引拓扑。`specialized_index_dml_test` 覆盖 V1、带旧 CRC 的有效坐标损坏后 heap fallback、畸形 sidecar fallback；`scripts/build.sh`、checksum E2E 的 SP-GiST signature/count/payload corruption、只读快照通过。未跑完整注册套件或 PG18.6 differential。WAL-06 仍 partial：V1 sidecar、catalog/FSM/VM 等 metadata 仍未统一 checksum；enable/disable/rewrite/progress 与离线语义验证未实现；TDE专项仍 deferred。总账 273：22 complete、163 partial、73 unverified、15 deferred_by_user。

2026-10-05 WAL-06 为 GiST sidecar 增加 checksum，source/test commits `64978375`, `7b210c05`：旧 `.gist` 文本记录无完整性校验，翻转成仍有效的范围值可静默改变查询候选；新建索引改用长度前缀二进制 V2 + CRC32C，在线先验 checksum，损坏时回退 heap，V1 仍可读并列为 unchecked。离线校验器增加 cluster/tablespace `.gist` 发现与 checked/unchecked 计数，仅验证签名、版本、count bound 和 CRC，不声称证明 range 语义或 PostgreSQL GiST tree。`scripts/build_one_test.sh gist_range_search_test`（有效范围位翻转 fallback、V1兼容、畸形 fallback）、`scripts/build.sh`、checksum E2E（magic/payload 损坏、合法 CRC 搭配越界 count、tablespace V1 分类、只读快照）通过。未跑完整注册套件或 PG18.6 differential。WAL-06 仍 partial：GiST V1、SP-GiST、旧索引格式及 catalog/FSM/VM 等 metadata 仍未统一 checksum；TDE专项仍 deferred。总账 273：22 complete、163 partial、73 unverified、15 deferred_by_user。

2026-10-05 WAL-06 修复 cold-cache 下畸形 SP-GiST sidecar 导致的静默漏查，source/test commit `d27eda0f`：此前没有内存缓存时，SP-GiST 文件解析失败会直接返回空结果；现在畸形文件走 heap scan fallback。新增测试先破坏 `.spgist` 文件，再用全新 `StorageEngine` 查询并断言命中真实 heap row。`scripts/build_one_test.sh specialized_index_dml_test` 与 `scripts/build.sh` 通过。该修复只覆盖解析失败/畸形文件，不检测仍可解析的位翻转，也没有为 SP-GiST 增加 checksum；完整注册套件和 PG18.6 differential 未跑。WAL-06 仍 partial；总账 273：22 complete、163 partial、73 unverified、15 deferred_by_user。

2026-10-05 WAL-06 扩展 BRIN sidecar 完整性覆盖，source/test commit `16b72b0a`：BRIN V1 原以 native-endian 保存 range summaries 且没有 checksum，位翻转可改变候选范围；新建索引使用 portable little-endian V2+CRC32C，查询端先验校验，旧 V1 仍可读并被标为 unchecked。`--verify-data-checksums` 现在扫描 cluster/tablespace `.brin`，单列 V2 checked 与 V1 unchecked 计数；校验器只验证头、range count 边界和整体 CRC，不声称证明 summary 语义。`scripts/build.sh`、`scripts/build_one_test.sh gin_brin_index_test`、checksum E2E（BRIN V2 corruption、tablespace V1 分类）、gap ledger checker 通过。完整注册套件和 PG18.6 differential 未跑。WAL-06 仍 partial：V1 BRIN、GIN、Hash/Bloom/B+Tree 旧格式仍 unchecked，离线 verifier 不解析 B+Tree 拓扑、GIN postings 或 BRIN ranges，GiST/SP-GiST 与多类 metadata 尚无统一 checksum；TDE 安全审计仍 deferred。总账 273：22 complete、163 partial、73 unverified、15 deferred_by_user。

2026-10-05 WAL-06 修复 GIN 空白键丢失并加入 sidecar checksum，source/test commit `f76809b6`：旧 `.gin` 以空格分隔 key 与 RID，JSON/array 中合法的 `"hello world"` 等 key 会无法表示并静默漏查；新 V2 改为长度前缀二进制 key/posting 与 CRC32C trailer，旧 V1 保持可读并由离线 verifier 标为 unchecked。`--verify-data-checksums` 现扫描 cluster/tablespace 的 `.gin`，验证 V2 magic/version/count 边界与 checksum 并报告 V1/V2 数量。正式 `scripts/build.sh`、`scripts/build_one_test.sh gin_brin_index_test`（空格键、checksum 损坏拒绝、V1 兼容）、checksum E2E（GIN V2 corruption、tablespace V1 统计）、gap ledger checker 与 `git diff --check` 通过。完整注册套件和 PG18.6 differential 未跑。WAL-06 仍 partial：GIN V1、旧 Hash/Bloom/B+Tree 格式仍 unchecked，离线 verifier 不解析 B+Tree 拓扑或 GIN postings，GiST/SP-GiST/BRIN 与 catalog/schema/FSM/VM 等 metadata 尚无统一 checksum，也无在线验证/重写流程；TDE 安全审计仍按用户要求 deferred。总账 273：22 complete、163 partial、73 unverified、15 deferred_by_user。

2026-10-05 WAL-06 增加 Bloom `.bidx` 完整性和耐久发布，source/test commit `b32ab185`：新 sidecar 使用 `BLM2` magic 与完整 little-endian payload 的 CRC32C trailer，loader 在解析 count 前验证；旧 `BLM1` 继续兼容读取且离线扫描标为 unchecked。保存路径从固定 `.tmp`、未 fsync 的 ofstream/rename 改为共享 atomic writer，文件与父目录均 fsync。离线 verifier 以只读/no-follow 发现 cluster/tablespace 中 `.bidx`，统计新格式 checked 和 `BLM1` unchecked。正式 `bash scripts/build.sh`、独立 Bloom checksum/BLM1/fsync-failure-retry 测试、完整 `bloom_index_test`、覆盖 `.bidx` 损坏及 tablespace legacy 的 `checksum_verification_e2e_test.py`、总账检查均通过。未跑完整注册套件或 PG18.6 differential；TDE安全审计仍按用户要求deferred。WAL-06仍partial：Bloom `BLM1`、Hash V1、旧 B+Tree `0xC551`/zero-marker仍无可靠 checksum，未解析B+Tree拓扑，GIN/GiST/SP-GiST/BRIN与catalog/schema/FSM/VM等metadata仍无统一校验，也没有enable/disable/rewrite/progress工具和在线全cluster验证；详见`docs/issue-wal-06-index-checksum-coverage.md`和`docs/PACKAGING.md`。总账273：22 complete、163 partial、73 unverified、15 deferred_by_user。

2026-10-05 WAL-06 增量修复 B+Tree 页交换绕过 checksum 的问题，source/test commit `2685454b`：此前 `0xC551` CRC 只覆盖页内容，两个完整页交换后校验仍有效，可能导致静默查找 miss。新 `0xC552` 格式将物理 block number 以固定小端编码纳入 CRC32C，header/page 首次载入均校验；保留 `0xC551` 内容校验兼容，旧 zero-marker 页仍按未校验处理，REINDEX 新文件使用页号绑定格式。修复前 page-swap 回归得到错误的 NotFound，修复后启动时拒绝损坏页；旧格式fixture可读。`scripts/build_one_test.sh bptree_topology_corruption_test`、`bptree_concurrency_test`、`bash scripts/build.sh` 和 crash matrix 12/12通过。full suite/PG18.6 differential未跑。WAL-06仍partial：旧索引需 REINDEX 才有页号绑定，Hash/Bloom/GIN/GiST/SP-GiST/BRIN及catalog/schema/FSM/VM等元数据仍无统一校验，离线工具仍只扫描heap family，也没有enable/disable/rewrite/progress工具；详见`docs/issue-wal-06-index-checksum-coverage.md`和`docs/PACKAGING.md`。总账273：22 complete、163 partial、73 unverified、15 deferred_by_user。

2026-10-05 WAL-06 增量修复 B+Tree 索引页缺少完整性检查的问题，source/test commit `93addab7`：原节点只验结构和排序，键区位翻转可造成静默查找 miss。新建及 REINDEX 新格式在每个4KiB页尾写CRC32C，并在buffer pool首次载入校验；旧zero-marker索引仍兼容读取但不声称已校验。`bash scripts/build.sh`、直接编译运行`bptree_topology_corruption_test`（翻转合法leaf key被拒、旧拓扑fixture仍可读）、`bptree_concurrency_test`以及改动后 crash matrix 12/12通过。full suite/PG18.6 differential未跑。WAL-06仍partial：未重建旧索引、Hash/Bloom/GIN/GiST/SP-GiST/BRIN及catalog/schema/FSM/VM等元数据仍无统一校验，离线工具仍只扫描heap family，也没有enable/disable/rewrite/progress工具；详见`docs/issue-wal-06-index-checksum-coverage.md`和`docs/PACKAGING.md`。总账273：22 complete、163 partial、73 unverified、15 deferred_by_user。

2026-10-05 WAL-05 修复真实 crash-recovery 启动崩溃，source/test commit `a7c15da5`：修正 crash matrix 的数据目录/模式/进程 PID 后，旧版4个 mid-transaction 重启均 exit 139；调用栈定位到全局 `StorageEngine` 在跨翻译单元静态初始化期运行恢复，遍历尚未完成动态初始化的活动事务 `std::set`。事务registry mutex/set/map 改为函数局部静态。`bash scripts/build.sh`、`bash scripts/build_one_test.sh redo_crash_recovery_test`通过；真实进程 `SIGKILL` 矩阵12/12通过。TDE组合仅为crash集成测试，未审计加密安全。未跑全注册suite或PG18.6 differential。WAL-05仍partial： before-image undo/replay 与 redo-based recovery、steal/no-force、并发checkpoint和所有写入/断电窗口的一致性尚未证明，详见`docs/issue-wal-05-startup-recovery-crash-matrix.md`。总账273：22 complete、163 partial、73 unverified、15 deferred_by_user。

2026-10-05 WAL-04 审计：PITR能按inclusive epoch撤销目标后的committed insert/update并durable fork新numeric timeline；`recovery_integrity_test`对malformed txn/heap/index WAL及unsafe index path fail-closed，timeline archive专项也通过。未实现restartpoint、hot-standby snapshot/recovery conflict、timeline history chain与promotion状态机。仅审计、无代码改动；full suite/PG18.6 differential未跑。WAL-04从unverified转partial，见`docs/issue-wal-04-standby-and-timeline-audit.md`。总账273：22 complete、162 partial、74 unverified、15 deferred_by_user。

2026-10-05 WAL-03 审计：`checkpoint_test`通过，覆盖buffer WAL barrier、活动事务阻止checkpoint、extent中断恢复、事务scope writeback及sidecar/WAL checkpoint LSN一致。实现会flush已加载cache、记录并fsync checkpoint WAL、原子写timestamp/max xid/LSN sidecar、归档且仅截断checkpoint前已archive segment。仍缺PG redo horizon、完整control file/checkpoint completion协议与dirty-buffer节流/调度；未修改代码，full suite/PG18.6 differential未跑。WAL-03从unverified转partial，详见`docs/issue-wal-03-checkpoint-audit.md`。总账273：22 complete、161 partial、75 unverified、15 deferred_by_user。

2026-10-05 WAL-02 审计：本地 WAL 已实现 CRC32C、8-byte 对齐、跨16MiB文件的连续 record I/O、按 LSN 范围 fsync/group commit、switchWal zero padding、归档保护回收和 page-image 子集；`wal_basic_test`、`redo_crash_recovery_test`、`wal_full_page_write_test`、`wal_truncate_test`、`wal_timeline_archive_test`均通过。跨 segment 仍没有 PostgreSQL WAL page header/continuation record，record compression与完整并发插入/FPI协议缺失。未发现新损坏 bug或修改代码；未跑full suite或PG18.6 differential。WAL-02从unverified分类为partial，详见`docs/issue-wal-02-insertion-segments-audit.md`。总账273：22 complete、160 partial、76 unverified、15 deferred_by_user。

2026-10-05 WAL-01 审计确认 WAL 已覆盖 heap before/after page image、BTree/hash whole-file image、transaction、checkpoint、SMGR truncate 的部分路径；`wal_basic_test`、`redo_crash_recovery_test`、`wal_full_page_write_test` 通过。恢复重放仍只应用 heap/index images；catalog WAL 只有对象操作描述，FSM/VM、TOAST、sequence、multixact、standby 等无完整 resource-manager redo。TOAST 提交前落盘和特殊索引提交时重建是局部替代机制，不等于完整 WAL。未修改代码，完整 suite 与 PG18.6 differential 未运行。WAL-01 从 unverified 分类为 partial，见 `docs/issue-wal-01-resource-manager-coverage-audit.md`。总账273：22 complete、159 partial、77 unverified、15 deferred_by_user。

2026-10-05 STO-06 审计确认当前 TOAST 是自定义 `__TOAST__<id>` marker + sidecar chunks/B+Tree，zlib 压缩仅在变小后保留；缺少 PostgreSQL varlena/external pointer、列级 storage strategy、可配 target、PGLZ/LZ4 与 dedup。现有 TOAST/VM-index consistency、snapshot-safe vacuum、失败清理等专项通过；未证实新的数据损坏错误，因此本次只更新审计状态，不声称修复这些兼容差距。全量 suite 未在此 revision 重跑，无 PG18.6 runtime differential。STO-06 仍 partial，见 `docs/issue-sto-06-toast-format-audit.md`。未push、Actions禁用、安全/TDE用户跳过项保持deferred。总账273：22 complete、158 partial、78 unverified、15 deferred_by_user。

2026-10-05 STO-05 修复简化 VACUUM 仅凭 line-pointer 状态就错误设置 all-visible 的问题：旧 REPEATABLE READ 快照仍需旧 tuple 版本时，回归确认 VACUUM 会发布错误 VM 位。由于此路径没有 visibility-horizon/OldestXmin 证明，现在保守清除 all-visible；定向回归旧实现失败、修复后通过，生产构建与 `parallel_vacuum_test` 通过。VM 当前没有查询/index-only consumer，因此不声称有当前查询结果变化。无全量套件重跑或 PG18.6 runtime differential。STO-05 仍 partial：FSM/VM crash rebuild、持久性、all-frozen、可见性 horizon 与 index-only 集成未完成，详见 `docs/issue-sto-05-vacuum-visibility-horizon.md`。代码/test commit `9a29e99a`；未push、Actions禁用、安全/TDE用户跳过项保持deferred。总账273：22 complete、157 partial、79 unverified、15 deferred_by_user。

2026-10-05 STO-04 修复 `BufferPool::invalidateAll()` 遗失仍被调用者持有的 pin：单帧池复现了 invalidate 后 fetch 另一页会覆盖旧指针，旧 unpin 还可能释放新页 pin。现将仍 pinned 的映射转为 orphan frame，最后一次 unpin 后才回收；新增 `tests/buffer_pool_invalidate_all_pins_test.cpp`，旧代码失败、修复后通过。`bash scripts/build.sh`通过。注册套件中461个C++测试全部通过；203个E2E有202个通过，唯一失败是`div14_feature_gate_test.py`的服务连接意外关闭，独立立即重跑通过，因此全量脚本如实记为exit 1。无PG18.6 runtime differential。STO-04仍partial：单metadata mutex、无content lock、prefetch、bulk/ring策略及自动ResourceOwner pin清理；详见`docs/issue-sto-04-buffer-invalidation-pins.md`。生产commit `019e5ee7`；未push、Actions禁用、安全/TDE用户跳过项保持deferred。总账273：22 complete、156 partial、80 unverified、15 deferred_by_user。

2026-10-04 第997项修复PostgreSQL wire与交互CLI中`statement_timeout`未中断执行的问题：行锁基线从4秒socket超时收敛为57014；Simple Query按语句计时，Extended Query计时从首个Parse/Bind/Execute/Describe消息到Execute或Sync完成；Parse停顿超时、Sync恢复、显式事务E/25P02及rollback恢复I、CLI超时后继续下一条SELECT均有回归。COPY FROM空闲及部分明文帧读取受deadline约束，超时发57014；遇到未完整帧则断开并验证回滚，CancelRequest可中断等待下一条CopyData；真实OpenSSL 3.5.5 TLS E2E验证COPY FROM空闲可恢复，单发TLS record头停顿则按deadline报57014并关闭。COPY TO输出改为deadline-aware非阻塞TCP写；受限接收窗口下未读取的多兆结果在超时后使backend退出active并断开。生产commit `9d13355b`、`aa904b22`、`78e0a0d7`、`57422dbf`、`a9b662e2`；TLS构建、TLS专项、完整`postgres_protocol_test.py`、COPY/CLI/pg_stat_activity回归通过。普通frontend读取、TLS COPY以外输出及其他阻塞socket I/O、非协作路径和完整ResourceOwner cleanup仍缺，因此OPT-17保持partial。完整注册suite首次发现两项旧`pg_stats` fixture错误查询已fail-closed的目录；更新后于`525116eb`重跑，退出0，460个C++和203个实际执行的E2E全部通过，另一个TLS E2E因默认stub按设计skip。无PG18.6 runtime differential；详见`docs/issue-997-statement-timeout-cooperative-cancel.md`和第989项补记。未push、Actions禁用、安全/TDE用户跳过项保持deferred。总账273：22 complete、152 partial、84 unverified、15 deferred_by_user。

2026-10-04 第996项核实CAT-01目录目前是CatalogManager内存缓存加每类单独CSV `.cat` 文件，OID计数器/空闲OID另存；`persistAll`会原子替换单个文件，但无heap tuple/WAL/MVCC、无多目录事务原子提交。catalog service/persistence failure/name resolution隔离测试通过；一个storage snapshot测试带有heap allocator close警告且不证明catalog MVCC。该项仅从unverified修正为partial，没有代码改动；不声称全套或PG18.6 runtime oracle/diff。详见`docs/issue-996-catalog-not-mvcc-audit.md`。CAT-01架构仍未完成；未push、Actions禁用、安全/TDE跳过项保持deferred。总账273：22 complete、151 partial、85 unverified、15 deferred_by_user。

2026-10-04 第995项复核`pg_stat_activity`旧renderer：实际锁等待证明普通用户可读另一会话的`PRIVATE_ACTIVITY_MARKER`，并且旧路径忽略投影/过滤且列类型全错。PG18 view为22列；当前只提供准确typed `pid/datname/usename/state/query`子集，对query实施本人/superuser/`pg_read_all_stats`可见性；并发隐私专项、Simple/Extended类型、完整协议、pg_settings、DIV-14、catalog SQLSTATE及生产构建通过。生产代码commit `9b3a7272`，本人可见性回归commit `fb01e1ec`。query_id、时间、wait、client、backend等仍缺，MON-04仅partial；无PG18.6 runtime oracle/diff，未跑完整注册suite。详见`docs/issue-995-pg-stat-activity-privacy.md`。未push；Actions禁用；安全/TDE跳过项保持deferred。总账273：22 complete、150 partial、86 unverified、15 deferred_by_user。

2026-10-04 第994项复核`pg_settings`旧renderer：它忽略投影/过滤，只返三列text且Extended Describe为NoData；官方PG18文档定义17列。代码/测试commit `d4ab5852`现只承诺准确的`name/setting/unit`typed子集，执行WHERE和LIMIT/OFFSET，返回OID25及unit SQL NULL；SELECT *、已知未实现列和复杂query fail-closed，unknown column仍42703。Simple/Extended E2E、完整协议、DIV-14、catalog SQLSTATE及生产构建通过；未声称18.6 runtime oracle/diff，未跑完整注册suite。CAT-03仍partial。详见`docs/issue-994-pg-settings-typed-subset.md`。未push；Actions禁用；安全/TDE跳过项保持deferred。总账273：22 complete、149 partial、87 unverified、15 deferred_by_user。

2026-10-04 第993项修正`pg_roles`四列text renderer冒充完整系统view的问题：PG18视图有13列，旧路径不遵循projection/filter。现限定目录及无同名用户relation的未限定查询返回0A000；`SHOW USERS/ROLES`、底层角色API及同名普通表/视图路径不变。生产构建、catalog SQLSTATE E2E、pg_class E2E、`div14_feature_gate_test.py`通过；官方PG18文档核schema，不声称18.6 runtime oracle；未跑完整注册suite。CAT-03仍partial。详见`docs/issue-993-pg-roles-view-fail-closed.md`。未push；Actions禁用；安全/TDE跳过项保持deferred。总账273：22 complete、149 partial、87 unverified、15 deferred_by_user。

2026-10-04 第992项修正`pg_type`/`pg_enum`不可信SQL目录输出：旧`pg_type`仅返回5个text列，PG18目录实际32列；`pg_enum`路径也忽略投影/过滤，且不报告列类型。现限定查询与不存在同名用户relation时的未限定查询均fail-closed为0A000，同名表/视图和底层enum DDL保留。生产构建、catalog SQLSTATE E2E、pg_class E2E及完整`postgres_protocol_test.py`通过；未跑完整注册suite。PG18官方文档核schema，不声称18.6 runtime oracle。CAT-03仍partial。详见`docs/issue-992-pg-type-enum-query-fail-closed.md`。未push；Actions禁用；安全/TDE跳过项保持deferred。总账273：22 complete、149 partial、87 unverified、15 deferred_by_user。

2026-10-04 第991项修正`pg_class`星号投影将七列子集冒充完整PG18目录的问题：当前schema共34列；现`SELECT *`及已知未实现列为0A000，未知列仍42703。生产构建、Simple/Extended pg_class E2E及catalog SQLSTATE E2E通过；官方PG18文档核schema，本机运行参考为17.2，不声称18.6 runtime oracle。CAT-03仍partial。见`docs/issue-991-pg-class-incomplete-projection.md`。未push；Actions禁用；安全/TDE跳过项保持deferred。总账273：22 complete、149 partial、87 unverified、15 deferred_by_user。

2026-10-04 第990项修正CAT-03的`pg_namespace`失真SQL路径：旧查询只返3个text列、暴露静态`pg_temp_1`且没有`nspacl`；现schema未完整前对限定名及未限定缺失用户表返回0A000。生产构建、catalog/pg_class协议E2E通过，同名用户表回归通过。官方PG18文档核schema，本机PG参考为17.2，不声称18.6 runtime oracle。CAT-03继续partial；详见`docs/issue-990-pg-namespace-shape-fail-closed.md`。未push；Actions禁用；安全/TDE跳过项继续deferred。总账273：22 complete、149 partial、87 unverified、15 deferred_by_user。

2026-10-04 第989项修正CAT-03中`pg_stats`与`pg_statistic`共用错误schema：旧wire查询将两者都描述为8个text列；现因缺少分离typed schema/权限语义而对限定名及未限定缺失用户relation返回0A000。底层统计API保持可用；`bash scripts/build.sh`、catalog与pg_class协议E2E、`pg_stats_test`通过。PG18官方文档核schema，本机PG参考为17.2，不声称18.6 runtime oracle；CAT-03仍partial。详见`docs/issue-989-pg-stats-shape-fail-closed.md`。未push；Actions禁用；安全/TDE跳过项仍deferred。总账273：22 complete、149 partial、87 unverified、15 deferred_by_user。

2026-10-04 第986项复核SQL-14：没有query-tree deep copy、rule rewrite runtime、权限标记传播或rewrite依赖失效。第983项`div14_feature_gate_test.py`已验证RULE和EVENT TRIGGER在PG/extended mode都按0A000拒绝且不产生伪兼容对象，因此SQL-14从unverified改partial但未完成；CAT-19仍独立partial。无源码改动、无PG18.6 rewrite oracle/diff。详见`docs/issue-986-query-rewrite-capability-audit.md`。未push；Actions禁用；用户跳过安全/TDE仍deferred。总账273项：22 complete、149 partial、87 unverified、15 deferred_by_user。

2026-10-04 第985项修复CAT-03中16个额外catalog缺失路径，代码/测试commit `c79c51bb`：18个已探测无SQL catalog执行路径的关系对`pg_catalog.name`及当前库无同名普通relation的未限定查询显式返回0A000，替代此前误报的55P03/42P01。定向协议E2E逐项通过，并确认支持的pg_database和当时未改动的pg_statistic路径、同名用户pg_constraint表仍可读写；`pg_stats`/`pg_statistic`的错误schema在第989项后续修正。生产构建通过；完整注册脚本因人工提前终止plpgsql_test退出1（不计全套通过），之后完成的其余测试通过，plpgsql_test单独重跑退出0（约94秒）。无PG18.6 oracle/diff；catalog数据/schema仍缺，CAT-03保持partial。详见`docs/issue-985-pg-catalog-fail-closed.md`。未push；Actions禁用；用户跳过安全/TDE继续deferred。总账273项：22 complete、148 partial、88 unverified、15 deferred_by_user。

2026-10-04 第984项修复CAT-03错误分类，代码/测试commit `ffb63d19`：未实现的`pg_catalog.pg_index`和`pg_operator`带限定及未限定名称查询，在当前库无同名relation时均显式返回0A000，避免此前误报55P03/42P01；新协议回归还验证同连接后续查询正常。最终生产构建和定向E2E通过；完整注册suite在未限定名称扩展前通过，扩展后未重跑；无PG18.6 oracle/diff。真实catalog schema/data及其余catalog仍缺，CAT-03仅partial。详见`docs/issue-984-pg-catalog-error-state.md`。未push；Actions禁用；用户跳过安全/TDE继续deferred。总账273项：22 complete、148 partial、88 unverified、15 deferred_by_user。

2026-10-04 第983项复核CAT-19，协议测试commit `64ab219f`：新增extended mode下`CREATE/ALTER/DROP RULE`及`CREATE/ALTER/DROP EVENT TRIGGER`的0A000回归，`python3 tests/div14_feature_gate_test.py`通过；默认模式既有gate同样通过且不产生伪compat对象。此处仅核实未实现命令不会假成功；规则rewrite、真实DDL event执行、顺序/事务语义及dependency仍缺，CAT-19从unverified转partial。无production源码更改、无PG18.6 oracle/diff。详见`docs/issue-983-rule-event-trigger-gate-audit.md`。未push；Actions禁用；用户跳过安全/TDE继续deferred。总账273项：22 complete、147 partial、89 unverified、15 deferred_by_user。

2026-10-04 第982项复核FUNC-03，协议测试commit `1944c64c`：用户自定义operator目前只有语法/兼容对象fallback，无runtime catalog或planner support。新加的extended-mode `CREATE/ALTER/DROP OPERATOR` protocol回归与已有PG mode、operator class/family gate用例由`python3 tests/div14_feature_gate_test.py`通过；失败路径为0A000且无`.pg_compat_objects`假对象。此处验证的是fail-closed，不是功能完成；commutator/negator、selectivity support、hash/merge标志、dependency/invalidation和PG18.6 oracle仍缺，FUNC-03从unverified转partial。详见`docs/issue-982-user-operator-gate-audit.md`。本步仅改测试/审计，无production变更，不push；Actions禁用，用户跳过的安全/TDE保持deferred。总账273项：22 complete、146 partial、90 unverified、15 deferred_by_user。

# DBMS_C_plus_plus 对标 PostgreSQL 18.6：完整差距清单

2026-10-04 第981项复核：OPT-06已有有限真实执行路径。`scalar_subquery_projection_cardinality_test`与`scalar_subquery_self_correlation_test`通过，覆盖相关scalar子查询、外层别名与NULL绑定、空结果和多行cardinality error；`volcano_select_phase51_test`通过，覆盖Volcano SemiJoin/AntiJoin、ExistenceFilter及ANY/ALL子集。完整注册C++／protocol／E2E suite在同一当前源码版本此前exit 0、`All tests passed`。无本步源码更改、没有PG18.6 oracle/diff。源码仍明确将复杂相关quantified、aggregate、order及expression形态留给legacy路径；parameterized path、inner index选择、initplan/memoize及完整costing/invalidation未实现，OPT-06由unverified改为partial。详见`docs/issue-981-correlated-subquery-execution-scope.md`。未push，Actions禁用；273项为22 complete、145 partial、91 unverified、15 deferred_by_user。

2026-10-04 第980项 source/test commit `e808d50d`：复现串行化 false positive：两个SERIALIZABLE事务分别对缺失主键99／100做空点查并插入不同键，旧实现因两个relation级SIREAD而错误拒绝第一笔commit。现仅当空结果有可用索引谓词覆盖时省去relation fallback；未索引空扫描仍保留保守检测。增加回归：无索引空范围的危险结构仍abort，互不相交索引点查均提交，交叉谓词cycle仍abort一笔，另保留page/cross-page用例。定向SSI通过；生产`bash scripts/build.sh`通过；最终完整注册C++／protocol／E2E suite exit 0、`All tests passed`。本步没有PG18.6 oracle/diff；完整predicate lock提升、tuple/page/index各AM覆盖及SSI冲突图仍未实现，TXN-09仅partial。详情见`docs/issue-980-ssi-empty-index-predicate-granularity.md`。未push，Actions禁用；273项为22 complete、144 partial、92 unverified、15 deferred_by_user。

2026-10-04 第979项 test commit `df0c59de`：OPS-01的data-directory启动路径已有显式`-D`／`--data-dir`与`DBMS_DATA_DIR`选择、启动CWD相对路径规范化、cluster control校验、目录锁及bootstrap后`chdir`。新增E2E从独立launch目录以相对`-D`启动既有cluster、认证、停止，并确认control identity不变且launch目录无误建文件；`python3 tests/data_directory_e2e_test.py`通过。该项只验证路径边界，无production改动、未专门重跑全注册suite。GUC context/source/reload/restart语义仍未闭合，OPS-01由unverified改为partial。详见`docs/issue-979-relative-data-directory.md`。未push，Actions禁用；273项仍22 complete、143 partial、93 unverified、15 deferred_by_user。

2026-10-04 第978项 source/test commit `1612d142`：PG18.6确认ATTACH已有child后，child既有行也从parent可见；本地却把行写在parent专属fork，child relation仍指向自己的文件，空child后续直接写入同样会分叉。现在对任何现存relation的`ALTER TABLE ... ATTACH PARTITION`及`CREATE TABLE ... PARTITION OF`在文件/schema变更前明确报`0A000`；刚建child的CREATE事务回滚且不留孤儿表。约束检查优先，因此DEFAULT冲突仍报23514；缺失ALTER child仍报42P01。虚拟storage partition名及底层路由仍可用。三个相关定向测试和`bash scripts/build.sh`通过；最终版本的全注册suite未重跑（中间版全套曾在此路径失败，最终定向回归通过），不报全量绿。真正的child relation存储/索引映射未实现，CAT-11保持partial，见`docs/issue-978-attach-relation-storage-guard.md`。未push，Actions禁用，安全/TDE跳过保持deferred；总账273项仍为22 complete、142 partial、94 unverified、15 deferred_by_user。

2026-10-04 第977项 source/test commit `40b72024`：修复DDL对不存在子表的`ATTACH PARTITION`会创建幽灵分区的问题。现在在storage变更前解析并检查子关系；不存在时报PostgreSQL一致的`42P01`且不发布分区元数据。`alter_table_only_test`验证缺失子表错误、parent schema不变，以及创建有效空child后ATTACH仍成功；正式生产构建成功。PG18.6（180006）oracle确认42P01。未在本小步后重跑全量注册套件；紧邻的#976全量注册套件通过。仍未验证child列结构或搬迁已有child数据，CAT-11继续partial。详见`docs/issue-977-attach-missing-partition-relation.md`。未push，Actions禁用，安全/TDE跳过项保持deferred；总账273项：22 complete、142 partial、94 unverified、15 deferred_by_user。

2026-10-04 第976项 source/test commit `c83e67ac`：修复RANGE `ATTACH PARTITION` 未验证DEFAULT分区现存行的问题。attach现在在元数据锁下扫描DEFAULT叶分区，若已有非NULL分区键落入新半开区间，则在元数据变更前返回CHECK_VIOLATION；DDL边界报PostgreSQL一致的23514，事务回滚保留原数据/分区定义。覆盖冲突拒绝、状态码与回滚、无冲突成功路由。定向`partition_test`及完整注册C++／protocol／E2E suite均通过，exit 0、`All tests passed`；PG18.6（180006）oracle确认相同行为。该修复不移动冲突行，CAT-11继续partial；约束证明、并发detach、分区索引和全局唯一性仍未实现。详见`docs/issue-976-range-attach-default-validation.md`。未push，Actions禁用，安全/TDE跳过项保持deferred；总账273项：22 complete、142 partial、94 unverified、15 deferred_by_user。

2026-10-04 第975项 source/test commit `5ac83dae`：复现并修复RANGE `ATTACH PARTITION ... FROM (lower) TO (upper)` 丢弃lower endpoint的问题；旧代码会把 `[10,20)` 错路由接收5，也会允许与之重叠的`[15,25)`。schema新增追加式RLB1扩展，旧schema按MINVALUE／前一区间upper推断；attach拒绝重叠、允许相邻，detach与DDL clone保持边界对齐。重启回归确认边界落盘有效。最终完整注册C++／protocol／E2E suite exit 0、`All tests passed`；本机PostgreSQL 18.6（`server_version_num=180006`）临时表oracle也确认 `[10,20)` 边界、路由、重叠拒绝和相邻 `[20,30)` 行为。CAT-11仅此范围转为partial，仍未解决default partition已有行验证/迁移、constraint proof、detach concurrently/finalize、partition indexes及跨分区唯一性。详情见 `docs/issue-975-range-partition-bounds.md`。未push，Actions禁用，安全/TDE跳过项继续deferred；273项现22 complete、142 partial、94 unverified、15 deferred_by_user。

2026-10-03 第974项 source/test commit `c18bd1bd`：确认 tuple header 的 `xmin/xmax` 仍为32位，而事务ID分配器是64位且没有epoch/freeze/wraparound机制；原逻辑超过 `UINT32_MAX` 后会继续分配并在tuple header写入时静默截断。现在在最后一个可表示的tuple XID后fail closed，拒绝后续事务ID分配，并增加边界、持久化高水位和重启回归。生产构建及完整注册套件 `DBMS_PROTOCOL_TEST_TIMEOUT=120 DBMS_PROTOCOL_STARTUP_TIMEOUT=120 DBMS_PROTOCOL_SHUTDOWN_TIMEOUT=120 bash scripts/build_tests.sh` 均通过（`All tests passed`）。这是避免静默事务可见性损坏的临时安全上限，不实现XID epoch、freeze、MultiXact或vacuum horizon；P0-08仍partial，未勾选。详情见 `docs/issue-974-xid-tuple-width-guard.md`。未push，GitHub Actions禁用，用户跳过的安全/TDE继续deferred。

2026-10-03 第973项最终验收：修复SemiJoinOp把空文本误识别为NULL的标量／复合 `IN`／`NOT IN` 问题，source/test commit `d6ce9adf`。最终注册测试套件 exit 0（`All tests passed`）；同一源码对真实本地 PostgreSQL 18.6（180006、en_US.utf8）完整兼容差分 `cases=464 failed=0`。本次只确认这些已覆盖行为一致，不将 QRY-04／QRY-10 或任何更大功能族标为完成；273项总账仍22 complete、140 partial、96 unverified、15 deferred_by_user。未push，Actions禁用，用户跳过的安全/TDE继续deferred。详情见 `docs/issue-973-semi-join-empty-text-null.md`。

2026-10-03 第965项 source `aa27818d` 改用列类型语义判断WITH TIES peers，修复CHAR尾随空格并让未知比较类型安全回0A000。生产build、定向结构化wire、FETCH boundary E2E及最终全量套件460 C++／197 E2E全部通过；fresh en_US.utf8 PG18.6 `set_operation_precedence` 差分 cases=1 failed=0。更大PG18差分在 `quoted_sequence_dot_schema_owned` 的DROP TABLE等待120秒超时；该三条sequence case family的fresh focused复跑均通过，未据此宣称全量差分通过。QRY-06仍partial，详见 `docs/issue-965-fetch-ties-column-equality.md`。

2026-10-03 第964项 source `bec759db` 在普通单表SELECT上实现有限的 `FETCH FIRST/NEXT n ROW(S) WITH TIES`，包括OFFSET、Numeric等值、NULL peer及显式ASC；支持限于plain-column投影和投影内排序键，复杂query形态明确fail-closed。生产构建、定向wire和PG18.6 direct oracle通过；最终460 C++／197 E2E全量suite由965复跑并通过，supported query shapes还在fresh en_US.utf8 PG18.6 `set_operation_precedence` case中差分通过。更大PG18差分存在独立DROP TABLE timeout，未冒称全量通过。QRY-06仍partial，详见 `docs/issue-964-ordinary-fetch-with-ties.md`。

2026-10-03 第963项 source `258bd554` 在UNION／INTERSECT／EXCEPT最终结果上执行合法 `FETCH FIRST/NEXT ... WITH TIES`，不再把limit错误放到右操作数；支持排序后offset、排序键peer扩展以及numeric等值／text collation／NULL peer。普通、parenthesized、offset、数值等价和NULL peer wire回归均过，PostgreSQL 18.6 direct oracle同结果；后续全套460 C++／197 E2E通过。第962项的无ORDER SQLSTATE修复仍有效；有限普通单表子集由964支持，复杂非集合query仍未闭合。QRY-06保持partial，详见 `docs/issue-963-set-fetch-with-ties.md`。

2026-10-03 第962项 source `36b73cd5` 修正外层 `FETCH ... WITH TIES` 缺少query级ORDER BY时的SQLSTATE：PG18返回42601，现在本地也返回42601；有ORDER BY的合法WITH TIES当前明确返回0A000，功能仍未实现。FETCH ONLY及OFFSET＋FETCH相邻wire回归和生产build通过，PG18.6 direct oracle核实；没有全量套件／generic PG diff。QRY-06仍partial，见 `docs/issue-962-fetch-ties-order-error.md`。

2026-10-03 第961项 source `9d095496` 修复集合运算 `ORDER BY` 对重复输出列名的错误绑定：名称引用歧义现在返回PG一致SQLSTATE `42702`，序号引用保持可用；定向生产构建、真实wire错误边界及PG18.6 direct oracle通过。全量套件未重跑，QRY-06仍partial，详见 `docs/issue-961-ambiguous-set-order-name.md`。

2026-10-03 第960项 source `439a8ef7` 补齐集合运算最终 `ORDER BY` 的多个输出键：可按列名／序号依次比较，并遵守每键ASC/DESC、显式NULLS及其默认顺序；真实PostgreSQL 18.6 direct oracle版本`180006`与目标行序一致，生产构建及结构化wire回归通过。通用差分runner默认Docker `pgref` 实为17.2，严格18.6 preflight拒绝，未冒称PG18 diff或全套通过。QRY-06仍partial，详见 `docs/issue-960-set-operation-multi-key-order.md`。

2026-10-02 第959项source commits `3df0fef7`／`fbd99ad3` 局部修复以括号开始的Simple Query、集合运算括号操作数、尾随注释和括号表达式后的ORDER/LIMIT tail；完整括号SELECT在wire上保留int4 OID，PostgreSQL 18.6八语句强差分通过。对应回归位于 `tests/set_operation_structured_protocol_e2e_test.py` 与 `tests/compat/cases/set_operation_precedence.sql`，细节见 `docs/issue-959-parenthesized-set-operations.md`。这只关闭QRY-06的一部分；复杂类型/collation、通用表达式排序分页与Extended Describe仍未完成。

2026-10-02 第958项 source `3cfe28db` 已整合至D `70c5a116`：TEMP relation／sequence／DDL 创建访问在顶层事务记录sticky标记，空表、零行DML、只读和已回滚子事务均不能绕过PREPARE TRANSACTION；拒绝时整段rollback，报 `0A000`／Ready `I`。SQL PREPARE与协议Parse共用无执行副作用的catalog／AST walker，覆盖scalar／EXISTS／IN／derived／WITH／非递归CTE作用域／UNION／DML target，并避免执行nextval或把普通字符串误判为relation访问。真实PG18.6强oracle、本地强wire、18不同C++、19wire（含完整协议）、19不同actual均exit0；旧失败保留，详见 `docs/issue-958-temp-prepared-access.md`。整合后本地脚本460 C++／197 E2E全过；同一生产二进制的PG18.6差分以 `DBMS_PROTOCOL_TEST_TIMEOUT=120` 运行 `cases=461 failed=0`。默认15秒首轮在一条超20秒DROP上超时；保留失败日志，同case提高预算通过后才重跑全量，未修改源码隐藏差异。ACCESS SHARE、完整binder、TEMP对象族及2PC资源／恢复未完成，CAT-13／15、TXN-05／06、SQL-13、PROTO-01／02仍partial。

2026-10-02 第956项 source `802344c6` 已整合至D `c6afebd1`：ALTER ADD CHECK 会检查既有行，拒绝false条件用23514／`CHECK_VIOLATION`，NULL通过；失败不发布constraint，多action失败与USER-SP回滚恢复行／列／metadata。PG18.6强oracle、Simple／Extended wire、20 C++、15wire（含完整协议）、15不同actual均exit0。发现并更新一项旧native测试仍期待 `INVALID_VALUE`，修正断言后定向重链测试exit0；最终组合460 C++／197 E2E脚本及PG18.6 461-case差分（120秒协议预算）全部通过。immutable依赖、NOT VALID／VALIDATE扫描、domain／partition约束证明及非法deferrable仍未实现，CONS-03继续partial。

当前D整合代码已含第948项针对 `DISCARD TEMP` 保留namespace时清理TEMP index catalog identity的修正（source `d1dee20b`，见 `dropSessionTemporaryTable`），组合内 `temp_index_catalog_test` 与 wire 回归均通过；954／955字面量保护也已进入D。以下较早的当日记录保留当时的“待整合／待门禁”状态，不能覆盖本段较新的结果。

2026-10-02 D整合949 `f4ce4992`→`ccde689f`、951 `e9b9264e`及952 `f75f9a6d`／953 `e0b0b874`→`68bbe256`，注册冲突保留全部双方入口。949现有pg_class七列typed SELECT／COUNT／WHERE／ORDER／NULL、无需执行Describe和quoted内部char OID18已按原样PG强oracle／旧生产反证、18相邻原生＋order专项、19wire含完整协议、16不同actual验收；951–953独立cold55单元、实际Parser／Main增量、两组强oracle／专项、16原生、18wire含完整协议、18不同actual全0，最终同一组合source分问题入commit，非各中间commit单独完整suite的宣称。D source68实际457 C++／192E2E／456actual，尚未对其构建／全量门禁，Root405未快进。948合入949后新强wire／actual真的失败：DISCARD TEMP保留namespace时索引catalog行残留，加强native同源assert134保留；补TableManage auto-owned index identities在物理dropTable成功后清除，测试保存点恢复同OID／物理索引，独立自有增量build中，尚未提交。954 PG原样oracle0／旧真实wire错误改写dollar大小写及空格1，改用已有protected-byte mask保真legacy copy，另树自有Main增量build中，未验收。不抹掉旧D整轮timeout／startup abort或其它历史失败。总账273仍22 complete／140 partial／96 unverified／15 deferred_by_user；不push，Actions仅本地disabled、安全／TDE跳过。

2026-10-02 第950项已独立本地提交 source `811cc75d`，验收账 `1c70b92e`，D整合 `60ceb474`：postfix :: type scanner在AS前停止，保留explicit／quoted output alias及chained cast类型。真实旧943 Parse-Describe-before-Execute强wire exit1，PG18.6同一强oracle exit0；独立全source统一自有headers生产、专项、12 C++、15wire含完整协议及15不同actual全exit0。D此前943–947的55生产单元全自有冷build已exit0；950仅重编改变的自有parser后link exit0，六个组合强wire均exit0，非第二次冷编译。现source实际454 C++／189E2E／453actual；新组合全量门禁尚未执行。949计数及过滤已正确，但先缺virtual pg_class Describe metadata，补齐后强测试又复现ORDER BY把末尾分号当列、42703；951正在隔离复现和修复，949／948均未提交、未宣称验收。旧整轮timeout、startup abort及currency DIFF仍保留，单独重跑不抹掉原exit1。总账273仍22 complete／140 partial／96 unverified／15 deferred_by_user；Root405未快进，不push，Actions仅本地disabled，跳过安全／TDE不重启。

2026-10-02 第943–947项独立source均已定向验收并整合到D `b4d5157e`：943 `38e697af`→`1c68b73b` transactional DISCARD TEMP；944 `d68b7781`→`0302afd4` planning-only pending P/B DISCARD ALL；945 `d1e571ce`／946 `b08ff199`／947 `b0bddd32`→`b4d5157e` backend currency、zero/FM/SG、exact decimal rounding。三次注册冲突均保留双方入口；源码实际453 C++／188E2E／452actual，新的统一自有headers/source production build仍在运行，尚未启动本source全量门禁，不借旧结果。旧D84b正式全量450 C++／182 E2E通过但完整protocol ALTER1197 timeout／整轮exit1；448 PG447OK／currency DIFF exit1保留。独立full protocol首次重跑在startup connect abort／exit1，第二次同source／same binary复跑exit0（/tmp/dbms-cache-discard-full-protocol-repeat-v2-939-942.log，socket120秒配置）；失败阶段不同、原因未定，不将旧整轮改称通过。948 TEMP index catalog已有旧native134及fixed19原生／13wire含完整协议／16不同actual均0，但新的强catalog查询必须结合949验收，尚未提交；949 typed pg_class SELECT强case已PG0／旧wire1，候选正确rows却cast alias metadata失败，950独立发现parser :: type吞AS alias，旧AST134／真实Parse-Describe wire1／PG0，独立生产构建中。总账273保持22 complete／140 partial／96 unverified／15 deferred_by_user，Root405暂未快进，不push、Actions仅本地disabled、跳过安全／TDE不重启。

2026-10-02 冻结D source `84b258de`（HEAD文档ce6c6d69）的正式全量已结束：450/450 C++、182/183 E2E；唯一失败是完整postgres_protocol_test.py在prepared_transaction_error_boundaries的ALTER TABLE ADD COLUMN（line1197）等待响应timeout／整轮exit1。与942早先Describe line931 timeout位置不同，原因尚未定位，不能称已修或通过；同source／同binary独立完整协议复跑正在运行。全448 PG18.6实际差分已结束447不同OK、to_char_numeric一DIFF／cases=448 failed=1／exit1，对照未改参考lc_monetary，差异由后续945 source d1e571ce修复。对应日志为/tmp/dbms-tests-cache-discard-combined-939-942.log及/tmp/dbms-pgdiff-cache-discard-combined-939-942.log，复跑日志/tmp/dbms-cache-discard-full-protocol-repeat-939-942.log。943 source38e697af／944 d68b7781／945 d1e571ce／946 b08ff199／947 b0bddd32各已独立定向验收及本地提交，但尚未合入此冻结D或Root405；948 TEMP index catalog修复仍待949结构化pg_class SELECT协同验收，保持原失败SQL及原生断言。当前总账273仍22 complete／140 partial／96 unverified／15 deferred_by_user，没有总清单完成声明，不push、Actions仅本地disabled、跳过安全／TDE不重开。

2026-10-02 第942项 source `416d640f`、D整合 `84b258de`：初始化后的真实pg_temp namespace在DISCARD ALL中原被删除，后续DROP INDEX错误3F000而PG为42704。新增保留namespace的cleanup重载，保留旧两参disconnect/startup清理接口；Main清理失败报58030、不无条件清空跟踪；Standalone Extended DISCARD ALL不再自动打开会触发25001的物理BEGIN，显式BEGIN仍25001／E。正确强PG18.6 oracle exit0／旧213真实强wire exit1、最终全自有统一headers/source生产、专项、16原生及15不同actual全exit0。相邻wire前12通过，最后完整协议第一次在Describe处120秒超时／exit1；未改source的独立完整协议重跑exit0，原失败日志保留且未断言已查明原因。最初误假定未初始化pg_temp也42704的oracle exit1、错误guard候选及会掩盖oracle失败的shell wrapper保留但不作验收证据：未初始化仍3F000。D新组合source84b应450 C++／183E2E／448actual，正在全自有统一headers重建，之后独立全门禁；不能借专项或旧438全差分冒充。943 DISCARD TEMP正接入严格typed grammar与既有物理DDL回滚，真实强化PG oracle已exit0；pending P/B后的DISCARD ALL、完整reset状态、TEMP index catalog与缺失sequence文件清理仍待。总账273仍22 complete／140 partial／96 unverified／15 deferred_by_user；不push，Actions仅本地disabled、安全／TDE继续跳过。

2026-10-02 第941项 source `83cfbc79`、D整合 `20c28477`：缺失数据库名的后台缓存清理原来可在COMMIT保有WAL裸指针时释放WAL/CLOG，真实20重复第2轮崩溃、40同源诊断轮9失败及WALManager::segmentPath栈保留。现在清理前尝试独占database-name transaction lock，活动事务及本线程活动context跳过；已有cacheMutex时绝不阻塞等待反向锁，并在取得锁后重新检查名称。独立全source自有统一headers生产、加强原native及60连续轮、21原生、13wire含完整协议、12不同PG actual全部exit0。仅该缓存生命周期竞争闭合，不宣称全WAL／全并发／sanitizer完成。942专项及16原生／15不同actual已exit0，但13wire的最后完整协议在Describe处120秒超时／exit1，失败日志保留，独立同源重跑及原因复核进行中；尚未提交942。943 DISCARD TEMP同一强PG18.6 oracle已exit0，旧213首条42601／exit1，事务化cleanup实现尚待。总账273仍22 complete／140 partial／96 unverified／15 deferred_by_user；不push，Actions仅本地disabled，安全／TDE跳过项不重开。

2026-10-02 第939项 source `8e407ffa`、D整合 `32a57b28`：SHOW transaction_isolation 原来报XX000／Unknown SHOW command；真实旧213生产强wire exit1与PG18.6强oracle exit0保留。现在SHOW普通参数、quoted大写参数及TRANSACTION ISOLATION LEVEL通过严格typed参数AST、source完整尾随检查，返回实际live block isolation／新BEGIN使用的idle默认read committed，结构化text／OID25／SHOW tag；statement及portal Describe无需Execute即可供给同一metadata，不误取得first query snapshot。强Simple／Extended、四种level、SHOW后合法SET、USER-SP、CHAIN继承、结束恢复与failed block25P02全部exit0；独立全source自有统一headers production、16 C++、18wire含完整协议、14不同actual也全部exit0。仅该显示及metadata边界闭合，不宣称SESSION/default GUC设置与完整恢复完成。

第940项 source `afab6812`、D整合 `b70db5d9`：TEMP表已进入真实catalog，但ALTER ADD／RENAME／DROP COLUMN仍沿用旧跳过分支；旧强wire在DROP SERIAL列后错误nextval=3、旧native缺新增attribute／typmod均exit1／134保留。三处catalog及owned sequence更新改为同样执行，storage-only transient relation仍按已有catalog helper的无relation边界处理。强Simple／Extended核对列改名、NULL-free原值、精确headers／OID23和1043、qualified pg_temp DROP、USER-SP失败恢复及owned sequence存在／删除；native核对真实OID、attnum、varchar typmod9、ownedByColumn与文件恢复，最终均exit0。先旧代码全source自有统一headers production，失败native后仅自有Ddl增量重编／link；最终16 C++、10wire含完整协议、15不同actual全exit0，不冒称第二次冷编译。相邻native首轮误填不存在的transaction_savepoint_test，被启动前guard拒绝；改为实际statement_savepoint_namespace／savepoint_stack等后完整16重跑，未把拒绝轮算通过。

旧D fc741门禁已实际结束：443 C++／174E2E通过，parser_phase1与DIV14失败／整轮exit1；442全PG差分441不同OK、discard_command_tags的DISCARD ALL后DROP INDEX pg_temp错误3F000（PG42704）／failed=1／exit1。新33冻结213源码447／180／445的全套仍在跑，另已真实复现begin_transaction_database_drop_race未捕获bad_alloc／length_error：原20重跑第2轮崩溃、同源同header显式诊断组合40轮9失败，栈到COMMIT的WALManager::segmentPath。941非阻塞database-name lock保护后台stale-cache释放，独立全源production及加强native／60连续轮已exit0，相邻门禁仍待收齐，不算已提交完成。942最初误假定未初始化pg_temp也应42704的PG夹具exit1保留；真PG未初始化3F000，初始化再DISCARD后42704，修正同一strong oracle后PG exit0／旧213 exit1。正在修DISCARD ALL保留真实namespace身份及独立Extended执行边界，新header全自有重编中；DISCARD TEMP、完整utility显式／隐式block、TEMP index catalog／缺失文件故障仍是后续项。D当前source b70应449 C++／182E2E／447actual，尚未生产整体验证，等941／942独立验收后再冻结新组合，不借33或旧Root438证据。总账273仍22 complete／140 partial／96 unverified／15 deferred_by_user；不push，Actions仅本地disabled，跳过的安全／TDE保持deferred。

2026-10-02 第932／937／938项已分别本地提交并整合，尚不代表总清单完成。932 source `da12e0ee`、整合 `d20030a4`：普通／E string、quoted identifier、dollar string 未闭合统一在原始SQL执行前报42601；标准字符串的backslash不再隐藏结束引号，E-string terminal escaped quote及identifier内$不误断句。PG18.6最终强oracle、27 C++、27wire含完整协议、18不同actual均exit0；首轮literal alias／NULL等真实失败保留，由已独立提交的933–935修复后按统一自有headers全source重编验收，不删AS或原字节控制。937 source `8e6eb0c4`、整合 `64ec43a6`：Simple Query结束之前Parse／Bind打开的隐式事务并清portal，正常／syntax error／empty／CHAIN及COPY路径一致；用户BEGIN提升则保留既有snapshot。PG18.6同一强oracle、独立全自有production、20 C++、23wire含完整协议、12不同actual全部exit0；旧fc741协议错误Ready T仍保留。938 source `8d388914`、整合 `21391c92`仅补强旧DIV-01夹具：真实Parse／Bind事务使既有USE无事务守卫优先25001，拒绝后的Ready I／数据库不变／portal过期／named statement及OID23仍有效均明确断言；整份DIV14用937真实binary exit0，旧fc741在Ready E断言exit1，不修改生产守卫或冒称新生产冷构建。已删除且仅删除932那份已提交改动的自有备份stash；用户其它stash未动。

新冻结集成source `21391c92`全部447 C++／180E2E／445actual已启动；生产builder只重编改变的自有Network／parser后统一自有对象link exit0（此前33全source冷编译，非本次新冷编译、无旧ABI借用）。本轮全套尚未结束，不能拿隔离专项或Root旧438差分冒充。D旧fc741的444／175完整脚本已记录parser_phase1_test与DIV14两失败，仍在执行其余测试；其442全差分亦在运行。Root405的真实438差分exit0，但完整C++／E2E只439／170通过且两失败／整轮exit1；936修正非法START shorthand夹具、938保留错误状态控制后新组合必须全验。PROTO-01／02继续partial，总账273为22 complete／140 partial／96 unverified／15 deferred_by_user。接着核实SHOW transaction_isolation、TEMP ALTER／index catalog与sequence缺失文件恢复，再按总清单处理剩余项；不push，Actions仅本地disabled，安全／TDE专项继续deferred。

2026-10-02 第933／934／935／936项独立source commits：`cb7fcd47`保留无表SELECT原始quoted syntax边界；`7c24c41e`解码prepared statement／portal描述的quoted alias；`e3b8238d`让裸FALSE／NULL与常量boolean组合进入typed predicate，不得被旧token路径变成无WHERE，SELECT零行及UPDATE／DELETE零修改且TRUE确实作用两行；936原commit `044b78cf`、本branch独立cherry-pick `40e4e075`修正phase1 START控制，完整ISOLATION LEVEL为正控制、原缺LEVEL输入保留失败断言。33先真实全自有统一headers/source production build exit0，再按实际Network／Main改动进行自有增量build；最终28原生、25wire含完整协议、另独立完整协议及19不同PG actual全exit0，三个强专项最终全部exit0。旧31真实production与PG18.6强oracle各自保留反／正证明：E-string alias原变?column?，Describe alias原带外层quote，WHERE FALSE原返回所有行。33第一次强wire Simple全通过但Extended quote metadata失败；34独立强fixture查出WHERE FALSE零行控制失败；35第一次修复错把未定型NULL当text拒42804，已按AST裸NULL放行而不放宽显式typed nonboolean。所有原输入／断言保留，最终复验闭合；一个邻居循环误填不存在的delete测试而guard exit1，改为全文件preflight后的25真实入口完整重跑，不将失败循环叫通过。范围仍是局部lexer adapter、metadata与常量bool条件，不是整个SQL／binder／类型／DML完成。根405本轮438不同PG全cases=438 failed=0／exit0，但A440／171脚本实际439 C++＋170E2E通过，parser_phase1与div14 USE／portal两项失败／整体exit1保留，不能宣称整套通过。D继续冻结fc74170b（444／175／442）整套仍运行，尚未合入33–36。932恢复本agent三个tracked修改的专用stash后，保留3个untracked测试，已在e3b8238d底座合入上述修复、注册冲突保留四项；新共享header须全自有重编，当前正式builder进行中。937混合P/B→SimpleQ生命周期已真实PG强oracle exit0、旧fc741强wire错误Ready=T／exit1；SELECT、syntax、empty、CHAIN及BEGIN提升保持snapshot均由oracle确认，独立正式build进行中，尚未验收。该反例违反旧PROTO-01 Ready完整性及PROTO-02 implicit lifecycle，二项由complete重新标partial，checkbox同步取消；总账273现在22 complete／140 partial／96 unverified／15 deferred_by_user，跳过项不重启。不push，Actions仅本地disabled workflow。

2026-10-02 第931项 source `9f831267`，D合并 `6738a1e9`；独立oracle修复 `8bd9624a`：SQL正文未闭合block comment原来当EOF成功，旧930真实production强wire返回SELECT 1／exit1，PG18.6同一强oracle exit0保留。共享tokenizer新增checked lexical error，parse／Main／Simple整批与Extended Parse统一在执行之前拒42601；Parse-only＋Flush即发E并先abort，Simple有合法INSERT前缀也不得执行，failed block中syntax42601优先而合法SELECT仍25P02。强Simple／Parse-only×idle／Top／USER-SP、重复error、child恢复只保留pre-SP写、Top rollback无残留、原literal comment text全部exit0。新parser.h接口仍由全自有统一headers/source production真实编译link验收，19原生、20wire含完整协议、12不同PG actual全部exit0，无旧ABI对象混链。第一次actual循环由测试工具describe_statement先本地拒非法SQL而失败，保留日志；真实wire不需要psql的gdesc保护，独立commit改为双方发送相同原始SQL而让PG供给真实SQLSTATE，39工具单测通过，修正后12 actual重新全exit0，不隐藏值或错误差异。仅闭合该block lexical边界；932普通／E／quoted identifier／dollar未闭合当前误42703及plain trailing backslash漏查comment已真实PG确认，强oracle exit0／旧931 exit1，独立自有生产构建进行中；E-string alias等其它解析差距未算完成。D含927／930／907／931累计应444 C++／175E2E／442actual，集成全套待全自有重编；根／A继续冻结405ebff2（440／171／438），本轮完整C++／E2E和438PG仍运行，部分OK不冒充整套。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user，不push，Actions仅本地disabled workflow。

2026-10-02 第907项 source `78a3216d`，D合并 `9d925a89`：TEMP SERIAL原来只有匿名auto-increment，没有真实namespace／sequence identity，旧405完整production在currval报42P01，PG18.6同一强oracle exit0保留。现在TEMP表（含CTAS）进入真实session pg_temp_N catalog，表／序列relpersistence=t与PgDepend auto owned-by引用列，生成schema-qualified nextval default；真实OID/search_path解析、TEMP表rename与DROP、ON COMMIT DROP、session断开及startup stale namespace清理接入owned sequence生命周期。强Simple／Extended覆盖nextval／currval、显式NULL23502、TRUNCATE RESTART、USER-SP DROP后恢复、Top CREATE rollback、rename、双会话同名独立和ON COMMIT DROP全exit0；native核对真实OID／depend／文件和幂等cleanup。首轮全自有source／headers编译后因测试注册中途变化被最终content-hash guard拒绝，保留exit1；冻结后正式builder校验所有自有对象hash并link exit0，非借旧ABI、非新冷编译。13原生、14wire含完整协议、23不同PG actual全exit0；原native／actual循环因误填不存在的测试文件终止，修正文件名后完整重跑且distinct去重，不冒充失败循环通过。TEMP ALTER列／index catalog、qualified sequence DDL alias及缺失文件故障恢复尚需专项，未把本项当完整CAT-13／CAT-15／WAL。D累计应443 C++／174E2E／441actual，组合全自有重编仍待；根／A继续冻结405ebff2（440／171／438），本轮全套运行中。931未闭合block强协议与19 native／12 actual已通过、完整协议仍在运行；差分oracle原先误用psql descriptor本地拒语法错误，独立commit `8bd9624a`改为wire发送原SQL，39工具测试exit0。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user，不push，Actions仅本地disabled workflow。

2026-10-02 第930项 source `aa0eaf3d`，D合并 `1448d175`：SQL正文的block comment scanner在首个inner */结束，残留outer正文成为列名；CR结尾line comment也被误吃到EOF。旧完整928+929真实强wire报42703／exit1、PG18.6同一强oracle exit0保留。tokenizer在literal／identifier quote之外复用SqlTrivia的nested depth与CR／LF／EOF扫描，保持后续SQL字节、quoted comment text和dollar literal token。专项Simple／Extended含Describe、两／三层nested、CR／CRLF、精确rows／NULL-free datum／列名／OID／tag／Ready全exit0；native核对token流、projection数量／alias、quoted identifier／dollar字节／EOF line comment。全source真实自有统一headers production编译link、17 C++、18wire含专项和完整协议、10不同actual全部exit0。仅合法comment扫描闭合，未闭合interior block仍沿用旧lexer EOF策略，已列931严格词法拒绝任务，不把本项当完整lexer／parse analyzer完成。D累计应442 C++／173E2E／440actual；根/A继续冻结405ebff2（source5cf30d61）440／171／438：根正式production exit0、A完整脚本与根同源438全PG差分仍运行，不能以历史9f全433代替。907真实TEMP SERIAL catalog／namespace／depend／cleanup修复在独立自有production构建，PG强oracle通过、旧405currval42P01失败保留，尚未验收。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user，不push，Actions仅本地disabled workflow。

2026-10-02 第927项 source `7a5c8ff7`，D合并 `7e93c35c`：SET TRANSACTION旧substring入口接受缺LEVEL／裸READ COMMITTED并丢组合选项，非法syntax误XX000；真实旧925强wire exit1、PG18.6同一强oracle exit0保留。新增SetCharacteristics typed AST，复用BEGIN严格mode grammar并保持源顺序／roundtrip；Main逐mode调用isolation、read-only、deferrability时机检查，syntax42601、非法时机25001，failed block的无效SET优先42601而有效SET仍25P02。Idle合法SET no-op，Simple／Extended structured warning25P01和SET tag，Extended不为该独立utility伪起BEGIN；不修改默认模式。强oracle／本项目覆盖9非法输入×active／idle／failed、组合／comma／comments／重复options的真实写限制、snapshot后非法前序mode、USER-SP NOT DEFERRABLE、idle warning／tag／Ready全exit0。AST enum／parser接口虽未新增字段，本worktree仍真实全自有统一headers生产编译link；17 C++、18wire含专项和完整协议、10不同actual全exit0，无旧对象混链。SESSION CHARACTERISTICS、完整GUC restore／utility快照及batch状态仍未全实现，不将本项当整个事务／SQL族完成。D累计应441 C++／172E2E／439actual；根／A继续冻结405ebff2（source5cf30d61）：440／171完整脚本仍运行，根同源正式production exit0、438全PG差分进行中，不混927／930／907。907 TEMP SERIAL真实catalog与cleanup独立构建进行中，PG强oracle通过、旧405缺currval真实42P01失败保留，尚未验收；930内部nested-comment独立生产／17native／10actual通过，完整协议仍复验。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user，不push，Actions仅本地disabled workflow。

2026-10-02 第925项 source `2a169ccb`，D合并 `5cf30d61`：NOT DEFERRABLE即使同值仍须在首次query之前、USER-SP之外设置；旧完整926强wire误成功BEGIN／exit1、PG18.6同一强oracle exit0保留。新增只读时机检查，重复BEGIN／START按924源顺序校验deferrable option，已有sticky query snapshot或USER-SP报25001，warning仍先于Error；尚无query的Top、无query child结束后的Top允许NOT DEFERRABLE，child取query snapshot后rollback／release也不能清掉限制。DEFERRABLE=true仍明确0A000 unsupported，不宣称SSI safe-snapshot实现。独立全source真实自有统一headers production编译link、14 C++、15wire含专项和完整协议、9不同actual全部exit0。D合并保留leading-trivia与NOT DEFERRABLE两测试注册，无删除；累计应440 C++／171E2E／438actual，合并后整体验证尚待运行。根9f上一轮434／164／433真正全闭合，不拿旧binary代替新source。927SET TRANSACTION完整mode grammar、907 TEMP SERIAL真实namespace／catalog／owned sequence lifecycle及其余总清单继续；273仍24 complete／138 partial／96 unverified／15 deferred_by_user，不push，Actions仅本地disabled workflow。

2026-10-02 第928项 source `80308114`，D合并 `8dbfa949`：合法leading block／line／nested comments使生产SQL路由误42601，真实旧统一22+23强wire exit1、PG18.6最终v4强oracle exit0保留。共享SqlTrivia仅定位前导trivia边界，Main执行、parser分类与完整parse、wire keyword scanner统一入口；不替换SQL内部quoted datum／identifier，未闭合block拒42601、comment-only无执行。完整parse也从相同offset开始，防止分类已跳过nested prefix但AST仍把残留comment当projection。独立native核对offset、原body、单projection／alias／quoted token、trivia-only／unclosed输入；强Simple／Extended明确Describe portal，核对原literal／NULL／rows／3列名／tag／Ready，原包含semicolon的SQL保留，由独立929修复terminator额外projection。最初PG夹具未发Describe却误断言headers的失败、错误include_headers参数失败、产品leading nested-comment多列失败均保留；按真实wire请求补Describe而非伪造header，最终PGoracle／产品强测试全exit0。928 worktree初始全source真实自有统一headers production exit0，随后仅按改动parser做自有增量重编，最终同layout928+929组合14 C++、16wire含完整协议、9不同actual全exit0；不借26新private layout或旧ABI对象、不把增量验证叫新冷构建。D保留26snapshot／924Mode及28／29全部测试入口，注册冲突保留双方；当前集成应439 C++／170E2E／437actual，仍须全自有重编和本source全套，不能用已闭合根9f的434／164／433冒充。925守卫独立自有production／14native／9actual已通过，完整协议仍复验；927SET模式grammar、907 TEMP SERIAL、SQL内部nested-comment lexer／统一parse分析／catalog等总清单仍有差距。273总账仍24 complete／138 partial／96 unverified／15 deferred_by_user，不push，Actions仅本地disabled workflow。

2026-10-02 第929项 source `3ebfe0aa`，D合并 `66cfd79a`：无FROM SELECT的projection loop把尾部分号解析为额外表达式，Extended Describe因此比实际DataRow多一列；真实旧完整924强wire在原SQL分号输入返回第四列、exit1保留，PG18.6同一强oracle exit0。parser在projection列表遇到statement terminator停止，不改字面量内分号／注释字节；独立native和Simple／Extended强wire覆盖无terminator、分号、尾block／line comment，精确3列名、3 OID、NULL、原datum、tag和Ready。本worktree先真实全自有统一headers production编译link，再按实际parser改动自有增量重编；928+929最终14 C++、16wire（含两专项和完整协议）、9不同actual全exit0，明确是同layout组合验证，不冒称929单独冷构建；native最初误把LiteralExpr原始quoted token当decoded datum而失败，已按源码字段契约校正，强wire仍核对真实decoded bytes。原928 leading nested-comment metadata失败另外保留，并修复parser入口前导trivia一致性；未靠取消Describe或删semicolon避开。根/A冻结9f4033f6本轮现真正闭合：正式434 C++／164E2E脚本、根同source真实production及433 PG18.6差分cases=433 failed=0／433不同OK／exit0，已另存9f正式binary，历史427不是本轮证据。D含26+29而未算28时应438 C++／169E2E／436actual，新增28组合还须合并后全自有统一headers重编，不把隔离专项当整套。SQL／typed结果／Parse analyzer／catalog／事务等功能族仍partial，273总账仍24 complete／138 partial／96 unverified／15 deferred_by_user；不push，Actions仅本地disabled workflow。

2026-10-02 第926项 source `e677b523`，D合并 `57cc4078`：无表SELECT1／NULL／VALUES后的首次query snapshot未被记录，导致SET／重复BEGIN可非法更改isolation或RO→RW；旧统一22+23正式binary强wire exit1保留。新增Top-level sticky querySnapshotUsed，真实读路径与import snapshot置位，SELECT／VALUES等执行、Extended Parse分析和Bind规划提前记录；USER-SP rollback／release不撤销首次snapshot事实，Top ending与新BEGIN清理。内部beginSqlCommand刷新read view不等于query取snapshot，因此不能对SHOW／SET等utility误置位。真实PG18.6最终v3强oracle、修复后专项、13 C++、13相邻wire加完整协议、9不同actual均exit0。最初误假定Parse／Bind不取snapshot的oracle失败、把内部utility refresh当首次query的v1专项／wire／actual失败均保留；先真实全自有统一headers production编译link exit0，再仅用本worktree同layout自有对象按改动重编Table，最终production和全部上述门禁exit0，不冒称v2冷构建或借旧ABI。新增private TransactionContext字段／parser接口／inline guards，D合并后须全自有重编；924有序Mode与926同时保留，两新增E2E注册冲突手动保留双方，无删除测试。原前导注释SELECT42601另列928，原失败输入继续强oracle保留，不用删断言掩盖；其真实全自有production已exit0，但新专项发现列描述多一列，仍待修复和验收。根／A继续冻结9f4033f6：434 C++／164E2E正式脚本exit0，433同源全PG差分仍运行，部分OK不能当整套通过。D新组合应437 C++／168E2E／436actual。925 NOT DEFERRABLE、927 SET TRANSACTION grammar、907 TEMP SERIAL真实catalog／namespace／sequence lifecycle及其余总清单未完成，snapshot／GUC／协议功能族仍partial。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user，不push，Actions仅本地disabled workflow。

2026-10-01 第924项 source `6243f94f`，D合并 `fc477bc7`：BEGIN重复options只保留最终值，已有query后SERIALIZABLE再READ COMMITTED、READ ONLY再READ WRITE被误接受；旧完整922强wire exit1保留。AST追加源顺序Mode列表，parser仍保留最终值与presence兼容字段，但toString不再丢弃前序options；重复BEGIN逐项按原顺序调用isolation／read-only setter，非法中间变更也报25001，不能被后来no-op遮盖。不重新开始transaction或重置资源。真实PG18.6及本项目Simple／Extended、BEGIN／START、空白／comma、after-query错误、USER-SP无query仍拒RO→RW、ROLLBACK TO parent可继续写、未取snapshot允许先后变更、同级重复允许与warning-before-error全exit0。AST布局变化，本worktree全source真实自有统一headers production编译／link exit0；13 C++、专项、13相邻wire加完整协议、9不同actual全exit0，无旧ABI混链。完整DEFERRABLE／safe-snapshot运行时仍未实现，不能把其AST顺序保真当功能完成；925真实PG已核实NOT DEFERRABLE在已有query或USER-SP中须25001，而本项目仍误成功。另926已核实SELECT1／NULL／VALUES无表query也取得first snapshot，subabort后保持该事实；Parse分析及Bind规划同样可能取snapshot，最初误假定Parse／Bind不取snapshot的oracle失败保留并按PG源码／wire校正，最终新版oracle exit0，独立修复已启动全自有重编，尚未验收。D上一组合67218476的真实production及grammar／extended-abort／isolation／完整协议4项exit0，已另存binary，不能拿它当新924 source验证。根／A仍冻结9f4033f6（source0c9908bb）：正式434 C++／164E2E全脚本已exit0，根同源正式433 PG差分正运行，保持源码冻结不混922以后的修复，不拿历史427冒充。D新组合应436 C++／167E2E／435actual。907 TEMP SERIAL真实namespace／catalog／sequence lifecycle、GUC／portal provenance／SubXID／snapshot族及其余总清单未完成。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user，不push，Actions仅本地disabled workflow。

2026-10-01 第923项 source `fc550090`，D合并 `f8763b32`：Parse／Bind／Execute／Describe等直接ErrorResponse没有走SQL执行的abort边界，真实旧完整921在Bind 22P02后仍持row／xact locks、Sync返回T；最终旧强schedule exit1保留。公共扩展协议错误出口在发送E前回滚latest USER-SP或Top、清相应notification／advisory transaction ownership，保留logical failed block与session advisory locks，skip-until-Sync规则不变。真实PG18.6及本项目7种前执行错误×有／无USER-SP，未发Sync前双连接row／advisory释放、pre-SP锁保留、SESSION锁保留、undo、25P02、ROLLBACK TO恢复、原named statement／portal在parent中存活，另Flush-only implicit Execute后的Bind错误立即abort且Sync转I，最终强oracle全部exit0。全自有统一headers/source的真实production build及其正式专项／完整协议exit0。此前显式组合严格逐文件cmp所有共享source／headers、核对compiler options，用921真实production对象和自有Network重编验证，7 native、9不同actual、11相邻wire及完整协议均exit0；不将该组合冒称独立冷构建。完整协议最初沿用旧duplicate Parse／Bind错误后T且Sync可直接复用对象的错误预期而失败；已由真实PG独立验证E及ROLLBACK TO恢复，再保留原对象值21与dedicated SQLSTATE断言，不削弱错误检查。新增强schedule列入标准E2E。22+23组合应435 C++／166E2E／434actual；根／A仍冻结9f4033f6，仅根正式build已exit0，A全脚本尚未全部结束，433同源全差分待开始，不把旧427或隔离修复当新整套。D两新增测试注册同位置merge冲突已同时保留，其余Network／full protocol改动自动合并后检查，无删除测试。924 ordered BEGIN modes、907真实TEMP SERIAL namespace／catalog／owned sequence生命周期、true SubXID／portal provenance／Parse analyzer及其余总清单继续，功能族不宣称完成。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user，不push，Actions仅本地disabled workflow。

2026-10-01 第922项 source `232f9d08`，D合并 `7cc1be3c`：BEGIN裸READ COMMITTED／缺LEVEL被误接受，合法comma transaction modes被拒且syntax误报XX000；旧完整921强wire误成功BEGIN／exit1保留。parser要求完整ISOLATION LEVEL，接受空白或合法comma分隔、拒绝leading／double／trailing comma，按PG接受初始block的重复mode；主入口报42601，failed block的非法BEGIN仍优先42601，有效BEGIN保持25P02，不误重启或发warning。真实PG18.6与本项目Simple／Extended、BEGIN／WORK／TRANSACTION／START、comments、合法／非法mode list、READ ONLY写限制、failed USER-SP恢复及rows／tag／Ready均exit0；本worktree全自有正式production build、12 C++、专项、11相邻wire加完整协议、9不同actual亦exit0。原presence和完整协议中的非法shorthand正控制改为完整LEVEL，并保留原输入42601反控制，不移除失败断言。组合应435 C++／165E2E／434actual。根／A仍冻结9f4033f6（source0c9908bb）434／164／433：根真实生产构建exit0，A全脚本仍运行，433同源全差分尚未开始，不混922／923或拿旧427冒充新source。923已核实Parse／Bind等错误未在ErrorResponse前abort导致锁保留、Sync错误T，独立修复已有PG oracle／组合／全自有production通过，正式专项和完整协议正在复验，不在922验收范围。另924真实PG确认已有query后BEGIN SERIALIZABLE再READ COMMITTED、READ ONLY再READ WRITE须在前序非法变更报25001；当前AST最终值折叠误忽略，继续保留有序options，不宣称完整重复mode／GUC族完成。907 TEMP SERIAL、SubXID／portal／snapshot／command-counter／SSI及其余总清单未完成。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user，不push，Actions仅本地disabled workflow。

> 审计日期：2026-08-23
> 项目基线：`5a31aa3` 加审计时的动态工作区（`src/main.cpp`、`src/parser/parser.cpp`、`src/commands/DmlExecutor.{h,cpp}` 有尚未提交的修改）
> 对标基线：PostgreSQL 18.6
> 文档定位：当前差距的权威清单；每项的代码落点、I/O 保真、性能和验收方案见 [postgresql-18-implementation-blueprint.md](postgresql-18-implementation-blueprint.md)；旧的 `all-gaps-todo.md`、`feature-gaps.md` 和 `postgresql-comparison.md` 保留作历史记录。

> 2026-09-08 续做：总清单尚未完成。最新逐项状态见 [gap-progress.json](gap-progress.json)，执行顺序见 [总清单执行计划](full-gap-execution-plan.md)。原审计条目须按当前代码重新核实；局部 bug 修复不等于整个功能族完成。

2026-10-01 第921项 source `a858f437`，D合并 `0c9908bb`：已有query后同级SET TRANSACTION／重复BEGIN isolation被误拒25001，不同level在没有query的USER-SP内反而误接受；旧完整19强wire、actual failed=1及全同source／header真实production对象重链专项native断言134保留。setter对live transaction相同enum提前成功且不清snapshotAcquired；真正修改同时检查USER-SP与已有读／写／DDL／snapshot状态，内部statement marker不误算user subtransaction。真实PG18.6与本项目最终四种isolation×Simple／Extended、same-level after-query／USER-SP、different-level before-query child拒绝、RELEASE后Top允许修改、双连接REPEATABLE READ在SET／BEGIN后仍看旧snapshot及结束后见新行，全部exit0。Header inline实现变化，无新增字段或disk改变，但本worktree全source production真实自有统一headers冷编译／link exit0；11 C++、专项、12wire邻居／完整协议及9不同actual亦全部exit0，无旧ABIs或假cache。组合应434 C++／164E2E／433actual。冻结根／A2d2db1d0本轮真正全闭合：429 C++／158E2E脚本exit0、根同源正式427 PG18.6差分cases=427 failed=0／427不同[OK]／exit0；根已保留 `build/dbms_bigint_minimum_signed_dml_formal_2d2db1d0`。之后方可快进并冻结新同源组合，再跑其真正全套和433全差分，旧427不能替代新source验证。922已read-only wire核实BEGIN READ COMMITTED、缺LEVEL等非法PG语法被接受，而合法comma transaction modes尚未支持／syntax误XX000；独立grammar核实继续，不在本项完成范围。完整snapshot／command-counter／SSI／GUC／lexer及907temp SERIAL等其余总清单仍未完成。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user，不push，Actions仅本地disabled workflow，不声明远端已收到未push的更改。

2026-10-01 第920项 source `3e3e144e`，D合并 `071bbfea`：保存点内SET READ ONLY后，ROLLBACK TO及RELEASE没有恢复parent mode；旧完整18强wire被错误25006拒绝write、actual failed=1，另外child READ ONLY可非法改READ WRITE，真实旧差分亦保留。SavepointState保存入栈readOnly，在成功rollback／release named subtree恢复对应parent值；READ ONLY向READ WRITE切换时USER-SP guard返回失败，内部statement marker不冒充用户block。真实PG18.6及本项目最终Simple／Extended、正常和failed ROLLBACK TO、failed COMMIT CHAIN、subcommit、nested／duplicate-SP parent差异、RO child禁止放宽与正常Top控制全exit0。初始RELEASE仍保持child mode的错误PG预期failed=1保留，源码xact.c的prevXactReadOnly明确在subcommit／subabort均恢复后，按真实oracle修正夹具，未把误假设当完成。新增private in-memory字段，因此本worktree所有source统一自有headers真实冷production build exit0，11 C++、专项、11wire邻居／完整协议及9不同actual全部exit0，heap／WAL故障native所报rollback incomplete为原断言覆盖的预期诊断，不冒充失败或全库sanitizer。无磁盘格式变化。组合应433 C++／163E2E／432actual；根/A仍冻结2d正式429／158已exit0，427同源差分仍运行，已输出部分[OK]但未声明整轮成功、不混新source。921已真实PG核实相同isolation level在snapshot后和USER-SP内合法且必须保留原read view，不同level在USER-SP中即使未query也须25001；旧完整19 strong wire／actual failed=1及其全同source／header真实production对象重链的新native assert134保留。921隔离修改setter早返回不清snapshot并约束USER-SP，正基于920新layout统一自有全重编；其PG同一oracle含四种level、Simple／Extended、双连接repeatable-read可见性控制已exit0，新生产尚待验证，不声明完成。完整isolation／GUC／resource owner／SubXID／readonly族、907temp SERIAL及其余总清单未完成。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user，不push，Actions禁用。

2026-10-01 第919项 source `35d4c7c4`，D合并 `1f440223`：重复BEGIN没有显式mode却清READ ONLY，且AdvisoryLockManager::beginTransaction释放旧transaction locks并重置USER-SP边界；真实旧13 mode误成功INSERT及旧完整18双连接try-lock=t／exit1保留。AST记录isolation／read-only／deferrable是否显式指定，BEGIN／START toString保留options；live block只应用显式mode且检查SET TRANSACTION setter结果，不重做engine begin、advisory／notification资源初始化和CHAIN origin。warning25001从完整有效AST＋显式block状态结构化产生，先于显式选项25001错误，不由stdout推断；Extended implicit block被用户BEGIN提升时不误发warning。START TRANSACTION tag按真实PG独立输出，不再冒充BEGIN。最终真实PG强oracle／本项目覆盖Simple／Extended、双连接pre／post-SP锁与ROLLBACK TO、无显式选项模式、显式模式生效、snapshot后修改错误、warning code及隐式提升全部exit0；11 C++、专项、10wire邻居／完整协议及9不同actual也全部exit0。AST新增字段，本worktree所有生产source真实自有统一headers冷编译／link exit0，无旧ABI借用、无disk改变。最初PG夹具错误假定重复BEGIN显式选项也忽略，随后tag错误均保留为夹具失败，修正为PG实际规则后完整强oracle通过，未把夹具错误当产品成功。组合应432 C++／162E2E／431actual；根/A仍冻结2d正式429／158已exit0，427同源差分仍运行，不混新source。920已真实PG核实read-only在subabort和subcommit均恢复prevXactReadOnly及RO child禁止READ WRITE；旧18 wire和actual failed=1保留。920保存／恢复字段与setter USER-SP guard、嵌套native／wire测试已隔离实现，新增SavepointState字段需全自有统一header重编，正式build进行中，未声明完成；初始RELEASE应保持child mode的错误oracle预期保留并按真实PG修正。完整GUC／subtransaction／read-only／SQL／协议、907temp SERIAL及其余总清单未完成。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user，不push，Actions禁用。

2026-10-01 第918项 source `8df2e3e0`，D合并 `48c737d4`：真实PG18.6确认CHAIN-origin top-level abort恢复链开始时的READ ONLY baseline，而普通BEGIN-origin abort仍恢复默认read-write；正常ending继承当前SET后的mode，failed user-SP仍保留live parent mode。新Session分别记录origin与baseline，ending在engine已物理abort但logical E时仅取CHAIN baseline，新的普通BEGIN与plain／NO CHAIN结束清理；不把后来SET的mode当abort baseline。最终强Simple／Extended×COMMIT／ROLLBACK、SET READ WRITE／READ ONLY两方向、成功ending、plain重置和普通BEGIN控制oracle／本项目均exit0；原915及完整913真实production误成功INSERT／exit1保留。Session新增字段，因此918 worktree真实全source production冷构建（main／Table／Network／parser等全为自有统一headers）exit0；10 C++、专项、9wire邻居／完整协议及9不同actual全部exit0，无ABI旧对象混用、无disk格式改变。组合应431 C++／161E2E／430actual，完整read-only／default GUC／isolation／DEFERRABLE族仍partial。冻结根／A2d2db1d0正式全套429 C++／158E2E已真正exit0（`/tmp/dbms-tests-bigint-minimum-signed-dml-915-916.log`），根同源427全差分正在运行（`/tmp/dbms-pgdiff-bigint-minimum-signed-dml-915-916.log`），尚未声明其成功、不混入917／913／918。919重复BEGIN无显式read mode却清掉READ ONLY、重置transaction advisory ownership已按PG建立独立强schedule；PG实际会应用显式BEGIN选项而非全部忽略，保留最初错误fixture预期失败并校正为真正规则，另START TRANSACTION tag须独立，不冒充BEGIN。919修复及AST option-presence新增layout全自有重编进行中。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user；907temp SERIAL及其余总清单仍未完成，不push，Actions禁用。

2026-10-01 第913项 source `8e985f7b`，D合并 `aba1ab67`：没有显式block的AND CHAIN及Extended Execute／Flush隐式block均须25P01；正常和logical failed block分别保留合法CHAIN语义。AST解析完整chain／no-chain选项、WORK／TRANSACTION／END／ABORT及注释分割，ending handler拒绝非ending AST；ROLLBACK WORK／TRANSACTION TO用结构性token分派到USER savepoint，Simple／Extended失败保存点恢复后保留之前write和T状态，不再误作整段rollback。failed COMMIT／END改写前解析完整命令，非法suffix保持42601／E／25P02，不丢尾部误启动新chain。旧915 idle CHAIN误成功及13中间版failed-invalid-ending误成功真实exit1保留，真实PG18.6最终同一强oracle全部exit0。AST／Session layout有新增字段，本worktree独立真实全source production构建后按实际修改main／parser／Network增量builder重编，最终统一自有headers／对象，无借用旧ABI缓存；最终10 C++、专项＋8wire邻居／完整协议及9不同actual全部exit0，原失败未放宽。应431 C++／160E2E／429actual；根／A仍冻结2d2db1d0，根正式production exit0，A429 C++已通过、158E2E继续，427全差分尚未启动，未混入917／913。918 CHAIN-origin top-abort READ ONLY baseline已真实PG核实Simple／Extended及SET两方向、正常结束、plain结束重置；旧13仍误接受INSERT，918隔离修复与全自有重编进行中，未宣告完成。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user；完整事务／协议、907temp SERIAL及其余总清单仍未完成，不push，Actions禁用。

## 1. 结论

这个项目已经不是“玩具 SQL 解析器”：它有约 9.2 万行 C/C++ 核心代码、8 KiB 页式堆、Buffer Pool、FSM/VM、TOAST、WAL、CLOG、MVCC、锁管理器、B+Tree/Hash/GIN/BRIN/GiST/SP-GiST/Bloom 风格索引、Volcano 执行器、PostgreSQL v3 协议子集、SCRAM、PITR 子集，以及 158 个 C++ 测试文件和 7 个 Python 协议/E2E 测试文件。当前源码可成功编译。

但它目前更适合定位为：**功能面很宽、实现深度不均的单机关系数据库原型/教学型内核**，还不能定位为 PostgreSQL 的兼容替代品，更不能承诺 PostgreSQL 级生产可靠性。

真正的差距不是再补几十条 SQL，而是以下系统性问题：

1. SQL 仍存在 typed AST/Volcano 与 `main.cpp` 字符串执行器两套路径，复杂查询和 DML 会回退 legacy 逻辑。
2. catalog 不是所有对象与执行语义的唯一事实源；大量能力由 schema 文件、sidecar 文件、兼容对象文件和虚拟系统表拼接。
3. 很多 `CREATE/ALTER/DROP` 只是在 `.pg_compat_objects` 中保存定义，执行时并没有 PostgreSQL 对应能力。
4. DDL 会在若干路径隐式提交，和 PostgreSQL 的事务化 DDL 不兼容。
5. SERIALIZABLE 只是 SSI 子集；索引范围 predicate lock、完整 rw-conflict 图和 safe snapshot 不完整。
6. 除 B+Tree/Hash 的部分路径外，索引结构、并发算法、WAL、vacuum/opclass 语义远未达到 PostgreSQL 访问方法水平。
7. 没有物理流复制、hot standby、同步复制、真正 WAL 驱动且 wire-compatible 的逻辑复制。
8. 缺少 PostgreSQL 扩展体系、FDW 运行时、fmgr、hooks、动态后台 worker 和工具生态。
9. 协议、catalog、错误码和工具链不足以让普通 PostgreSQL 客户端/ORM/运维工具无差别工作。
10. 没有跨版本升级承诺和存储格式兼容路径；当前文档明确要求旧格式导出后重建。

因此，不建议给出一个“完成了 PostgreSQL 的百分之多少”的数字。按语法关键词计数会严重高估成熟度：AST 中出现命令名、能够保存一条兼容对象记录、能够通过单线程 happy-path 测试，都不等于实现了 PostgreSQL 语义。

## 2. 审计口径和证据

### 2.1 状态定义

| 状态 | 含义 |
|---|---|
| `P` | 有真实执行路径，但仅为 PostgreSQL 语义子集；仍是差距项 |
| `S` | 只有解析、分类、定义保存或兼容对象骨架；不能视为对应功能已实现 |
| `X` | 当前生产入口没有可用实现 |

本清单中的“完整”是指：覆盖 PostgreSQL 18 的全部 183 条 SQL 命令，并覆盖类型、表达式、catalog、优化器、事务、存储、复制、协议、安全、运维和扩展等能力域。PostgreSQL 内置函数、GUC、catalog 列和错误码有数千项，本文件按完整的**功能族**列出，不逐个复制官方手册中的每个函数重载或每一列定义。

### 2.2 本次实际检查

- 审阅了 `README.md`、发布说明、构建脚本、现有差距文档、`src/` 全部模块目录和测试清单。
- 重点核对 `parser/ast`、DDL/DML executor、Volcano planner/executor、catalog/type registry、存储/WAL/锁、网络协议、复制和 PL/pgSQL 实现。
- `./scripts/build.sh` 编译通过；本机未检测到 OpenSSL，因此构建的是 TLS stub。此次没有重新执行完整回归套件，不能把历史文档中的 PASS 数当作本次验证结果。
- 审计期间工作区源码仍在变化；本次只写入本文、README 文档索引和旧清单的历史标记，没有修改这些在途源码或 `v0.2.0/` 未跟踪目录。

### 2.3 主要源码证据

| 证据 | 说明 |
|---|---|
| `src/main.cpp` | 仍包含巨型 SQL 预处理/字符串分发、legacy 查询与 DDL/DML 兼容路径、输出文本捕获 |
| `src/parser/ast.h`, `src/parser/parser.cpp` | 递归下降 parser 和宽命令枚举；“可分类/可生成 AST”不代表可执行 |
| `src/commands/DdlExecutor.cpp` | typed DDL 只接管明确子集；其他命令回退 legacy；多个 DDL 路径先隐式提交 |
| `src/commands/DmlExecutor.cpp` | typed DML 的受支持边界和大量显式 fallback/unsupported 分支 |
| `src/executor/ExecutionPlan.cpp` | Volcano 算子、简化成本模型；明确缺 index-only scan 等路径 |
| `src/catalog/` | 核心 catalog/OID 框架，但对象全集和 catalog 驱动执行未完成 |
| `src/commands/TableManage.cpp` | 主存储引擎以及大量 sidecar、虚拟 catalog、DDL/DML/事务实现集中点 |
| `src/storage/`, `src/transaction/` | 页、Buffer Pool、WAL、CLOG、FSM/VM、TDE、锁和事务基础 |
| `src/replication/LogicalDecoder.cpp` | 写路径采集式逻辑变更，不是 PostgreSQL WAL 解码/复制协议 |
| `src/network/NetworkServer.cpp` | PostgreSQL protocol 3.0 子集、SCRAM、扩展查询和受限 binary I/O |
| `src/utils/plpgsql.cpp` | 最小 PL/pgSQL 解释器，不是 PostgreSQL PL/pgSQL 完整运行时 |

## 3. 当前能力画像

| 维度 | 当前可确认能力 | 对 PostgreSQL 的判断 |
|---|---|---|
| SQL 前端 | 递归下降 parser、typed AST、较宽语法面 | 复杂语句仍依赖文本改写和 legacy 分支，缺统一 parse/analyze/rewrite 管线 |
| DDL/DML | 常见表、索引、视图、角色、基础 INSERT/UPDATE/DELETE/MERGE | 常用子集可用；对象全集、复杂语义、事务性和依赖一致性不完整 |
| 查询执行 | Volcano 算子、三类 join、聚合、集合操作、窗口、部分并行执行 | 有真实执行器；planner、spill、参数化路径、复杂子查询等差距很大 |
| 存储 | 8 KiB slotted page、heap tuple header、FSM/VM、TOAST、Buffer Pool | 是真实内核基础；仍缺 PG 的成熟格式、vacuum/freeze、全资源 WAL 和长期兼容性 |
| 事务 | WAL/CLOG/MVCC/savepoint/2PC 子集、死锁检测、SSI 子集 | 不能声称 PostgreSQL 等价的 ACID/Serializable/DDL transaction |
| 索引 | 多种访问方法名称和若干真实候选扫描 | 除基础 B+Tree 外，多数是专用 sidecar/简化结构，不是 PG AM/opclass 实现 |
| 协议 | Startup、SCRAM、Simple/Extended Query 子集、部分 binary I/O | 可连接部分客户端；不满足完整 libpq、COPY、portal、replication protocol |
| 复制恢复 | crash recovery、WAL 归档、单时间线 PITR 子集、逻辑变更流 | 无可用 physical standby；逻辑流不兼容 pgoutput 且不能 WAL replay |
| 安全 | 角色、基础 ACL/RLS、pg_hba 子集、SCRAM、TLS wrapper | 认证方法、TLS/channel binding、对象 ACL、安全上下文均不完整 |
| 扩展生态 | 兼容对象记录、少量内置 hook-like C++ 接口 | 无 PostgreSQL extension/fmgr/FDW/background worker 生态 |

## 4. P0：在宣称“可替代 PostgreSQL”前必须关闭

- [ ] **P0-01：统一 SQL 执行管线。** 所有语句必须经过 lexer/parser → parse analysis/binder → rewrite → planner → executor，删除 `main.cpp` 中会改变语义的字符串重写和复杂 SQL legacy fallback。
- [ ] **P0-02：结构化结果与错误。** 执行器返回 typed rows、command tag、warning/error、SQLSTATE 和 diagnostics，禁止以捕获 `std::cout` 文本作为协议结果来源。
- [ ] **P0-03：catalog 成为唯一事实源。** 表、列、索引、类型、函数、约束、权限、依赖、统计、复制对象全部以事务化 catalog 为准，清理重复 sidecar/虚拟表拼装状态。
- [ ] **P0-04：完整事务化 DDL。** 去掉 DDL 隐式提交；支持语句原子性、事务回滚、savepoint、并发 DDL 锁、catalog WAL 和 crash recovery。
- [ ] **P0-05：完整持久性证明。** heap、所有索引、catalog、FSM/VM/TOAST、序列、统计、复制槽和配置状态均遵守 write-ahead/fsync/原子发布规则，并通过系统化 kill-point 测试。
- [ ] **P0-06：索引 WAL 重构。** 停止依赖整文件 before/after image 或未记录增量的 sidecar；为每种生产访问方法实现 page/logical WAL、redo、split recovery 和 vacuum cleanup。
- [ ] **P0-07：完整 MVCC/SSI。** 补齐物理索引范围 predicate lock、完整 rw-dependency 图、safe snapshot、DEFERRABLE read-only transaction 和精确 serialization failure。
- [ ] **P0-08：XID 生命周期。** 实现 wraparound 防护、freeze/all-frozen、MultiXact、oldest xmin horizon、长事务/复制槽对 vacuum horizon 的影响。
- [ ] **P0-09：单实例并发与故障隔离。** 当前明确不支持两个进程共享数据目录；需要可靠 postmaster/backend 隔离或明确定义并证明另一套共享状态架构。
- [ ] **P0-10：真实备份和恢复链。** 在线一致 base backup、backup manifest、增量备份、归档恢复、多 timeline、恢复目标和校验工具必须闭环。
- [ ] **P0-11：物理复制和 hot standby。** WAL sender/receiver、startup/recovery process、standby query、复制槽保留、同步/quorum、promotion/timeline 和故障切换。
- [ ] **P0-12：wire protocol 合规。** 完整错误/notice、portal、COPY、cancel、SSL negotiation、binary type I/O、pipeline 和 replication protocol，建立 libpq/psql/JDBC/ORM 兼容矩阵。
- [ ] **P0-13：权限边界统一。** 每条 SQL、函数、视图、触发器、COPY、大对象、schema/search_path 和维护命令都经过同一 owner/ACL/RLS/security context 检查。
- [ ] **P0-14：资源治理。** work_mem/temp spill、连接/worker/内存/I/O 限额、取消与超时传播、磁盘满/文件描述符耗尽/OOM 的 fail-safe 行为。
- [ ] **P0-15：可升级性。** 定义 catalog/storage version、升级工具和兼容窗口；不能继续以“旧目录导出后重建”作为生产升级策略。
- [ ] **P0-16：差分兼容测试。** 对同一 SQL/并发 schedule/崩溃点在 PostgreSQL 18 与本项目上做结果、SQLSTATE、catalog 和持久化差分。

## 5. PostgreSQL 18 全部 183 条 SQL 命令审计

这里的 `P` 也仍然代表“有差距”。当前没有任何一条命令可以仅凭现有测试证据宣称覆盖 PostgreSQL 的全部语法、权限、并发、错误和 catalog 语义。

### 5.1 ALTER 命令（42/42 已列）

| 状态 | 命令 | 主要差距 |
|---|---|---|
| `P` | `ALTER COLLATION`, `ALTER DATABASE`, `ALTER DEFAULT PRIVILEGES`, `ALTER DOMAIN`, `ALTER GROUP`, `ALTER POLICY`, `ALTER ROLE`, `ALTER SEQUENCE`, `ALTER STATISTICS`, `ALTER SYSTEM`, `ALTER TABLE`, `ALTER TABLESPACE`, `ALTER TYPE`, `ALTER USER` | 有具体 handler，但只覆盖选项子集；owner/ACL、依赖、并发锁、事务回滚、catalog 一致性和 PostgreSQL 错误语义不完整 |
| `S` | `ALTER AGGREGATE`, `ALTER CONVERSION`, `ALTER EVENT TRIGGER`, `ALTER EXTENSION`, `ALTER FOREIGN DATA WRAPPER`, `ALTER FOREIGN TABLE`, `ALTER FUNCTION`, `ALTER INDEX`, `ALTER LANGUAGE`, `ALTER LARGE OBJECT`, `ALTER MATERIALIZED VIEW`, `ALTER OPERATOR`, `ALTER OPERATOR CLASS`, `ALTER OPERATOR FAMILY`, `ALTER PROCEDURE`, `ALTER PUBLICATION`, `ALTER ROUTINE`, `ALTER RULE`, `ALTER SERVER`, `ALTER SUBSCRIPTION`, `ALTER TEXT SEARCH CONFIGURATION`, `ALTER TEXT SEARCH DICTIONARY`, `ALTER TEXT SEARCH PARSER`, `ALTER TEXT SEARCH TEMPLATE`, `ALTER TRIGGER`, `ALTER USER MAPPING` | 多数只修改 `.pg_compat_objects` 定义/owner/name，未改变对应运行时能力 |
| `X` | `ALTER SCHEMA`, `ALTER VIEW` | parser 枚举存在，但当前主执行入口没有可确认的 PostgreSQL 语义实现 |

### 5.2 CREATE 命令（42/42 已列）

| 状态 | 命令 | 主要差距 |
|---|---|---|
| `P` | `CREATE COLLATION`, `CREATE DATABASE`, `CREATE DOMAIN`, `CREATE FUNCTION`, `CREATE GROUP`, `CREATE INDEX`, `CREATE MATERIALIZED VIEW`, `CREATE POLICY`, `CREATE PROCEDURE`, `CREATE PUBLICATION`, `CREATE ROLE`, `CREATE SCHEMA`, `CREATE SEQUENCE`, `CREATE STATISTICS`, `CREATE TABLE`, `CREATE TABLE AS`, `CREATE TABLESPACE`, `CREATE TRIGGER`, `CREATE TYPE`, `CREATE USER`, `CREATE VIEW` | 常用子集有真实对象/数据路径；仍缺完整对象模型、依赖、权限、并发、事务和 option 语义 |
| `S` | `CREATE ACCESS METHOD`, `CREATE AGGREGATE`, `CREATE CAST`, `CREATE CONVERSION`, `CREATE EVENT TRIGGER`, `CREATE EXTENSION`, `CREATE FOREIGN DATA WRAPPER`, `CREATE FOREIGN TABLE`, `CREATE LANGUAGE`, `CREATE OPERATOR`, `CREATE OPERATOR CLASS`, `CREATE OPERATOR FAMILY`, `CREATE RULE`, `CREATE SERVER`, `CREATE SUBSCRIPTION`, `CREATE TEXT SEARCH CONFIGURATION`, `CREATE TEXT SEARCH DICTIONARY`, `CREATE TEXT SEARCH PARSER`, `CREATE TEXT SEARCH TEMPLATE`, `CREATE TRANSFORM`, `CREATE USER MAPPING` | 主要是解析或兼容对象/独立元数据记录；没有相应 executor、AM、FDW、extension、rewrite 或 subscriber 运行时 |

### 5.3 DROP 命令（43/43 已列）

| 状态 | 命令 | 主要差距 |
|---|---|---|
| `P` | `DROP COLLATION`, `DROP DATABASE`, `DROP DOMAIN`, `DROP FUNCTION`, `DROP GROUP`, `DROP INDEX`, `DROP MATERIALIZED VIEW`, `DROP OWNED`, `DROP POLICY`, `DROP PROCEDURE`, `DROP PUBLICATION`, `DROP ROLE`, `DROP SCHEMA`, `DROP SEQUENCE`, `DROP STATISTICS`, `DROP TABLE`, `DROP TABLESPACE`, `DROP TRIGGER`, `DROP TYPE`, `DROP USER`, `DROP VIEW` | 实际删除范围不一；`CASCADE/RESTRICT`、跨对象依赖、owner、并发和事务回滚不完整；`DROP OWNED` 主要清权限记录，不是 PG 对象所有权语义 |
| `S` | `DROP ACCESS METHOD`, `DROP AGGREGATE`, `DROP CAST`, `DROP CONVERSION`, `DROP EVENT TRIGGER`, `DROP EXTENSION`, `DROP FOREIGN DATA WRAPPER`, `DROP FOREIGN TABLE`, `DROP LANGUAGE`, `DROP OPERATOR`, `DROP OPERATOR CLASS`, `DROP OPERATOR FAMILY`, `DROP ROUTINE`, `DROP RULE`, `DROP SERVER`, `DROP SUBSCRIPTION`, `DROP TEXT SEARCH CONFIGURATION`, `DROP TEXT SEARCH DICTIONARY`, `DROP TEXT SEARCH PARSER`, `DROP TEXT SEARCH TEMPLATE`, `DROP TRANSFORM`, `DROP USER MAPPING` | 多数只删除兼容对象记录；没有可删除的真实运行时对象 |

### 5.4 其余命令（56/56 已列）

| 状态 | 命令 | 主要差距 |
|---|---|---|
| `P` | `ABORT`, `ANALYZE`, `BEGIN`, `CALL`, `CHECKPOINT`, `CLOSE`, `CLUSTER`, `COMMENT`, `COMMIT`, `COMMIT PREPARED`, `COPY`, `DEALLOCATE`, `DECLARE`, `DELETE`, `DISCARD`, `DO`, `END`, `EXECUTE`, `EXPLAIN`, `FETCH`, `GRANT`, `INSERT`, `LISTEN`, `LOCK`, `MERGE`, `MOVE`, `NOTIFY`, `PREPARE`, `PREPARE TRANSACTION`, `REASSIGN OWNED`, `REFRESH MATERIALIZED VIEW`, `REINDEX`, `RELEASE SAVEPOINT`, `RESET`, `REVOKE`, `ROLLBACK`, `ROLLBACK PREPARED`, `ROLLBACK TO SAVEPOINT`, `SAVEPOINT`, `SECURITY LABEL`, `SELECT`, `SELECT INTO`, `SET`, `SET CONSTRAINTS`, `SET ROLE`, `SET SESSION AUTHORIZATION`, `SET TRANSACTION`, `SHOW`, `START TRANSACTION`, `TRUNCATE`, `UNLISTEN`, `UPDATE`, `VACUUM`, `VALUES` | 均只有子集；详细差距见后续各能力域 |
| `S` | `IMPORT FOREIGN SCHEMA`, `LOAD` | FDW 没有运行时；`LOAD` 只记录“loaded_library”，并未 `dlopen`/加载 PostgreSQL shared library |

汇总：`P=110`、`S=71`、`X=2`，合计 183。这个统计只说明审计覆盖完整，不代表实现完成度。

## 6. SQL 前端、名称解析和语义分析

- [ ] **SQL-01** 用正式 grammar/lexer 覆盖 PostgreSQL 词法规则，去掉基于 `substr/find/tokenize` 的二次解析。
- [ ] **SQL-02** 支持完整 quoted identifier、Unicode escape identifier/string、escape string、bit/hex literal、标准 conforming strings 和嵌套注释。
- [ ] **SQL-03** 将当前 SQL 预处理中的 boolean、array、CASE、ANY/ALL 等文本 rewrite 移到 AST/analyze 阶段。
- [ ] **SQL-04** 实现 binder：relation/column/function/operator/type 的 namespace lookup、歧义检测和逐层 scope。（已修复普通 JOIN 的 quoted column 投影绑定；物化 LATERAL 的外层可见列/限定叶列、USING 合并键、歧义和别名隐藏已有独立协议回归，但不是通用逐层 binder，仍 partial。见 `docs/issue-query-projection-binding-and-lateral-scope.md`。）
- [ ] **SQL-05** 完整实现 schema-qualified 名称、`search_path`、临时 schema、`pg_catalog` 隐式搜索和跨 schema 同名对象。
- [ ] **SQL-06** 实现 PostgreSQL identifier folding 和最大长度规则，所有 catalog/物理文件名使用同一规范。
- [ ] **SQL-07** 实现 unknown literal、参数类型推断、preferred type、category 和 common supertype 选择。
- [ ] **SQL-08** 实现 assignment/implicit/explicit cast graph；当前 `CREATE CAST` 记录不能驱动 analyzer/executor。
- [ ] **SQL-09** 实现函数/过程/聚合/operator 重载解析、默认参数、variadic、多态伪类型和 schema 可见性。
- [ ] **SQL-10** 实现 collatable expression 推导、collation conflict 和 provider/version 处理。
- [ ] **SQL-11** 对所有语句做尾随 token 检查、准确 error position、hint/detail/context 和 PostgreSQL SQLSTATE 映射。
- [ ] **SQL-12** 移除以空格分割行/列的内部结果格式；它不能正确承载含空格、NULL、转义和复合值的数据。
- [ ] **SQL-13** 统一 SQL 级 `PREPARE ... AS ... $n` 与协议 prepared statement 的类型、计划、失效和 portal 生命周期。
- [ ] **SQL-14** 实现 rewrite system 所需的 query tree 复制、权限标记和依赖失效。（已核实未实现，partial；RULE/Event Trigger 入口 fail-closed，尚无 rewrite runtime，见第986项。）

## 7. Catalog、OID、对象和 DDL

- [ ] **CAT-01** 将 catalog 从 CSV/sidecar 缓存提升为 WAL/MVCC 管理的普通系统关系。现为每库内存vector/hash加每目录独立 `.cat` CSV、显式/析构持久化（第996项）；无目录heap tuple、WAL/MVCC或多目录原子DDL事务。
- [ ] **CAT-02** `pg_class`、`pg_attribute`、`pg_type`、`pg_proc`、`pg_depend`、`pg_namespace` 等必须是内部执行的真实来源，而不是另一套虚拟输出。
- [ ] **CAT-03** 补齐 `pg_constraint`、`pg_index`、`pg_am`、`pg_opclass`、`pg_operator`、`pg_cast`、`pg_collation`、`pg_rewrite`、`pg_trigger`、`pg_policy`、`pg_auth*`、`pg_default_acl`、`pg_database`、`pg_namespace`、`pg_tablespace`、`pg_stats`、`pg_statistic*`、复制 catalog 等。未实现的 `pg_index`／`pg_operator` 查询已改为明确 `0A000`（第984项）；`pg_database` 仅准确暴露 `datname`／UTF8 `encoding` 子集（第987项）；`pg_stats`／`pg_statistic` 在补齐各自typed schema前fail-closed（第989项）；`pg_namespace` 在真实schema/ACL/session temp生命周期齐备前fail-closed（第990项）；`pg_class`星号投影和已知未实现列 fail-closed（第991项）；`pg_type`/`pg_enum`不可信的文本SQL renderer fail-closed（第992项）；`pg_roles`不完整的四列renderer fail-closed（第993项）；`pg_settings`目前仅提供准确`name/setting/unit`子集、拒绝不完整星号投影并正确支持Extended列描述（第994项）。catalog 完整 schema、数据与执行语义仍缺。
- [ ] **CAT-04** 实现所有对象的稳定 OID、reg* 查找、OID 引用和 dump/restore 保真。
- [ ] **CAT-05** 统一 owner、ACL、comment、security label、extension membership 和 dependency graph。
- [ ] **CAT-06** 完整 `CASCADE/RESTRICT`、internal/auto/normal/pin/extension dependency 行为。
- [ ] **CAT-07** DDL 对 catalog 和物理文件的修改必须同一事务提交/回滚/恢复。
- [ ] **CAT-08** `CREATE DATABASE` 补 template、owner、encoding、locale/ICU provider、collation version、tablespace、strategy 和 connection limit。
- [ ] **CAT-09** schema 不应通过 `schema__table` 或 marker 模拟；补真正 namespace、rename/owner、权限和 search_path。
- [ ] **CAT-10** 表/列 DDL 补完整 rewrite、`USING`、dependency invalidation、锁等级、递归/ONLY 和多 action 原子性。
- [ ] **CAT-11** 分区表补默认分区、约束证明、attach validation、detach concurrently/finalize、分区索引和跨分区唯一性。
- [ ] **CAT-12** 继承补约束/default/generated/identity/统计/权限传播和多父表冲突规则。
- [ ] **CAT-13** 临时对象补 `pg_temp_N` catalog、search_path、ON COMMIT、两阶段事务限制和 session/backend 清理语义。
- [ ] **CAT-14** unlogged relation 补 init fork、crash truncate、复制和备份行为。已有 clean/unclean 重启及子进程 SIGKILL 后恢复的测试；真实掉电/存储故障注入、可差分物理复制仍未实现，故此项保持 partial。
- [ ] **CAT-15** sequence 补 relation/catalog 语义、cache、cycle、min/max、owned-by、并发、WAL、session currval/lastval 和非事务行为。
- [ ] **CAT-16** view 补完整 rewrite rule、自动可更新判断、CHECK OPTION、security barrier/invoker、recursive view 和依赖。
- [ ] **CAT-17** materialized view 补列类型推断、依赖、populate state、真实 concurrent refresh 和唯一索引要求。
- [ ] **CAT-18** trigger 补 trigger function、transition table、constraint/deferred trigger、列列表、`WHEN` scope、触发顺序和递归规则。
- [ ] **CAT-19** event trigger 和 rule 当前只是骨架，需要真实 DDL/rewrite 事件执行。
- [ ] **CAT-20** tablespace 补 cluster 级 catalog、OID/symlink 布局、owner/ACL、并发、跨设备持久化和备份恢复。
- [ ] **CAT-21** `COMMENT`/`SECURITY LABEL` 覆盖 PostgreSQL 对象全集，并使用 catalog dependency。
- [x] **CAT-22** 删除 `.pg_compat_objects` 的“成功但无运行时效果”语义；未实现命令应返回 `0A000 feature_not_supported`。

## 8. 数据类型、I/O、函数和操作符

当前 `TypeRegistry` 注册了较宽的类型名，表达式执行器也有约 140 个内置标量函数名，但多数类型仍以规范化文本或 string-backed 形式承载。这与 PostgreSQL 的 binary datum、typmod、cast、operator class 和函数生态不是一个完成度。

- [ ] **TYPE-01** numeric 补 PostgreSQL 精度/scale、NaN/±Infinity、舍入、溢出、比较、hash、sort support 和 binary protocol。
- [ ] **TYPE-02** integer/float 补完整溢出、NaN ordering、implicit cast、平台无关 binary storage 和错误语义。
- [x] **TYPE-03** `money` 补 locale-aware I/O 和精确内部表示；不能走 double/字符串近似。
- [ ] **TYPE-04** character/text 补无限 text/bytea 语义、typmod、blank padding、encoding、collation 和 Unicode 行为；当前 65535 等项目限制不是 PG 限制。
- [x] **TYPE-05** bytea 补全部 operators/functions、binary I/O 和大值 TOAST 行为。
- [ ] **TYPE-06** date/time/timestamp/timestamptz/interval 补微秒精度、BC/infinity、完整 timezone database、DST、timezone abbreviation、typmod 和所有边界值。
- [ ] **TYPE-07** boolean 保持真正三值类型，避免预处理阶段把 `TRUE/FALSE` 文本改成 `1/0` 引起类型偏移。
- [ ] **TYPE-08** enum 补 catalog ordering、rename/add value 的并发可见性、比较/hash 和 dump/restore。
- [ ] **TYPE-09** geometric types 补 PostgreSQL binary representation、全部 operator/function、NaN/边界和 GiST/SP-GiST opclass。
- [ ] **TYPE-10** inet/cidr/macaddr 补完整网络运算、排序、包含、hash 和 binary protocol。
- [ ] **TYPE-11** bit/varbit 补完整位运算、移位、比较、substring 和 binary I/O。
- [ ] **TYPE-12** tsvector/tsquery 补配置/字典/parser、词干/停用词、完整 query tree、headline/rank 和 GIN/GiST opclass。
- [x] **TYPE-13** UUID 使用 16-byte RFC datum 与原生索引键，保留 PostgreSQL 宽松输入/规范输出，并实现 `gen_random_uuid()`、`uuidv4()`、`uuidv7([shift])`、版本/时间提取及 binary protocol。
- [ ] **TYPE-14** XML 补 libxml 语义、well-formed document/content、XMLTABLE/XMLNAMESPACES 和相关函数。
- [ ] **TYPE-15** JSON/JSONB 补真正 binary JSONB、完整 operators/functions、SQL/JSON、jsonpath evaluator、GIN opclass、duplicate key/numeric/collation 细节。
- [ ] **TYPE-16** array 补任意元素类型、多维 lower bounds、rectangularity、comparison/hash、array assignment、record/array binary I/O 和完整函数集。
- [ ] **TYPE-17** composite/record 补 row descriptor、anonymous record、field selection/update、comparison、I/O 和函数返回 record 推导。
- [ ] **TYPE-18** range 补 canonical/subtype diff、empty/infinite bounds、operators、aggregate 和 GiST/SP-GiST；multirange 当前只是 string-backed 名称，需要真实实现。
- [ ] **TYPE-19** domain 补任意层嵌套、multiple CHECK、NOT NULL/default、cast、全表 revalidation 和 domain-over-composite/array。
- [ ] **TYPE-20** OID 家族补 `oid`, `regclass`, `regtype`, `regproc`, `regprocedure`, `regoperator`, `regnamespace`, `regrole` 等真实 catalog resolution。
- [ ] **TYPE-21** 内部类型补 `xid/xid8/cid/tid`, `pg_lsn`, `pg_snapshot`, `aclitem`, `name`, `char`, `cstring` 等 PostgreSQL I/O/比较语义。
- [ ] **TYPE-22** pseudo/polymorphic 类型补 `anycompatible*`, `anymultirange`, `internal`, `trigger`, `event_trigger`, `table_am_handler`, `index_am_handler` 等调用约束。
- [ ] **FUNC-01** 补 PostgreSQL 数学、字符串、binary、格式化、日期时间、枚举、几何、网络、全文、XML、JSON、array/range、系统信息、管理、统计和 replication 函数族。
- [ ] **FUNC-02** 实现 aggregate catalog/executor：ordered-set、hypothetical-set、partial/combine/serialize、moving aggregate、FILTER/ORDER BY/DISTINCT 完整组合。
- [ ] **FUNC-03** 实现 user-defined operator、commutator/negator、selectivity function、hash/merge 标记和 dependency。
- [ ] **FUNC-04** volatility/strict/leakproof/parallel safety/security definer/cost/rows/SET 属性必须真正影响 planner 和 executor。
- [ ] **FUNC-05** 实现 SQL function inlining、support functions、polymorphism、variadic/default/named arguments 和重载。
- [ ] **FUNC-06** PL/pgSQL 补 records/rowtype、exceptions、diagnostics、dynamic SQL、cursors、trigger variables、subtransactions、packages of statements、plan cache 和 dependency invalidation。（SELECT INTO/scalar identity、COLLATE/timezone/DISTINCT值角色及supported function atomicity已有独立修复和组合专项验证，但变量/源列歧义、函数限定参数、writing CTE前metadata准备、INTO output demand及WHERE/ORDER/subquery/EXPLAIN函数执行仍未完成。见 `docs/issue-plpgsql-query-binding-preflight.md`、`docs/issue-plpgsql-select-into-execution-demand.md`、`docs/issue-stored-function-statement-atomicity.md`。）

## 9. 约束和数据完整性

- [ ] **CONS-01** primary key/unique 支持完整多列、NULLS NOT DISTINCT、deferrable、partitioned table 和 concurrent conflict 语义。
- [ ] **CONS-02** foreign key 支持多列、MATCH FULL/PARTIAL、deferrable、循环 FK、partitioned table、各种 action 与并发 snapshot/locking 规则。
- [ ] **CONS-03** CHECK 支持 immutable dependency、NOT VALID/VALIDATE、domain/table 语义和 partition constraint 证明。
- [ ] **CONS-04** exclusion constraint 使用真实 GiST/opclass、任意表达式/操作符、多列、deferrable 和并发冲突检查。
- [ ] **CONS-05** generated column 补 PostgreSQL 18 virtual/stored 默认与限制、依赖和复制行为。
- [ ] **CONS-06** identity 补 ALWAYS/BY DEFAULT、OVERRIDING、sequence options、ALTER 和 partition/inheritance 行为。
- [ ] **CONS-07** constraint trigger 接入统一 deferred event queue，并保证 savepoint/rollback/crash 语义。
- [ ] **CONS-08** DML 的所有入口，包括 legacy、view、trigger、COPY、partition route、MERGE、ON CONFLICT，都必须经过同一约束路径。

## 10. 查询、DML 和执行语义

- [ ] **DML-01** 删除 DML 双入口；复杂 `INSERT ... SELECT`、view/CTE/partition target 全部使用结构化 ModifyTable。
- [ ] **DML-02** `ON CONFLICT` 补 index inference、partial/expression index、constraint target、复杂 expression/subquery、并发 speculative insertion。
- [ ] **DML-03** `UPDATE ... FROM`/`DELETE ... USING` 补任意 join tree、outer/lateral/subquery、重复 source row 和 `WHERE CURRENT OF`。（缺失/无法解析的 FROM/USING 来源，以及无 ON/USING 的普通来源 JOIN（递归检查）已在发布 AST 前拒绝，协议返回 42601 并验证目标行不变；这不等于完整 FROM grammar/任意来源执行已完成。见 `docs/issue-dml-03-missing-source-parser.md`。）
- [ ] **DML-04** `MERGE` 补多个 WHEN、MATCHED DELETE、NOT MATCHED BY SOURCE/TARGET、DO NOTHING、复杂 source、RETURNING 和并发规则。
- [ ] **DML-05** PostgreSQL 18 `RETURNING WITH (OLD/NEW AS ...)`、任意表达式、subquery/window、trigger 后值和协议 metadata。
- [ ] **DML-06** `COPY` 补 protocol STDIN/STDOUT、binary、PROGRAM、FREEZE、ON_ERROR、REJECT_LIMIT、HEADER MATCH、encoding 和权限。
- [ ] **QRY-01** SELECT target list 补完整 expression、SRF、row expansion、star qualification、alias visibility 和 resjunk column。（quoted JOIN 普通列、table CASE 的 AST/NULL/type、scalar function ORDER BY 的 structured/resjunk cells，以及 bounded LATERAL 外层裸列/qualified star/输出标签已分别修复提交；不等于完整 expression/SRF/row expansion。见 `docs/issue-query-projection-binding-and-lateral-scope.md`。）
- [ ] **QRY-02** FROM 补完整 LATERAL、table function、ROWS FROM、WITH ORDINALITY、TABLESAMPLE、XMLTABLE/JSON_TABLE。（LATERAL 裸外层列绑定支持无自身 FROM 的 evaluator-supported target/WHERE，以及本地列集合可由简单 base-table FROM 推知时的非冲突裸列引用；左输入现支持 simple base-table tree，叶子之间可为逗号/CROSS，或 `INNER/LEFT/RIGHT/FULL [OUTER] JOIN ... ON`，并逐行保留类型、NULL 位图和 fan-out；INNER/LEFT boolean `ON` 和 LEFT SQL NULL extension 亦在该范围测试。两步 FROM-less scalar chain、quoted alias 与一个 mixed-case quoted column 有协议回归。简单 USING 与 NATURAL INNER/LEFT/RIGHT/FULL 的裸相关键映射、复合键、RIGHT/FULL 合并键及各叶列的独立 NULL 已有协议回归；物化后的外层唯一裸列与 star、限定 star、FULL 合并键、两步链及后接简单普通 JOIN 的别名/元数据已有回归；复杂/未知 scope、复杂 lateral chain、CTE/derived/function 本地 FROM、table function、ROWS FROM、WITH ORDINALITY、TABLESAMPLE、XMLTABLE/JSON_TABLE 仍缺或未验证。）
- [ ] **QRY-03** join 补 USING/NATURAL 的输出列合并、FULL/outer null extension、lateral/parameterized join 和任意嵌套语义。（多表链现保留 outer join 的书写顺序，支持纯 CROSS 链、简单 top-level `ON ... AND ...` residual 条件、post-join WHERE、简单列、unary/binary/literal/cast 标量表达式、简单 CASE 和 evaluator 支持的普通标量函数投影、typed protocol metadata、列/ordinal及已投影表达式排序和 LIMIT/OFFSET；简单 USING/NATURAL 列名与复合键可合并输出并保留 FULL/outer NULL extension，普通单连接路由识别 `FULL JOIN` 和 `FULL OUTER JOIN`，简单 `alias.*` 按基表列展开。LATERAL 支持简单 base-table JOIN tree：comma/CROSS 与 `INNER/LEFT/RIGHT/FULL OUTER JOIN ... ON`，并已覆盖 NULL extension、fan-out、空左输入、限定/唯一裸列绑定、两步 FROM-less scalar chain 及 bounded quoted alias/column。简单 USING 与 NATURAL INNER/LEFT/RIGHT/FULL 左输入现按可见键映射裸相关列；物化 LATERAL 的外层裸列/star/qualified star、FULL 合并键、两步链及后接简单普通 JOIN 命名空间已有专项回归；SRF/聚合/窗口 target list、GROUP/HAVING、schema-qualified star、collation-aware ordering、更复杂 quoted/schema-qualified refs、复杂/任意嵌套 LATERAL 与 parameterized semantics 仍缺或未验证，见 `docs/issue-qry-03-multijoin-order-and-where.md` 与 `docs/issue-query-projection-binding-and-lateral-scope.md`。）
- [ ] **QRY-04** subquery 补 correlated scalar/EXISTS/IN/ANY/ALL、row comparison、decorrelation、parameter passing 和 NULL 三值逻辑。
- [ ] **QRY-05** CTE 补 recursive evaluation、SEARCH/CYCLE、materialized/not materialized、data-modifying CTE snapshot 和 visibility。
- [ ] **QRY-06** set operations 补任意 query expression、对应列类型/collation、嵌套 precedence、ALL duplicate count 和 ORDER/LIMIT scope。第959项已修复括号开头的statement路由、完整外围括号操作数、尾部注释和相应Simple/Extended执行；任意type/collation解析、通用表达式排序分页与Extended Describe仍未闭合。
- [ ] **QRY-07** aggregate/grouping 补 grouping sets 的任意组合、GROUPING/GROUPING_ID、ordered/distinct aggregate 和 functional dependency。
- [ ] **QRY-08** window 补多个 window specs、named/inherited window、所有 frame/exclusion/peer 边界、ordered-set interaction 和 spill。
- [ ] **QRY-09** DISTINCT/DISTINCT ON 补 PostgreSQL ordering 约束、NULL/collation 和可 spill 实现。
- [ ] **QRY-10** ORDER BY 补任意 expression、operator USING、NULLS、collation、stable tie handling 和 top-N/external sort。
- [ ] **QRY-11** row locking 补 `FOR UPDATE/NO KEY UPDATE/SHARE/KEY SHARE OF ... NOWAIT/SKIP LOCKED` 的完整冲突和 EPQ recheck。
- [ ] **QRY-12** inheritance/partition scan、ONLY、view rewrite 和 RLS 必须在 planner 中统一展开，不能靠文本标志或旁路扫描。
- [ ] **QRY-13** snapshot、trigger、rule、RLS、generated column 和 RETURNING 的执行顺序需与 PostgreSQL 一致。

## 11. 优化器和执行器

- [ ] **OPT-01** 建立 PostgreSQL 式 relation/path/parameterization/equivalence class/pathkey 框架，而不是直接拼单棵计划树。（当前仅有 `PlanContext` 到单一 `OpPtr` 的 builder；`PathKey` overload 保留 Sort 安全回退且忽略 equivalence classes。现有空表测试不验证路径选择，详见 `docs/issue-opt-01-path-planner-audit.md`。）
- [ ] **OPT-02** 实现 exhaustive/DP join search、GEQO 阈值、outer/semi/anti join constraints 和 bushy plan。（多表 outer join 链已禁止不安全重排并按书写顺序执行；目前仍没有 DP/exhaustive search、GEQO、semi/anti 约束搜索或 bushy plan，见 `docs/issue-qry-03-multijoin-order-and-where.md`。）
- [ ] **OPT-03** 完整 predicate implication、constant propagation、equivalence class、join removal、outer join reduction 和 partition pruning。
- [ ] **OPT-04** 完整统计：采样、null fraction、ndistinct、MCV、histogram、correlation、extended ndistinct/dependencies/MCV、表达式统计。
- [ ] **OPT-05** 实现 selectivity/cost support function、数据类型/operator/collation-aware 估算和统计失效。（已修复 `enable_nestloop=off` 被有统计的小表 join shortcut 覆盖的问题；更多代价/选择率语义仍缺，见 `docs/issue-opt-05-small-join-guc-cost-override.md`。）
- [ ] **OPT-06** parameterized path、nested-loop inner index scan、subplan/initplan、memoize 和 correlated execution。
- [ ] **OPT-07** 补 Seq/TID/TID Range/Sample/Function/Values/CTE/WorkTable/Foreign/Custom/Append/MergeAppend 扫描节点。
- [ ] **OPT-08** 补真正 index-only scan、visibility map 条件、heap fetch fallback 和 INCLUDE column。
- [ ] **OPT-09** bitmap 补 block/lossy bitmap、range predicate、parallel bitmap、recheck 和 work_mem spill。
- [ ] **OPT-10** sort/hash/aggregate/window/join 必须使用 work_mem 并支持临时文件 spill、批处理和 skew handling。
- [ ] **OPT-11** 补 Incremental Sort、Memoize、Materialize、Unique、LockRows、ModifyTable、ProjectSet、RecursiveUnion 等节点完整语义。
- [ ] **OPT-12** parallel query 补 worker pool/lifecycle、parallel-aware append/bitmap/hash、partial/final aggregate、leader participation、parallel safety。
- [ ] **OPT-13** PostgreSQL 18 AIO：异步 read queue、prefetch、io_method/io_combine_limit、统计和取消；不只等于“使用 io_uring”。
- [ ] **OPT-14** LLVM JIT：expression、tuple deform、cost threshold、EXPLAIN JIT 信息和平台构建。
- [ ] **OPT-15** generic/custom prepared plans、catalog/GUC/statistics invalidation、search_path/role/RLS 安全的 plan cache。
- [ ] **OPT-16** EXPLAIN 覆盖所有 statement/node，补 VERBOSE、COSTS、SETTINGS、WAL、MEMORY、SERIALIZE、SUMMARY、FORMAT JSON/XML/YAML 完整结构。
- [ ] **OPT-17** executor cancellation、interrupt、statement timeout、error cleanup 和 resource owner 必须贯穿全部算子/worker/I/O。CLI及wire现有协作执行/锁等待超时（第997项）；Extended Query从首个Parse/Bind/Execute/Describe消息开始计时，Parse停顿会超时且Sync可恢复；COPY FROM空闲/部分明文帧及真实TLS idle/部分record等待、COPY TO TCP输出反压超时已有覆盖。仍缺普通frontend blocking reads、TLS COPY以外输出的端到端验证、其他阻塞socket I/O、所有非协作算子/worker的中断传递及完整ResourceOwner清理。

## 12. 索引和访问方法

- [ ] **IDX-01** 实现 catalog-driven Table AM/Index AM API、handler、support routine、validator、cost estimate、build/insert/scan/vacuum callbacks。
- [ ] **IDX-02** operator class/family、strategy/support number、cross-type operator、collation 和 sort support。
- [ ] **IDX-03** B-tree 补 PostgreSQL key ordering、NULL、dedup、suffix truncation、page split/delete/recycle、fast root、high key、concurrent scan 和 corruption checks。
- [ ] **IDX-04** PostgreSQL 18 B-tree skip scan 和多列统计驱动 path 选择。
- [ ] **IDX-05** Hash 补 metapage/bucket/overflow/split、并发锁、WAL、vacuum 和 hash support function。
- [ ] **IDX-06** GIN 补 entry/posting tree、pending list/fastupdate、extract/consistent/triConsistent、vacuum、WAL 和 jsonb/array/tsvector opclass。
- [ ] **IDX-07** GiST 当前只是 flat interval sidecar；需真实 tree、union/penalty/picksplit/same/consistent/distance、KNN、WAL 和 opclass。
- [ ] **IDX-08** SP-GiST 补 radix/quad/k-d 等节点模型、choose/picksplit/inner-consistent/leaf-consistent、WAL/vacuum/opclass。
- [ ] **IDX-09** BRIN 补 page range summary tuple、revmap、autosummarize、desummarize、minmax/minmax-multi/bloom/inclusion opclass。
- [ ] **IDX-10** expression/partial index 补 immutable 检查、dependency、predicate implication、planner matching 和 HOT safety。
- [ ] **IDX-11** covering/index-only scan 补 VM、included payload、non-key attribute 和 heap visibility fallback。
- [ ] **IDX-12** partitioned index、attach/detach、parent validity 和唯一约束跨分区规则。
- [ ] **IDX-13** 真正 `CREATE INDEX CONCURRENTLY`/`REINDEX CONCURRENTLY`：invalid state、多事务阶段、等待旧 snapshot、失败恢复。
- [ ] **IDX-14** vacuum cleanup、page deletion、bulk delete、amcheck、pg_stat index/progress 和 corruption recovery。
- [ ] **IDX-15** 所有索引 DML、DDL、reindex、crash recovery、TDE、backup/restore 路径使用统一且可证明的持久化协议。

## 13. 事务、MVCC、锁和 VACUUM

- [ ] **TXN-01** 明确实现 PG 的 Read Uncommitted=Read Committed；逐语句 snapshot 和 command counter 行为一致。
- [ ] **TXN-02** Repeatable Read、Serializable、read-only/deferrable 的 snapshot 时机和错误条件一致。
- [ ] **TXN-03** tuple xmin/xmax/cmin/cmax/ctid/infomask、combo CID、HOT chain、redirect/dead line pointer 的全部可见性规则。
- [ ] **TXN-04** subtransaction 使用真实 SubXID/parent、overflow、CLOG/pg_subtrans、resource owner 和错误状态。
- [ ] **TXN-05** savepoint 回滚 catalog、locks、files、deferred events、portals、LISTEN/NOTIFY 和 sequence 相关状态。
- [ ] **TXN-06** 2PC 使用全局 durable prepared transaction state，恢复 locks/subxacts/invalidation/notify，并支持跨 backend 管理和清理。
- [ ] **TXN-07** 完整 heavyweight lock modes/conflict matrix、fast-path locks、lock queue fairness、deadlock soft edge/reorder 和 wait events。
- [ ] **TXN-08** tuple locks、MultiXact、key-share/no-key-update、EPQ 和 FK/unique 冲突的锁规则。
- [ ] **TXN-09** predicate lock 在 relation/page/tuple/index range 间升级，覆盖所有访问方法和空范围。
- [x] **TXN-10** advisory lock 的 session/transaction 两类、two-int key、shared/exclusive、try-lock 和 cleanup。
- [ ] **TXN-11** commit/abort/group commit 顺序、synchronous_commit 级别、commit timestamp 和 WAL flush wait。
- [x] **TXN-12** LISTEN/NOTIFY 在事务提交后投递、rollback 丢弃、payload/channel 规则、跨 session/backend 队列和协议异步通知。
- [ ] **VAC-01** VACUUM 的 prune/freeze/index cleanup/truncate、visibility/freeze map、failsafe 和 wraparound 防护。
- [ ] **VAC-02** autovacuum launcher/worker、per-table thresholds/cost delay、worker slots、anti-wraparound 优先级和冲突取消。
- [ ] **VAC-03** VACUUM FULL 使用 transactional table rewrite/swap；ANALYZE/VACUUM option 与 progress view 对齐。（已修复父括号及旧式 `ANALYZE`/`FULL` 选项误当普通 VACUUM、事务块中未拒绝和 unsupported `FREEZE` 被静默忽略；`FULL` 仍非 crash-atomic table swap，FREEZE/progress view 仍缺。详见 `docs/issue-vac-03-maintenance-options.md`。）
- [ ] **VAC-04** HOT eligibility 必须考虑所有索引（含 expression/partial）和 page space；chain pruning 与 concurrent snapshot 安全。

## 14. 存储、WAL、checkpoint 和恢复

- [ ] **STO-01** 定义稳定 on-disk format、control file、system identifier、catalog version、block size、endianness 和 feature flags。
- [ ] **STO-02** relation locator/fork/segment 布局、database/tablespace OID 和临时 relation 命名对齐内部模型。
- [ ] **STO-03** page header、item identifier、tuple/varlena/toast pointer、special space、LSN/checksum 的兼容且自描述格式。（布局 v5 已绑定物理块号并拒绝 v5 页错块；v4 首次 dirty mark 升级。完整 PostgreSQL checksum、tuple/varlena/TOAST 与 LSN 兼容仍缺，详见 `docs/issue-sto-03-page-identity.md`。）
- [ ] **STO-04** Buffer Manager 补 shared hash/partition locks、buffer content locks、I/O-in-progress、prefetch、bulk strategy、ring buffer 和 resource owner pin cleanup。（已修复 `invalidateAll()` 复用仍 pinned frame 的错误；更完整 Buffer Manager 仍缺，详见 `docs/issue-sto-04-buffer-invalidation-pins.md`。）
- [ ] **STO-05** FSM/VM 持久化、crash rebuild、all-visible/all-frozen 和 index-only/VACUUM 交互。（已修复简化 VACUUM 不检查 visibility horizon 却误设 all-visible 的问题；现在保守清除此位，见 `docs/issue-sto-05-vacuum-visibility-horizon.md`。完整 horizon、all-frozen、FSM/VM crash rebuild 与 index-only 集成仍未完成。）
- [ ] **STO-06** TOAST 补 varlena short/compressed/external datum、storage strategy、toast_tuple_target、pglz/lz4、dedup/delete/vacuum 和索引一致性。（当前有自定义压缩分块、索引一致性检查及快照安全的 orphan vacuum；PG varlena/external pointer、列级存储策略、可配 target、PGLZ/LZ4 与 dedup 仍缺，详见 `docs/issue-sto-06-toast-format-audit.md`。）
- [ ] **WAL-01** WAL resource manager 记录 heap/index/catalog/fsm/vm/toast/multixact/sequence/standby 等所有资源变化。（目前有 heap/index/XACT/checkpoint/SMGR 的子集；TOAST 部分依提交前落盘、特殊索引部分依事务边界重建，catalog 记录不含可重放行状态；FSM/VM、sequence、multixact、standby 等完整 WAL/replay 仍缺，详见 `docs/issue-wal-01-resource-manager-coverage-audit.md`。）
- [ ] **WAL-02** WAL insertion、page LSN、full-page image、compression、continuation、segment switch、recycling 和 concurrent writer 规则。（本地已有 CRC32C/alignment、连续跨 segment I/O、fsync/group commit、checkpoint page-image、archive/recycle 子集；无 PostgreSQL WAL page header/continuation 与 compression，完整并发插入/FPI协议仍缺，详见 `docs/issue-wal-02-insertion-segments-audit.md`。）
- [ ] **WAL-03** checkpoint 补 redo horizon、dirty buffer scheduling、checkpoint completion、control file、WAL retention 和节流。（本地 checkpoint 已实现 cache flush/WAL barrier、事务空闲门控、checkpoint WAL与sidecar LSN、archive-gated truncate；redo horizon、PG控制文件/completion协议、成熟的dirty-buffer调度和节流仍缺，详见 `docs/issue-wal-03-checkpoint-audit.md`。）
- [ ] **WAL-04** restartpoint、recovery consistency、recovery conflict、hot standby snapshot、timeline history 和 promotion。（现有离线redo/undo及PITR timeline fork有专项覆盖；restartpoint、standby snapshot/conflict、timeline history chain与promotion仍缺，详见 `docs/issue-wal-04-standby-and-timeline-audit.md`。）
- [ ] **WAL-05** recovery 必须是 redo-based 状态机；当前额外 before-image undo 模型需证明与 steal/no-force、并发 checkpoint 的所有 crash window 一致。（修复启动恢复遍历未初始化活动事务集合导致的 SIGSEGV；真实 `SIGKILL` 矩阵12/12通过。undo/replay与redo状态机、并发checkpoint及所有崩溃窗口仍未证明，见 `docs/issue-wal-05-startup-recovery-crash-matrix.md`。）
- [ ] **WAL-06** 数据页、所有索引和元数据 checksum；补离线/在线启停与 `pg_checksums`/verify 工具等价物。（heap页及B+Tree子集已有自定义checksum；新B+Tree `0xC552` 将block number纳入CRC，旧 `0xC551` 需REINDEX才具备此保护；Hash/Bloom/GIN/BRIN/GiST/SP-GiST 新格式有自定义checksum、旧版明确列为unchecked；离线 verifier 扫描 `.idx`/`.idx_*`、`.hidx`、`.bidx`、`.gin`、`.brin`、`.gist`、`.spgist` 并单列 legacy 未校验数量，但不解析B+Tree拓扑、GIN postings或BRIN/GiST/SP-GiST entries语义；全部metadata仍缺统一checksum，也无enable/disable/rewrite/progress workflow，见 `docs/issue-wal-06-index-checksum-coverage.md`。）
- [ ] **WAL-07** 目录/fsync/rename/link/unlink 顺序覆盖 ext4/XFS、跨设备表空间、磁盘满、partial write、torn write 和 power-loss。（补 `CREATE/DROP SCHEMA` 标记失败恢复与持久化、`CREATE/DROP/RENAME DATABASE` 集群父目录同步、`RENAME/DROP SEQUENCE` fsync 失败回滚、`tlist.lst` 原子固定记录发布，以及 LargeObject 目录/空对象创建耐久性；启动清理失败会 fail-closed，相关故障注入回归通过。LOB object file 访问现拒绝符号链接；write/truncate fsync 内容，DROP 先同步 canonical-name 删除，目录 fsync 失败则尝试恢复并同步原名，`large_object_drop_durability_test` 覆盖两种 barrier 结果。持续 EIO 下的 rollback 仍可能是 indeterminate，且事务/WAL 边界未接入。DROP/RENAME 表在目录持久化失败后的多文件 DDL 仍可能是 indeterminate，调用方须重查。ext4/XFS、跨设备全流程、ENOSPC、partial/torn write 与真实 power-loss 仍未覆盖，见 `docs/issue-wal-07-no-replace-schema-marker-durability.md`。）
- [ ] **WAL-08** unlogged/temp relation、2PC、sequence、DDL、logical slot 在 crash 后的专门恢复规则。（修复 startup recovery 在清理前尝试重建缺失 schema 的 session-temp relation 索引、导致服务启动失败的问题；`stale_temp_startup_recovery_test` 与 TEMP DDL/owned-sequence 回归通过。仅覆盖该临时关系残留窗口；2PC、temp WAL、sequence/DDL、logical slot 的 crash 状态机仍缺，见 `docs/issue-wal-08-stale-temp-startup-recovery.md`。）
- [ ] **STO-07** 大对象补 catalog、ACL、事务、64-bit offset、lo_* API、protocol/libpq 和 vacuum。（存储文件访问现拒绝 object-file 符号链接、拒绝初始化时的 `.lobjects` symlink，并为读写/截断使用验证过的普通文件描述符；该窄项有 `large_object_symlink_guard_test` 覆盖。目录描述符未被 manager 全生命周期固定，父目录并发替换不在覆盖范围内；catalog/ACL、WAL/事务、完整 64-bit/API/protocol/libpq 与 vacuum 仍缺，见 `docs/issue-sto-07-large-object-path-integrity.md`。）
- [ ] **STO-08** TDE 使用成熟密码库和审计过的 AEAD/KMS 方案；补索引/WAL/temp/backup 全覆盖、密钥轮换、per-database key 和灾难恢复。自研 SHA-256-CTR+EtM 不应直接作为生产加密承诺。

## 15. 复制、高可用和备份

- [ ] **REPL-01** physical replication connection、`IDENTIFY_SYSTEM`、`START_REPLICATION`、CopyBoth、keepalive/feedback 和 WAL sender/receiver。
- [ ] **REPL-02** standby startup/replay、read-only query、recovery snapshot、冲突处理、`hot_standby_feedback`。
- [ ] **REPL-03** physical/logical replication slot 持久化、restart/confirmed LSN、WAL retention、xmin/catalog_xmin、drop/advance/copy/sync/failover。
- [ ] **REPL-04** synchronous replication、remote_write/flush/apply、priority/quorum、sync standby reconfiguration 和 commit wait。
- [ ] **REPL-05** cascading replication、timeline follow、promotion、rewind 和 split-brain 运维边界。
- [ ] **REPL-06** logical decoding 必须从 WAL 解码 committed transaction，而不是 DML 写路径旁路采集。
- [ ] **REPL-07** 实现 wire-compatible `pgoutput`、protocol v1-v4、large transaction streaming、2PC、origin、binary、schema/type messages。
- [ ] **REPL-08** publication 补 column list、row filter、partition root、publish_via_partition_root 和 ALTER 行为。
- [ ] **REPL-09** subscription/apply worker、initial table sync、replication origin、conflict、disabled slot、two-phase、failover slot。
- [ ] **REPL-10** logical decoding plugin API、snapshot export、reorder buffer、spill 和 output plugin lifecycle。
- [ ] **BACKUP-01** online base backup protocol/API、start/stop backup、backup_label、tablespace map、WAL inclusion 和 throttling。
- [ ] **BACKUP-02** backup manifest、checksums、验证工具和损坏/缺文件诊断。
- [ ] **BACKUP-03** PostgreSQL incremental backup/backup summary 和 `pg_combinebackup` 等价链路。
- [ ] **BACKUP-04** PITR 补 `restore_command`、recovery.signal/standby.signal、name/time/xid/LSN/immediate target、inclusive/action/pause。
- [ ] **BACKUP-05** 多 timeline archive、history file、archive cleanup、archive module/command retry 和安全 shell substitution。
- [ ] **BACKUP-06** `pg_dump`/`pg_restore` 兼容的 schema/data/archive 格式、依赖排序、parallel dump/restore、large object、ACL/owner。
- [ ] **BACKUP-07** `pg_rewind`、promote 后重新加入、备份恢复演练和 RPO/RTO 证据。

## 16. PostgreSQL 协议和客户端兼容

- [ ] **PROTO-01** 协议 3.0/3.2 negotiation、startup parameters、ParameterStatus、BackendKeyData、ReadyForQuery 状态完整性。
- [ ] **PROTO-02** Simple Query 多 statement、implicit transaction、empty query、command tag 和错误后跳过规则。
- [ ] **PROTO-03** Extended Query Parse/Bind/Describe/Execute/Close/Flush/Sync、unnamed replacement、portal suspension、error recovery 和 pipelining。
- [ ] **PROTO-04** RowDescription/DataRow 使用准确 type OID/typmod/table OID/attnum/format；不能由文本输出猜列。
- [ ] **PROTO-05** 所有内置/用户类型的 text/binary input/output，尤其 numeric、array、range/composite、jsonb、inet、interval、bytea。
- [ ] **PROTO-06** COPY IN/OUT/BOTH、CopyData/Done/Fail 和流式 backpressure。
- [ ] **PROTO-07** CancelRequest 使用 backend PID/secret key，在阻塞锁、I/O、并行 worker 和长算子中及时生效。
- [ ] **PROTO-08** ErrorResponse/NoticeResponse/NotificationResponse 全字段和 SQLSTATE；异步消息可以在查询之间/期间发送。
- [ ] **PROTO-09** SSLRequest/GSSENCRequest、TLS negotiation、证书、SNI、channel binding 和 secure renegotiation policy。
- [ ] **PROTO-10** replication mode 与 physical/logical streaming protocol。
- [ ] **PROTO-11** Unix-domain socket、IPv4/IPv6、keepalive、TCP user timeout、client_encoding 和 locale。
- [ ] **PROTO-12** 建立并持续运行 libpq、psql、JDBC、Npgsql、psycopg、pgx、SQLAlchemy、Django、Hibernate 兼容套件。
- [ ] **CLIENT-01** 提供或兼容 `psql` 元命令、COPY、describe、变量、脚本错误控制和密码处理。
- [ ] **CLIENT-02** 工具链：initdb、pg_ctl、createdb/dropdb、createuser/dropuser、vacuumdb、reindexdb、clusterdb、pg_isready、pgbench。

## 17. 安全、认证和权限

- [ ] **SEC-01** `pg_hba.conf` 完整 record type、samehost/samenet、replication、database/role list、include、map、reload 和错误诊断。
- [ ] **SEC-02** 认证方法：peer/ident、cert、LDAP、PAM、RADIUS、GSSAPI/Kerberos、SSPI、BSD、OAuth；不支持时明确拒绝而非近似。
- [ ] **SEC-03** SCRAM-SHA-256-PLUS/channel binding、iteration policy、verifier lifecycle、password encryption GUC 和 credential rotation。
- [ ] **SEC-04** TLS 使用真实 OpenSSL 构建作为发布门槛，补协议 negotiation、client cert、CRL/OCSP、cipher/min protocol、reload 和统计。
- [ ] **SEC-05** role membership 补 INHERIT/SET/ADMIN option 的 PostgreSQL 18 语义、grantor/dependency、循环和 DROP/REASSIGN OWNED。
- [ ] **SEC-06** ACL item 和所有对象类型：database/schema/table/column/sequence/function/procedure/language/type/domain/FDW/server/tablespace/large object/parameter。
- [ ] **SEC-07** default privileges 使用 catalog，并正确作用于 owner/schema/object type/large object。
- [ ] **SEC-08** RLS 补 planner/rewrite integration、policy dependency、partition/inheritance、prepared plan、leakproof ordering 和完整 owner/bypass/force 规则。
- [ ] **SEC-09** SECURITY DEFINER/INVOKER 的 user identity、search_path、防对象劫持、SET 配置和异常恢复。
- [ ] **SEC-10** view security_barrier/security_invoker、function leakproof、row security 与 optimizer pushdown 的安全证明。
- [ ] **SEC-11** `SECURITY LABEL` provider、sepgsql/MAC hook；当前 label 文件不是强制访问控制。
- [ ] **SEC-12** COPY/PROGRAM、file read/write、large object、extension/library load 的超级用户/预定义角色权限和路径防护。
- [ ] **SEC-13** 审计日志需防篡改、结构化、敏感参数脱敏、rotation/retention；同时明确它不是 PostgreSQL 核心兼容能力。

## 18. 系统目录、information_schema、监控和运维

- [ ] **MON-01** `pg_catalog` 补 PostgreSQL 18 system catalogs/views/functions 的结构、OID、类型和权限过滤。
- [ ] **MON-02** 当前 `information_schema` 只有 tables/columns/statistics/routines/views/triggers/key_column_usage 等少量虚拟表；补标准全集和角色可见性。
- [ ] **MON-03** 统计子系统补 shared/persistent counters、snapshot semantics、reset、track_* GUC、function/SLRU/WAL/checkpointer/bgwriter/I/O 统计。
- [ ] **MON-04** `pg_stat_activity` 目前仅有 `pid/datname/usename/state/query` typed 子集，且 query 文本已按本人/superuser/`pg_read_all_stats`过滤（第995项）；仍需补 `query_id`、xact/query start、wait_event_type/event、backend type、client、leader pid 等 PostgreSQL 18 字段与完整权限/快照语义。
- [ ] **MON-05** `pg_locks` 补全部 locktag/mode/granted/fastpath/waitstart 和 predicate locks。
- [ ] **MON-06** 复制/归档/SSL/GSS/subscription/slot/recovery 统计视图。
- [ ] **MON-07** PostgreSQL 18 `pg_stat_io`、`pg_aios`、WAL/checkpointer/slru 和 backend memory context 视图。
- [ ] **MON-08** ANALYZE/VACUUM/CREATE INDEX/CLUSTER/COPY/base backup progress views。
- [ ] **MON-09** `pg_stat_statements` 补 queryid/jumble、plans、rows、block/WAL/JIT/parallel、reset/minmax、容量淘汰和权限。
- [ ] **MON-10** logging collector、stderr/csvlog/jsonlog/syslog、rotation、log_line_prefix、statement/duration/error verbosity 和采样。
- [ ] **MON-11** auto_explain、pg_buffercache 等应通过真实 extension 或清楚标注为内置兼容视图，字段必须对齐。
- [ ] **OPS-01** 配置采用 data directory/control file/GUC context/source/reload/restart 语义；当前大量状态依赖启动 CWD。
- [ ] **OPS-02** `SHOW ALL`/`pg_settings` 补完整 name/setting/unit/category/context/source/min/max/enum/pending_restart。
- [ ] **OPS-03** SIGHUP/reload、smart/fast/immediate shutdown、startup lock/PID file、crash restart 和 child supervision。
- [ ] **OPS-04** 日志、WAL、数据、temp、archive 的目录/权限/umask/ownership 和服务管理约定。
- [ ] **OPS-05** 磁盘空间、inode、FD、内存、CPU、I/O、连接风暴和慢客户端的监控及保护。

## 19. 扩展、FDW、过程语言和生态

- [ ] **EXT-01** `CREATE EXTENSION` 控制文件、SQL install/update、version graph、relocatable/schema、membership、dependency、dump/restore。
- [ ] **EXT-02** 动态库加载、fmgr ABI、Datum/NullableDatum、memory context、PG_FUNCTION_INFO、error/interrupt 安全。
- [ ] **EXT-03** 自定义 base/composite/range/multirange type I/O、typmod、analyze、subscript handler。
- [ ] **EXT-04** 自定义 function/procedure/aggregate/operator/cast/collation/conversion 的真实 catalog 驱动执行。
- [ ] **EXT-05** index/table access method API、operator class/family 和 WAL for extensions。
- [ ] **EXT-06** planner/executor/utility/object access/emit log 等 hooks 和 CustomScan API。
- [ ] **EXT-07** shared memory/LWLock tranche/dynamic shared memory、shared preload/session preload。
- [ ] **EXT-08** background worker 注册、启动时机、restart、signal、DB connection 和 shared memory。
- [ ] **EXT-09** procedural language handler/validator/inline handler；PL/Python、PL/Perl、PL/Tcl 等生态接口。
- [ ] **FDW-01** FDW handler/validator、foreign server/user mapping、GetForeignRelSize/Paths/Plan、scan/modify/direct modify、transaction callbacks。
- [ ] **FDW-02** IMPORT FOREIGN SCHEMA、parameterized pushdown、join/aggregate pushdown、async foreign scan 和 EXPLAIN。
- [ ] **EXT-10** logical decoding output plugin、archive module、OAuth validator module 和 injection point 等 PG18 扩展点。
- [ ] **EXT-11** 兼容常用扩展的现实前提：pg_stat_statements、auto_explain、pg_trgm、btree_gin/gist、hstore、citext、uuid-ossp、postgres_fdw 等。

## 20. 工程质量、发布和生产化

- [x] **ENG-01** 修正文档事实漂移：README、RELEASE-NOTES、CHANGELOG、feature-gaps 和 production-status 中存在互相冲突的 PASS 数、版本和“已完成”描述。
- [x] **ENG-02** 修正版本单一事实源：`version.h` 的 `MAJOR/MINOR/PATCH` 当前仍是 0/1/0，而字符串与 CMake 是 0.2.0。
- [ ] **ENG-03** 每个 release 必须在 clean worktree、固定 compiler/dependency、真实 TLS 构建上完成全量测试并保存机器可读报告。
- [ ] **ENG-04** 单元测试之外增加 SQLLogicTest、PostgreSQL regression/isolation test 移植、ORM suites 和随机 differential SQL。
- [ ] **ENG-05** parser/expression/protocol/WAL/page/catalog/backup 输入 fuzzing，以及 corpus/minimization。
- [ ] **ENG-06** fault injection 覆盖每个 write/fsync/rename/unlink/alloc/thread/lock/WAL point，配合 power-cut 模拟和恢复不变量检查。
- [ ] **ENG-07** TSAN/ASAN/UBSAN/LSAN、debug assertions、不同优化级、GCC/Clang 和 32/64-bit/endianness 策略持续运行。
- [ ] **ENG-08** 并发 history checking：事务隔离用 Elle/Jepsen 风格验证，而不是只看测试返回 PASS。
- [ ] **ENG-09** 长时间 soak、连接风暴、锁风暴、checkpoint/vacuum/backup/DDL/DML 混合、磁盘慢/满/错误测试。
- [ ] **ENG-10** pgbench/TPC-C/TPC-H/TPC-DS 与 PostgreSQL 同硬件基准，记录吞吐、p50/p95/p99、WAL 放大、CPU、内存、I/O。
- [ ] **ENG-11** crash-compatible upgrade/downgrade、catalog migration、rollback plan 和长期 on-disk compatibility 测试。
- [ ] **ENG-12** 发布二进制、包管理、容器、SBOM、依赖/CVE、签名、reproducible build 和支持平台矩阵。
- [ ] **ENG-13** 安全评审：密码学、协议、SQL 权限、文件路径、extension/load、backup/restore、DoS 和敏感日志。
- [ ] **ENG-14** 明确 SLA、支持范围、已知限制、数据恢复手册、备份恢复演练和 incident response。
- [x] **ENG-15** 建立兼容版本策略：是“接受 psql 的自有 DBMS”，还是“PostgreSQL 18 行为兼容”；两种目标的验收标准完全不同。

## 21. 非 PostgreSQL 语法和行为偏移

这些能力可以作为项目扩展保留，但必须放入显式 compatibility mode，不能混入 PostgreSQL 模式：

- [x] **DIV-01** `USE DATABASE`：PostgreSQL 连接建立后不能用 SQL 切换 database。
- [x] **DIV-02** `REPLACE INTO`：MySQL 语法；PostgreSQL 使用 `INSERT ... ON CONFLICT`。
- [x] **DIV-03** `LOAD DATA INFILE`：MySQL 风格；PostgreSQL 使用 `COPY`/psql `\copy`。
- [x] **DIV-04** `SELECT ... INTO OUTFILE`：MySQL 风格；PostgreSQL `SELECT INTO` 是建表。
- [x] **DIV-05** `DESC`/`DESCRIBE`、`VIEW TABLE`、`VIEW DATABASE`、`SHOW USERS/ROLES/POOLS` 等是项目命令或客户端元命令风格。
- [x] **DIV-06** `AUTO_INCREMENT`、unsigned integers、`TINYINT`、`DATETIME`、`BLOB`、`NCHAR/NVARCHAR`、`BINARY/VARBINARY` 是兼容别名或非 PG 类型。
- [x] **DIV-07** `CREATE FULLTEXT INDEX`、`CREATE HASH INDEX` 等快捷语法不是 PostgreSQL 的标准写法；PG 使用 `CREATE INDEX ... USING ...` 和 operator class。
- [x] **DIV-08** `CREATE ASSERTION` 被当作 compatibility object 接受，但 PostgreSQL 18 自身也没有实现 SQL assertion；项目当前更没有约束运行时。
- [x] **DIV-09** 普通 SQL 形式的 `CREATE/DROP REPLICATION SLOT` 和 `SHOW LOGICAL ...` 是项目接口；PostgreSQL 通过 replication protocol 或系统函数管理/消费槽。
- [x] **DIV-10** `DUMP`、`BACKUP DATABASE`、`RESTORE DATABASE`、`RESTORE ... PITR`、`CLEAR PLAN CACHE` 是项目命令，不是 PostgreSQL SQL reference 命令。
- [x] **DIV-11** `SET GLOBAL` 和部分 `SHOW` 命令是 MySQL 风格，不是 PostgreSQL GUC 语法。
- [ ] **DIV-12** 内置 PgBouncer 风格连接池、TDE、`BACKUP/RESTORE DATABASE` 是项目扩展，不等于 PostgreSQL 核心同名能力。
- [x] **DIV-13** 默认端口、数据目录和启动 CWD 状态布局与 PostgreSQL 不同，应避免给工具造成“这是 PostgreSQL cluster”的假象。
- [x] **DIV-14** 对仅保存兼容记录的命令返回“created/loaded/altered”会误导用户；应改为 feature-not-supported，直到运行时真正存在。

## 22. 建议实施顺序和验收门

### Gate 0：先定义目标，不再按关键词追功能

1. 冻结一份 PostgreSQL 18 compatibility contract。
2. 建立 183 命令和各功能族的自动化 matrix。
3. 建立 PostgreSQL differential、isolation、crash 和 protocol 测试框架。
4. 所有未实现骨架统一返回 `0A000`，禁止“保存记录后报告成功”。

### Gate 1：统一内核执行边界

1. 完成 binder/type inference/rewrite。
2. DQL/DML/utility 全部 typed AST 执行。
3. 结构化 result/error/SQLSTATE。
4. 删除 legacy 文本结果和语义 rewrite。

验收：复杂 SQL 不进入字符串 fallback；协议层不解析 `std::cout`；同一语句 CLI/协议结果完全一致。

### Gate 2：catalog 和事务化 DDL

1. catalog 关系化、MVCC/WAL 化。
2. object/owner/ACL/dependency 全集。
3. DDL 无隐式提交并支持 savepoint/crash recovery。
4. schema/search_path/temp/partition/trigger/view 统一 catalog 驱动。

验收：随机 DDL transaction 在任意 kill point 后与 PostgreSQL 的可见对象集合一致。

### Gate 3：存储、索引和 MVCC 正确性

1. XID/MultiXact/freeze/autovacuum。
2. 完整 SSI/predicate lock。
3. 所有索引使用真实 AM、WAL、vacuum、concurrent build。
4. work_mem spill、resource owner、故障注入。

验收：隔离测试、长稳、故障注入、索引一致性和恢复矩阵全部通过。

### Gate 4：协议、安全和工具兼容

1. protocol/COPY/binary/SSL/cancel/pipeline。
2. ACL/RLS/SECURITY DEFINER/认证。
3. libpq/psql/JDBC/ORM matrix。
4. catalog/information_schema/monitoring/tooling。

验收：主流 PG client 和 ORM 无项目专用分支即可完成迁移、事务、COPY、DDL 和 introspection。

### Gate 5：备份、复制和 HA

1. base backup/manifest/incremental/PITR 多 timeline。
2. physical streaming/hot standby/sync replication/slots。
3. WAL-based logical decoding/pgoutput/subscription。
4. promote/rewind/failover 演练。

验收：持续写入下完成备份恢复、主备切换、故障重加入，满足明确 RPO/RTO 且无数据分叉。

### Gate 6：扩展生态和高级性能

1. extension/fmgr/hooks/background worker/FDW/PL。
2. 完整统计/path planner、parallel、AIO、JIT。
3. pg_upgrade、发布包、长期兼容和安全审计。

真正做到 PostgreSQL 等级是一个多年、多人、持续验证的数据库工程，不应继续沿用旧文档中“单人几十周即可完整对齐”的估算。

## 23. PostgreSQL 官方对标来源

2026-10-01 第917项 source `f469eed0`，D合并 `46e5969e`：BIGINT／NUMERIC SUM经serial／parallel GroupAggregate的15位double输出丢精度，旧915强wire及actual failed=1真实保留；legacy三聚合路径另仍执行int64累加，旧Table定向signed-overflow sanitizer在MAX＋MAX真实报runtime error／exit1。按真实scalar column type选exact Numeric accumulator，BIGINT超int64、NUMERIC全部小数位与NULL维持；float输入保留原floating路径。legacy删除未使用的有符号累加，补有效数值count及NULL跳过，让flat／group／grouping sets都用精确结果。新native覆盖三legacy API、serial／parallel两种group shape、真实300输入worker分组、最小值和NULL；最终10 C++、专项、8wire邻居／完整协议及9不同actual均exit0，独立Table signed-overflow instrumentation重跑专项亦exit0（不是全库UBSan／ASan）。真实PG18.6相同强oracle全部通过，无numeric tolerance放宽。初版去掉legacy累加后没有同步SUM count，native及完整协议SUM=0真实失败，已修正并全部重跑闭合，原日志保留。两个CPP-only真实重编＋916Dml／914Network／908正式其他对象冻结组合，逐个cmp source，无header／layout／disk变更，不称独立冷构建。组合应430 C++／159E2E／428actual；根／A仍冻结2d2db1d0（到915／916，429／158／427），根正式production exit0，A完整脚本运行，之后再以同源正式binary跑427全差分，不混入917。完整numeric／float语义、expression aggregate、overflow／binary、907temp SERIAL及其余总清单未完成。913真实PG已核实CHAIN须显式block且END／ABORT／WORK／TRANSACTION／注释分割选项合法；新的chained-origin abort READ ONLY恢复差异单独留918继续。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user，不push，Actions禁用。

2026-10-01 第915项 source `5f19850a`，D合并 `c3882b66`（含独立916前置 `3eb708e0`）：合法BIGINT INT64_MIN与INF失败／NULL sentinel冲突，原INSERT／UPDATE22023、原native insert abort134保留。storage内部改用optional checked decimal；v2 heap依真实NULL bitmap决定NULL，保留旧nullable无bitmap格式的sentinel限制且不做disk迁移。materialized OLD／NEW、typed matcher、FK referenced image、RETURNING／logical image及UPDATE／INSERT／DELETE undo index提取绑定各自NULL bitmap；真实强wire曾依次复现WHERE漏行、UPDATE RETURNING空值、负谓词legacy回退（独立916）及SP restore后secondary index丢键致DELETE58030，失败日志全部保留。最终真实PG18.6相同完整wire oracle、专项、15 C++（12邻居＋3 rollback heap／WAL failure）、10wire邻居、完整协议和9个不同actual case均exit0；MIN／MAX／NULL、BETWEEN／IS NULL、PK duplicate、secondary IndexScan、NULL↔MIN UPDATE、精确RETURNING、SP恢复后删除与COPY验证通过，故障注入的rollback incomplete诊断为既有测试预期且全exit0。Table CPP-only重编，逐个cmp源文件后重链916Dml／914Network／908正式真实对象，无既有header／layout／disk改变，不称独立冷构建或整库sanitizer证明。首次native夹具误用API／未清空追加输出vector的问题已独立纠正，原失败记录不冒充产品复现；强wire断言未放宽。组合应429 C++／158E2E／427actual，正式生产／整套／全差分需新冻结验收，旧9f的426／153／424全exit0不代替它。SUM／AVG精确大值、legacynullable格式、其他codec／numeric overflow与完整TYPE-02及其他族仍未完成；907temp SERIAL和913无block CHAIN继续。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user，不push，Actions禁用。

2026-10-01 第916项 source `3eb708e0`，D合并 `660f10d1`：parser把负整数／显式正整数条件表示为UnaryOp，DML bridge原只接受LiteralExpr而退回旧路径，UPDATE／DELETE漏RETURNING且可误改／误删不匹配行；旧914强wire及actual failed=1均保留。新增严格的单层signed integer literal绑定，只接受未加引号十进制数字token并保留字符串大小／signed spelling（INT64_MIN不先转换positive int64），不把cast／text／其他表达式误折叠。真实PG18.6 oracle、专项wire、10 C++、9邻居／完整协议、9个不同actual case全部exit0；负条件、显式+条件、AND、quoted column、NULL不匹配、exact affected tag及全部未匹配行保持均校验。DmlExecutor CPP-only重编＋914Network／908正式其他对象冻结组合，source逐个cmp，无header／layout／disk变更，不称独立冷构建。最初邻居选择使用不存在case而exit1已保留，最终实存9case重跑无allowlist通过。与915存储修复的组合仍在验：915新wire又发现savepoint恢复时BIGINT最小值secondary index丢键（DELETE58030），已保留原失败并继续修复；915尚未提交。根9f完整426／153／424全exit0仅属到909／908冻结基线，不含916。其他numeric predicate、legacy fallback、SUM溢出、907temp SERIAL、913无block CHAIN及其余总清单未完成，不push，Actions禁用。

2026-10-01 冻结 `9f86fa62`（source `7baa97e9`，仅到909／908）的正式验收已全部闭合：根正式production真实全header重编exit0，A完整脚本426/426 C++及153/153协议/E2E exit0（`/tmp/dbms-tests-abort-cleanup-908-909.log`），根同源正式二进制424/424真实PG18.6差分exit0（`/tmp/dbms-pgdiff-abort-cleanup-908-909.log`，cases=424 failed=0，424个不同[OK]）。这份全量结果不包含后来911／912／914的source；三项已各自定向验收，组合正式全量待下一冻结轮次。915仍在隔离修复BIGINT最小值：原INSERT／UPDATE拒绝、UPDATE RETURNING丢值及DELETE typed matcher漏行均保留失败记录，尚未提交或计通过。907临时SERIAL及其他未完成族继续。总账仍273：24 complete／138 partial／96 unverified／15 deferred_by_user，不push，Actions禁用。

2026-10-01 第914项 source `462ceffe`，D合并 `892664fa`：COPY scalar SMALLINT／INT／BIGINT非法输入原22023而非22P02，旧911强wire exit1保留。新增独立IntegerTextCodec，在COPY field boundary按真实signed width解析并canonical decimal；非法syntax22P02、超范围22003，保留PG decimal／0x／0o／0b、前导sign、单underscores（base prefix后可单underscore）、周围whitespace和small-overflow/trailing-invalid与huge-overflow的错误优先次序，NULL optional与array field不误转。真实PG18.6完整codec wire oracle、项目专项、8 C++（含新unit）／单独ASan＋UBSan、9邻居／完整协议及9不同actual全exit0；late-row失败清掉前行但原表已提交数据保持，CopyDone后I／无COPY CommandComplete，valid边界与NULL bitmap保持。Network CPP-only＋新增纯header helper与冻结组合真实objects重链，无既有类layout或disk变更，不称独立冷构建。该codec native可解析INT64_MIN，但storage仍以INF sentinel拒绝该值；独立915 originalactual已cases=1 failed=1，INSERT／UPDATE22023及missing row均保留，不把codec测试冒充完整BIGINT runtime。普通Bind／CLI／其他COPY类型、binary与codec族仍未完成。组合应427 C++／156E2E／425actual；根/A9f仍冻结426／153／424，本轮C++已426/426通过，正式E2E正在运行，随后才跑424全量差分；907临时SERIAL未修，其他族继续。

908隔离正式组合已快进95b20267并真实增量重编main／Network，五wire（911／912／908／903／COPY）、912actual和完整协议均exit0；保留原194正式binary于build/dbms_subtransaction_release_formal_19461193，组合95正式binary另存build/dbms_savepoint_copy_chain_formal_95b20267。这是独立正式脚本真实对象，不是手工cache移植；尚不含914。总账273仍24 complete／138 partial／96 unverified／15 deferred_by_user，未push，Actions禁用。

2026-10-01 第911项 source `257beec6`，D合并 `dc67d9d2`：COPY行错误已发ErrorResponse但尚等CopyDone／Sync时仍持有旧tuple及transaction advisory locks，原912强wire在该精确阶段55P03／exit1保留。receiveCopyIn在发送错误前调用与普通SQL共享的USER SP／top-level abort；COPY guard改用结构性internal statement SP（碰撞后用实际返回名），同步notification／advisory快照。已被USER rollback或top abort移除的guard不再发ROLLBACK TO／RELEASE；显式failed block继续E／25P02且再COPY不进入G，SESSION ownership保留。提前COPY校验／gate错误也走abort，direct SP异常转结构化错误，snapshot恢复绑定连接当前live Session而非短租已归还对象。最终真实PG18.6四组合（Simple／Extended×TOP／USER）在CopyDone／Sync发送前的锁／advisory、earlier write、已copy前行原子undo及恢复全部通过；10 C++、专项、8邻居／完整协议及9不同实际case全exit0。Network CPP-only＋冻结908真实其他对象及912真实main重链，source逐个cmp，header不变，无disk改变，不称独立冷构建。最初invalid integer fixture发现独立22023代替22P02，留待914；换duplicate-PK fixture的PG在批量flush前不发送该错误，原reference timeout保留，不能套用错误的即时E预期。最终NOT NULL fixture在PG与本项目均是23502并真实复现未释放锁；未放宽SQLSTATE／锁断言。Parse／Bind等一般protocol-error abort、完整portal／SubXID／resource-owner以及codec／binary／backpressure族仍未完成。组合应426 C++／155E2E／425actual，根/A仍冻结9f验收426／153／424，不含911／912；907temp SERIAL、914 COPY整数、915 BIGINT最小值sentinel冲突继续，不push、Actions禁用。

2026-10-01 第912项 source `f7ff4ac3`，D合并 `146aea0b`：正常COMMIT／ROLLBACK AND CHAIN会在engine结束事务时清掉READ ONLY，随后新事务错误接受INSERT（旧强wire exit1、actual cases=1 failed=1保留）。main在结束前捕获mode，并仅为新chain恢复；SET TRANSACTION READ ONLY同样继承。真实PG强oracle还证明顶层abort后chain恢复read-write，而仅user subtransaction失败时parent READ ONLY保持，两例均纳入最终断言，不能笼统继承failed block最初的mode。专项wire、8邻居（含908即时锁释放／COPY／deferred）、完整协议、专项＋8不同actual全部exit0。main CPP-only重编，逐个cmp其余source／header并重链冻结908正式真实production对象；无header／layout／disk改变，不称独立冷构建。最初自有夹具patch拼接SyntaxError和缺失case选择失败保留，不冒充旧产品复现；修正夹具后真实旧wire／actual独立失败，未放宽oracle。新组合应426 C++／154E2E／425actual；根/A仍冻结9f的426／153／424本轮正式验收，不含912。没有active block的AND CHAIN 25P01、链选项lexer／别名及unsupported DEFERRABLE仍未完成，不勾整个TXN族；907temp SERIAL仍未修，911 COPY资源abort路径继续。

2026-10-01 冻结494正式全量闭合：424/424 C++、151/151E2E和同源423/423 PG18.6实际差分（`/tmp/dbms-pgdiff-savepoint-gap-serial-null-903-906.log`，cases=423 failed=0）均exit0。随后根与A快进并冻结 `9f86fa62`（source `7baa97e9`，新增909／908），正式production全重编及426 C++／153E2E正在真实运行，之后才启动该正式binary的424全量差分；旧423不能替代新source验收。912正常COMMIT／ROLLBACK AND CHAIN的READ ONLY丢失已保留原wire误成功INSERT和actual failed=1，PG完整强oracle通过；此前夹具拼接SyntaxError和不存在选择项失败不计旧产品证据，原日志保留。912隔离修复及测试继续，不在9f本轮全量中；907temp SERIAL仍未修。

2026-10-01 第908项 source `19461193`，D合并 `7baa97e9`：三连接真实PG18.6 schedule确认错误必须在ErrorResponse之前释放active user savepoint之后的tuple／transaction advisory locks；没有user SP时必须立即释放全部transaction资源，而SESSION advisory lock不随abort释放。旧84强wire在未发送ROLLBACK TO时保留row2锁而exit1；初版只清理subtransaction的Top-level advisory断言亦exit1，原日志保留。新增latestUserSavepoint结构性查找，Network在普通SQL错误边界恢复最近USER SP或物理abort无SP事务，并同步notification／advisory transaction资源；协议仍E／25P02直至客户端恢复，先前用户SP工作和锁保持。最终独立真实production全重编＋Network增量、12 C++、最终三连接PG oracle／本项目强wire、8邻居／COPY／完整协议、9不同actual case均exit0。v1本地Table括号编译错误已修正且原失败日志保留；两个邻居选择脚本的不存在文件名错误亦保留，不算产品失败或成功用例，最终使用实际文件名重跑通过。非virtual方法无新增对象字段或磁盘格式更改。COPY wire内部SP仍是legacy USER分类，Parse/Bind等非SQL错误边界、真正SubXID/resource owner及其余savepoint族未完成，不勾整个TXN-05。

正式冻结494基线：根production全header真实重编exit0；A完整脚本424/424 C++、151/151 E2E、exit0已闭合（`/tmp/dbms-tests-savepoint-gap-serial-null-903-906.log`）。随后根494同正式binary的423全量PG18.6差分已启动，尚在运行，不能声称全量结果；根/A仍冻结494，不含909／908。含两项下一轮应426 C++／153E2E／424actual。907temp SERIAL原failed=1尚未修；912正常AND CHAIN丢失READ ONLY已真实PG确认，接续独立修复。总账273：24 complete、138 partial、96 unverified、15 deferred_by_user；未push，Actions禁用。

- [PostgreSQL 18.6 Documentation](https://www.postgresql.org/docs/18/)
- [PostgreSQL 18 SQL Commands：183 条命令目录](https://www.postgresql.org/docs/18/sql-commands.html)
- [The SQL Language](https://www.postgresql.org/docs/18/sql.html)
- [Concurrency Control](https://www.postgresql.org/docs/18/mvcc.html)
- [Indexes](https://www.postgresql.org/docs/18/indexes.html)
- [Server Administration](https://www.postgresql.org/docs/18/admin.html)
- [Monitoring Database Activity](https://www.postgresql.org/docs/18/monitoring.html)
- [Frontend/Backend Protocol](https://www.postgresql.org/docs/18/protocol.html)
- [High Availability, Load Balancing, and Replication](https://www.postgresql.org/docs/18/high-availability.html)
- [Logical Replication Architecture](https://www.postgresql.org/docs/18/logical-replication-architecture.html)
- [Extending SQL](https://www.postgresql.org/docs/18/extend.html)
- [PostgreSQL 18 release notes](https://www.postgresql.org/docs/18/release-18.html)

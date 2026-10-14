# 2026-10-13 审查问题修复进度

对应[原审查报告](code-review-2026-10-13.md)。修复起点为`b54cc7f0`；原始观测和失败日志保留，不以修复后的结果覆盖历史证据。

每项修复使用独立commit，包含实现、能复现原问题的回归及问题文档。此前独立worktree工作逐项合入master；当前在主工作区继续逐项修复。所有提交留在本地，由用户push。GitHub Actions保持禁用。

## 执行计划

1. 修复页压缩/插入容量、连接池生命周期和等待、协议会话上下文，验证数据保持及正常关停。
2. 修复写入RETURNING/ON CONFLICT、SQL EXECUTE求值，核对错误阶段、事务清理和副作用次数。
3. 修复目录加载/值快照、失败回滚恢复、TEMP启动和维护任务状态，保留正常格式及恢复语义。
4. 修复数值精度、窗口类型比较/frame/排名偏移、递归CTE和严格语法。
5. 修复逻辑预览编码/错误传播、测试注册，以及原回归失败与观察项。
6. 合入后运行新回归与相邻测试，最后重新执行完整注册回归、文档/状态检查，登记真实终态。

用户此前暂缓的安全/TDE及CTAS/后台checkpoint专项仍暂缓；本轮普通数据完整性与现有回归失败照常修复。CMake、TLS及sanitizer的实际验证范围按结果记录。

## 逐项清单

| 项目 | 内容 | 状态 | commit / 验证 |
| --- | --- | --- | --- |
| R01 | NUMERIC SQRT | 已修复 | `91951e37`；1023原生控制O2/ASan/UBSan、46协议PG/Root及5完整native邻居通过，见[验证](issue-review-r01-numeric-square-root.md) |
| R02 | 窗口排序、peer、MIN/MAX | 已修复 | `491dae49`；最终28协议原24失败→PG/Root全过，168native、八完整native/十一完整协议通过；C/默认locale区别明确，见[验证](issue-review-r02-window-declared-comparisons.md) |
| R03 | 窗口分区的NUMERIC等价 | 已修复 | `cd3980fe`；原12native全失败/18协议11失败→PG/Root全过，七完整native/十完整协议通过，见[验证](issue-review-r03-window-partition-equivalence.md) |
| R04 | 窗口frame边界 | 已修复 | `c71a7526`；原最终307协议275失败→PG/Root全过；349native、78初始化控制、十五完整native/十八完整协议通过，方向/空区间/EXCLUDE/typed offset/嵌套变量及初始化次数保留，见[验证](issue-review-r04-window-frame-boundaries.md) |
| R05 | NTILE分桶 | 已修复 | `aeaa3257`；24 native与18 PG/Root协议控制及六完整窗口native通过，见[验证](issue-review-r05-ntile-distribution.md) |
| R06 | NTILE非正参数 | 已修复 | `41d45c4a`；20native原8失败、72协议原57失败→PG/Root全过；九完整native/十八完整协议通过，保留NULL、参数、逐分区求值次数，见[验证](issue-review-r06-ntile-argument.md) |
| R07 | LAG/LEAD偏移 | 已修复 | `c94a8714`；原native120中36失败、原合法负偏移协议触发退出→最终122native/152 PG及Root协议全过；最终58当前生产对象、十五完整native/十九完整协议通过，见[验证](issue-review-r07-window-signed-offset.md) |
| R08 | binary float参数往返 | 已修复 | `2d600491`；Root标准构建及86协议控制通过；PG18.6与native邻居见[验证](issue-review-r08-binary-float.md) |
| R09 | 连接池等待期间引用寿命 | 已修复 | `2332382f`；原实现ASan失败；修复O2/ASan及重复测试通过，见[验证](issue-review-r09-pool-wait-reference.md) |
| R10 | 连接池跨数据库等待 | 已修复 | `cb934db9`；Root真实源新编译4控制通过；O2/ASan及重复验证见[验证](issue-review-r10-pool-database-capacity.md) |
| R11 | 窗口SUM中间溢出 | 已修复 | `b8020acc`；原实现UBSan红证据、PG/Root12控制48行及六完整窗口native通过，见[验证](issue-review-r11-window-exact-accumulator.md) |
| R12 | 测试注册 | 已修复 | `a6bb7ef7`；五个原驱动实际0；新增完整注册盘点及构建路由检查通过，见[验证](issue-review-r12-test-registration.md) |
| R13 | 逻辑预览NULL及字段编码 | 已修复 | `123ac811`；原native64中61失败、最终wire53中50失败→Root77native/53wire全过；十二完整native/七完整协议通过，带版本逐列类型/NULL编码及虚拟列空串修复，见[验证](issue-review-r13-logical-tuple-encoding.md) |
| R14 | publication读取错误传播 | 已修复 | 原成员读取五个错误实际失败；Root fresh58 与写入前保护、原逻辑解码及目录/owner九完整native全过，见[验证](issue-review-r14-publication-read-errors.md) |
| R15 | 关停唤醒顺序 | 用户暂缓，未修复 | 此前单独要求跳过的模块，不重开；不据此跳过普通注册集成回归 |
| R16 | catalog坏行及完整加载 | 已修复 | `0d85f6b9`/`a0d2efde`；691、cold正常API及原catalog_snapshot全通过，见[初始验证](issue-review-r16-catalog-complete-read.md)/[兼容修复](issue-review-r16-optional-catalog-directory.md) |
| R17 | 失败回滚恢复图像 | 已修复 | `9edf30ce`；候选fresh58、23完整native及20重复通过；Root fresh58、最终31完整native与完整PGprotocol实际0，见[验证](issue-review-r17-rollback-recovery-ownership.md) |
| R18 | catalog值快照 | 已修复 | `8f6e3c43`；候选fresh58及提交前/后8完整native通过，Root R14 集成 fresh58 后再次通过 snapshot/complete/cold/owner，见[验证](issue-review-r18-catalog-value-snapshots.md) |
| R19 | TEMP启动恢复 | 已修复 | `18e91f2b`；原失败134→0、候选八完整native和Root十二完整native/PGprotocol通过；新文件清理问题独立R40，见[验证](issue-review-r19-stale-temp-startup.md) |
| R20 | RETURNING子查询 | 已修复 | 原Root native29检查失败；PG/Root 35协议、七完整native及14完整协议全过，见[验证](issue-review-r20-returning-query-owner.md) |
| R21 | ON CONFLICT BOOLEAN条件 | 已修复 | `1463b6be`；119 native、75 PG/候选/Root协议及完整邻居、Root十二native/PGprotocol通过，见[验证](issue-review-r21-conflict-boolean-context.md) |
| R22 | 严格模式FROM/INTO | 已修复 | `799636f7`；36 native及拒绝控制通过；完整协议的独立R31后续通过，见[验证](issue-review-r22-dml-required-keywords.md) |
| R23 | 维护任务持久化门禁 | 已修复 | `07f71677`；49故障控制原28失败→0、ASan/UBSan、20重复与原管理器通过；Root十一native、五协议及完整DIV14通过，见[验证](issue-review-r23-maintenance-persistence-gate.md) |
| R24 | EXECUTE参数求值 | 已修复 | `1d5aac14`；59 PG/候选/Root控制、私树两批十一完整协议、Root九native/十三协议全过，见[验证](issue-review-r24-execute-argument-values.md) |
| R25 | Numeric::withPrecision | 已修复 | `021b3031`；原始12345→123失败保留；41控制O2/ASan/UBSan和原Numeric完整驱动通过，见[验证](issue-review-r25-numeric-precision.md) |
| R26 | 递归CTE类型/typmod | 已修复 | `7cae7c1f`；PG/候选/Root57控制及Root六完整native通过，Root SQRT46控制也通过，见[验证](issue-review-r26-recursive-cte-types.md) |
| R27 | 页压缩全部存活行 | 已修复 | `58f1ee3d`；原生26组O2/ASan/UBSan通过；SQL及进程重启通过，见[验证](issue-review-r27-page-compaction.md) |
| R28 | 页插入空间计算 | 已修复 | `261ffb86`；原始满页插入失败证据；198边界O2/ASan/UBSan及26压缩邻居通过，见[验证](issue-review-r28-page-insert-capacity.md) |
| R29 | LIKE ESCAPE类型比较 | 已修复 | `d7a3293d`；Root项目flags的-Werror=address通过；34绑定控制及相邻协议见[验证](issue-review-r29-pattern-escape-type.md) |
| R30 | Parse/Describe会话上下文 | 已修复 | `8ba70380`；Root标准构建及12控制通过；5个原失败驱动在候选通过，见[验证](issue-review-r30-session-metadata.md) |
| R31 | DELETE ONLY回退错删多行 | 已修复 | `eabfc340`；新12控制PG/候选、原R22完整协议、3 native/3协议/完整PGprotocol通过，见[验证](issue-review-r31-delete-only-typed-owner.md) |
| R32 | 科学计数法字面量的NUMERIC写入绑定 | 已修复 | `40e0a152`；203 native、110 PG/候选/Root协议及完整邻居通过；独立微小scale问题由R34关闭，见[验证](issue-review-r32-scientific-numeric-literals.md) |
| R33 | legacy窗口路径丢失最终ORDER BY | 已修复 | `41054ead`；最终44控制原39失败→PG/Root全过，独立CLI、八完整native/十七完整协议通过；中间别名失败均保留，见[验证](issue-review-r33-window-final-order.md) |
| R34 | 科学计数法NUMERIC算术scale | 已修复 | `a36efcb5`；120 native、123 PG/候选/Root协议，原111字节快照/known-gap断言及完整邻居通过，见[验证](issue-review-r34-scientific-numeric-arithmetic.md) |
| R35 | INSERT SELECT的ON CONFLICT分句边界 | 已修复 | `a196264d`；42 native、17 PG/候选/Root协议及完整邻居通过，原失败保留；执行owner另记R39，见[验证](issue-review-r35-insert-select-conflict-boundary.md) |
| R36 | gcd/lcm指数NUMERIC结果OID | 已修复 | `d1cd3cce`；63 native、61 PG/候选/Root协议，六native/五协议邻居通过，见[验证](issue-review-r36-gcd-lcm-result-types.md) |
| R37 | TEXT与NUMERIC算术类型检查 | 已修复 | `b5fc5b73`；109 PG/候选/Root协议与60native通过；Root真实fresh58、十一完整native/PGprotocol通过，见[验证](issue-review-r37-numeric-text-arithmetic.md) |
| R38 | 表列混合指数表达式投影 | 已修复 | `758c375b`；60协议原18失败→PG/候选/Root全过，36native及完整邻居通过；Root十一native/五协议通过，见[验证](issue-review-r38-scientific-column-projections.md) |
| R39 | INSERT SELECT冲突/复杂source执行owner | 已修复 | `2d66c751`；53native原39失败→0、原完整23协议PG/候选/Root全过，保留全部known-gap断言；直接UNKNOWN输出的中间回归已修复，Root十一native/五协议通过，见[验证](issue-review-r39-insert-select-owner.md) |
| R40 | TEMP清理误删空尾相似名称的持久表fork | 已修复 | `707a23f5`；原known-gap断言134→0、新强化file-owner与六完整native邻居通过；Root真实fresh58、十一native/PGprotocol通过，见[验证](issue-review-r40-temp-cleanup-file-ownership.md) |
| R41 | INSERT SELECT来源检查回归 | 已修复 | R39复查新增；38 native原13失败→最终53全过，七完整native和八完整协议（含PGprotocol）通过，见[验证](issue-review-r41-insert-source-preflight.md) |
| R42 | 索引故障测试的失败回滚合约 | 已修正回归夹具 | 原exit134→四完整native全过，五隔离重复通过；保留pending XID、修复后显式retry、原REINDEX/数据断言；未改产品，见[验证](issue-review-r42-index-rollback-fixture.md) |
| R43 | 函数属性与LANGUAGE/AS交错 | 已修复 | `0c3ebac4`；52native原36失败→候选/Root全过，Root真实重编/九native/十三协议通过，见[验证](issue-review-r43-function-attribute-order.md) |
| R44 | legacy窗口临时列名覆盖真实源列 | 已修复 | `80fe342e`；原39控制29失败→PG/Root全通过；十五完整native/二十完整协议通过，独立结果槽不修改源列身份，见[验证](issue-review-r44-window-internal-namespace.md) |
| R45 | 窗口聚合的表达式参数 | 已修复 | 原300协议287失败→PG/Root全0；typed参数/NULL/结果声明、frame推进与VOLATILE重算、LIMIT需求和REAL宽度已修，Root十四完整native/十七完整协议全0，见[验证](issue-review-r45-window-aggregate-arguments.md) |
| R46 | 原生/SQL schema 目录及事务 owner | 已修复，待主分支集成 | 原 namespace 18 条断言失败，PG/候选46全过；最终35完整native和十一完整协议全0，中间同引擎/保存点/夹具及原默认协议失败保留，见[验证](issue-review-r46-native-schema-ownership.md) |
| R47 | BIT 与整数转换的真实 cast 关系 | 已修复，待主分支集成 | 174native/160协议及十完整native/九完整协议全0；扩展172的BIT输出typmod另列R50，未放宽断言，见[验证](issue-review-r47-bit-integer-casts.md) |
| R48 | 故障注入回归的 pending rollback 合约 | 已修正夹具，待主分支集成 | 四个原完整驱动失败；加强后的四驱动在主分支五轮20次全0，候选六完整native全0，未改产品，见[验证](issue-review-r48-pending-rollback-fixtures.md) |
| R49 | INSERT SELECT 物化视图 source lowering | 已修复，待主分支集成 | 原完整native错误0A000/原41协议22失败→PG/候选41全0；parser24、十完整native/九完整协议全0，见[验证](issue-review-r49-materialized-insert-sources.md) |
| R50 | BIT/VARBIT 的协议及目录长度描述符 | 已修复，待主分支集成 | 原新60全部失败→PG/候选全0；原完整172、42native及七完整native/八完整协议全0，保留未通过邻居及全部known-gap断言，见[验证](issue-review-r50-bit-type-modifiers.md) |
| R51 | recovery graph 邻居的 DROP DATABASE 观察 | 独立复验通过，历史原因未定 | 原驱动一次失败保留；R47固定生产代独立诊断及5+20次新隔离cwd全0，未改原断言，待主分支组合复验 |
| R52 | 原生测试终态输出换行 | 已修正夹具，待主分支集成 | 使用真实换行分隔测试输出与PASSED marker；完整原驱动重新编译执行0，原完整日志及严格解析遗漏保留，见下方复验记录 |
| R53 | INSERT SELECT 的INTERVAL字段修饰符上下文 | 已修复，待主分支集成 | 原27中1失败/新133中9失败→PG/候选全0，78纯binder控制及六完整native/七完整协议全0；标量与数组解析阶段分开，见[验证](issue-review-r53-insert-interval-context.md) |
| R54 | 聚合表达式中未提供参数的错误优先级 | 已修复，待主分支集成 | 实际PG18.6新54全0，候选原13失败→0；原完整聚合scope及五完整协议全0，保留合法Extended/SQL PREPARE，见[验证](issue-review-r54-missing-parameter-priority.md) |
| R55 | BYTEA 存储别名错误目录身份 | 已修复，待主分支集成 | 新28协议原17失败→PG/候选全0；原完整patterns及七完整协议/六完整native全0，7个目录/cold/显式历史修复控制全0，见[验证](issue-review-r55-bytea-catalog-identity.md) |
| R56 | ARRAY_AGG 窗口函数的解析/绑定 | 待修复 | 原window_type完整协议中array_agg(v) OVER报42883；其他窗口实现不代表该聚合已覆盖，待参考和独立复现 |
| R57 | 缺失索引后的协议连接中断 | 待复现 | 原missing_index完整驱动在INSERT时连接关闭，而非预期58030；需保留服务端日志并核对失败回滚owner，不能只调整超时或期望 |
| R58 | 两个协议驱动的启动连接失败 | 原样复验通过，历史原因未定 | 原errno103保留；两个原完整驱动在主分支按默认超时独立复跑0，不宣称已修复启动竞态，见[完整记录](issue-full-registered-f4087f5d.md) |
| R59 | 新组合物化视图target协议超时 | 原样复验通过，历史原因未定 | 71个原target控制及source/事务控制主分支原样复跑0，原超时保留；来源55000缺陷归R49，见[完整记录](issue-full-registered-f4087f5d.md) |
| O01 | owner健康重试及测试覆盖 | 已修复 | `c9a1e3c3`；原全部owner断言、新真实peer/dirty冲突及10完整native提交前/后通过；Root R14集成fresh58后两完整owner再次通过，见[验证](issue-review-o01-table-owner-health-retry.md) |
| O02 | EXISTS索引错误的访问计划/断言 | 已修正回归夹具 | 原完整协议exit 1→0，保留六严格XX001，新增精确t/f，完整EXISTS协议/19native通过；未改产品，见[验证](issue-review-o02-index-access-path-expectations.md) |
| O03 | EXPLAIN JOIN图及过时断言 | 已修复 | 实际执行的source/cache/join/scan图及相关重绑；最终完整60控制、十二native/十协议CLI全过，OFFSET阳性及42601原/PG对照保留，见[验证](issue-review-o03-explain-join-source-graph.md) |
| O04 | HAVING BOOLEAN绑定 | 已修复 | `157e5d2e`；20 native、49 PG/候选/Root协议、五native/六协议邻居与完整PGprotocol通过；原超时及原错误均保留，见[验证](issue-review-o04-having-boolean-context.md) |
| O05 | ORDER BY重复别名 | 已修复 | `c5d10cc3`；原117协议85失败→PG/Root全过；74native、十四完整native/十六完整协议通过，纯绑定歧义、星号真实来源、typed常量及实际输出owner/初始化次数保留，见[验证](issue-review-o05-order-by-duplicate-alias.md) |
| O06 | 物化视图刷新超时 | 当前完整复验通过 | 原驱动未改，默认15秒及原10秒配置两完整exit0；历史原因未定，见[验证](issue-review-o06-matview-refresh-revalidation.md) |
| O07 | ALTER ARRAY历史超时 | 当前完整复验通过 | 原驱动未改，默认15秒及原10秒配置两完整exit0；历史原因未定，见[验证](issue-review-o07-alter-array-revalidation.md) |

原273项功能总账独立维护；关闭这里的缺陷不自动宣称完整PostgreSQL功能兼容。

## R52：测试终态行的真实分隔

`prepared_srf_host_ownership_test` 的成功提示原来输出字面量反斜杠加 `n`，
使随后构建脚本的 `[test-build] ... PASSED` 紧接在同一行。测试确已成功退出，
但按行首严格盘点会漏记该项。修正方法仅为输出实际换行，不改变任何测试断言
或脚本终态解析规则。候选完整驱动以当前全部58份生产对象 receipts 为门禁重新
编译、链接、隔离执行，结果0；证据为
`/tmp/dbms-review-r46.ifu6SWLp/r52-native.log`。
主分支仍运行的完整回归保持原输入与日志不变，其严格汇总与独立标记核验须同时
保留，不能用候选修正覆盖原始观测。

## 最新主分支集成记录

`b5fc5b73` 的公共算术类型头变化后真实重编全部58个生产翻译单元，标准构建
实际exit 0（`/tmp/dbms-review-fix-donor.N9sdas6f/root-r37-r40-fresh58-build.log`）。
R37协议109控制、最终十一完整native及完整postgres_protocol均exit 0。首次
native批次在五个驱动通过后因调用者拼错 rollback 驱动文件名而exit 1；保留
`root-r37-r40-native.log`，更正真实文件名并预校验全部输入后完整重跑十一项
exit 0，不将首次批次记为十一项通过。

`07f71677`（已含R38/R39）真实重编Main、DML、binder、maintenance四个变化
翻译单元并链接exit 0；逐对象核对全部58份当前receipts及最终输入不变。
`root-r38-r39-r23-native.log` 的十一完整native、五个
`root-r38-r39-r23-*_test.py.log` 协议和 `root-r23-div14-whole-protocol.log`
完整能力测试均exit 0。私树R23首次DIV14早期DDL超时仍保留，不改断言/timeout。
上述为集成门禁，尚不是最后一次完整注册回归，也不关闭表内其余待修复项。

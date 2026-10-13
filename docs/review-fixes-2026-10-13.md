# 2026-10-13 审查问题修复进度

对应[原审查报告](code-review-2026-10-13.md)。修复起点为`b54cc7f0`；原始观测和失败日志保留，不以修复后的结果覆盖历史证据。

每项修复使用独立commit，包含实现、能复现原问题的回归及问题文档。并行工作在独立worktree进行，验证后逐项合入master；所有提交留在本地，由用户push。GitHub Actions保持禁用。

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
| R02 | 窗口排序、peer、MIN/MAX | 待修复 | |
| R03 | 窗口分区的NUMERIC等价 | 待修复 | |
| R04 | 窗口frame边界 | 待修复 | |
| R05 | NTILE分桶 | 已修复 | `aeaa3257`；24 native与18 PG/Root协议控制及六完整窗口native通过，见[验证](issue-review-r05-ntile-distribution.md) |
| R06 | NTILE非正参数 | 待修复 | |
| R07 | LAG/LEAD偏移 | 待修复 | |
| R08 | binary float参数往返 | 已修复 | `2d600491`；Root标准构建及86协议控制通过；PG18.6与native邻居见[验证](issue-review-r08-binary-float.md) |
| R09 | 连接池等待期间引用寿命 | 已修复 | `2332382f`；原实现ASan失败；修复O2/ASan及重复测试通过，见[验证](issue-review-r09-pool-wait-reference.md) |
| R10 | 连接池跨数据库等待 | 已修复 | `cb934db9`；Root真实源新编译4控制通过；O2/ASan及重复验证见[验证](issue-review-r10-pool-database-capacity.md) |
| R11 | 窗口SUM中间溢出 | 已修复 | `b8020acc`；原实现UBSan红证据、PG/Root12控制48行及六完整窗口native通过，见[验证](issue-review-r11-window-exact-accumulator.md) |
| R12 | 测试注册 | 已修复 | `a6bb7ef7`；五个原驱动实际0；新增完整注册盘点及构建路由检查通过，见[验证](issue-review-r12-test-registration.md) |
| R13 | 逻辑预览NULL及字段编码 | 待修复 | |
| R14 | publication读取错误传播 | 已修复 | 原成员读取五个错误实际失败；Root fresh58 与写入前保护、原逻辑解码及目录/owner九完整native全过，见[验证](issue-review-r14-publication-read-errors.md) |
| R15 | 关停唤醒顺序 | 待修复 | |
| R16 | catalog坏行及完整加载 | 已修复 | `0d85f6b9`/`a0d2efde`；691、cold正常API及原catalog_snapshot全通过，见[初始验证](issue-review-r16-catalog-complete-read.md)/[兼容修复](issue-review-r16-optional-catalog-directory.md) |
| R17 | 失败回滚恢复图像 | 已修复 | `9edf30ce`；候选fresh58、23完整native及20重复通过；Root fresh58、最终31完整native与完整PGprotocol实际0，见[验证](issue-review-r17-rollback-recovery-ownership.md) |
| R18 | catalog值快照 | 已修复 | `8f6e3c43`；候选fresh58及提交前/后8完整native通过，Root R14 集成 fresh58 后再次通过 snapshot/complete/cold/owner，见[验证](issue-review-r18-catalog-value-snapshots.md) |
| R19 | TEMP启动恢复 | 已修复 | `18e91f2b`；原失败134→0、候选八完整native和Root十二完整native/PGprotocol通过；新文件清理问题独立R40，见[验证](issue-review-r19-stale-temp-startup.md) |
| R20 | RETURNING子查询 | 已修复 | 原Root native29检查失败；PG/Root 35协议、七完整native及14完整协议全过，见[验证](issue-review-r20-returning-query-owner.md) |
| R21 | ON CONFLICT BOOLEAN条件 | 已修复 | `1463b6be`；119 native、75 PG/候选/Root协议及完整邻居、Root十二native/PGprotocol通过，见[验证](issue-review-r21-conflict-boolean-context.md) |
| R22 | 严格模式FROM/INTO | 已修复 | `799636f7`；36 native及拒绝控制通过；完整协议的独立R31后续通过，见[验证](issue-review-r22-dml-required-keywords.md) |
| R23 | 维护任务持久化门禁 | 待修复 | |
| R24 | EXECUTE参数求值 | 待修复 | |
| R25 | Numeric::withPrecision | 已修复 | `021b3031`；原始12345→123失败保留；41控制O2/ASan/UBSan和原Numeric完整驱动通过，见[验证](issue-review-r25-numeric-precision.md) |
| R26 | 递归CTE类型/typmod | 已修复 | `7cae7c1f`；PG/候选/Root57控制及Root六完整native通过，Root SQRT46控制也通过，见[验证](issue-review-r26-recursive-cte-types.md) |
| R27 | 页压缩全部存活行 | 已修复 | `58f1ee3d`；原生26组O2/ASan/UBSan通过；SQL及进程重启通过，见[验证](issue-review-r27-page-compaction.md) |
| R28 | 页插入空间计算 | 已修复 | `261ffb86`；原始满页插入失败证据；198边界O2/ASan/UBSan及26压缩邻居通过，见[验证](issue-review-r28-page-insert-capacity.md) |
| R29 | LIKE ESCAPE类型比较 | 已修复 | `d7a3293d`；Root项目flags的-Werror=address通过；34绑定控制及相邻协议见[验证](issue-review-r29-pattern-escape-type.md) |
| R30 | Parse/Describe会话上下文 | 已修复 | `8ba70380`；Root标准构建及12控制通过；5个原失败驱动在候选通过，见[验证](issue-review-r30-session-metadata.md) |
| R31 | DELETE ONLY回退错删多行 | 已修复 | `eabfc340`；新12控制PG/候选、原R22完整协议、3 native/3协议/完整PGprotocol通过，见[验证](issue-review-r31-delete-only-typed-owner.md) |
| R32 | 科学计数法字面量的NUMERIC写入绑定 | 已修复 | `40e0a152`；203 native、110 PG/候选/Root协议及完整邻居通过；独立微小scale问题由R34关闭，见[验证](issue-review-r32-scientific-numeric-literals.md) |
| R33 | legacy窗口路径丢失最终ORDER BY | 待修复 | R11新控制发现；配合nth_value的合法窗口查询按partition顺序返回而忽略最终ORDER BY id；独立失败证据保留 |
| R34 | 科学计数法NUMERIC算术scale | 已修复 | `a36efcb5`；120 native、123 PG/候选/Root协议，原111字节快照/known-gap断言及完整邻居通过，见[验证](issue-review-r34-scientific-numeric-arithmetic.md) |
| R35 | INSERT SELECT的ON CONFLICT分句边界 | 已修复 | `a196264d`；42 native、17 PG/候选/Root协议及完整邻居通过，原失败保留；执行owner另记R39，见[验证](issue-review-r35-insert-select-conflict-boundary.md) |
| R36 | gcd/lcm指数NUMERIC结果OID | 已修复 | `d1cd3cce`；63 native、61 PG/候选/Root协议，六native/五协议邻居通过，见[验证](issue-review-r36-gcd-lcm-result-types.md) |
| R37 | TEXT与NUMERIC算术类型检查 | 待修复 | R34邻居发现；显式TEXT加NUMERIC错误接受；原SQL与日志见R34文档 |
| R38 | 表列混合指数表达式投影 | 待修复 | R34邻居发现；`a+1e-40`旧投影错拆`1e`为列，原SQL与日志见R34文档 |
| R39 | INSERT SELECT冲突/复杂source执行owner | 待修复 | R35 parser修复后，已有typed分派仍拒绝source DO UPDATE/JOIN/scalar；完整PG通过而候选失败的驱动及所有断言保留在`tests/compat/insert_select_conflict_owner_known_gap.py`（将随R35提交），不据parser通过宣称执行兼容 |
| R40 | TEMP清理误删空尾相似名称的持久表fork | 候选已提交，待集成复验 | 候选`39ae50a6`；原known-gap断言134→0、新强化file-owner与六完整native邻居通过，见[验证](issue-review-r40-temp-cleanup-file-ownership.md)（随修复合入） |
| O01 | owner健康重试及测试覆盖 | 已修复 | `c9a1e3c3`；原全部owner断言、新真实peer/dirty冲突及10完整native提交前/后通过；Root R14集成fresh58后两完整owner再次通过，见[验证](issue-review-o01-table-owner-health-retry.md) |
| O02 | EXISTS索引错误的访问计划/断言 | 待复核 | |
| O03 | EXPLAIN JOIN图及过时断言 | 待修复 | |
| O04 | HAVING BOOLEAN绑定 | 已修复 | `157e5d2e`；20 native、49 PG/候选/Root协议、五native/六协议邻居与完整PGprotocol通过；原超时及原错误均保留，见[验证](issue-review-o04-having-boolean-context.md) |
| O05 | ORDER BY重复别名 | 待修复 | |
| O06 | 物化视图刷新超时 | 待定位 | |
| O07 | ALTER ARRAY历史超时 | 待复核 | |

原273项功能总账独立维护；关闭这里的缺陷不自动宣称完整PostgreSQL功能兼容。

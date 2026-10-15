# 冻结332bcff4的完整注册回归终态

未过滤的 `bash scripts/build_tests.sh` 已完整结束，2026-10-15 15:15 UTC
确认实际退出1。1382个预期入口各出现一次，无missing、extra或duplicate。
本轮不是全量通过，没有在首个失败后中断，也没有重跑失败入口到绿。

| 输入/产物 | SHA256或固定值 |
| --- | --- |
| HEAD | 332bcff4564f92086fcb01f8b5fec2fd04866cb0 |
| 全部tracked输入 | 48ce094be2274d4cf658296e519213cc812b3dbe8375904e7fa8d0335eb296a0 |
| 生产binary | 6eec23f641672e097ed1eee9aed0768e77fefa16663e7b256974594499c95a59 |
| 生产cache输入 | 12c2c9025b73c3ed08ef57f672a2f2a4e760476df38ab8f719c2697a79e4dd1c |
| 完整日志 | eb9227429f4d417a43e07012310ea989ffe7dce590a22d258cbce6ae602f5a49 |

| 类别 | 全部入口 | PASSED退出 | FAILED |
| --- | ---: | ---: | ---: |
| native | 875 | 874 | 1 |
| 真实Main frontend | 3 | 3 | 0 |
| Python注册入口 | 504 | 503 | 1 |
| 合计 | 1382 | 1380 | 2 |

1380个PASSED退出包含1个明确的TLS环境skip：实际通过1379、环境跳过1、失败2。
`statement_timeout_tls_protocol_e2e_test.py` 报告binary没有OpenSSL支持，未执行
真实TLS控制。其它“already exists / does not exist, skipping”是SQL NOTICE，
不是测试跳过。没有COMPILE FAILED或LINK FAILED，但编译有warning；本轮不证明
全库sanitizer、CMake、真实TLS或完整PostgreSQL兼容。

## 冻结与独立终态核对

原始目录 `/tmp/dbms-review-full-r91.hdjkSELm/` 包含run-terminal.log、
full-regression.log、expected-drivers.txt、driver-summary.log。原执行句柄71579
已实际终止，退出1；包装器的canonical和最终退出码也都是1。
包装器确认HEAD、全部tracked输入、生产binary/cache及全部58份receipt未变。

随后独立只读脚本再次核对清洁HEAD、上述输入/binary/cache、生产及测试build
stamp、全部58份生产receipt、57份test库与对应Root生产库逐字节相同，以及
875+3+504注册顺序与冻结expected文件完全相同，实际退出0。证据为
`/tmp/dbms-review-r46.ifu6SWLp/r91-full-independent-terminal-fence.log`。
只有实际退出和独立核对都完成后才解除冻结、编辑本页；不存在以进度文件推定
终态或在运行中修改源码、测试、SQL、期限、注册集及HEAD的情况。

开跑前复用了同一Root的57份匹配O2生产库，fresh编译test stubs；不冒称重编
全部58个生产单元。875份native驱动仍逐一实际编译、链接和隔离执行，三个
真实Main前端不链接test stubs。上述临时路径为本机辅助证据，不是可移植发行产物。

## 两项失败及后续方法

| 入口 | 本轮原始观测 | 修改/定位方法与边界 |
| --- | --- | --- |
| stored_function_engine_owner_test | line126末尾 `assert(local.dropDatabase(db) == DBStatus::OK)` 中止；日志没有打印DROP实际返回值。前面的owner/事务/回滚断言已执行，但完整入口仍FAILED | 保留完整断言，不猜测返回25或与R51同根因。用户暂缓的DROP/cache专项不因标准回归观测而重开，也不将此清理失败称为已证实的函数owner错值；保持未解决记录 |
| numeric_square_root_protocol_e2e_test.py | line70的 `setup("CREATE SCHEMA sqrt_" + uuid.uuid4().hex[:12])` 在read_until_ready中TimeoutError；46项SQRT控制尚未开始，实际生成的schema名称未打印 | 保持原完整驱动/断言/期限，复现时记录确切语句和自有server的失败线程，区分锁、同步I/O及目录发布，再按确证根因独立修复。不称为SQRT算法/类型错误，不提高期限或重试到绿；若定位进入用户暂缓专项，保留交界证据，不扩大为所有DDL暂缓 |

原始失败片段分别位于full-regression.log的13925–13929及15599–15621行。
没有采集这两个失败的返回状态细分或server失败栈，不能补造原因。
SQRT算法的历史修复及局部参考证据仍按[R01](issue-review-r01-numeric-square-root.md)
原输入读取，不能用本轮准备阶段超时改写为46项计算控制失败。

## 与上一轮及新增修复的关系

[冻结27bac6ee](issue-full-registered-27bac6ee.md)的1368个入口全部保留，本轮
新增11个native、3个Python入口，无删除或重复登记。新增14个完整入口均通过：
统计重复比较/排列、ARRAY_POSITION空数组NULL起点、MOD结果域/原生输入兼容、
ROUND单参数结果域/原生输入兼容、TOAST发布成本/持久化保留范围，以及两项
nextval/setval保存点映像测试。对应[R86](issue-review-r86-array-position-empty-null-start.md)、
[R87](issue-review-r87-mod-result-domains.md)、
[R88](issue-review-r88-analyze-sort-comparison-cost.md)、
[R89](issue-review-r89-round-single-result-domains.md)、
[R90](issue-review-r90-toast-durable-id-reservations.md)、
[R91](issue-review-r91-savepoint-sequence-runtime-images.md)的独立修复，不代表整个族完成。

上一轮六个失败入口本轮均通过：index_scan_corruption_error、默认PG协议、
LATERAL组合、完整COPY、MV目标和CHECK验证。它们没有被删除、缩小或改期限。
这不抹去旧轮失败，不将index清理的一次通过作为R51修复，也不证明所有间歇性
目录/保存点超时共享根因；本轮新的CREATE SCHEMA超时仍须保留。

## 文档与总账复核

终态后515份Markdown、915个本地内联目标、524份Python AST和13份shell语法
检查均通过；文档状态、兼容性契约、504唯一注册/486 standalone盘点、版本
一致性和6个总账单测均通过。`check_gap_progress.py --require-complete` 实际
退出1，明确报告273项尚未完成；机械检查不证明全部历史技术结论正确。

## 后续执行计划与完成边界

1. 独立提交本轮终态、失败方法和文档入口，再继续普通数据完整性工作，不push。
2. 先在当前旧实现复现“已有长值而TOAST计数文件缺失”：同时检查插入状态、
   元数据是否被重新创建及原值是否保持，不能只检查新写入失败。外部夹具尚未
   编译/执行，源码疑点不当作已复现bug。若确证，显式初始化合法CREATE/ADD/
   DROP COLUMN新TOAST代际，同时使已有关系缺失元数据时fail-closed，覆盖保留
   长列、真实NULL和marker形用户文本，独立回归/commit。
3. 对普通CREATE SCHEMA超时保留原完整驱动观测；并行布尔聚合表达式、数组
   类型等价比较、ROUND/TRUNC双参数及原273的未完要求分别验证，不以源码草案
   或既有测试通过跳过要求。下一项进入用户暂缓范围时不自行恢复该专项。

原273仍为22 complete、166 partial、70 unverified、15 deferred_by_user。
本轮完整回归不关闭这些未完项；安全/TDE、CTAS/后台checkpoint、R15、R51及
R65专项范围见[暂缓说明](deferred-scope.md)。README保持长期项目入口，Actions
保持禁用，提交只留本地。总目标未完成。

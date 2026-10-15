# 冻结d974c80d的完整注册回归终态

标准未过滤`bash scripts/build_tests.sh`于2026-10-15 01:25 UTC完整结束，exit1。
1362个预期入口各出现一次，missing、extra、duplicate均为空；不能写成全量通过。

| 输入/产物 | SHA256或固定值 |
| --- | --- |
| HEAD | d974c80d251bcbc524cd34c5380a9ee880fc72d1 |
| 全部tracked输入 | 7abd336755de6541fe8fb50bfb27348f7d245d14ac8c5b5ff37767587a870fea |
| 生产binary | eba80763b7b0168a72ca2dc5e6a98236dddb3fb58086d30673c2492c094abe2c |
| 生产cache输入 | a83f275a6be9b245f8caaad6cb5d1ee4571636505215c45f344bfdd0f2387a66 |
| 完整日志 | 375b3685beefa71f57c7a75783ae18945f6ad5cc0a89ae65a3d87762c9721a9e |

| 类别 | 全部入口 | PASSED退出 | FAILED |
| --- | ---: | ---: | ---: |
| native | 860 | 859 | 1 |
| 真实Main frontend | 3 | 3 | 0 |
| Python注册入口 | 499 | 497 | 2 |
| 合计 | 1362 | 1359 | 3 |

1359个成功退出包含1个因TLS stub环境显式skip的入口，即1358个实际通过、
1个环境跳过、3个失败。TLS运行、全库sanitizer、CMake及完整PostgreSQL兼容性
均不能据此标记完成。没有COMPILE FAILED或LINK FAILED。

证据目录`/tmp/dbms-review-full-20261014-r81.cVtnsQGt/`含run-terminal.log、
full-regression.log、expected-drivers.txt、driver-summary.log。包装器末尾冻结
检查一致；01:26 UTC另做独立只读复核，干净HEAD、tracked输入、binary、cache、
build stamp及全部58份生产对象receipt一致，exit0。独立日志为
`/tmp/dbms-review-r46.ifu6SWLp/r81-terminal-fence.log`。
临时路径只作本机辅助证据，不是可移植发行产物。

生产test cache使用同一主工作区当前57份Root库对象，逐字节核对；当前test
stubs重新编译，未借用私树对象。全部native仍逐文件实际编译、链接、隔离执行；
三个真实Main前端及499个Python入口未过滤，运行期间不编辑tracked文件或提交HEAD，
不修改断言、注册集合或原期限。没有把cache复用称为重新编译全部58个生产单元。
上一轮1360入口集合完整保留，仅新增R79/R80两个native入口，没有删除项。

## 三个失败及下一步方法

| 入口 | 原始观测 | 后续处理方法与边界 |
| --- | --- | --- |
| prepared_dml_plan_test | 所有前置断言执行后，末尾`g_engine.dropDatabase(db)==DBStatus::OK`断言失败，exit134；日志未打印实际返回状态 | 保留失败，不将其猜成已知状态25或R65的同一根因；DROP/cache争用的R51专项仍按用户要求暂缓，不加重试或放宽清理断言来制造通过 |
| join_type_protocol_e2e_test.py | 第94行setup循环的`runner.ours_query`等待响应时socket timeout，尚未进入JOIN结果断言；未记录触发的具体setup SQL | 用原完整驱动、原期限串行复现，外部观察器记录发送SQL/响应/耗时，超时时保留服务端线程栈；区分建表/插入、锁等待和I/O，再按证实根因单独修复提交，不先归咎JOIN类型或提高超时 |
| bound_dml_query_children_protocol_e2e_test.py | 第九项quoted-alias WITH UPDATE已正确返回两行`(2,21)/(3,31)`，随后第83行`ROLLBACK TO bd_case`socket timeout | 保留完整37项矩阵、回滚后的数据/副作用/类型断言；按原期限记录完整回滚调用链与线程状态，修复确证的普通事务问题，再完整复验；不删该项或把中断前正确结果当成全矩阵通过 |

本轮不能证明三个失败共享根因，也不能证明由R79/R80引入；上一轮通过不消除
当前失败。若后续定位确实进入[用户暂缓范围](deferred-scope.md)，保留证据及未完成
状态，不用一般“继续”自行重开暂缓专项。

## 已修问题的本轮组合结果

[上一轮b5def9ba](issue-full-registered-b5def9ba.md)的两个失败入口本轮均实际通过：
原生LENGTH九控制及[R79](issue-review-r79-native-binary-length.md)新增30控制，
窗口legacy排序原44及[R80](issue-review-r80-unbound-scalar-inference.md)新增17控制
全部保留。[R81](issue-review-r81-restore-stage-completion.md)真实restore-stage、
R78轮询负控及完整DIV14入口也实际通过；不将这些局部结果扩大为全量全绿。

通用回调/UDF签名覆盖的R62仍partial。原273功能账仍22 complete、166 partial、
70 unverified、15 deferred_by_user；没有用本轮注册通过数替代完整要求验收。
全部修复保留独立本地commit，未push，GitHub Actions保持禁用。

## 终态后的原样复验，不是修复证明

产品源码和binary未改变。`r82-original-timeouts.log`的串行批次再次在JOIN的
第94行setup等待响应超时；批次遇错即停，bound DML没有在该批次执行。
另行完整运行bound DML的37项矩阵退出0，`r82-original-bound.log`及同名逐项
日志目录；生产输入/binary/全部58 receipt首尾一致。不能用这次通过抹去原回滚超时。

外部观测器只记录原JOIN驱动的SQL/响应/实际15秒socket期限和耗时，未重试SQL
或更改断言。诊断加载了仅在失败时取栈的临时preload，不计为无插桩验收；
`r82-join-observed-1.log`记录175次实际响应，完整退出0，最长CREATE TABLE约
10.86秒，未触发超时，因而没有捕获到失败线程栈。一次独立线程采样看到工作线程
处于`jbd2_log_wait_commit`，只是当时文件系统等待的线索，不证明全部超时的根因。
临时观测器及服务端日志均在前述本机证据目录，没有合入产品。

随后无观测的原完整JOIN再次退出0，`r82-original-join-second.log`及同名逐项
日志目录，生产输入/binary/全部58 receipt首尾一致。binary仍为本页冻结摘要。
这两项间歇性超时保持未解决；继续捕获失败当时的具体SQL/调用链，不增加期限、
改断言或据此关闭原273项。其他可确定复现的普通函数签名问题继续独立修复。

## R83/R84之后的新增CREATE超时观测

以下输入已经包含后续产品修复，不是本页d974c80d冻结全量的重复运行；原
1362入口及三个FAILED不改写。[R83](issue-review-r83-float8-scalar-signatures.md)
的完整PG在line860 `CREATE TABLE toasted_unicode_varchar (v VARCHAR(3000))`
等待响应超时，原socket期限10秒；[R84](issue-review-r84-integral-rounding-result-types.md)
的COPY error/lock release驱动在line53首个CREATE TABLE等待响应超时，尚未
进入COPY主体，原socket期限15秒。两个批次和原SQL/断言均保留，没有证明
它们与JOIN/ROLLBACK TO共享根因，也不将其归为数学结果类型错误。

R84二进制（SHA256 `e4d49ecef2907d1a94c8fe94339c14c19cb0bea42b4704fa315ac3b1890f5688`）
随后在无生产构建/其他Root测试并行的情况下运行原完整COPY诊断：106次query
响应，原驱动全部断言通过，进程正常退出0；证据为
`r82-copy-fsync-1-client.log` 和 `r82-copy-fsync-1-server.log`。
这是有插桩诊断，不计无插桩验收，更不表示超时已修复。两个初始CREATE分别
约0.130/0.161秒，没有捕获超时，故没有发送取栈信号或取得失败线程栈。

临时preload只记录50ms以上的fsync/fdatasync，保持实际调用返回值与errno，
没有跳过同步、改持久化策略或重试SQL。这次记录到三个成功fsync：pg_xact
临时文件约52ms、索引文件约271ms、WAL约224ms；调用均返回0，成功时残留的
非零errno不能当作I/O失败。这里只能说明当次文件同步耗时，不能从通过的
观测推断此前socket超时根因。新外部观察器支持直接加载原PG主模块后hook，
严格核对exact Popen子进程的PID/start-time/exe和preload，仅真实TimeoutError
才观察其线程并取栈；没有对其他已有服务器发信号。工具没有合入产品。

R82及这两项新的超时观测继续保持未解决。原生prepared DML和后续stored
function metadata末尾DROP清理断言失败另行保留；不猜实际返回状态，也不以
普通“继续”重开已暂缓的DROP/cache专项。

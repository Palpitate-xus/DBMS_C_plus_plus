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

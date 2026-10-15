# R91：序列运行态与保存点映像写放大

状态：本范围已修复，原完整MV协议固定三次通过；不把有插桩通过当作验收，
不改变原期限，不宣称原273总清单已经完成。

## 失败与当前定位证据

[R90 后原完整 MV 目标协议](issue-review-r90-toast-durable-id-reservations.md)
仍在 line140 的 `SAVEPOINT mv_case;` 超时，前一条序列 sentinel 返回65。
本项从清洁66df32ec开始，原 SQL、71项目标/副作用/source guards、15秒期限
不变，没有先删失败或降低断言。

外部只读观察器复用真实 fsync 转发器和已预热的取栈 handler，只允许在原
TimeoutError 内，核对单个自有 Popen 的 PID/start ticks/exe/cwd/preload 后取栈。
`r91-mv-savepoint-frozen66-1-client.log` 实际退出0，没有捕获原超时，未发送
失败取栈信号；这不是修复或无插桩验收。fsync日志中残留errno不是成功调用
的I/O错误，诊断会扰动时序。

第二个预定单次文件 generation 观察保持原完整驱动，实际也退出0。记录
`r91-mv-generations-frozen66-1-client.log` 显示71个用户保存点产生71份整库
备份；两保存点之间变化只在 `.sequence_runtime`、实际 UUID 运行态文件和
`pg_catalog/.oid_counter`。无变化的 ROLLBACK TO 未替换 base.stc。这是实际
备份放大证据，不是“每次回滚都会整库恢复”或一次fsync持续15秒的证明。

原生三组对照 `r91-sequence-image-probe-fixed.log`：无序列推进的四轮保存点
只用一份映像；每轮 nextval 后变为四份，四轮无变化回滚都未替换schema。
最初 probe 的缺少 makeIntColumn 参数只导致编译退出1，修正夹具后才执行；
不把 r91-sequence-image-probe.log 的编译错误记成产品bug。

所有本机辅助证据位于 `/tmp/dbms-review-r46.ifu6SWLp/`，不是可移植发行产物。

## 旧基线与修改方法

永久 `savepoint_sequence_runtime_image_test.cpp` 旧实现先后两版均实际退出1：
r91-old-native.log 及加入缺失/越界运行态拒绝的 r91-old-strengthened-native.log
均记录16轮的 aliases=16、sequence_only_kept_image=0、resealed_reusable=0，
数据与非事务序列语义仍正确，最后 nextval=23。首次测试源码另保存于
r91-first-sequence-image-test.cpp，不用加强后的文件改写旧输入。

原 receipt 按所有物理文件 generation 判断复用；合法 nextval/setval 的
原子运行态更新必然改变 inode/时间，所以原完整映像不能复用。SQL回滚本来
就把当前运行态覆盖到恢复后的声明映像，不能回滚 nextval/setval。

修复只在同一完整文件集合、同一 WAL LSN、原目录快照字段检查通过和其它文件
generation 不变时，允许已校验的 SQL 序列运行态值变化。运行态目录仍要求
相同 device/inode/mode；文件须是精确 UUID 命名、原 device/mode 的常规文件。
每个当前运行态记录都实际解析；所有活动声明还要校验 generation、范围、
exhausted/call-state，不能只按名字或header放行。缺失、畸形或parseable但
越界的运行态保守退回原路径，不忽略文件。

验证使用现有序列互斥量的 try-lock，拿不到时只保守回退，避免新增阻塞锁序。
原前后完整 generation 双重检查保留，防止读取校验过程中发生变化。备份自身
的全部 generation、初次 manifest/完整内容比对、内存目录字段、未知sidecar、
schema/声明/WAL/日志边界及其它缓存 quiescence 规则均未削弱。不排除整个
运行态目录、不改 AUTO_INCREMENT 缓存、不删除fsync/改变恢复发布规则。

## 永久回归与最终验收

新原生覆盖16个同名保存点的nextval推进/只读回滚、只有序列变化的回滚、
未知物理sidecar强制恢复、恢复后的再次复用、坏运行态/越界/缺失拒绝及修复后
健康回滚，完整事务回滚后序列仍连续到23。保存点重复命名的旧frame和映像
alias生命周期不变，不能以删除旧frame或让seq回退来减少备份。

第二个永久 `savepoint_setval_runtime_image_test.cpp` 检查 called=false 返回50、
called=true后的61，以及合法向后setval后的1/2/3；新实现用一份映像，两个
仅运行态回滚均不替换schema。历史对照 r91-setval-historical.log 使用逐字节
核对冻结27bac6ee的57份Root库、相同公共头及fresh driver/stubs，实际退出1：
aliases=3、两个kept_image=0，数据仍到3。它是历史API对照，不冒称当前门禁。

`r91-core-native.log` 当前永久新测试记录 aliases=1、sequence_only_kept_image=1、
resealed_reusable=1，仍 final_nextval=23；三项完整 native（含原映像无变化/
保守mutation回退和alias生命周期）全部通过，全部58 receipt与输入签名一致。
真实PG18.6原完整MV协议 `r91-pg18-mv-reference.log` 71目标及source guards全0。
`r91-build.log` 标准构建实际重编 TableManage.cpp 并链接/重复构建退出0；公共
头未改，其它57份生产对象保持匹配，不称全58份实际重编。当前binary SHA256：
`6eec23f641672e097ed1eee9aed0768e77fefa16663e7b256974594499c95a59`。
`r91-final-native.log` 二十三个不同完整native全0，包含两个永久新测试、原
无变化/损坏映像保护、alias/同名/只读保存点、序列full/DDL回滚/legacy迁移、
DDL事务、失败回滚图像与重试、物理restore/manifest、UNLOGGED、TRUNCATE/
serial/temp serial，以及R90两项真实持久化/并发/SIGKILL边界；全部58 receipt
与输入签名一致。setval新控制 aliases=1、两个kept_image=1、final_nextval=3。

按R90失败时的同一顺序原样运行四个完整协议，r91-original-failure-protocols.log
实际全部退出0：默认PG、LATERAL、MV目标和CHECK验证；终态输入/binary/全部
58 receipt一致。原MV再预定两次完整无插桩复验，r91-mv-fixed-repeats.log 的
repeat-2/3均0，故当前该入口固定三次通过，不是失败后重试到绿。

同一外部metadata观察器的当前完整驱动
r91-mv-generations-current-1-client.log 也0，71个保存点全部只保留1份映像，
与旧71份对照；它只支持备份数量测量，不计入无插桩验收次数。
`r91-neighbor-protocols.log` 十四个完整协议全0：sequence、sequence DDL rollback/
identity、只读保存点、事务隔离、bound DML、cold transaction backup、restore
parameter status、真实取消、CLI、原完整COPY及COPY error/lock release、DIV14、
stored-function atomicity；生产输入/binary/全部58 receipt首尾一致。加前述四个
原失败入口与MV两次固定追加，本轮共18个不同完整Root协议、20次执行均退出0；
PG参考与外部有插桩观察不混入该计数。

旧冻结27bac6ee的57份test库在更新缓存前逐个校验并另存
`/tmp/dbms-review-27bac-test-libraries.Sag9MiHB/`；producer记录为
r91-historical-cache-archive.log。后续 build/test_obj 将是当前Root库，不能再
拿它冒充旧库作历史基线。r91-full-cache-preparation.log 仅逐字节复制同Root
当前57份生产库、fresh stubs，并核对全部58 receipt，不称重新编译57份库。

独立 r91-terminal-fence.log 再核对全部58 receipt、cache/build stamp、上述
binary哈希、57份test库与Root库逐字节相同及重复构建，实际退出0。提交前514份
Markdown/900个本地内联目标、524 Python AST、13 shell语法、504 unique注册/
486 standalone E2E、文档状态/兼容契约/版本及六个总账单测均通过。文档检查
不证明全部历史技术结论已在当前输入验收。r91-require-complete-actual-exit.log
实际退出1，原273仍22 complete、166 partial、70 unverified、15 deferred。
新组合875 native +3真实Main
frontend +504 Python =1382入口，计划在清洁提交后运行原始
`bash scripts/build_tests.sh`，预留证据目录
`/tmp/dbms-review-full-r91.hdjkSELm/`。实际启动/终态以该目录原始日志及真实进程
句柄为准，这段计划不是已运行或全量通过证明；运行期间不改tracked输入/HEAD。

## 未完成范围

后续新1382组合已于2026-10-15 15:15 UTC实际完整终1，见
[冻结332bcff4终态](issue-full-registered-332bcff4.md)：1379实际通过、1 TLS
环境skip、2失败，全部冻结输入/58 receipt/57 test库字节独立复核0。本项两新
native及原完整MV目标均通过；失败为另一入口末尾DROP断言和SQRT准备CREATE
SCHEMA超时。不改写旧失败，也不将本项推广为完整catalog/WAL或原273完成。

这不删除完整DDL快照或改成仅凭逻辑epoch许可恢复，不实现统一catalog/WAL，
也不证明旧15秒超时的所有耗时或其它协议超时共享根因。本项提交时尚未运行
的新1382组合现已有上述独立完整终态，不能把其两项失败写成全绿。原273总账
和暂缓专项状态保持；逐项本地commit，不push，Actions禁用。

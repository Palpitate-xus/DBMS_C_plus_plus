# R90：TOAST ID 持久化上界预留与原完整 COPY 验证

状态：本范围已修复，原完整 COPY 固定三次通过；其它 SAVEPOINT 超时及完整
持久性总项仍未完成。

## 原问题与修复方法

[原完整 COPY 失败现场](issue-full-registered-27bac6ee.md)在等待 COPY 256
完成时超时。前十五秒内，同一 COPY 线程有九组 TOAST 计数文件/父目录的慢
fsync，合计约 5.615 秒；这不是全部查询耗时，也不证明其它超时共享根因。
[R88](issue-review-r88-analyze-sort-comparison-cost.md)减少统计排序比较后，
原完整 COPY 仍在相同 line262 超时，需要继续处理实际写入路径。

旧 allocToastId 每个外部值都把计数器加一，并走原子临时文件写入、文件
fsync、rename 和父目录 fsync。新实现一次先持久化最多 128 个 ID 的排他
上界，只有完整发布成功后才向调用者返回区间中的 ID。重启丢弃未用区间，
使用已有磁盘上界继续分配；ID 可以有空洞，但不从未确认持久化的区间发号。
UINT64_MAX 仅作耗尽上界，不是可返回的 ID，末尾区间按剩余容量截断。

不删除或跳过任何预留发布的同步屏障，不调整自动统计开关/阈值、COPY
协议、socket 期限、undo/WAL/heap 提交或数据格式。原 24 字节 TID1/version1/
checksum 格式和 8 字节 legacy 读取保留。旧进程读取新上界只会跳过未用 ID。

## 缓存与失效边界

每线程只保留一个区间和一个 O_CLOEXEC 只读描述符，不引入全局 map/互斥量，
不随表数量增长。切换关系或线程退出时释放描述符、烧掉未用 ID。

每次分配仍持有原跨进程 flock，并重新读取实际计数器、检查精确长度、版本、
checksum 与合法上界；缓存不遮蔽截断、校验和损坏或 legacy 格式替换。命中
还要求进程 PID、上界以及当前文件/持有 FD 的 device/inode 一致。持有旧 inode
直到失效，避免 unlink 后 inode 号回收让旧区间被误认为新关系的区间。
其它线程/进程发布、TRUNCATE 或恢复替换文件时，保守烧掉旧区间重新预留。
fork 后 PID 不同也不能复用继承区间；本轮实际 crash 测试使用全新 exec。

发布在 rename 后父目录同步失败时也返回 0，不安装区间；下次从读到的
新上界继续预留，不发放尚未确认区间。无法打开只读持有 FD 时，只返回已
持久化覆盖的首个 ID，其余区间作废，不用无身份保护的内存缓存。

## 旧基线与首版失败

证据目录 `/tmp/dbms-review-r46.ifu6SWLp/`：

| 运行 | 实际结果 |
| --- | --- |
| 匹配 ea604242 的旧实现，r90-old-cost.log | 64 次分配/64 次计数文件发布，floor=65，全部 ID 先有磁盘上界；操作次数门禁实际退出 1 |
| 首版候选，r90-core-native.log | 同 64 次分配仅 1 次发布，floor=129；随后真实 reservation 测试在 UINT64 尾部区间断言退出 134，未提交首版 |
| 匹配最终边界修复，r90-final-core-native.log | 64 次分配/1 次发布，八组 reservation 边界与原完整 TOAST 驱动全部退出 0；全部 58 receipt 与输入签名一致 |
| 原完整 COPY 单次无插桩，r90-copy-unmodified.log | 原 15 秒期限、全部 SQL/帧/断言不变，完整驱动实际退出 0；含原 line262 的 COPY 256 和后续慢读 COPY TO 取消检查 |
| 预先固定两次附加复验，r90-copy-fixed-repeats.log | repeat-2 与 repeat-3 两个完整 COPY 均实际退出 0；各自原始日志/首尾输入、binary、58 receipt 独立保留，没有失败后重试到绿 |

首版先拒绝磁盘 UINT64_MAX 上界，误拒绝已确认末尾区间中仍小于该上界的
剩余 ID。修正为先尝试有效预留区间，再拒绝没有可用区间的耗尽状态；不改
原最大边界断言，也不返回 UINT64_MAX/0 或让算术溢出绕回 1。

## 永久回归与验收

`toast_id_publication_cost_test.cpp` 使用真实 API 分配 64 次，逐次读取真实
24 字节计数文件并校验 magic/version/checksum，断言 ID 唯一、严格小于
已经发布的上界，记录实际文件内容变化次数。不是 mock 或墙钟时间测试。

`toast_id_reservation_test.cpp` 包含相同内容原子替换导致的 inode 失效、同
inode 的坏 CRC/截断及恢复、原 post-rename 目录同步失败注入、四线程 64 个
不重复 ID、两次全新 exec 分配后实际 SIGKILL、活 parent 区间失效与再次冷
启动、真实 TRUNCATE 重置、legacy 迁移/同 inode 格式替换、uint64 尾部容量、
耗尽与零上界。没有把正常子进程 exit 当作 SIGKILL 或将 kill-point 扩为掉电证明。

`r90-barrier-historical-invariants.log`：最终带同步起跑屏障的永久 reservation
测试链接逐字节验证的
冻结 27bac6ee Root 57 份库、fresh driver/stubs 也全部通过。公共头未变，
producer 与独立校验过程保留在 r90-run-barrier-historical-invariants.sh；
早期无起跑屏障的 r90-historical-invariants.log 另保留，不覆盖。它们是历史
同 API 的正确性对照，不冒称当前候选或全注册组合验收。

最终构建标准/重复退出 0，首版及末尾区间保护分别实际重编 TableManage.cpp，
其余 57 份生产对象保持匹配、公共头不变，不称全 58 份重新编译。最终 binary
SHA256：`569ba8a1d470bd9e2272e7e017c3862e2f52ac3e861df30744d079b39d4c62e1`。
三次固定原完整 COPY 全部通过。r90-final-native.log 二十个完整 native 全0，
含两个永久新测试与 BYTEA、TOAST 读错/缺文件、共享 heap/index、vacuum、
TRUNCATE/恢复、UNLOGGED/真实SIGKILL、物理备份/恢复/替换/manifest、heap WAL
generation/crash/identity、缺索引 guard；加核心批次的原完整 toast_test，
共21个不同完整 native 有当前产品通过记录。全部58 receipt及输入签名一致。

r90-neighbor-protocols.log 七个完整协议全部退出0：COPY error/lock release、
COPY integer input、restore parameter status、cold-start transaction backup、
真实取消、CLI取消、完整DIV14。生产输入/binary/全部58 receipt首尾一致。
包含下节四个历史失败入口的复查后，本轮共12个不同完整协议、14次执行，
13次退出0、MV一次退出1；不称全部协议通过。

独立 r90-terminal-fence.log 再核对全部58 receipt、cache/build stamp、上述
binary哈希与重复构建，实际退出0。提交前513份Markdown/893个本地内联目标、
524 Python AST、13 shell语法、504 unique注册/486 standalone E2E、文档状态/
兼容契约/版本及六个总账单测均通过。文档检查不证明全部历史技术结论当前已验收。
r90-require-complete-actual-exit.log 实际退出1，原273仍22 complete、166 partial、
70 unverified、15 deferred；最新873 native +3真实Main +504 Python =1380入口，
这套组合尚未全量运行，不能用以上定向结果替代完整注册回归。

## 其它冻结失败入口的原样复查

COPY 三次之后，另串行运行四个原完整协议，不修改 SQL/帧/断言/期限，
不在失败后重试。r90-original-failure-protocols.log 包装器对每项记录实际
状态并始终执行终态 fence；整体实际退出 1，不因末尾 fence 通过改称全绿。

| 原完整入口 | 实际终态 |
| --- | --- |
| postgres_protocol_test | 退出 0，真实完整默认驱动，原 CREATE dec_t 超时本次未再现 |
| materialized_lateral_composition_protocol_e2e_test | 退出 0，用户 TEMP 关系/物化组合原断言保留 |
| materialized_view_dml_target_protocol_e2e_test | 退出 1，仍在 line140 的 SAVEPOINT mv_case 等待超时；本次前一条 nextval sentinel=65，不是冻结原69 |
| check_add_validation_protocol_e2e_test | 退出 0，extended 的23514/未发布列等原矩阵保留 |

生产输入/binary/全部 58 receipt 首尾一致。三个通过不证明 TOAST 发布是它们
的原根因，也不将原间歇性超时自动关闭。物化视图的副作用/目标拒绝断言已
执行的部分不冒称完整矩阵通过；接着定位实际 SAVEPOINT 路径。R51 末尾
DROP/cache 专项继续暂缓，不因本次复查重开。

## 仍未完成

单线程交替关系/其它 writer 持续替换会丢弃未用区间，正确性优先于命中率；
本项不承诺所有并发负载的性能增益。分配仍每次读/校验元数据并加文件锁。
这不是所有存储/WAL/catalog/统计状态的系统掉电持久性证明，也不是原六项
冻结失败的自动关闭。新组合未全量运行，原 273 状态保持，暂缓专项不重开，
逐项本地提交、不 push，GitHub Actions 禁用。

后续[R91](issue-review-r91-savepoint-sequence-runtime-images.md)独立处理合法序列
推进导致的保存点映像复制；原MV目标协议固定三次通过。本页R90输入下的真实
SAVEPOINT失败继续保留，不用后续通过改写旧日志或推断所有超时共享根因。

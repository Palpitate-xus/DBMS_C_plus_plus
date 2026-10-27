# R289：统计快照锁不能隔离同进程线程，且被无关 close 提前释放

状态：有限修复；不代表统计持久化全部边界或原 273 条总清单验收。
关联 MON-03 的 persistent counters / snapshot semantics，仅关闭下述锁缺陷；
MON-03 整体和 MON-09 完整 pg_stat_statements 要求继续开放。

| 问题 | 实际影响 | 修改方法 |
|---|---|---|
| StatsFileLock 使用进程关联的 F_SETLKW 锁 | 同一进程的两个实例能同时进入同一快照临界区；基于 PID 的临时发布名也可能冲突，读改写丢更新或发布失败 | 每次 open 使用 F_OFD_SETLKW，按打开文件描述而不是进程持锁 |
| 同一 inode 的其他描述符 close 会释放进程锁 | 一个实例退出或无关 open/close 可使仍存活的实例不再排斥其他进程 | 对应以 F_OFD_SETLK 解锁，仅释放本实例的锁；其他独立描述符的 close 不释放它 |

只修改 `src/process/StatsPersistence.cpp`：构造/析构的两种锁命令及说明。
不改头文件、类布局、统计格式、锁文件名称、合并公式、发布顺序或调用者 API。
现有 EINTR 重试、获取失败时关闭描述符及 `ok()==false` 语义保留。
SqlStats 和 RuntimeStats 均在取得文件锁后取得各自内存统计互斥量，顺序未改。

Linux 的进程锁范围、任意 close 的影响、OFD 语义及两类锁相互冲突，见
[Linux fcntl locking 手册](https://man7.org/linux/man-pages/man2/fcntl_locking.2.html)。
未使用与旧 fcntl 锁相互独立的 flock 来替换协议。旧版进程仍能与新版互斥；
这里指遵守旧锁生命周期的持有者，不能修复未升级进程自身的多线程提前解锁。
OFD 需要支持该命令的 Linux 内核/文件系统，不支持时获取失败，不退回已知不安全的旧锁。
这不是网络文件系统、系统崩溃或跨平台的完整证明。
OFD 锁可随 fork/dup 继承打开文件描述；生产源码当前没有实际 fork 调用，
任意嵌入式调用者的 fork/exec 描述符生命周期与父进程异常退出仍不在本次验收范围。

## 同一最终夹具的前后对照

`tests/stats_file_lock_test.cpp` 最终固定 61 项。Root 集成投影及纯已接受源码
旧版各完整 61 项、15 项失败、exit 1；不是 15 个独立缺陷。修复后两侧
同一夹具各完整 61 项、0 失败、exit 0，未删案例或延长 deadline。

覆盖同路径、dot-dot、硬链接及符号链接别名的线程互斥；不同快照互不阻塞；
无关描述符关闭；仍存活的第二实例对旧 POSIX 进程锁的排斥；旧版进程先持锁、
等待线程收到三次真实 SIGUSR1 后继续等待并最终成功；失败 open；析构释放。
内核锁探针使用另一个真实进程，而非把预期实现写成唯一 oracle。
另做真实 read/modify/fsync/rename/directory-fsync：同进程三线程完成 60 次更新，
两个进程内合计五线程完成 100 次更新，逐项检查成功、最终精确值及临时文件清理。
计数文件是私有测试快照，不把它称为已经复现业务表行丢失或所有统计合并问题。

等待门保持原 120ms 排斥观察、5 秒唤醒及 10 秒子进程 deadline，整体仍为 180 秒。
最初 52 项旧版两侧也各 15 项失败，原始证据保留；最终版保留全部原案例，
在多线程 fork 之前准备子进程探针路径，并增加旧进程先持锁和信号控制。

## 构建与有限回归

每侧只新编译一份 StatsPersistence.cpp；另外 120 个生产对象及两份 stub 均核对
当前源码、134/131 个头、MMD 依赖、编译器和对象哈希后复用，不冒称 fresh。
新夹具两份及 8 个统计邻接测试 TU 新编译；既有 88 个测试/Main 对象匹配复用。
98 个测试/Main 程序及两台服务器均新链接当前库；六个真实 Main 对象不是新编译。
R283 私有布局仍匹配，不使用更早布局对象。

完整有限正常矩阵 136 条：92 native、6 Main、38 wire（每侧 19 个完整协议入口），
全部实际 exit 0。包含 SQL/runtime 持久化、planner 统计、四个 ANALYZE/MCV/
histogram 协议及此前完整有限存储/字符串矩阵，未改原夹具。
另两侧分别新编译 StatsPersistence.cpp 与新夹具做 scoped UBSan，完整 61 项均 0，
共 138 条当前运行记录实际全 0；其他库为正常对象，不宣称整库 ASan/LSan/TSan。

提交仅包含本 CPP、一个自动发现的原生测试及本文。已接受源码投影为 2310 文件；
980 native + 598 注册 Python + 3 Main = 1581 仅为库存，未宣称全量执行。
五项静态门两侧提交前后均通过，require-complete 两侧仍实际 exit 1；
原冻结 1492 矩阵和原 273 条状态未重写。R288/R99 用户暂缓，R287 依赖 held，
其他用户暂缓项保留；没有接受其候选或恢复 R222，没有 push，没有启用 Actions。

证据目录：`/tmp/dbms-r289-stats-lock.Parad7xs`。临时目录不保证长期保存。

| 证据 | SHA-256 |
|---|---|
| R286匹配基线及七源隔离 | `b18a6c24e2ff7b663f6299b5a2fd142d536bc5fcdfe12a865436a1ea7967f5f4` |
| 首轮52项完整失败 | `841e4193ef44bbadda4727571db2923da63ef2a4265e502caf74fa05124df6d2` |
| 保留原案例的探针及旧进程/信号扩展 | `b70737dd69bc9192edc5323880ede9d572d2faaaaae6f101ffc40d8b58325335` |
| 最终同一61项旧两侧完整失败 | `8c777bfe6c12fc6bd7411696f5633cd1b204709bdd964ec7ef28902cb171204f` |
| OFD增量与工作区保留 | `df898948e53241ed49f6a53ad86d435ff8f67caa2dcb01f847aaec097ceee174` |
| 两个生产及两个新原生TU构建 | `9b751b1310ea36e7c2bce08388c9cb920436e5ae8aa7ecccce6004c1b325dc0b` |
| 98新链接程序及8个邻接新编译TU | `963ecbd3c7a796da010a39742401559652fc495d2a62bba0000ff5711ede6aec` |
| 最终同一61项新两侧通过 | `728f934e7fd71ad14e8b26d3d9040aa33047f12f5184227c6c4cc96d23a8d183` |
| 136条当前有限运行 | `bfec06135e3f3c4465b9e2123d71e8196cf5ab7d9c6831299910a13d6a072e3d` |
| 两个scoped UBSan完整61项 | `42dccb20652cc045b3f422a84758169e67b2531784068e8bc538db21899ffc7c` |
| 源码/头/MMD/对象/程序与工作区复核 | `a8371401b15c00090cf06f45abcac5e34bdd398d0b32c98c758748094f7ef40f` |

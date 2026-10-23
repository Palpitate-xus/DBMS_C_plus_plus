# R231：VACUUM 页内容锁和维护调用的锁作用域

状态：有限并发维护修复通过；不是 VAC-01/VAC-04、STO-04、TXN-07、OPT-17 整项完成。

## 已复现的问题

普通 vacuumPage 直接 compact/markDirty/freePage，没有使用 INSERT/UPDATE/DELETE
所用的独占页内容锁。buffer pin 只防止淘汰，不能保护可变页内容；项目的 Shared
关系锁与 IX 写者兼容，因此 CONCURRENTLY/native concurrent 路径并不靠表锁排斥 DML。
旧版在另一线程实际持有页 Shared/X 时已改变该页；不能以返回的 live rows 仍正确
代替临界区正确。并行线程还缺少错误回传、创建失败后的 join 及线程本地取消状态。

首个页锁候选解决了临界区问题，但成功调用后还有四个 caller 关系锁状态失败。
vacuumToast 的 lockExclusive 会转换 caller 已有的 Shared token，plain unlock 又将
这个借用 token 释放；外层 checkpoint 不能复活已经没有 holder 的锁。升级是转换，
不是新增 token，不能通过改变公共约定掩盖 helper 的作用域错误。

## 修改方法

- 压缩/回收在实际 pageLockExclusive 下进行；每页 checkpoint 保护 caller 已有的
  页模式及其他锁，RAII 管理本次取得的锁，pin 在内容锁释放前解除。失败转换
  已丢失旧弱 token 的嵌套情形仍在下文待审，不以这里的 guard 证明它已修复。
- 整个 VACUUM 用原入口 checkpoint 管理自身锁。vacuumToast 的关系锁和引用扫描
  页锁也按 checkpoint 恢复，不盲目 unlock 借用的 token，并对读取 pin 做 RAII。
- worker 继承实际 SessionInterruptState 和锁超时；异常捕获后停止剩余工作，先
  join 所有已启动线程再在 caller 重抛第一个错误。创建线程失败也执行同样 join。
  一个 worker 失败会使仍在等待的其他 worker 协作退出，不改变会话取消位。
- 页锁失败精确 55P03，实际取消精确 57014；受检查的读取/映射初始化失败不伪报
  清理成功。线程设置初始化失败后不在 cleanup 中重复初始化。

没有修改公共头、tuple/文件格式、关系锁兼容矩阵、排序器或暂缓模块。

## 完整原生验证与失败记录

新119项使用八个独立多页碎片堆，覆盖串行/四 worker、实际 Shared/X holder、
等待图、锁持有期间的整页字节、36条完整存活行、超时/取消及 caller 的原锁状态。
时间上限只是挂起保护，不是性能断言；没有通过延长既有超时或删除错误检查验收。

| 阶段 | 两侧实际结果 |
| --- | --- |
| 首夹具仅 MVCC DELETE | 51项/9失败，未制造实际空闲行指针，不算有效产品基线 |
| 改用实际 INSERT undo 的 V2/V3 | 完整119项各20失败；包含8个未绑定 namespace 的夹具 cleanup 失败 |
| 正确绑定 native caller namespace 的 V4 旧基线 | 完整119项各20失败；空闲指针、锁/字节/行前提均真实，caller 状态错误也实际复现 |
| 首个页锁候选 | 完整119项各4失败，helper 借用关系锁仍丢失 |
| 已接受 R232 为基线的未修复父版本 | 完整119项两侧各20失败 |
| 首轮 V5 最终候选原生 | Root最后DROP/清理abort，未取得成功footer；纯提交119项退出0 |
| 同一二进制、原断言/超时不变的独立复查 | Root完整119项退出0，纯提交原119项也为0 |
| 每侧9 native +6协议既有回归 | 全部退出0 |

最终候选共32次有限程序/驱动全部退出0。历史20/4是重复控制失败数，不是独立bug数。
所有首源码、诊断、失败日志保留；V2/V3与V4同为20不表示失败分类相同。
首轮V5 Root日志在最后删除测试库时失败，随后cleanup出现extent_pending文件消失错误，
实际abort134。没有改源码、二进制、断言、超时或持久化选项，独立新目录重跑取得
完整119/0；这不是该DROP失败的根因修复证明。首失败仍普通待审，不能从后来PASS
倒推它与本项无关或称所有历史尝试都通过，也不能据此声称当前完整工作区/总清单通过。
没有修改原 lock_manager_concurrency 的转换约定断言。独立 R232 只修复成功转换
丢弃多次持有深度的问题；其已接受218项也作为本项完整回归，不与 helper 错误混同。

回归覆盖 parallel_vacuum、VACUUM FULL/TOAST、跨 backend/锁并发、原生错误释放、
derived map coherence、split_part 及锁深度；协议覆盖维护/analyze、表/行锁超时、
死锁、statement timeout 和 subtransaction 错误释放。未恢复默认协议主入口。
内部页锁 API 没有 PostgreSQL 的一对一接口，本次不声称新 PG 差分通过。

## 未关闭要求与版本记录

完整 MVCC prune/freeze、VM horizon、partition vacuum、索引清理、WAL/crash 原子性、
全资源限额/ResourceOwner及所有故障矩阵仍不由此证明。线程创建失败的 join 分支
有实现但本轮没有真实资源耗尽注入，不能把取消/锁超时当成整库 OOM 证明。
关系/页锁失败升级时丢失旧弱 token 的分支仍普通待审；本项不关闭它，也不改写
既有兼容矩阵。119项超时/取消调度的 VACUUM 线程此前没有该页的弱 token，
因此不能据此证明“已有弱锁再升级失败”的嵌套 native API 所有权正确。

以已接受 R232 ba60c239 为依赖，最后阶段两侧 fresh TableManage TU及新/九个原生
测试，其他自己的源/头/对象、编译器/stub匹配后复用并新链接。Root62对象/134头、
纯提交60/131，布局不混用；不是normal/repeat或全部fresh。Root Main仍用pre-R229
匹配donor，不编译/运行跳过草稿，不能宣称当前整份工作区构建。
一次辅助脚本在完成精确编辑后因旧 fixture receipt 名称冲突退出1；没有覆盖它，
后续反向核对两侧完整CPP都恢复原V1摘要后，以独立V5收据继续，记录不充当构建失败。

2,254纯源码输入对应942 native+589注册Python+3 Main=1,534清单，不是完整全套运行。
独立index仅提交本项TableManage、新119项原生和本文档，其他工作区及原暂存18保持。
R229及其他用户暂缓不恢复；原273/冻结1492历史不改写，总目标未完成。不push，Actions禁用。

本地证据目录：/tmp/dbms-r231-vacuum-page-lock.9htNTxJO。

| 记录 | SHA256 |
| --- | --- |
| 旧完整V4基线 | `0cae438f1c2f3d56d3b2eb10c54038a3d8ea37c73a2c0a1df047cb22beee14b5` |
| 首候选119项失败 | `166e952d5505b830b6a4e7b5884b7abb833ec1130556fbd99d11c8f54f938e9c` |
| 接受依赖后的父版本基线 | `da25a3f63cce0a573c334857bc7749e48611804424f267d9c51944413ef43d12` |
| V5原生首轮含DROP失败 | `443f092dd569dfea787f3f02d662b5221881edd79545fac83868aa5af881ad80` |
| 同一二进制原断言复查 | `161891bf6945eecba6603630625795fd303b50b372d441eaaa8281e4ea916f8e` |
| 实际构建 | `6f8d84016dc3e8ffb316479241c7f116f6396c5ecd83aeb0286d96d329a2cfe0` |
| 最终32次运行 | `02ec29bbd420776b3f02f4d3c79d89dd9aad5d0b6563e8814240e372948aa50d` |

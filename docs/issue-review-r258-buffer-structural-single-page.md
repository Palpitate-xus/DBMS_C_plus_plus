# R258：单页刷盘遵守结构页最后发布约定

状态：有限接口顺序修复通过；STO-03/STO-04及原273总清单仍未整体完成。

## 问题与修改方法

| 问题 | 影响 | 修改方法 |
| --- | --- | --- |
| `BufferPool::flushPage()`直接写指定结构页，不走既有write-last协议 | 脏数据页仍在缓存时结构页已写入；此前淘汰的数据页也没有先经过同步屏障 | 指定结构页先标为dirty，再在同一mutex内调用既有`flushUnlocked()`：数据写入、首次同步、结构页写入、最终同步 |
| 已clean的结构页仍有显式重写要求 | 直接调用整池flush而不标dirty会静默跳过目标页 | 强制标dirty保留现有flushPage的clean页修复语义；失败或异常后保留待重试状态 |

这是同一个公开接口契约缺口，不把64个失败控制称为64个独立bug。原整池刷盘及淘汰
已经遵守write-last约定；仅单页入口绕过。`PageAllocator::flushPage(0)`本来就拒绝，
不能据此宣称现有PageAllocator提交路径已观察到该结构页发布故障，也未声称发生了
真实断电丢失数据。更完整的content lock、pin所有权和恢复协议不由本项闭合。

生产改动仅BufferPool.cpp内7行，不改公开头、布局、WAL/page格式、Main、全局锁或
TDE逻辑。未配置write-last或目标为普通页时保留原单页写回行为，不新增普通页fsync。
目标未缓存仍沿原有no-op返回；结构页刷盘沿用完整flush的WAL barrier与错误处理。
显式flush仍由调用者负责其页面内容同步，不借此恢复后台写回专项。

## 同一68项完整旧新对照

Root及独立纯提交投影，旧实现各完整68控制、64失败、进程退出1；修复后各完整68
零失败、实际退出0。旧新使用逐字节相同的夹具和期望，没有删除或放宽失败断言。

| 控制 | 数量 | 检查 |
| --- | --- | --- |
| 结构页0/3 × 初始dirty/clean × 缓存首/尾位置 × 7种正常/失败模式 | 56 | 精确write/fsync顺序、WAL barrier先于任何写入、完整页内容、dirty保留、重试及新reader重开 |
| 配置/不配置结构页 × 普通页dirty/clean | 4 | 普通单页仍强制重写、无新增fsync、不写脏结构页、未缓存目标no-op |
| 结构页0/3 × dirty/clean × 有/无此前淘汰的数据页 | 8 | 无其他脏frame时仍先sync，再强制结构页重写和最终sync |

7种模式是正常、barrier返回false、barrier抛标记异常、数据pwrite EIO、首次fsync
EIO、结构页pwrite EIO、最终fsync EIO。数据写入或首次同步失败时不得提前写结构页；
失败后各待写frame仍dirty，健康重试必须恢复完整顺序和内容。正常首轮后的强制重试
只写结构页，但仍有前后两个屏障。异常来自测试callback，不宣称内核会抛C++异常。

夹具使用真实私有文件，以dlsym转发实际libc pwrite/fsync，仅在arm期间按dev/inode
匹配施加确定性EIO并记录固定容量事件；全部实际成功字节来自真实I/O。未模拟提前
成功或把失败调用转成成功；不使用概率调度或性能阈值。只清理自身mkdtemp目录。

## 构建、回归与限制

90次有限程序运行全部退出0：每侧新68控制、34个既有native、3个真实Main frontend、
7个完整协议。既有native包括完整CLOG状态442及读取49、R246—R253缓存回归、共享
heap/extent、derived maps、WAL codec、BTree topology、savepoint alias、事务提交失败
与insert/update/delete rollback、跨进程txnid和redo crash。七个协议为savepoint_read_only、
index_corruption、explain_json_analyze、explain_json_cache、explain_timing、
commit_failure_recovery和transaction_begin_error_state；原断言、默认超时未改变。
这不是全库存验收，也不运行默认postgres_protocol MAIN或用户暂缓COPY/security/TDE。

定向ASan/UBSan完整68项零失败、实际退出0，未弱化检查选项。只给BufferPool.cpp及
新夹具插桩，PageCrypto是匹配的未插桩依赖；不是全库sanitizer、TSan或TDE验收。
生产编译只保留既有notifyOrphanDrained未使用参数警告，新夹具无编译警告。

每侧fresh编译BufferPool.cpp、新夹具及34个既有native；其余生产对象按源码、全部
134/131头、编译器和对象SHA核对后复用。Root62/纯提交60生产CPP保持分离，没有
混用布局，也不是本轮全部122CPP fresh或normal/repeat build。6个Main frontend
复用R247匹配对象，先重链接还原原二进制SHA，再链接当前库运行，不链接test_stubs。
Root服务Main继续使用已验证pre-R229源码投影，不使用工作区R229草稿或旧dbms_main。

2283个提交源码输入与测试纯投影一致；959 native+597注册Python+3 Main=1559只是
库存。独立index仅提交本项CPP、新测试、本文档3路径。原暂存18路径及其他工作区
文件字节保持不变。冻结1492结果和原273历史状态不重写；总目标仍未完成。
R257按2026-10-26用户要求暂缓，候选归档后生产已恢复；不复用其候选CLOG对象。
其他所有明确暂缓继续保留。不push，GitHub Actions保持禁用。

证据目录：`/tmp/dbms-r258-write-last-page.1iaToA0d`。

## 证据SHA256

| 记录 | SHA256 |
| --- | --- |
| 完整旧68项64失败 | `f5b5ae11264ef9ccb448ed2081d432b747d3cf351b665f992d170a935ebd3c4d` |
| 结构页单页入口使用既有有序flush | `44360378298f4b0bbdf91fe2af236d130679dd7cdb01a03bb1ffb260b9df4dbf` |
| 匹配对象及新生产CPP构建 | `f47a4809e92dd766ae7e5fc16cae65c1a8541ad65ca36879e9ca544bbe6a5b24` |
| 新68项双侧运行 | `9ff21136217043ef70593ca91463d8c772c4358d45586fe1f4cd7fe0aa601a58` |
| 定向ASan/UBSan完整68项 | `d9b848f9639e56ce0f576f131406e8b9e4100e53683ee6aa533625ec986e8ad4` |
| 68既有native fresh与6Main复现重链接 | `eeebb1d268020ea9436897e9c2cbb8657a51c43d6ffabe84dc4dbdcd0c890bb2` |
| 完整90次候选运行 | `645f399674b985d4de276c2f91fa9f8b6b109134b4401bff0246231eae16e89d` |

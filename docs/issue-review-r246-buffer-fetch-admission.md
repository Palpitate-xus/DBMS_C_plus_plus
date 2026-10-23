# R246：页缓存加载准入竞态及无效状态

状态：三类有限缺陷修复通过；对应原STO-04的缓存映射与pin一致性，STO-04整体仍未完成。

## 缺陷与修改方法

| 缺陷 | 实际影响与复现 | 修改方法 |
| --- | --- | --- |
| 快速未命中与慢路径获取锁之间未重查缓存 | peer已缓存同页并持有pin，单帧池仍返回NULL；多帧池重复pread，注入这次不必要读取的EIO后错误拒绝有效缓存页 | 在同一元数据锁内循环重查已缓存映射、in-flight加载与orphan状态，再选frame |
| 关闭后访问已清空frame数组 | 普通构建未必崩溃，但标准库边界检查构建在4个关闭/重开控制的子进程中断言越界 | 在frame准入前检查实际fd关闭状态；关闭或打开失败返回NULL |
| 内部free/orphan哨兵被当作外部pageId | `UINT32_MAX`、`0xFFFFFFFE`实际进入缓存并触发pread，和内部frame身份冲突 | 两个内部保留值在任何缓存/磁盘操作前拒绝 |

重查也适用于orphan等待释放锁后的重新准入，不再解锁再次走快速查找而留下新间隙。
慢路径加载等待使用predicate，使已登记的condition_variable在虚假唤醒期间仍有效。
保持原先50次、每次200ms的orphan自持pin有界重试，不延长等待上限。
不改已有clock算法、页面校验、WAL/writeback barrier、页格式、公开类布局或Main。
内部哨兵本来就不是可表示的真实frame身份；没有承诺全部32位数值都可用作数据页。

## 完整实测

| 同一完整35项原生夹具 | 旧工作区 / 纯提交 | 修复后工作区 / 纯提交 |
| --- | --- | --- |
| 正常O2构建 | 各退出1，27失败 | 各退出0，零失败 |
| 仅BufferPool额外启用标准库边界检查 | 各退出1，31失败 | 各退出0，零失败 |

旧27个失败控制为24个cache-gap交错、1个实际PageAllocator调用及2个内部哨兵。
检查构建额外4个失败是关闭后越界：子进程实际abort，父驱动仍完整执行35项；
没有把普通release运行说成已观察到相同崩溃，也不把断言数说成独立缺陷数。
正常构建和检查构建各自使用对应的真实BufferPool对象，未冒称全库sanitizer。

POSIX信号量在真实公开fetchPage调用的第二次元数据锁获取前停住线程；peer先
缓存同页，再放行原线程。覆盖1/2/4帧、peer pin保留/释放、clean/dirty、正常/EIO
的完整24种组合，要求相同缓存指针和内容、精确pin计数、零额外pread和零额外写回
barrier。不是概率压力测试或性能耗时阈值；5秒仅为同步失败退出界限。
同一交错也通过真实PageAllocator接口执行；另外保留cold单loader/等待者、加载失败
后重试及另一个页不被阻塞的正向控制。关闭检查隔离在子进程，不删掉旧崩溃。

新原生测试按仓库原有`*_test.cpp`规则自动发现，无需附加链接包装参数；测试线程
通过dlsym取得实际libc的pthread_mutex_lock/pread，只在明确arm期间控制/计数，
其他调用原样转发。已在当前Linux/GNU配置实际正常链接与运行，不作跨平台完备声明。
最初临时probe缺少unistd头，编译失败日志保留；补头后的独立probe实际证明peer已有
有效缓存而旧fetch仍返回NULL。正式35项夹具在旧/新运行之间未变更任何断言。

每侧另有14个既有原生和2个完整协议入口全部退出0；加上正常/检查两次新35项，
总36次候选程序运行均通过。既有原生为buffer_pool_invalidate_all_pins、
buffer_pool_snapshot_quiescence、page_allocator_shared_extent、derived_map_owner/snapshot/
peer_coherence/close_retry、shared_heap_engine_owner/rows/index_rows、shared_heap_runtime_missing_file、
heap_wal_identity_codec、bptree_topology_corruption和savepoint_image_alias。
两个协议为savepoint_read_only和index_corruption，原断言与默认超时未改变。
本次内部缓存交错没有与PG内部实现作一对一差分，不能据此关闭P0-16或完整STO-04。

## 版本管理与未完成范围

每侧fresh编译普通/检查两种BufferPool、新原生及14个既有原生；其余匹配生产对象、
头文件、编译器和stub核对后复用并新链接。Root62生产对象/134头、纯提交60/131，
各用自己的布局；Root Main明确使用pre-R229匹配donor，不运行跳过草稿或默认旧
dbms_main。不是normal/repeat、全部fresh或完整工作区构建。

2271个提交源码输入与实际测试纯投影逐文件一致；949原生+597注册Python+3真实Main=
1549只是入口清单，不是全1549验收。独立index只提交本项三个路径，原18个暂存路径
及其他工作区内容保持不变。原273与冻结1492历史结果不改写；不push，Actions禁用。

BufferPool仍没有完整buffer content lock、ResourceOwner/RAII pin cleanup、分区hash、
prefetch、bulk/ring策略；本项不保证close时仍持有raw pointer的调用者生命周期。
FSM/VM crash rebuild、完整horizon/all-frozen、全部恢复窗口也未由本项证明。
所有明确暂缓保留，尤其R243、安全/TDE、全局DDL缓存锁和后台专项；没有借本项恢复它们。
最初尝试派发只读审查遇到agent thread limit，未重启旧的暂缓agent。

证据目录：`/tmp/dbms-r246-buffer-fetch.p9YhEFri`。

| 记录 | SHA256 |
| --- | --- |
| 独立旧probe | `842b8606e056a24f64e042ba4bc7380c9a0a66cc988541778ff8163a28ff89a6` |
| 完整旧35/检查35 | `ee95dc4c45a6904cf59e0e8e7a076bf777992ed1793ab3b15cbb182ca1688615` |
| 生产准入修改 | `f135c4efcd48383545f8df90e77ed832b0843b76447cbf28e0f612a9e3120bee` |
| 实际新编译与链接 | `38aee0c343b0b0d153d528cbe7ef616c9d119d2f2e4423c4d8e6abc1e4883bac` |
| 既有14原生新编译和链接 | `b5b35c8eab7c84813e5035fb0834e1856324eed276d918473fd22b13b3bfce94` |
| 完整36次候选运行 | `dddf1d9de8cb4372f08337e4fb95ad998ebaf2f3b1dcd740a982e0a676e93907` |

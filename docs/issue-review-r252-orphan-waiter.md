# R252：orphan等待登记的生命周期

状态：有限等待登记修复通过验证；STO-04/P0-14和原273总清单仍未整体完成。

## 问题与修改方法

| 问题 | 实测影响 | 修改方法 |
| --- | --- | --- |
| invalidateAll虚假唤醒后再次push同一栈cv，旧登记未清除 | 下一次vector增长分配失败时，等待调用已退出、cv已析构，最后一次unpin仍尝试通知其地址 | 每次wait只持有一个登记，普通/异常出口均删除自己的指针，再由调用方循环检查orphan状态 |
| timed fetch的登记清除只在wait_for普通返回后执行 | 源码上等待抛异常时会跳过清除；本项不声称实际复现了系统锁异常 | 统一使用同一异常安全辅助函数，保留原200ms等待与50次重试规则 |

仅在BufferPool.cpp内增加orphan等待辅助函数并用于两个位置。vector对象跨wait
仍存在，虽然unpin可能清空其内容；清理仅删除自己的cv指针，不清除其他等待者，
也不分配内存。异常继续原样重抛。R251的按页加载等待协议和R250失效准备阶段保留。
无头字段、对象布局、页格式、Main或全局锁变更。

## 完整旧新对照

独立两项probe先在两侧复现：普通对照通过，注入控制记录一次过期通知，整体退出1。
正式同一4项夹具在两侧旧实现均完整退出1、1项失败；新实现两侧4项零失败、退出0。
3项控制原本就通过，不称为新修复了4个旧错误：

| 控制 | 旧结果 | 修复后 |
| --- | --- | --- |
| invalidateAll等待原orphan pin自然释放 | 通过 | 通过 |
| 虚假唤醒后下一次分配失败，再释放原pin | bad_alloc退出后仍有一次向已析构cv的通知尝试 | 无过期通知；复用vector容量，不再需要该次增长分配，正常完成等待 |
| timed fetch等待原pin释放，无虚假唤醒 | 通过 | 通过 |
| timed fetch等待原pin释放，有一次虚假唤醒 | 通过 | 通过 |

修复后避免重复积累指针，也可能消除下一次分配。夹具接受合法的异常退出或已实际
登记的等待，不能要求未发生的分配必须抛bad_alloc；没有吞掉异常。信号量选择真实
返回/等待事件后才释放原pin，检查旧指针数据、后续读取值、精确读盘计数、pin释放
与snapshot quiescence。5秒/12秒仅是同步失败边界，不是性能阈值；旧四项无deadline。

夹具复用已提交R251原生文件的私有interposition helpers（重命名其main后include），
不在本项四项内运行或重复计算R251八项。额外转发pthread_cond_timedwait/clockwait，
只在指定线程制造一次允许的持锁虚假返回，并在实际timed wait入口报告登记事件。
新旧四项控制/期望不变，编译无新测试警告；所有文件均位于私有测试目录。

过期对象探针仅在被测调用返回、cv已完成析构后arm，observer线程栈保留至unpin
结束。探针记录实际broadcast尝试，但不向已析构cv转发危险调用，不把此结果称为
实际UAF、崩溃或ASan内存报告。其他锁/通知/读盘调用均转发真实函数。

## 检查与构建边界

本轮58次有限程序运行全部退出0：每侧新4项、20个既有原生、3个真实Main frontend
及5个完整协议。既有原生另行执行R251完整8项、R250的170项、R248的53项、R249的
15项、R247的24项、R246的35项普通构建，以及pin/snapshot/shared extent/derived
maps/shared heap/codec/BTree topology/savepoint alias。协议为savepoint_read_only、
index_corruption、explain_json_analyze、explain_json_cache、explain_timing。
保留原断言和默认超时，不将四项或58次运行当作全库存验收。

定向ASan/UBSan实际退出0、完整4项零失败。仅BufferPool.cpp和新测试TU（含复用
的R251 helpers）插桩，PageCrypto为匹配未插桩对象；未降低检查选项，不宣称全库
sanitizer或TSan通过，也不称为对真实系统等待异常的穷举。

两侧各fresh编译BufferPool.cpp、新测试和20个既有原生测试；其余使用各自匹配的
已验收对象，核对源码、134/131头、编译器及对象哈希，复用的R251夹具源码也保持
已提交哈希。最多两个CXX进程，Root62/纯提交60生产CPP不混用；非normal build或
本轮全122CPP fresh声明。6个Main对象复用R247，先重链接复现原二进制SHA再链接
当前库运行，核对原测试体/头，不链接test_stubs。Root Main草稿不动，仅使用记录
过的pre-R229投影对象，不恢复用户跳过项。

2277个提交源码输入与测试纯投影完全一致。955原生+597注册Python+3Main=1555
仅为库存。独立index只提交本项生产、测试、本文档3路径；原暂存18路径及其他工作区
字节保留。冻结1492结果和273历史状态不重写。完整ResourceOwner、raw pin生命周期、
全局缓存生命周期和其他STO-04/P0-14缺口仍开放；R244规划错误阶段仍开放。
R243、安全/TDE、后台/CTAS和所有其他明确暂缓保留。不push，Actions保持禁用。

证据目录：`/tmp/dbms-r252-orphan-waiter.qV2UwE77`。

## 证据SHA256

| 记录 | SHA256 |
| --- | --- |
| 独立两项orphan复现 | `77298d96c507e7dbbb1d288509a74f46891bade73c8af97d6a125b8f1c87277e` |
| 完整旧4项 | `6ac21fb9f39bef8596846dc2681b8148d382ccac77bd2b98fbe4940b35e4da83` |
| orphan等待登记生命周期修改 | `9f5c5cd1f61e6f25deadb23cba61a7d05787ac868c1bd99684c428e1e257108f` |
| 匹配对象及新生产CPP构建 | `4bb76667aa4a71ce503870ea4129f92ebac3a44e6d141e93cc7de6c4f77f3cbc` |
| 新4项双侧运行 | `2a55900d51ab564a65a0c5db3056f3505b1d031d0698759d4d20ad8cd1ec9067` |
| 定向ASan/UBSan完整4项 | `c326d149582016266263aa986157ef6ce3faf722f78e5ec33a5e1edb9a1b38ce` |
| 40原生fresh与6Main对象复现重链接 | `0a583397ccfbdc55dd88e5cc52552f3c51abef04761b60e1012cd6cee7f0cbee` |
| 完整58次候选运行 | `6b365bdd43a0aff25d878b9fe59339dcf14ed1b52cd60f652cb0ad3091afb806` |

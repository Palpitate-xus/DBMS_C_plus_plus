# R96：内置collation退出期生命周期（用户暂缓）

状态：用户明确要求“skip it, then continue”，本项未修复、未验收，不继续调查。
仅暂缓本次内置collation退出期生命周期问题，不扩大到全部collation/类型或
其它普通SQL正确性。generic continue不恢复本项，也不改名绕过跳过要求。

## 已有证据

R95完整native收尾有“collation does not exist: C”warning。一个外部、纯组件
驱动在main前构造EarlierOwner，在main中首次调用 `collation::isValid("C")`
并得到true，然后在EarlierOwner析构中再次调用同一函数。原
`src/catalog/collation.cpp:70–75` 使用函数内动态 `static const std::set`；
该set可先于EarlierOwner销毁。两翻译单元的ASan+UBSan编译实际退出0，运行
实际退出1：main控制为true，退出期报告heap-use-after-free，调用点为
`collation.cpp:75`，释放栈为set析构。

驱动没有链接StorageEngine/g_engine/ConnectionPool，没有数据库I/O或后台
线程。这证明该组件的特定退出顺序问题，**没有**证明R95 warning来自同一
运行时栈，不是全库sanitizer结果，也不是R15连接池shutdown wake-order调查。
没有生产修改、候选修复或已接受的永久回归，不能把本项登记为已修复。

原始本机辅助证据位于 `/tmp/dbms-review-r46.ifu6SWLp/`：
r96-builtin-collation-exit-probe.cpp、r96-builtin-collation-exit-build.log、
r96-builtin-collation-exit-asan.log及r96-builtin-collation-exit.probe.san。
这些临时路径不是可移植发行产物；没有在用户跳过后重新运行或继续修改该组件。

## 边界与后续

保留未解决状态。只有用户明确重新授权本项后才分析修复及验收，不凭本页
推定可恢复调查。本次新增暂缓独立于原273项总账中的15个deferred条目，
不改总账22 complete/166 partial/70 unverified/15 deferred_by_user的计数。
其它独立类型、查询和存储问题继续逐项修复、测试、独立本地commit；不push，
GitHub Actions保持禁用。

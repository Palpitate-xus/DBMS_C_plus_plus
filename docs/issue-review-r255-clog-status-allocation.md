# R255：CLOG状态登记分配失败的逐entry一致性

状态：登记顺序已修复；后续[R256](issue-review-r256-clog-segment-path.md)已修复下述2项路径错误。以下430/2是R255提交时的历史结果，不是当前版本结果。
TXN/WAL/P0-14及原273总清单仍未整体完成。

| 问题 | 影响 | 修改方法 |
| --- | --- | --- |
| setStatus/setStatuses先写seg.data，再分配pendingBits节点/buckets | bad_alloc后缓存暴露未登记状态；flush可能不写它，或把它覆盖回原值 | 先成功取得pending登记，再在同一mutex内更新缓存、掩码和dirty标记 |

分配失败继续原样抛bad_alloc，不改成成功、零值或吞异常。登记成功后只剩不会分配的
两位数据/掩码和dirty写入，其他线程不会观察锁内中间状态。两个入口均修改，没有
头、布局、页格式、Main、全局锁或TDE变化。批量入口仍允许已完成前缀保留为可刷盘
状态；不承诺整个batch全有或全无，也没有为了通过测试重定义批次原子性。

## 同一完整夹具的实际结果

`tests/clog_status_atomicity_test.cpp`在Root与独立纯提交投影旧实现均完整430项、
210项失败、退出1；登记顺序修复后均完整430项、2项失败、仍退出1。原控制与期望
全部保留，没有删除冷加载用例或把失败改成通过。

140个配置包括两API × 四个packed位置 × 四种原状态 × 四种新状态的128项完整
转换配置；另有两API各5种登记状态：已有bucket容量、同byte已登记、其他byte已脏、
13个pending触发bucket增长、未缓存冷加载；以及两种批量前缀。每配置先跑完整成功
路径并记录实际new分配，再逐次注入全部观察到的288个分配点，另有2项INVALID_XID。
因此430=140次无故障配置+288次分配故障+2次invalid正例，不是430个独立缺陷。

夹具使用thread-local new计数/注入，转发真实malloc/free和pthread_mutex_lock。
setStatuses进入flush时第二次取得同一mutex，夹具只在该边界关闭分配注入，实际
flush仍照常执行；本项不混入保存/IO阶段的独立锁所有权问题。冷加载配置额外覆盖
进入登记前实际发生的buffer、路径、缓存节点分配。分配数与尺寸实际记录，不硬编码
只测试第一个node。增长配置观察到24字节node和232字节bucket分配；已有同byte
登记时不分配，保留该正常更新正例。

检查失败entry保持原状态、邻近packed两位不变、磁盘完整字节不变、已经发布的状态
全部可刷盘并与新CommitLog读到的值一致。允许已经成功的batch前缀保留；之后实际
让peer持久化另一个状态，再重试原操作，验证合并不丢失peer。测试文件位于各次
mkdtemp私有目录，结束后清理；不读写用户数据库。

## R255提交时剩余2项：后续由R256修复

两个冷加载配置的第2次分配失败被segmentPath中的ostringstream吞掉，操作继续
使用错误路径。scalar还会暴露错误的其他缓存状态；batch在后续flush后碰巧恢复
数据，但分配异常已丢失。本问题不由pending登记顺序引起，独立作为R256继续修复：
改用不会隐藏分配异常的十六进制路径构造，保留所有冷加载检查并复查合法文件名。
上述R255历史430项不能说成零失败。当前R256保留全部140配置及所有实际分配点（旧288、新286，两个冷配置各少一次分配），另加14个段名控制，完整442项两侧零失败；仍不代表整个CLOG异常路径完成。

V1夹具最初缺少unistd.h，两个编译实际退出1；只补头文件后完整重跑，控制/期望
未改，原失败日志和V1夹具保留。

## 构建与回归边界

88次候选程序运行中，86个既有原生/Main/完整协议退出0，2个本项主程序各因上述
R256的2项断言失败而退出1。不是全88通过。每侧33个既有原生包含R254完整49项、
CLOG普通/事务集成、回滚/恢复图、失败重试、跨进程XID、redo crash、R246至R253
BufferPool和shared heap/derived maps等。7个协议为savepoint_read_only、
index_corruption、explain_json_analyze、explain_json_cache、explain_timing、
commit_failure_recovery、transaction_begin_error_state，均保留原断言与默认超时。

定向ASan/UBSan插桩CommitLog.cpp与新测试TU，完整430项同样仅2项R256断言失败、
实际退出1，没有ASan/UBSan诊断。未弱化选项，但这不是sanitizer测试全绿或全库验收。
新生产TU及新夹具编译无警告。

每侧fresh编译CommitLog.cpp、新夹具和33个既有原生测试，其他生产逐项核对源码、
134/131头、编译器与对象哈希后复用。Root62/纯提交60生产CPP严格分开。6个真实Main
frontend复用R247对象，先重链接还原父二进制SHA再链接新库运行，不链接test_stubs；
不称为六次fresh Main或全122CPP fresh/normal build。Root Main保持pre-R229投影，
没有恢复被跳过草稿或复用R244新布局对象。

2282个提交源码输入与测试纯投影一致。958原生+597注册Python+3Main=1558只是库存。
独立index只提交本项生产、夹具、本文档3路径，原18路径暂存及其他工作区字节保持
不变。冻结1492结果、原273要求和历史状态不重写。R244、R243、安全/TDE、后台/CTAS
及全部其他明确暂缓保持暂缓。不push，Actions禁用。

证据目录：`/tmp/dbms-r255-clog-status-atomicity.yHVG9EzR`。

## 证据SHA256

| 记录 | SHA256 |
| --- | --- |
| 完整旧430项 | `97cce9d460ee08656e7592c4c870cee89dc67de86dba749c33f86f7fc84effd4` |
| CLOG两个状态入口的登记顺序 | `f257e86a847c353a8fbbf3727b93b9df0bcfe973931c1473fd80bee588355791` |
| 匹配对象及新生产CPP构建 | `7f3e228a48f9d4895e27aa2d86bb2a1a68697287e4ab2936040991d3d46f5cdf` |
| 候选430项仍各2项R256失败 | `fc05e4536024f2b94c0a5ff04c39d168428af5c3bc7b4f0bb41bac767be8d8b8` |
| 定向插桩430项仍2项R256断言失败 | `da1ac5c3fa3dd17e3232292d0eea5528df9f6f2157d835fd79fd18c4ea6ccf7b` |
| 66原生fresh与6Main对象复现重链接 | `3549ea9cc8812a2758f5801be72934525e589e213e80e2326269bf9026f28ecb` |
| 88次候选运行其中2个程序保持R256失败 | `910e37cf0c29d30fd865b2c856e793802b0f5dbcbe16dd179b7191a2d1cb5610` |

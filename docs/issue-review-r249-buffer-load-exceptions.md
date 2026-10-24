# R249：页缓存加载异常的预留资源清理

状态：有限异常清理修复通过验证；STO-04/P0-14和原273总清单仍未整体完成。

## 问题与修改方法

| 问题 | 使用影响 | 修改方法 |
| --- | --- | --- |
| frame已预留，loading映射分配失败 | 未登记的frame仍被pin，重试或其他页无法复用 | 把登记过程纳入异常回滚范围，清除未发布frame的预留状态 |
| scratch、读取适配器、validator或发布映射抛异常 | loading标记和pin遗留，fetch/close可能永久等待，等待者得不到通知 | 对登记到发布的整个私有加载区间捕获异常，必要时先重新取得pool锁，调用既有failLoad清理并通知等待者，再原样重抛 |

只修改BufferPool.cpp，不新增头字段、改变持久化格式或修改Main。成功发布之前的
frame属于加载器，不属于调用者；成功路径和普通失败返回继续使用原有行为。没有
吞掉异常、把异常变成nullptr或改变SQLSTATE。普通缓存计数问题R248仍开放，未在
本项混入计数策略修改，也未被用户暂缓。

## 完整旧新对照

同一原生夹具先在两侧旧实现运行：均15项、12项失败、进程退出1。12项中，2项
分配失败很快返回但遗留frame，另外10项由子进程deadline完整记录为退出91；不是
12项都死等。普通EIO返回、validator返回false和正常加载这3项原来就通过。
修复后两侧均完整15项、零失败、退出0，控制和期望未删改。

夹具先记录成功cold load实际发生的每个分配，再逐一注入bad_alloc。本轮观察到
5次分配：加载映射2次、scratch1次、发布映射2次；不是硬编码只挑某个失败点。
还检查validator的runtime_error、保留SQLSTATE/message的DbError和非标准异常；
读取适配器的runtime_error/DbError；实际已登记等待者的读取/校验失败交错。
读取抛异常通过测试包装器合成，并非声称操作系统pread会抛C++异常。

pthread_cond_wait探针在等待者仍持pool锁时报告登记位置，然后转发真实等待，
加载器必须在等待者释放锁后完成清理并唤醒。所有负例检查原异常、没有暴露的
预留frame、同页重试、异页复用、无残余pin及close/reopen后的原数据。3秒/8秒
deadline仅使旧死等可收集，非性能阈值；数据文件均在私有测试目录。

## 检查与构建边界

定向ASan/UBSan实际退出0，完整15项、零失败。仅BufferPool.cpp和新测试被插桩，
PageCrypto使用匹配的未插桩对象；没有降低检查选项，不宣称全库sanitizer或TSan通过。

本轮50次有限运行全部退出0：每侧新15项、16个既有原生、3个真实Main frontend及
5个完整协议。原生包含R246完整35项普通构建、R247统计24项及pin/snapshot/shared
extent/derived maps/shared heap/codec/BTree topology/savepoint alias回归。协议为
savepoint_read_only、index_corruption、explain_json_analyze、explain_json_cache和
explain_timing。原断言和默认超时保留，没有过滤失败。

头文件未改变，两侧各fresh编译BufferPool.cpp、新测试及16个既有原生测试；其余
生产对象来自R247的全量fresh构建，逐项核对各自源码、134/131头、编译器及对象哈希。
Root62个/纯提交60个生产CPP不混用。并非本轮重编全部122个CPP或执行normal build。
Root Main仍使用已记录pre-R229临时源码的匹配对象，工作区被跳过的Main草稿不动。

6个真实Main frontend对象复用R247匹配对象：先用父库重链接，所得二进制必须与
原已测试二进制SHA256完全相等，再链接本项新库并运行。检查当前头和原测试体哈希，
不链接test_stubs；不是本轮fresh编译6个Main测试，也不是恢复R229。

2273个提交源码输入与测试纯投影一致。951原生+597注册Python+3Main=1551只是
入口库存，不是全1551验收。独立index仅提交本项生产、测试、本文档3路径；原暂存
18路径和其他工作区字节保持不变。原冻结1492结果和273历史状态不重写。

本项不解决完整ResourceOwner/RAII pin生命周期、所有OOM/等待者注册异常、content
locks、全局缓存生命周期或其他STO-04缺口。R244规划错误阶段、R248计数仍开放；
R243、安全/TDE、后台/CTAS和全部其他明确暂缓保留。不push，Actions保持禁用。

证据目录：`/tmp/dbms-r249-buffer-load-cleanup.Q9uLG2wA`。

## 证据SHA256

| 记录 | SHA256 |
| --- | --- |
| 完整旧15项 | `8534250bdbd77c3533c5454f562255e2881ae77b497d786a09855df3b1dad647` |
| 有限异常清理修改 | `760ac5bf76cfd5dc237bf68238928040f33ebe53071d80f54fa57f4bfad5e6bb` |
| 匹配对象及新生产CPP构建 | `80ac88c132be56a19e6a565d35a2398de5ffafd4dd0a3b504a9435b93765045d` |
| 新15项双侧运行 | `26895412db73fbf590b687a287669800413698f86f82593b77863a378e3a19f8` |
| 定向ASan/UBSan完整15项 | `3e03f78c43b67142510c5fa349c7a95e47663ef8df5e641a24483116624cafe8` |
| 32原生fresh与6Main对象复现重链接 | `9c3b2366ea670982debbb51d26266858374a86b154e7b2ebe9bfabbf19932e3f` |
| 完整50次候选运行 | `bfec5de8d6eac38f4b94b819e840b85d1c26101904d6c7a531712510ae2d42cd` |

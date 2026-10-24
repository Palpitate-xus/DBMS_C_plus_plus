# R250：缓存失效操作的分配异常原子性

状态：有限异常安全修复通过验证；STO-04/P0-14和原273总清单仍未整体完成。

## 问题与修改方法

| 问题 | 实测影响 | 修改方法 |
| --- | --- | --- |
| invalidatePage先删除pageMap、清除dirty，再分配orphan记录 | bad_alloc后原pin找不到释放路由，缓存重取可能读盘而丢掉内存修改，flush也无法保留原脏页修改 | 对仍被pin的frame先准备orphan记录，只有成功后才删除映射、改变frame |
| invalidateAll逐页分配orphan记录时已改变部分frame | 中途失败可留下部分orphan与旧pageMap同时拥有frame，原unpin被旧映射截获；dirty和usage也已部分清除 | 在同一pool锁内、已有加载及orphan排空后，先准备完整新orphan映射；异常时清空仅属于本次准备的记录并原样重抛；全部成功后才进行不分配内存的frame转换 |

准备阶段不增加新容器、头字段或存储格式。invalidateAll保留既有orphan map的容量；
失败后bucket容量可能不同，但页映射、frame元数据、dirty、字节和调用者pin保持原值。
清理使用平凡记录的clear，不再次分配内存。单页操作只预备目标页，不触碰其他orphan。
成功失效仍可主动丢弃dirty缓存，这是原有操作语义；修复只防止失败操作提前丢弃它。

没有改变加载/已有orphan的等待策略、成功失效后的pin路由、缓存统计、返回值或异常
类型。R249加载清理和R248计数修复保留。本项只修改BufferPool.cpp，不改Main或头。

## 完整旧新对照

同一夹具在两侧旧实现均完整170项、116项失败、退出1；修复后两侧170项零失败，
退出0。116为故障控制数，不是116个独立缺陷；54项普通控制原本就通过。

| 控制 | 数量 | 内容 |
| --- | --- | --- |
| 成功操作的完整分配trace | 48 | 1/2/4页、1/3次pin、clean/dirty、全pin/混合未pin页、单页/全部失效 |
| 对每个trace实际观察到的全部分配逐次注入bad_alloc | 116 | 包含节点、bucket及前面记录已准备后的后续节点分配，不只检查第一个失败点 |
| 无pin正常失效 | 4 | 单页/全部、clean/dirty，在首分配即失败的探针下仍不分配且成功 |
| 目标不存在 | 2 | 已打开/未打开，无分配、不改变缓存 |

每个故障控制检查frame元数据完全不变、原缓存指针和值、warm fetch不读盘且精确
命中、原pin可释放、flush成功后真实磁盘字节、snapshot quiescence及其他页可复用。
旧版58个带dirty的故障控制在flush后，实际磁盘未保留期望内存修改；不是仅凭源码
推断数据影响。另一些旧控制遗留pin/orphan，不能据此称116项都发生同样的数据丢失。

分配探针仅在被测调用期间arm，按成功操作实际观察的序号穷举；低层IO在未arm时
原样转发。新旧控制和期望未改，编译无新增测试警告。数据文件均在私有测试目录，
没有修改用户数据库。没有用超时、删控制或吞掉bad_alloc来通过测试。

## 检查与构建边界

本轮54次有限程序运行全部退出0：每侧新170项、18个既有原生、3个真实Main
frontend及5个完整协议。原生包含R248计数53项、R249异常15项、R247统计24项、
R246准入35项普通构建，以及pin/snapshot/shared extent/derived maps/shared heap/
codec/BTree topology/savepoint alias。协议为savepoint_read_only、index_corruption、
explain_json_analyze、explain_json_cache、explain_timing，原断言和默认超时保留。

定向ASan/UBSan实际退出0、完整170项零失败。仅BufferPool.cpp和新测试插桩，
PageCrypto为匹配未插桩对象；未降低检查选项，不宣称全库sanitizer或TSan通过。

两侧各fresh编译BufferPool.cpp、新测试和18个既有原生测试，其余生产对象使用各自
匹配的已验收R248/R247对象，核对源码、134/131头、编译器及对象哈希。最多两个
CXX进程，Root62/纯提交60生产CPP不混用；非normal build或本轮全122CPP fresh声明。
6个真实Main测试对象复用R247：先以父库重链接复现原二进制SHA，再链接当前库
运行，核对原测试体与头哈希，不链接test_stubs。Root工作区Main草稿保持原字节，
只使用记录过的pre-R229投影对象，不恢复被跳过的模块。

2275个提交源码输入与测试纯投影完全一致。953原生+597注册Python+3Main=1553
仅为库存，不是全1553验收。独立index仅提交本项生产、测试、本文档3路径；原暂存
18路径及其他工作区字节保留。冻结1492结果和273历史状态不重写。

此证据覆盖等待完成之后的orphan登记与frame转换，不覆盖所有等待者注册/系统锁
异常、raw pointer生命周期、完整ResourceOwner或全部OOM行为。STO-04/P0-14仍开放，
R244规划错误阶段仍开放；R243、安全/TDE、后台/CTAS和全部其他明确暂缓保留。
不push，GitHub Actions保持禁用。

证据目录：`/tmp/dbms-r250-invalidate-atomicity.dUXtnKgs`。

## 证据SHA256

| 记录 | SHA256 |
| --- | --- |
| 完整旧170项 | `ca75b0edb9cfe898662cfb24e039c2ad5f835342c6adbb479edc29d9f1caa54f` |
| 有限失效异常准备修改 | `09163b73d77e2a99174ad76a43058bb58b04e1a10fe19118c648762b92cef901` |
| 匹配对象及新生产CPP构建 | `6a526b50a8bdfac796e6d8f2151a4150975a3bfd3894f8a553e1cc9ecde1a464` |
| 新170项双侧运行 | `04590d7b4fe2fe57bf91986a6d73c091a294d4289f981438f98af66992b1e04b` |
| 定向ASan/UBSan完整170项 | `7930e050b0ec692d2156486221a4566559b4624a736ea4e038ab7910da60bde2` |
| 36原生fresh与6Main对象复现重链接 | `3f0aa66bd05f5189cfbfaedd3b491bce3926f0d8040c40bb0e9999b145c59968` |
| 完整54次候选运行 | `bbcad506d9bc7aabf4331d469b993c411929e5355a7392ed6c97e72bf53efe13` |

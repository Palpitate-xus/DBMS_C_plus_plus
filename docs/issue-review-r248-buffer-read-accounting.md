# R248：缓存命中与实际读盘尝试的计数

状态：有限计数修复通过验证；STO-04/P0-16和原273总清单仍未整体完成。

## 问题与修改方法

| 问题 | 使用影响 | 修改方法 |
| --- | --- | --- |
| slow-path入口立即增加misses | 已关闭、无可用frame、writeback拒绝或读取前分配失败都虚增shared_read | 仅在预留和scratch分配完成、即将发起页面读取时增加misses，仍在pool锁内记账 |
| peer在锁间隙或等待期间已缓存该页，重查直接返回却不记hit | 实际一读一复用被记成hits=0/misses=2，影响缓存命中率和EXPLAIN统计 | 重查复用缓存时增加hit，不增加read |

misses是本项目历史API名称，StorageEngine/EXPLAIN把它消费为shared_read；本项
将其限定为页面加载读取的尝试数，不是所有fast-path查找未命中次数，也不是仅成功
读取次数。已经开始读取但返回EIO、validator拒绝、回调异常或发布失败，仍计一次read。
一页TDE sidecar的低层调用不是另一页shared_read；本项没有修改或恢复TDE专项。

[PostgreSQL18 EXPLAIN文档](https://www.postgresql.org/docs/18/sql-explain.html)把hit解释为因数据已在缓存而避免读取。
[pg_stat_statements文档](https://www.postgresql.org/docs/18/pgstatstatements.html)分别定义shared_blks_hit和shared_blks_read。
本机PG18.6 `src/backend/storage/buffer/bufmgr.c` 的ReadBuffersCanStartIO路径对peer
已完成加载的backend记hit，真正启动IO的backend记read；shared_blks_read在启动
IO时增加，并非仅在读取成功后增加。per-relation pgstat_count_buffer_read包含请求，
与EXPLAIN的shared_blks_read不是同一个计数，不能混用来推导期望。

本项不改变页面、异常、pin、等待者、淘汰或返回值策略；R249异常清理修复完整保留。
只改BufferPool.cpp的两个记账位置，无头文件、存储格式或Main改动。reset仍只重置
累计统计，不清缓存，也没有新增counter epoch/回绕策略或全局跨pool快照保证。

## 完整旧新对照

独立初始probe在单帧受控锁间隙中实际退出1，观察到hits=0/misses=2、预期1/1，
文件保存在`/tmp/dbms-r248-buffer-accounting.1Q2nCET8`。
随后同一完整53项夹具在两侧旧实现均退出1，38项失败；修复后两侧53项零失败，
退出0。38是控制数量，不是38个独立缺陷：

| 控制 | 数量 | 旧结果 | 修复后 |
| --- | --- | --- | --- |
| 1/2/4帧、保留pin、脏页、worker IO拒绝开关的peer缓存交错 | 24 | hit缺失/read多计 | 全通过 |
| 未打开、打开失败、close各变体及重开 | 6 | 未读取仍多记read | 全通过 |
| 全pin、barrier拒绝、writeback失败、结构页不可淘汰 | 4 | 未读取仍多记read | 全通过 |
| slow-path实际登记等待者随后复用peer成功加载 | 1 | hit缺失/read多计 | 通过 |
| 读取前映射/scratch分配故障 | 3 | 未读取仍多记read | 全通过 |
| 其余正例、实际IO错误、其他等待者及读取后发布分配故障 | 15 | 原本通过 | 继续通过 |

IO探针统计实际调用，锁探针只暂停指定线程在同一pool第二次加锁之前；信号量决定
交错，不是概率压力。等待者在真实pthread_cond_wait登记后才放行loader，分别覆盖
fast/slow路径及失败后重读。所有低层调用在未arm时原样转发。5秒仅为同步失败界限。
抛异常pread为测试适配器合成，不声称操作系统会抛C++异常。

夹具观察到成功cold load的5个分配点，逐个故障注入，并根据实际读取调用检查
0或1次read；读取后发布失败保留read，读取前故障不计read。检查页面值、指针
复用、pin数量、原错误、重试和重开，不是只核对数字。初轮有5处排版警告，随后
仅换行消除；去除空白后的源字符完全相等，控制/期望不变，两侧重新跑完整旧53项
仍为38失败。旧日志保留，以V2夹具作最终旧新对照。

## 验证范围与版本管理

本轮52次有限运行全部退出0：每侧新53项、17个既有原生、3个真实Main frontend
和5个完整协议。原生包含R249完整15项、R247统计24项、R246准入35项普通构建，
以及pin/snapshot/shared extent/derived maps/shared heap/codec/BTree topology/
savepoint alias。协议为savepoint_read_only、index_corruption、explain_json_analyze、
explain_json_cache、explain_timing。保留原断言和默认超时，没有过滤失败。

定向ASan/UBSan实际退出0，完整53项零失败。仅BufferPool.cpp和新测试插桩，
PageCrypto为匹配未插桩对象；未降低检查选项，不宣称全库sanitizer或TSan通过。

本轮fresh编译两侧BufferPool.cpp、新测试和34个既有原生测试；其余各自匹配的
生产对象来自已验收R249/R247，校验源码、134/131头、编译器及对象哈希。最多两个
CXX进程，Root62/纯提交60生产CPP不混用。非normal build或本轮全122CPP fresh声明。
6个真实Main测试对象复用R247：先以父库重链接复现原二进制SHA，再链接当前库
运行，核对原测试体与头哈希，不链接test_stubs。Root工作区Main草稿不动，仅使用
记录过的pre-R229投影对象，不恢复被跳过的模块。

2274个提交源码输入与测试纯投影完全一致。952原生+597注册Python+3Main=1552
仅为库存，不是全1552验收。独立index只提交本项生产、测试、本文档3路径，原暂存
18路径及其他工作区字节保持不变。冻结1492结果和273历史状态不重写。

R244规划错误阶段及其他原清单缺口仍开放；R243、安全/TDE、后台/CTAS和全部其他
明确暂缓保留。本项未恢复用户跳过项，不push，GitHub Actions保持禁用。

证据目录：`/tmp/dbms-r248-accounting-fix.dMc7DqgT`。

## 证据SHA256

| 记录 | SHA256 |
| --- | --- |
| 完整旧V2的53项 | `759b3c3c0d2a92ee801f045b73097bd998a11eb364f7cdc545c91c4642bda509` |
| 有限计数位置修改 | `3e30448013d210c243e6f96a63dbef67ce78113b96e1fa52fd1b3ae77eca980c` |
| 匹配对象及新生产CPP构建 | `8d9ec37580d8b9c9012a57971e84130d60f24c9164aabb6049a48bb634686655` |
| 新53项双侧运行 | `d8d2705dcec9e999faebbe67acf2a0685384e816046ce3ea00dbfcf14cef3542` |
| 定向ASan/UBSan完整53项 | `2039170c872711eb53a7bee76cecbf3d4a49b12f87c8653c6d597e66f9950ac4` |
| 34原生fresh与6Main对象复现重链接 | `b5a9c002dacea1040dc7169af5c15d8e75b18fa76a22cbbb72444a779485d22b` |
| 完整52次候选运行 | `9bc83e7656bc19b1f8a39e0f85167916aed2d716e7a2210a1617977caead18e8` |

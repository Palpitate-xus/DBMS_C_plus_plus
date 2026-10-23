# R247：页缓存统计的同步与配对快照

状态：有限统计修复验收；STO-04和原273总清单仍未整体完成。

## 问题与修改方法

| 问题 | 证据与影响 | 修改方法 |
| --- | --- | --- |
| hits/misses/hitRate/resetStats访问普通字段但未持pool锁 | fetch在锁内写，统计读/重置没有同步，存在C++数据竞争；8个受控交错验证既有四接口在锁被占用时仍直接完成 | 四接口使用已有pool元数据锁，计数对在同一临界区复制或重置 |
| 先读hits再读misses不是同一时刻的统计 | 并发fetch/reset可在两个getter之间改变状态；原接口没有配对快照能力 | 增加按值的Statistics；BTree保留tree→pool锁顺序，一次取得配对值，现有聚合消费者使用该快照 |
| 命中率分母先做size_t加法 | 源码上存在加法回绕风险；新值快照用合成SIZE_MAX边界验证，不宣称曾真实执行SIZE_MAX次fetch | 先转double再求和，快照两计数均为最大值时命中率仍为50% |

原有单独getter继续保留，但跨两次调用不能保证配对一致；需要一致计数对的调用者
使用statistics()/cacheStatistics()。重置不清除缓存页，关闭后仍可读取或重置累计值。
没有添加持久化字段、修改对象存储布局、页格式、counter epoch或wraparound策略。
聚合的容器遍历、生命周期和跨pool全局一致性不由本项保证；没有修改或恢复R51
全局DDL缓存锁流程。计数跨pool累计超过size_t的策略也未由本项重新定义。

## 完整实测与检测范围

同一24项原生夹具在工作区/纯提交旧实现各退出1，17项失败；修复后两侧均24项
零失败。17是断言/控制数，不是17个独立缺陷：8项针对既有四接口缺锁，另9项针对
缺少新增快照接口（含2个快照锁控制）。旧6个基本统计控制和1个已有engine消费者
控制本来就通过，不能称作新修复的旧错值。

writeback callback在真实pool锁内等待；另一线程执行统计操作，libc锁转发探针
报告其尝试取得哪一把锁，信号量决定交错。要求同一pool统计等待该临界区，而另一
pool不被阻塞；不是概率压力或性能时间阈值。5秒只是同步失败退出界限。探针仅在
测试线程arm时记录，其他libc调用原样转发，不改变生产锁或调用规则。

检查另含4000个并发热命中的精确计数、读取单调快照、快照副本跨reset保持值、
最大计数分母、BTree关闭/已打开状态及实际StorageEngine消费者。首轮原生编译因
遗漏PageAllocator头失败；补头和两处警告排版后，24项控制/期望保持不变，旧日志保留。

ThreadSanitizer仅对BufferPool和独立hits读写probe作定向编译；其他库未全量插桩。默认PIE运行实际退出66，启动时unexpected memory mapping，未进入测试主体，不是数据竞争报告或通过。 显式-no-pie仅链接对照也在启动映射检查处失败；没有改变主机ASLR或把未执行的检查计为通过。

本轮共48次有限运行全部通过：每侧新24项、15个既有原生（含R246完整35项普通
构建）、3个真实Main frontend及5个完整协议。既有存储入口包括原pin/snapshot/
shared extent、derived maps、shared heap、codec、BTree topology和savepoint alias；
协议为savepoint_read_only、index_corruption、explain_json_analyze、explain_json_cache、
explain_timing。原断言和默认超时保留，没有为了通过删掉控制或过滤失败。
R246的checked35变体没有在本轮另行运行，不将普通35项代替该诊断变体的当前证明。
内部统计快照没有与PG内部缓存实现作一对一差分，不能关闭完整P0-16。

## 构建、版本管理与仍开放范围

共享头定义改变后，两侧全部122个生产CPP（Root62、纯提交60）和2个test stub均
以各自当前头文件/编译器/原O2配置fresh编译，最多两个CXX进程；不复用旧头对象。
Root134头/纯提交131头分别核验。Root工作区Main草稿保持不动，只把记录过的R229
变换逆投影到临时文件，确认哈希与既有pre-R229源码完全一致，再用新头fresh编译。
这不是接受/恢复R229，也不是normal build.sh或repeat构建声明。

三类Main frontend从HEAD原测试体取出，仅将Main include路径指向各自已验证源码；
Root未使用工作区被修改的session测试体。explain/session沿原脚本O0、window沿O2，
均fresh编译并链接自己的新生产库，不链接test_stubs。实际Main程序入口没有被调用。

2272个提交源码输入与实际测试纯投影一致；950原生+597注册Python+3Main=1550
只是入口库存，不是全1550验收。独立index只提交本项6路径，原暂存18路径及其他
工作区内容保持不变。原273和冻结1492结果不重写，不push，GitHub Actions保持禁用。

STO-04仍缺完整content locks、ResourceOwner/RAII pin cleanup、分区hash、prefetch/
bulk/ring等；其他接口和close期间raw pointer生命周期也不能据24项测试宣称全闭合。
R243、安全/TDE、后台/CTAS和全部其他明确暂缓保留，R244规划错误阶段仍开放。

证据目录：`/tmp/dbms-r247-buffer-stats.Yy2KWeZa`。

## 证据SHA256

| 记录 | SHA256 |
| --- | --- |
| 夹具补头，不改24控制 | `e27569ef378a605dff74750801e3bb2deb622a234642ea9a35d45c2e725d8c6d` |
| 完整旧24 | `2982d0da8d338f1c0f6ed19c5a3d97c9dfc1a0bc8a9b4a990e47811e30644bf6` |
| PIE线程检查实际启动失败 | `89707b35bcdd63825f4ce3dcc18030180b8ee8cf661d9a5fcd6c694874e3c138` |
| 非PIE链接诊断 | `c3350a13e52ea3e71793756956cc25a5168d9c52237ff45ac702c0c436619080` |
| 有限生产修改 | `fafd9a307c47d35f2fcd8a607b09d04ba2b701f2063748451dafd574f86c03e9` |
| 122生产CPP与2stub fresh构建 | `04ebdbaeda2ea6b275061a60e73d9333d3e4fa2edbd5c43cf105c6f66c632870` |
| 30原生与6真实Main fresh构建 | `768f2964b3124d82affd1f671d4d3c2de4d5c9463fcbde37054c30d5fc2c0354` |
| 完整48次候选运行 | `2e0d9272c50d9a1f0ed7e5bf97484570a3c66e370272c1a27ed809b1f5271429` |

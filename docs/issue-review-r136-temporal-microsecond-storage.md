# R136：标量时间类型的小数秒不能写入普通表

状态：本项有限修复完成。Root 59个不同完整入口、干净候选6个完整入口均实际0，
独立证据核对0。不是原273全部完成，也不是新1442入口的全量回归。

## 发现与修改方法

| 问题 | 影响与修改 |
| --- | --- |
| 合法小数秒写入报22023 | 旧TIME四字节/时间戳八字节存秒，检查和编码都不能保存fraction。新SQL标量TIME/TIMESTAMP/TIMESTAMPTZ显式采用TemporalMicros；旧native工厂及schema9/A/B/C仍为Legacy |
| 单位无法由宽度辨认 | Column增加强类型codec标记；schema-D（0x4442000D）写必要DFT1/RLB1/RID1/DSO1/ITM1及TCM1逐列标签，读时验证类型、固定宽度、标签、数量、必要扩展和严格EOF；不改heap v2，也不静默把旧八字节秒解释为微秒 |
| epoch0、负值、NULL和precision | timestamp以2000年为epoch的int64微秒，0是有效值，负值格式化向下拆秒；NULL与正负infinity哨兵分开。TIME用int64微秒，含24:00:00。输入先真实runtime datum CAST，后按实际列precision舍入；timestamp按PG epoch正负对称舍入 |
| 多路径丢精度或错时区 | 两种行布局、INSERT/UPDATE、读回、比较、排序、索引、约束及返回行统一使用列codec。TSTZ内部规范值保留UTC后缀，防触发器/最终校验二次应用时区；CLI优先用typed cells，不拆坏带空格兄弟列 |
| 微秒主键误重复、外键误接受 | 实际B+Tree键宽为20字节，不是32；ISO时间尾部fraction被截断。仅对Micro列将int64符号位偏置后编码为16位保序hex键，不改逻辑显示或全局索引磁盘格式；写入、lookup、PK/FK/unique共享同一值编码，查询不按列precision重新舍入 |
| 同类型ALTER的单位转换 | 即使timestamp宽度相同也要解码旧行后重写；显式复制目标codec。旧TSTZ解码的无后缀UTC在同类型转换时补UTC标记，非UTC会话也保持时刻。新继承子表使用自己的微秒heap，旧父表不改单位 |
| 新D的截断误接收 | 初始接入漏掉四个旧可选扩展EOF分支的D要求；加强夹具实际检出四处prefix截断，再补齐必需检查。保留失败日志，不删除控制 |

新增[TemporalStorageCodec.h](../src/common/TemporalStorageCodec.h)只消费真实CAST之后的
canonical datum，不是第二套SQL输入解析器；没有插值SQL、浮点换算微秒或把非法值
悄悄变为0。Legacy路径保留原物理单位，新列单位不能通过用户可变storageParams冒充。

## 永久测试及实际结果

| 入口/阶段 | 结果与范围 |
| --- | --- |
| [temporal_storage_codec_test](../tests/temporal_storage_codec_test.cpp) | 最终916控制全过；负epoch、精度ties/幂等、NULL/infinity、24h、基本civil边界。早期914/916额外纯header O2也通过，但它们不替代最终正常链接native入口 |
| [temporal_storage_layout_test](../tests/temporal_storage_layout_test.cpp) | 92控制全过；9/A/B/C parser及真实Legacy payload解码、实际A与D cold读、D损坏/截断拒绝、混合布局、旧列ALTER和继承；最终补强非UTC旧TSTZ ALTER |
| [temporal_column_precision_test](../tests/temporal_column_precision_test.cpp) | 782控制全过；不删除旧检查，严格更新为scalar-D、array-C、无precision的scalar仍D，并加强与省略precision列的codec一致性 |
| [temporal_heap_microseconds_protocol_e2e_test](../tests/temporal_heap_microseconds_protocol_e2e_test.py) | 实际PG18.6/180006最终412全过；本项目408全过。三类型×固定/混合布局、三协议模式、精确text/binary值/元数据/tag/Ready、排序/索引/REINDEX、ALTER、rollback、NULL/infinity/epoch0/24h/precision/PK/FK/时区及真实本地冷重启 |
| [temporal_heap_microseconds_cli_e2e_test](../tests/temporal_heap_microseconds_cli_e2e_test.py) | 4控制全过，实际独立进程读回UTC/Kathmandu/NewYork下的微秒、负epoch、24h和带空格列。复用既有CLI夹具，仅验时间数据，不作为认证证据 |
| Root正常构建 | 公共头变化后全部58生产TU fresh O2/repeat0；必要EOF检查和键/UTC转换各再正常重编1个私有TableManage/repeat0，全部实际0，未降生产优化 |
| 干净候选 | 仅cb5d3fd8已接受内容加本项hunk及测试，独立fresh58 O2和两次私有TM1/repeat0；新2native+2Python及原R135/R133两个协议，共6个完整入口全0，不借旧ABI donor或原未接受候选 |
| Root最终门禁 | 28native+28Python+3真实Main，共59个有序、唯一、完整入口全0；包括原134/266时间协议邻居。每个实际退出码独立保存，不用fail-fast造成漏测后冒全过 |
| Main来源 | 本轮新Column头后，explain/session测试TU fresh O0、window fresh O2，生产Main O2。两次私有TM/布局夹具调整没有改变Main、三个前端源或全部头/flags；逐字节receipt核对后再链接当前57 O2库，无stubs，并实际隔离执行三入口；不是R132对象复用 |
| 独立复核 | 59名字/顺序/次数、clean6、旧新矩阵、初始58及两次1CPP/repeat0、两份source封印、58当前receipt、双cache、57相同库字节、Main对象/新链接产物均实际0 |

9/B/C的合成镜像验证parser和既有payload单位，不能把缺RID1的9镜像配现代WAL当成
真实旧数据库恢复。早期夹具的RID1/WAL配对、B域名长度及跨owner合成文件发布问题
导致的中止日志保留；改为单owner操作parser镜像后，真实A/D另开cold owner。
不据夹具调整宣告修复catalog ownership。独立Root/clean真实server的六种新SQL
布局确实关闭后重启原数据目录；reference只新开backend，不重启共享PostgreSQL。

## 失败证据未覆盖

冻结R135旧程序完整408控制、281失败、实际1；不是281个独立根因。
PG初版412中3项因bytes diagnostic键的检查器错误误报，修正后按相同SQL/值/SQLSTATE
重跑最终412全过，strict计数版也全过。旧程序strict408/281也重新实际跑完。

原92夹具检出四处D截断，补完后相同截断控制通过；最终92还补强非UTC ALTER。
首次新Root整批59完整保留57实际通过/2失败、终态1：旧782中一个过时的scalar-C
断言，以及408中两个实际PK/FK碰撞。修正并加强断言/键编码后重新跑完整59全0，
没有覆盖第一轮日志、改期望数值、放宽SQLSTATE、跳过这两个完整入口或伪造缓存。

## 证据与产物

日志目录为`/tmp/dbms-review-r46.ifu6SWLp/`：
`r136-reference-initial.log`、`r136-reference412-strict.log`、`r136-old408-strict.log`、
`r136-layout-valid-images-before-tail-fix.log`、
`r136-normal-build.log`、`r136-clean-normal-build.log`、
`r136-main-object-compile.log`、两份`*-followup-*`及`*-keyfix-*`正常构建日志、
`r136-final-gate.log`（首59/终态1）、
`r136-acceptance-gate.log`/`r136-acceptance-native-results/`/
`r136-acceptance-protocol-results/`、三个acceptance frontend日志、
`r136-clean-acceptance-gate.log`及`r136-independent-acceptance.log`。
干净源码/真实程序在`/tmp/dbms-r136-clean-source.PzHdNERa/`，输入保持封印。

Root binary SHA256：
`83c5c094db6bc3cf8b69f59ba2bafac6e609664a0b5ebffd37bd81b5473226b2`；
cache：
`1b06d2c8d035cf776b0f3aeda0bf0e59e2750e00aee461d65c8de9d8eaa60cf6`；
59入口运行时的src/tests/scripts封印：
`d73cdabc68ea066a7720a74a60193362aaec5d76a1c1c932b8de275a65cfeaf6`。
Root全部头/编译flags签名348f43c0；绝对路径不同的clean签名不混作ABI证据。

暂存检查另发现新CLI文件EOF多一个空行。最后只删除该LF，AST相同；两份旧输入
封印都能通过补回该LF逐字节重构，证明其余src/tests/scripts未变，未重写旧59日志。
两份真实程序各重新执行CLI4全过，独立复核再次0。最终Root源码封印为
`a0cd8e29fb23527e354d99b724fbc891bbeb31dc3519c2ffa6194981e153ff6f`，
生产程序/cache/头flags未变；新证据见`r136-postformat-check.log`、两份
`r136-postformat-*-cli.log`及`r136-independent-postformat.log`。

## 仍未关闭的范围与版本管理

基于现有1..9999 civil解析域，不冒全域heap差分、BC/全部大年份、历史秒级offset、
TIMETZ物理/二进制、数组precision、全部时间函数/错误阶段、通用超过20字节复合
索引键或完整升级/降级验收。旧列不自动迁移，操作前备份并在副本验证，见
[部署](PACKAGING.md)和[手册](MANUAL.md)。

本项接受559MD、902native+537Python+3Main=1442；工作区含原三个MD/六个候选入口，
为562MD、905native+540Python+3Main=1448；561 Python AST/13 Bash另检。
原56933字节候选差异和三个暂缓draft逐字节保护，仅自有hunk/测试/相关文档单独提交。
原273条目/checkbox与22complete/166partial/70unverified/15deferred保持；
新1442尚未全量，原ecf1436仍1433实际通过/2失败/1TLS环境skip、原终态1。
不重开后台/R51、安全或其他已跳过专项；不push，GitHub Actions保持禁用。

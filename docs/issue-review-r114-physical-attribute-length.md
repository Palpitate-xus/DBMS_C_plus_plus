# R114：逻辑字段长度泄漏物理存储布局

## R190：区分当前 SQL TIME 的微秒布局与真实旧列

冻结 `2442e4c7` 原规范全套完整执行本夹具的 102 项，实际退出 1、仅 1 项失败：
仍要求新 SQL TIME 列为 4 字节。后续已接受的[R136](issue-review-r136-temporal-microsecond-storage.md)
明确将新 SQL 标量 TIME 改为 8 字节 TemporalMicros，旧工厂/旧 schema 保留 Legacy；
不能为通过旧断言而倒退已实现的微秒存储，也不能删除旧列兼容性检查。

当前夹具对新 SQL TIME 同时要求宽度 8 与显式 TemporalMicros；原 14 列在无关 ALTER
前后的检查还要求 codec 不变。新增真实 4 字节 native TIME heap/整秒行，以实际
CatalogManager API 构造其前升级 SQL catalog 输入（初始 attlen=4），验证无关 RENAME
后物理宽度/单位和值不变、目录逻辑长度为 8，以及独立 StorageEngine 冷读保持旧值。
这是明确构造的旧目录测试输入，不冒充从历史生产集群恢复的升级证明。

原 102 项除这一过时的新列期望外均保留，并增加 13 项旧列/目录前提/存活性控制；
两套库完整 115 项均实际退出 0、零失败。每侧新编译并链接该测试，核对各自 57 个
生产库对象/stub、127 头与编译配置。生产代码/头/入口数量未改；工作区保留的未接受
改动不借此验收。原全套继续冻结输入，其 102/1 失败记录未被新结果覆盖。

增强夹具 V1 完整 112 项两侧各 1 失败：原生物理表 API 未自动注册 pg_class，缺少
新增目录断言的前提。V2 显式构造上述真实 catalog 输入，仍保留而非删除目录长度
断言；V1 源和失败日志保留。自动注册/统一 catalog 的更大缺口仍属未完成总账，
没有通过夹具补前提而宣称 P0-03/CAT-02 完成。

证据：`/tmp/dbms-r190-physical-length-v2.5krddmw0/completed-focused-receipt.json`，
SHA256 `9d18163dafec6dc3569fa0e2c6d154f875bcf145e7ed6f883c90108425bf7785`。
原全套日志：`/tmp/dbms-accepted2442-full1492.9obmw81r/canonical-full-registered.log`；
V1 失败：`/tmp/dbms-r190-physical-length.gwcy769o/completed-focused-receipt.json`。
这不是修复后全1492、PG 差分、TLS/sanitizer 或原273验收。R187 不明退出仍开放，
R185 与其他明确暂缓项不恢复，不 push，Actions 禁用。下文数字与物理宽度按历史输入解释。

状态：本范围已修复并完成有限验收，R115先行、本项随后独立本地提交。

## 问题与影响

真实表目录将DATE/TIME/TIMETZ/INTERVAL的attlen写为物理codec长度12/4/4/-1，
而PostgreSQL逻辑长度应为4/8/12/16。物理数组还可能暴露固定元素宽度而非-1。
协议固定长度映射同时遗漏TIMETZ。客户端Describe、空结果和实际表RETURNING
可能拿到错误长度；不能通过修改行数据或把二进制字段当文本来解决。

由R113二进制间隔测试的严格size16断言发现，原断言保留；该codec仍单独待做。

## 修改方法

1. DDL目录属性从真实声明的pg_type.typlen取逻辑长度，数组为-1，不再复制heap
   dsize/isVariableLength。域使用其真实类型身份；未改变任何Column物理布局。
2. 目录冷读/加载和已有bootstrap升级仅识别已知旧内置类型签名及真实域祖先，
   将上述四类和活动数组的旧长度规范化。校验OID、namespace11、类型名、kind、
   声明/基类型长度；防循环和缺失祖先，跳过dropped/system/公共同名影子类型。
   冷读不写文件、不分配OID；仅显式owner publication使升级持久化。
   snapshot自身不清洗脏编辑，保持hasUnpublishedChanges语义。
3. 协议长度取不可变真实内置类型行，保留自定义类型的原有fallback；不每次查询
   活目录，不修改提供者生命周期、二进制发送codec或实际行存储。

## 核心验证与失败保留

`tests/physical_attribute_length_test.cpp` 最终102控制已全部通过：14种物理
声明、ALTER/冷暖重载、原行值和全部布局保持；旧目录的四个标量、数组、真实域、
影子和dropped守卫；文件/OID字节无写入、脏缓存观察与显式发布后耐久性。
旧版本先运行98/38失败和101/41失败，实际退出1。随后发现IO夹具读错了不存在
的oid_counter.dat，修正为真实`.oid_counter`并增加非空断言，最终为102；
不声称旧版本跑过102。隔离器在全局初始化前切目录，未直接运行Engine二进制。

协议永久控制28条覆盖Simple/ParseDescribe/真实Bind binaryNULL Execute、
空/NULL表、INSERT RETURNING、重命名/新增/修改typmod、别名重排和视图声明。
实际180006参考28全部通过，固定旧595869a9版本22失败、实际退出1。
首个R114候选原生102及catalog service/complete-read全过；协议28剩5个既有
CHAR typmod失败，RETURNING数组和INTERVAL modifier完整入口通过。此批整体
退出1，不能标成全绿。该独立别名身份问题由[R115](issue-review-r115-scalar-alias-projection-metadata.md)单独修复提交。

最初混合36控制的参考全部通过、旧30失败，保留完整草稿
`r114-initial-mixed-contract-draft.py`，后聚焦28条逻辑长度。额外19条参考/旧
观察均完整采集退出0，但旧有8次SQL错误和5个command tag差异；不是全通过。
视图执行/origin检查移交下表，永久28仍保留视图真实声明shape检查。

日志目录 `/tmp/dbms-review-r46.ifu6SWLp/`：`r114-reference36-initial.log`、
`r114-old36-initial.log`、`r114-reference28.log`、`r114-old28.log`、
`r114-old-native-{initial,domain}.log`、`r114-extra-{reference,old}.log`、
`r114-core-{native,protocol,terminal-fence}.log`及core逐协议日志。
第一次候选normal3CPP/repeat0CPP、修正IO夹具后closure0/0CPP均实际退出0；
其binary为510564cbd48b6ff15418e066be066c773728d25b6ccdbdd872229a7fa5f7aff8，
并非包含R115后的最终版本。

## 最终有限验收与版本边界

R115先修复并独立提交；最终冻结输入下15个完整native、21个完整协议共36个
不同入口全部实际退出0，包括本项102/28、R115的228、目录691、冷暖snapshot、
clean publication/失败回滚、域/数组/BYTEA/声明身份、真实列来源及默认完整协议。
日志`r114-r115-final-{driver,core,native,neighbor}.log`与逐协议目录保留。
normal/repeat自身3/0CPP、IO夹具closure0/0CPP与R115后1/0CPP均实际0；
没有改heap布局，没有借此声称重新编译58份。native用fresh stubs及隔离器，
生产/库均O2。最终binary：
`b4128ced2d381c48f7c783550cb69753ce99d7924f851a4f801f576cfce393dc`；cache：
`4432f1879906d8c4b101a16fc5022095961dd083d923956a3c0d07e94a04c917`；
src/tests/scripts seal：
`500f83626c629c534525c21bc6bd52b8c1d221094bb3d9b46c3afdfdfb363dcf`。
终态双stamp、58生产receipt、57测试库字节、冻结输入/公共头核对0。

当前538MD、542Python AST、13Bash语法/本地内联目标与状态/注册检查0；
原273一致性0、require-complete真实1。只接受本项15路径：catalog.cpp、
DdlExecutor.cpp目录长度hunk、NetworkServer.cpp逻辑长度函数、两个新测试、
单个协议注册和文档；已接受R115不重收，R113/R98/R99/R109候选不混入。
接受组合535MD/889native/517Python，注册889+3+517=1409；工作区892+3+521=1416。
原56933字节保留补丁重建后源码逐字节与三个草稿SHA核对0。首次临时比较器的
difflib/git hunk分割差异不代表源码变化，修正比较方法后的0与原失败日志都保留。
没有新完整注册集合、clean HEAD全量、真实TLS或整库sanitizer证明。

## 检测中发现的独立问题及后续状态

下表保留R114的原始发现和方法；其中非物化TEMP VIEW命令tag后由
[R116](issue-review-r116-view-command-tags.md)单独修复，时态NULL视图物化布局另由
[R118](issue-review-r118-temporal-view-materialization.md)单项记录实现与验收；目录SQL和来源仍开放。

| 问题 | 实际证据 | 后续修改方法 |
| --- | --- | --- |
| pg_attribute SQL源查询 | 限定和未限定的attrelid=regclass过滤均42P01，参考返回行 | 检查虚拟目录源绑定、路由与regclass过滤；不能仅诊断为隐式namespace搜索 |
| nullable DATE/TIME/TIMETZ视图执行 | 原表NULL正常，相应TEMP视图SELECT及WHERE false均XX000；INTERVAL/数组视图正常 | 追踪逻辑schema到物化层的布局和NULL转换，根因仍待证明，不写成已知codec修复 |
| 视图列来源 | Describe的table origin为0，参考为正 | 根据真实视图声明/绑定输出传递来源，不从显示名称猜测 |
| CREATE TEMP VIEW命令tag | 旧CREATE TEMP，参考CREATE VIEW，5次差异 | 由解析后的真实对象种类生成命令标签 |

这些是普通待做项，不是用户暂缓。R113发送codec、R111规划也仍开放。
原273项仍22complete/166partial/70unverified/15deferred，不提升勾选。
未获得新完整注册回归、真实TLS或整库sanitizer证明。R98/R99/R109候选和R106
草稿保留、不混入本项；不push，Actions禁用。

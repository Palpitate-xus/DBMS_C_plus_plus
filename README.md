# DBMS C++

<!-- P43-D18 unaccepted draft checkpoint -->
## 2026-11-07 P43：身份序列与系统目录草稿

本地 `master` 保存 P43/D18 草稿检查点，包含身份列 DEFAULT、REGCLASS 存储及系统目录元数据的实现进展。选定39个原生入口和两份协议回归通过，仍有明确兼容性缺口；未接受为完整功能，未运行本批正常性能对照。默认程序未替换，原273项状态保持。当前范围、失败记录和后续要求见[草稿验证报告](docs/performance-report-2026-10-30.md#identity-sequence-catalog-draft-20261107)；下方 P42 及更早段落保留各自历史结果。


## 2026-11-07 P42：纯读取事务减少提交刷盘

最新候选在确认同一 WAL 未推进且无写入／DDL／TEMP提交动作等状态时，跳过无用 COMMIT WAL 和 CLOG同步；SSI、约束、快照与锁清理保持。五类原生无写入事务从每次48B WAL、2～3次真实fsync降为0，写入保留原同步。新增95条SQL的旧／新请求回复逐字节相同，完整数据和崩溃后恢复通过。

正常621次计时、90个Main0及全部135行成本比较见[完整报告](docs/performance-report-2026-10-30.md#no-effect-transaction-publication-20261107)。固定字段全量查询墙钟中位数变化（首测／回测）：-24.44% / -19.53%。其余查询有混合结果，其他操作和所有负向均保留，不能称为全面提速。33个选定原生入口当前31个0；既有TOAST断言及WAL故障测试超时未通过，未运行全套。源码已更新，根目录默认程序未替换。直接master、指定身份、无推送，Goal active；原273项状态保持。下方P41及更早为历史记录。

<!-- P41 large-varchar-toast-search begin -->
<a id="large-varchar-toast-search-current"></a>
## 2026-11-07：长VARCHAR与TOAST写入优化

新增VARCHAR逻辑字符长度元数据：显式长度支持1..10485760字符，未指定长度使用独立的无界标记，名义物理dsize仍不超过65535。列声明、pg_attribute／RowDescription、普通写入／更新、数组元素、domain／generated赋值、ALTER及项目逻辑导出使用真实长度。标量不再把65535字节误当成字符上限；已验证70000字符／245000字节UTF8和1MiB无界值。内部无界字节上限为1073741819，但没有实际验收1GiB值或整个PG字符类型族。CHAR旧限制、完整encoding／collation语义仍开放。

本批功能有进展，长值写入部分提速；全面提速尚未成立。新增1MiB混合插入墙钟中位1801.508072ms，相对旧bf9首测／同旧回测-10.720%／-10.799%；更新RETURNING 411.759537ms，-13.372%／-12.280%；重新插入1431.222130ms，-9.197%／-10.414%。对应服务器CPU分别-12.689%／-12.822%、-20.934%／-21.267%、-12.666%／-12.762%。

负向保持OPEN：新长值DELETE RETURNING墙钟43.553088ms，+9.954%／+23.830%，服务器CPU+67.962%／+39.067%；首次VACUUM墙钟25.753918ms，+22.574%／+1.265%；FULL墙钟218.226818ms，+32.204%／+21.215%，服务器CPU+7.756%／+1.774%。新增完整值读取墙钟+2.080%／-2.014%，结果混合。通用270项矩阵的30个区域，墙钟6低于两旧／18高于两旧／6居间；服务器CPU10／8／12，客户端CPU6／14／10。没有排除、重采或把漂移归因为已证实的产品原因；全部负向与原因继续开放。

GitHub原53.45MiB JSON早已移出当前跟踪树，完整内容保留为5.70MiB gzip，原始大小／SHA及解压验证在manifest；历史未重写。当前HEAD无50MiB以上文件，忽略规则继续阻止同目录原始大JSON（manifest除外）。本次保留有用的压缩证据，不把原始大文件重新放进Git。 [完整正确性、升级边界与225行性能比较](docs/performance-report-2026-10-30.md#large-varchar-toast-search-20261107)。Goal active。
<!-- P41 large-varchar-toast-search end -->

<!-- P40 vacuum-full-relation-snapshot begin -->
<a id="vacuum-full-relation-snapshot-current"></a>
## 2026-11-07：减少FULL无关刷盘和备份复制

普通VACUUM FULL在备份前与重建后，只刷新目标逻辑关系及其分区／TOAST缓存，避免把其他表的脏页一并刷出。物理文件已同步，且在任何重建写入前全部unlink：这些文件优先用硬链接保留旧inode作为回滚映像，失败则完整复制；schema和specialized-dirty元数据继续独立复制。每份备份的fsync、完整大小／模式／SHA256校验，以及manifest、pending、替换文件／schema、WAL、COMMIT和undo的持久化顺序全部保留。本批只改TableManage.cpp的vacuumFull，没有修改全局缓存、index_file、WAL格式、CRC或恢复授权规则。

本批只证明固定字段FULL墙钟低于两侧旧版：-11.57%／-1.51%；单长／多长FULL服务器CPU均较低，墙钟各为-3.35%／+1.84%、-1.35%／+3.87%，结果混合。没有证明数据库全面提速，也没有追回P39相对更早84bb的全部恢复成本。

明显负向保持OPEN：首次普通VACUUM多长字段墙钟C1中位67.010218ms，旧c8ca首测30.951480ms／回测25.722458ms，即+116.501%／+160.512%。原三次样本分别为C1 [71.510152,26.679347,67.010218]ms、旧首测[35.992784,30.951480,25.821788]ms、旧回测[25.722458,23.927581,29.365169]ms，全部保留。单长字段大批重新插入的墙钟也+51.70%／+58.00%。未因负向重跑、排除样本或更改预期。

GitHub原先提示的53.45MiB原始JSON已经以5.70MiB gzip保存完整性能原始数据及错误，manifest记录原始字节数／SHA和解压逐字节验证；当前版本不再跟踪原大JSON，忽略规则阻止未来同类原始JSON（manifest除外）。保留有用压缩证据，没有重写Git历史。

[完整测试、负向结果与性能报告](docs/performance-report-2026-10-30.md#vacuum-full-relation-snapshot-20261107)。Goal active，下方各批为历史记录。
<!-- P40 vacuum-full-relation-snapshot end -->

<!-- P39 vacuum-full-wal-generation begin -->
<a id="vacuum-full-wal-generation-current"></a>
## 2026-11-07：修复FULL重写后的冷启动恢复

修复普通VACUUM FULL仍沿用旧物理身份、冷启动把旧页镜像重放到压缩后堆的问题。每次重写分配新物理代次；同一内部事务记录出生、旧代次退休意图／完成及COMMIT。替换前先持久化只属于该关系的原文件、完整模式／大小／SHA256清单及pending记录，替换文件与schema全部落盘后才提交。失败和未提交崩溃先恢复旧文件／schema，再重放WAL；已提交只清理备份。完成回滚的durable receipt防止遗留备份覆盖后续已提交DML。旧09 schema仅在FULL成功时发布新代次及专门的0x23完成记录，普通旧名称重用仍拒绝错误归属。没有放宽CRC、物理身份或页缺口校验。

原失败的22条旧库SQL现全部通过，关闭／强杀后的完整1MiB TEXT读回通过；旧损坏库和跨FULL的PITR不在本批修复范围。

本批没有证明全面提速。270项原样读写矩阵保留27个全新数据库、54个正常Main及1098条完整回复。30个区域的墙钟：7低于两旧／13高于两旧／10居间；服务器CPU：5低于两旧／14高于两旧／11居间；客户端CPU：7低于两旧／12高于两旧／11居间。所有90项比较均列出，性能原因OPEN。 vacuum-full-fixed墙钟 +64.55%／+78.39%；vacuum-full-fixed服务器CPU +10.12%／+10.45%；vacuum-full-single墙钟 +78.30%／+112.77%；vacuum-full-single服务器CPU +17.97%／+38.94%；vacuum-full-overflow墙钟 +131.53%／+114.90%；vacuum-full-overflow服务器CPU +18.05%／+13.79%（相对旧84首测／同旧回测，正数为更慢）。

最初被GitHub提示的53.45MiB原始JSON，目前以5.70MiB gzip保留完整性能原始数据与错误供复核，manifest记录原始长度／SHA和逐字节解压验证。当前版本不再跟踪该原始大JSON，忽略规则拦截同目录未来原始JSON（小manifest除外），不重写历史。

[完整测试与性能报告](docs/performance-report-2026-10-30.md#vacuum-full-wal-generation-20261107)。Goal active，下方各批为历史记录。
<!-- P39 vacuum-full-wal-generation end -->

<!-- P38 scalar-text-logical-limit begin -->
<a id="scalar-text-logical-limit-current"></a>
## 2026-11-07：TEXT长值容量修复

修复标量TEXT把持久化名义宽度65535误当逻辑值上限的问题：统一的字符字节限制函数对非数组TEXT使用1,073,741,819字节上限，并让旧式UPDATE校验也使用该函数。既有TOAST读回、插入和行构造因此一致处理完整长值；表结构dsize仍为65535，内联引用和存储格式不变。实际验证到1MiB，未进行接近1GiB的端到端容量验证。旧数据可由新程序读取；旧程序不能读取新写入的超64KiB TEXT，不能据此承诺降级后的值兼容。

新增33检查旧版21失败／当前0失败；完整1MiB TEXT已通过提交后强杀恢复与读回。

本批完整1500次正常计时没有证明全面提速。仅ID查询服务器CPU比旧版首测／回测高1.78%／1.86%；单长字段首次VACUUM CPU高10.98%／13.76%，墙钟高3.66%／9.08%。多长字段首次VACUUM墙钟低18.80%／高3.52%，CPU低0.33%／高3.27%，属于混合结果。全部负向、旧版漂移、首轮与长尾保留，原因OPEN；本批交付为TEXT容量修复，没有用局部结果宣称整体性能改善。

普通VACUUM FULL后旧页镜像重放失败仍待修复，未扩大功能完成承诺。用户指出的analyze-reindex-main-costs-20261104原始JSON为56,047,459字节（53.45MiB），当前Git使用5,977,159字节（5.70MiB）的.json.gz保存完整内容，manifest记录原始SHA及逐字节解压验证。原始大JSON不再跟踪，忽略规则拦截后续同目录原始JSON（小manifest除外）；保留压缩证据用于复核实际耗时和错误，未重写历史提交。

[完整测试与性能证据](docs/performance-report-2026-10-30.md#scalar-text-logical-limit-20261107)；Goal active。
<!-- P38 scalar-text-logical-limit end -->

<!-- P37 vacuum-physical-references begin -->
<a id="vacuum-physical-references-current"></a>
## 2026-11-07：VACUUM长字段数据安全修复

当调用者仍绑定一次真实 TableScan 的正 RID，特别是另一个数据库中同名表的 NULL 行时，旧收集器可能漏掉仍在使用的 TOAST 引用并回收这些对象；新代码只使用正在检查的物理元组。该触发由原生维护接口复现，未将正常 SQL 对照中的旧／新相同结果伪称为 SQL 数据丢失复现。

新增 vacuum_toast_physical_reference_test 在同一冻结源码上执行全部16种情形：旧版160检查／48失败／actual1，C1为160／0／actual0。覆盖普通表与原生范围分区存储、vacuumToast及外层vacuum、真实同库／跨库同名表调用者、跨位图字节的NULL、空串、文本NULL、UTF-8、类似marker的普通字符串、TEXT11003／40003及BYTEA70003／11009。独立完整值物理解码核对16种情形中的80个存活对象全部原始字节，以及每个真实孤儿的回收、调用者绑定恢复和第二次维护幂等。

本批1500项完整正常计时没有证明全面提速。在共同的真实after-DELETE种子上，单长字段首次VACUUM服务器CPU比旧版及旧版回测低8.36%／7.73%；多长字段首次VACUUM墙钟高9.83%／27.97%，CPU分别低1.92%／高35.29%。在共同after-UPDATE种子上，仅选ID的CPU高8.43%／8.39%，墙钟高9.77%／14.77%；三个全列查询墙钟也高于两次旧版，完整正负结果保留，原因OPEN。

用户指出的analyze-reindex-main-costs-20261104原始JSON为56,047,459字节（53.45MiB）；当前版本已使用5,977,159字节（5.70MiB）的.json.gz保存，原始SHA与逐字节解压核对保存在manifest，当前Git不跟踪原始大JSON，忽略规则阻止未来同目录原始JSON误加入（小manifest除外）。该证据用于复核原始耗时与错误，保留压缩内容；未重写旧提交历史。

[完整性能与压缩证据](docs/performance-report-2026-10-30.md#vacuum-physical-references-20261107)；Goal active，数据库功能与性能继续逐项推进。
<!-- P37 vacuum-physical-references end -->

<!-- P36 physical-row-null-reuse begin -->
<a id="physical-row-null-reuse-current"></a>
## 2026-11-06：查询执行继续优化

固定数据下的 720 次完整查询对照中，固定字段排序、单长字段排序、多长字段全列无排序的墙钟中位数分别下降约 27%～29%、23%～24%、10%～11%。只选 ID 的服务器 CPU 高于两次旧版约 12.37%／5.00%，墙钟居间；不能称所有查询或数据库操作都更快。

新增 sorted_projection_null_bitmap_cost_test 在相同冻结测试源码上，旧版 1156 检查／8 失败／exit1，C1 1156／0／exit0；每 64 行排序后投影的文件校验从 353 降到 0，两个回退控制仍为 233／353。覆盖内存与真实落盘排序、升降序、NULL/空串/文本 NULL、UTF-8、二进制、长字段、过滤来源、schema 不匹配、子类虚调用、LIMIT 与虚拟列错误。

另外 7 项相关原生回归在旧版和 C1 均实际 exit0。既有 toast_test 在两版均于第 604 行 `!corruptScan.open()` 断言中止（actual -6）；保留为失败，后续断言未运行，未修改测试预期或调查／修复该故障模块。首个原生控制器因此停止，后续仅执行原计划尚未启动的 11 个运行，没有重跑已完成项，也没有宣称全套通过。

VACUUM 与部分写入／维护 CPU 的负向结果继续 OPEN，完整270操作与720次固定数据查询均已保留。[性能报告](docs/performance-report-2026-10-30.md#physical-row-null-reuse-20261106)。源码、测试、压缩证据与既有文档已同步，Goal active。
<!-- P36 physical-row-null-reuse end -->

<!-- P35 view-column-dependencies begin -->
<a id="view-column-dependencies-current"></a>
## 2026-11-06：视图依赖删除保护（P35-C4，partial）

创建／兼容替换视图时，从已绑定的真实来源、具体列及查询子树记录依赖，覆盖投影、谓词、排序、分组、窗口、子查询与 USING/NATURAL 键。删除前先检查依赖；RESTRICT 返回 2BP01，未引用列仍可删除。CASCADE 按依赖顺序删除相关视图，后续子命令失败时由已有 DDL 快照恢复文件和目录记录。列序号压缩后，剩余引用沿用已有重映射。

新增 242 项原生检查和 7 项既有专项通过；28 次依赖删除返回正确状态，普通事务／保存点、真实旧库与冷重启有完整对照。本轮没有整体提速：普通 30 区域墙钟 19 高于两旧，创建视图墙钟 +41.47%／+53.49%，全部负向与原始分布见[性能报告](docs/performance-report-2026-10-30.md#view-column-dependencies-20261106)。

压缩后再清理生成副本，最初 53.45 MiB 报告保留为 5.70 MiB gzip；新证据 20.41 MiB。功能仍 partial，默认程序保持，Goal active；下方各批为历史结果。
<!-- P35 view-column-dependencies end -->

<!-- P34 projected-toast-demand begin -->
## 2026-11-06：部分列查询减少长字段读取（P34-C2）

仅取 id 的单长字段表墙钟相对旧首测/旧回测为 -54.47% / -47.06%，多长字段表为 -61.28% / -61.63%。全列查询保留原读取路径。新回归 14 组/83 检查及 5 项相关原生测试通过；完整协议回复保持。两轮各 270 项性能对照、全部负向与 PostgreSQL 兼容差异见[性能报告](docs/performance-report-2026-10-30.md#projected-toast-demand-20261106)。本批更新已有文档，性能证据压缩为 20.91 MiB，CPU 诊断另存 26.74 MiB；下方各批为对应版本的历史结果。
<!-- P34 projected-toast-demand end -->

<!-- P33 pg-class-declaration begin -->
## 2026-11-06: owned pg_class declaration, partial

`pg_class` now has a real persisted self declaration, 34 contiguous attributes, a row type/array pair and actual field-type dependencies. Unsupported value conversions for these metadata-only types, including NULL, fail explicitly with `0A000`. Seven value columns remain executable; query origins, the other27 values/wildcard, `pg_attribute` SQL and the type value/result codecs are unfinished.

Final native regression: six groups/550 checks pass, plus three existing native tests on both old and C2. The new strict PostgreSQL41-query reference passes; this engine matches17/41, with24 query differences retained. Full ordinary SQL cost controls do **not** show general acceleration: server CPU medians are above both old roles in16/30 regions. See the [implementation boundaries and complete performance report](docs/performance-report-2026-10-30.md#pg-class-declaration-20261106). No new Markdown path was added; evidence is compressed into 15.92 MiB.
<!-- P33 pg-class-declaration end -->

<!-- CURRENT_LEGACY_LOG_APPEND_20261106 -->

## 普通日志追加与实际查询成本（2026-11-06，P32-C1）

普通日志 `::log` 仍按 time、四个空格、user、四个空格、operation 和换行追加原始字节；每次实际打开当前路径，以 `O_APPEND` 和六段 `writev` 写入，EINTR 重试、正短写继续消费未写后缀，零进展或其他写错误停止并关闭。打开失败仍输出 `error 1` 并返回0；权限、已有前缀、rename/unlink 与真实 symlink 重新解析保持。`getTime`、`auditLog` 正文字节未变，没有持久缓存日志FD或改服务端协议发送。

正常CMake C++17/O2、旧版→C1→同旧版各三轮的固定矩阵完整结束：27个数据库上下文、54个正常Main、270个计时区间，校验1098响应、96624行/386388单元/27864个NULL。30案例的服务器CPU为16项低于两旧、6项高于两旧、8项居间；墙钟为13低/5高/12居间。固定/单BYTEA直接仅id查询有有限收益，但多LOB直接全列和仅id的服务器CPU均高于两旧，其中仅id墙钟也回退，全列墙钟居间。全部负向、旧回测漂移及长尾保留，未证明一致SQL提速、稳定吞吐或PG速度等价；原因仍OPEN，P30原负向证据保持。

新日志原生回归C1为13组/105检查通过；原协议仍53/58、另4个冷锚失败，既有9项普通差异保留。当前正式测试2393，默认程序仍425，Goal active。[本批完整报告](docs/performance-report-2026-10-30.md#legacy-log-append-20261106)与[可逆原始证据](docs/benchmarks/legacy-log-append-20261106.json.gz)。下方各批内容为其当时证据。

<!-- CURRENT_ORDINARY_PROJECT_BITMAP_20261106 -->

## 当前行NULL位图复用与按需求捕获（2026-11-06，P30-C2）

普通物理表的直接投影现在按选中列需求，复用当前行已有的完整NULL位图；局部作用域在正常或异常退出时恢复，只选非空物理列时不再复制。既有值提取、虚拟列求值顺序、TOAST处理、文件身份校验函数和暂停范围保持。

正常CMake C++17/O2的最终C2，在固定/单BYTEA/多LOB三类直接完整查询中，墙钟中位数对前后两次旧版降低约9%–32%，服务器CPU降低约11%–35%；这是本机各三样本的有限结果。主键投影负控仍有回退，固定表服务器CPU为+26.19%/+25.84%，单BYTEA主键查询墙钟为+8.03%/+3.65%。跳过多余复制没有消除这些回退，其根因未证，不能据此归因频率、负载或噪声，也不能称全库提速。

原273项仍为22 complete/166 partial/70 unverified/15 deferred_by_user，`IDX-07`保持`unverified`且`evidence=[]`。完整PG参考58/58；三代产品均完成全58目标与4冷控制，目标各53/58，另4冷锚控制仍失败。原9项目录/普通错误帧差异完整保留、没有改为PASS或新增暂缓。仅新增1个正式CPP回归；whole58 known-gap协议草稿未运行、未登记。Goal active，默认程序未替换，无服务重启、分支切换或推送。

完整原始分布、既有差异、许可和验收边界见[本批性能报告](docs/performance-report-2026-10-30.md#ordinary-project-bitmap-20261106)与[可逆压缩证据](docs/benchmarks/ordinary-project-bitmap-20261106.json.gz)。以下P29及更早内容为对应代别的历史记录。

<!-- CURRENT_MULTILOB_20261106 -->

## 多个大字段存储与真实命令结果（2026-11-06，P29-C2）

原七列建表同时含两个 BYTEA 和两个 TEXT 时，类型逻辑上限之和会超过堆头的 uint32 字段。现在仅对 format 2 中这种确实含变长列且最小实际元组可装页的溢出定义，将堆头名义宽度记为 0；可表示的旧宽度、列上限、实际变长元组和 TOAST 格式保持。UNLOGGED 初始化分叉在原有维护入口校验自身宽度，不匹配时重建。执行结果另显式标记无 RETURNING 的计数命令，普通 WITH DML 也据实际执行结果发送命令帧。

原19项定义/排序规则诊断在C1及C2均完整通过：每代父进程2154项检查，两个工作进程合计604项（452+152）。原失败发生在旧七列建表前置阶段，不能把它写成19项全已执行。另C1/旧P28C4双代12工作进程共2662项检查、6组真实冷重启RID对照全过；C2正式物理生命周期用例为10工作进程2483项、父进程17809项，覆盖真实大值/NULL/RID、备份恢复、分区、UNLOGGED及维护路径。以上是不同固定夹具和进程角色，不能相加冒充全仓测试数。

固定SimpleQuery文本格式0的37+3条、七列全值/NULL/十六进制BYTEA/列描述七项/命令与Ready检查，PG18.6首次40/40，C1为38/40，C2为40/40；C2自包含正式协议用例另一次同40控制也通过。C1保留的两项是DELETE多发空RowDescription帧，行值/NULL/状态/命令原本正确。C2按真实DML命令角色修正该帧形状，并保留SELECT/RETURNING结果形状；没有靠搜索字符串中的RETURNING判断。PG与本地分别读取真实关系OID进行来源锚定，不要求两个数据库的原始OID或整段原始协议字节相同。

原constraint154协议回归在C2完整通过。独立结果形状mini21参考PG21/21，C1为13/21，C2为19/21，仍保留两个普通缺口：零列一行SELECT缺少一个零列DataRow；零列表RETURNING *的错误代码实际42P01而PG为42601。它们未被改为PASS。

两个产品专属普通WAL角色共实际12条动作：COMMIT确认后强杀与未提交UPDATE确认后强杀，各自退出为预期-9，随后正常Main首先打开同一目录完成恢复并正常退出0；没有由原生读取器抢先恢复，也没有PG崩溃参考。后续在4份正常关闭后的独立副本上运行真实公共HeapScanCursor，34行/238单元/44个NULL和全部真实RID身份检查通过，四观察者均正常0且PID/进程组消失。初始到冷开共同键1..7不变，未提交角色全部8个旧键恢复，提交角色7个未修改旧键保持，实际修改键3及新键使用真实唯一RID。原WAL回执不回写；独立sidecar补充可见性证据，绝不把LP_NORMAL或虚构SQL ctid当可见行。

正常 CMake C++17/O2 固定成本对照已完整执行：旧版—C2—同一旧版各三轮，固定表及单 BYTEA 表各256行，另有C2专属多LOB表128行。168句实际计时、546个响应、24个正常Main及42个只读副本观察者均完成；全值/NULL/来源/冷开与真实RID验证通过。结果不支持本批普遍提速：16个可比操作中，墙钟与服务端CPU各为1项低于两旧、12项高于两旧、3项介于两旧；固定表查询在原双表流程中服务端CPU中位数15.667→30.044→18.536ms，C2增加91.76%/62.08%。全部变慢项、原三样本、长尾及漂移保留。DELETE旧版还多一个空T帧，不能当完全相同协议工作量；新多LOB因旧版建表失败，仅描述新版成本，不计算提速比。

后续固定CPU8的单表、三代正常构建窄诊断另有12次目标观测与24个正常Main；C2查询墙钟中位数22.086ms，对两旧24.181/26.336ms，服务端CPU14.326ms，对两旧15.406/16.605ms；C2相对C1的CPU中位数仍增加4.40%。这个更窄条件未重现原流程的大幅差异，但不能冲销原负向结果、归因于CPU亲和性或证明稳定收益，回退原因仍未确认。四角色用户态CPU采样的512次完整查询均通过，显示共同热点在逐列NULL检查及文件身份核验；采样计时没有混入正常性能表。

最终验证对象是候选C2（SHA `b53f853b356e9c8cf1e8fedf9832201d9bf705cf81d817b35314b4660e334594`），默认安装程序未替换，未推送或切换分支。原53.45MiB性能JSON当前保留为5.70MiB gzip，未压缩基准文件已被Git忽略；本批完整证据同样采用可逐字节还原的gzip归档。只对核验可恢复的生成副本安排清理，Git历史不重写。性能目标继续，不能把功能修复称为全库优化完成。

这是有限的 partial 进展，Goal active。原 273 项状态保持 22 complete/166 partial/70 unverified/15 deferred_by_user；`IDX-07` 仍为 `unverified`、`evidence=[]`。零列行集、零列表和原 TOAST 损坏夹具的普通缺口仍保留，用户暂缓边界保持。本批不证明完整 PostgreSQL 数据类型上限、文件格式、WAL 或协议等价。

完整定义、首轮失败与测试边界见[本批报告](docs/performance-report-2026-10-30.md#multilob-storage-20261106)和[可逆压缩证据](docs/benchmarks/multilob-storage-20261106.json.gz)。下方 P28 及更早段落是对应代别的历史记录。

<!-- CURRENT_CONSTRAINT_VALIDATION_20261106 -->

## CHECK/FK NOT VALID 与真实历史验证（2026-11-06，P28-C4）

`ADD CONSTRAINT ... NOT VALID` 仍在真实元数据锁下绑定 CHECK/FK 定义，只跳历史行验证；后续写入仍由原真实约束执行链检查。`VALIDATE CONSTRAINT` 对未验证的 CHECK/FK 扫描真实可见历史行，仅在成功后发布 validated 标记；失败保留定义和未验证状态，已验证约束不重复求值。真实 SQL NULL 与非 NULL 空字符串分别处理。CHECK 的 volatile 函数不因 volatility 标记被一律禁止；定义绑定不执行用户函数，实际验证仍按到达的行求值。

正常候选 C4 为 CMake C++17/O2，程序 SHA256 `5cd5fd9e15f72257d13f2622e180d643e5cc93fffe8a128cbf8e820f64caf539`；before 为 P27-C2 `dd5d9b85…`。原生父测试 4,896 checks/0 failures（含文件快照断言），其中真实 lifecycle worker 779/cold worker 158；两 worker 正常退出，捕获两份当时文件映像，不能将这些层级的计数相加。真实目标 PageAllocator fetch 38 个观测中 18 个为零、20 个为非零 fetch，另有真实读取正对照证明 hook 确实生效；这只说明历史验证访问路径，不代表整句无 I/O。原协议 142 主流程+12 冷控制共154条全部匹配 SQLSTATE、完整值/NULL、真实本地 relation OID 的 T7、tag 和 Ready；58份参数快照、5项保留标志检查通过。修改前同源用例实际83条（56匹配/27差异）及71条 NOTRUN；C3全部154条中有4处不匹配，原失败保留，C4修复后原断言未放宽。独立 append10 全10条通过：已经 validated 的带 nextval CHECK 不再求值，PK/UNIQUE VALIDATE为42809、缺失名42704。新增真实 PostgreSQL18.6 mini20 全20条符合首预测；产品对应19条+6条冷控制全25条、13项真实 `.params` 检查通过，ASCII collation 只比较大小写语义角色，PG使用真实 ICU、本地使用 nocase，不声称 provider/OID/Unicode 等价。mini19 v1接收器在启动前缺少 Path import：0SQL/0owner，原首错误保留后仅修接收器，不称产品重试或产品失败。原159参考的3个预期行快照错误及 reserved RESET 首预测错误都保留，后续 oracle 仅从已记录 PG 离线更正。完整原始帧不是逐字节 PG 等价：mini19主流程12组 raw 相同、7组不同（错误文本字段或真实本地 OID），不抹掉差异。

普通 ADD CHECK/FK 与同样有效数据的旧实现可比较所需验证成本。NOT VALID 可描述去掉不需要的历史扫描，但旧版多做扫描，不能称同扫描工作量提速；旧 VALIDATE 仅改标记，其耗时不是正确验证的性能基线。整个普通 ALTER TABLE 仍有 DDL 物理回滚快照、文件复制/摘要/同步成本，目标 PA fetch=0 不表示整句 O(1)、无堆文件读取或无 I/O。

固定0/128/1024行×三轮before-first/candidate/before-return，27个独立主流程+27次同数据冷重启均正常0；216个ADD/VALIDATE计时点、3024份响应、2384640个完整单元格（157950个NULL）、18360份物理文件映像（其中2700份堆映像）全部独立核对。1,024行时，CHECK NOT VALID wall中位数对前/后旧版分别−63.939%/−61.677%、server CPU −71.962%/−72.695%；FK NOT VALID wall −40.047%/−39.400%、CPU −15.482%/−13.469%，这两项是消除旧版多做的历史扫描。普通ADD CHECK wall −18.399%/−14.808%，CPU +0.124%/−4.360%；普通ADD FK wall −38.575%/−35.423%，CPU −4.131%/−0.022%。补上真实历史检查的CHECK VALIDATE wall +52.762%/+43.290%、CPU +260.317%/+251.282%，属于新增正确性工作成本，不能与旧假验证比较成等价速度。

负结果也保留：空表普通ADD CHECK CPU +29.920%/+35.993%；1,024行已验证FK重复VALIDATE wall对首/返旧版为−36.791%/+51.673%。全样本最长单点17.977415s，旧版wall中位数往返漂移−58.325%～+60.792%，server CPU漂移−28.186%～+4.689%。完整72行metric统计含各角色raw3/min/p50/max、双基准差值与旧版漂移，没有剔除负值/长尾，没有稳定倍数、因果结论或PG速度等价声明。计时是普通socket请求至Ready，包含DDL物理快照成本；process CPU与proc快照非原子、含所有服务线程，不是独占算子成本。

本批四个生产文件、两份新增回归、原登记入口和既有文档在 master 做本地版本管理，使用 `Palpitate-xus <palpitate.xus@outlook.com>`。正常验收程序独立冻结，根目录运行程序仍为 `425165af…`。原始证据只以完整可逆 gzip 入库；53.45MB 的旧 analyze/reindex JSON 已由约5.7MiB gzip替换，旧提交历史未重写。恢复步骤和逐流SHA保存在归档内，构建目录中的原始文件不再直接进入Git。

本批为有限普通约束生命周期的 partial 进展，Goal active。原 273 项状态保持（22 complete/166 partial/70 unverified/15 deferred_by_user）；`IDX-07` 仍是 `unverified`、`evidence=[]`。本批 GiST 页树资料只是未实现研究，不能将 P27 的 batch partial 记录当作 IDX-07 已部分验证。原用户暂缓边界保持。

完整代别、首轮失败、普通成本全表与范围见[本批报告](docs/performance-report-2026-10-30.md#constraint-validation-20261106)和[压缩原始证据](docs/benchmarks/constraint-validation-20261106.json.gz)。下方 P27 及更早段落保持历史口径。

<!-- CURRENT_GIST_PROBE_READER_20261106 -->

## GiST 读取开销与空文件回退（2026-11-06，P27-C2）

GiST 对不超过1MiB的 V2/V3 文件使用本次调用拥有的缓冲，CRC与条目解析检查同一份捕获字节；严格递增RID直接证明唯一，乱序时只排序私有验证向量。大文件和合法非空文本旧格式保留原读取路径，全部条目、边界、RID和尾部仍验证。零字节文件现在拒绝作为有效空索引，交回原可见堆；正规24字节V3零记录索引仍可使用。

真实机制观测减少读取调用和分配请求，同时增加累计分配请求字节；这不是RSS或正常SQL提速结论。新82命令协议的旧版12项失败已由C2修复，原PG75离线对照仍保留两处DELETE额外T帧差异。完整结果、正常SQL全部正负性能及证据由[本轮报告](docs/performance-report-2026-10-30.md#gist-probe-reader-20261106)统一给出。

本地仍是完整扫描的flat sidecar，分页GiST树、opclass/KNN/WAL/并发等价没有完成。既有普通缺口和用户暂缓边界保持；本批为partial，Goal active。下方P26及更早内容保留历史口径。

固定九进程正常SQL对照中，高端范围的服务器CPU中位数减少39.409%/46.918%，不存在范围减少16.759%/23.868%（相对首轮旧版/同旧回测）；无索引列控制CPU增加3.760%/4.748%。墙钟旧版回测漂移达−66.653%至+1819.366%，所有慢值保留，不作稳定倍率或因果结论。

<!-- CURRENT_GIST_EMPTY_BOUNDS_20261106 -->

## GiST 空字符串、边界与 LIKE 检索（2026-11-06，P26-C4）

当前物理heap格式的存储列构建/REINDEX会写GiST V3，覆盖真实非NULL空字符串；NULL仍由真实位图区分。V2编码本来允许零长度，缺陷在旧producer省略空键，V3标记新的覆盖保证。旧V2/文本文件继续严格读取；无法证明覆盖、CRC、文件归属或可见性时交回原堆来源。有限空端点、缺失端点、严格/非严格边界和字面DEL分别表示；最终完整谓词、MVCC、NULL及TOAST仍检查。

LIKE必要字节前缀使用单独的tryGiSTSearchLikePrefix，保留原pattern/escape角色；默认locale下不把字节前缀的权限推广到SQL标量比较。无有效Unicode上界时保留无上界，避免DEL哨兵截断。普通EXPLAIN的>=空串不再独立触发全宽索引遍历；JSON usedIndex仅在真实索引候选读取成功时为true。

本轮正确性和固定九进程性能对照、全部负面结果见[性能报告](docs/performance-report-2026-10-30.md#gist-empty-bounds-20261106)及[压缩原始证据](docs/benchmarks/gist-empty-bounds-20261106.json.gz)。本地GiST仍是flat sidecar，每次候选检索完整校验文件，并非PG的分页GiST树、opclass/KNN/WAL/concurrent全功能。虚拟键仍保留旧覆盖来源；native-only丢失定义、虚拟Bloom NEW NULL、既有missing-memory协议失败及其不完整旧版配对仍开放。下划线文件名歧义现在安全回堆，物理文件命名/关系身份布局仍需后续改造。原用户暂缓范围保持，Goal active；本批为partial。

以下 P25 及更早段落均保留当时的实现、计数和未完事项；本轮状态以本节为准。


<!-- CURRENT_REINDEX_ACCESS_METHODS_20261106 -->

## 普通访问方法 REINDEX 与空文本生命周期（2026-11-06）

本批补入普通 `REINDEX TABLE` 的 B-tree/Hash/Bloom/FTI/GIN/GiST/SPGiST/BRIN 重建、物理 Bloom NULL/空文本维护与 UPPER/LOWER 表达式索引 SQL 桥接。有效旧头的 BRIN `pages_per_range` 与 Bloom `k` 保留；`m` 随真实新键集按既有 loader 默认128 bitsPerEntry重算。旧头损坏/缺失时回退宽64/k7。默认程序仍为 `425165…`，没有服务重启或默认升级。

当前统计：2378 测试文件（原2375字节不变加2 CPP及1协议），顶层CPP1115（含链接桩）、总CPP1120/其中子目录5；登记680个driver/660个独立E2E。源248/头177，754 Markdown、725保护记录；24份旧gzip逐字保持，归档总计25份。原273项22/166/70/15及275条历史保持，仅新增partial第276条。

GiST 空文本编码、native-only 丢失定义、虚拟 Bloom NEW NULL 和上述失败均保留普通未完；不关闭原273项。

完整代别、首轮失败、工作量差异与全部成本表见[本批报告](docs/performance-report-2026-10-30.md#普通访问方法-reindex-与空文本生命周期2026-11-06)。以下P24及更早段落保留当时历史，不把旧数字当作当前统计。

下方 P24 及更早段落为历史记录。


<!-- CURRENT_BRIN_OPERATOR_CONSISTENCY_20261106 -->

最新P24-C2将普通BRIN摘要比较改为运算符专用路径；仍校验min<=max、所有文件/CRC/identity/dirty/snapshot fences与完整heap Filter复核，NULL flags不新增求值，legacy timestamp原路径保留。实际功能门禁及全部首次结果、固定9进程的完整维护/查询wall、CPU、长尾、负数与漂移见[性能报告](docs/performance-report-2026-10-30.md)和[压缩证据](docs/benchmarks/brin-operator-pruning-20261106.json.gz)；不称完整PG BRIN、所有SQL提速或因果已隔离。性能外层runner因label覆盖退出1、九backend正常0，二者分开记录；原source/plan/complete=false回执保留，后续仅离线finalize，不重采样、不当产品故障。默认425保持，partial、Goal active。旧2374测试逐字不变+1 native=2375；679/659 roster、源248/头177、顶层CPP1113/全部1118、754MD/725保护记录保持，历史275，旧23gzip保持+新24。下方P23及更早段落为历史记录。


<!-- CURRENT_BRIN_MAINTENANCE_OPTIONS_20261106 -->

最新P23-C1修复BRIN在VACUUM FULL、加列、删列和TRUNCATE/reset重建时丢失pages_per_range：健康V3配置在重建前保存，宽度1和SQL默认128均保持；缺失/旧格式/损坏仍沿用64回退。生产只改TableManage.cpp四处捕获/四次传参。新协议42命令/12镜像/4路径检查、25相关原生、6专项协议、3自动统计均正常0；PG18.6共享35SQL/19,327字段、11真实metapage一致；恢复7worker/2477检查。

固定9个正常socket进程测量完整VACUUM FULL及其后7类查询，所有wall/CPU、尾部、控制项和负数见[性能报告](docs/performance-report-2026-10-30.md)及[压缩证据](docs/benchmarks/brin-maintenance-options-20261106.json.gz)。该修复保留用户配置，不代表所有操作均提速；REINDEX仅Btree、完整PG BRIN AM/增量维护/WAL、规划重复验证开销、Bloom/GiST非NULL空文本覆盖和P22数组ROLLBACK间歇超时仍开放。前批密集CPU回测+6.64%负面记录保持。本批partial、Goal active。源248/头177，2373旧测试字节不变+1=2374，679注册/659独立E2E；原273item状态不变、历史274；754MD/725保护记录/22旧gzip保持。下方P22及更早段落为历史记录。


<!-- CURRENT_BRIN_TYPED_SCAN_20261105 -->

最新 P22-C10 修复 BRIN 数值边界/NULL 状态并接入真实候选页扫描，支持普通 SQL/绑定后五类比较和 NULL 谓词、反向常量及 pages_per_range。23页 extent 的原生候选 cursor 实际只 fetch1页；完整谓词仍复核。格式组件 O2/ASan各5588、新原生1834、新协议101命令/25路径断言、真实PG18.6共享73SQL/10490字段及7恢复worker/2477检查通过。有限81原生80正常0+1父代TOAST中止；旧32协议29正常0+3父代相同失败，全部保留。

固定9个正常socket进程、4096×4、七类查询，服务器CPU减少（首轮／同旧回测）：高端窄范围 41.19%／42.36%；低端窄范围 48.68%／49.29%；非主键等值 25.78%／20.13%；不存在的范围 94.22%／94.12%；密集范围控制 -7.44%／-6.64%；主键控制 30.31%／28.42%；无对应索引列控制 7.79%／4.38%。所有wall/CPU/控制负数、原始帧和尾部见[性能报告](docs/performance-report-2026-10-30.md)与[压缩证据](docs/benchmarks/brin-typed-scan-20261105.json.gz)。新BRIN文件V3，旧1/2只作兼容读取/安全回堆，旧二进制不能读取V3；当前物理heap2保持，暂缓format2仍指wire/output，NetworkServer.cpp未改。REINDEX尚仅Btree、部分重写配置保存、PG opclass/revmap/增量维护/WAL/concurrent、SSI及特殊类型/来源路径等仍开放，不声称全部数据库操作或PG性能等价。本批partial，Goal active。源248/头177、测试2373，678注册/658独立E2E；原273对象状态不变、历史273，754MD/725保护记录和21旧gzip保持。下方P21及更早段落为历史记录。


<!-- CURRENT_INDEX_BUILD_NULL_OWNER_20261105 -->

最新 P21-C1 让索引构建/REINDEX 复用当前行物理 NULL 位图，并修复 BRIN 将真实 SQL 全 NULL 整数摘要写成 `[0,0]` 的问题；空字符串范围保留。新原生13366检查、新协议40命令、真实PG18.6共享35命令/2250字段及7恢复worker/2477检查通过。有限相关79原生78正常0+1父代相同TOAST中止；旧32协议29正常0+3父代相同失败，全部保留。

正常2048行维护9进程、368640字段核对；相对同旧回测CPU中位数减少：Btree 19.30%，复合 19.02%，写事务 45.10%，回滚 47.67%；BRIN -154.76%（退步）。wall波动大且多项变慢，另9进程完整socket查询控制也没有一致提升，详见[性能报告](docs/performance-report-2026-10-30.md)与[压缩证据](docs/benchmarks/index-build-scan-owner-20261105.json.gz)。BRIN数值排序及SQL执行算子等仍为普通开放缺口，本批partial，Goal active。当前物理heap版本2保持；暂缓format2指协议/输出所有权。源248/头177、测试2370，677注册/657独立E2E；原273状态不变、历史272，754MD/725保护记录与20旧gzip保持。下方P20及更早段落为历史记录。


<!-- CURRENT_INDEX_CRC32C_20261105 -->

最新P20-C2为共享索引CRC32C加入通用查表和受CPU能力保护的SSE4.2路径，保留完整校验、所有文件字节与C++17基本构建要求。组件44392检查及ASan/UBSan、无SSE4.2模拟35520检查通过；12真实存储worker/3840字段与六类索引旧新双向读写、重建文件逐字节一致。最终68原生67正常0及1项父代相同TOAST中止、旧32协议29正常0及3项父代相同失败全部保留。9正常socket进程wall中位数减少（首轮／回测）：热点前缀 18.62%／-0.48%；热点等值 3.68%／0.90%；低频前缀 11.82%／14.98%；低频等值 -12.80%／-22.17%；不存在等值 6.78%／0.48%；主键控制 -1.07%／-6.83%；控制差异不作因果归因。源248/头177、测试2368，676注册/656独立E2E；原273状态不变、历史271，754MD/725保护记录与19旧gzip保持。Goal active；下方P19及更早段落为历史记录。详见[性能报告](docs/performance-report-2026-10-30.md)。


<!-- CURRENT_GIST_READ_OWNER_20261105 -->

最新P19-C3修正普通当前读事务被GiST误判为重叠写者的问题，并复用可见元组NULL位图。专项1214检查、新39协议、PG18.6共享37命令/1287字段对照通过；候选检索阶段目标堆页fetch由旧版2021降至0，返回行仍正常回表校验。历史/重叠/自身写入/缺失损坏/空文本范围及SERIALIZABLE保留堆后备。旧60原生59正常0和1项父代相同TOAST中止、旧32协议29正常0和3项父代相同严格失败全部保留。9正常socket进程wall中位数减少（首轮／回测）：热点前缀 2.54%／3.42%；热点等值 -0.42%／-1.44%；低频前缀 56.62%／52.19%；低频等值 -12.42%／-10.29%；不存在等值 2.15%／1.44%；主键控制 -6.09%／-3.54%，负数与控制波动也报告。旧GiSTScan/idx_scan只代表算子，不能单独证明底层物理索引访问；下方旧章节按此限制理解。源248/头177、测试2367，676注册/656独立E2E；原273状态不变，历史270，754MD/725保护记录保持，18旧gzip保留并新增1。仍partial，Goal active；详见[性能报告](docs/performance-report-2026-10-30.md)。


<!-- CURRENT_TYPED_PLANNER_STATISTICS_20261105 -->

最新批次 P18-C6 将普通等值与二进制文本 LIKE 扫描成本接入拥有真实身份的 typed ANALYZE 统计，并修正事务内显式分析的两份统计文件回滚。新65条协议/15次扫描、原LIKE123/27、最终原生专项及cold exec、组件O2/ASan/UBSan、7个恢复worker和真实PG18.6共享33命令/6732字段对照通过；相关旧协议29通过、3项父代完全相同严格失败，首次自动数组/range回滚超时及后续原deadline固定旧/新/旧六次通过均保留。完整socket性能中位数减少（首轮／回测）：热点前缀 73.72%／73.25%；热点等值 51.25%／15.72%；低频前缀 58.53%／-8.58%；低频等值 44.30%／38.10%；不存在等值 24.66%／24.31%；主键控制 30.77%／29.00%，负数也保留，只代表该负载。原273与22/166/70/15状态保持，历史269，测试2365，源248/头177，675注册/655独立E2E，754MD/725保护记录、17旧压缩包保持。仍是partial，Goal active；事务内ANALYZE需要临时数据库物理备份，旧统计格式、未转换消费者和用户暂缓模块未整体完成。详见[性能报告](docs/performance-report-2026-10-30.md)与压缩原始证据。


<!-- CURRENT_LITERAL_LIKE_SCAN_20261105 -->

## LIKE前缀的真实索引读取与密集前缀成本（2026-11-05，P17-C6）

普通无参数SELECT中的TEXT/VARCHAR字面量LIKE现在可实际使用已有GiST侧索引。已绑定AST提供真实关系/列身份；前缀范围只筛选候选，原类型化LIKE、AND剩余条件和NULL语义继续复查。转义百分号/下划线、多字节ESCAPE、UTF8上界以及打开前删除索引均有回归。可用ANALYZE的MCV与完整覆盖直方图桶识别密集前缀，按真实表extent和成本参数保留堆扫描；未知探测至少计1行成本。

详见[性能报告](docs/performance-report-2026-10-30.md)与[压缩原始证据](docs/benchmarks/like-prefix-scan-20261105.json.gz)。本批仍为partial，Goal active。


<!-- CURRENT_ORDINARY_QUERY_PROFILE_20261105 -->

## 普通查询的规划元数据与解析开销（2026-11-05，P16-C2）

本批在一次普通查询规划内复用关系schema，并按需各读取一次B-tree/Hash/Bloom/GiST列清单；主键与不可能使用位图/GiST的条件省去无用发现。普通表达式先排除不可能的类型常量前缀，避免对数字、表达式分隔符前的列名做推测类型解析及异常处理。真正的限定/引号/修饰/复合类型常量与显式CAST仍使用原声明语法。

详见[性能报告](docs/performance-report-2026-10-30.md)与[压缩原始证据](docs/benchmarks/ordinary-query-profile-20261105.json.gz)。本批是普通读取partial进展，Goal active。


<!-- CURRENT_EQUALITY_SCAN_COST_20261105 -->

## 普通等值查询的实际扫描选择与完整网络成本（2026-11-05，P15-C7）

本批让普通原生标量堆表查询实际消费基于ANALYZE频率的扫描选择：热点等值可选顺序扫描，低频等值和主键保留索引路径。此前只修改规划器时，无ORDER BY的普通查询仍绕开物理计划；现在结构化输出已接通，并修复无可用索引OR的显示文本合并，以保留重复行、NULL、空串和换行。B-tree、位图AND/OR与GiST在探测前取得语句快照，并在快照不适合当前索引或探测期间提交边界改变时读取完整可见堆候选，再做原条件检查。

这是普通读取的partial进展，使用现有原生 `.stats` 中的MCV/计数；尚未让优化器全面消费新的typed `.analyzed_statistics` 槽。普通EXPLAIN的文本缓存键现包含精确的实时成本设置；没有修改已暂缓的retained/custom/generic规划、参数、portal/format2或mutation/provider生命周期。当前私有普通后端P15-C7 SHA256为 `3ff5f16a9cc3a223d6618db87d8a5bbe85240dfd4c08cfdceafeb6ba49c03d3a`。默认根程序425与既有服务保持；Goal active，PostgreSQL整体功能仍partial。

原始测量、失败与独立复核见[压缩机器归档](docs/benchmarks/equality-scan-cost-20261105.json.gz)和[性能报告](docs/performance-report-2026-10-30.md)。


<!-- CURRENT_PG_STATS_SOURCE_20261105 -->

## pg_stats 完整基础查询与读取成本（2026-11-05，P14-C6）

本批接通实际 `pg_catalog.pg_stats` 的17列基础SQL查询、参数为零的纯Describe、文本/二进制/混合结果格式，以及实际 owning anyarray 输出；支持已测的过滤、投影、排序、LIMIT、别名、CTE和JOIN。每次查询只读取当前数据库的真实统计与目录，重用本次查询内的关系/属性索引和每关系一次的物理模式校验，减少逐列重复扫描、模式复制和文件散列。

本节更新下方P13当时“17列SQL尚未接通”的结论；下方历史批次保留当时事实，当前状态以本节为准。当前私有普通后端P14-C6为 `9362b02aba17d45038702211ba42a6a1d797ef3580d97edb8ffabce80e0f63b6`。默认根程序425和既有服务未切换，Goal保持active；整体PostgreSQL功能仍为partial。

原始测量、失败及逐字复核见[压缩机器归档](docs/benchmarks/pg-stats-source-20261105.json.gz)与[性能报告](docs/performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_RANGE_COLLECTOR_20261105 -->

## 范围统计采集与完整 ANALYZE 成本（2026-11-05，P13-C2）

本批新增六种内置范围类型int4range/int8range/numrange/daterange/tsrange/tstzrange及实际范围域的ANALYZE采集：独立排序下界和上界、保存范围边界与长度直方图、空范围比例、NULL比例和实际存储宽度。边界保留包含关系、无界端点和子类型语义；本次样本局部缓存解析键，避免排序时反复解析。

最终私有普通后端P13-C2为 `3593b23e5e142605fe71149daec3347015003fb7a84d8a2505c80578e4e77a5d`，默认根程序425保持。完整pg_stats目录/17列SQL及output-only anyarray尚未接通，两条实际探针仍严格失败0A000。多范围尚缺真实目录/存储拥有者；本批仍为采集阶段的partial进展，Goal active。

原始测量、失败和复核见[压缩机器归档](docs/benchmarks/analyzed-range-collector-20261105.json.gz)及[性能报告](docs/performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_ARRAY_COLLECTOR_20261105 -->

## 数组统计采集与完整 ANALYZE 成本（2026-11-05，P12-C3）

本批把真实数组存储输入接到同一次ANALYZE采样：保存整数组MCV/直方图/相关性，以及常见元素、按非NULL数组计算的成员频率和不同元素数直方图。实际验证integer/text/numeric/boolean/float8数组的NULL、空数组、字面NULL、重复值、多维/下界和冷读；其他元素类型及全部PostgreSQL功能仍需分别验收。

最终私有普通后端P12-C3为 `7d3cc2a1353c105ecac62579a919ec96d088cd325fa74234bc47252061e1029a`，默认根程序425保持。完整pg_stats目录/17列SQL与output-only anyarray尚未接通，两条实际探针仍严格失败0A000；本批是采集阶段的partial进展，Goal active。

原始测量、失败和复核见[压缩机器归档](docs/benchmarks/analyzed-array-collector-20261105.json.gz)及[性能报告](docs/performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_SCALAR_COLLECTOR_20261105 -->

## 标量统计采集与 ANALYZE 优化（2026-11-05，P11-C4）

已接入真实ANALYZE的标量采集和快照发布；旧优化器/原生8列统计保持。普通服务器四条SQL流程与独立快照读回通过，完整pg_stats查询仍有2项0A000失败。PG对照的3处存储宽度及2处collation OID差异仍开放。

本批同功能版本的完整ANALYZE对照（4096行、4类型，每进程计时5次，前/新/回测各3进程）CPU中位数降低51.93%/54.81%，wall降低50.64%/54.17%；9份完整数据/RID/NULL和统计快照逐字一致。对照是本批P11-C2/P11-C4，不是父提交后端或任意SELECT查询提速。

源236/头166；原234生产文件仅TableManage.cpp变更，164旧头和2342旧测试保持。新增3测试后2345（顶层1094 CPP含桩、去桩1093，另5子目录CPP）。原273项22/166/70/15及261条历史保持，仅加1条partial后262；Goal active。

详见[性能报告](docs/performance-report-2026-10-30.md)、[验证状态](docs/production-status.md)和[原始证据](docs/benchmarks/analyzed-scalar-collector-20261105.json.gz)。


<!-- CURRENT_ANALYZED_STATS_SNAPSHOT_20261105 -->

## 完整统计视图的快照基础（2026-11-05，P10）

P10 已加入完整17字段的类型化统计模型、7类统计槽及无损快照读写组件。ANALYZE采集器和SQL视图尚未接入；两条完整17列SQL探针仍返回0A000，保留为严格失败。普通后端仍是P9-C2 `30c57ca8…`，原有`.stats`和SQL行为保持。

组件父进程568次检查、冷读和两个fresh写入子进程通过，O2、ASan/UBSan及正式原生入口通过。新组件编码CPU p50较本批未优化组件低14.42%/14.41%，属于组件对照，不能称数据库查询提速。详见[报告](docs/performance-report-2026-10-30.md#完整统计视图的快照基础2026-11-05p10)和[机器归档](docs/benchmarks/analyzed-statistics-snapshot-20261105.json.gz)；默认425及既有服务保持。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧节保留各批次当时结果。


<!-- CURRENT_STORAGE_LIFETIME_20261105 -->

## 存储退出期生命周期（2026-11-05，P9-C2）

当前源码P9-C2（正常程序 `30c57ca8…`）修复表名列表标记与维护状态表的两处退出期生命周期错误。原七阶段恢复回归7个工作进程正常0，完整Memcheck的父进程及7个工作进程均未报告内存错误；正常Main仍有5项探针元数据差异。

旧／新／旧各3次完整恢复调用全部通过，CPU p50约±1%，墙钟受旧版前后漂移影响，未观察到稳定加速。本批是正确性修复。默认425及既有服务保持；详见[完整报告](docs/performance-report-2026-10-30.md#存储退出期生命周期2026-11-05p9-c2)和[机器归档](docs/benchmarks/relation-list-engine-lifetime-20261105.json.gz)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user，Goal active。下方旧节保留当时版本的结果。


<!-- CURRENT_RESTORE_TRANSACTION_HORIZON_20261105 -->

## 物理恢复事务号水位（2026-11-05，P8-C3）

P8-C3 正常程序为 `d778afbd…`，补入公开物理恢复发布前的事务号水位对齐，并优化该公开入口的清单验证；保持C++17。原始较早目标的正常Main首次探针由1/4行恢复为4/4，数据、后续写入与冷启动检查通过，仍有5项描述符/目录身份检查失败。相同七阶段原生验收在最终C3仍于advance阶段异常退出，后四阶段未运行，不能称完整恢复验收通过。

第二轮独立公开API计时中，C3 wall p50较两次旧P7-C1低4.16%/6.42%；第一轮C2仍慢于旧版，两轮全部长尾保留。这是固定正确目标上的完整恢复调用，完整35操作/36查询与PostgreSQL速度未重测。默认程序仍为425，现有服务未重启。详见[完整报告](docs/performance-report-2026-10-30.md#物理恢复事务号水位2026-11-05p8-c3)和[机器归档](docs/benchmarks/restore-transaction-horizon-20261105.json.gz)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧节保留各批次当时结果，不能覆盖本节的失败与范围。


<!-- CURRENT_BACKUP_ANALYZE_20261104 -->

## 备份摘要与 virtual 列统计（2026-11-04，P7-C1）

本批 P7-C1（正常程序 `724c1147…`）优化物理备份清单的创建端摘要，并按 PostgreSQL 18.6 排除 virtual 生成列统计；仍使用 C++17。新增两个永久测试共884项通过，相关12个原有入口通过。

完整备份：wall p50为737.771→587.376→677.477ms，较两次旧版低20.38%/13.30%；四类正常 SQL DDL：wall p50为2快/2慢，ALTER与TRUNCATE慢于两次旧版。完整35操作/36查询未对本批重跑，默认程序仍为425，现有服务未重启。SQL对照34项仍有17项差异，另保留一个旧版恢复缺口。详见[性能与范围报告](docs/performance-report-2026-10-30.md)和[机器归档](docs/benchmarks/backup-analyze-20261104.json.gz)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方按批次保留当时记录，当前状态以本节为准。


<!-- CURRENT_GENERATED_DEMAND_20261104 -->

## 虚拟生成列读值需求与完整 SQL 实测（2026-11-04）

本批源码 P6-C8（正常程序 `45264111…`）补入虚拟生成列实际读值需求、声明赋值和排序输入身份验证，仍使用C++17。四条100行/八virtual的正常SQL在C2/C8/C2中全部正确；SELECT * p50 923.223→80.538→916.453ms，未选分支CASE为863.623→21.730→871.064ms。

固定258观察新增82个整项通过、无整项通过回退，仍有20项差异及6项组件描述符损失；原767观察保留129差异。默认仍是425，整库性能未验收。详见[性能与范围报告](docs/performance-report-2026-10-30.md)及[完整机器归档](docs/benchmarks/generated-column-demand-20261104.json.gz)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧批次保留当时事实，当前状态以本节为准。


## ANALYZE 与 REINDEX 成本优化（2026-11-04）

本批源码 P5-C2（a3baaca6…）优化同次 ANALYZE 的类型/locale 比较和真实行 NULL 位图，以及 REINDEX 逻辑 map 保留。C++17、原维护频率/持久化和用户暂缓边界保持。ANALYZE p50 369–403→74 ms，范围 UPDATE 600–650→389 ms；完整 35 操作为4快/28慢/3居间，36组查询为4快/6慢/26居间，整体性能提升尚未验收，默认仍为425，现有服务未重启。

详见[完整性能报告](docs/performance-report-2026-10-30.md)和[本批机器归档](docs/benchmarks/analyze-reindex-main-costs-20261104.json.gz)。下方旧批次保留其当时结论，当前以本节与报告首节为准。

## C4功能与完整成本记录（2026-11-04）

当前源码C4（9d85c688…）补入RETURNING纯声明与真实来源元数据、虚拟列拥有值及部分MERGE来源输出。新增123原生控制122通过/1个严格元数据差异，新132协议对照全过，原56项为55通过/1差异；273项状态未变，Goal继续active。

实际35操作为7快/19慢/9居间，范围UPDATE及REINDEX仍慢；36组完整查询25快/2慢/9居间。未通过本机默认程序的整体性能提升验收，默认仍为425（425165af…），现有服务未重启。独立API512行的分配/CPU收益及空表/单行负值分开报告，不能概括为所有数据库操作已提速。

使用本批新增功能需要构建当前源码。以下日期更早的章节保留当时结果。

详见[本批报告](docs/performance-report-2026-10-30.md#returning-声明真实行值与操作热路径2026-11-04)及[机器归档](docs/benchmarks/returning-declarations-hotpaths-20261104.json.gz)。以下为历史记录。

使用 C++17 实现的关系型数据库管理系统，提供 SQL 交互、页式存储、索引、事务和查询执行模块，以及 PostgreSQL wire protocol 3.0 的部分实现。

本仓库面向数据库实现的学习、研究与开发。评估具体使用场景时，请先阅读兼容性契约，并在独立的数据目录验证所需的 SQL 与客户端行为。

## 内置函数注册与复合 RETURNING（2026-11-03）

根目录本地默认程序现已原子安装为同批实测C3 `425165af…`；原S3程序已备份，未重启现有服务。

C3 `425165af…` 将无实例状态的内置函数注册表共享为只读数据，保留每个求值器的覆盖、时钟与真实回调语义，并补入实际 INSERT/UPDATE/DELETE RETURNING 的拥有值传递。在已有正确描述符的路径，record、record[] 以及真实物理数组的二进制结果有新增严格控制；OLD/NEW 别名描述符、虚拟数组载荷与历史数组位图仍有缺口。

独立辅助求值夹具的分配减少，但公开游标首次执行CPU也有慢值；两种局部测量都不能替代最终完整SQL对照。

新增两个原生入口共34控制通过；固定301观察为188匹配/113差异，新56项 RETURNING 为48通过/8差异，没有声明功能族完成。完整代别、原失败与测量状态见[本批报告](docs/performance-report-2026-10-30.md#内置函数注册与复合-returning2026-11-03)；最终36组查询中位数全部低于两次S3对照；35类操作为18快/2慢/15居间，保留慢项和漂移，不代表所有操作均已加速，原273状态不变。独立机器归档见[全部原始统计](docs/benchmarks/function-registry-returning-20261103.json.gz)。以下C8/C7记录按其当时版本保留。

## 最新功能验证（2026-11-03）

C8 `91d339db…` 保留原始类型行物化的分配优化，功能结果与C7一致；36组完整查询对照为5快/18慢/13居间，未证明稳定跨预算收益，默认安装仍为S3。局部分配减少不代表整体查询提速，完整样本见[本批报告](docs/performance-report-2026-10-30.md#原始类型行物化分配优化2026-11-03)及[机器归档](docs/benchmarks/owned-datum-primitive-20261103.json.gz)。

前批C7记录保留：功能增量已提交 `89696698`。36组速度对照为4快/23慢/9居间，新程序未替换根目录S3；类型、目录及DML仍有缺口。全部样本、功能验证和后续优化点见[本批报告](docs/performance-report-2026-10-30.md#拥有值的复合类型输出与引用类型存储2026-11-03)。

## 快速导航

- [构建](#构建)与[运行](#运行)：环境准备、编译和启动。
- [SQL 示例](#sql-示例)与[使用手册](docs/MANUAL.md)：查询和事务操作。
- [测试](#测试)与[参与贡献](#参与贡献)：验证改动和报告问题。
- [性能测试报告](docs/performance-report-2026-10-30.md)：查询、增删改、事务、DDL、COPY、索引与维护实测；含有界排序、按页扫描、普通 LIMIT、外部归并、会话工作内存、类型化/集合排序、整数键缓存、VALUES 查询子句、全局计数表达式/动态排序上限及类型化窗口宿主/有序输出复用、窗口整数键缓存/实际执行成本、窗口结果共享与排序投影，以及未接受的暂存候选、类型输出调研的完整结果和复现方法。
- [兼容性契约](docs/compatibility-contract.md)：与 PostgreSQL 接口的差异和边界。
- [与 PostgreSQL 的距离](docs/postgresql-comparison.md)：273 项功能总账、主要缺口和 PostgreSQL 18.6 同机性能对照。
- [当前状态与验证范围](docs/production-status.md#当前状态与验证范围)：最新性能批次、剩余瓶颈及文档检查入口。

## 项目定位与兼容性

本项目使用自己的存储格式，不是 PostgreSQL 的直接替代品，也不能直接打开 PostgreSQL 数据目录。客户端能够连接，不代表 SQL、系统目录、事务或存储行为与 PostgreSQL 等价。

SQL 与客户端协议的接口边界见[兼容性契约](docs/compatibility-contract.md)，具体用法见[使用手册](docs/MANUAL.md)。

## 功能概览

- SQL 解析、AST、类型绑定，以及数据定义、查询和数据修改的执行入口。
- 页式堆存储、缓冲池、变长数据和 TOAST。
- B+ 树、Hash 等索引访问方法。
- Volcano 查询执行器，包含扫描、过滤、投影、连接、排序、聚合和窗口算子。
- 事务、MVCC、保存点、锁管理、WAL 和恢复模块。
- 交互式命令行，以及 PostgreSQL Simple / Extended Query 协议入口。

## 构建

### 环境与依赖

- Linux，支持 C++17 的编译器；脚本构建使用 `g++`。
- Bash、`pkg-config`。
- zlib 与 ICU 开发库。
- OpenSSL 开发库用于 TLS 网络服务；未检测到 OpenSSL 时构建 TLS stub。
- Python 3 用于测试；CMake 构建需要 CMake 3.16 或更高版本。

Ubuntu / Debian 安装依赖示例：

```bash
sudo apt-get update
sudo apt-get install build-essential cmake pkg-config python3 \
    zlib1g-dev libicu-dev libssl-dev
```

### 脚本构建

```bash
git clone https://github.com/Palpitate-xus/DBMS_C_plus_plus.git
cd DBMS_C_plus_plus
bash scripts/build.sh
```

生成的程序位于仓库根目录：`./dbms_main`。

### CMake 构建

```bash
cmake -S . -B build/cmake
cmake --build build/cmake --parallel
```

生成的程序为 `build/cmake/dbms_main`。两种构建方式使用同一份[生产源码清单](cmake/dbms_sources.txt)。

## 运行

每次启动都必须用 `-D` / `--data-dir` 或 `DBMS_DATA_DIR` 显式指定本项目的数据目录。不要使用 PostgreSQL 的数据目录。登录角色和访问配置须提前准备；数据目录、角色配置与部署约定见[打包与部署说明](docs/PACKAGING.md)。

交互式命令行：

```bash
./dbms_main -D /srv/dbms/main
```

网络服务：

```bash
export DBMS_TLS_CERT=/etc/dbms/tls/server.crt
export DBMS_TLS_KEY=/etc/dbms/tls/server.key
./dbms_main -D /srv/dbms/main --server 9999
```

服务端 TLS 需要真实 OpenSSL、证书和私钥。仅在本地开发时，可显式使用 `--insecure` 启动明文服务。

使用已有角色连接：

```bash
psql "host=localhost port=9999 dbname=info user=admin sslmode=require"
```

仓库也提供 [Dockerfile](Dockerfile) 和 [Compose 配置](docker-compose.yml)。使用 Compose 前需准备角色、访问配置与 `deploy/tls` 下的证书和私钥；数据库文件通过数据卷保存。

### 数据目录使用约定

- 一个数据目录只运行一个服务端进程；多个客户端通过该服务端连接。
- 不要让交互式命令行与服务端同时操作同一个数据目录。
- 数据目录包含数据库状态，请与源码和构建产物分开保存。更换程序或迁移环境前保留备份，并先在副本上验证兼容性。

## SQL 示例

在已连接的数据库中执行：

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    score INTEGER
);

INSERT INTO users VALUES (1, 'Alice', 85), (2, 'Bob', 72);

SELECT id, name, score
FROM users
WHERE score >= 80
ORDER BY id;

BEGIN;
UPDATE users SET score = 90 WHERE id = 1;
SAVEPOINT before_second_update;
UPDATE users SET score = 75 WHERE id = 2;
ROLLBACK TO SAVEPOINT before_second_update;
COMMIT;
```

`USE DATABASE` 等项目扩展命令需要显式启用 extended compatibility mode，不属于 PostgreSQL 标准接口。

## 测试

完整本地回归入口：

```bash
bash scripts/build_tests.sh
```

CMake 的 `check` 目标调用同一测试编排器：

```bash
cmake --build build/cmake --target check
```

运行单个 C++ 测试：

```bash
bash scripts/build_one_test.sh window_functions_test
```

以上命令均在仓库根目录执行。完整回归会编译生产程序，并运行原生测试和已注册的协议测试。

与 PostgreSQL 对比的协议测试需要可连接的参考实例；连接参数、参考版本及其他专项依赖以对应测试脚本为准。执行前检查测试配置，不要把已有的业务数据库用作参考实例或测试目标。

测试、示例和实验应使用独立的数据目录，不要指向正在使用的数据库。报告测试问题时，请附上运行命令、环境与相关输出，便于复现。

## 项目结构

```text
src/
├── main.cpp       # 命令行与服务启动入口
├── parser/        # SQL 解析、AST 与绑定
├── catalog/       # 类型与系统目录
├── commands/      # DDL、DML 与存储引擎接口
├── executor/      # 查询计划与执行算子
├── expression/    # 表达式与预绑定查询执行
├── storage/       # 页面、缓冲池与存储组件
├── access/        # 索引与访问方法
├── transaction/   # 事务与日志组件
└── network/       # PostgreSQL 协议与 TLS
tests/             # 原生测试与协议测试
scripts/           # 构建、测试与打包入口
cmake/             # 共享源码清单
docs/              # 使用、兼容性与开发文档
```

## 文档

- [文档索引](docs/README.md)：使用、开发、代码审查与历史记录的入口。
- [使用手册](docs/MANUAL.md)：SQL 与操作说明。
- [兼容性契约](docs/compatibility-contract.md)：SQL、协议及扩展模式的边界。
- [打包与部署](docs/PACKAGING.md)：源码包、数据目录和配置约定。
- [CHANGELOG](CHANGELOG.md)：版本变更记录。

## 参与贡献

欢迎通过 [Issue](https://github.com/Palpitate-xus/DBMS_C_plus_plus/issues) 报告问题、讨论使用与设计，或通过 [Pull Request](https://github.com/Palpitate-xus/DBMS_C_plus_plus/pulls) 提交改进。

提交问题时请提供：

- 使用的提交或版本、编译器和操作系统。
- 可在独立测试数据目录执行的最小 SQL 复现及启动参数。
- 预期与实际结果，以及相关错误信息。

不要附带真实数据库数据、私钥或凭据。

提交改动时：

- 从自己的分支提交修改，并在 Pull Request 中说明问题、改动与验证结果。
- 补充覆盖问题的回归测试，并说明实际运行的测试范围。
- 保持每个提交独立、易复查，避免混入无关修改。
- 新增生产源码时同步 `cmake/dbms_sources.txt`。
- 新增测试时接入对应测试入口。
- 更新受影响的使用文档，并在 CHANGELOG 中记录面向用户的变更。

## 许可证

仓库未提供独立的 `LICENSE` 文件。授权方式请向项目维护者确认。

## 致谢

- [hyrise/sql-parser](https://github.com/hyrise/sql-parser)
- [zcbenz/BPlusTree](https://github.com/zcbenz/BPlusTree)
- [Jefung/simple_DBMS](https://github.com/Jefung/simple_DBMS)
- [niteshkumartiwari/B-Plus-Tree](https://github.com/niteshkumartiwari/B-Plus-Tree)

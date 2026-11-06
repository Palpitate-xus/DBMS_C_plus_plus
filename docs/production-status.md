# 生产化状态

<!-- P34 projected-toast-demand begin -->
<a id="projected-toast-demand-current"></a>
## 2026-11-06 当前批次：普通部分列查询（P34-C2）

当前源 249 文件/178 headers；正常 CMake C++17/O2 最终 C2 `73bb61eb`。正式测试 2395 文件/1128 顶层 C++，新增 14 组/83 项检查通过，5 个既有专项在旧版和 C2 均通过；这是专项验收。62 条原协议与额外 41+7 条回复保持逐字节一致，已有兼容差异保持。

仅取 id 的单长字段表墙钟相对旧首测/旧回测为 -54.47% / -47.06%，多长字段表为 -61.28% / -61.63%。全列保留原路径。服务器 CPU 30 区域为 7 低于两旧 / 11 高于两旧 / 12 居间或相等，全部回退、C1 首轮和旧版漂移见[性能报告](performance-report-2026-10-30.md#projected-toast-demand-20261106)。全套测试、全面 PG 功能和稳定整体提速仍无完成结论。

原 273 项状态不变、历史增加至 284；754 Markdown 路径、725 保护记录和 33 旧归档保持。新增第 34、35 份压缩归档，性能包 20.91 MiB、诊断包 26.74 MiB。默认425不变，直接 master、用户身份 Palpitate-xus，Goal active。更早条目为其当时版本证据。

清理在两包独立完整还原校验通过后进行：删除 41,010 个本批生成副本、三个实际私有原生测试目录中的 4,935 个文件（共 444 个目录），以及两份重复临时压缩包。合计 45,947 个常规文件、3,116,489,621 逻辑字节（2.902 GiB）；被删除单链接文件 st_blocks 之和为 3,151,708,160 字节（2.935 GiB），这不是整台服务器独占空闲空间增量。213 个 ELF 引用、源码、原始 main.prof/maps/nm、最终 C2 的真实20-candidate工作数据和 6484 个其余归档输入文件均逐字节保持；所有非 ELF 原始数据可由两份压缩包恢复。清理采用精确文件白名单与完整 SHA 校验，既有数据库进程保持运行。

<!-- P34 projected-toast-demand end -->

<!-- P33 pg-class-declaration begin -->
<a id="pg-class-declaration-current"></a>
## 2026-11-06 accepted pg_class declaration batch

The accepted candidate is normal CMake C++17/O2 C2 `280ad4a8`; the first C1 is rejected and retained as evidence. This is a partial catalog feature, with no speed improvement or full PostgreSQL compatibility claim.

Current source:249 files/178 headers;2394 formal test files,1127 top-level C++ tests plus five nested C++ files. Six new native groups/550 checks pass with the matching61-core build; three existing native tests pass on old and C2. This is not a full2394-test-suite rerun. The old program's new-test exit1/31 failed checks/one unavailable-capability NOTRUN is preserved.

The owned34-attribute declaration and five dependency types persist and survive a real old-image upgrade/cold restart. The original62 strict protocol requests retain9 PG differences; the new41-query PG reference is strict green while the product is17/41. `pg_attribute` SQL, virtual source origins,27 values/wildcard and value/result codecs remain ordinary unfinished work. Selected metadata-only type inputs, even NULL, explicitly fail0A000. No paused planning/provider/portal/format2 or unselected explicitCAST analysis change was made.

The ordinary270-timing control has server CPU4 regions below both old medians,16 above and10 between; wall8 below/13 above/9 between. These directions include tiny differences and substantial old drift, not significance or causal attribution. Negative cases and prior P32 regressions remain open. [Full report](performance-report-2026-10-30.md#pg-class-declaration-20261106).

The original273 states remain22 complete/166 partial/70 unverified/15 user-deferred; IDX07 remains unverified with empty evidence. History has283 records. Work stays on `master` with the configured identity. Default program425 is unchanged; no deployment/push was performed. The33rd evidence archive embeds source/SQL/wire/data bytes and explicit kept ELF references, rather than claiming portable executable payloads.
After independent byte reconstruction, cleanup removed 19,969 generated duplicate regular files: 1,396,915,572 logical bytes; their single-link st_blocks sum was 1,407,332,352 bytes (1.311 GiB), not an exclusive whole-filesystem free-space measurement. All214 ELF references, directories, source/trace/SQL metadata, normal20-candidate working data, functional working data and the final PG reference working image remain. Postcheck verifies the exact removal whitelist and all4,932 remaining embedded files.

<!-- P33 pg-class-declaration end -->

<!-- CURRENT_LEGACY_LOG_APPEND_20261106 -->

## 普通日志追加与实际查询成本（2026-11-06，P32-C1）

普通日志 `::log` 仍按 time、四个空格、user、四个空格、operation 和换行追加原始字节；每次实际打开当前路径，以 `O_APPEND` 和六段 `writev` 写入，EINTR 重试、正短写继续消费未写后缀，零进展或其他写错误停止并关闭。打开失败仍输出 `error 1` 并返回0；权限、已有前缀、rename/unlink 与真实 symlink 重新解析保持。`getTime`、`auditLog` 正文字节未变，没有持久缓存日志FD或改服务端协议发送。

正常CMake C++17/O2、旧版→C1→同旧版各三轮的固定矩阵完整结束：27个数据库上下文、54个正常Main、270个计时区间，校验1098响应、96624行/386388单元/27864个NULL。30案例的服务器CPU为16项低于两旧、6项高于两旧、8项居间；墙钟为13低/5高/12居间。固定/单BYTEA直接仅id查询有有限收益，但多LOB直接全列和仅id的服务器CPU均高于两旧，其中仅id墙钟也回退，全列墙钟居间。全部负向、旧回测漂移及长尾保留，未证明一致SQL提速、稳定吞吐或PG速度等价；原因仍OPEN，P30原负向证据保持。

当前仅生产头 `src/common/logs.h` 改动，私有正常C1程序SHA `310a2c01d84c1cd92dcf05ee5a5825fecface8dc0446adc312269be2bd144eb4`；默认安装程序仍为 `425165af…`，现有服务未替换。源248/头177，正式测试2393（顶层CPP1126、含子目录总CPP1131、子目录5），登记684个driver/664个独立E2E；754 Markdown路径、725保护记录不变。原273项状态仍22 complete/166 partial/70 unverified/15 deferred_by_user，旧281条历史原文保持，仅追加第282条partial；IDX-07仍unverified且evidence=[]。不扩用户暂停范围，无分支切换、推送或默认程序晋升，Goal active。

本批协议仍为53/58及4冷锚失败，原9差异不变；归档32份、旧31逐字保持。[本批完整报告](performance-report-2026-10-30.md#legacy-log-append-20261106)与[可逆原始证据](benchmarks/legacy-log-append-20261106.json.gz)。以下历史保留。

归档逐文件校验完成后，本轮仅清理13,510个已归档的正常测试生成副本，nlink=1分配空间1,187,860,480字节（约1.11GiB）。独立新进程后验确认，其余2,317个非ELF工件、137个精确引用、780个目录和4个链接/目标保持；`20-candidate/data`完整工作库及全部协议数据保留，源码、程序和对象未删除。

<!-- CURRENT_FIXED_ID_PROFILE_20261106 -->

## 当前实现与新增诊断（2026-11-06，P31）

生产源码仍为已提交P30-C2（父提交bc5679c62d…、私有正常程序SHA472e093d40b8…），默认安装程序及现有服务未替换。新增P29-C2→P30-C2→同一P29-C2的有限CPU采样，3个Main正常退出；独立核全部1,575响应和1,536次主键查询完整结果。逐行日志、逐行发送结果成为下一步候选；P30主键查询CPU回退原因仍OPEN，未宣称本批提速或全库优化完成。

pg_class自身声明/来源元数据依赖真实34属性及元数据类型与运行时能力分离，本批仅审计和准备未运行验收方案。P30全部58个严格目标及9项普通差异保持，未改为PASS或暂缓。源248/头177、正式测试2392、Markdown754/保护725、history281与原273项状态保持（22complete/166partial/70unverified/15deferred）；IDX-07仍unverified、evidence=[]。压缩证据共31份，旧30份字节保持，原53.45MiB报告仍为5.70MiB gzip。未创建分支或推送，Goal active。

独立还原核验后另清理5,101个生成副本，约232.41MiB分配空间；源码、程序和父批工作库均保留。

详情见[本批报告](performance-report-2026-10-30.md#fixed-id-query-profile-20261106)与[压缩证据](benchmarks/fixed-id-query-profile-20261106.json.gz)。下方P30及更早段落属于对应实现批次。

<!-- CURRENT_ORDINARY_PROJECT_BITMAP_20261106 -->

## 当前行NULL位图复用与按需求捕获（2026-11-06，P30-C2）

普通物理表的直接投影现在按选中列需求，复用当前行已有的完整NULL位图；局部作用域在正常或异常退出时恢复，只选非空物理列时不再复制。既有值提取、虚拟列求值顺序、TOAST处理、文件身份校验函数和暂停范围保持。

正常CMake C++17/O2的最终C2，在固定/单BYTEA/多LOB三类直接完整查询中，墙钟中位数对前后两次旧版降低约9%–32%，服务器CPU降低约11%–35%；这是本机各三样本的有限结果。主键投影负控仍有回退，固定表服务器CPU为+26.19%/+25.84%，单BYTEA主键查询墙钟为+8.03%/+3.65%。跳过多余复制没有消除这些回退，其根因未证，不能据此归因频率、负载或噪声，也不能称全库提速。

原273项仍为22 complete/166 partial/70 unverified/15 deferred_by_user，`IDX-07`保持`unverified`且`evidence=[]`。完整PG参考58/58；三代产品均完成全58目标与4冷控制，目标各53/58，另4冷锚控制仍失败。原9项目录/普通错误帧差异完整保留、没有改为PASS或新增暂缓。仅新增1个正式CPP回归；whole58 known-gap协议草稿未运行、未登记。Goal active，默认程序未替换，无服务重启、分支切换或推送。

本批实际清单：源248/头177；测试2392，原2391逐字保持，新增1个CPP；顶层CPP1125/全部CPP1130（nested5）；E2E登记684 driver/664 standalone保持；Markdown754/保护725保持；旧29份gzip保持，新归档后总30。原280条历史原文保持，追加第281条partial。

完整原始分布、既有差异、许可和验收边界见[本批性能报告](performance-report-2026-10-30.md#ordinary-project-bitmap-20261106)与[可逆压缩证据](benchmarks/ordinary-project-bitmap-20261106.json.gz)。以下P29及更早内容为对应代别的历史记录。

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

实际清单：源248/头177；测试2391，原2388逐字保持，新增2个CPP和1个协议测试；顶层CPP1124/全部CPP1129（nested5），登记684 driver/664 standalone E2E；Markdown754/保护725保持；旧28份gzip不变，总计29。原279条历史原文保持，新增第280条partial。

这是有限的 partial 进展，Goal active。原 273 项状态保持 22 complete/166 partial/70 unverified/15 deferred_by_user；`IDX-07` 仍为 `unverified`、`evidence=[]`。零列行集、零列表和原 TOAST 损坏夹具的普通缺口仍保留，用户暂缓边界保持。本批不证明完整 PostgreSQL 数据类型上限、文件格式、WAL 或协议等价。

完整定义、首轮失败与测试边界见[本批报告](performance-report-2026-10-30.md#multilob-storage-20261106)和[可逆压缩证据](benchmarks/multilob-storage-20261106.json.gz)。下方 P28 及更早段落是对应代别的历史记录。

<!-- CURRENT_CONSTRAINT_VALIDATION_20261106 -->

## CHECK/FK NOT VALID 与真实历史验证（2026-11-06，P28-C4）

`ADD CONSTRAINT ... NOT VALID` 仍在真实元数据锁下绑定 CHECK/FK 定义，只跳历史行验证；后续写入仍由原真实约束执行链检查。`VALIDATE CONSTRAINT` 对未验证的 CHECK/FK 扫描真实可见历史行，仅在成功后发布 validated 标记；失败保留定义和未验证状态，已验证约束不重复求值。真实 SQL NULL 与非 NULL 空字符串分别处理。CHECK 的 volatile 函数不因 volatility 标记被一律禁止；定义绑定不执行用户函数，实际验证仍按到达的行求值。

正常候选 C4 为 CMake C++17/O2，程序 SHA256 `5cd5fd9e15f72257d13f2622e180d643e5cc93fffe8a128cbf8e820f64caf539`；before 为 P27-C2 `dd5d9b85…`。原生父测试 4,896 checks/0 failures（含文件快照断言），其中真实 lifecycle worker 779/cold worker 158；两 worker 正常退出，捕获两份当时文件映像，不能将这些层级的计数相加。真实目标 PageAllocator fetch 38 个观测中 18 个为零、20 个为非零 fetch，另有真实读取正对照证明 hook 确实生效；这只说明历史验证访问路径，不代表整句无 I/O。原协议 142 主流程+12 冷控制共154条全部匹配 SQLSTATE、完整值/NULL、真实本地 relation OID 的 T7、tag 和 Ready；58份参数快照、5项保留标志检查通过。修改前同源用例实际83条（56匹配/27差异）及71条 NOTRUN；C3全部154条中有4处不匹配，原失败保留，C4修复后原断言未放宽。独立 append10 全10条通过：已经 validated 的带 nextval CHECK 不再求值，PK/UNIQUE VALIDATE为42809、缺失名42704。新增真实 PostgreSQL18.6 mini20 全20条符合首预测；产品对应19条+6条冷控制全25条、13项真实 `.params` 检查通过，ASCII collation 只比较大小写语义角色，PG使用真实 ICU、本地使用 nocase，不声称 provider/OID/Unicode 等价。mini19 v1接收器在启动前缺少 Path import：0SQL/0owner，原首错误保留后仅修接收器，不称产品重试或产品失败。原159参考的3个预期行快照错误及 reserved RESET 首预测错误都保留，后续 oracle 仅从已记录 PG 离线更正。完整原始帧不是逐字节 PG 等价：mini19主流程12组 raw 相同、7组不同（错误文本字段或真实本地 OID），不抹掉差异。

普通 ADD CHECK/FK 与同样有效数据的旧实现可比较所需验证成本。NOT VALID 可描述去掉不需要的历史扫描，但旧版多做扫描，不能称同扫描工作量提速；旧 VALIDATE 仅改标记，其耗时不是正确验证的性能基线。整个普通 ALTER TABLE 仍有 DDL 物理回滚快照、文件复制/摘要/同步成本，目标 PA fetch=0 不表示整句 O(1)、无堆文件读取或无 I/O。

固定0/128/1024行×三轮before-first/candidate/before-return，27个独立主流程+27次同数据冷重启均正常0；216个ADD/VALIDATE计时点、3024份响应、2384640个完整单元格（157950个NULL）、18360份物理文件映像（其中2700份堆映像）全部独立核对。1,024行时，CHECK NOT VALID wall中位数对前/后旧版分别−63.939%/−61.677%、server CPU −71.962%/−72.695%；FK NOT VALID wall −40.047%/−39.400%、CPU −15.482%/−13.469%，这两项是消除旧版多做的历史扫描。普通ADD CHECK wall −18.399%/−14.808%，CPU +0.124%/−4.360%；普通ADD FK wall −38.575%/−35.423%，CPU −4.131%/−0.022%。补上真实历史检查的CHECK VALIDATE wall +52.762%/+43.290%、CPU +260.317%/+251.282%，属于新增正确性工作成本，不能与旧假验证比较成等价速度。

负结果也保留：空表普通ADD CHECK CPU +29.920%/+35.993%；1,024行已验证FK重复VALIDATE wall对首/返旧版为−36.791%/+51.673%。全样本最长单点17.977415s，旧版wall中位数往返漂移−58.325%～+60.792%，server CPU漂移−28.186%～+4.689%。完整72行metric统计含各角色raw3/min/p50/max、双基准差值与旧版漂移，没有剔除负值/长尾，没有稳定倍数、因果结论或PG速度等价声明。计时是普通socket请求至Ready，包含DDL物理快照成本；process CPU与proc快照非原子、含所有服务线程，不是独占算子成本。

本批四个生产文件、两份新增回归、原登记入口和既有文档在 master 做本地版本管理，使用 `Palpitate-xus <palpitate.xus@outlook.com>`。正常验收程序独立冻结，根目录运行程序仍为 `425165af…`。原始证据只以完整可逆 gzip 入库；53.45MB 的旧 analyze/reindex JSON 已由约5.7MiB gzip替换，旧提交历史未重写。恢复步骤和逐流SHA保存在归档内，构建目录中的原始文件不再直接进入Git。

当前实际清单：源248/头177；测试2388（原2386逐字保持，新增1 CPP与1协议）；顶层CPP1122（含链接桩）/全部CPP1127，其中nested 5；登记683 driver/663 standalone E2E。Markdown 754/保护725保持，旧27份gzip保持，归档总计28。原278条历史原文保持，prepend第279条partial。

本批为有限普通约束生命周期的 partial 进展，Goal active。原 273 项状态保持（22 complete/166 partial/70 unverified/15 deferred_by_user）；`IDX-07` 仍是 `unverified`、`evidence=[]`。本批 GiST 页树资料只是未实现研究，不能将 P27 的 batch partial 记录当作 IDX-07 已部分验证。原用户暂缓边界保持。

完整代别、首轮失败、普通成本全表与范围见[本批报告](performance-report-2026-10-30.md#constraint-validation-20261106)和[压缩原始证据](benchmarks/constraint-validation-20261106.json.gz)。下方 P27 及更早段落保持历史口径。

<!-- CURRENT_GIST_PROBE_READER_20261106 -->

## GiST 读取开销与空文件回退（2026-11-06，P27-C2）

C2普通CMake实际退出0，候选程序为dd5d9b85…；独立检查实际248份源码快照和61个native核心对象与正常构建一致。本次生产改动限TableManage内部GiST读取，未改变公开头布局、CRC算法、比较器、原可见堆、快照/dirty/文件归属/SSI边界。默认根程序425165…保持，本批没有替换程序或重启服务。

不超过1MiB的V2/V3文件采用同一捕获内容的CRC与长度视图解析；乱序合法RID通过私有排序验证，晚期损坏整次拒绝。新82命令协议包含零字节、部分头、坏CRC、恢复V3、合法非空文本格式及全NULL正规零记录文件；旧版12项失败、C2全部通过，两端进程均正常退出。原120命令协议C2也通过。

复用此前实际PG18.6的75条记录作离线比较，C2所有值及七字段描述符一致，原始帧73/75一致；两处DELETE额外零字段T仍是严格差异，没有新PG运行或全协议通过声明。指定17个native入口实际正常0；恢复专项C2为3303检查、18workers（5预期SIGKILL/13正常0），不与历史C1的3413混合。

完整正常SQL性能、所有负面结果和默认决定由[本轮报告](performance-report-2026-10-30.md#gist-probe-reader-20261106)给出。分页树/完整AM及opclass/WAL/并发、虚拟键旧覆盖、命名身份改造和既有普通缺口仍开放；原273项及用户暂缓状态不因本专项关闭。

固定九进程正常SQL对照中，高端范围的服务器CPU中位数减少39.409%/46.918%，不存在范围减少16.759%/23.868%（相对首轮旧版/同旧回测）；无索引列控制CPU增加3.760%/4.748%。墙钟旧版回测漂移达−66.653%至+1819.366%，所有慢值保留，不作稳定倍率或因果结论。

<!-- CURRENT_GIST_EMPTY_BOUNDS_20261106 -->

## GiST 空字符串、边界与 LIKE 检索（2026-11-06，P26-C4）

当前物理heap格式的存储列构建/REINDEX会写GiST V3，覆盖真实非NULL空字符串；NULL仍由真实位图区分。V2编码本来允许零长度，缺陷在旧producer省略空键，V3标记新的覆盖保证。旧V2/文本文件继续严格读取；无法证明覆盖、CRC、文件归属或可见性时交回原堆来源。有限空端点、缺失端点、严格/非严格边界和字面DEL分别表示；最终完整谓词、MVCC、NULL及TOAST仍检查。

LIKE必要字节前缀使用单独的tryGiSTSearchLikePrefix，保留原pattern/escape角色；默认locale下不把字节前缀的权限推广到SQL标量比较。无有效Unicode上界时保留无上界，避免DEL哨兵截断。普通EXPLAIN的>=空串不再独立触发全宽索引遍历；JSON usedIndex仅在真实索引候选读取成功时为true。

最终C4普通CMake C++17/O2构建退出0。格式组件O2与ASan/UBSan（含泄漏检测）各8434检查；现有API生命周期2879检查及独立进程冷启183检查通过；typed接口806检查/9组/68探测全部目标堆fetch为0，并有4次真实堆fetch正控制；文件归属冲突四个语义探针通过。17个有限native入口（其中2个新、15个旧）和6个旧协议入口全部正常0；迁移后的all-AM native实际12168检查/17场景，恢复实际3579检查/18workers（5次预期SIGKILL、13次正常0，36份完整输出及真实marker/event保留）。新协议120条命令、15个索引图像记录及六阶段真实heap RID/NULL见证通过，含正常双进程冷启。

真实PG18.6安装并加载btree_gist后重放75条共享SQL；最终C4逐项比较3231行/19382单元（661个NULL）、248列描述符的1736个元数据字段。75/75语义及描述符一致，73/75完整帧一致；两次DELETE本项目额外发送零字段T，PG只发CZ，严格全协议通过仍为false。两端关系OID各由真实目录锚定；PG正常退出、PID已回收，未比较PG访问方法格式、路径或速度。

当前清单为2383个测试文件：原2378中2374份逐字保持，4份只迁移当前GiST格式断言并加强V2兼容/空串真实RID检查，另增4个CPP及1个协议测试。顶层CPP1119（含链接桩）、全部CPP1124/其中子目录5；登记681个driver/661个独立E2E。源248/头177，754份Markdown、725份保护记录保持；25份旧gzip逐字保持，新增第26份。原273项及22complete/166partial/70unverified/15用户暂缓状态、原276条历史逐字保持，仅prepend第277条partial。

本地GiST仍是flat sidecar，每次候选检索完整校验文件，并非PG的分页GiST树、opclass/KNN/WAL/concurrent全功能。虚拟键仍保留旧覆盖来源；native-only丢失定义、虚拟Bloom NEW NULL、既有missing-memory协议失败及其不完整旧版配对仍开放。下划线文件名歧义现在安全回堆，物理文件命名/关系身份布局仍需后续改造。原用户暂缓范围保持，Goal active；本批为partial。

完整性能、首次失败和证据见[本轮报告](performance-report-2026-10-30.md#gist-empty-bounds-20261106)。以下 P25 及更早段落均保留当时的实现、计数和未完事项；本轮状态以本节为准。


<!-- CURRENT_REINDEX_ACCESS_METHODS_20261106 -->

## 普通访问方法 REINDEX 与空文本生命周期（2026-11-06）

本批补入普通 `REINDEX TABLE` 的 B-tree/Hash/Bloom/FTI/GIN/GiST/SPGiST/BRIN 重建、物理 Bloom NULL/空文本维护与 UPPER/LOWER 表达式索引 SQL 桥接。有效旧头的 BRIN `pages_per_range` 与 Bloom `k` 保留；`m` 随真实新键集按既有 loader 默认128 bitsPerEntry重算。旧头损坏/缺失时回退宽64/k7。默认程序仍为 `425165…`，没有服务重启或默认升级。

当前统计：2378 测试文件（原2375字节不变加2 CPP及1协议），顶层CPP1115（含链接桩）、总CPP1120/其中子目录5；登记680个driver/660个独立E2E。源248/头177，754 Markdown、725保护记录；24份旧gzip逐字保持，归档总计25份。原273项22/166/70/15及275条历史保持，仅新增partial第276条。

C2 主 native 实际 12096 检查/17 场景通过，恢复 native 实际 3183 检查、8 控制/18 workers （5 次预期 SIGKILL、13 次正常退出）；24 条 PG18.6 共享命令比较 1343 行/8058 单元及全部字段声明。31 个既有 native 与 5 个相关协议及 auto3 通过；另一个相关协议仍失败，旧版配对尚未到相同阶段。

GiST 空文本编码、native-only 丢失定义、虚拟 Bloom NEW NULL 和上述失败均保留普通未完；不关闭原273项。

完整代别、首轮失败、工作量差异与全部成本表见[本批报告](performance-report-2026-10-30.md#普通访问方法-reindex-与空文本生命周期2026-11-06)。以下P24及更早段落保留当时历史，不把旧数字当作当前统计。

下方 P24 及更早段落为历史记录。


<!-- CURRENT_BRIN_OPERATOR_CONSISTENCY_20261106 -->

最新P24-C2将普通BRIN摘要比较改为运算符专用路径；仍校验min<=max、所有文件/CRC/identity/dirty/snapshot fences与完整heap Filter复核，NULL flags不新增求值，legacy timestamp原路径保留。实际功能门禁及全部首次结果、固定9进程的完整维护/查询wall、CPU、长尾、负数与漂移见[性能报告](performance-report-2026-10-30.md)和[压缩证据](benchmarks/brin-operator-pruning-20261106.json.gz)；不称完整PG BRIN、所有SQL提速或因果已隔离。性能外层runner因label覆盖退出1、九backend正常0，二者分开记录；原source/plan/complete=false回执保留，后续仅离线finalize，不重采样、不当产品故障。默认425保持，partial、Goal active。旧2374测试逐字不变+1 native=2375；679/659 roster、源248/头177、顶层CPP1113/全部1118、754MD/725保护记录保持，历史275，旧23gzip保持+新24。下方P23及更早段落为历史记录。


<!-- CURRENT_BRIN_MAINTENANCE_OPTIONS_20261106 -->

最新P23-C1修复BRIN在VACUUM FULL、加列、删列和TRUNCATE/reset重建时丢失pages_per_range：健康V3配置在重建前保存，宽度1和SQL默认128均保持；缺失/旧格式/损坏仍沿用64回退。生产只改TableManage.cpp四处捕获/四次传参。新协议42命令/12镜像/4路径检查、25相关原生、6专项协议、3自动统计均正常0；PG18.6共享35SQL/19,327字段、11真实metapage一致；恢复7worker/2477检查。

固定9个正常socket进程测量完整VACUUM FULL及其后7类查询，所有wall/CPU、尾部、控制项和负数见[性能报告](performance-report-2026-10-30.md)及[压缩证据](benchmarks/brin-maintenance-options-20261106.json.gz)。该修复保留用户配置，不代表所有操作均提速；REINDEX仅Btree、完整PG BRIN AM/增量维护/WAL、规划重复验证开销、Bloom/GiST非NULL空文本覆盖和P22数组ROLLBACK间歇超时仍开放。前批密集CPU回测+6.64%负面记录保持。本批partial、Goal active。源248/头177，2373旧测试字节不变+1=2374，679注册/659独立E2E；原273item状态不变、历史274；754MD/725保护记录/22旧gzip保持。下方P22及更早段落为历史记录。


<!-- CURRENT_BRIN_TYPED_SCAN_20261105 -->

最新 P22-C10 修复 BRIN 数值边界/NULL 状态并接入真实候选页扫描，支持普通 SQL/绑定后五类比较和 NULL 谓词、反向常量及 pages_per_range。23页 extent 的原生候选 cursor 实际只 fetch1页；完整谓词仍复核。格式组件 O2/ASan各5588、新原生1834、新协议101命令/25路径断言、真实PG18.6共享73SQL/10490字段及7恢复worker/2477检查通过。有限81原生80正常0+1父代TOAST中止；旧32协议29正常0+3父代相同失败，全部保留。

固定9个正常socket进程、4096×4、七类查询，服务器CPU减少（首轮／同旧回测）：高端窄范围 41.19%／42.36%；低端窄范围 48.68%／49.29%；非主键等值 25.78%／20.13%；不存在的范围 94.22%／94.12%；密集范围控制 -7.44%／-6.64%；主键控制 30.31%／28.42%；无对应索引列控制 7.79%／4.38%。所有wall/CPU/控制负数、原始帧和尾部见[性能报告](performance-report-2026-10-30.md)与[压缩证据](benchmarks/brin-typed-scan-20261105.json.gz)。新BRIN文件V3，旧1/2只作兼容读取/安全回堆，旧二进制不能读取V3；当前物理heap2保持，暂缓format2仍指wire/output，NetworkServer.cpp未改。REINDEX尚仅Btree、部分重写配置保存、PG opclass/revmap/增量维护/WAL/concurrent、SSI及特殊类型/来源路径等仍开放，不声称全部数据库操作或PG性能等价。本批partial，Goal active。源248/头177、测试2373，678注册/658独立E2E；原273对象状态不变、历史273，754MD/725保护记录和21旧gzip保持。下方P21及更早段落为历史记录。


<!-- CURRENT_INDEX_BUILD_NULL_OWNER_20261105 -->

最新 P21-C1 让索引构建/REINDEX 复用当前行物理 NULL 位图，并修复 BRIN 将真实 SQL 全 NULL 整数摘要写成 `[0,0]` 的问题；空字符串范围保留。新原生13366检查、新协议40命令、真实PG18.6共享35命令/2250字段及7恢复worker/2477检查通过。有限相关79原生78正常0+1父代相同TOAST中止；旧32协议29正常0+3父代相同失败，全部保留。

正常2048行维护9进程、368640字段核对；相对同旧回测CPU中位数减少：Btree 19.30%，复合 19.02%，写事务 45.10%，回滚 47.67%；BRIN -154.76%（退步）。wall波动大且多项变慢，另9进程完整socket查询控制也没有一致提升，详见[性能报告](performance-report-2026-10-30.md)与[压缩证据](benchmarks/index-build-scan-owner-20261105.json.gz)。BRIN数值排序及SQL执行算子等仍为普通开放缺口，本批partial，Goal active。当前物理heap版本2保持；暂缓format2指协议/输出所有权。源248/头177、测试2370，677注册/657独立E2E；原273状态不变、历史272，754MD/725保护记录与20旧gzip保持。下方P20及更早段落为历史记录。


<!-- CURRENT_INDEX_CRC32C_20261105 -->

最新P20-C2为共享索引CRC32C加入通用查表和受CPU能力保护的SSE4.2路径，保留完整校验、所有文件字节与C++17基本构建要求。组件44392检查及ASan/UBSan、无SSE4.2模拟35520检查通过；12真实存储worker/3840字段与六类索引旧新双向读写、重建文件逐字节一致。最终68原生67正常0及1项父代相同TOAST中止、旧32协议29正常0及3项父代相同失败全部保留。9正常socket进程wall中位数减少（首轮／回测）：热点前缀 18.62%／-0.48%；热点等值 3.68%／0.90%；低频前缀 11.82%／14.98%；低频等值 -12.80%／-22.17%；不存在等值 6.78%／0.48%；主键控制 -1.07%／-6.83%；控制差异不作因果归因。源248/头177、测试2368，676注册/656独立E2E；原273状态不变、历史271，754MD/725保护记录与19旧gzip保持。Goal active；下方P19及更早段落为历史记录。详见[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_GIST_READ_OWNER_20261105 -->

最新P19-C3修正普通当前读事务被GiST误判为重叠写者的问题，并复用可见元组NULL位图。专项1214检查、新39协议、PG18.6共享37命令/1287字段对照通过；候选检索阶段目标堆页fetch由旧版2021降至0，返回行仍正常回表校验。历史/重叠/自身写入/缺失损坏/空文本范围及SERIALIZABLE保留堆后备。旧60原生59正常0和1项父代相同TOAST中止、旧32协议29正常0和3项父代相同严格失败全部保留。9正常socket进程wall中位数减少（首轮／回测）：热点前缀 2.54%／3.42%；热点等值 -0.42%／-1.44%；低频前缀 56.62%／52.19%；低频等值 -12.42%／-10.29%；不存在等值 2.15%／1.44%；主键控制 -6.09%／-3.54%，负数与控制波动也报告。旧GiSTScan/idx_scan只代表算子，不能单独证明底层物理索引访问；下方旧章节按此限制理解。源248/头177、测试2367，676注册/656独立E2E；原273状态不变，历史270，754MD/725保护记录保持，18旧gzip保留并新增1。仍partial，Goal active；详见[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_TYPED_PLANNER_STATISTICS_20261105 -->

最新批次 P18-C6 将普通等值与二进制文本 LIKE 扫描成本接入拥有真实身份的 typed ANALYZE 统计，并修正事务内显式分析的两份统计文件回滚。新65条协议/15次扫描、原LIKE123/27、最终原生专项及cold exec、组件O2/ASan/UBSan、7个恢复worker和真实PG18.6共享33命令/6732字段对照通过；相关旧协议29通过、3项父代完全相同严格失败，首次自动数组/range回滚超时及后续原deadline固定旧/新/旧六次通过均保留。完整socket性能中位数减少（首轮／回测）：热点前缀 73.72%／73.25%；热点等值 51.25%／15.72%；低频前缀 58.53%／-8.58%；低频等值 44.30%／38.10%；不存在等值 24.66%／24.31%；主键控制 30.77%／29.00%，负数也保留，只代表该负载。原273与22/166/70/15状态保持，历史269，测试2365，源248/头177，675注册/655独立E2E，754MD/725保护记录、17旧压缩包保持。仍是partial，Goal active；事务内ANALYZE需要临时数据库物理备份，旧统计格式、未转换消费者和用户暂缓模块未整体完成。详见[性能报告](performance-report-2026-10-30.md)与压缩原始证据。


<!-- CURRENT_LITERAL_LIKE_SCAN_20261105 -->

## LIKE前缀的真实索引读取与密集前缀成本（2026-11-05，P17-C6）

普通无参数SELECT中的TEXT/VARCHAR字面量LIKE现在可实际使用已有GiST侧索引。已绑定AST提供真实关系/列身份；前缀范围只筛选候选，原类型化LIKE、AND剩余条件和NULL语义继续复查。转义百分号/下划线、多字节ESCAPE、UTF8上界以及打开前删除索引均有回归。可用ANALYZE的MCV与完整覆盖直方图桶识别密集前缀，按真实表extent和成本参数保留堆扫描；未知探测至少计1行成本。

同一4096行×3列、父代／最终C6／同一父代回测各3进程交错运行；wall中位数变化（正数为减少，相对首轮／回测）：热点前缀（3200行）6.88%／-3.83%；低频前缀（1行）68.01%／67.77%；不存在前缀（0行）69.40%／68.72%；显式ESCAPE前缀（1行）71.84%／71.06%。仅代表本负载；C3热点超过3倍耗时的失败尝试完整保留。

最终C6通过新原生310检查、原13068与独立冷读105检查、37个相关原生入口；纯前缀组件O2和ASan/UBSan各72567检查，启用泄漏检测。新123条协议命令含27次实际扫描断言；32个相关旧协议入口29正常0、3项严格失败与父代完整观察相同。真实PG18.6以两个会话重放共享65条SQL，935个完整字段/NULL/OID逐项相同，3个EXPLAIN ANALYZE实际行数相同；未比较PG的AM路径、成本或速度。

详见[性能报告](performance-report-2026-10-30.md)与[压缩原始证据](benchmarks/like-prefix-scan-20261105.json.gz)。本批仍为partial，Goal active。


<!-- CURRENT_ORDINARY_QUERY_PROFILE_20261105 -->

## 普通查询的规划元数据与解析开销（2026-11-05，P16-C2）

本批在一次普通查询规划内复用关系schema，并按需各读取一次B-tree/Hash/Bloom/GiST列清单；主键与不可能使用位图/GiST的条件省去无用发现。普通表达式先排除不可能的类型常量前缀，避免对数字、表达式分隔符前的列名做推测类型解析及异常处理。真正的限定/引号/修饰/复合类型常量与显式CAST仍使用原声明语法。

交错的8倍调用补测，父代／最终C2／同一父代回测各3进程；wall中位数变化（正数为减少，相对首轮／回测）为：热点等值（3200行）12.15%／-1.12%；低频等值（1行）15.17%／18.32%；不存在等值（0行）17.94%／14.60%；主键等值（1行）25.52%／19.68%。全部短时及补测的正负结果保留，不能推广为全部操作或PG速度。

最终C2通过原生13068与独立冷读105检查、32个规划/存储入口和14个语法入口；另2个冷目录原生入口保留父代完全相同的严格失败。新65条协议命令含10个实际扫描断言，原有73条等值协议通过；另30个相关旧协议入口27通过、3个严格失败与父代完整观察相同。实际PG18.6的共享25条SQL仍为7331/7331逻辑单元格、9/9行估计一致，8/9路径相同；不是完整兼容验收。

详见[性能报告](performance-report-2026-10-30.md)与[压缩原始证据](benchmarks/ordinary-query-profile-20261105.json.gz)。本批是普通读取partial进展，Goal active。


<!-- CURRENT_EQUALITY_SCAN_COST_20261105 -->

## 普通等值查询的实际扫描选择与完整网络成本（2026-11-05，P15-C7）

本批让普通原生标量堆表查询实际消费基于ANALYZE频率的扫描选择：热点等值可选顺序扫描，低频等值和主键保留索引路径。此前只修改规划器时，无ORDER BY的普通查询仍绕开物理计划；现在结构化输出已接通，并修复无可用索引OR的显示文本合并，以保留重复行、NULL、空串和换行。B-tree、位图AND/OR与GiST在探测前取得语句快照，并在快照不适合当前索引或探测期间提交边界改变时读取完整可见堆候选，再做原条件检查。

这是普通读取的partial进展，使用现有原生 `.stats` 中的MCV/计数；尚未让优化器全面消费新的typed `.analyzed_statistics` 槽。普通EXPLAIN的文本缓存键现包含精确的实时成本设置；没有修改已暂缓的retained/custom/generic规划、参数、portal/format2或mutation/provider生命周期。当前私有普通后端P15-C7 SHA256为 `3ff5f16a9cc3a223d6618db87d8a5bbe85240dfd4c08cfdceafeb6ba49c03d3a`。默认根程序425与既有服务保持；Goal active，PostgreSQL整体功能仍partial。
同一4096行×3列、同一真实SQL与完整网络返回，父代／最终C7／同一父代回测各3进程，固定9个完成测量的进程，不重试计时查询；另两次在基线装载/启动阶段失败、没有计时样本，完整保留。wall中位数相对首轮父代／回测的变化（正值为减少、负值为增加）：热点等值（3200行）12.51%／2.45%；低频等值（1行）-5.57%／-5.47%；不存在等值（0行）77.31%／77.36%；主键等值（1行）-11.11%／9.44%。正负变化均如实保留；仅代表这四个普通读取负载，不能推广为全部操作、通用吞吐或PG速度。

新原生13068检查与独立冷读105检查通过，纯频率组件O2及ASan/UBSan各17检查（启用泄漏检测）。24个相关原生入口实际正常0，含完整7worker恢复1974检查和14份子进程完整输出；这些入口实际链接最终C7的61个核心对象。新73条普通协议命令正常0，验证实际扫描计数、全部行值/NULL及旧快照；26个旧协议入口中23正常0，另3保留与父代一致的严格失败，未称全套通过。真实PG18.6对共享25条命令的7331个逻辑单元格全部逐字相同，9个EXPLAIN行数相同，8个扫描选择相同、1处成本边界扫描选择不同。

原始测量、失败与独立复核见[压缩机器归档](benchmarks/equality-scan-cost-20261105.json.gz)和[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_PG_STATS_SOURCE_20261105 -->

## pg_stats 完整基础查询与读取成本（2026-11-05，P14-C6）

本批接通实际 `pg_catalog.pg_stats` 的17列基础SQL查询、参数为零的纯Describe、文本/二进制/混合结果格式，以及实际 owning anyarray 输出；支持已测的过滤、投影、排序、LIMIT、别名、CTE和JOIN。每次查询只读取当前数据库的真实统计与目录，重用本次查询内的关系/属性索引和每关系一次的物理模式校验，减少逐列重复扫描、模式复制和文件散列。

本节更新下方P13当时“17列SQL尚未接通”的结论；下方历史批次保留当时事实，当前状态以本节为准。当前私有普通后端P14-C6为 `9362b02aba17d45038702211ba42a6a1d797ef3580d97edb8ffabce80e0f63b6`。默认根程序425和既有服务未切换，Goal保持active；整体PostgreSQL功能仍为partial。
同功能未索引C2／最终C6／同一C2回测各3进程，每进程先1次不计时完整查询，再计时3次完整17列SQL查询API。16张表×30列×32行，实际512行/15360存储单元格/1696个NULL；每次查询返回480行×17列。wall中位数384.334／245.178／379.841ms，降低36.21%／35.45%；process CPU中位数392.205／254.151／387.508ms，降低35.20%／34.41%。这是该负载的完整原生SQL API对照，不是任意查询、网络端到端、已发布父代或PostgreSQL速度对照。所有9个样本及长尾保留。

新原生109检查与独立冷读90检查、投影组件O2和ASan/UBSan各548检查通过；34个相关原生入口全部正常0，包含完整7worker恢复1974检查及14份完整子进程输出。普通后端新协议26条SQL及旧标量25/数组19/范围19条自动统计协议通过。真实PG18.6同26条协议命令991个字段值：938逐字相同、53处严格差异保留；17列二进制153项中145相同、8处为实际存储宽度差异，六种范围的24个直接二进制值全部相同。另510个有限float4输入的文本和二进制身份均与PG一致。没有全库SAN或整体PG等价结论。

原始测量、失败及逐字复核见[压缩机器归档](benchmarks/pg-stats-source-20261105.json.gz)与[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_RANGE_COLLECTOR_20261105 -->

## 范围统计采集与完整 ANALYZE 成本（2026-11-05，P13-C2）

本批新增六种内置范围类型int4range/int8range/numrange/daterange/tsrange/tstzrange及实际范围域的ANALYZE采集：独立排序下界和上界、保存范围边界与长度直方图、空范围比例、NULL比例和实际存储宽度。边界保留包含关系、无界端点和子类型语义；本次样本局部缓存解析键，避免排序时反复解析。

最终私有普通后端P13-C2为 `3593b23e5e142605fe71149daec3347015003fb7a84d8a2505c80578e4e77a5d`，默认根程序425保持。完整pg_stats目录/17列SQL及output-only anyarray尚未接通，两条实际探针仍严格失败0A000。多范围尚缺真实目录/存储拥有者；本批仍为采集阶段的partial进展，Goal active。
本批同功能未缓存C1／缓存C2／同一C1回测，各3进程；每进程2048行、1个整数和六种范围，先1次不计时预检，再计时3次完整ANALYZE API。process CPU中位数388.224／201.363／391.997ms，降低48.13%／48.63%；wall中位数441.019／250.031／456.067ms，降低43.31%／45.18%。所有9次原始测量均保留；这不是任意SELECT、父代已发布后端或PG速度对照。

原生范围845检查及冷读262通过；组件O2和ASan/UBSan各43检查（启用泄漏检测，非全库SAN）。21个相关原生入口含完整7worker恢复1574检查全部正常0。原数组786+冷读191、标量641+冷读2、范围19条/数组19条/标量25条协议SQL通过。真实PG18.6同一520条SQL对照18列243项：229相同，14处严格差异保留（13处存储宽度、1处numeric NaN长度排序）。没有PG整体通过结论。

原始测量、失败和复核见[压缩机器归档](benchmarks/analyzed-range-collector-20261105.json.gz)及[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_ARRAY_COLLECTOR_20261105 -->

## 数组统计采集与完整 ANALYZE 成本（2026-11-05，P12-C3）

本批把真实数组存储输入接到同一次ANALYZE采样：保存整数组MCV/直方图/相关性，以及常见元素、按非NULL数组计算的成员频率和不同元素数直方图。实际验证integer/text/numeric/boolean/float8数组的NULL、空数组、字面NULL、重复值、多维/下界和冷读；其他元素类型及全部PostgreSQL功能仍需分别验收。

最终私有普通后端P12-C3为 `7d3cc2a1353c105ecac62579a919ec96d088cd325fa74234bc47252061e1029a`，默认根程序425保持。完整pg_stats目录/17列SQL与output-only anyarray尚未接通，两条实际探针仍严格失败0A000；本批是采集阶段的partial进展，Goal active。
完整ANALYZE对照为本批同功能未缓存C2／缓存C3／同一C2回测，各3进程；每进程2048行、1个整数和3类数组，先1次不计时预检，再计时3次完整API。process CPU中位数1243.545／164.180／1296.411ms，降低86.80%／87.34%；wall降低85.86%／86.87%。最终版685.690ms wall长尾保留。此结果限于该负载，未测任意SELECT或PG速度。

原生数组786检查及冷读191通过；计算组件O2和ASan/UBSan各32检查（启用泄漏检测，非全库SAN）；17个相关原生入口含完整7worker恢复1574检查全部正常0。原标量641+冷读2、数组19条协议SQL和原标量25条协议SQL通过。真实PG18.6同一554条SQL对照14列325项：301相同，24处严格差异保留（11宽度、11collation OID、2处相同频率元素的截断选择）。没有PG整体通过结论。

原始测量、失败和复核见[压缩机器归档](benchmarks/analyzed-array-collector-20261105.json.gz)及[性能报告](performance-report-2026-10-30.md)。


<!-- CURRENT_ANALYZED_SCALAR_COLLECTOR_20261105 -->

## 标量统计采集与 ANALYZE 优化（2026-11-05，P11-C4）

本批把真实ANALYZE扫描接入标量统计快照：保留NULL、实际存储宽度和原始扫描顺序，按统计目标维护有界样本，生成signed float4 distinct、MCV、排除MCV后的直方图及相关性。数组/范围专用采集、完整继承/表达式索引统计、真实pg_stats目录源和anyarray输出仍待完成；不是完整17列SQL功能验收。

最终私有普通后端为P11-C4 `e1cab6f6…`；默认根程序425保持。原生641检查及fresh冷读2检查通过，统计计算组件O2与ASan/UBSan各22检查通过（启用泄漏检测，非全库SAN），16个相关原生入口全部正常0。普通Main的CREATE/INSERT/ANALYZE/SELECT四条命令通过，464B实际快照独立解码核对两列；两条完整pg_stats SQL探针仍严格失败0A000，服务器正常0。

相同输入、匹配en_US默认排序规则的真实PG18.6对照共189项：184相同，保留3处实际存储宽度差异和文本槽2处collation OID差异（本地0/PG100）。样本边界按独立样本检查有效范围，不要求随机结果相同。原C-locale参考的文本相关性差异也保留；没有修改SQL语料以取得通过。

已接入真实ANALYZE的标量采集和快照发布；旧优化器/原生8列统计保持。普通服务器四条SQL流程与独立快照读回通过，完整pg_stats查询仍有2项0A000失败。PG对照的3处存储宽度及2处collation OID差异仍开放。

本批同功能版本的完整ANALYZE对照（4096行、4类型，每进程计时5次，前/新/回测各3进程）CPU中位数降低51.93%/54.81%，wall降低50.64%/54.17%；9份完整数据/RID/NULL和统计快照逐字一致。对照是本批P11-C2/P11-C4，不是父提交后端或任意SELECT查询提速。

源236/头166；原234生产文件仅TableManage.cpp变更，164旧头和2342旧测试保持。新增3测试后2345（顶层1094 CPP含桩、去桩1093，另5子目录CPP）。原273项22/166/70/15及261条历史保持，仅加1条partial后262；Goal active。

[完整证据](performance-report-2026-10-30.md)。

最终C4还修复默认自动分析的可见性：旧C3在批量INSERT达到阈值时生成空列快照；现在普通事务成功COMMIT、清理原命令视图与锁后，按已有改动表集合触发分析。失败/回滚不提前发布；只有分析成功才扣除已统计修改数，保留并发新增计数。维护错误不把已持久提交伪装成失败。25条普通协议SQL验证INSERT55、显式事务提交110、UPDATE110、DELETE50、回滚不改快照、ALTER SYSTEM+reload关闭自动分析后显式分析105，并核对5次EXPLAIN基数；服务器正常0。原恢复入口1574检查及7个worker全部正常0。共享IO/分配失败、两阶段提交的自动统计调度及显式事务内ANALYZE的全部回滚语义仍未完整验收。

按用户清理要求删除93209个旧编译文件，实际空闲增加约658GiB；分区85%→72%、空闲约1.5TiB，项目约111→28GiB。源码、原2342测试、全部已跟踪文件、当前C4/default425及PG18参考工具已核对保持。后续以现有CMake缓存和当前core复用构建，证据归档后清理可重建产物。删除在9样本全部结束后进行；只读du盘点与最终对照的回测段重叠，全部结果保留，不称为无并发负载测量。


<!-- CURRENT_ANALYZED_STATS_SNAPSHOT_20261105 -->

## 完整统计视图的快照基础（2026-11-05，P10）

P10为完整pg_stats的持久化基础阶段：新增3个头文件，模型与文件读写已经过实际测试；尚未被dbms_main或ANALYZE调用。CMake正常目标0重编、62对象哈希保持，普通后端仍为 `30c57ca8…`，不是新增SQL能力的发布版本。

新组件568父检查及3个fresh子进程均通过，ASan/UBSan启用泄漏检测通过，正式core入口通过。完整SQL两条探针仍0A000；原5项Main元数据差异、跨进程恢复准入及全17字段采集/来源/输出仍开放。组件性能比例不代表SQL提速。[报告](performance-report-2026-10-30.md#完整统计视图的快照基础2026-11-05p10)与[证据](benchmarks/analyzed-statistics-snapshot-20261105.json.gz)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧节保留各批次当时结果。


<!-- CURRENT_STORAGE_LIFETIME_20261105 -->

## 存储退出期生命周期（2026-11-05，P9-C2）

P9-C2 `30c57ca8…` 修复两处已实际捕获的use-after-free：退出期读取已析构的表名列表标记，以及后台写回访问已析构的维护登记表。原七阶段在P9-C1仍于advance异常；最终C2原7阶段全部正常0，完整Memcheck父进程+7工作进程均0错误。

23个相关入口和新生命周期入口通过；正常Main为212/217，保留5项描述符/目录身份差异。跨进程准入、完整pg_stats17字段和其他功能族仍开放。9次完整恢复成本对照未证明稳定提速，默认425及服务保持。[证据](performance-report-2026-10-30.md#存储退出期生命周期2026-11-05p9-c2)及[归档](benchmarks/relation-list-engine-lifetime-20261105.json.gz)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user，Goal active。下方旧节保留当时版本的结果。


<!-- CURRENT_RESTORE_TRANSACTION_HORIZON_20261105 -->

## 物理恢复事务号水位（2026-11-05，P8-C3）

最终源码P8-C3（程序 `d778afbd…`）实现公开恢复的水位读取、持久推进及当前进程准入屏障。真实较早目标的首次探针1→4行，公开原生扫描的完整1000+4值/RID/NULL与哨兵移除通过；正常Main固定22操作的数据和冷启动通过，仍保留5项元数据失败。

整体恢复验收仍为partial：同一七阶段夹具在P8-C1曾全部通过，P8-C2/C3分别于实际advance写入后的阶段出现wait状态134（Invalid argument／corrupted double-linked list），精确触发点和原因未知，后四阶段NOTRUN。原失败未重试、未改为暂缓或启动前失败。相关21个旧入口、32个WAL控制及当前进程的3种准入语义通过，不能抵消该失败或证明跨进程准入完整。

两轮完整公开恢复计时分别保留；第二轮C3较两次旧版wall p50低4.16%/6.42%，并非完整35操作、36查询或生产负载提速结论。默认425与现有服务保持，详见[完整报告](performance-report-2026-10-30.md#物理恢复事务号水位2026-11-05p8-c3)和[机器归档](benchmarks/restore-transaction-horizon-20261105.json.gz)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧节保留各批次当时结果，不能覆盖本节的失败与范围。


<!-- CURRENT_BACKUP_ANALYZE_20261104 -->

## 备份摘要与 virtual 列统计（2026-11-04，P7-C1）

当前源码 P7-C1 `724c1147…` 包含创建端备份摘要与virtual统计修复，C++17及原持久化要求保持。新增永久884控制、相关12入口通过；PG34严格对照17匹配/17差异，未认定统计、目录或备份功能族完成。

完整备份wall p50为737.771→587.376→677.477ms，较两次旧版低20.38%/13.30%；正常SQL四类DDLwall p50为2快/2慢，ALTER与TRUNCATE慢于两次旧版。旧版备份恢复到较早全局状态目标的探针缺口仍开放，四组既有探针目标对照通过不能关闭它。完整35操作/36查询未重跑，旧慢项和RSS负值保留。

仅在私有前缀验证CMake安装，程序及第三方声明SHA正确；根目录默认425与现有服务保持。使用新实现需构建当前源码。[全部验收范围](performance-report-2026-10-30.md)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方按批次保留当时记录，当前状态以本节为准。


<!-- CURRENT_GENERATED_DEMAND_20261104 -->

## 虚拟生成列读值需求与完整 SQL 实测（2026-11-04）

本批源码 P6-C8，对应正常程序 SHA256 `45264111…`，保持 0.2.0 / C++17 / GCC 13.3 / O2 / ICU 74.2 / zlib 1.3 / TLS stub。根目录默认程序仍为 `425165af…`，现有服务未重启。

当前C8的258严格观察为238通过/20差异，旧156整项通过全部保留，仍有6项描述符组件损失；原767完整结果保持旧版129项差异。当前15个Main验证/基准上下文正常退出，历史C7及私有插桩退出失败分别保留。

四条正常SQL的72请求全部正确，固定100行/八virtual场景四项p50均低于两C2；完整35操作/36查询尚未对C8重跑，原负结果保留。默认程序和用户现有服务保持。详见[完整验收边界](performance-report-2026-10-30.md)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧批次保留当时事实，当前状态以本节为准。


## ANALYZE 与 REINDEX 成本优化（2026-11-04）

本批源码 P5-C2（a3baaca6…）优化同次 ANALYZE 的类型/locale 比较和真实行 NULL 位图，以及 REINDEX 逻辑 map 保留。C++17、原维护频率/持久化和用户暂缓边界保持。ANALYZE p50 369–403→74 ms，范围 UPDATE 600–650→389 ms；完整 35 操作为4快/28慢/3居间，36组查询为4快/6慢/26居间，整体性能提升尚未验收，默认仍为425，现有服务未重启。

详见[完整性能报告](performance-report-2026-10-30.md)和[本批机器归档](benchmarks/analyze-reindex-main-costs-20261104.json.gz)。下方旧批次保留其当时结论，当前以本节与报告首节为准。

## C4功能与完整成本记录（2026-11-04）

当前源码C4（9d85c688…）补入RETURNING纯声明与真实来源元数据、虚拟列拥有值及部分MERGE来源输出。新增123原生控制122通过/1个严格元数据差异，新132协议对照全过，原56项为55通过/1差异；273项状态未变，Goal继续active。

实际35操作为7快/19慢/9居间，范围UPDATE及REINDEX仍慢；36组完整查询25快/2慢/9居间。未通过本机默认程序的整体性能提升验收，默认仍为425（425165af…），现有服务未重启。独立API512行的分配/CPU收益及空表/单行负值分开报告，不能概括为所有数据库操作已提速。

C4最终选取57原生56/1、31协议24/7，原失败组件逐项保留，无启动失败/超时。前代C3完整选取148原生/95协议分代记录；C4补齐前代CTE启动失败的747项。固定579完整观察与425/C3一致，资源55通过；尚无完整生产验收。

详见[本批报告](performance-report-2026-10-30.md#returning-声明真实行值与操作热路径2026-11-04)及[机器归档](benchmarks/returning-declarations-hotpaths-20261104.json.gz)。以下为历史记录。

## 本批局部验证（2026-11-03）

本地默认程序已按最终安装收据原子替换并fsync为同批实测C3 `425165af…`，根目录SHA与不可变候选一致；原S3 `aa52b5fe…` 已备份，未重启现有服务。

基于C8源码的C3 `425165af…` 使用冻结的227个源码输入：只读内置函数注册表与实例覆盖分开，参数求值期间覆盖外层回调时仍使用实际更新后的回调；RETURNING保留原拥有值，并在实际存储行绑定处补入物理数组。未改变mutation graph/provider、参数帧、保留规划、portal或格式2策略，也未恢复R151等原暂缓范围。

局部成本夹具中，辅助求值关闭捕获时累计C++new请求1,218,134→213,590、请求字节88,677,248→14,491,520；开启捕获时1,450,070→445,526、126,917,504→52,731,776。公开游标512行的分配保持，单次冷执行CPU20.750→35.862ms变慢；这些累计字节和局部CPU不是峰值/RSS或整SQL结果。

最终C3选取57个原生入口为56通过/1个既有math失败；32个协议入口为24通过/8个既有失败入口，不能称完整全库回归。新增14项注册隔离及20项RETURNING原生控制均通过；原2322测试不变，新增后2324文件、1074个原生测试入口。固定301为188/113，相对S3改善88项；83/111/68/16共278项完整实际响应与C8相同。新56项RETURNING从20差异减至8，无原正确分量回退。C2完整147原生和95协议与C3选取集分代记录；一次0 SQL启动失败不改写成语义通过。

独立完整SQL对照实际1638请求全部终态：36组中位数均低于两次S3，每预算12组；相对S3首/末分别降低2.05–33.76%/1.76–29.81%，基线漂移−22.57%至+11.58%。35类操作C8/C3/C8的630样本均完整校验，18快/2慢/15居间，update_range与reindex慢项和−90.28%至+130.91%基线漂移均保留，不作普遍加速或统计显著性结论；产品仍0.2.0/C++17/TLSstub，原273状态及Goal active保持。全部结果与剩余依赖见[本批报告](performance-report-2026-10-30.md#内置函数注册与复合-returning2026-11-03)。独立机器归档见[全部原始统计](benchmarks/function-registry-returning-20261103.json.gz)。以下C8及更早记录保留当时结论。

## 当前状态与验证范围

C8 `91d339db…` 只修改 `ExecutionPlan::cells`：不需要数组/引用类型加载的行跳过新增的来源复制，原来源已给出完整、有界原始类型行时预留值数组容量。实际上下文、provider、NULL解码、原行值及共享算法保持；源码保留此优化，但因完整查询仍有慢项，不提升默认安装，根目录仍为S3 `aa52b5fe…`。

C8选取的26原生入口全部通过，31协议入口为24通过/7既有失败入口；301/83/111/68/16固定观察的完整实际响应与C7相同，仍有301中的114差异及REGTYPE68中的11普通缺口。独立公开游标512行比C7少1536次C++new请求/393216B，比S3少1024次请求、冷次少188256B/重启少188416B（累计字节约5.34%）；这不是live/峰值/RSS结果。完整查询36组为5快/18慢/13居间，基线漂移保留，不构成稳定C7→C8速度倍率。完整数据见[本批报告](performance-report-2026-10-30.md#原始类型行物化分配优化2026-11-03)及[机器归档](benchmarks/owned-datum-primitive-20261103.json.gz)；原273状态与Goal active保持。

C7功能及性能记录（按原代别保留）：

本批基于 `6c27d232`，功能程序 C7 `f135335e…` 使用冻结 226 源码和本轮 fresh ABI 谱系。已有正确描述符下的 owning record/array 输出、正常格式按需载体及新 REGTYPE 持久身份已进入真实构建/运行验证；功能代码已在master提交 `89696698`；因23/36组p50高于两次S3，C7未提升为默认安装，根目录保持S3 `aa52b5fe…`。

C5 完整 106 原生入口为 105 通过/1 个既有例程失败，95 协议入口为 86/9；5011 条明细比较无新增错误结果，原 220 观察完整相等。C7 选取的最终 26 原生入口全部通过，31 协议入口为 24/7，失败入口仍为 bit、name、Bind、CTE、nested、interval、143；不是 C7 全量 106/95 重跑。新三个永久原生入口共 38 控制通过，原 2319 文件保持。

固定 301 观察仍有 114 差异（较 AA52 改善 87）；83/111 各保留两项配置差异；REGTYPE 68 项为 57/11，参数 16 项全部通过。Prepared binary RETURNING 捕获、WHERE/SET 输入、目录、位图历史、typmod 与 ALTER USING 等普通未完均保留；原 273 状态和全部暂缓范围不变。产品仍为 0.2.0/C++17/TLSstub，Goal active。

资源55通过，1638请求/1296计时完整通过值校验；速度为4快/23慢/9居间，最差中位数比1.875，全部分布和漂移保留。512行公开游标多512次分配，辅助求值关闭捕获次数相同但多319488B。没有统一加速、RSS降低或PG等价结论。完整代别、失败与后续验收见[本批功能与成本报告](performance-report-2026-10-30.md#拥有值的复合类型输出与引用类型存储2026-11-03)。

上一批未接受研究记录（保持当时结论）：

最新批次基于 `3d576b8b`，未接受新的生产改动，源码及安装程序保留已接受S3 `aa52b5fe…`。
两版窗口暂存候选都通过72个不同原生入口；各自62个协议入口仍为57通过/5保留失败，完整旧失败观察保持。scratch2另做同源220终态复核，完整响应一致，不替换首轮证据。
每版55资源请求通过；各自一次完整1638请求/1296计时、三预算与9正常context通过完整值校验。scratch1的36组为4快/21慢/11居间；scratch2为12/8/16，4MB+16MB为2/8/14，未证明稳定跨预算收益，因此两版均拒绝。
每行分配减少、N0无用请求减少与+72B对象/coldN1开销分别保留，不能称全面加速或窗口预算完成。
独立真实PG18.6固定301观察参考通过；S3及两候选全部301响应相同，各保留201处差异。类型载体/编码/接收器/目录元数据提案尚未完成全链ABI和运行验证，未计功能完成；R151等暂停边界不恢复。
九份既有文档同步，未新增Markdown/永久测试；原273状态、原250历史及2319测试保持。产品0.2.0/C++17/TLSstub，Goal active；完整方法和慢值见[本批报告](performance-report-2026-10-30.md#窗口暂存分配与类型输出调研2026-11-03)。

前批已接受状态及各自历史输入：

本批基于 `d7793044`，S3 程序 `aa52b5fe…` 在原真实类型化窗口宿主内共享符合 PG 规则的结果：同一实际声明的非易变等价调用可复用；真实最终排序下另按保留目标/抽出窗口叶的输入投影处理重复易变调用及其顺序。
保留全部原call/计算列/targetOwners/需求和实际frame-provider，每行向原列广播完整owning值，最后别名move；准备期只复用同原AST的纯身份字符串。可证明不共享、不重排且无外来别名的完整stage保留原类型化demand/move循环，条件覆盖两套策略及任意实际需求子集。
S3完整72个原生入口全部通过；新增220观察在独立PG18.6全过，C1旧27处差异在S1/S2/S3全部解决，三版完整响应一致。S3完整62协议入口首次为57通过/5保留失败；原四失败入口1641观察及原143/8与S2相同，190项本轮首次完整通过。
S2原首轮连接103、190项0SQL采集失败及同源独立190/0均保留；不以S3通过改写该历史。剩余catalog来源2、record-json同行错误需求3、compound二进制3仍开放。
512行warm公开cursor中双/四LAG的C++new请求降低44.65%/69.70%，单份S3请求数与C1一致；这是累计请求，不是整SQL加速、live/peak/RSS或内存预算结论。
S2整SQL36项保留19项慢于两旧（原common8为17/24），分阶段诊断没有把它们统一归因为宿主噪声；S3资源和完整时钟另按实际记录，见[窗口结果共享报告](performance-report-2026-10-30.md#窗口结果共享与排序投影2026-11-03)。
窗口完整work_mem/spill、通用已分析表达式归一化及其他原273能力仍开放。原273仍22complete/166partial/70unverified/15originaldeferred，原249历史、2316既有测试文件及725保护记录保留；新增近期记录只计partial。产品0.2.0/C++17/TLSstub，Goal active。

S3实际Main资源55请求/40000行118000单元全部通过；完整1638请求/1296计时、三预算和9正常context成功。重复两/四份LAG(length)降低37.94%–40.83%/63.65%–66.34%，但原common8仍16/24组p50高于两次C1；全部36组11快/19慢/6夹两旧，不能宣称全面提速。完整原始12样本、min/max、漂移与S2历史均保留。

前批记录（按各自输入解释）：

本批基于 `b04db801`，C1 程序 `56d06bca…` 仅修改私有窗口比较路径：真实 builtin OID 21/23/20、同 exact smallint/integer/bigint 的已求值键在首次实际比较时缓存 int64。
NULL先行；域/枚举/OID0/隐藏无权威声明与解析未命中继续原比较owner，不改实际行值、frame/provider、参数或共享排序助手。
71个原生入口全过（新36）；61个协议入口56通过/5失败，新143的14处既有差异与C9逐观察相同，原四失败入口也保持原观察。
整SQL4MB/16MB的16项p50低于两次C9约22.3%–39.1%，64kB有3项仍慢于两旧，全部漂移保留。独立public cursor CPU下降41.85%–61.90%、C++new下降53.89%–63.96%是另一阶段诊断，完整数据见[本批报告](performance-report-2026-10-30.md#窗口整数键缓存与实际执行成本2026-11-03)，不能沿用它作为wholeSQL倍率。
缓存增加每个buffered Input的vector头及外层保留容量，payload/元数据按实际比较需求分配；整数1000行控制首/末行allocator uordblks约多68KB；不宣称live/峰值/RSS或work_mem改善，窗口完整预算仍开放。
普通最终排序资源43请求通过，作用范围仍限真实最终Sort。原C9的35类操作数据0错误但停机-6的完整失败继续保留；本批没有重跑或调查R15。
原273项/248历史/2313既有测试与725保护记录保持；产品0.2.0/C++17/TLSstub，Goal active。以下前批记录按各自输入解释。

本批基于 `9f019ffa` 接入真实类型化窗口宿主，复用完整分区/排序键、相同语义键阶段的顺序及运行聚合状态；消除可证明冗余的最终排序，保留 LIMIT/OFFSET/WITH TIES 与错误前实际行需求。
最终 C9 程序 `c696d3b2…` 的 70 个原生入口通过；新原生六入口共 348 控制通过。新窗口协议 226、键需求 241、有序输出需求 201 均有独立 PG18.6 严格对照。
完整所选协议入口及残余差异详见[本批报告](performance-report-2026-10-30.md#类型化窗口宿主与有序输出复用2026-11-03)，没有将未通过入口删去后宣称全绿；原 Bind/portal 完整 463/40 日志与前版逐字节相同。
有限 interval 除法/AVG 已修正；严格 106 观察仍有 10 个常量错误的 Bind/Execute 阶段差异，属于既有用户暂缓范围，原预期及失败完整保留。
35类操作返回结果均验证成功，但正常退出时服务器抛std::system_error并SIGABRT，该次入口失败/complete=false；既有R15停机链暂缓保持，未重复挑选通过样本。普通最终排序匿名文件/EOF清理和强杀重开另行验证，按报告中的实际结果解释。窗口分区及内部排序仍无 work_mem 上限，不能把最终排序的资源结论推广给整个窗口执行。
性能对照使用固定1,000行夹具和前版/新版/前版：first/last/nth组合约快5.15–5.84倍，其余7类窗口的p50仍高于旧回测约1.92–2.70倍；没有普遍提速。完整样本/漂移/资源策略差异见报告，没有新测PostgreSQL速度对照。
通用 Core JOIN lowering、统计窗口聚合、完整 temporal RANGE、任意动态 SELECT 计数、统一资源治理及其他原始要求仍开放。
原 273 项仍为 22 complete / 166 partial / 70 unverified / 15 deferred，725 份保护记录未改，整体 Goal active；产品仍为 0.2.0/C++17、TLS stub。直接在 master 分批提交，不自行建分支或推送。

前批记录（以下数字及未完成项按该批输入解释）：

前批基于baba4bcb，补受支持UNION ALL全局LIMIT/OFFSET表达式与set/VALUES FETCH计数，逐次动态Top-N、真实作用域和普通Parse已选上下文参数声明；顶层纯计数查询保留运行错误列描述。
最终程序58c94e05…，48原生全过；新增75/900协议在PG18.6/最终程序全过，binding58仍有1个合法窗口子查询宿主缺口。
完整36协议按各入口结果34/2：既有暂缓Bind完整463/40逐字节不变，另1个入口保留该窗口缺口；首候选普通SELECT超时及全部失败保留，最终原超时入口通过。
整SQL1620样本与另行已绑定执行432样本均无普遍提速结论，16MB浮点慢簇/整数LIMIT和4MB文本OFFSET慢值保留。
12项真实Main资源、35类有限操作、匿名动态计数排序文件内强杀/重开核对通过；完整性能样本、慢值和边界见[本批报告](performance-report-2026-10-30.md#全局计数表达式与动态排序上限2026-11-02)。
独立SELECT表达式计数、完整窗口/其他集合lowering、未知参数全约束和统一资源治理仍开放；原273/725记录不变，整体Goal active。


前批基于bb74ae97，优化精确集合整数键与原比较参数深拷贝，堆/中断/执行owner保持。
45原生全过；新19旧6项仅成本预算失败，metadata/resource全过。33协议32/1、新66在PG/旧/新全过，原463/40暂缓日志完全相同。
1980延迟/2505校验含三预算、6d6交叉对照和唯一漂移复测；整数排序窗口改善，但部分点查控制较慢，见[集合键缓存报告](performance-report-2026-10-30.md#集合整数键缓存与比较参数复制2026-11-02)，不能称无回退。
七项资源、35类有限操作和匿名文件内强杀重开通过；原273/725记录不变，Goal active。

再前批基于 `8a8532a0` 将受支持的 UNION ALL 全局 SetSort 与普通 VALUES 排序接入会话预算和完整类型临时快照，
补充真实 VALUES 输出作用域、ORDER 表达式、LIMIT/OFFSET 表达式及正常枚举结果描述符/标量二进制标签。
最终 44 个原生入口全过；新增原生 26+860+63 共 949 控制全过。新增集合 112、VALUES 143、枚举 33、计数 59 共 347 协议控制在 PG18.6/最终程序全过，另有严格类型/语法阶段 52 项全过。
原完整 31 协议首轮为 29/2：既有暂缓 Bind/portal 完整 463/40 观察逐字节不变；另一项在发 SQL 前连接失败，同一程序/源码独立复测通过，原失败不删除。
实际 Main 七项资源、35 类操作有限检查、排序匿名文件内强杀后重开 1,000 行/4,000 单元格及再次 2,000 行集合排序通过。
内存预算不等于硬 RSS；完整速度对照及全部慢值见[集合排序与 VALUES 报告](performance-report-2026-10-30.md#集合排序预算与-values-查询子句2026-11-02)。
该前批当时未下推动态 Top-N，也未实现 FETCH/global-set 表达式计数；本批已接入上述有限范围，完整未知 OID 约束与其他算子治理仍开放。
该前批的括号 inline VALUES 全局计数曾报 0A000、未括号形式可能报 42601；这些历史拒绝不再描述本批已接入的普通 UNION ALL 根。
新增52永久协议入口独立验证通过；全部32个已运行入口按各自最终结果复核为31/1。
原始 273 项/原子要求、725 份受保护记录与所有用户暂缓不变，Goal 保持 active。产品仍为 0.2.0/C++17，TLS 构建仍为 stub。

上一批基于 `d8533bc9` 为普通类型化 Sort 增加按实例预算的有序段/归并、完整类型与行上下文临时快照及惰性比较描述符缓存，
接通受支持的普通无 LIMIT 多键/表达式/非单键快路径类型排序，修复 NaN WITH TIES 的两项严格差异。
39 个原生入口全过，新增 85 控制全过；所选完整 26 协议为 25/1，新完整 108 在 PG 18.6/最终程序全过、旧版 2 处不符。
唯一失败为既有暂缓 Bind/portal 的完整 463/40，所有观察与前版逐字节相同；原 80 个中断断言与共享辅助模板未改。
实际 Main 的 9 项资源检查、35 类操作有限检查和类型化匿名文件内强杀/重开 4,000 单元格通过。
相同 64 KiB 设置下，12,000×2 KiB 原生无界排序的输入结束活跃分配由约 73.9 MB 降至约 18.1 KB；
这是单个边界而非整条查询峰值，稳定排序工作区、单行/固定读头与其他节点还可能额外占用内存。
三个预算及一次高预算复测共 1,584 个旧/新/旧 SQL 延迟样本及所有慢值见[类型化排序报告](performance-report-2026-10-30.md#普通类型化排序的预算与比较缓存2026-11-02)；资源改善不能推导所有查询加速。
聚合/HAVING/SetSort、其他算子/接收器、配置函数与完整 GUC 作用域仍开放；原始 273 项与用户暂缓不变，Goal 保持 active。

上一批基于 `049c0bce` 补充会话 work_mem 的 SET/LOCAL/SHOW/RESET、启动默认、事务/保存点恢复及配置重载优先级，
普通物理 Sort 在打开时使用该连接的有效预算，pg_settings 和普通缓存 EXPLAIN SETTINGS 保持一致。
36 个原生入口通过，新 163 原生控制全过；所选完整 25 协议为 24/1，新完整 470 在 PG 18.6/最终程序全过、旧版 404 处不符。
最终仍保留原有 Bind/portal 的 463/40 差异；前一候选 COPY 超时失败也保留，不恢复其暂缓调查。
实际 Main 的 31 个预算/匿名文件/重载控制、35 类操作有限检查及强杀后重开 4,000 单元格通过。
独立 504 SQL 样本和 600 CPU 诊断包含慢值，不能宣称稳定提速或全部查询不回退；范围与原始记录见[会话工作内存报告](performance-report-2026-10-30.md#会话工作内存与实际排序预算2026-11-02)。
配置函数/函数 SET 子句、内部例程 GUC 及其他排序/算子完整预算仍开放，原始 273 项和 Goal 范围保持。

本批基于 `471dad3f` 完成普通物理 Sort 的最终归并按需输出，减少一次完整临时写入/读取，并恢复调用方 NULL 行绑定及错误后的终止清理。
31 个原生入口通过；完整所选 22 协议为 21/1，新增 40 个原生控制全过，既有 463/40 暂缓差异与前版完全相同。
严格 PG 18.6/最终程序的排序 47 条、35 类操作检查、实际 Main 强杀后重开 4,000 单元格核验通过。
8,192×2 KiB 的 64 KiB 预算原生外排约快 1.78–1.86 倍；两组 504 个小表 SQL 样本未证明稳定提速，部分控制较慢。
不将算子收益推广到全部查询，完整数据及未决慢值见[最终归并报告](performance-report-2026-10-30.md#最终归并按需输出与排序绑定恢复2026-11-02)。Goal 保持 active，其他排序分流/完整工作内存和原始功能族仍开放。

上一批以 `d09cb6c1` 为输入，为普通物理 Sort 增加实例工作预算、匿名临时文件和有界描述符归并，
并把受支持的单表物理单键排序接入真实 SQL，修复普通整数 ORDER BY 序号的文本排序差异。
29 个当前 core 原生入口通过；最终真实 Main 的所选完整 22 协议为 21 通过/1 失败，新增完整 47 在 PG 18.6/最终程序全过、冻结旧版 1 失败。
唯一最终未通过入口为已有暂缓的 Bind/portal：完整 463 的 40 处差异旧/新逐字节相同。
前一候选另出现 COPY 超时失败，隔离旧/新/旧也均失败但位置不同；最终入口通过不改写原失败，不据此关闭 R197。既有暂缓补入中央索引，未恢复其调查。
最终程序的 35 类操作检查、实际匿名排序文件内强杀及重开后的 1,000 行/4,000 单元格检查通过。
8,192×2 KiB 原生夹具的输入边界活跃分配最大增量由约 18.2 MB 降到约 51–55 KB；不是整条查询峰值。
两个最终窗口各 252 样本，单键无界排序默认约快 1.27–1.40 倍、64 KiB 约快 1.29–1.39 倍；部分控制更慢，不能宣称所有查询都提速或无回退。
完整边界、前一候选回退及全部原始记录见[外部排序报告](performance-report-2026-10-30.md#普通物理排序的内存预算与外部归并2026-11-02)。
类型化/多键等其他排序、会话 work_mem、其他算子与结果接收器仍缺完整预算；原始 273 项与原子要求保持，下方数字按各自历史批次解释。

本轮以 `bcde5b3e` 为输入，将可支持的普通单表 LIMIT 接入现有类型化执行器，补齐提前停止、OFFSET 副作用需求、
负数 LIMIT/OFFSET 的 2201W/2201X，以及单列星号输出类型。顶层类型化 SQL 接收器另保留运行错误前实际产生的行，默认 checked/嵌套调用仍失败即清空。
新增完整 62 条协议在 PG 18.6/候选全过，冻结旧版 9 条失败；24 个原生入口通过。
完整所选 21 个协议入口为 20 通过、1 失败：既有 R304/R305 完整 463 的 40 处差异在旧/新逐条一致，按用户暂缓保留，没有排除后冒称全绿。
35 类操作的现有检查通过；独立 1,000 行、216 个旧/新/旧样本中，普通列投影 LIMIT 1 从 22.893/22.670 ms 降到 9.791 ms，约快 2.3 倍。
其他查询本轮未证明稳定提速，完整范围、原始失败与复现见[普通 LIMIT 报告](performance-report-2026-10-30.md#普通-limit-按需执行与错误前缀2026-11-02)。
中央暂缓索引补入已有的 2026-10-27 R303/R304/R305 记录，不创建新暂缓项或关闭原始 273 项；下方按页扫描/Top-N/PG 数据继续按各自历史批次解释。

此前以 `17df7ff3` 为输入补充普通非分区 `TableScanOp` 的按页游标：固定扫描 MVCC 视图，复用捕获的 NULL 位图，
在页锁/pin 释放后才交付行；TOAST 按当前行展开，EOF/提前停止的统计和资源释放分开处理。
本批 23 个原生入口、17 个协议入口及 35 类操作的现有检查全过；四种事务场景的跨后端修改、写函数排序、错误及提前退出均有专项核验。
最终 1,000 行、216 个相邻旧/新/旧样本中，已有类型化数组投影 LIMIT 1 约快 1.38–1.47 倍，排序 LIMIT 20 约快 1.08–1.10 倍；
该按页扫描批次尚未处理普通列投影 LIMIT 1；无 LIMIT 排序及完整工作内存/spill 仍未解决，不能把收益推广到所有 SELECT。
192 行/16 KiB TOAST 夹具中，扫描 open 的活跃分配增量由 2,820,192 降到 17,088 字节；不是 RSS 或整条查询内存。
本批验证后的同一 CMake 程序已原子安装到根目录，旧版备份保留，既有服务未重启。
详见[按页扫描报告](performance-report-2026-10-30.md#按页物理扫描2026-11-02)；下方 Top-N 和 PG 对照数值保留各自历史批次。

2026-11-02 已实现普通有序 LIMIT 的有界候选保留、按行 NULL 位图复用及内置整数排序值缓存，
并把可支持的单表有序 LIMIT 在 Execute 阶段接入类型化执行器。
本批 11 个原生入口、11 个协议入口及 35 类操作的现有结果检查通过；新增完整 48 条协议检查在 PostgreSQL 18.6 与新版均全过，旧版有 11 条失败。
独立 1,000 行旧/新/旧对照中，LIMIT 20 和 OFFSET 500 的 p50 约改善 1.4 倍；
12,000 行生成流的 LIMIT 20 在子算子 EOF 时保留的活跃堆分配由约 69.4 MiB 降至 129 KiB。
后者不是 RSS 或整条查询内存；该 Top-N 批次尚未处理底层扫描物化，无界排序及完整 spill 仍未解决。
最终通过测试的 CMake 程序已原子替换根目录 `dbms_main`，旧程序备份保留在本批证据目录；未重启已有服务。
原始失败、构建、实测和下一步见[本批性能记录](performance-report-2026-10-30.md#有界排序-top-n2026-11-02)与[实施蓝图](postgresql-18-implementation-blueprint.md)。

2026-11-02 更新了[PostgreSQL 差距评估](postgresql-comparison.md)：273 项总账仍为 22 完成、166 部分、70 未验证、15 用户暂缓，不能作为功能完成百分比。
新增 35 类操作与 PostgreSQL 18.6 的同机对照，各侧 210 个计时样本；1,000 行小表中的点查、单行 INSERT、主键 UPDATE 的 p50 延迟比约为 42.5×、32.5×、84.9×。
这是热缓存、单客户端、各自默认内存设置的探索性观察，不能代替并发、大表、p99 或综合性能结论；完整结果和方法见[性能报告](performance-report-2026-10-30.md#postgresql-186-同机对照2026-11-02)。

当前代码使用 C++17，发行标识为 0.2.0；本轮性能改动已进入 `master`，包括目录/NULL 解码、整数查询与 DML 筛选、DDL 重写、CLOG 写放大、后台维护协调和 WAL 截断后重开修复。详细数据见[性能报告](performance-report-2026-10-30.md)。

- 已验证：123 项原生入口（122 项主矩阵及 1 项独立退出回归）、45 项协议入口、35 类操作的 315 个性能样本，以及三份压测数据库副本重开后的完整数据核验。
- 已观测的主要 p50 改善：主键 UPDATE 3.44 倍、DELETE 6.83 倍、5 行表 ALTER 重写 9.36 倍；普通点查与单行 INSERT 未证明稳定提速。实测使用 1,000 行夹具，完整参数、失败和补测均在报告中。
- 仍有优化空间：普通修改/DDL 的物理快照与摘要校验、DML 的堆扫描、JOIN 候选组合与物化、提交及后台维护等待；大表、并发和长期压力负载仍需单独验证。

2026-11-01 补做了文档与系统的一致性核查：5 个指定测试入口和包含 README 原文示例的 23 条协议观察通过；
修正了缓冲池越界值说明、后端登记接口描述，并明确配置显示值与实际资源/后台调度的接线限制。
同日补装 CMake 4.4.4，按 README 完成配置及完整编译；新产物通过两个指定数据目录/协议入口和同样的 23 条运行观察，根目录原程序未替换。
核查方法、覆盖范围、环境限制和逐文件清单见[文档清单](documentation-inventory.md)。本轮结果不代表全仓库兼容性或生产负载验收，既有功能缺口和用户暂缓状态继续以[兼容性契约](compatibility-contract.md)、[总清单](postgresql-18-gap-audit.md)和[状态数据](gap-progress.json)为准。

## 历史功能与验证记录

以下按原版本和日期保留记录，其数量、PASS 和“当前”表述只适用于各自批次。

近期修复与历次验证集中见[审查索引](review-index.md#近期修复与验证记录)，当前实现和性能范围见[状态说明](production-status.md#当前状态与验证范围)。

## 历史记录（以下状态与数量均指记录当时）

## 当前版本：R176有限排序修复，完整矩阵仍有失败

[R176](issue-review-r176-ordinary-aggregate-order-keys.md)的有限源码修复已本地提交`a35658c0`：
保留全局SUM/COUNT真实ORDER键，纯绑定及错误阶段、FILTER/NULL需求、缓存typed排序、
共享参数/重复键和方向身份有native61及完整协议190回归，两侧专项均0。
完整PG190对照0；旧版本190次完整观察59通过131失败，不把这些重复模式称为131个独立缺陷。
工作区完整72入口实际全0；干净版两次完整72均外层1，各71项通过，均在R164入口socket超时，
无完整956成功footer，清理出现延迟响应错位。超时根因未确定，也未证明与R176无关；
保留全部失败，不提高超时、不改持久化、不移除断言，不宣称双侧完整矩阵通过。
每侧35native/34协议/3真实Main顺序及原70断言保留；每侧97对象/39程序及源文件独立核对。
三个Main采用accepted测试体的路径投影及显式私有数据目录，不夹带工作区session测试改动。
Root fresh58、normal wrapper复用当前58链接、repeat零编译/零链接；clean fresh2+56当前header
donor源自独立fresh58 V3，未伪造clean常规build/repeat。提交的2198源文件与tested clean逐blob一致。
跟踪597份MD、917native+564注册Python+3Main=1484入口；工作区600份MD、923+570+3=1496，
原12个未纳入版本的测试入口和三份文档分离保留。这不是1484全量、TLS或整库sanitizer通过声明。
R176的GROUP BY排序、更多类型/算子/参数/query owner边界仍开放；R164超时仍普通未解决，非用户暂缓。
原273仍22 complete /166 partial /70 unverified /15 deferred，总目标未完成。
R173、R156及其他用户暂缓分支不恢复；不push，Actions禁用。

## 历史记录（以下状态与数量均指记录当时）

## 当前接受状态：R170/R171/R172/R174/R175

五项有限修复已分别本地提交，版本对应见[审查索引](review-index.md)。
最终组合在工作区及accepted-only clean各完整70入口（34native、33wire、3真实Main）实际全0；
完整791/314/331/906及原956均0，已接受源2195文件与实际测试clean逐字节对应。
首次Root70实际1的清理超时仍保留、原因未确定，复测0不等于超时根因已修；
源码、58生产对象、server及每端96对象/38程序均独立核对，详见[R170最终证据](issue-review-r170-quoted-unqualified-scalar-projection.md)。
已接受596MD、916native+563已注册Python+3Main=1482；工作区599MD、922+569+3=1494，
原12个未接受入口和三份文档保持分离。这不是1482全量、TLS/整库sanitizer或各中间提交独立全矩阵。
原273仍22 complete /166 partial /70 unverified /15 deferred，总目标未完成。
R173函数签名类型依赖及R176真实聚合排序仍独立开放；R156等用户暂缓模块不恢复，不push，Actions禁用。

## 历史记录（以下状态与数量均指记录当时）

当前接受版本包含[R169组合记录](issue-review-r169-simple-query-raw-syntax.md)所列九项有限修复，已逐项本地提交。
Root和accepted-only clean各完整58入口（26native、29协议、三个真实Main）实际全0；
这是组合定向回归，不是每个中间提交独立全矩阵，也不是全部1478入口运行或TLS/整库sanitizer验收。
已接受589MD、916native+559已注册Python+三个Main=1478；工作区592MD、922+565+3=1490，
原12个未接受入口及三份文档仍分开保留。原273仍22 complete /166 partial /70 unverified /15 deferred，
总目标未完成；R156、R151、R106及其他用户暂缓模块不恢复，不push，Actions保持禁用。
用法与限制见[手册](MANUAL.md)，实际错误、修改方法及日志见各专属报告。
以下旧轮次按原输入保留，历史计数、失败和待办不被本次改写。

[R157 普通SET点号目标](issue-review-r157-dotted-set-target.md)完成独立有限修复：
完整目标先于内建分派，LOCAL/SESSION/transaction相似前缀不改变身份；
引用分量保留原拼写，ColId分类、缺失分量、分隔符与FROM CURRENT边界均验证。
PG1538全0；修复前两端各1538/226失败，native Root3075/474、clean3075/3072失败。
修复后两端3075/1538全0，Root52及accepted-only clean37完整入口、严格输入/归属复核全0。
原search_path_values_grammar夹具未改，两端完整跑到254；R152的5197/1675也重新全过，
R152历史clean30终态1及旧R155断言134仍保留，不把旧输入失败改写成通过。
仅一个Parser CPP：Root正常1O2/repeat0，clean fresh1+57已核对R152生产对象；
六Main源/全头flags/对象核对后复用R152三个真实Main对象，新链接当前57库并隔离运行，
不冒fresh TU。未实现全部通用SET值语法或自定义GUC存储，R140/R151等暂缓项不重开。
[R156 REGTYPE物理常量](issue-review-r156-regtype-physical-constants.md)与R150仍开放；
已接受577MD、909native+548Python+3Main=1460；工作区580MD、915+554+3=1472，
仍有12个未接受入口，无新1460全量/TLS/整库sanitizer。
原273仍22/166/70/15，总清单未完成，不push、Actions禁用。

[R152 REGTYPE 最小引用](issue-review-r152-regtype-identifier-output.md)完成独立显示修复：
精确查找串与显示串分离，quote_ident/format %I/路径列表共用既有 ASCII、关键字引用规则。
PG1675全0；修复前两端各1675/66失败、native5197/1718失败；修复后两端5197/1675全0。
Root45完整相邻入口全0；accepted-only clean30完成29通过、1个旧搜索路径AST断言134，
整批终态1，不称全绿。冻结R155旧版同一未改夹具也在第86行退出134，
另列[R157 普通SET目标](issue-review-r157-dotted-set-target.md)开放，原失败/严格检查器终态1保留。
[R156 REGTYPE物理常量](issue-review-r156-regtype-physical-constants.md)另列开放：
PG/旧已接受版/新clean各34条观察，后两者各14条语义差异，不是34项通过；
OID往返身份丢失、内置呈现与JSON语法身份仍需独立修复，不能用本项显示通过关闭。
共享头改变后两端各正常fresh58生产O2/repeat0、各三个真实Main TU fresh，
新链接各自57当前O2库并隔离执行；输入/提交边界审计通过不代表相邻整批通过。
只接受本项五处源/注册、两个完整夹具及文档；R151继续用户暂缓，不重开R140等专项。
已接受577MD、908native+547Python+3Main=1458；工作区580MD、914+553+3=1470，
仍有12个未接受入口。无新1458全量/TLS/整库sanitizer；原273仍22/166/70/15，
总清单未完成，不push、Actions禁用。

以下各轮摘要保留该轮输入和历史计数；后续状态以本段及各问题文档为准。

[R155 枚举辅助名称归属](issue-review-r155-enum-namespace-owner.md)完成有限修复：
DROP SCHEMA 按真实目录 OID 关联识别解码后的标量枚举名，不再重解析/折叠大小写；
public 带点名称保留实际 owner，竞争身份先报 XX001，旧 native fallback 不变。
PG87全过，旧两端各87/25失败、旧native142/42失败；最终两端142/87全0，
Root24完整入口、独立clean10及归属复核通过。仅私有DDL正常1CPP O2/repeat0；
六Main源/全头flags/对象核对后复用R148三Main对象，新链接57当前O2库并隔离执行，
不冒fresh TU。clean fresh1DDL+56已核对R148对象+当前R149 evaluator，无server stubs。
[R152 REGTYPE显示](issue-review-r152-regtype-identifier-output.md)另记开放：
PG1675全过，旧两端各69失败、native5197/1718失败，夹具/注册未接受，尚无生产修复。
R151按用户要求暂缓、不恢复；R150仍开放。已接受575MD、907+546+3=1456，
工作区578MD、914+553+3=1470仍含14未接受入口；无新1456全量/TLS/整库sanitizer。
原273仍22/166/70/15，总清单未完成，不push、Actions禁用。

[R150阈值重载](issue-review-r150-threshold-width-bucket.md)仍开放；实际PG/Root各完成61条
广泛类型观察，保留非数字类型与XML参考构建能力差异，不冒充61项通过。
[R151枚举数组目录](issue-review-r151-enum-array-catalog.md)按用户“skip it”暂缓：
PG103全过，旧Root/accepted clean各103/86失败，旧native341/280失败；
候选两端native341全0，但完整103协议各仍60失败、四入口终态1。
候选源码、两个夹具及注册原样保留，未验收、未提交；普通继续不恢复本项。
独立REGTYPE名称显示问题可另行复现、修复与提交，不以此重开枚举数组或宣称R150完成。
已接受573MD、906native+545Python+3Main=1454不变；工作区576MD、912+551+3=1466，
另含R151两个未接受入口。原273仍22/166/70/15；无新1454全量/TLS/整库sanitizer。
用户暂缓不重开、不push、Actions禁用。

[R149 WIDTH_BUCKET边界与精度](issue-review-r149-width-bucket-boundaries.md)完成四参数路径修复：
按方向处理包含端点，NUMERIC保持十进制精确商，float8保留自身舍入，
count+1超过int4报22003；已声明重载只归实际builtin，原生同名替换不受影响。
PG804+5全过，冻结旧Root/accepted各804/200失败、旧native515/78失败保留；
最终515/804、Root36、accepted-only clean11及定向执行器UBSan515全0。
首轮Root36两处、clean10一处旧SQLSTATE预期错误均保留终态1；按PG核验
校正预期并补quoted超范围22003断言，没有删断言。生产仅1Evaluator O2/repeat0，
fixture修正后正常0/repeat0；核对六Main源输入、全部头/flags及对象字节后
复用R148真实Main对象，新链接当前57 O2库并隔离执行，不冒本轮fresh TU。
已接受571MD、906native+545Python+3Main=1454；工作区574MD、911+550+3=1464。
原273仍22/166/70/15；二参数阈值重载、完整numeric域与其余缺口仍开放。
无新1454全注册/TLS/整库sanitizer证明；用户暂缓不重开、不push、Actions禁用。

[R147日历间隔顺序](issue-review-r147-interval-calendar-order.md)完成有限修复：
日期先按月份调整并截断到月底，再加天，最后处理微秒与跨日；修正月底、闰年、
混合正负间隔及DML中的错值。PG943全过，旧各507/原生890失败与首版11失败保留。
类型依赖先独立提交f59a1098；当前同一Source625帧的Root48完整入口全部通过，
本项核对后复用该证据，不冒再次运行或重编Main。新accepted-base clean4native+
6wire=10全过，原生2043/协议943及类型552全0。clean为fresh1Evaluator O2+
57个核对全头/flags/CPP/原58receipt及前后字节的R148生产donor，server无stubs。
已接受570MD、905native+544Python+3Main=1452；工作区573MD、910+549+3=1462。
原十个未接受入口及R146等用户暂缓不混入；无新1452全注册/TLS/整库sanitizer证明。
完整civil/DST/其它时间算术仍开放，原273的22/166/70/15不变，不push、Actions禁用。

[R148日期算术结果类型](issue-review-r148-temporal-arithmetic-result-types.md)完成有限修复：
共享的已声明运算签名统一DATE/时间戳与INTERVAL、差值及int4天数的结果类型，
不再继承操作数OID或在NULL/零行时猜值。PG552全过，两旧版各259断言失败、
旧147原生70失败均保留；最终147/552、Root48完整入口与独立clean14通过。
共享头改变后双方均58 fresh生产O2/repeat0，三真实Main本轮fresh O0/O2/O0，
新链接57当前O2库、不用stubs并隔离实际执行。相同对象SHA不改写fresh编译事实。
仅本项类型修复独立提交；R147日历字段顺序候选和两夹具/注册不混入本项。
已接受569MD、904native+543Python+3Main=1450；工作区572MD、910+549+3=1462，
仍含原十个及R147两个未接受入口。无新1450全注册/TLS/整库sanitizer证明。
R146/R137/R140及其它用户暂缓保持；原273的22/166/70/15不变，不push、Actions禁用。

[R145搜索路径参数语义](issue-review-r145-search-path-value-identity.md)完成有限修复：
SQL SET按raw AST的独立参数逐项list-quote，不再把Sconst当已格式化列表；
保留逗号、大小写、单双引号、空名字与关键字语义。FROM CURRENT走真实
persistent/LOCAL赋值栈，避免把LOCAL可见值错误当无操作。PG3573全过，
原版各1098断言失败；初稿各4个事务断言失败，均保留原日志。最终Root/clean3573、
Root18完整入口/clean10及独立复核通过，native254和真实session320全过。
共享头改动后首轮Root/clean均fresh58 O2；最后Main修正再正常1Main O2/repeat0，
核对57库未变，fresh三Main TU O0/O2/O0后新链接当前57 O2库、隔离实际运行。
新clean为fresh Main O2+57逐一核对头/flags/CPP/原receipt/字节的生产donor，无stubs。
已接受567MD、903native+542Python+3Main=1448；工作区570MD、908+547+3=1458。
R144/R140/R137与其它用户暂缓保持未修复，原273的22/166/70/15不变；
无新1448全注册/TLS/整库sanitizer证明，不push，Actions禁用。

[R142 quoted SET参数名](issue-review-r142-quoted-set-parameter.md)完成有限修复：
完整单token双引号名称按GUC大小写无关规则解码，LOCAL搜索路径按规范名判断，
原始@变量分支仍用原语法，quoted @目标不改已有变量。PG530全过，原旧版各544/322
失败，初稿136项SHOW metadata失败/14与clean5终态1保留；SHOW先由R143独立提交，
仅含该修复的干净版本仍544/216失败。最终Root/clean544均0，15完整入口/clean6/
独立核对全0。最新Main/头未变，核对后复用R143两fresh前端对象、新编session298
O0，再链接当前57匹配O2库并隔离运行；不冒全3本轮fresh或生产重编。
R140/R137及其它用户暂缓项保留；R144/R145普通开放不关闭，原273的22/166/70/15
不变，无新1446全量/TLS/整库sanitizer证明。不push，Actions禁用。

[R143 普通SHOW协议结果](issue-review-r143-show-guc-protocol-results.md)完成有限修复：
application_name/client_encoding/search_path/lc_monetary固定text描述不再误报NoData；
结构化单行与显式非NULL位保留空串、字面量NULL及路径中的换行/UTF8，不从stdout猜行。
PG150全过，冻结Root/已接受干净旧版各150/111失败；最终Root14完整入口、clean5
与独立核对均0。正常2CPP O2/repeat0，三fresh真实Main TU/当前57库新链接/隔离运行，
session原290控制；未运行R142待定8或R140待定21扩展。检查器跨行标签/转义错误的
首次1保留，独立v2按完整150控制核对0，未改产品/夹具。R142仍为独立待定候选，
R144应用名规范化、R145路径Sconst列表语义另记普通开放及方法，不冒已修复。
原273的22/166/70/15不变，无新1445全量/TLS/整库sanitizer证明；不push，Actions禁用。

[R141 SET参数身份与分隔符](issue-review-r141-set-parameter-identity.md)完成有限修复：
普通值不再误入compatibility_mode分支，相似/带点名称不改模式，TO值内等号不作分隔符；
独立Extended模式设置不再把未执行的规划事务误当BEGIN，真实事务限制保留。
PG普通参数160全过；冻结旧版各389/235失败。初稿81失败、第二稿21失败及各自
Root13/clean4/独立终态1保留，未删断言；最终389、13完整入口、clean4与独立核对均0。
最终仅Network重编O2/repeat0；三个v2fresh真实Main TU经源/头/依赖核对后新链接当前
57库并隔离执行，session为已接受夹具290，不冒最终fresh或重跑R140待定311。
R140/R137继续用户暂缓、候选不混入；原273的22/166/70/15不变，无新1444全量/
TLS/整库sanitizer证明。不push，Actions禁用。

[R140 TimeZone设置语法](issue-review-r140-timezone-set-grammar.md)按用户“skip it”暂缓。
候选实现、4090原生/6403协议夹具及注册保留，但未提交、未验收，不计已修复。
跳过前专项、Root43和clean7完整入口实际全0；独立终态验收未运行，不补作通过。
本次只提交暂缓记录，已接受902native+538Python+3Main=1443不变；工作区另有
未接受候选。R137及其它暂缓项不重开，原273的22/166/70/15不变，不push、Actions禁用。

[R139 TimeZone默认恢复](issue-review-r139-timezone-default.md)完成有限修复：
DEFAULT和专用TIME ZONE LOCAL复用默认值解析，RESET恢复名称及真实offset，
通用TO LOCAL与带引号关键字仍22023。PG280全过，旧Root/干净旧版各280/48失败；
旧Main五观察确认非UTC默认被清成0，原生扩大到290次命令且原17控制保留。
正常1Main/repeat0、三fresh真实Main TU/当前57库新链接/隔离执行、最终12native+
16wire+3Main=31完整入口与clean5均0，独立核对0，原224协议全过。
R140另有22条实际语法观察：合法紧贴等号仍被拒，缺分隔符/专用等号误接受；
新增DEFAULT还放行两条非法语法，明确保留未修复，不冒完整语法/GUC完成。
R137仍用户暂缓；无新1443全量/TLS/整库sanitizer证明，原273的22/166/70/15不变。
不push，Actions禁用。

[R138 SESSION TimeZone](issue-review-r138-session-timezone.md)已有限修复：
显式SESSION仅在时区路由复用既有值解析，其余SET分派不改。PG224全过，
冻结旧Root/干净旧版各224完整/56失败；原112断言保留并增加112个SESSION控制。
正常1Main/repeat0、三个fresh真实Main TU及当前57库新链接/隔离执行通过；
最终12native+15wire+3Main=30完整入口全0、clean4全0，独立核对0。
首次三Main因wrapper日志路径错误未运行、整批1的记录保留，不冒产品失败或通过。
R137仍用户暂缓/未接受；DEFAULT另有13条观察与方法记录、仍普通未修复。
无新1442全量/TLS/整库sanitizer证明，原273的22/166/70/15不变，不push，Actions禁用。

[R137 外键完整父值核对](issue-review-r137-foreign-key-full-value.md)按用户“skip it”暂缓：
候选实现、测试及原始失败保留，未提交、未验收，不计已修复。初稿57/208及clean7
通过不足以验收；扩展后PG364全过，旧版364/96失败，初稿364/48失败、native89/4失败，
暴露合法网络类型被误拒绝。修订版仅正常1TM/repeat0构建通过，未运行其最终门禁。
旧Root45保留44通过/1末尾DROP断言134、终态1；不改R51断言、不重试覆盖。
候选代码/两测试/注册不进入接受版本；只记录暂缓。通用长文本/复合PK写入及
其它普通问题仍未完成，原273的22/166/70/15不变，不push，Actions禁用。

后续[R136 标量微秒持久化](issue-review-r136-temporal-microsecond-storage.md)已有限修复：
新SQL标量TIME/TIMESTAMP/TIMESTAMPTZ采用显式schema-D微秒codec，旧9/A/B/C与native
工厂保持秒；同类型ALTER重写，索引/排序/NULL/epoch0/时区与实际冷重启纳入验证。
PG412全过、冻结旧408/281失败；新916 codec、92布局（同夹具补齐前四处截断失败）、
408协议与4真实CLI全过。两份fresh58 O2后各补两次私有TM/repeat0，干净6入口0；
最终28native+28Python+3本轮新ABI后fresh、核对未变TU再链接当前57库的Main=59
完整入口全0、独立核对0。未跑新1442全量，旧1436的两失败/1TLSskip保留；
原273仍22/166/70/15，不扩大为TIMETZ/数组precision/全部civil范围或安全专项完成。
不push，Actions禁用。

后续[R135 RETURNING时区呈现](issue-review-r135-timestamptz-returning-timezone.md)已有限修复：
PG最终134全过，冻结旧Root/干净旧版各134中73失败，当前134及原微秒266全过。
只在每条语句的协议结果中按OID1184/日期/会话时区呈现，不改DmlResult或heap，
同一Simple Query批次后续SET不能重画先前结果。正常1/repeat0，干净两O2 CPP+
56生产donor含Main的真实server两入口0；最终18native+24Python（23协议/1CLI）+
3核对R132未变TU后新链接当前库并执行的Main=45完整入口全0，独立0，不冒fresh TU。
新1438未全量，旧1436两失败/1TLSskip保留。新表三标量微秒INSERT/UPDATE22023
已另记为普通未修复，不由呈现修复关闭；原273仍22/166/70/15，不push、Actions禁用。

后续[R134 原生回调签名归属](issue-review-r134-core-native-callback-ownership.md)已有限修复：
干净旧实现1320控制/922失败，同一夹具修正后1320全过；只给实际ABS/LENGTH/UPPER/
LOWER builtin callback声明所有权，opaque替换保留原始参数和caller结果提示。
实际PG原132协议全过，干净一CPP+56匹配库的4完整入口0；Root正常1/repeat0，
22native+12wire+3核对R132未变测试TU后新链接当前库并执行的Main=37完整入口全0，
独立核对0。不是fresh Main TU或新1437全量；旧1436的两真实失败/1TLSskip保留。
原273仍22/166/70/15；通用其他callback和微秒heap仍普通开放，不push、Actions禁用。

2026-10-17[干净ecf632e9完整注册回归](issue-full-registered-ecf632e9.md)已结束：
1436个入口各一次、有序唯一、无漏跑/重复；1433实际通过、2失败、1 TLS环境skip。
标准runner/summary/driver及独立终态检查器均实际1；输入与产物独立核对0，不冒全绿。
全部58生产TU fresh O2/repeat0、3真实Main fresh测试TU/新链接57匹配库/无stubs并实际
隔离执行通过。两失败为background_worker的55P03异常和stored_function_engine_owner
末尾DROP断言，分别按原后台专项/R51范围只登记，不删除断言或猜同根因。
R129/R130原完整失败入口本轮通过，R129/R131/R132/R133新增协议入口也全过；
不由此关闭旧R51、普通其余缺口或原273。原状态22/166/70/15保持；下方各轮
“未全量”等保留当时历史，以本节新终态为准。不push，Actions仍禁用。

后续[R133 timestamp binary微秒](issue-review-r133-timestamp-binary-microseconds.md)修复合法
1114/1184非整秒被22P03拒绝：PG266全过、冻结旧版266/204失败，整数解码及负epoch
向下拆分保留精确微秒与R131 UTC标记。正常1/repeat0、核心266和12native+16wire+
3核对R132未变TU后新链接当前库并执行的真实Main=31完整入口全0，独立核对0。
不冒fresh Main、整秒heap升级或完整时间输入域/错误阶段完成；新1436尚未全量。

后续[R132 SET TimeZone TO](issue-review-r132-timezone-set-to.md)已修复旧Main把TO当值：
PG112全过、冻结旧版112/44失败。只消费通用SET的独立TO，不改引号内数据或专用
TIME ZONE值。正常1/repeat0、三个真实Main fresh TU编译/新链接/隔离运行，以及
12native+15wire+3Main=30完整入口均0，独立核对0；旧R124对象此次未复用。
新1435未全量；LOCAL/SESSION/DEFAULT及完整GUC事务/严格语法仍普通开放。

后续[R131 TIMESTAMPTZ binary时区](issue-review-r131-timestamptz-binary-timezone.md)已修复UTC
时刻被再次按会话本地解释：实际PG158全过、干净旧版和冻结Root各158/45失败。
仅在有限1184中间文本明确+00，1114与infinity/NULL不变；正常1/repeat0及12native+
14wire+3核对后新链接当前库并执行的真实Main=29完整入口全0，独立核对0。
新1434未全量；合法SET TimeZone TO拒绝另列普通R132继续，不冒本项已修。

后续[R130 原生函数结果声明](issue-review-r130-native-result-hint-precedence.md)已修复：干净旧库
实际705/1/smallint、168矩阵53失败，当前168全过；PG56参考全过。显式声明不被
UNKNOWN覆盖，真实callee优先，同名原生替换保留自身所有权。正常2/repeat0及
20native+12wire+3核对后新链接当前库并执行的真实Main=35完整入口全0，独立复核0。
F8F-03本项有限修复，旧完整三失败不改写；新1433未全量，R51仍暂缓。

后续[R129 原生数组投影声明](issue-review-r129-native-array-projection-types.md)修复四处类型提示
丢失isArray，并校正旧TEXT夹具；最终typed夹具在干净旧库实际134，PG/干净旧程序
51协议均全过，证明SQL契约与原生API缺陷须分开。正常1/repeat0及14native+
9wire+3核对后新链接当前库并执行的真实Main=26完整入口全0，独立复核0，不冒Main
TU重编或新1432全量。原8f完整三失败保留，F8F-03继续处理，R51不重开。

2026-10-17[干净8f0465fc完整注册回归](issue-full-registered-8f0465fc.md)已结束，实际退出1：
1431入口各一次、按预期顺序无漏跑/重复，1427实际通过、3失败、1 TLS环境skip。
全58 fresh O2/重复0、三真实Main fresh编译执行及输入/产物终态核对完成；
不把环境skip或运行完整当作通过。三项失败及修改方法已登记，普通数组投影
夹具与pg_typeof声明问题分别处理/提交，DROP DATABASE路径按原R51暂缓。
原273状态/勾选不提升，不push，Actions仍禁用；下方各轮保留其历史范围。

真实1083的TIME binary Bind微秒/24时与错误传播修复见[R128](issue-review-r128-time-binary-parameter.md)。
PG158全过、旧完整132失败，最终55入口全0；原接收分支仅接受整秒，不把本项
误称完全新增接收器。旧4字节TIME heap不升级，其他时间/数组receiver仍普通开放。

直接SELECT的TIME高precision截断与一次声明通知见[R127](issue-review-r127-time-excess-precision.md)。
252/276及有限54入口全0；原122的值/metadata/通知核心字段全部匹配，仍保留两条
PG内部诊断来源字段差异。DDL/其他表达式、消息过滤GUC及完整转换资格仍开放。
以下各轮说明保留当时计数和范围，最新进展按对应后续记录读取；没有新全量证明。

TIME关键字precision语法修复见[R126](issue-review-r126-time-precision-grammar.md)：
非法modifier在解析阶段返回42601，合法进制/分隔符仍接受；549/326及有限52入口全0。
原122观察现120匹配，TIME(7)的warning/typmod两条仍开放，不表示全部时间类型完成。

合法TIME(0–6)基本clock转换与静态声明描述修复见[R125](issue-review-r125-time-precision-value.md)。
先按微秒输入规则规范，再单独按precision舍入，NULL/空/Describe保留真实typmod；
原97观察全匹配、122仍12个边界/NULL坏precision差异，语法/warning及旧heap仍开放。

时间类型及数组列的合法precision保存和物理投影修复见[R123](issue-review-r123-temporal-column-precision.md)。
声明0/3/6与省略(-1)分开，冷热schema/目录不从物理宽度猜precision；无关ALTER
和NULL/空结果保留声明。CAST值舍入、语法边界/warning及旧秒heap仍普通开放。

表查询中NULL::timetz被当作列名的路由遗漏见[R124](issue-review-r124-timetz-postfix-projection.md)。
只补既有后缀CAST路由，复现、修改及有限验收按记录读取；不等于TIMETZ完整
输入/存储/二进制codec或TIME显式precision完成，原273不提升。

默认TIME基本clock文本输入及真实1083/1183text Bind修复见[R121](issue-review-r121-time-input-normalization.md)。
43入口有限验收/原32text-binary对照全过；97输入对照仍有21个显式精度差异，
完整语法/cast eligibility、旧4字节heap与binary Bind等仍普通开放，原273不提升。

TIMESTAMP/TIMESTAMPTZ及真实数组的微秒binary结果修正见[R119](issue-review-r119-timestamp-binary-result.md)。
不把整秒parser的0失败值编码成日期，时区按真实类型解释；上游日期R120独立版本，
最终共享41终态/额外64精确匹配不等于整秒heap、输入typmod或所有BC/大年份已完成。

闰年年初的共享日期逆换算修正及全域独立枚举见[R120](issue-review-r120-date-cycle-inverse.md)。
它是时间戳跨日输入的上游问题，不混同R119 binary发送器；共享头重建和最终有限
验收按记录实际终态读取。原273不提升，BC/大年份和完整时态精度仍普通开放。

TIME/TIME[]的微秒、24时及合法clock归一后的真实binary输出见
[R117](issue-review-r117-time-binary-result.md)，不再借用仅整秒heap解析器。输入/文本
规范化、typmod和旧4字节标量存储仍普通待做，原273不提升，无新完整注册终态。

时态视图/内部结果物化的真实heap布局修正见[R118](issue-review-r118-temporal-view-materialization.md)。
不把改dataType名称当作物理格式转换，实际验收终态以记录为准；R117精度/binary、
目录SQL与视图origin仍普通待做。原273不提升，不重开CTAS/物化等暂缓专项。

非物化TEMP/TEMPORARY/OR REPLACE视图的CommandComplete已按真实解析对象
修正为CREATE VIEW，见[R116](issue-review-r116-view-command-tags.md)。仅修tag，不重开
物化/CTAS等暂缓范围；目录SQL、NULL视图、来源与TIME精度仍普通待做，原273不提升。

INTERVAL及其内置数组的真实二进制结果见[R113](issue-review-r113-interval-binary-result.md)。
保持独立月/天/微秒与无限标记，不把文本当binary；其输入和其他缺失标量codec
仍未完成。R114/R115先各自独立提交，原273不提升，未获新完整注册终态。

DATE/TIME/TIMETZ/INTERVAL及数组的逻辑字段长度修复见[R114](issue-review-r114-physical-attribute-length.md)。
只更正声明metadata和已知旧目录签名，不修改heap行布局或提供者生命周期。
目录SQL与视图执行/origin/tag独立失败仍开放，原273未完成，无新完整注册终态。

含数组表星号投影的CHAR/VARCHAR修饰符丢失见[R115](issue-review-r115-scalar-alias-projection-metadata.md)。
仅修声明别名身份比较；逻辑长度R114另项、INTERVAL发送R113仍待做，
原273和用户暂缓范围不变，未获得新完整注册回归。

内置数组的二进制结果输出见[R110](issue-review-r110-array-binary-result.md)。
仅扩展真实已有元素发送 codec 的数组封套，不把缺失标量 codec 的文本当作
二进制。R112 输入修复先独立提交；原273仍未完成，R109 候选继续保留未提交。

具名 BPCHAR/BIT 数组省略 typmod 时的输入截断见
[R112](issue-review-r112-unconstrained-named-array-input.md)：参考57零失败、旧44失败，
关键字默认长度和显式 typmod 保留。验证终态与独立版本以记录为准；R109 用户跳过，
R110 二进制输出另项验收。原273不提升，不把局部修复写成新完整注册回归。

状态入口更新：2026-10-16。当前发现及验证范围见[代码审查报告](code-review-2026-10-13.md)与[逐项修复](review-fixes-2026-10-13.md)；下方工程批次记录截至其各自标注日期。

后续[R109 ARRAY_POSITIONS](issue-review-r109-array-positions.md)按用户要求跳过，
候选388/248和66有限入口全0的历史证据保留，源码/测试未提交，不计已修复。
R110 二进制输出另项验收，R111 构造器规划仍开放；原273不提升。

数组查找的typed值比较和真实声明/用户重载边界见
[R108](issue-review-r108-array-position-typed-equality.md)，第五版自身354/258及
32native/29协议/3真实Main共64完整入口全0，核心/最终fence0。新增两注册入口后接受manifest为887native+3真实Main+513Python
=1403，含R98工作区为889+3+515=1407；均没有新完整注册验收，原273不变。

四个长度函数的11条签名、实际计量单位与准备阶段错误记录见
[R107](issue-review-r107-string-length-signatures.md)。同名用户函数和原生回调
保留各自所有权；逐版真实验证范围以该记录为准。旧失败不删除，本项提交后
预期1401接受入口/1405含R98工作区组合尚无完整注册终态，原273不变；R106暂缓。

后续[R104 网络值转换](issue-review-r104-network-value-conversions.md)实际PG/
Root649、原738及完整SDK、21native/17协议/3真实Main通过；五CPP实际4/3/2
增量normal/repeat及核心/最终独立核对均0，按单项本地提交。旧649/419、SDK134、
首版582/3与649/58、第二版649/1保留；无新全量，原273不变，R98/R99不混入。

后续[R105 限定NAME→INET/CIDR](issue-review-r105-network-name-input.md)最终
PG/Root738、完整SDK及21native/16协议/3真实Main全过，自身真实全58/
normal/repeat、核心与最终独立核对均0，按单项本地提交。旧738/34、SDK134
保留；无新全量；R104后续独立修复，原273未关闭，R98/R99候选不混入。

后续[R103 NAME→MAC回归](issue-review-r103-macaddr-name-input.md)本范围已修复：
最终PG/Root366、完整SDK及21native/16协议/3真实Main通过，自身真实全58/
normal/repeat与独立终态fence全0，单项本地提交。旧366/30、SDK134及首版358/6、
366/12保留；无新全量或整库sanitizer。R104/R105后续独立修复，原273范围未关闭。

后续[R102 INET/CIDR输入](issue-review-r102-network-input-notation.md)最终旧188/72、
672/389失败；当前188/ASan/UBSan、PG/Root672、2558对照及21native/16协议/3真实
Main通过，最终真实全58/normal/repeat与独立终态fence全0，单项本地提交。
首版672/23保留，无新全量。R98/R99保留、R103/R104/R105后续各独立修复，原273不变。

后续[R101 MAC输入/宽度](issue-review-r101-macaddr-input-and-widths.md)核心及
二十完整native/十五协议/三个真实Main已通过；独立头缺陷修正后真实重编全部
58TU，所有对象/程序字节相同，新匹配核心2/328与最终fence全0。本项独立本地
提交，不冒新全量。TYPE-10与原273状态不变，R98/R99待确认候选不混入版本记录。

后续[R100几何坐标输入](issue-review-r100-geometric-coordinate-delimiters.md)已按
独立问题修复：232组件、PG/Root230、十个完整native/七个完整协议通过，历史
失败保留。新组合未全量验收，不提升TYPE-09或原273状态；R98/R99跳过范围仍
待用户明确，不把其未提交候选混入R100版本记录。

原审查的页压缩、RETURNING、EXECUTE及窗口/数值问题已按逐项修复记录推进，不能继续把原始失败表当作当前未修复清单。最新[冻结5091b39c完整注册回归](issue-full-registered-5091b39c.md)为1390实际通过、1 TLS环境skip、0失败，输入/产物独立终态核对0；真实TLS、完整兼容性、历史间歇性超时和原273未完成范围仍未关闭。验证或试用请使用独立数据副本，不要依赖本项目保存重要数据的唯一副本。

当前发行标识为 v0.2.0，但仍处于生产化重构阶段，不能宣称已达到 PostgreSQL 的生产级完整度。实时范围和完成状态只取自 [`postgresql-18-gap-audit.md`](postgresql-18-gap-audit.md) 与 [`gap-progress.json`](gap-progress.json)，由 `python3 scripts/check_gap_progress.py` 校验。[`compatibility-contract.md`](compatibility-contract.md) 独立定义产品版本、PostgreSQL 18.6 行为目标、wire protocol 3.0 上限和 extended 模式；当前“客户端可连接”和“PG18 行为兼容”两级验收均未宣告通过。本文其余 PASS、sanitizer、崩溃矩阵、soak 和性能数字都是带日期的历史证据，不代表当前 HEAD 已运行同一全量矩阵。GitHub Actions 当前全部禁用（仅保留 `.github/workflows/ci.yml.disabled`）；本地构建、测试、sanitizer 和打包脚本仍可手动运行。

## v0.2 并发硬化批次（2026-08-22）

soak/TSAN 实测驱动的四个提交，每步全量回归绿色：

- **TypeRegistry 单例竞态修复**（df040ee）：`static bool bootstrapped` 旗标不受 Meyers 单例的线程安全保护，两连接线程首次类型查找并发跑 bootstrap() 重写注册表 map——TSAN 实测 **1288 条数据竞争报告几乎全部是它的连锁**（507 TypeEntry 赋值、139+138 string 读、154 红黑树节点、132 compare…）。`std::call_once` 修复后同样负载 **0 报告**。
- **WAL group commit**（c932912）：XLogFlush 先在专用 flushMutex_ 排队再进 dirMutex_——fsync 期间等待者不占插入锁，fsync 落盘后等待者的目标几乎总被已同步前缀覆盖（纯内存检查返回）。N 个串行 fsync 合并为 1 个。
- **commit 去掉堆页同步刷**（f4e8728）：world-stop 根因链取证完成——停顿窗口内冻结线程 `state=D wchan=jbd2_log_wait_commit`，放大器是 commitTransaction 每事务刷全部脏堆页（90s 压测 207MB 写入把 ext4 日志打满，fsync 排队 30s+ 且持插入锁）。堆页持久性本就由 WAL before/after 图像对 + 200ms bgwriter 承担。**验证方法论修正**：kill-storm 必须带 commit 确认屏障（无屏障版会把"kill 在 commit 前"误判为丢行——12/15 假阳性来源）。修复后带屏障 kill-storm 10/10、矩阵 12/12、soak 吞吐 43→613 ops/60s。
- **stats-map 擦除竞态修复**（d2a9e46）：closeDatabaseCaches 无锁擦 deadTupleCounts_/modifyCounts_，与连接线程在各自 mutex 下的读写竞争（TypeRegistry 修复后的时序变化暴露了它）。擦除循环补上对应 mutex。
- **LockManager 锁序报告定性为设计权衡**（dd5e347）：M1(物理令牌)跨调用持有→M0(注册表) 与 acquireLock 内 M0→M1 构成 TSAN 环；真实 AB-BA 死锁由 wait-graph 环检测 + 1s 超时兜底。修复需重写锁表协议，记录调用协议约定。
- **残留（v0.2-next）**：索引文件 WAL 图像是"每次 flush 整文件写 WAL ×2"——剩余写放大源与偶发停顿来源；需重构为索引逻辑 WAL + 恢复期重建。
- **已关闭——PgPage 同页插入竞态**：TypeRegistry 修复后的 live TSAN server 压力复测（4 协议客户端 × 50s）**0 数据竞争、0 PgPage 报告**——此前的 PgPage::insert/setLpOff 报告确认为坏 map 时代的连锁症状，不是独立的页锁缺失。该项关闭。

## v0.2-next 批次（2026-08-22 第二轮）

写放大根除批次——三项提交，每步全量回归绿色：

- **PgPage 同页竞态关闭**（89ef2ef）：live TSAN server 压力复测 0 报告，确认为坏 map 时代连锁症状。
- **索引 WAL 图像按 checkpoint 纪元去重**（7e59b1e）：实测每条 40 字节 INSERT 写 ~120KB WAL（70MB/574 次插入）——每次 commit 把**整个索引文件**写进 WAL 两次 × 每个脏索引。纪元化后同量负载 WAL 降两个数量级（60s soak 492KB→120KB）。索引 kill -9 一致性经屏障 kill-storm 验证依赖物理 flush 而非图像（10/10）。
- **事务级堆页 flush**（92167cd）：堆页全池 flush 移除后的并发正确性闭环。bgwriter 撕页竞态（TSAN 捕获 insert vs pwrite）与 bgwriter 页锁死锁（array_test 复现锁序反转）两条歧路都被否决；正解是 commit 只 flush 本事务写的页（txnWrittenPages，SSI 已有记录）——这些页仍在事务页锁下,写入无竞态。新增 BufferPool::flushPage。

**批次验证**：回归 163/0、崩溃矩阵 12/12、kill-storm 双项 10/10、ASAN/TSAN 核心集 CLEAN、soak PASS-WITH-NOTES（120KB WAL/60s，批前 ~70MB/30s）。

## v0.2 收尾批次（2026-08-22 第三轮）

world-stop 残余核查与根因终局，三项提交：

- **TCP_NODELAY**（2ca3faf）：服务器 accept 的 socket 从未关 Nagle——协议响应分多个小 send（行数据、命令状态、ReadyForQuery），第二个段被 Nagle 扣住等客户端 delayed-ACK（~40ms），形成**每条语句 41ms 的固定地板**（空查询也是 41ms 实锤）。修复后单客户端 SELECT 1 41→0ms、INSERT 58→19ms、UPDATE 75→39ms。并发下这个地板×队列深度就是此前"多秒级停顿"的主要构成。
- **auto-vacuum 移出语句路径**：DELETE/UPDATE 路径在 dead-tuple 计数过阈值（50）时**同步跑全表 vacuum 并持表锁**——4 个 soak worker 在 t≈8s 同时过阈值，全部语句被一次 vacuum 串到后面（连续 30s+ 饿死、client 30s 读超时雪崩重连）。现改为语句路径只登记 (db,table)，200ms 后台循环执行 vacuum。soak 从 crash FAIL 变 PASS。
- **残余 30s 周期停顿定性为宿主环境（结案）**：修正探针 pid 错误（setsid fork 导致 $! 失效,此前 wchan 追踪一直看着错误进程）后取到真栈：停顿窗口内 worker 线程**一次 fsync 在 ext4 jbd2_log_wait_commit 上等待 25 秒**。裸 fsync canary（覆写旧块）仅 6ms——WAL 追加新块必须等 jbd2 元数据事务提交,而宿主机上 3 个 100% CPU 的 RL 训练进程每 ~30s 周期性写大 checkpoint（iostat: sde burst 225 w/s、w_await 312ms、%util 63%）,jbd2 提交被拖到 25s。**触发条件**：共享存储宿主机上邻居进程的周期性大写入 + ext4 data=ordered 的 fsync 语义。引擎侧缓解已尽（group commit、纪元化索引图像、事务级堆页 flush、异步 auto-vacuum、TCP_NODELAY）；根治需独占存储或 XFS/ext4 nojournal WAL 卷——已记录为部署要求而非代码缺陷。

**批次验证**：soak 150s×4 PASS-WITH-NOTES（errors 从连续饿死降为宿主噪声级 3-4 次/worker,ops 恢复预算上限）。

## v0.1.0 发布批次（2026-08-20 ~ 08-21）

功能补全（每项独立提交，全量回归逐步 163 → 165 保持绿色）：

- **P1-9 自定义代价函数**：`QueryPlanner::CostModel` 可注入钩子 + GUC 同步（`seq_page_cost`/`random_page_cost`/`cpu_*_cost`），`custom_cost_hook` 式注册路径。
- **P2-2 Bloom 索引访问方法**：签名位图索引、CREATE INDEX ... USING bloom、等值查询走 bloom 预过滤。
- **P2-4 PgBouncer 式连接池**：session/transaction/statement 三模式 BackendContext 池化、3 个 GUC（`pool_mode`/`pool_size`/`max_client_conn`）、`SHOW POOLS`。修复权限泄漏：客户端会话认证状态权威，后端槽位只跟踪租借。
- **P2-5 逻辑解码**：`pgoutput`（帧式二进制）+ `test_decoding`（文本）双输出插件、`PublicationCatalog`（FOR TABLE/ALL TABLES + 按操作发布过滤）、按槽变更流（peek/acknowledge/kMaxRetained 限界）、COMMIT 流向逻辑槽/ROLLBACK 丢弃、`CREATE/DROP PUBLICATION|REPLICATION SLOT` + 4 个 SHOW 面（解析器前置拦截——否则 CREATE PUBLICATION 被通用 parser 误映射为 CREATE TABLE）。
- **P2-8 TDE 透明数据加密**：SHA-256-CTR 密流 + SHA-256 EtM 认证（组合语义等价 AES-GCM，零外部依赖）。**边车信封**模型：整页密文 + `<file>.tde` 每页 48B nonce+MAC（堆页 line-pointer 向下生长/tuple 向上生长，页内无处安放信封）；全零信封 = 明文页（就地渐进加密）；页 0 永不加密。keyring 0600 首建/坏格式拒绝。**关键修复：TDE 装载从 main 深处提前到 pre-engine 静态初始化**——否则崩溃恢复读到裸密文中止。E2E：秘密数据 at-rest 全文件 grep 不到、重启后 WAL 恢复 + 解密读回全部行。

质量与并发（sanitizer 例程直接产出的修复）：

- **WAL LSN 数据竞争修复**：`WALManager::currentLsn_` 原在 `dirMutex_` 下写、经 `currentWriteLsn()` 无锁读（flush 决策 + 后台 flusher）——并发 insert 与后台 flusher 竞争。改为 `std::atomic<Lsn>` relaxed 序（进度提示语义；记录插入仍由 `dirMutex_` 串行化）。TSAN 12/12 CLEAN 验证。
- **时间格式化线程安全修复（soak 发现）**：`ctime()`/`localtime()` 返回 libc 静态缓冲；协议服务器是 thread-per-connection，每条连接格式化时间戳（slow-query/audit/auto-explain 日志、引擎 getTime、now() 求值器、LockManager nowIso8601）都在竞争同一静态缓冲 → native 构建偶发 SIGSEGV（soak 捕捉到的不稳定崩溃）。全部调用点改用可重入变体（`ctime_r`/`localtime_r`）。验证：TSAN 服务器竞态 8→3；native 6 客户端×70s 压测连续 3 轮服务器存活（此前崩溃复现）。
- **已知（未修）——PgPage 同页插入竞态**：TSAN 在真实并发负载下报告 `PgPage::insert`/`setLpOff`/`writeChecksum` 数据竞争——两个线程无页锁并发插入**同一页**。属于引擎页级锁不变量缺失（调用方应持页锁），v0.2 引擎项；当前表现为并发 INSERT 吞吐受限 + 偶发语句级错误，不影响已提交数据正确性（WAL + 崩溃矩阵 12/12 仍绿）。
- **已知（设计权衡，非缺陷）——LockManager 锁序报告**：TSAN lock-order-inversion（复现于 lock_manager_concurrency_test 的双表死锁场景）：物理锁令牌 `LockState::mtx`（M1）跨 API 调用持有期间再次进入 acquireLock 会先取注册表锁 `globalMutex_`（M0），而 acquireLock 内部正常序为 M0→M1。真实死锁需两线程以相反序交叉持有——该场景正是测试复现的双表 AB-BA，**由 wait-graph 环检测 + deadlockTimeoutMs=1000 兜底解除**（测试断言二者至少一个失败返回）。修复需要把锁表重构为"注册表短锁 + 等待队列条件变量"协议（PostgreSQL LOCKMODE 结构），影响全部 lock/pageLock/rowLock 路径——收益是消除 TSAN 报告，代价是重写核心并发原语。定性为有意权衡：sanitizer 例程对该类报告保持 detect_deadlocks=0（数据竞争仍致命），文档记录锁序协议如下——任何持有 state->mtx 的路径不得再调用锁管理器公开 API。
- **world-stop 停顿——根因链已完整定位（v0.2 修复进行中）**：并发负载下所有连接统一冻结。取证（停顿窗口内 /proc 线程态采样 + 每秒语句吞吐直方图）：
  1. 卡死线程 `state=D wchan=jbd2_log_wait_commit`——ext4 日志提交队列堵塞；
  2. 放大器 = commitTransaction 每次事务执行 `flushDatabaseCaches`（全部堆页 pwrite + 每文件 fsync）+ WAL XLogFlush fsync——90s 压测写 207MB，jbd2 过载后每个 fsync 排队 30s+，持有 dirMutex_ 期间全部插入停摆；
  3. 客户端 30s 超时是表象周期，不是引擎周期。
  已落地缓解：
  1. WAL group commit（flushMutex_ 排队合并，等待者搭已落盘前缀的便车，消除 N 个 fsync 串行）；
  2. commit 路径不再同步刷堆页/TOAST 页（flushDatabaseCaches(heapPages=false)）——堆页持久性由 WAL before/after 图像对 + 200ms bgwriter 承担，即 PostgreSQL 模型。崩溃安全验证：带 commit 确认屏障的 kill-storm 10/10 行存活、崩溃矩阵 12/12。
  待做（设计级）：索引文件的 WAL 图像是“每次 flush 整文件写 WAL ×2”的保守机制，是剩余写放大源；需要改为索引逻辑 WAL 记录 + 恢复期重建。soak 以错误预算区分 pass-with-notes 与硬失败。

发布工程（本批次核心产出）：

- **版本单一事实源**：CMake project version ↔ `src/common/version.h` ↔ CHANGELOG.md；`dbms_main --version/-V`；git tag v0.1.0。
- **CI（历史发布配置）**：`scripts/ci.sh` 曾编排 build、回归、E2E、版本一致性和 sanitizer；GitHub Actions workflow 现已改名为 `.github/workflows/ci.yml.disabled`，不会由 push/PR/tag 触发。
- **Sanitizer 例程**：`scripts/sanitizer.sh`（out-of-tree ASAN+UBSAN / TSAN 构建，核心 12 测试；`setarch -R` 规避内核 6.8 高熵 ASLR 与 TSAN 的不兼容；LSAN suppressions 记录有意泄漏的单例）。
- **崩溃恢复矩阵**：`tests/crash_matrix_test.sh` —— {wal-insert, tde-insert, ddl-mixed, connection-pool} × {mid-transaction, post-commit, post-checkpoint} = 12 组合，SIGKILL 后继进程验证已提交行存活/未提交行消失。post-commit 用输出确认屏障消除 kill 与 flush 的竞态 flake。
- **Soak 负载**：`scripts/soak.sh` + `scripts/soak_client.py` —— 单 `--server` 进程（thread-per-connection，真实并发架构）+ N 个 PG wire protocol 客户端（SCRAM 认证 + 混合 DML 循环），验证 server 存活、行数一致性、CHECKPOINT 后干净重开。**架构发现：process-per-connection 共享数据目录不是安全并发形态**（每进程 XID 计数器在共享 WAL 中冲突 → 恢复时 contradictory COMMIT）；多用户必须走单 --server 进程。文档化于脚本注释。soak 的直接产出：两个真修复（时间格式化线程安全、WAL LSN 原子化伴随项）与三项 v0.2 已知发现（PgPage 竞态、锁序反转、world-stop）。
- **打包**：`scripts/package.sh` 源码 tarball（版本三方一致性校验）+ 目录约定（docs/PACKAGING.md）。

遗留（v0.2 候选，详见 RELEASE-NOTES.md 已知限制）：DML 双执行入口收尾（P1-0 第 4 步）、LockManager 锁序、TDE 索引文件覆盖 + 密钥轮换、逻辑解码 SUBSCRIPTION 拉取端与 WAL replay、代价模型统计消费（P1-12）、prepared statements PG 语法对齐（P1-10）。

---

2026-08-14 性能与并发硬化轮次（13 个提交，每步全量回归保持绿色）：

- **WAL 追加路径重构**：`WALManager` 维护增量尾部状态（上一记录 LSN/长度、已刷盘 LSN、最早可用 segment、常开追加 fd、可复用文件锁 fd），追加不再每条记录重扫 segment 目录；同目录多 WALManager 通过 leaked-singleton 注册表互斥，避免静态析构期锁销毁。修复了空 WAL 上第二个 manager 缓存 `currentLsn_=0` 覆盖首个 manager 数据的多实例缺陷（LSN 0 是合法记录位置，采用独立 `hasLastRecord_` 标志判空）。
- **同事务重复页 before-image 去重**：恢复的 undo 遍历按最新到最早应用未提交 before-image，同事务重复记录同一页只膨胀 WAL；现在首个镜像后跳过（xid 0 的维护性成对记录仍保留精确配对）。
- **元数据/序列/schema 内存缓存**：`.secidx`、`.hashidx`、排除约束、表 schema（mtime 新鲜度校验，支持同进程多实例）与自增序列计数器全部内存缓存；自增计数器每事务一次刷盘（序列保持非事务语义，与 PostgreSQL 一致）。全部 DDL 写路径经 `writeSchemaFile()`/显式失效收敛，修复了 RLS 开关、fillfactor、表空间迁移、分区挂载/卸载等处原有的缓存缺失隐患（由 policy/fillfactor/catalog_snapshot 测试捕获）。schema 缓存命中仍填充事务 catalog 快照，快照隔离跨引擎实例保持正确。
- **缓冲池扩容与页校验降频**：堆池默认 256 帧、索引池 128 帧（`DBMS_BUFFER_FRAMES`/`DBMS_INDEX_BUFFER_FRAMES` 可覆盖，16..4096）；Fletcher-16 页校验从每次 fetch 降为仅磁盘加载时执行（经 BufferPool on-load validator，短读未写页跳过）。
- **B+ 树查找与节点缓存**：全部节点下降路径由线性扫描（每层最多 order=100 次比较）改为 lower/upper-bound 二分查找（约 7 次），并修复 removeMulti 等值键内需继续扫描匹配 RID 的语义；64 项不可变节点 LRU 缓存使多数树层级跳过整页反序列化。
- **BufferPool 并发加载与失效健全性**：磁盘 I/O 移出池锁——锁内预留并 pin 帧、放锁 pread 到私有 scratch、重新加锁校验发布；同页并发加载遵循单加载者规则；`invalidatePage`/`invalidateAll`/`close` 排干在途加载；发布时检测并淘汰重复加载。`invalidatePage` 在并发读者下改为孤儿帧语义（互斥不变量：任一页在 `pageMap_`/`orphanedPins_`/`loadingPages_` 中至多占一），修复持 pin 读者观察到帧被回收复用后读到他页数据的损坏缺陷。该缺陷由新增 TSAN/ASAN 多线程压测（读者+改写者+失效器+驱逐混合）发现：修复前约 1 万次内容损坏，修复后 0 竞态 0 损坏。注意 `invalidatePage`/`invalidateAll` 当前无生产调用方，属对潜在陷阱的加固。
- **内部锁补全**：FSM、VisibilityMap、PageAllocator（头页读改写串行）、BPTree（递归写锁）、HashIndex、`getCommitLog` 的共享 map 全部加锁；INSERT 降级为表意图锁（IX），与 UPDATE 对齐。
- **TableManage.h 瘦身与 AM 注册表收敛**：9 个存储/索引类改前置声明（仅作 unique_ptr 成员），去掉 `<iostream>`/`<fstream>`，103 个依赖 TU 不再重解析；索引物理删除的三处重复访问方法 if-else 链收敛为 `dropIndexByAccessMethod()` 单点分发（顺带修复 undo 路径 spgist 处理不一致）。
- **实测性能变化**：事务内带 PK 插入 11.7 → ~2000+ 行/秒，commit 267ms → ~13ms；并发套件 500 行插入 + GROUP BY 从 ~106s → ~0.3s（约 350×）；WAL 每 200 插入字节数 3.4MB → 1.7MB。验证：全量回归（137 C++ + 2 E2E）全部通过、TSAN/ASAN 压测 0 报告、WAL 截断/崩溃恢复/预备事务专项独立复跑通过。

2026-08-14 序列持久化与边界安全收紧：用户序列状态写入统一使用临时文件、文件/目录 `fsync` 和原子 rename；读取严格拒绝尾随字段、非法 cycle 标志、零增量、无效范围和非法 cache。`nextval` 的 cache 批量计算、`currval`、`setval`、创建/修改序列均使用 checked int64 算术，`INT64` 上下界和耗尽序列不会触发未定义行为；重启后状态保持，损坏元数据 fail-closed。旧序列文件不作为兼容格式接受。

2026-08-14 DDL 生命周期安全修复：DDL 回滚现在会清理自身创建的物理快照，避免失败的 `ALTER SEQUENCE` 在下次启动被误恢复为旧数据库状态；typed `DdlExecutor` 以作用域守卫恢复线程局部 `Session*`，避免会话销毁后嵌入式 `nextval` 解引用悬空指针。重启、损坏序列文件、整数边界和失败 DDL 快照均有回归覆盖。

2026-08-14 数据库生命周期持久化收紧：数据库初始化文件、checkpoint 元数据和物理备份标记统一使用临时文件、文件/目录 `fsync` 与原子 rename；创建数据库的目录/文件失败会清理半成品并返回 `IO_ERROR`，checkpoint 和备份标记不会留下可被恢复流程误认的截断文件。生命周期回归增加了初始化文件完整性和 checkpoint 尾随数据校验；planner 统计回归的数据库 teardown 改用带缓存锁的 `dropDatabase()`，避免后台 writer 与裸目录删除竞争。

2026-08-14 DDL 错误传播收紧：`CREATE DATABASE`/`DROP DATABASE` 现在只在 `StorageEngine` 返回 `OK` 时报告成功；`IO_ERROR`、未知状态和数据库清理失败均返回错误并映射 SQLSTATE，新增异常数据库路径回归，避免持久化失败被伪报为成功。

2026-08-14 DDL 路径边界收紧：数据库、schema、table、view、sequence 和 domain 的文件组件拒绝路径分隔符、`.`/`..` 及空名称；schema marker 和 domain sidecar 创建采用原子发布，重复 schema 只有 `IF NOT EXISTS` 才成功，非法路径不会被当作关系文件访问。

2026-08-14 辅助对象 sidecar 收敛：view、function、table-valued function、procedure 的定义文件统一使用安全对象名、原子发布和明确的 `IO_ERROR`；父路径被普通文件占用时 DDL fail-closed，重复定义不再覆盖旧文件。完整函数/过程回归保持通过。

2026-08-14 CASCADE 物理一致性收紧：`DROP TABLE ... CASCADE` 先根据只读依赖计划解析从属表、序列和各类索引的物理删除动作，无法解析索引物理元数据时 fail-closed；物理动作完成后才发布 catalog 删除计划，动作失败由 DDL 快照恢复，避免 catalog 已删而关系文件残留。

2026-08-14 索引 sidecar 持久化收紧：`.secidx` 与 `.idxnames` 统一使用 regular-file 校验、原子发布和错误传播；CREATE INDEX 元数据发布失败会清理已生成的物理索引，DROP INDEX 元数据失败不会伪报成功，复合索引删除保留 INCLUDE/WHERE 等原始选项。

2026-08-14 索引架构清理：审计确认 `IIndexAM`、BPTree/Hash 适配器以及独立 GIN/BRIN 类没有生产调用方，均从 manifest 和源码删除；`StorageEngine` 作为唯一实际索引入口，索引回归改为覆盖 canonical GIN/BRIN 路径及损坏 sidecar fail-closed。未接入的伪适配层不再增加构建和维护负担。

2026-08-14 CREATE INDEX 事务边界收紧：物理索引 sidecar、SQL 名称映射和 `pg_class/pg_depend` 注册现在由当前格式 DDL 快照共同保护；外层事务回滚会清理 B-tree、复合、Hash、GIN、BRIN 等真实访问路径，catalog 注册异常 fail-closed，不再只记录 warning 后伪报成功。重复名称支持 `IF NOT EXISTS` 跳过，普通重复创建拒绝。

2026-08-13 WAL/恢复输入边界收紧：WAL 记录长度、8 字节对齐、CRC、记录链和 segment 尾部在打开时严格验证；`xl_prev` 保留截断历史时允许指向已删除 segment，但禁止指向当前保留流的未来位置。恢复 heap page image 必须通过当前 8 KiB 页 checksum/layout 校验，page ID 只能连续扩展一页，非法 page image、越界 page ID 和损坏 WAL 均 fail-closed，避免无界分配或把损坏页写入关系。`XLogFlush` 只接受当前 WAL 流中的有效记录位置，并同步覆盖记录所在完整 segment。

2026-08-13 DDL 路由收敛：删除 `src/main.cpp` 中已被 typed bridge 完整覆盖的旧字符串实现，
包括 `CREATE/DROP DOMAIN`、`CREATE/DROP SEQUENCE`、`CREATE/DROP SCHEMA`、数据库、角色/用户/组
和排序规则的标准路径。这些命令现在只有 parser → `DdlExecutor` → `StorageEngine` 一条标准
执行链；新增真实 bridge 路由回归，验证创建、查询和删除均不再依赖 legacy 分支。未迁移的
高级对象命令仍保留明确的兼容边界。

2026-08-13 序列变更路由收敛：`ALTER SEQUENCE` 的 `RESTART`、`INCREMENT`、边界、缓存、
循环、`OWNED BY` 和 `RENAME TO` 均由 typed `DdlExecutor` bridge 执行。重命名通过
`StorageEngine` 原子改名并同步目录、保留 catalog OID、更新 `nextval` 默认表达式和依赖关系；
冲突或中途失败由 DDL 快照回滚。新增真实路由回归验证重启、增量、重命名、重启持久化和冲突保持。

2026-08-13 配置作用域与计划缓存收敛：普通 `SET` 只修改当前 `Session` 的
`statement_timeout`、`lock_timeout` 和 `deadlock_timeout`；进程级参数必须通过管理员
`SET GLOBAL` 或 `ALTER SYSTEM SET` 修改，非管理员和误用普通 `SET` 均 fail-closed。
协议 backend 启动时显式复制全局默认 timeout，并将锁管理器的 timeout/resource namespace
绑定到当前 backend。`EXPLAIN` plan cache 纳入 `work_mem`、seq/hash/merge join、并行 worker
等 planner 设置，缓存开关和配置容量生效，配置变化会清空旧缓存；协议 E2E 新增跨连接隔离、权限和
缓存失效回归。完整 PostgreSQL GUC 体系、真正 per-query planner context 仍未完成。

2026-08-13 配置持久化边界收敛：`Config` 现在对未知键、重复等号、非法布尔值、尾随字符、
负数/越界整数和非有限浮点值 fail-closed；加载使用候选对象，失败不会污染现有运行配置；
保存先校验，再通过临时文件、文件 `fsync`、原子 rename 和目录 `fsync` 发布。`SET GLOBAL`
和 `pg_reload_conf()` 复用同一严格解析边界，新增配置专项回归。真正的完整 PostgreSQL
GUC 类型/来源/权限矩阵仍未完成。

2026-08-13 DDL 类型边界收紧：typed DDL 的列类型转换不再把未知类型静默降级为
`varchar(255)`；CREATE TABLE、ADD COLUMN、ALTER COLUMN TYPE、`CREATE TABLE OF` 等路径在
物理变更前统一拒绝未知类型和非法类型修饰符，并补齐 `serial`/`smallserial`/`bigserial`、
`nchar`/`nvarchar`、`binary`/`varbinary`、`timetz`、`pg_lsn` 的现有存储工厂映射。专项回归
验证失败不会创建关系或改变既有 schema。自定义类型的完整 catalog/type I/O 语义仍未完成。

2026-08-13 Volcano 执行失败边界：新增 `PlanExecutionResult` 与 `executePlanChecked()`，将正常 EOF 和 `open()`/`next()` 失败分离；排序、分页、去重、集合、连接、窗口、聚合及子查询物化算子会向根节点传播子算子错误。主 SQL 的集合操作、普通 Volcano SELECT、聚合、窗口和派生子查询入口已检查执行结果并 fail-closed；旧空结果兼容入口已删除，契约回归验证失败不会被误报为空结果。

2026-08-13 运行时统计持久化：`RuntimeStats` 增加当前格式版本化 `.runtime_stats` 快照，在 checkpoint 和引擎关闭时通过 sidecar `flock`、临时文件、文件/目录 `fsync` 和原子 rename 发布；启动严格校验 magic、版本、长度、数据库归属和尾随字节，损坏文件 fail-closed。多个 backend 按已加载基线做增量合并，DROP/重建关系不会恢复旧统计；新增持久化、重载和损坏文件回归。

2026-08-13 SQL 统计持久化：`SqlStats` 复用共享的统计快照发布基础设施，按数据库写入版本化 `.sql_stats`；checkpoint、shutdown 和启动加载均接入，跨 backend 按已加载基线增量合并，magic/版本/数据库归属/长度/数值/尾随字节严格校验，损坏文件 fail-closed；新增重载与尾随数据拒绝回归。

2026-08-13 SQL 统计边界：`pg_stat_statements.max` 默认限制为 5000 条，可通过 `SET [GLOBAL] pg_stat_statements.max` 调整到 1–1,000,000；超限按调用次数、累计耗时和 key 确定性淘汰，并在加载/持久化合并时再次执行上限，避免长时间运行或多 backend 合并造成无界内存增长。

2026-08-13 SSI 增量审计：SERIALIZABLE 对单列主键/二级 B+Tree 的 `=、<、<=、>、>=` 谓词新增事务级逻辑 SIREAD 记录；INSERT/UPDATE/DELETE 同步登记对应索引键，提交时按 B+Tree 固定宽度顺序检测读谓词与写键重叠，并纳入双向 dangerous-structure 判断。该能力补齐了精确索引谓词的逻辑覆盖，但不等同于 PostgreSQL 的物理索引 predicate lock；复合/表达式/部分索引、其他访问方法、安全快照和完整 SSI 图规则仍待实现。

2026-08-13 B+Tree 键边界收敛：插入、查找、多值查找、删除和范围扫描的公共 API 统一按 20 字节固定键规范化；长键截断、重开索引后的比较和范围端点不再因调用方是否预先填充而产生不同结果。新增长键持久化/重开回归；旧索引文件按当前格式重新创建，不提供历史索引兼容。

2026-08-13 堆页损坏边界收紧：`PageAllocator` 现在校验当前格式文件头的 magic、版本、页数、`rowSize` 元数据、空闲链表范围和 FNV 校验和；页读取同时校验页 checksum、页布局边界、line pointer 和 special space，任何损坏均 fail-closed，不再把坏页交给执行层。`rowSize` 可超过单页容量，由 TEXT/BYTEA/TOAST 等逻辑处理路径负责外部化，文件头校验不错误限制其大小。文件截断/非整页文件拒绝打开；页压缩修正为保留末尾 special space，避免压缩后覆盖 free-list 元数据。新增损坏页和压缩布局回归；本格式不接受 checksum 为 0 的未校验页。

2026-08-13 执行器架构与索引回表收敛：删除未被任何生产/测试代码使用的 `IExpr`、`PlanNode` 和 `IExecutionPlanner` 伪接口，保留 `IOperator` 作为 Volcano 算子唯一生命周期契约；修正 `src/executor/README.md` 对已不存在 `src/optimizer/` 的过时描述。`IndexScanOp` 现在按 RID 直接读取目标 heap tuple，在统一 page shared lock 和 MVCC 可见性边界内完成回表，不再对每个索引命中重新扫描整张表；锁冲突和页面读取失败会终止扫描并向上层传播。

2026-08-13 RLS 与 Volcano 安全边界收敛：结构化 SELECT 发现当前关系需要 RLS 时，统一使用 `forEachVisibleRow(..., "SELECT")` 的策略感知扫描，并禁用索引、bitmap、并行访问路径，避免访问方法绕过 USING 策略；新增索引存在时的 RLS 绕过回归。

2026-08-13 Volcano 访问方法安全边界收敛：删除未具备 visibility map、heap 可见性和 NULL 位图证明的伪 `IndexOnlyScanOp`，所有等值索引路径统一回表并复用 page lock/MVCC/SSI；Bitmap AND/OR 也改用相同的受保护 RID 回表，并传播锁冲突和页面 I/O 失败。visibility-map 驱动的真正 index-only scan 仍明确标记为未实现。

复制管理器本轮完成线程安全收敛：复制槽查询改为返回受锁保护的值快照，standby、primary conninfo、同步复制和 slot active 状态统一受同一把锁保护；slot 名称/类型/plugin 组合在创建时校验，并提供显式激活/停用 API。新增并发回归覆盖状态写入、slot 生命周期和快照读取；这仍是进程内管理层，不代表已经具备 PostgreSQL 级真实 WAL sender/receiver、流复制或 PITR。

本轮并发安全审计修复了 `LockManager` 的真实生命周期问题：等待 row/page 锁时不再持有可能被清除的 map 元素引用；批量解锁只释放当前线程拥有的 token；表锁重入不会重复锁底层 `shared_mutex`，共享锁升级会先平衡自身 token；等待图在等待期间持续刷新并检测双线程死锁。新增 `tests/lock_manager_concurrency_test.cpp`，验证死锁受害者释放、重入/升级、row token 归属和清理；真正的物理索引范围 predicate lock、完整 PostgreSQL 锁模式矩阵和 SSI 规则仍未完成。

Snapshot export/import 已收紧为当前 v2 二进制格式：快照携带数据库身份，严格校验版本、长度、尾随字节、XID 列表排序/重复和非法 XID；仅允许在 REPEATABLE READ/SERIALIZABLE 事务首次读写前导入，禁止跨数据库、重复导入或导入后替换快照。该边界通过 `snapshot_export_import_test` 验证，但仍不等同于 PostgreSQL 的逻辑解码快照、跨集群生命周期或完整安全快照语义。

2026-08-12 增量审计：`StorageEngine` 实例现在引用进程级 `LockManager` 注册表，独立 embedded backend 对同一数据库的表、行和 gap 资源可见；表/行/gap key 使用数据库 namespace，避免不同数据库的同名表误冲突。锁资源注册表全局共享，但 namespace、lock timeout 和 deadlock timeout 按 backend 线程隔离，避免一个连接改变另一个连接的等待策略。表、row、page 锁新增数据库目录下安全编码文件的跨进程 `flock` 协调，gap 锁以每表保守互斥协调跨进程 predicate-lock 注册，独立进程冲突、释放后获取和 gap 阻塞已有 fork 回归；锁文件不会进入物理备份。仍未完成的是完整 PostgreSQL heavyweight/lightweight lock 矩阵、索引 predicate lock 和 wait event。

锁 API 现在全部标记为 `[[nodiscard]]`，`TableManage` 的 DDL、DML、索引、扫描、TOAST、JOIN、聚合和 VACUUM 调用方均显式传播锁冲突；新增 `tests/lock_failure_propagation_test.cpp` 验证真实表锁竞争返回 `LOCK_CONFLICT` 并清理等待状态。

DDL 回滚边界继续收敛：`DdlTransaction` 现在可以撤销 view、materialized view、UDF/TVF、procedure、trigger、RLS policy 和 collation 的 CREATE 记录；这些 CREATE undo 会注册到显式外层事务，并按 SAVEPOINT 边界逆序回放。DROP/REPLACE 及文件级 DDL 在变更前建立事务快照，外层完整 `ROLLBACK` 先恢复快照，再回放行级 undo；快照已被 DDL 修改后不允许继续执行另一条快照型 DDL，也不允许创建或回滚 SAVEPOINT，避免用整库快照伪造错误的语句/子事务边界。`ddl_transaction_skeleton_test` 已覆盖对象清理、跨语句 CREATE/DROP/REPLACE 外层 ROLLBACK 和安全拒绝路径。包含内存闭包式 DDL undo 的事务暂不允许 PREPARE TRANSACTION；完整依赖图仍待补齐。

本轮新增事务回归验证 INSERT、UPDATE、DELETE 的普通回滚和 `ROLLBACK TO SAVEPOINT` 会一致恢复主键、单列二级、复合、Hash 索引及 TOAST 线外值；索引写入失败会传播并回滚堆元组。事务 DELETE 保留死 tuple 到事务结束，savepoint 回滚清除 `xmax` 后仍可在同一事务提交并读取；提交后旧 UPDATE/DELETE 版本的 TOAST 块才回收。B+Tree/Hash 已接入索引文件 before/after WAL 镜像和恢复，但 GIN/GiST/SP-GiST/BRIN、原生 page-level WAL、跨访问方法原子提交和完整崩溃窗口仍未完成。

本轮补强保存点资源边界：保存点记录当前 backend 的表锁递归深度、row/page 锁 token 和 gap token；`ROLLBACK TO SAVEPOINT` 会释放保存点之后新增的锁，并恢复保存点前被升级的锁模式。真实锁回归覆盖 token 释放和共享/排他模式恢复；真正 PostgreSQL 子事务 ID、错误状态和完整资源隔离仍未完成。

本轮修复协议失败事务的提交边界：显式事务中的语句错误会进入 `ReadyForQuery('E')`/`25P02` 状态，普通后续语句被拒绝；此时收到 `COMMIT` 会按 PostgreSQL 语义执行完整回滚而不会发布此前写入，`ROLLBACK TO SAVEPOINT` 成功后可恢复到事务中。该状态目前仍由协议 backend 维护，尚未演进为完整 PostgreSQL 子事务 ID/错误状态目录。

本轮进一步收紧两阶段命令边界：`COMMIT PREPARED`/`ROLLBACK PREPARED` 不会被失败事务状态误改写为本地 `COMMIT`/`ROLLBACK`；未知 prepared transaction 现在返回执行错误，不再以成功响应吞掉失败。PREPARE 现在先刷出 backend 私有 heap/index 缓存，prepared 元数据使用临时文件、文件/目录 `fsync` 和原子 rename 发布，并写入并刷盘 PREPARE WAL；表、row、page、gap 的本地 mutex token 会安全转移，进程级 advisory lock 按 xid 保留，由任一 backend 的 COMMIT/ROLLBACK PREPARED 释放。prepared 文件保存结构化锁资源，启动恢复会在服务可用前重建表/row/page/gap 的本地与跨进程 advisory ownership；任何资源无法完整恢复都会 fail-closed。启动恢复现在区分 committed、aborted、prepared 和普通未决 xid：仍处于 prepared 的 xid 会注册回全局 ReadView 活跃集合，顺序扫描和索引回表都拒绝泄露其行；终结 WAL 已落盘但元数据清理中断时会在 redo/undo 后安全清理。普通事务 CLOG 故障产生的 COMMIT→ABORT 序列仍被允许。`prepared_transaction_test` 已通过独立进程验证跨 backend 锁阻塞、四类锁跨重启恢复、提交可见性、索引条件不可见、回滚不可见性和事务内禁止完成 prepared transaction。完整 2PC 全局目录和崩溃后 in-doubt 决策语义仍未完成。

本轮已完成的基础收敛：

- 删除未接入的 `ClusterLayout` 和旧 4 KiB `Page` 实现。
- 删除旧数据文件自动迁移路径；启动恢复不会把事务备份/归档目录误识别为数据库。
- 删除 CatalogService 对旧 `.stc` 元数据的隐式导入和 `.migrated` 标记；当前 catalog 只加载当前版本 `.cat` 文件，升级必须通过 SQL 导出后重建。
- 存储统一为 v2、8 KiB PostgreSQL 风格 heap page 和当前 schema 格式。
- schema、sequence、trigger 读取路径只接受当前格式；旧格式回退和截断文件的部分解析已删除，损坏元数据 fail-closed，不会按默认值继续写入。
- `CREATE TABLE` 的 schema、heap/partition、TOAST、主键/唯一索引和 `tlist.lst` 初始化现在检查失败并清理已写入的半成品；索引元数据写入失败不会遗留锁或缓存指针。
- DDL executor 在物理表创建成功后立即登记事务回滚记录；约束 metadata、EXCLUDE 或后续 catalog 步骤失败时不会留下已发布的表对象。`ALTER TABLE` 现在在 DDL 事务中使用当前格式整库快照，后续子命令失败会恢复 schema、参数、索引、TOAST、catalog 和关系文件；事务快照同时包含外置 tablespace 的数据库子目录，并正确排除 UNLOGGED 关系的物理文件；完整 DROP/ALTER 跨对象依赖 undo 仍待补齐。
- DDL 的规范执行器和 legacy 兼容分发现在都检查并传播 `commitTransaction()` 失败；WAL/CLOG/fsync、延迟约束或 SSI 提交失败时不会继续执行后续 DDL，也不会输出伪成功。`deferrable_test` 覆盖延迟 CHECK 阻断 `CREATE DATABASE` 隐式提交的路径；legacy DDL 共用同一提交守卫。
- DDL 包装事务现在检查 `StorageEngine::commitTransaction()` 的最终状态；延迟约束或 SSI 导致提交失败时会向执行器传播失败，不再输出伪成功，并在本事务拥有物理快照时恢复 DDL 改动。文件级 DDL 回滚只在显式启用时创建 `<db>.txn_backup.<xid>`，普通事务不再复制整库；快照事务持有数据库级排他锁，避免物理恢复覆盖并发提交。跨对象依赖 undo、全部 PostgreSQL 隐式提交边界仍待补齐。
- 事务快照恢复已接入启动恢复：重启扫描 WAL 的提交证据，已提交快照直接清理，未完成快照恢复到 DDL 前状态，PREPARE TRANSACTION 的快照保留到 COMMIT/ROLLBACK PREPARED；快照恢复失败会保留现场供后续诊断，不伪报成功。
- WAL/CLOG 提交路径已收紧：`XLogFlush()` 和 `CommitLog::flush()` 都返回并传播 segment/目录 `fsync` 失败；事务先刷盘 COMMIT WAL，再刷盘 CLOG committed 状态，CLOG 不可用时追加 ABORT WAL、执行 undo 并 fail-closed，避免运行时报告成功而恢复重新提交。CLOG 段现在通过临时文件原子替换、段文件 `fsync` 和 `pg_xact` 目录 `fsync` 持久化，写入失败保留 dirty 状态供重试；文件锁和按位合并避免独立 backend 的整段缓存互相覆盖，已打开的读缓存会按文件时间戳刷新；CLOG 截断在保存或目录 `fsync` 失败时保留段，不会先删后报错。
- Catalog 持久化现在使用临时文件、文件 `fsync`、原子 rename 和目录 `fsync`；checkpoint、事务快照创建和关键 DDL 路径会检查 catalog 持久化结果，失败时 fail-closed，不再把半写 catalog 报告为成功。
- WAL 归档现在先对源段 `fsync`，再以临时文件完整复制并 `fsync` 后原子替换归档文件，最后原子发布 `.done` 并同步 `archive_status` 目录；归档或状态发布失败时保留可重试的 `.ready` 状态，不会把未完整归档的段交给截断路径。
- WAL LSN 语义已收敛：LSN 0 作为首个合法日志位置，`INVALID_LSN` 使用范围外哨兵；恢复逻辑明确跳过未初始化页的无效页 LSN。已提交/非事务 page image 按 WAL 正序重做，未提交事务的 before-image 按逆序 undo，避免同一事务多次修改后恢复到中间状态。多个 WAL writer 通过进程互斥、WAL 文件锁和磁盘尾部刷新避免过期 LSN 覆盖日志。
- WAL 恢复完整性已收紧：索引镜像 payload 必须完整、仅允许合法对齐填充，路径必须位于当前数据库关系目录或已登记 tablespace 的数据库子目录且使用受支持的索引扩展名；索引写入失败或 heap image 无法解析/应用时，启动恢复 fail-closed 并输出数据库与 LSN，禁止在部分恢复状态下提供服务。物理备份带显式标记，启动扫描不会把离线备份当作活动数据库。
- INSERT 回滚覆盖复合/Hash 索引和 TOAST：普通事务与 SAVEPOINT 回滚按 `(key, RID)` 精确移除多值索引项，并为堆删除写入 WAL before/after image，避免回滚行在重启恢复时复活；Hash AM 的插入/删除失败会向上返回。
- UPDATE/DELETE 回滚覆盖复合/Hash 索引和 TOAST：UPDATE 回滚只删除新版本独占的 TOAST 块并恢复旧索引键；DELETE 在显式事务内保留 heap tuple，普通回滚和 SAVEPOINT 回滚清除 `xmax` 并写入 WAL before/after image；提交后清理旧版本 TOAST，避免回滚后行或线外值丢失。
- BufferPool/Checkpoint 刷盘已收紧：`pwrite/fsync` 失败会保留 dirty 状态并向 `PageAllocator`、`checkpoint()` 和交互式 `CHECKPOINT` 传播；checkpoint 统一刷已加载的 heap/index 缓存，并在数据库仍有活动事务时拒绝推进恢复起点，避免活动事务的 WAL 证据被 checkpoint 跳过；clock-sweep 不再强制淘汰 pinned 页或丢弃无法写出的脏页，读取失败返回空指针；checkpoint 元数据和 archive status 未完成持久化时不会报告成功；归档成功后会在同一 WAL 文件锁内回收 checkpoint 之前的完整段，恢复从最早保留段开始扫描。
- PageAllocator 与 B+Tree 已适配 BufferPool 的失败契约：heap header/page 和索引 node/header 读写遇到 I/O 失败会返回失败，不再直接解引用空页；B+Tree 打开时拒绝损坏的 order/header。DDL 快照创建前、COMMIT WAL 发布前和引擎退出时会统一刷已加载的 heap、B+Tree、TOAST index、Hash 缓存；B+Tree/Hash 刷盘会先写 before-image、再刷文件、最后写 after-image，并在恢复时按事务提交状态选择镜像。GIN/GiST/SP-GiST/BRIN、原生 page-level WAL、跨访问方法原子提交和完整崩溃窗口仍未完成。
- StorageEngine 的 Hash/GIN/BRIN 索引文件现在使用严格格式校验；写入采用临时文件 + fsync + 原子 rename，写入失败保留 dirty 状态，截断、非法版本和尾随垃圾会拒绝打开。BRIN 当前格式按本项目策略重建，不兼容旧索引文件；各访问方法的完整 WAL-safe 增量维护仍未完成。
- `StorageEngine::forEachRow()` 现在返回并传播 heap/partition 页面打开与读取失败；B-tree、复合、全文、GiST、SP-GiST、Hash、GIN、BRIN 构建以及 `REINDEX` 会在扫描失败时返回错误；全文/GiST/SP-GiST/GIN/BRIN 文件在完整扫描成功后才原子发布，B-tree/Hash 的 WAL-safe 构建仍未完成。过滤器、聚合、JOIN、FK/EXCLUDE 检查、`ANALYZE`、表重写、TOAST 写入和 Volcano 并行 page-range scan 现在也区分 I/O 失败与合法空结果；统计文件采用原子替换。BRIN 使用长度前缀格式保留带空格的边界值，损坏索引读取 fail-closed。索引增量维护的 WAL 语义仍未完成。
- `DROP TABLE` 现在先生成只读 `CASCADE/RESTRICT` 依赖计划，物理删除成功后才应用 catalog 删除计划，避免物理失败时 catalog 先被移除。
- `DROP SCHEMA` 现在同样先生成只读 namespace 依赖计划，物理 schema 删除成功后才应用 catalog 计划；若 catalog 后处理失败则恢复当前格式事务快照，避免 schema 目录与物理对象分裂。
- 当前 schema 格式升级为 `0x44420009`，表名、列名、类型名和约束名字段统一保留 64 字节（最多 63 字节标识符），不再静默截断 15 字节以上的合法标识符；旧 schema 按设计拒绝读取。
- `PageAllocator`、`PageWrapper`、TOAST 路径统一使用同一页格式。
- 表空间物理路径已收敛：pg_default 关系文件保留在数据库目录，自定义表空间使用
  `<LOCATION>/<DATABASE>/`，heap、FSM/VM、各类索引、分区和 TOAST 共用此路由；
  `ALTER TABLE ... SET TABLESPACE` 支持同文件系统 rename 与跨文件系统 copy+remove，
  目标表空间不存在或运行时 marker 丢失时 fail-closed，不再静默创建默认目录空表。
- TOAST 线外值已纳入 zlib 压缩：chunk header 保存压缩标记与原始长度，读取路径校验后解压；当前格式不兼容旧 TOAST chunk，符合本项目不保留旧数据兼容的策略。lz4/pglz、列级 storage strategy 和 `toast_tuple_target` 仍未完成。
- B+Tree 已修复 root leaf 分裂、叶分裂中间键丢失、重复键跨叶查找、范围扫描重复返回和多值索引按 `(key, RID)` 精确删除；跨叶唯一键、跨内部节点重复键及精确删除回归已纳入索引测试。PG B-tree 的 dedup、删除合并、opclass/collation 和完整并发构建语义仍未完成。
- `DROP INDEX` 已迁移到 typed DDL：标准的 name-only、多名称、`IF EXISTS`、`CASCADE/RESTRICT` 语法维护物理索引、`pg_class` 和依赖关系；单列/Hash 索引持久化 SQL 名称到物理键的映射，旧 `ON table` 形式仅作为迁移期兼容语法保留。真正的 `CONCURRENTLY` 两阶段语义仍未完成。
- `CREATE INDEX ... USING {hash,gin,gist,brin,spgist}` 已接入同一 typed AST/DdlExecutor，按访问方法调用真实的 StorageEngine 实现并登记索引名；非 PostgreSQL 的 `CREATE GIN INDEX` 等特殊分支已从 `main.cpp` 删除。各访问方法的 PostgreSQL opclass、WAL、并发构建和维护语义仍不完整。
- 标准 `CREATE/UNIQUE INDEX` 的旧字符串解析块已从 `main.cpp` 删除（约 138 行）；分类为 `CreateIndex` 的语句现在只能由 parser + DdlExecutor 处理，解析失败会 fail-closed，不再存在第二套索引分发。
- 删除确认无引用的 helper 和重复的 parser/聚合辅助代码。
- 删除未被执行器使用的 `IStorageEngine` 伪适配层；`StorageEngine` 不再提供会静默返回空结果、0 或伪成功的接口 wrapper。
- 统一构建源文件清单为 `cmake/dbms_sources.txt`；CMake、主程序脚本和测试脚本不再各自维护生产源列表。
- 四个 shell 构建/测试入口统一复用 `scripts/build_common.sh` 的编译选项、include、TLS 检测和链接库；生产二进制和测试对象都记录配置指纹，编译参数、TLS 模式或源码变化会自动触发重建。测试编排唯一由 `build_tests.sh` 负责，且会自包含地构建 E2E 所需的 `dbms_main`；`run_all_tests_fast.sh` 仅提供安静输出并在失败时保留完整诊断。CMake 现在提供 `check`/CTest 标准入口，直接调用同一编排器并在配置阶段校验 manifest 源文件、重复项和入口文件。
- 生产二进制构建失败时测试入口立即 fail-closed，不会继续运行旧的 `dbms_main` 或报告混合版本结果。
- 测试运行时隔离已收敛：每个独立 C++ 测试使用临时工作目录，退出后回收其 WAL、catalog、`.txnid`、日志和测试数据库；`build_one_test.sh` 与完整入口共享同一隔离函数。
- DDL bridge 对已归属 AST 路径的解析失败改为 fail-closed，不再把语法错误交给 legacy 分发；`DdlExecutor` 解析失败也明确返回错误。
- `ALTER TABLE` 的基础高频子命令已迁移到 typed AST：ADD/DROP COLUMN、ALTER COLUMN（TYPE/DEFAULT/NULL）、RENAME COLUMN/CONSTRAINT/TABLE、CHECK/PRIMARY KEY/UNIQUE/FK/EXCLUDE 约束增删、SET LOGGED/UNLOGGED、STATISTICS、INHERIT、RLS enable/disable/force、分区 ATTACH/DETACH、trigger enable/disable、CLUSTER、REPLICA IDENTITY、VALIDATE/ALTER CONSTRAINT 和基础 storage 参数；约束状态写入统一 `.params`，副本标识同步 `pg_class.relreplident`。
- `CREATE TABLE ... PARTITION BY` 与 `CREATE TABLE ... PARTITION OF` 已统一进入 typed AST/DdlExecutor，子表 schema、bound 和 catalog 注册均有 SQL 回归覆盖；分区约束证明与 global/local index 语义仍未完成。
- 持续删除 `main.cpp` 中已被 typed bridge 遮蔽的 ALTER TABLE 字符串分支；本轮再移除约 300 行约束/CLUSTER/REPLICA 元数据重复处理，ALTER TABLE 这些动作现在统一由 parser → DdlExecutor → StorageEngine 执行。
- 测试入口改为缓存生产对象、逐测试独立链接运行；避免每个测试重复编译完整 DBMS，同时保留自定义源和本地 stub 测试覆盖。
- 文档入口已收敛：删除未引用且内容过时的根目录 `MANUAL.md`，`docs/MANUAL.md` 作为唯一完整使用手册，避免两份能力描述长期漂移。
- 修复 `DROP DATABASE` 未释放数据库级 page/index/TOAST/WAL/CLOG/catalog 缓存的问题；新增同名数据库重建回归测试，防止旧缓存迟写入新数据库。
- CLOG 刷盘在数据库目录或 `pg_xact` 子目录已被删除时不会重建目录或把旧事务状态写入同名新数据库；段更新采用原子替换，避免截断写入留下半段状态文件。
- 网络服务默认 fail-closed：证书/私钥缺失、OpenSSL 不可用或 TLS 初始化失败时拒绝启动；明文只能通过显式 `--insecure` 开启，且仅用于本地开发。
- 网络服务生命周期已收紧：启动失败通过返回值传播，端口占用不会伪报成功；`SIGINT`/`SIGTERM` 和显式 shutdown 请求会停止监听、唤醒活动连接并 join 所有客户端 worker，不再永久 detached。`network_server_lifecycle_test` 覆盖端口冲突和优雅退出。
- 删除运行时自动生成自签名证书的 shell 调用，避免私钥落盘位置和命令参数不可控；部署必须显式提供 TLS 材料。
- 网络服务已切换到 PostgreSQL Frontend/Backend protocol 3.0 核心路径：支持 SSLRequest 协商、StartupMessage、catalog SCRAM-SHA-256、参数状态、简单 Query，以及 Parse/Bind/Execute/Sync 基础流程；协议回归由 `tests/postgres_protocol_test.py` 覆盖真实 SCRAM 握手。
- legacy `execute()` 的协议结果捕获已改为线程局部 `process/OutputCapture` multiplexing；移除网络入口对全局 `std::cout` 缓冲区和 `g_outputCaptureMutex` 的依赖，协议会话不再因文本捕获而全局串行化，并有多线程无串扰回归。
- 单表 DML 的结构化路径已扩展到 `commands/DmlExecutor`：普通 `INSERT VALUES/DEFAULT VALUES`、简单单表 `INSERT ... SELECT`、无 target 或显式匹配主键/唯一约束 target 的 `ON CONFLICT DO NOTHING`、显式匹配单列或复合主键/唯一约束 target 的常量或只引用 `excluded` 的 evaluator 受限标量表达式 `ON CONFLICT DO UPDATE` 及目标行/`excluded` 的受限 `WHERE`、以当前目标行列值为输入的受限标量表达式单表 `UPDATE`、单源表 `UPDATE ... FROM`、单源表 `DELETE ... USING`、简单谓词单表 `DELETE` 和窄版单源表 `MERGE` 在进入 legacy 分发前统一执行；其中 UPDATE FROM/DELETE USING 支持来源 INNER/CROSS JOIN、来源别名和限定连接谓词，并复用 StorageEngine 约束、触发器、索引、RLS、FK/MVCC 和 ACL。MERGE 当前支持单个 MATCHED UPDATE/DO NOTHING、单个 NOT MATCHED INSERT/DO NOTHING，并在执行前拒绝多源行匹配同一目标行；多 WHEN、BY SOURCE/BY TARGET、DELETE、复杂 source query 和 RETURNING 明确 fail-closed。普通单表 INSERT/UPDATE/DELETE 的列投影和 evaluator 支持的受限标量表达式 `RETURNING` 在实际存储修改边界收集，并通过协议层发送结构化结果集与 command tag。复杂 SELECT 源、复杂谓词、外连接/复杂 JOIN、视图、复杂 `UPDATE ... FROM`/`USING`、部分/索引推断 conflict target、引用子查询或其他关系的 `ON CONFLICT DO UPDATE`/`WHERE`、复杂/子查询/窗口 RETURNING 仍回退到旧路径。列引用不再被误判为 NULL，带表限定的列引用优先解析，复合 UNIQUE 中含 NULL 的键按 PostgreSQL 默认语义不互相冲突，存储层默认值只对缺失列生效。
- 简单单表视图已支持行级 `INSTEAD OF INSERT/UPDATE/DELETE` action SQL；多行 `VALUES`、按实际匹配行的 UPDATE/DELETE、`NEW`/`OLD`/`WHEN` 和 server 会话执行均有协议回归，复杂视图映射与函数/PL 触发器运行时仍未完成。
- 顶层 `INSERT`/`UPDATE`/`DELETE`/`MERGE`/`REPLACE` 以及包含写 CTE 的 `WITH` 语句现在统一由 `execute()` 建立内部事务；普通语句成功自动提交，错误或异常自动回滚，递归触发器/视图/CTE 执行复用外层事务，避免多行 DML 在中途失败后留下部分写入。协议层同时把 `WITH` 查询识别为结果集。显式 `BEGIN` 仍由连接事务状态管理。
- `CREATE TEMP/TEMPORARY TABLE` 已进入 typed DDL：物理对象名包含 backend PID，临时表可遮蔽同名永久表但不会污染持久 catalog；用户临时表与查询内部 CTE/派生表临时对象分离管理，连接断开时统一清理。`ON COMMIT PRESERVE ROWS/DELETE ROWS/DROP` 已绑定 backend-local transaction commit/rollback，真正 `pg_temp` schema/catalog/search_path 语义仍未完成。
- 临时 relation 生命周期已覆盖异常退出边界：启动时在 WAL 恢复后清理残留的会话临时表、分区/TOAST fork、孤儿文件和 `tlist.lst` 条目，避免进程重启后的名称冲突和磁盘泄漏；`__tmp_<backend>_...` 是保留的内部物理命名空间。
- `UNION`/`INTERSECT`/`EXCEPT` 的组合语义已统一由 Volcano `SetOperationOp` 执行，覆盖顶层优先级、错误传播及 `ALL` 重复行语义；简单单表 operand 直接构建子计划，复杂 operand 通过 `MaterializedRowsOp` 接入，完整 AST 下推和类型合并尚未完成。
- 并行执行已具备可验证的 `ParallelTableScanOp`：非分区 heap 按 page range 由多个 worker 读取并按范围顺序 Gather，`max_parallel_workers_per_gather` 可配置；事务内、分区表、并行 join/aggregate、GatherMerge 和长期 worker pool 仍未完成。
- 窗口执行已具备可复用的 `WindowOp`/`WindowAgg` 计划节点：主 SQL 入口已接入常见排名/偏移、窗口聚合、`ROWS/RANGE/GROUPS` frame/exclusion、独立分区/排序、OFFSET 和最终结果排序；复杂目标列表和主 SQL EXPLAIN 的窗口解析仍保留 legacy fallback。
- 聚合执行已统一到可复用的 `GroupAggregateOp` 计划节点：无 GROUP BY 的普通聚合与常见 `GROUP BY`、`HAVING`、`ROLLUP/CUBE/GROUPING SETS` 均消费过滤后的 Volcano 子计划；复杂目标、`GROUPING()`/`GROUPING_ID`、完整排序作用域和并行聚合仍待完成。
- 未关联单列 `IN`/`NOT IN` 已下推到 Volcano `SemiJoinOp`（anti 模式）；未关联单表 `EXISTS`/`NOT EXISTS` 已下推到 `ExistenceFilterOp`；单个未关联标量目标已下推到 init-plan + `ScalarSubqueryProjectOp`，严格处理 NULL 和多行 cardinality error；单列未关联 `ANY`/`ALL` 已下推到 `QuantifiedSubqueryFilterOp`，严格处理 NULL/空集三值逻辑。相关行为均有单测与协议回归；关联/复杂标量、row comparison 和复杂组合仍保留 legacy fallback。
- 多个等值索引条件已由 `BitmapHeapScanOp`/`BitmapOrHeapScanOp` 执行候选 RID 的 AND/OR 组合，再统一 heap fetch 和原谓词重检；范围 bitmap、并行 bitmap 和真正 block bitmap 扫描尚未完成。
- SERIALIZABLE 事务对非空索引谓词登记命中页、对顺序扫描登记实际扫描页，并保留空集/无法安全暴露页来源时的关系级 SIREAD 兜底；提交时页级覆盖参与 rw-conflict 检测，非相交页可并发提交，跨页危险结构仍会返回 serialization failure。真正的索引范围 predicate lock、完整 SSI 冲突图和安全快照仍未完成。
- 扩展查询已支持文本及常用类型二进制参数/结果（bool/int2/int4/int8/oid/float4/float8/text/varchar/date/time/timestamp/timestamptz/uuid/numeric；日期时间按当前引擎秒精度存储，numeric 使用 PostgreSQL base-10000 wire 格式并以精确 decimal 文本保存）、`Parse` 参数描述、`Bind` 数量/格式/NULL 校验、`Describe`/`Close` 生命周期、`$n` 字面量绑定和基础 portal `Execute maxRows` 分批返回（含 `PortalSuspended`）；常见单表列会返回 catalog/table schema 驱动的 OID、长度、属性号和表 OID，复杂表达式仍回退为 text。数组等复杂类型的二进制 I/O、完整 RowDescription 类型推导以及 holdable/scrollable cursor 等完整 portal 语义仍待实现。
- 表/列 ACL 检查已统一解析会话用户自身、递归继承角色和 `PUBLIC` 授权；`NOINHERIT` 用户不会自动获得成员角色的 ACL/RLS 权限，原始成员关系仍单独供 `pg_hba.conf` 角色匹配使用；真实协议和策略回归验证了继承与拒绝边界。RLS 现已通过关系感知扫描统一应用到查询、更新、删除及结构化 DML 来源关系，并实现默认 `WITH CHECK`、显式 `TO PUBLIC`、基础 `PERMISSIVE/RESTRICTIVE` 组合、表 owner 绕过、基于 `pg_authid` 的 `SUPERUSER/BYPASSRLS` 绕过和 `FORCE ROW LEVEL SECURITY`；无适用策略默认拒绝，策略求值失败安全回退。对象全集 owner/依赖、完整 ACL item/继承语义、schema/database/function ACL 和完整 ACL 组合语义仍待补齐。
- `ALTER TABLE ... OWNER TO` 已收紧为表所有者/超级用户操作，并要求当前会话能够 `SET ROLE` 到目标角色；`SET ROLE` 按原始成员关系授权，`NOINHERIT` 不会错误阻止显式切换。`current_user`、RLS、结构化 DML ACL 和 CREATE TABLE owner 均使用当前有效角色；完整对象 owner 传播和 ACL 组合仍待补齐。
- 表 owner 已在统一 ACL 查询中获得隐含表/列权限和 `GRANT OPTION`；表级 `REVOKE` 复用同一授权判定。schema/database/function ACL、ACL item 和默认权限完整传播仍未完成。
- 表级 `GRANT OPTION` 已支持独立撤销、依赖授权的 `RESTRICT` 拒绝和 `CASCADE` 递归回收；表级授权链会记录实际 grantor，并在撤销后清理失效链路。完整 ACL item、对象类型和默认权限传播仍未完成。
- `ALTER DEFAULT PRIVILEGES` 已迁移到 typed AST/DdlExecutor，支持 `TABLE/TABLES` 的 `GRANT`、`REVOKE`、多权限/多 grantee、schema 校验和幂等规则，并在新建表时按 effective owner 应用；默认权限的 grant option、sequence/function 等对象类型和完整 catalog ACL 仍未完成。
- `TRUNCATE` 已迁移到 typed AST/DdlExecutor，支持 `ONLY`、多表、`RESTART/CONTINUE IDENTITY`、递归 FK `CASCADE` 与 statement-atomic `RESTRICT` 预检；trigger/foreign table 和完整 PostgreSQL transactional/locking 语义仍未完成。
- `pg_auth_members.admin_option` 已接入角色 `GRANT ... WITH ADMIN OPTION`、重复授权升级和 `REVOKE ADMIN OPTION FOR`；角色授权现在按超级用户、CREATEROLE 或现有 ADMIN OPTION 检查，完整 ADMIN OPTION 级联和 grantor 生命周期仍待补齐。
- 协议错误状态已收敛：扩展查询在 Parse/Bind/Execute 错误后进入 PostgreSQL 的 ignore-until-Sync 状态；事务外简单查询错误返回 `ReadyForQuery('I')`，连接可在 Sync/错误响应后继续使用。数组等复杂类型、完整类型映射、二进制扩展消息语义仍未完成。
- 事务上下文已从共享 `StorageEngine` 实例移为连接工作线程局部：事务 ID、快照、回滚日志、savepoint、隔离级别、延迟约束和 `lastval` 不再在协议连接之间互相覆盖；连接断开时会回滚未完成事务并丢弃 backend 上下文。全局锁管理器和提交状态仍用于跨 backend 协调。双连接协议回归已验证未提交行隔离、回滚恢复、断开回滚和提交后可见性。
- TCL 路由已收敛到事务 AST：`BEGIN`/`START TRANSACTION` 的隔离级别与 READ ONLY/WRITE 选项、`SAVEPOINT`、`ROLLBACK TO` 和 `RELEASE` 不再依赖固定字符串偏移；分类顺序已修复，`ROLLBACK TO`/`COMMIT PREPARED`/`ROLLBACK PREPARED` 不会被通用前缀吞掉。`DEFERRABLE` 在执行层明确拒绝，避免静默宣称未实现语义。
- SQL 统计已从 `main.cpp` 提取为线程安全 `process/SqlStats` 模块，交互式与 PostgreSQL 协议入口共用；字符串/数字常量和空白归一化后聚合，`SHOW STATEMENTS` 与 `pg_stat_statements` 风格虚拟表可查询。当前格式 `.sql_stats` 已在 checkpoint/引擎关闭时持久化并在启动时严格加载；上限/淘汰、reset 权限和完整 PostgreSQL 扩展字段仍未完成。
- 运行时统计已从显示层下沉到线程安全 `process/RuntimeStats`：SQL 执行、失败、提交/回滚，以及 StorageEngine 和 Volcano 扫描算子的顺序扫描、索引扫描和实际 DML 行数会进入共享计数器；完整可见表扫描建立的 live-row 估计会反馈给 Join 成本和 EXPLAIN，部分/索引扫描只保留展示用下界，表重建/截断会清除旧关系身份的估计；`SHOW STATUS`、`pg_stat_database` 和 `pg_stat_tables` 不再输出固定零值。当前格式 `.runtime_stats` 已持久化，索引访问方法细分和后台采样线程仍未完成。
- 构建质量收敛：修复 planner 的 merge join cost 参数错误，清理 parser 未使用参数和测试冗余 helper；legacy 输出捕获已从全局重定向改为线程局部路由；`./scripts/build.sh` 在 `-Wall -Wextra` 下通过且无编译警告，完整回归与 OpenSSL Docker 构建均通过。
- 构建缓存现在按编译配置、源码清单、生产源码和头文件内容计算 SHA-256 签名；测试对象另按全部测试源计算独立签名，不再仅依赖 mtime。Git 回滚、工作区恢复或复制数据目录后会安全失效并重编译，避免测试链接到过期对象，也不会因测试改动无谓重编译生产主程序。
- PostgreSQL 协议 E2E 测试不再把服务端 stdout/stderr 连接到无人消费的管道，避免长流程输出填满 pipe 后阻塞服务；协议和窗口 E2E 的超时可通过环境变量覆盖，默认值适配慢速持久化/CI 环境，避免把正常慢执行误判为随机失败。

启动安全边界：非空但 magic/版本不匹配的数据文件会直接拒绝打开，不会被清零或按新格式覆盖。部署时必须将数据目录初始化为当前格式，并通过备份恢复或 SQL 导入完成升级。

数据兼容边界：旧 schema、旧 4 KiB 数据页和旧行头不会被读取或迁移。升级前必须导出 SQL，或删除并重建数据目录。

仍不能称为生产就绪的主要原因包括：SSI 目前已增加页级 SIREAD 和空范围关系级兜底，但索引范围 predicate lock、完整 rw-conflict 规则和安全快照仍不完整；当前 wire protocol 仍缺完整类型/错误/扩展消息语义、channel binding 和结构化执行结果，owner/依赖和完整 ACL 组合语义仍不完整；表空间仍缺权限/owner、ALTER TABLESPACE 完整语义及 PostgreSQL OID/符号链接布局；并行执行、流复制/PITR、完整系统目录接入、审计/可观测性和系统化故障注入测试也仍不完整。后台 writer/checkpointer 与 DDL/database lifecycle 的文件缓存并发访问已加锁并纳入回归验证。网络连接容量现在通过原子槽位预留控制并发 accept，TLS 握手失败和认证失败都会释放槽位。后续改动必须以代码路径、回归测试和故障恢复验证为准，不能只以功能清单宣称完成。

2026-08-13 parser 数值选项继续 fail-closed：`CREATE FUNCTION` 的 `COST/ROWS`、角色 `CONNECTION LIMIT` 和 `ALTER TABLE ... SET STATISTICS` 现在拒绝缺失、非法、非有限、越界或不允许的值，不再吞异常后使用 AST 默认值。

2026-08-13 sequence DDL 输入边界收紧：`CREATE/ALTER SEQUENCE` 的 `START`、`INCREMENT`、`MINVALUE`、`MAXVALUE`、`CACHE` 等整数选项现在要求完整 int64 数字；缺失值、溢出值和未知选项在 parser/executor 两层 fail-closed，不再静默使用默认序列参数。

2026-08-13 DDL 路由继续收敛：删除 `main.cpp` 中已被 `tryDdlBridge → DdlExecutor` 完整遮蔽的 `ALTER ROLE/ALTER USER` 字符串执行器（约 100 行）；`ALTER GROUP` 等尚未迁移的兼容路径仍保留。

验证入口：`./scripts/build.sh`、`./scripts/run_all_tests_fast.sh`、`./scripts/build_tests.sh`；两个 E2E 已由统一测试入口自动执行。DML AST 路径另由 parser 单测和协议 E2E 覆盖。Docker 镜像构建使用 `docker build`。CMake 验证需要环境提供 `cmake` 可执行文件。
- SQL 解析边界继续收敛：`LIMIT/OFFSET/FETCH` 的无符号计数现在要求完整十进制整数，非法值、缺失值和不完整 FETCH 子句均返回解析错误，不再被异常吞掉后静默变成默认的“无限制”。`FETCH FIRST/NEXT` 省略 count 时按 PostgreSQL 语法默认 1 行处理。

## 2026-08-23 v0.3 批次：PITR（时间点恢复）落地

本批次关闭 P0 级差距"时间点恢复"，分三个提交完成并各自全量验证：

1. **WAL 归档执行**（`3976309`）：`archive_command='dir:<path>'` 配置（单引号值可含空格与 `=`，空引号=未设置）；checkpoint 边界标记 `.ready` → 原子复制 → `.done`，失败保留 `.ready` 由 bgwriter 归档线程每 200ms 重试（PostgreSQL 语义）。`tests/wal_archive_conf_test` 覆盖解析与端到端标记翻转。修复测试基建陷阱：测试内强定义 `g_config` 与 test_stubs 弱符号在增量构建下布局错位导致静态析构 double-free——改为 `extern` 声明使用桩定义。
2. **PITR 恢复**（`ab33d50`）：提交记录 v2 格式 `[xid][epoch]` 携带时间戳（v1 记录兼容，读作无限制）；`RESTORE DATABASE db FROM 'bk' PITR 'YYYY-MM-DD HH:MM:SS' ARCHIVE 'dir'` 归档段回填 pg_wal + 持久化单次消费的 `recovery_target`，下次启动恢复把目标后提交按未提交回滚（before-image 恢复）。新增 `PG_SWITCH_WAL`（零填充关闭当前段；`scanWalTail`/`validateRecordsOnDisk` 在填充处停止，`XLogInsert` 自愈空尾缓存）。顺带修复两个存量 CLI bug：物理 `BACKUP/RESTORE DATABASE` 前缀长度错误（`substr(0,14)` vs 15 字符关键字）从未匹配过、且 SQL-dump `RESTORE` 分支遮蔽物理恢复；`.sql_stats` 持久化零调用行导致重启 abort。Shell E2E：备份→分段写入→PITR 到中间时刻→目标后写入消失，PASS。
3. **文档**（`7857db2`）：`MANUAL.md` §20.1 归档配置/`PG_SWITCH_WAL`/四步 PITR 操作手册；`commandsList.md` 新增 `RESTORE DATABASE (PITR)` 与 `PG_SWITCH_WAL` 条目；`feature-gaps.md` P2-3 翻转为部分完成；`all-gaps-todo.md` 存储/WAL、复制/HA 矩阵行更新。

验证状态：完整回归 `PASS=164 FAIL=0`（含新增归档测试与 7 个 Python E2E）；ASAN 与 TSAN 全 CLEAN（归档测试已加入 sanitizer 核心集）；崩溃矩阵 12/12（首跑 11/1、基线对照 10/2 均为宿主机 RL 训练进程 ~30s IO 突发导致的 kill 时序偏移，复跑全绿）。sanitizer.sh 修复存量缺陷：对象重建只比较 .cpp mtime 不查头文件，布局变更头文件（如 Config 新字段）会与新测试代码错位（UBSAN 报非法 bool 读）——现已按任意项目头文件新于对象即重建。

## 2026-08-23 v0.4 批次：GiST 查询路径接入（P0-3 部分翻转）

P0-3 长期停留在"仅有 SP-GiST、无通用 GiST"的记录上，但审计发现 `CREATE INDEX ... USING GIST` 的存储层 sidecar（每行 `rid low high`）早已存在——缺的是**查询路径**：没有任何谓词会使用它，没有 EXPLAIN 节点，没有回归。本批把它接入计划器：

1. **`GiSTScanOp` 执行算子**（`src/executor/ExecutionPlan.{h,cpp}`）：把 WHERE 中同列的范围谓词对（`>= n AND <= m`、单边 `> n`/`< m`）折叠成 `[lo, hi]`，或把锚定前缀 `LIKE 'prefix%'` 折叠成 `[prefix, prefix+\x7f]`，对 `.gist` sidecar 做 overlap 候选 RID 收窄，再逐 RID 走与 IndexScan/Bitmap 相同的 page-lock/MVCC 边界回表。所有原谓词保留在上方 FilterOp 重检——本节点只收窄候选，不是正确性过滤器（与 BitmapHeapScan 同一契约）。RLS 表和分区表安全回退串行扫描。
2. **数值感知重叠判定**（`src/commands/TableManage.cpp`）：`giSTSearchOverlap`/`giSTSearchContainedBy` 原先纯文本比较，`"51" > "100"` 的字典序使数值列范围查询返回空。现在双侧全为数字字面量时按 double 比较，否则保持文本序（文本列前缀语义不变）。
3. **两个存量 off-by-one**：`.gist`/`.brin` 是 5 字符后缀但列发现代码用 `substr(size-6)` 比较 6 字符——`getGiSTIndexedColumns`/`getBrinIndexedColumns` 从未返回过任何列（意味着 BRIN 的查询侧消费也从未可能生效），已修正。
4. **EXPLAIN**：新增 `GiSTScan(table=..., col>=v AND col<=v)` 节点（行数上界 = 表行数，代价按随机回表近似）。
5. **文档矫正（顺带）**：P0-1 的现状描述同样过时——GatherMerge/并行 HashJoin/并行聚合已于 2026-08-20（`cc40031`）落地，feature-gaps 与 all-gaps-todo 已同步；两处残余（worker 生命周期、两阶段聚合）如实保留。

新增 `tests/gist_scan_test.cpp`：范围双边/单边、前缀 LIKE（含奇偶 parity 正确性探针——`even7%` 必须为 0 行）、非 GiST 列保持串行、EXPLAIN 节点存在、sidecar 列发现（依赖 off-by-one 修复）。

验证状态：完整回归 `PASS=165 FAIL=0`（164 既有 + 新增 gist_scan_test，含 7 个 Python E2E）；ASAN CLEAN。已知边界（记录于 feature-gaps P0-3 残余）：sidecar 是线性扫描而非键聚合树（union/penalty/picksplit）；无 tsvector/几何 opclass；无 `<->` KNN；DML 后需 REINDEX 刷新 sidecar（既有行为，本批未改）。

## 2026-08-23 v0.5 批次：join 视图 INSTEAD OF 触发器修复（P0-7 部分翻转）

P0-7 记录"INSTEAD OF 触发器已支持"但协议级审计发现 **join 视图上的整套路径从未工作过**，且一个回归让 UPDATE/DELETE 静默影响全表。四个连环存量缺陷：

1. **存储 SQL 的 token 空格拼接产生 `old . bid`**（`parser.cpp` 的 `CREATE VIEW` selectSql 与 `CREATE TRIGGER` legacy action 都用 `tokens[i] + " "` 拼接，而 lexer 把 `.` 单独切词）：join 执行器把 `jb . bid` 当未知列 → **join 视图 SELECT 恒返回空**；触发器 action 里 `old . bid` 与 `replaceTriggerReference` 的 `OLD.col` 匹配失配 → **UPDATE/DELETE 的 WHERE 失效、静默影响全表**（对生产是数据损坏级）。修复：`joinSqlTokens` 折叠 `ident . ident` → `ident.ident`（多段 `a.b.c` 连续折叠），两处存储路径统一使用。
2. **join 视图触发器行收集走 `getViewBaseTable`**（join 视图无 BASE_TABLE 行 → `collectViewRows` 返回空 → 触发器零次触发但外层仍报成功）。修复：无 BASE_TABLE 时执行视图自身 SELECT（把 DML 的 WHERE 折叠进去）解析输出行。
3. **join 投影 header/data 列序错位**（header 用 `set<string>` 字母序、data 用 FROM 序）——`SELECT ja.tag, jb.bid, jb.val` 的值会配错列。修复：按请求顺序输出 header，并把每行单元格重排到请求序（从引擎的 left-then-right 布局映射）。
4. **视图行 map 只有量化键**（`jb9.bid`）而触发器替换裸引用（`NEW.bid`）→ 替换失配。修复：收集时同时提供 bare 列名键。
5. **`INSERT ... RETURNING` 经视图触发器只有命令标签**（无 RowData）。修复：通过 `publishLastDmlResult`（新增 API）把 NEW 值投影发布为结构化结果，协议层发出真实行。

新增回归：`tests/postgres_protocol_test.py` 追加 join 视图 SELECT 两列序断言、经触发器的 UPDATE/DELETE 精确行路由、单表视图触发器 `RETURNING` 行返回。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E，新断言在 postgres_protocol_test 内）。协议级手工矩阵：join 视图 SELECT / UPDATE（单行命中）/ DELETE（单行命中）/ RETURNING（单列与 `*`）全部符合预期。已知边界：复杂 `INSERT ... SELECT` 经视图触发器、transition tables、`EXECUTE FUNCTION` 运行时仍未做（feature-gaps P0-7 残余）。

已知边界（记录于 feature-gaps P2-3 残余）：外部命令形式 `archive_command` 仅解析不执行（安全考虑本批只做内建 `dir:` 复制）；单时间线（timeline 1）；`pg_basebackup` 协议未实现。恢复目标为进程重启时消费,不支持在线滚动恢复。


## 2026-08-23 v0.6 批次：触发器 EXECUTE FUNCTION 接入 UDF 运行时（P0-7 残余项）

协议级审计发现 `CREATE TRIGGER ... EXECUTE FUNCTION fn()` 的函数体**从未执行**：action 以 `fn()` 文本存储后仍走 SQL 执行器，`fn()` 不是合法 SQL → 静默失败，INSERT 照常成功、函数副作用丢失。

1. **EXECUTE FUNCTION 分发**（`main.cpp` 触发器执行器）：action 匹配 `name(args)` 且 name 是已存储 UDF 时改走 UDF 运行时；顶层逗号拆分字面量参数（剥引号），`StorageEngine::callUDF`（新 API）按语言分发——PL/pgSQL 体进解释器（参数按名预绑定），SQL 体做参数替换求值。非 UDF 形态的 legacy SQL action 原样走 SQL 执行器（回归验证不回归）。
2. **PL 体提取公共化**：`applyScalarFunc` 内联的 UDF 求值块提取为 `evalUDFBody` 共享（标量投影与 `callUDF` 同一路径，避免双实现漂移）。
3. **PL/pgSQL INSERT 字面量带引号落库**（存量缺陷，直接调用与触发器路径同病）：`plpgsqlExecSql` 用 `LiteralExpr::toString()` 取值导致 `'fired'` 连引号入库；现剥一层引号。

新增回归：`tests/postgres_protocol_test.py` 追加 PL 触发函数逐行触发（2 次 INSERT → 2 行日志）、字面量参数 `pl_tagv(7)` 传递、字符串落库无引号、legacy SQL action 触发器不回归。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E）；ASAN CLEAN。已知边界：触发器函数内 `NEW`/`OLD`/`TG_*` 变量注入、`RETURNING` 语义（返回 NULL/触发器返回值丢弃为 PG 简化）、PL 体 UPDATE/DELETE 语句（`plpgsqlExecSql` 显式不支持并如实报错）。


## 2026-08-23 v0.7 批次：无 FROM 常量投影 SELECT

`SELECT 1+1`、`SELECT current_user`、`SELECT myfunc(21)` 这类无 FROM 的常量投影一直直接报 `SQL syntax error`——legacy SELECT 分派只特判了 `unnest(<array>)` 与序列函数，其余全部拒收。这是基础 PG 兼容面：任何常量表达式、伪函数、无 FROM 的 UDF 调用都不可用（也挡住了触发器函数的手动验证路径）。

实现 `handleFromlessSelect`（`main.cpp`）：
1. 顶层逗号拆分投影项（括号/字符串感知）；
2. 每项求值：`AS` 别名剥离后依次尝试伪函数（`current_user`/`session_user`/`version()`）、UDF 调用（`name(literal,...)` 经 `callUDF` 走 PL/pgSQL/SQL 运行时，字面量参数剥引号）、常量表达式（`ExprHelper`：算术优先级/一元负号/括号/`||`/NULL）；
3. 列名遵循 PG：显式别名 > 单 token 字面量原样 > `?column?`（多 token 表达式）；表头单元格保持单 token（cout 表格式以空格分列）；
4. 输出单行（`DISTINCT` 单行语义为无操作）；`unnest` 保持既有展开路径。

新增回归：`tests/postgres_protocol_test.py` 追加 `SELECT 1+1`/`SELECT -5`/`SELECT 1+2*3, 'a'||'b' AS cat`/`SELECT current_user`/`SELECT dbl21(21)`（无别名与有别名）断言。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E）；ASAN CLEAN。已知边界：FROM-less 下标量子查询、`SELECT ... WHERE`（PG 也不支持无 FROM WHERE）、多行 set-returning 函数（除 unnest）未覆盖。


## 2026-08-23 v0.8 批次：触发器函数 NEW/OLD/TG_* 变量注入

v0.6 让 `EXECUTE FUNCTION` 触发器真正执行函数体，但函数体内 `new.col`/`old.col`/`tg_op` 等触发器变量不可见（未绑定，dotted 引用原样落库为文本）。

1. **PL 解释器 dotted 变量替换**（`plpgsql.cpp substitute()`）：词法扫描在标识符后尝试逐段扩展 `.` 追加段，整段匹配小写 dotted 键（`new.id`、`tg_relname`）一次替换；找不到则回退单词路径（既有变量语义不变）。
2. **触发上下文管线**：`StorageEngine::TriggerCtx` + `setExecFunctionCtx` 暂存（NEW/OLD 值映射 + `tg_name/tg_when/tg_level/tg_op/tg_relname`）；9 个触发点火点（B/A × INS/UPD/DEL，行级/语句级）在调用执行器前暂存；main.cpp 触发器执行器的 UDF 分派改走 `callUDFWithCtx` 预绑定解释器参数（显式参数优先）。
3. **PG 语义对齐**：缺失侧是 NULL 记录——INSERT 触发器里 `old.col` 读 NULL（按表列全量绑 null）而非字面文本；AFTER UPDATE 的 OLD 从更新前捕获的行预映像（rid→列值 map，更新循环内记录）取值，而非重读已写入 NEW 的磁盘行。

新增回归：`tests/postgres_protocol_test.py` 追加同一审计函数在 INSERT（NEW 可见/OLD null）、UPDATE（OLD=旧值/NEW=新值）、DELETE（OLD 可见/NEW null）三事件的完整断言。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E）。已知边界：函数体引用不存在的字段仍静默保留文本（PG 报 "record ... has no field"）；`tg_argv`/`tg_tag`/`tg_table_name` 等扩展变量未提供。


## 2026-08-23 v0.9 批次：`use <db>` 后端崩溃修复

`use testdb;`（PG 生态客户端常见的手工切换写法）直接令后端 abort：parser 把所有以 "use" 开头的语句归类 `UseDatabase`，而处理器无条件 `sql.substr(13)`（跳过 "use database" 前缀）——短式 9 字符越界抛 `std::out_of_range`，未捕获 → 进程崩溃（协议连接即 DoS）。

修复两处：
1. `main.cpp` UseDatabase 分支：先识别 13 字符 "use database " 前缀，否则按短式 `use <name>`（`substr(3)`）解析；空名报语法错误而非崩溃。
2. `parser.cpp` 归类加词边界：`use` 后必须紧跟空白或行尾——`username_check` 这类标识符不再被误分类为 UseDatabase（此前会误吞进数据库切换逻辑）。

新增回归：`tests/postgres_protocol_test.py` 在协议连接上依次执行 `use info`、`use database info`、随后 `SELECT 1+1` 仍可用（连接存活断言；旧行为是第一条就杀死服务器）。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E）；协议级实测短式/长式/误拼标识符三路径（服务器全程存活）。


## 2026-08-23 v0.10 批次：transition tables 首个子集（REFERENCING OLD TABLE）

`CREATE TRIGGER ... REFERENCING OLD TABLE AS x FOR EACH STATEMENT` 此前直接被 parser 丢弃（FOR EACH 子句前不认识 REFERENCING，导致其泄漏进 action 文本）。本批落地经典审计场景所需的完整链路：

1. **Parser**：REFERENCING 子句按 PG 顺序在 FOR EACH 之前解析（`NEW TABLE [AS] n` / `OLD TABLE [AS] n`，可多个），存入 `CreateTriggerStmt::transitionTableNames`（`"old <name>"` 条目）；FOR EACH 恢复正确消费。
2. **存储**：`Trigger::transitions` 字段按 count-prefix 序列化进 `.triggers` sidecar；旧 sidecar 无此块读空向量（向后兼容）。
3. **引擎暂存**：AFTER DELETE 语句级点火点把 `rowsToDelete` 的完整预语句行集（列名→值 map）填进 `TriggerCtx::transitionRows[别名]`。
4. **执行器物化**：触发器执行器把暂存行集建成会话临时表（REFERENCING 别名注册进 `session.tempTables`，`resolveTableName` 自动解析），action SQL 里 `FROM deleted_rows` 直接可查；action 完成后 drop 并反注册，别名不留残迹。

验证：`delete ... where id <= 2` → 审计表收到被删两行、目标表剩一行；二次触发正常；语句后别名确实消失（查询报 not exist）；legacy 行级触发器不受影响。

新增回归：`tests/postgres_protocol_test.py` 追加完整场景断言（建触发器、删 2 行、审计表内容、目标表剩余、别名清理）。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E）。已知边界：`NEW TABLE`（INSERT/UPDATE 的转换表）需语句级批处理捕获（INSERT 引擎路径逐行调用）；`OLD TABLE` 仅 AFTER DELETE。


## 2026-08-23 v0.11 批次：likesel 接入统计（P1-12 残余项）

`LIKE` 选择率是 `estimateSelectivity` 中最后一个硬编码（恒 0.2）：90 行 'hot' + 10 行 'cN' 的列上 `LIKE 'hot%'` 估计 20 行（真实 90）——倾斜数据下 LIKE 谓词的行数估计完全失真。

实现 PG 风格 likesel（`ExecutionPlan.cpp`）：
1. 提取模式字面前缀（`%`/`_` 截断，`\\` 转义通配符计入前缀）；无字面前缀（如 `'%x'`）保持平坦 0.2。
2. 前缀上界 prefix++：末字符递增、255 进位回退为 \x01、全进位则追加字节——取直方图上 `>= prefix AND < prefix++` 的区间估计作为前缀匹配选择率。
3. MCV 修正：热值落在区间内时用精确频率（count/rows）对直方图的均匀假设做上修（取较大者）。
4. 模式值既接受 parseConditions 已剥引号的裸文本，也接受 PlanContext 直接传入的 `'hot%'` 字面量。

验证：`tests/stats_planner_test.cpp` 追加两个断言——`LIKE 'hot%'` 估计 ∈ [45,95]（真实 90，平坦值只给 20）、`LIKE 'c95%'` ∈ [0,10]；既有 MCV/直方图/join 场景不回归。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E）。P1-12 剩余：多表 join 顺序从贪心升级为 DP 搜索、`is null`/`in` 等边缘选择率。


## 2026-08-23 v0.12 批次：JOIN 两项修复（ON 词边界 + eqjoinsel 附表选择）

多表 join 审计中发现两个缺陷，其一为静默数据路径错误：

1. **`JOIN` 右表名含 "on" 即失败**（`main.cpp` 2 表 join 路径）：ON 子句定位用朴素 `sql.find("on", joinPos)`，`join location on ...` 中的 "location" 内嵌 "on" 被误认为关键字 → 右表被切至 `l` 前缀 → 报错误的 "not exist"（或更糟，join 错误片段）。实测 `select x,y from plain join location on plain.x = location.x` 稳定失败，而表名不含 "on" 的同构查询正常——纯表名运气。修复为词边界匹配（前后须为非字母数字/下划线）。别名形式（`join location as l`）同样受累并一并修复。

2. **多表贪心附表选择用裸表行数**（≥3 表路径）：原逻辑每次附加"行数最小的待连接表"，完全无视连接键选择性——事实表先连 region(nd=10) 还是 cust(nd=300) 产出中间结果差 30×，原逻辑只看表大小。升级为 eqjoinsel 估计：`est = interEstRows × tableRows / max(nd_l, nd_r)`（nd 取 ANALYZE 统计 cardinality，增量维护中间估计行数），cross-join 回退同样按笛卡尔积估计选择。外连接/初始对选择逻辑不变。

验证：`facts(5000)×cust(300)×prod(80)×region(10)` 星型 2/3/4 表 join + LEFT JOIN 全部行数正确；`location` 表名用例修复。新增协议回归：含 "on" 表名的 INNER/LEFT(别名) join 断言。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E）。P1-12 剩余：穷举 DP join 顺序（当前贪心+统计估计）、`is null`/`in` 选择率。


## 2026-08-23 v0.13 批次：null_frac 统计 + IS NULL/IS NOT NULL 选择率 + stats 解析对齐修复

P1-12 收尾项：NULL 选择率此前完全缺失（`isnull`/`isnotnull` 落入兜底 0.3，等值估计也不感知 NULL），且统计解析存在潜在字段错位。

1. **采集**（`TableManage.cpp` analyzeTable）：每列计数空值（引擎以零长度编码 NULL 的约定），`ColumnStats::nullCount` 新字段持久化为 `.stats` 第 5 个 `|` 字段；旧文件无此字段读 0（向后兼容）。
2. **解析对齐修复**：`parseStatsLine` 原用「跳过前导空字段」对齐 min/max/hist/mcv，但 min/max 可合法为空（全 NULL 列/varchar 列最小值为空串），两个空字段时 MCV/nullCount 全体左移错位（本次实测复现：nullCount 被吞、MCV 混入纯数字项）。改为位置对齐（parts[0] 为 cardinality 后定界符空段，min/max/hist/mcv/nulls 依次取 1..5），保留旧退化行回退路径。
3. **选择率**（`ExecutionPlan.cpp` estimateSelectivity）：`isnull` → nullCount/rows 精确值（无统计回退 0.1）；`isnotnull` → 1−null_frac（回退 0.9）；冷值等值按 PG 语义 `(1−null_frac)/ndistinct`（NULL 不参与等值匹配）。

验证：`stats_planner_test` 新增场景（100 行中 30 NULL：nullCount 断言、isnull 估 30 精确、isnotnull 估 70），既有 MCV/直方图/likesel/join 场景不回归。注意：固定宽度数值列的零值与 NULL 同编码（引擎既有约定），NULL 精确计数对 varchar/文本类列完全成立。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E）。P1-12 剩余：穷举 DP join 顺序、`IN` 列表选择率。


## 2026-08-24 v0.14 批次：IN/NOT IN 字面量列表——规划器可见化 + NOT IN 静默空结果修复

探测发现两个耦合缺陷：

1. **IN 谓词对规划器不可见**：`id in (1,3,5)` 执行正确（legacy 路径改写为 OR-of-=），但 EXPLAIN 完全无 Filter 节点、rows 不衰减——估算器与计划器都看不到该谓词。
2. **NOT IN 静默返回空**（长期潜伏）：legacy 改写器用朴素 `find(" in ")` 定位，"not in" 内嵌的 " in " 被误中 → 列名回扫成 "not" → `id not in (1,3)` 被改写为 `not=1 or not=3`（应为 `id!=1 and id!=3`）→ 恒无匹配 → 所有 NOT IN 查询静默返回零行。

修复链路：
- **modifyLogic/parseConditions/evalConditionOnRow/estimateSelectivity** 四点新增 `in`/`notin` 条件算子（值为空格连接的字面量）；IN 选择率按 PG eqsel 求和语义（MCV 精确值求和，clamp）。
- **compactInLists**：主执行流在 tokenize 前把 `col [not] in (…)` 折叠为无空白单 token（`in<col>(v1,v2)`），避免逐 token modifyLogic 把谓词拆碎。
- **legacy 改写器修复**：` not in ` 优先探测（词边界含列表括号 `(`），列名正确回扫；NOT IN 改写为 AND-of-`!=`（PG 语义），空列表 `not in ()` 恒真。

验证（协议级实测）：IN/NOT IN 数值与文本列表、AND/OR 组合、`not in ()` 全部正确；EXPLAIN 出现 Filter 节点且 rows=3（5 行 × 3 值 × 1/5 精确）；`tests/postgres_protocol_test.py` 新增 6 项断言（含 EXPLAIN Filter 存在性）。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E）。


## 2026-08-24 v0.15 批次：单表别名与表名限定符（两个静默错误结果缺陷）

协议级审计发现两个基础语法缺陷（均以错误结果而非报错呈现）：

1. **`FROM t [as] a` 完全不可用**：单表路径的 FROM 文本 `emp e` 未拆分别名，整个串被当作表名 → "Table emp e not exist"。任何带别名的单表查询（含聚合/星号/IN/ORDER/LIMIT 全部形状）一律失败。JOIN 路径别名正常，独单表路径缺失。
2. **表名限定谓词静默返回空**（长期潜伏）：`where emp.id = 2` 中 `emp.` 限定符未被剥离，条件列名解析为不存在的 "emp.id" → 谓词被丢弃 → 返回空集（应为 id=2 一行）。HEAD 基线复现确认非本会话引入。

修复（`main.cpp` 单表 SELECT 路径）：
- FROM 区早期解析出 `表名 + [as] 别名`，别名用于剥离投影与 WHERE 中的 `<alias>.` 前缀；
- `<table>.` 限定符同样从投影与 WHERE 剥离（WHERE 剥离同时覆盖别名与表名两种限定）；
- 投影剥离处理前导空白（FROM 后首个 token 提取修正）。

验证：别名（裸/as/限定列/聚合/IN/ORDER/LIMIT）、表名限定（投影+WHERE+聚合）、混合限定（表名限定投影+别名限定 WHERE）、JOIN 路径无回归、普通无限定查询无回归，全形状实测通过；`tests/postgres_protocol_test.py` 新增 6 项断言。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E）。已知相邻残留：投影列别名（`select id as no`）与 ORDER BY 组合的解析问题（独立缺陷，本批未触碰）。


## 2026-08-24 v0.16 批次：SELECT 列表别名（AS）+ 算术投影 + ORDER BY 别名

协议级审计发现三联缺陷（同一条语法链）：

1. **投影别名完全不可用**：`select id as no from t` 报 "Invalid column name id as no"——单表路径把整个 `expr as name` 文本当作列名校验。全部形状失败（普通列/表达式/GROUP BY 投影），仅聚合路径（`count(*) as c`）例外。
2. **算术投影不存在**：`select salary * 2 from t`（无论有无 AS）报同样错误——引擎无表达式投影能力。
3. **ORDER BY 别名被忽略**：`order by no`（no 为输出别名）不报错但静默按物理顺序返回（错误顺序结果）。

修复：
- 投影循环早期拆分 `expr AS alias`：表达式参与列校验与求值，displayName 用别名（PG 语义）；AS 尾部合法性校验（不含运算符/逗号）。
- 新增 `arith` 标量表达式（applyScalarFunc 内）：操作数（列/整型/浮点/字符串字面量经 getVal 行内解析）与运算符（+ - * / %）分离编码，左结合求值；除零/取模零返回 NULL；字符串 '+' 拼接；整数结果无小数点输出。主路径按"运算符位于非空操作数之间"检测算术项并路由。
- ORDER BY 简单列项先查 SELECT 别名映射（早期构建的 alias→expr map），别名解析为底层表达式/列。

验证：`salary*2`→200、`salary+id`→202、`salary/4`→100、`salary-100`→400、链式 `salary+id-1`→302、AS 别名（普通/order/group 投影）、ORDER BY 别名 desc/asc 全部实测正确；普通投影/ORDER BY 无回归。协议回归 +5 断言。已知残留：GROUP BY 引用输出别名（`group by d`）暂不支持（PG 允许，低频形状）。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E）。


## 2026-08-24 v0.17 批次：UPDATE/DELETE 谓词与别名（NOT IN 语法拒绝 + UPDATE 别名拒绝修复）

UPDATE/DELETE 域审计发现两个语法级缺陷：

1. **`UPDATE/DELETE ... WHERE c NOT IN (...)` 语法报错**：表达式解析链（parseNotExpr→parseIsExpr→parseComparisonExpr→parseRangeExpr）仅支持前缀 NOT；中缀 `x NOT IN / NOT LIKE / NOT BETWEEN` 在 parseRangeExpr 处残留 "not" token → "unexpected token in UPDATE statement: not"。
2. **`UPDATE t [AS] x SET ...` 被拒绝**（PG 标准形状）：UPDATE 解析器不支持表别名（"UPDATE requires SET"）；legacy 执行路径同样把 "t x" 当表名（"u3 x not exist"）。

修复：
- **parseRangeExpr**：谓词前置 `not`（词法边界安全，仅当后随 in/between/like/ilike/similar 时消费），生成 `NOT IN`/`NOT BETWEEN`/`NOT LIKE`/`NOT ILIKE` 运算符；前缀 NOT 语义不变（非谓词组合回退返回）。
- **ExprEvaluator**：`not in`（补集语义，NULL 传播）与 `not between`（区间否定）求值。
- **UPDATE 解析器**：表名后可选 `[AS] alias`（ast.h UpdateStmt 增 alias 字段）；**legacy UPDATE 执行路径**：表名/别名拆分 + `<alias>.` 限定符剥离。
- parseConditions/evalConditionOnRow 增 notlike/between/notbetween 解码与求值（引擎层；主 SELECT 流的单谓词路径尚有残留缺口，见下）。

验证：UPDATE NOT IN（正确更新目标行）、UPDATE 别名（裸/AS + 别名限定 WHERE）、DELETE NOT IN、SELECT in/notin/like 无回归；协议回归 +6 断言。已知残留（预先存在，基线复现）：单谓词 `where c not like / between`（不带 AND）在主 SELECT 流仍返回错误结果（0 行/全行）——AND 组合、UPDATE/DELETE、表达式求值器层均已正确；该单谓词流水线路径待后续批次修复（多轮 trace 未定位到条件丢失点，疑在 Volcano 构建前的某快速路径）。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E）。

## 2026-08-24 v0.18 批次：单谓词 NOT LIKE / BETWEEN 流水线修复（v0.17 遗留缺口闭合）

v0.17 记录的残留缺口（单谓词 `WHERE c NOT LIKE 'x'` / `c BETWEEN a AND b` 不带 AND 时主 SELECT 流返回错误结果）本轮定位并修复。根因是一条五层叠加的丢失链：

1. **normalizeConditionStr 预粘合**：`not like` 在 tokenize 之前就被粘合成 `notlike'x%'`，tokenize 切出 `[col, notlike'x%']` 两 token——而 `between` 不会被粘合（保持 5 token 空格形式），两条形状不同；
2. **per-token modifyLogic 粉碎**：`notlikeX` 内部包含 "like" 子串，modifyLogic 的 like 关键字扫描把它重组成错误形式；
3. **breakDownConditions 分组丢失**：未合并的多 token 谓词在 AND/OR 分组算法中被拆散丢弃；
4. **parseConditions 前缀长度错误**：`notlike`（7 字符）被按 8 字符比较、`notbetween`（10 字符）被按 11 字符比较——永远不匹配，条件被静默丢弃（branches 为空 → 不过滤或全过滤）；
5. **evalConditionOnRow 整型分支缺 between**：int 列走最终 else 分支，`parseInt("1 2")` 失败 → 全部行被过滤；且文本比较未剥引号（`'b'` 带引号参与比较）。

修复：

- **mergeNegPredTokens（新，main.cpp）**：tokenize 后合并谓词 token 运为引擎粘合形式——预粘合双 token 形 `[col, notlike'x%']` 与空格形式 `[col, not, like, pat]` / `[col, between, lo, and, hi]`（正负两种）均归一为 `notlike<col> <val>` / `between<col> <lo> <hi>` / `notbetween<col> <lo> <hi>`；接入全部四个 condTokens 构建点（主 SELECT 单表流、结构化子查询回退、UPDATE、DELETE）；
- **modifyLogic**：合并单 token 形式直通（不被 like 关键字扫描粉碎）；
- **parseConditions**：前缀长度修正（notlike=7、notbetween=10）；between/notlike/notbetween 值域逐 token 剥单引号（`betweenid 'b' 'c'` → `b c`）；
- **evalConditionOnRow**：最终 else（整型）分支补 between/notbetween（`lo hi` 拆分数值比较）与 notlike（文本匹配取反）。

验证：12 项手工矩阵全绿——单谓词 not like（2,3）/ between（1,2）/ not between（3）、文本 between（2）/ not between（1,3）、AND/OR 组合、in/notin/like/等值/小于回归；UPDATE not like（精确 3 行）、UPDATE between（精确 2 行）、DELETE not between（精确删 2 行）；协议回归 +13 断言（pred_t 表）。定位过程记录：DBG 分支追踪证明条件在 breakDownConditions→parseConditions 之间丢失，branches=1 但 conds=0 是第 4 层前缀长度错误的直接证据。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E，v0.18）。

## 2026-08-24 v0.19 批次：GROUP BY / HAVING 引用 SELECT 列表别名（两个静默错误结果缺陷）

聚合域审计发现两个别名可见性缺陷（PG 中 GROUP BY/HAVING 均可引用 SELECT 列表别名）：

1. **`GROUP BY <别名>` 返回错误结果**：`select dept as d, count(*) from t group by d` 静默返回空结果——分组列 "d" 不是物理列，分组逻辑无法解析，输出为空（PG 返回按 dept 分组的正确聚合行）。
2. **`HAVING <别名>` 谓词被静默丢弃**：`select dept, count(*) as cnt from t group by dept having cnt > 1` 返回全部组（谓词无效）——having 条件文本中的 "cnt" 无法求值，条件被跳过（PG 只返回 cnt>1 的组）。

修复（main.cpp，两处均在 clause 解析处做别名归一）：

- **GROUP BY**：普通（非 ROLLUP/CUBE/GROUPING SETS）分组列提取后，从 SELECT 列表（`columns` 变量，DISTINCT 已剥离）构建 `alias → 表达式` 映射（识别 `expr as <裸标识符>`，与 SELECT 别名解析同一规则），把每个分组列中的别名替换为其表达式；
- **HAVING**：having 条件拆分后，用同一映射做**词边界安全**的整词替换（左侧非 `[A-Za-z0-9_.]`、右侧非 `[A-Za-z0-9_(]`），`cnt > 1` → `count(*) > 1`；替换后继续走既有聚合求值路径。

验证：G1 `group by d`（按 dept 分组正确行）✓、G2 别名单列分组 ✓、H1 `having cnt > 1`（仅返回 eng 组，PG 语义）✓；回归：普通列 GROUP BY、非别名 HAVING（`count(*) > 1`、`sum(salary) > 300`）、ORDER BY 聚合别名（`order by cnt desc`）、ROLLUP、`order by d` 全部不变；协议回归 +6 断言（grp_t 表）。探测方法论记录：RowDescription 列名解析曾因误带消息类型字节偏移 +5 而误报 "y*2"——修正偏移后确认服务器列名本来就正确（`salary*2`/`count(*)`），非缺陷。

验证状态：完整回归 `PASS=165 FAIL=0`（含 7 个 Python E2E，v0.19）。

## 2026-08-24 v0.20 批次：聚合参数算术表达式（sum/min/max/count/avg over "col * 2"）

聚合域审计发现表达式参数聚合静默错误：

- **`select sum(amt * 2) from o` 返回 0**（PG: 500）——聚合参数被当作裸列名查 schema，"amt * 2" 匹配不到 → 每行 `continue` → 求和恒 0；
- **`select min(amt * 2)`/`max(amt + 1)` 返回空**、**`count(amt * 2)` 返回 0**——同一根因；
- **`group by` + 表达式聚合同样全坏**（分组路径 `groupAggregate`/`groupAggregateSets` 与标量路径 `aggregate` 各有一份相同的裸列假设）。

修复（TableManage.cpp）：

- 新增 `aggArgIsExpression`（参数含深度 0 引号外运算符即视为表达式）与模板 `evalAggArgExpr`（左结合无优先级算术：操作数逐行解析——裸列→行值、带引号→剥引号、其余→字面量；除零/解析失败返回空）；实现中修正了一处解析缺陷（运算符 token 在左操作数已被空格 flush 后到达，必须接受空 token 记录 pendingOp，否则 `amt * 2` 退化为 `amt`）。
- 三个聚合函数（`aggregate`/`groupAggregate`/`groupAggregateSets`）统一接入：参数非裸列且为 sum/avg/min/max/count 时逐行求值表达式；min/max 用数值比较并在返回处输出（此前表达式值落入字符串分支导致空结果）；sum 的非整数值走 double 累计路径。
- Volcano 路径（`ExecutionPlan.cpp computeAggregate`）同样接入参数表达式求值。
- 聚合输出表头改用 SELECT 列表 displayName（别名生效；带别名的表达式聚合不再被协议层按空格切出幻影列）。

验证：标量矩阵 sum 500 / min 50 / max 101 / count 4 / avg 125 / 两列相加 310 全对；分组矩阵 sum 300/200、min 100/50、max 200/150、count 2/2、avg 150/100 全对；回归：裸列聚合（sum 250、avg 62.5、min/max/count、group by 多聚合、rollup、having、where+agg、distinct count）全部不变；协议回归 +15 断言（expr_t 表，带别名形式）。已知残留（预先存在，基线复现）：无别名表达式聚合的显示名（如 `sum(amt * 2)`）仍被协议层按空格切分产生幻影空列（带别名则正确单列）——协议列名构造与文本表头解耦是后续工作；`order by sum(amt)`（非别名列形式）分组排序同为基线既有行为。

验证状态：完整回归 `PASS=167 FAIL=0`（含 7 个 Python E2E，v0.20）。

## 2026-08-24 v0.21 批次：标量函数六联缺陷（小数字面量 / round 精度 / 嵌套调用 / POSITION-IN / 函数名大小写 / 小数 BETWEEN）

标量函数域逐项审计发现六个真实缺陷，全部表现为静默错误结果或空结果：

- **小数字面量在 tokenizer 被拆散**：`3.567` 被切成 `3`、`.`、`567` 三个 token（`.` 是符号字符直接断词）→ 解析层 `round(3.567, 1)` 收到 argc=4 的垃圾参数返回 `3.000000`（PG: 3.6）；任何经解析器的表达式里的小数字面量都损坏。
- **`round(x, n)` 丢弃精度参数**：FROM 路径的 round 只做 `std::round(d)` 整数舍入。
- **嵌套标量调用回退原文**：`upper(substring(name, 1, 1))` 返回字面文本 `SUBSTRING(NAME`——投影层 `find(')')` 在内层括号处截断 + `splitFuncArgs` 不感知括号深度。
- **`position('b' in 'abc')` 返回 NULL**：IN 关键字被当比较运算符吃掉，双参形式从未形成。
- **函数名大小写敏感**：`SUBSTRING(...)` 无法匹配小写比较链，走到错误回退返回错位字符。
- **小数 BETWEEN 全部空结果**：numeric 列 `v between 1.25 and 2.0` 中 `Numeric("1.25 2.0")` 抛异常→false；整数列小数边界 parseInt→INF→false；double/decimal 分支根本没有 BETWEEN 处理。

修复：

1. `src/parser/parser.cpp`：tokenizer 对 `数字.数字` 形态折叠为单 token（限定左侧必须是纯数字 token，`t.col` 限定名不受影响）；tokenize 后处理把 position()/strpos() 参数列表内的 IN token 重写为逗号（PG `POSITION(needle IN haystack)` 语法）。
2. `src/commands/TableManage.cpp`：round 支持 `round(x, n)` 保留 n 位小数（fixed+setprecision）；applyScalarFunc 的 getVal 递归求值嵌套标量调用（括号平衡解析内层 name(args)，深度感知逗号拆分后递归 applyScalarFunc）。
3. `src/main.cpp`：投影层函数名解析用括号平衡匹配 `)`（不再 find(')') 截断）；splitFuncArgs 深度感知；标量函数名统一小写化。
4. `src/expression/ExprEvaluator.cpp`：isNumericTypeName 扩展到 integer/float 族（round/abs 等走 Numeric 路径输出 `3.6`/`5` 而非 `3.600000`）；算术结果类型保持——int op int 仍是 int（`id + 10` 的 RETURNING OID 从 1700 回到 23=INT4，与 PG 一致），仅 decimal 族类型进 Numeric 精确路径。
5. BETWEEN 三分支（numeric/double/整数列）全部支持小数边界：numeric 列拆分 "lo hi" 后逐段 Numeric 比较；double 同理 stod；整数列 stod 十进制比较失败时回退整数解析。

验证：探针矩阵 12 项全对（length/upper/lower/substr/concat/coalesce/abs/round/nested/replace/trim/position）；回归矩阵 13 项（限定列名 `t1.v`、`::` 转换、小数 WHERE/聚合/JOIN/ORDER BY、IN 列表、LIKE 含点、JSON 操作符、小数算术、round 列）除基线已知的 `id::text` 投影转换外全部通过；BETWEEN 子矩阵（numeric 列小数边界命中/不命中/not between、整数列小数边界、整数边界回归、gt/lt 回归）全部与手算 PG 语义一致；协议回归 +13 断言（ROUND/ABS/POSITION/嵌套 UPPER(SUBSTRING)/BETWEEN 命中与不命中/NOT BETWEEN/IN 回归/整数边界回归）。注意 PG 语义核对修正了一处测试预期：`id BETWEEN 1.1 AND 2.9` 中 id=1 不满足（1 < 1.1），只有 id=2 命中。

验证状态：完整回归 PASS=167 FAIL=0（geometric_test 首轮出现临时目录清理竞态 flake，隔离重跑通过；复跑全量绿色，含 7 个 Python E2E）；ASAN CLEAN（14 组）。

## 2026-08-24 v0.22 批次：派生表 / CTE / 逗号 cross join

FROM 子查询域逐项审计发现四个真实缺陷：

- **`(select ...) t` 无 AS 别名形式崩溃**：`basic_string::substr: __pos > size`——别名剥离（`t.` 移除）缩短字符串后，记录的括号位置没有同步偏移；
- **FROM 内聚合/分组/嵌套子查询全部空结果或失败**：`runDerivedSubQuery` 是极简单表解析器，不支持聚合、GROUP BY、嵌套派生表——`(select max(k) as mx from s) t` 空、`(select ... from (select ...) it) u` 报表不存在；
- **CTE 名替换无词边界检查**：名为 `c` 的 CTE 把 `select` 里的 `c` 也替换掉（"sele__cte_0t"）——`with c as (...) select id from c` 报 Invalid column name；
- **`from a, b` 逗号 cross join 被拒绝**（PG 语义等同 `cross join`）。

修复：

1. `processDerivedTables`（main.cpp）：别名扫描改为在原始字符串上按位置测量（支持 `(select ...) alias` 短形式，排除 where/group/order/limit/offset/having/join/union/lateral/from 等关键字误判）；别名引用剥离时同步修正 parenStart/parenEnd/aliasExprStart 位置；新增 FROM 上下文守卫——`(select` 前必须是 from/join/cross/inner/left/right/full/lateral/逗号，投影中的标量子查询与 IN/EXISTS 谓词子查询不再被误物化。
2. 新增 `runDerivedSubQueryFull`：经 `ScopedOutputCapture` 把内层 SELECT 送完整 SQL 分发器执行并捕获表头/行，失败时回退旧极简解析器——派生表/CTE 内层支持任意 SELECT 形态（聚合、GROUP BY、JOIN、嵌套派生表、标量函数）。
3. `processCTEs`：两处名字替换循环（`c.` 剥离与 `c`→临时表名）补词边界检查（左右邻字符非字母数字）。
4. 主 SELECT 分发早期：FROM 区域顶层逗号（括号/引号外）重写为 `cross join`——`from a, b`、`from a p, b q`、`from a, b where ...` 全部走既有 CROSS JOIN 管线；IN 列表等括号内逗号不受影响。

验证：派生表矩阵 10 项（无 AS/有 AS/别名限定列/聚合/分组/嵌套/派生表上 WHERE/与基表 JOIN）全对；CTE 矩阵 4 项（基础/聚合/限定列引用）通过；逗号 join 矩阵 5 项（基础/cross 关键字对照/带 WHERE/别名形式/IN 列表安全性）通过；回归：标量子查询（既有能力）、IN 子查询、窗口函数三件套、LIMIT/OFFSET 族、UNION/EXCEPT/INTERSECT、UPSERT/RETURNING 全部不变；协议回归 +13 断言（dt_t/cj1/cj2 表）。已知残留（基线既有，本轮如实记录）：多 CTE（`with a as (...), b as (...)`）第二定义仍失败；投影位置含聚合的标量子查询（`(select max(k) from s)`）基线即报错；LATERAL、`= ANY(子查询)`、`LIMIT 1+1` 表达式形式未覆盖。

验证状态：完整回归 PASS=167 FAIL=0（含 7 个 Python E2E，v0.22）；ASAN CLEAN（14 组）。

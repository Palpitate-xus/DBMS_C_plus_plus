# 兼容版本契约

> 历史基准原始数据不随 Git 仓库提供。本文中的 `benchmarks/` 路径仅指可能存在的本地可选证据；新克隆仓库没有这些文件，原始结果只能在另行取得数据后复核。文中的测试与性能摘要仍保留。

## P42 当前事务提交补充（2026-11-07）

无写入事务仍保留内部txid、快照、SSI和锁生命周期；同一物理WAL owner位置未变且所有保守条件满足时，不为未被持久变更引用的保留ID写COMMIT或CLOG。不能据READ ONLY或SQL前缀省略写事务同步。目录失效与序列写入单独标记，UNLOGGED DDL／TEMP提交动作及检测到共享WAL推进时保留原路径。没有引入异步写入提交，没有改变磁盘／协议编码或校验。

新95条SQL与旧版回复逐字节一致，完整数据与实际崩溃恢复通过；原PG描述符来源差异仍可见，未声称整体PG等价。仍C++17／O2，根目录默认程序未提升。参见[最新边界、失败和性能证据](performance-report-2026-10-30.md#no-effect-transaction-publication-20261107)；原273和既有暂缓保持，Goal active。

<!-- P41 large-varchar-toast-search begin -->
<a id="large-varchar-toast-search-current"></a>
## VARCHAR逻辑长度与新旧元数据边界

新增VARCHAR逻辑字符长度元数据：显式长度支持1..10485760字符，未指定长度使用独立的无界标记，名义物理dsize仍不超过65535。列声明、pg_attribute／RowDescription、普通写入／更新、数组元素、domain／generated赋值、ALTER及项目逻辑导出使用真实长度。标量不再把65535字节误当成字符上限；已验证70000字符／245000字节UTF8和1MiB无界值。内部无界字节上限为1073741819，但没有实际验收1GiB值或整个PG字符类型族。CHAR旧限制、完整encoding／collation语义仍开放。

旧typeMod=-1按原dsize字符上限读取，不从65535猜测成无界；新显式正长度和私有-2保存在已有ITM1扩展，-2输出PG时为-1。旧数据可以由新版读取；旧程序没有识别这些新VARCHAR修饰符，不能承诺降级读取新写的schema。旧无界意图缺少可判定来源时不自动迁移，须明确新DDL／ALTER。名义dsize、SQL字符数和实际UTF8字节数分别处理。没有改变元组布局、校验、TOAST指针格式或WAL格式；仍是项目自定义存储，不是PG物理格式等价。

新141条VARCHAR SQL在独立PG18.6、旧bf9、候选C6均完整执行，三角色共423条核心SQL、305行／1249单元／132 NULL、228,538,937字节；10个正常Main0及2个预期SIGKILL -9。全值、顺序、重复数、NULL、状态／tag／Ready和全部七个描述字段与实际PG核对，关系OID只用各自真实探针映射。C6普通数据要求通过，仍有17个pg_class来源描述差异，以及3个大常量ADD DEFAULT的PG实现限制差异：PG对70000字节常量报54000，随后读取／DROP列报42703，项目接受并正确存取；这些差异保持开放，strict PG=false。137条V1参考模型的4个假设被真实PG否定，首次actual1及全部原始数据保留；141条V2在任何候选执行前冻结，另加4097字节默认值成功控制，没有按候选结果降低要求。

相关48个原有正式测试按当前要求：46个actual0；toast_test仍在604行 !corruptScan.open() 断言actual -6、后续NOTRUN；declared_type_cold_binding_test旧bf9与C6都actual1，12项中2个初始catalog条件失败，保留OPEN。全套未运行，full suite=false。原type_registry_test把65536声明当错误，旧源码在C6实际-6的首次观察保留；依据已冻结PG声明范围，扩充为65536／10485760成功及10485761／非法修饰符拒绝，更新源码在旧版-6、C6为0。没有删除负向或改掉其余要求。加上两个新测试，共50个不同正式测试，当前48个0和上述两个失败；另外的历史原断言失败单列保存。旧26项对照来自相同bf9正常核心／源码的P40实测，另外22项旧版在本批真实重跑；70个原源码终态及全库文件／模式独立核对。

所有正常窗口关闭后，项目DUMP扩展补测15条完整SQL、3个正常Main0：有界／无界VARCHAR及两类数组的真实逻辑导出，在全新数据库恢复并再次冷开，全值／NULL／全部七个描述字段正确。只验收这组类型声明与值，不声称项目DUMP已完整保留所有PG对象／约束。

[完整证据与保留差异](performance-report-2026-10-30.md#large-varchar-toast-search-20261107)。不关闭整个类型或存储族，用户暂缓范围不变。
<!-- P41 large-varchar-toast-search end -->

<!-- P40 vacuum-full-relation-snapshot begin -->
<a id="vacuum-full-relation-snapshot-current"></a>
## 当前FULL刷盘、备份与恢复边界

普通VACUUM FULL在备份前与重建后，只刷新目标逻辑关系及其分区／TOAST缓存，避免把其他表的脏页一并刷出。物理文件已同步，且在任何重建写入前全部unlink：这些文件优先用硬链接保留旧inode作为回滚映像，失败则完整复制；schema和specialized-dirty元数据继续独立复制。每份备份的fsync、完整大小／模式／SHA256校验，以及manifest、pending、替换文件／schema、WAL、COMMIT和undo的持久化顺序全部保留。本批只改TableManage.cpp的vacuumFull，没有修改全局缓存、index_file、WAL格式、CRC或恢复授权规则。

新增vacuum_full_relation_snapshot_test，以完全相同源码链接旧c8ca与正常C1核心。两个模式各有256行目标表／删除后16行、12行无关表、真实脏缓存、主键／二级／hash／bloom索引、70003字节以上TEXT、UTF8、空串／字面NULL／SQL NULL。旧版父actual1：6个优化断言失败，两次setup为1、四次冷开均0；所有热态／冷态完整值和RID NULL位图已正确，不是旧版数据损坏复现。C1父actual0且6子阶段全部0：成功模式实际10份同inode／nlink≥2物理备份；故障钩子对10次硬链接实际返回EXDEV，随后完整复制回退并通过热态及两次新进程冷开。无关堆同步2→0，无关脏页保留给自身owner，之后自身flush正常完成。所有拦截实际调用真实系统同步，未跳过fsync；EXDEV是显式故障控制，不冒充真实跨文件系统部署验证。

原22、62、72、28条SQL及实际wide OID控制全部重新执行，共185完整请求／2588行／11613单元／969NULL／45,817,233请求回复字节，8个正常Main0和1个预期SIGKILL -9，2315个闭库文件核对。全部完整请求与回复逐字节等于P39真实c8ca观察，再由独立解码器检查完整值／NULL／全部T7／错误／NOTICE／tag／Ready。原PG18.6参照逐字节复用，未声称本批新跑PG；62条中的9项、72条中的7项、28条中的3项SQL子分区差异继续开放，strict PG=false。

六种FULL专属有限故障控制在C1均首次尝试通过：schema发布前强杀、发布后提交前强杀、COMMIT后清理前强杀、发布报错后回滚schema期间再次强杀、提交后清理失败、回滚后清理失败。共120条完整SQL与4条真实中断FULL、10400行／48966单元／3696NULL／64,393,755字节，4个预期SIGKILL -9、8个正常Main0，6285个实际映像文件，一个预期XX000。6087条／47,972,984字节WAL独立核对CRC／prev链、COMMIT／ABORT／未提交边界和代次不可复用；两种清理失败后的后续DML与显式CHECKPOINT顺序及冷开全值正确。使用P39已接受V2钩子的同一字节，仅初开进程插桩，冷开正常C1；P39原V1漏拦截remove的失败属于历史保留，不是本批重试，未覆盖原记录。

本批仍只验收未来普通关系FULL的有限重写／关闭／强杀／回滚恢复。不能修复已被旧FULL写坏且没有新pending／退休授权的旧集群；P38实际损坏库继续保留。跨FULL的PITR／物理基备未验收；数据库排他锁范围仍大于PG表级锁，未验收分区FULL冷开或高并发等价。硬链接需要文件系统支持，实际失败会走复制回退；不宣称所有平台／文件系统已验证。原15项安全／TDE及额外用户暂缓范围保持，这六种FULL故障不是已暂缓的通用IO／分配器／保存点矩阵，也未恢复执行图／child-provider或retained planning／portal工作。

正式测试库存2402个文件（1135顶层／1140总C++），2401个旧测试逐字节不变；注册E2E684／独立E2E664不变。250个生产文件只改TableManage.cpp，正常CMake C++17/O2 C1为bf9ad7ae，旧c8ca0215、更早84bbadb4；61核心对象冻结。默认425165af不变。754个Markdown路径、725份保护记录保持，只在已有8文档插入说明。原273项和22 complete／166 partial／70 unverified／15 deferred_by_user不变；289条历史保持，新增partial第290条；IDX-07仍unverified/evidence[]。Goal active，直接master提交，未推送或替换默认程序。下方各批是各自历史程序的记录。 [完整报告](performance-report-2026-10-30.md#vacuum-full-relation-snapshot-20261107)。
<!-- P40 vacuum-full-relation-snapshot end -->

<!-- P39 vacuum-full-wal-generation begin -->
<a id="vacuum-full-wal-generation-current"></a>
## 当前FULL恢复能力与边界

修复普通VACUUM FULL仍沿用旧物理身份、冷启动把旧页镜像重放到压缩后堆的问题。每次重写分配新物理代次；同一内部事务记录出生、旧代次退休意图／完成及COMMIT。替换前先持久化只属于该关系的原文件、完整模式／大小／SHA256清单及pending记录，替换文件与schema全部落盘后才提交。失败和未提交崩溃先恢复旧文件／schema，再重放WAL；已提交只清理备份。完成回滚的durable receipt防止遗留备份覆盖后续已提交DML。旧09 schema仅在FULL成功时发布新代次及专门的0x23完成记录，普通旧名称重用仍拒绝错误归属。没有放宽CRC、物理身份或页缺口校验。

原先因冷开退出-6中断的同一旧库22条SQL计划，现在C1完整执行22条／2130行／9813单元／738NULL／12,433,869请求回复字节，两个正常Main实际0，944个闭库文件核对。初开16条完整请求和回复与P38旧84bb原观察逐字节一致，新增完成原先未运行的6条冷开。原始旧库种子334个名字／模式／字节不变；P38原失败、被旧观察器遮蔽的旧终态及缺失阶段保持原记录。

六种FULL专属有限故障控制全部经独立完整值／NULL／全部T7／错误／tag／Ready核对：schema发布前强杀、发布后提交前强杀、COMMIT后清理前强杀、发布报错后回滚schema期间再次强杀、提交后清理失败、回滚后清理失败。共120条完整SQL、4条确实发出后被中断的FULL、10400行／48966单元／3696NULL／64,393,755字节，4个预期SIGKILL -9和8个正常Main0，6285个实际映像文件；一个预期XX000保留。6087条WAL／47,972,984字节独立CRC与prev链核对，并核实各自COMMIT／ABORT／未提交及不可复用代次。两种清理失败后继续UPDATE，再显式CHECKPOINT，检查点确实晚于该DML提交；冷开值未被旧备份覆盖。只在初开Main加载版本化syscall钩子，冷开使用未插桩的同一正常C1。

原62、72、28条SQL及一条实际wide OID控制在本批C1重新执行，共163请求、458行／1800单元／231NULL／33,383,364字节，6个正常Main0和1个预期SIGKILL -9。所有完整请求与回复均与P38实际84bb逐字节一致，独立第二解码器核对全部值、NULL、T7、错误、NOTICE、tag和Ready；复用的是已认证归档中的真实PG18.6参照，没有本批新跑PG的声明。原62的9项差异、72的7项差异及28的3项SQL子分区差异继续保留，strict PG=false。

本批验收只覆盖未来普通关系FULL的有限重写／关闭／强杀／回滚恢复情形。不能修复已被旧FULL写坏且没有新pending／退休授权的旧集群；P38实际失败库仍保留。跨FULL的PITR／物理基备重建未验收，不能仅凭新生命周期记录重建重写后的完整堆。内部事务暂时使用数据库排他锁，再取表锁和cache锁，并非PG表级并发等价；热态分区既有回归通过，不宣称分区冷开闭环。原始15项安全／TDE暂缓与额外用户暂缓记录保持；这六项不是已暂缓的通用IO／分配器／保存点故障矩阵，也未修改执行图／child-provider、retained planning／portal或CPU-sort interrupt。

正式库存2401个测试文件（1134顶层／1139总C++），2399个旧测试逐字节不变，注册E2E684／独立E2E664不变。源码250个文件，正常CMake C++17/O2候选C1为c8ca0215，旧84bbadb4、更早56aad0c0；默认425165af不变。754个Markdown路径和725份保护记录保持，只在8份已有文档插入本批说明。原273项和22 complete／166 partial／70 unverified／15 deferred_by_user不变，既有288条历史保持、新增partial第289条；IDX-07仍unverified/evidence[]。Goal active，直接master分批提交，未推送或替换默认程序。下方各批均为对应历史程序的记录。 [完整报告](performance-report-2026-10-30.md#vacuum-full-wal-generation-20261107)。
<!-- P39 vacuum-full-wal-generation end -->

<!-- P38 scalar-text-logical-limit begin -->
<a id="scalar-text-logical-limit-current"></a>
## 2026-11-07：标量TEXT长值及实际兼容边界

修复标量TEXT把持久化名义宽度65535误当逻辑值上限的问题：统一的字符字节限制函数对非数组TEXT使用1,073,741,819字节上限，并让旧式UPDATE校验也使用该函数。既有TOAST读回、插入和行构造因此一致处理完整长值；表结构dsize仍为65535，内联引用和存储格式不变。实际验证到1MiB，未进行接近1GiB的端到端容量验证。旧数据可由新程序读取；旧程序不能读取新写入的超64KiB TEXT，不能据此承诺降级后的值兼容。

新增72条SQL分别在真实PG18.6、旧版56aad0c0和当前程序执行：216条核心请求、507行、1935单元、80NULL、73,199,459请求／回复字节，7个Main正常0、2个主动SIGKILL终态-9。独立解码器核对完整值、NULL、全部T7、tag、Ready、错误及NOTICE，当前目标TEXT用例通过；严格PG比较仍有7项差异：4项系统目录列来源OID元数据，以及大声明／无界／多字节大VARCHAR的3项差异。初轮强杀前已删除1MiB行，其最大存活TEXT为200006字节；该组1MiB重写后的冷开来自正常关闭，不把它计作1MiB崩溃恢复证据。

实际旧版30412452创建、正常闭库并冷开验证的after-DELETE种子，334个名字／模式／字节在本批前后完全一致，固定／单长／多长字段人口241／241／113。另做不含FULL的18条普通维护控制，每角色18条，共36请求／3070行／14670单元／1110NULL／16,337,172字节，4个Main正常0；当前在旧模式中写入70003字节及1MiB TEXT后，普通VACUUM和新Main冷开全值／T7通过，未改动固定／单长字段完整wire。再以相同原始种子做独立18条崩溃控制，共36请求及相同全值工作量，2个主动SIGKILL -9、2个新Main正常0，确认提交后完整1MiB TEXT存活并读回。两项控制各有1600个闭库／崩溃映像文件核对；它们不替代下面失败的原始FULL测试。

原62条SQL重新用旧版和当前执行，共124请求、4个Main正常0，完整请求与回复逐字节一致，真实PG18.6原始oracle和既有9项差异保持。P37原28条维护SQL计划字节不变，另在真实PG／旧版／最终C2重新执行，共84核心请求、144行、553单元、129NULL、2,873,498字节、6个Main正常0；额外4条实际OID／PG版本控制单列。原TEXT70003插入、立即读回、冷开读回3个确切缺口现与PG完整值和T7相符，其余25条旧／新完整回复字节不变。28条中24项值／状态／tag匹配、1项按各库真实正OID核对，仍有SQL子分区创建0A000、子表目录身份及冷开身份缺行3项差异，strict PG=false。

新发现的普通VACUUM FULL／WAL恢复缺陷仍OPEN，属于下一项必须处理的非暂缓工作。原始旧模式22条矩阵，两版各完成初开16条并正常退出0；当前16条全值正确，但之后冷启动失败，两版各6条冷开SQL未运行。旧版首次观察器的cleanup断言遮蔽了已退出子进程的wait状态，因此其冷开actual exit未记录；修正观察器后只继续原先未启动的当前角色，实际冷开退出-6，未重跑或补写旧版状态。提前运行的独立审计也因缺少完整receipt实际退出1，失败保留。独立CRC32C核对定位LSN6784528的有效8192字节页镜像：cv_sig、物理身份3、旧块22；FULL后同身份的堆仅18页，重放拒绝跨越4页的缺口。CRC为0xd116b066，名义行宽0是合法format2而非本次原因。本批未修改恢复代码；需要正确的重写物理代次／恢复边界，不能靠忽略错误或无界补页关闭。实际失败快照和正常旧模式混合长值种子均保留在本地。

10项相关原生测试在旧版与当前均actual0：vacuum_toast_test、parallel_vacuum_test、vacuum_page_lock_test、vacuum_full_test、bytea_large_toast_test、ordinary_project_toast_demand_test、sorted_projection_null_bitmap_cost_test、view_column_dependency_test、vacuum_toast_physical_reference_test、alter_column_toast_generation_test。旧版前9项使用既有相同源码／对象／ELF的真实终态，第10项重新执行；当前10项均本批执行。既有toast_test两版仍在第604行 `!corruptScan.open()` 断言中止（actual -6），后续检查NOTRUN，未改预期，full suite=false。

strict PG=false，没有关闭整个功能族。[完整报告](performance-report-2026-10-30.md#scalar-text-logical-limit-20261107)。
<!-- P38 scalar-text-logical-limit end -->

<!-- P37 vacuum-physical-references begin -->
<a id="vacuum-physical-references-current"></a>
## 2026-11-07：物理TOAST引用不依赖调用者NULL绑定

当调用者仍绑定一次真实 TableScan 的正 RID，特别是另一个数据库中同名表的 NULL 行时，旧收集器可能漏掉仍在使用的 TOAST 引用并回收这些对象；新代码只使用正在检查的物理元组。该触发由原生维护接口复现，未将正常 SQL 对照中的旧／新相同结果伪称为 SQL 数据丢失复现。

修复普通 VACUUM 的长字段引用收集：在原有页锁和 pin 内，直接读取所属物理元组的 NULL 位图、变长字段目录和有界字节跨度，不再通过可能读取调用者 RID/NULL 状态的执行器取值接口。固定区大小与变长列索引每次维护预先计算，普通内联值无需复制整行或逐值构造字符串。活动快照保护、所有物理存活槽的遍历、锁／pin、索引／分块校验和回收策略保持。

原62条SQL在旧版30412452和C1全部执行，4个初开／冷开Main正常0，完整请求／回复逐字节相同，沿用并核对真实PostgreSQL18.6原始oracle，既有9条协议差异仍在。另以真实PG18.6、旧版和C1各执行28条新维护SQL及冷开读取，共84请求／142行／549单元／129NULL／2733516字节，6个Main正常0；第二套解码器核对全值、NULL、完整T7、tag、Ready和原始错误／NOTICE。28条中旧／新完整wire一致；21项值／状态／tag与PG相符，1项用各库真实正OID分别核对，另6项PG差异保留，strict PG=false。

本批实际PG缺口包括：TEXT70003字符INSERT在产品返回22023而PG成功，立即读取与冷开读取缺该行；SQL `CREATE TABLE ... PARTITION OF` 返回0A000，子表目录身份及冷开身份读取缺行。原生分区物理存储控制不代表这项SQL功能已实现，也不代表逻辑分区扫描已通过。

8项相关原生测试在旧版和C1均actual0：vacuum_toast_test、parallel_vacuum_test、vacuum_page_lock_test、vacuum_full_test、bytea_large_toast_test、ordinary_project_toast_demand_test、sorted_projection_null_bitmap_cost_test、view_column_dependency_test。其中旧版后3项复用相同源码／冻结对象／ELF的既有正常终态，其余均本批实际执行。既有toast_test在旧版与C1仍于第604行 `!corruptScan.open()` 断言中止（actual -6），保留失败，后续断言未运行；未修改测试预期，full suite=false。

未扩大PG兼容承诺；[真实正负开销与边界](performance-report-2026-10-30.md#vacuum-physical-references-20261107)。
<!-- P37 vacuum-physical-references end -->

<!-- P36 physical-row-null-reuse begin -->
<a id="physical-row-null-reuse-current"></a>
## 2026-11-06：当前物理行 NULL 状态的局部复用

扫描展开长字段时，借用该物理行已捕获的 NULL 位图；排序后的投影在核对真实来源、具体 Sort 类型和全部物理列声明后，借用排序已保存的位图。借用仅存在于一次取行调用内，退出与异常均恢复原绑定；表结构不匹配、Sort 子类及其他来源继续原路径。没有新增页／allocator 缓存或改变文件身份检查本身。

选中的虚拟列仍读取实际依赖并保留运行时错误；未扩大投影需求或修改 Sort 子类的虚调用。内存／落盘排序与NULL/空串/文本NULL、二进制、长字段均有完整值检查。

原 62 条 SQL 在旧版与 C1 全部重新运行，初开／冷开 4 个 Main 正常0，完整请求／回复逐字节一致；全部 T7、值／NULL、SQLSTATE、诊断、tag 与 Ready 保留。既有 9 条 PostgreSQL 差异仍在，strict PG=false；沿用并核对真实 PostgreSQL18.6 的固定原始 oracle，没有运行 PG 速度对比。

另外 7 项相关原生回归在旧版和 C1 均实际 exit0。既有 toast_test 在两版均于第 604 行 `!corruptScan.open()` 断言中止（actual -6）；保留为失败，后续断言未运行，未修改测试预期或调查／修复该故障模块。首个原生控制器因此停止，后续仅执行原计划尚未启动的 11 个运行，没有重跑已完成项，也没有宣称全套通过。

只接受有限执行优化，未承诺所有操作变快；[完整开销与未解决项](performance-report-2026-10-30.md#physical-row-null-reuse-20261106)。
<!-- P36 physical-row-null-reuse end -->

<!-- P35 view-column-dependencies begin -->
<a id="view-column-dependencies-current"></a>
## 2026-11-06：普通 SQL 视图依赖与删除契约

创建／兼容替换视图时，从已绑定的真实来源、具体列及查询子树记录依赖，覆盖投影、谓词、排序、分组、窗口、子查询与 USING/NATURAL 键。删除前先检查依赖；RESTRICT 返回 2BP01，未引用列仍可删除。CASCADE 按依赖顺序删除相关视图，后续子命令失败时由已有 DDL 快照恢复文件和目录记录。列序号压缩后，剩余引用沿用已有重映射。

CREATE/REPLACE 从纯绑定声明记录真实关系 OID 与列号，不执行表达式推断值；星号固定当前展开，列别名映射回实际来源。有效旧视图在删除前只读绑定，单来源旧星号需历史输出前缀的明确证据；歧义旧定义会失败，不猜测已丢失的数据。编译目录来源不要求物理堆／view 文件。

删除保护在注释清理、堆重写前执行；IF EXISTS 的缺失列不删除视图。CASCADE 包含依赖视图与其子视图，失败恢复由已有 DDL 事务快照负责。普通保存点恢复经真实 SQL 验证，未扩展到通用注入故障矩阵。

本批范围是普通 SQL 的静态 DDL 关系／列依赖。视图来源 T7 的零 origin、列别名运行时重绑定、部分嵌套视图、字面量／envelope 运行时、含歧义历史的旧视图、物化视图、函数／类型依赖及大量视图的扩展性仍有缺口；普通 pg_attribute 查询尚未补齐。用户暂缓范围保持，未修改执行图／child-provider、portal 或通用故障矩阵。

完整 T7、NOTICE／错误明细等 PG 差异仍保留，未声称完整协议等价。[测试](test-report.md#view-column-dependencies-current)及[真实开销](performance-report-2026-10-30.md#view-column-dependencies-20261106)给出本批界限。
<!-- P35 view-column-dependencies end -->

<!-- P34 projected-toast-demand begin -->
<a id="projected-toast-demand-current"></a>
## 2026-11-06：普通物理部分列投影

普通物理游标上的部分列投影按实际选择解码字段，复用当前行 NULL 位图，仅解析被选中长字段的 TOAST 引用。选中长字段读取失败仍在行输出前报错，输出值、NULL、RID 与旧路径的列顺序保持。全列/星号投影继续原 materialization 路径；选中虚拟列保留原完整输入。过滤、索引、排序、缓冲和不同 schema 的子路径仍由各自已有执行路径处理。

原生用例覆盖真实缺失 TOAST 文件、SQL NULL/空串/字面 NULL、marker-looking 字符串、UTF8、换行、BYTEA 和虚拟列错误。普通协议的完整 RowDescription/数据/NULL/错误/tag/Ready 与旧版一致；原 PostgreSQL 差异仍保留。本批适用于当前普通执行器，保留既有存储格式；retained/参数/portal/wire-format2、未选中显式 CAST 分析和用户暂缓边界保持。详见[完整报告](performance-report-2026-10-30.md#projected-toast-demand-20261106)。
<!-- P34 projected-toast-demand end -->

<!-- P33 pg-class-declaration begin -->
<a id="pg-class-declaration-current"></a>
## 2026-11-06 pg_class declaration and value boundary

The accepted candidate is normal CMake C++17/O2 C2 `280ad4a8`; the first C1 is rejected and retained as evidence. This is a partial catalog feature, with no speed improvement or full PostgreSQL compatibility claim.

The mutable CatalogService startup owner now creates and persists actual relation1259, rowtype83/array273 and34 sequential attributes. The extra actual types are `pg_node_tree`194, `aclitem`1033 (16 bytes/alignment `d`) and `_aclitem`1034. Cold metadata snapshots neither bootstrap nor allocate or publish. A loaded incompatible class/type/attribute/dependency declaration raises `XX001`; the finite tested conflicts do not claim an exhaustive corruption/fault matrix.

Type discoverability does not grant a value codec. Selected CAST/typed-literal conversions for these metadata-only declarations, including NULL/array input, raise `0A000`. No input/output function OID is invented. The generic declaration resolver and unselected explicit-CAST input analysis are unchanged. Ordinary qualified user types with the same basename remain separate.

The ordinary `pg_class` renderer still implements only `oid, relname, relnamespace, relkind, relnatts, relpersistence, relowner`; the other27 values and wildcard remain unsupported. Virtual source origins still differ from PG; the tested `pg_attribute` SQL route still fails `42P01`. This does not provide a catalog heap/MVCC, ACL/security semantics, retained planning/portal/format2 work or full CAT02 closure. [Full unfiltered evidence](performance-report-2026-10-30.md#pg-class-declaration-20261106).
<!-- P33 pg-class-declaration end -->

<!-- CURRENT_LEGACY_LOG_APPEND_20261106 -->

## 本批有限日志契约（P32-C1）

普通日志 `::log` 仍按 time、四个空格、user、四个空格、operation 和换行追加原始字节；每次实际打开当前路径，以 `O_APPEND` 和六段 `writev` 写入，EINTR 重试、正短写继续消费未写后缀，零进展或其他写错误停止并关闭。打开失败仍输出 `error 1` 并返回0；权限、已有前缀、rename/unlink 与真实 symlink 重新解析保持。`getTime`、`auditLog` 正文字节未变，没有持久缓存日志FD或改服务端协议发送。

验证限本地普通文件与默认char-codecvt环境；真实短记录并发、轮转及有限系统调用控制不构成NFS、大记录事务原子性、fsync持久性或完整故障矩阵承诺。本批不改协议、目录/参数帧/retained/provider边界；58目标仍53通过、4冷锚仍失败，共9项普通差异未豁免。产品、PG行为目标和wire格式版本仍分别定义。[本批完整报告](performance-report-2026-10-30.md#legacy-log-append-20261106)与可逆原始证据（可选本地文件 `benchmarks/legacy-log-append-20261106.json.gz`）。以下原契约不变。

[R145](issue-review-r145-search-path-value-identity.md)有限支持SQL SET search_path的
逐参数list-quote、原样名字数据、空名字及FROM CURRENT真实事务赋值。
PG3573、Root18/clean10及独立复核通过；不据此宣称startup raw GUC格式、
全部数字/Unicode标识符语法、quote_all_identifiers或其它参数事务语义完整。
R144应用名规范化按用户要求暂缓，没有在本项接受。

本契约定义 DBMS_C_plus_plus 可以对外作出的兼容性声明。它把产品版本、
PostgreSQL 行为目标、wire protocol 和项目扩展分开版本化，避免把“psql 能连接”
误写成“与 PostgreSQL 等价”。当前总账状态仍只取自
[`postgresql-18-gap-audit.md`](postgresql-18-gap-audit.md) 与
[`gap-progress.json`](gap-progress.json)。

## 四个独立版本轴

| 版本轴 | 当前值 | 含义 | 不代表什么 |
| --- | --- | --- | --- |
| 产品版本 | `0.2.0` | 本项目自己的 SemVer；唯一代码事实源是 `src/common/version.h`，并与 CMake、包和 CHANGELOG 校验 | 不等于 PostgreSQL server 版本 |
| 行为兼容目标 | `PostgreSQL 18`，差分基线 `18.6` | `postgresql18` 模式要逐项逼近的 SQL、catalog、事务、错误和协议行为 | 当前尚未达到该目标 |
| wire protocol 上限 | `3.0` (`196608`) | 本地实现的最高协议版本；3.x 新 minor 会通过 `NegotiateProtocolVersion` 降到 3.0 | 不表示实现 PostgreSQL 18 的全部前后端消息或 libpq 行为 |
| 扩展契约 | `extended` | 显式启用项目自己的非 PostgreSQL SQL 和管理入口 | 不是 PostgreSQL、MySQL 或其磁盘格式兼容模式 |

这四个值可以独立变化。产品发版不能暗示 PostgreSQL 行为目标已经完成；修改
wire protocol 不能自动提升行为兼容声明；增加 extended 命令也不能扩大
`postgresql18` 模式的声明范围。

## 对外定位和模式边界

在 273 项审计全部达到验收门之前，本项目的准确定位是：

> 一个提供 PostgreSQL wire protocol 3.0 子集、可供部分 PostgreSQL 客户端连接的
> 自有 DBMS，正在以 PostgreSQL 18.6 为差分基线建设行为兼容性。

不得使用“PostgreSQL 18 compatible”、“drop-in replacement”、“PG18 equivalent”
或同义的无范围限定声明。

`postgresql18` 是默认模式。它只能接受 PostgreSQL 18 语法或项目已经实现并验证的
对应行为；未实现能力必须返回稳定且合适的 SQLSTATE，不能写入 sidecar 后伪造成功。
项目语法必须被拒绝，不能因默认值、客户端或调用入口不同而泄漏。

`extended` 只能由 `DBMS_COMPATIBILITY_MODE=extended` 或会话级
`SET compatibility_mode = extended` 显式进入。它可以开放项目命令，但不会把没有
运行时的 PostgreSQL 对象变成成功操作。切换模式不改变产品版本、wire protocol
版本、数据格式或当前 database。

模式分派只匹配完整目标参数名，不扫描普通SET的值内容；相似或带点名称
不是该builtin。普通参数的TO/等号分隔符从引号外定位，不把数据中的等号当语法。
[R141](issue-review-r141-set-parameter-identity.md)仅验证这些有限边界，不代表完整GUC
语法、参数域、quoted模式值或事务级LOCAL已经实现。

[R143](issue-review-r143-show-guc-protocol-results.md)仅覆盖application_name、
client_encoding、search_path、lc_monetary四个已实现标量SHOW的固定text结构与值传输：
支持Simple及已Describe的Extended text/binary，空串与文字NULL不冒SQL NULL，
路径换行不拆行。这不代表完整SHOW族、应用名值规范化或全部GUC事务语义完成。

[R142](issue-review-r142-quoted-set-parameter.md)将完整单token的quoted普通SET目标
按GUC大小写无关规则解码，覆盖已实现应用名/编码/路径/locale与LOCAL搜索路径；
原始@变量语法不由quoted目标进入。值解析、未知参数错误映射与其它事务作用域
没有扩展，不意味着完整GUC或TimeZone暂缓语法完成。

进程启动必须通过 `-D/--data-dir` 或 `DBMS_DATA_DIR` 显式选择本项目数据根。
`DBMS_CONTROL` 的项目 magic 和 system identifier 只声明 DBMS-C++ 自有 cluster
身份，不代表磁盘格式与 PostgreSQL 兼容；含 `PG_VERSION` 的 PostgreSQL cluster
会被拒绝，运行状态不会写入调用者的启动 CWD。

## 本项目时间列的物理版本

schema9/A/B/C中的Legacy秒列保持原物理单位；新SQL标量TIME/TIMESTAMP/
TIMESTAMPTZ使用schema-D的TCM1逐列标签及int64微秒。timestamp基准为2000年，
不能按槽宽或typmod猜单位，也不能用旧程序打开新格式后静默降级解释。
具体写入、转换、索引及冷读的限定验证见[R136](issue-review-r136-temporal-microsecond-storage.md)；
完整升级/降级、TIMETZ、数组precision与全部时间输入域仍不得推导为已验收。
这不改变产品0.2.0、wire3.0、目标18.6或A/B门状态。

## 两级验收标准

### A. “PostgreSQL 客户端可连接”

只有同时满足下列条件，才可对明确列出的客户端版本声明“通过 PostgreSQL wire
protocol 子集连接”：

1. 真实 TLS 构建完成 startup、认证、ParameterStatus、简单查询、扩展查询、取消、
   通知和错误恢复的协议测试；未覆盖的消息必须列为限制。
2. 服务器正确协商其真实最高 wire 版本；不得把 `server_version` 或协议协商值用作
   行为兼容证据。
3. 对支持的消息和 SQL，返回的帧顺序、字段、OID、command tag、SQLSTATE 和事务状态
   有差分回归；畸形输入 fail-closed，不能崩溃或使后续连接失步。
4. 发布说明列出测试过的客户端、版本、TLS/认证方式及已知缺口。仅“psql 能启动”
   或单条 `SELECT 1` 成功不通过此门。

通过 A 门只允许声明 wire/client interoperability 的限定子集，不能声明 PostgreSQL
行为兼容。

### B. “PostgreSQL 18 行为兼容”

只有同时满足下列条件，才可声明 PostgreSQL 18 行为兼容：

1. [`postgresql-18-gap-audit.md`](postgresql-18-gap-audit.md) 的 273 项全部为
   `complete`；`partial`、`unverified` 和 `deferred_by_user` 均阻止该声明。
2. 183 个 PostgreSQL 18 命令及 SQL/type/function/catalog/transaction/storage/
   replication/protocol/operations 各功能族都有可重复的 PostgreSQL 18.6 差分矩阵；
   值、类型元数据、错误、可见性和副作用均一致，不能只比较文本成功与否。
3. PostgreSQL regression/isolation、协议客户端、崩溃恢复、故障注入、升级/降级、
   sanitizer、fuzz、soak 和并发 history 验证达到审计中对应门槛，并为 clean worktree
   生成机器可读发布报告。
4. `postgresql18` 模式下所有项目扩展均有负向测试；未实现能力明确失败，不存在
   compatibility-record 假成功。
5. 发布文档逐项限定二进制、扩展 ABI、catalog、逻辑/物理复制及 on-disk 格式。
   除非它们各自通过专项验收，不得推导为兼容。

## 发布声明矩阵

| 状态 | 允许的声明 | 禁止的推导 |
| --- | --- | --- |
| A 门未通过 | “实现中的 PostgreSQL protocol 接口” | 任何客户端兼容或 PG 行为兼容声明 |
| 仅 A 门通过 | “已验证客户端 X/Y 可通过 wire 3.0 子集连接”，并链接报告和限制 | PostgreSQL-compatible、SQL/catalog/transaction 等价 |
| A、B 门均通过 | “对 PostgreSQL 18.6 行为兼容”，并链接全量报告与仍不兼容的非目标接口 | PostgreSQL 二进制、extension ABI、物理复制或磁盘格式兼容，除非另有专项证明 |

当前状态是 **A 门和 B 门均未宣告通过**。任何发版、README、包描述和客户端握手
字段都必须遵守这张矩阵；实时进度变化由总账决定，而不是修改本文中的结论。

## 变更规则

- PostgreSQL 主版本目标、差分 patch baseline 或 wire 上限变化，需要独立变更记录、
  对应矩阵刷新和测试更新。
- 行为差异必须进入 273 项总账对应条目；没有条目时先新增审计项，不能只改营销文案。
- 只有 `scripts/check_gap_progress.py --require-complete` 成功且 B 门的发布证据齐全，
  才能把 B 门状态改为通过。
- `server_version` 为客户端解析所需的互操作字段，不是产品身份或兼容认证；产品身份
  始终使用 `DBMS_VERSION_STRING`。

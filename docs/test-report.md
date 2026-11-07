# DBMS 功能测试报告

## P42 最新有限验证（2026-11-07）

新正式`read_transaction_publication_test.cpp`：旧版38项中15个新增优化要求失败、冷开数据成功；C1的38项全部成功。独立核对两版16份完整原生观测与55条WAL记录。另32个既有原生入口旧／新各30个0，TOAST在604行断言-6、WAL追加故障测试300s未完成并由本任务执行器终止-15，两版全部保留且不计通过。加新回归后C1的33个不同入口为31个0、2项未通过，full suite=false。

实际PG18.6／旧／新各95条SQL完整模型通过，旧新95份请求回复逐字节相同；六份产品闭库WAL共475条CRC／prev链核对0。TEMP提交动作、序列、事务DDL、读后写、失败事务、RR并发与崩溃后完整值／NULL均保留严格检查。性能前还有10个不计时场景、366条完整SQL、20个Main0通过。621次新正常成本及全部135行比较在[报告](performance-report-2026-10-30.md#no-effect-transaction-publication-20261107)，任何负向不排除、不重采。

原2404测试字节不变、新增1后2405（顶层CPP1138／全部CPP1143）。源251、Markdown754及725保护记录保持；原273状态及291条历史不变，只追加partial第292条，Goal active。

<!-- P41 large-varchar-toast-search begin -->
<a id="large-varchar-toast-search-current"></a>
## 2026-11-07：长VARCHAR全值、冷开及性能验证

两个新正式原生测试验证完整数据。large_varchar_toast_test在C6三种模式各setup／cold1／cold2，共9个新进程子阶段全部0，覆盖更新返回完整行、回滚、越界原子拒绝、删除、VACUUM／FULL、提交后DML、TRUNCATE、索引RID与NULL位图。独立解码C6的29份完整观测及旧版2份成功部分观测，共31份／140行／420单元／106 NULL／11,200,242字节；旧版三种setup为1、六次冷开NOTRUN，属于不支持准入，未写成数据损坏。新TOAST布局测试在旧bf9与C6均0，两版各2个实际新子进程，180个数据页／544个存活分块的全部二进制值、头、Fletcher-16、页槽RID与朴素串行first-fit模型相等，短末块复用早期空洞，删除及冷开正确。其原始二进制API输入包含NUL，不能理解为SQL TEXT允许NUL。

相关48个原有正式测试按当前要求：46个actual0；toast_test仍在604行 !corruptScan.open() 断言actual -6、后续NOTRUN；declared_type_cold_binding_test旧bf9与C6都actual1，12项中2个初始catalog条件失败，保留OPEN。全套未运行，full suite=false。原type_registry_test把65536声明当错误，旧源码在C6实际-6的首次观察保留；依据已冻结PG声明范围，扩充为65536／10485760成功及10485761／非法修饰符拒绝，更新源码在旧版-6、C6为0。没有删除负向或改掉其余要求。加上两个新测试，共50个不同正式测试，当前48个0和上述两个失败；另外的历史原断言失败单列保存。旧26项对照来自相同bf9正常核心／源码的P40实测，另外22项旧版在本批真实重跑；70个原源码终态及全库文件／模式独立核对。

新141条VARCHAR SQL在独立PG18.6、旧bf9、候选C6均完整执行，三角色共423条核心SQL、305行／1249单元／132 NULL、228,538,937字节；10个正常Main0及2个预期SIGKILL -9。全值、顺序、重复数、NULL、状态／tag／Ready和全部七个描述字段与实际PG核对，关系OID只用各自真实探针映射。C6普通数据要求通过，仍有17个pg_class来源描述差异，以及3个大常量ADD DEFAULT的PG实现限制差异：PG对70000字节常量报54000，随后读取／DROP列报42703，项目接受并正确存取；这些差异保持开放，strict PG=false。137条V1参考模型的4个假设被真实PG否定，首次actual1及全部原始数据保留；141条V2在任何候选执行前冻结，另加4097字节默认值成功控制，没有按候选结果降低要求。

原72条TEXT／VARCHAR SQL另用本批新PG参照核对，三角色216条核心SQL、600行／2292单元／96 NULL、96,178,911字节；7个正常Main0与2个预期-9。原3个VARCHAR声明／写入差距修复，4个catalog来源描述差距仍在。22／62／72／28及wide OID控制共185条新完整请求，2588行／11613单元／969 NULL、45,817,003字节、8个正常Main0与1个预期-9。相对P40只有上述3条VARCHAR成功结果改变，其余请求回复逐字节一致；62条原9项与28条原3项PG差距保留。以上计数按各队列给出，72条复用于185桥接，没有重复计算成新的SQL执行。

六种既有FULL专属有限故障计划在C6均通过，保留同一真实透传钩子字节：120条完整SQL、4条中断FULL、10400行／48966单元／3696 NULL、64,393,755字节、4个预期-9与8个正常Main0、6285映像文件。6087条／47,972,984字节WAL的CRC32C／prev链及COMMIT／ABORT／未提交边界独立核对。它们没有重开用户已暂缓的通用IO／分配器／保存点矩阵。

所有正常窗口关闭后，项目DUMP扩展补测15条完整SQL、3个正常Main0：有界／无界VARCHAR及两类数组的真实逻辑导出，在全新数据库恢复并再次冷开，全值／NULL／全部七个描述字段正确。只验收这组类型声明与值，不声称项目DUMP已完整保留所有PG对象／约束。

诊断不计正常成本：C2和C3在141条计划中均完成60条，第61条ALTER请求已发且60秒无回复，其后80条未发、全部冷开NOTRUN；两个Main均正常关闭0。C2／C3闭库失败映像和全wire分别保留；映像在超时后正常关闭保存，不能当作故障瞬间或零检查点崩溃证明。C3只对自己的Main允许父进程后代GDB附加，四次真实栈显示writeToast反复访问并校验缓存页，不能写成死锁证明。系统sudo非交互安装失败，GDB由下载包解到自有临时目录，未更改系统ptrace设置。C6全部141条在无调试器下完成。另一次相同原生数据的strace，两版toast.dt pread64都是364次，请求2,981,888字节、实际读1,499,136字节；没有磁盘读调用数减少的证据。新正式测试最初日志写CRC，窗口后仅修正为实际Fletcher16并重新运行两版；断言、模型、生产程序和1680个时钟未改变，原日志保留。

本轮1680次新正常计时：159个实际正常Main0、3288条完整SQL、467859行／1695258单元／121014 NULL、1,090,713,993请求回复字节、22881个闭库文件。五组采集器和五组独立归约器实际0；窗口内没有本任务编译、调试、插桩、压缩或其他SQL，自然缓存、不绑定CPU，不声称宿主机全空闲。墙钟／客户端CPU覆盖发包到ReadyI，服务器CPU为整个Main并包括后台线程。全部样本一次执行、0排除／0重采；P40原1545次观察保持关闭和原样。本轮是新的程序／时间窗口，不能替代旧负向。

负向保持OPEN：新长值DELETE RETURNING墙钟43.553088ms，+9.954%／+23.830%，服务器CPU+67.962%／+39.067%；首次VACUUM墙钟25.753918ms，+22.574%／+1.265%；FULL墙钟218.226818ms，+32.204%／+21.215%，服务器CPU+7.756%／+1.774%。新增完整值读取墙钟+2.080%／-2.014%，结果混合。通用270项矩阵的30个区域，墙钟6低于两旧／18高于两旧／6居间；服务器CPU10／8／12，客户端CPU6／14／10。没有排除、重采或把漂移归因为已证实的产品原因；全部负向与原因继续开放。

正式库存2404个文件（1137顶层／1142总C++），2401个旧测试逐字节不变、一个类型注册测试按上述范围扩充、另加两个新测试；注册E2E684／独立E2E664不变。生产251个文件，改6个cpp并加CharacterColumn.h，正常CMake C++17/O2程序C6为587f898d，旧bf9ad7ae、更早c8ca0215，61核心对象冻结；没有靠升级C++23宣称提速。默认425165af不变。754个Markdown路径／725份保护记录保留，仅已有8文档插入说明；原273项及22 complete／166 partial／70 unverified／15 deferred_by_user不变，290条旧历史原样，新增partial第291条，IDX-07仍unverified/evidence[]。全部用户暂缓范围不变。Goal active，直接master提交，未推送或替换默认程序。下方各批均为各自历史版本。 [全部225行性能及原始证据](performance-report-2026-10-30.md#large-varchar-toast-search-20261107)。
<!-- P41 large-varchar-toast-search end -->

<!-- P40 vacuum-full-relation-snapshot begin -->
<a id="vacuum-full-relation-snapshot-current"></a>
## 2026-11-07：FULL关系快照、回退与全部成本验证

新增vacuum_full_relation_snapshot_test，以完全相同源码链接旧c8ca与正常C1核心。两个模式各有256行目标表／删除后16行、12行无关表、真实脏缓存、主键／二级／hash／bloom索引、70003字节以上TEXT、UTF8、空串／字面NULL／SQL NULL。旧版父actual1：6个优化断言失败，两次setup为1、四次冷开均0；所有热态／冷态完整值和RID NULL位图已正确，不是旧版数据损坏复现。C1父actual0且6子阶段全部0：成功模式实际10份同inode／nlink≥2物理备份；故障钩子对10次硬链接实际返回EXDEV，随后完整复制回退并通过热态及两次新进程冷开。无关堆同步2→0，无关脏页保留给自身owner，之后自身flush正常完成。所有拦截实际调用真实系统同步，未跳过fsync；EXDEV是显式故障控制，不冒充真实跨文件系统部署验证。

C1本批重新执行26项相关原生测试：12项WAL／身份／出生／退休／redo／UNLOGGED／TRUNCATE、10项长字段／VACUUM／投影／视图、热态vacuum_full_test及既有两个FULL恢复测试通过，合计25项actual0。既有toast_test仍在604行 `!corruptScan.open()` 断言actual -6，后续NOTRUN；未修改预期，full suite=false。旧c8ca的26个对照终态来自P39已认证实际运行，逐项核对相同冻结源码、250生产文件、61核心对象和正常ELF后复用，没有声称本批重跑旧版26项。

既有vacuum_full_cold_recovery_test在C1的6子阶段实际为0／0／0／-9／0／0：包括一次预期强杀，不能写成6次全0。堆352256字节（43页）压到32768字节（4页），随后提交1MiB TEXT更新、插入、删除，关闭／强杀后连续两次新进程冷开全值通过。两种模式各597条／4986712字节WAL，共1194条CRC32C与prev链核对；旧代次578镜像、最大块42，新代次7镜像，0个checkpoint。既有generation测试10子阶段均0，覆盖首次堆WAL前有效空09 schema、三次FULL／重命名、空表两次FULL／后续DML；最终身份2／4／3、0x23提交边界及计数器核对通过，0个checkpoint。

原22、62、72、28条SQL及实际wide OID控制全部重新执行，共185完整请求／2588行／11613单元／969NULL／45,817,233请求回复字节，8个正常Main0和1个预期SIGKILL -9，2315个闭库文件核对。全部完整请求与回复逐字节等于P39真实c8ca观察，再由独立解码器检查完整值／NULL／全部T7／错误／NOTICE／tag／Ready。原PG18.6参照逐字节复用，未声称本批新跑PG；62条中的9项、72条中的7项、28条中的3项SQL子分区差异继续开放，strict PG=false。

六种FULL专属有限故障控制在C1均首次尝试通过：schema发布前强杀、发布后提交前强杀、COMMIT后清理前强杀、发布报错后回滚schema期间再次强杀、提交后清理失败、回滚后清理失败。共120条完整SQL与4条真实中断FULL、10400行／48966单元／3696NULL／64,393,755字节，4个预期SIGKILL -9、8个正常Main0，6285个实际映像文件，一个预期XX000。6087条／47,972,984字节WAL独立核对CRC／prev链、COMMIT／ABORT／未提交边界和代次不可复用；两种清理失败后的后续DML与显式CHECKPOINT顺序及冷开全值正确。使用P39已接受V2钩子的同一字节，仅初开进程插桩，冷开正常C1；P39原V1漏拦截remove的失败属于历史保留，不是本批重试，未覆盖原记录。

在正常计时前，使用真实fsync／fdatasync／msync透传钩子核对FULL工作量。旧c8ca固定／单长／多长各58次同步，其中8／6／6次属于无关关系；C1为50／52／52次，无关关系均0。逐路径原始日志、实际PID／线程、返回值、完整SQL与闭库映像保留。C1诊断墙钟438.800／458.549／502.184ms，明显高于旧版；诊断时可能与本任务编译重叠，明确不能作为正常成本或线程独占成本证据。只从这些诊断接受实际工作量减少，不能用减少的系统调用数代替运行时间结果。

全部正常计时1545项／129个正常Main0／3003完整SQL／467094行／1693173单元／120549NULL／911,125,308请求回复字节／19656闭库文件，四个采集器及四个独立归约器实际0。窗口没有本任务编译、压缩、插桩、剖析或其他SQL；自然缓存、不绑定CPU，不声称宿主机全空闲。墙钟和客户端CPU覆盖socket至ReadyI；服务器CPU为整个Main并包含后台线程。所有原始计时一次执行，0排除／0重采。

明显负向保持OPEN：首次普通VACUUM多长字段墙钟C1中位67.010218ms，旧c8ca首测30.951480ms／回测25.722458ms，即+116.501%／+160.512%。原三次样本分别为C1 [71.510152,26.679347,67.010218]ms、旧首测[35.992784,30.951480,25.821788]ms、旧回测[25.722458,23.927581,29.365169]ms，全部保留。单长字段大批重新插入的墙钟也+51.70%／+58.00%。未因负向重跑、排除样本或更改预期。

正常窗口全部关闭后，因上述负向另做一次有界同步诊断：同一after-DELETE种子、旧c8ca与C1各单长／多长普通VACUUM一次，共28条完整SQL、3576行／14874单元／1098NULL／8,603,834字节、2个正常Main0、652个闭库文件。四次维护各实际两次同步，均为目标.toast.dt与.toast.idx，无额外无关同步。多长诊断旧版墙钟39.305ms（同步24.028ms），C1墙钟11.593ms（同步3.656ms）；这个反向诊断结果只说明本次同步延迟不同，不证明原正常负向的原因，不替换原样本，也不用于宣称提速。诊断和独立校验均actual0，无编译重叠。

本批仍只验收未来普通关系FULL的有限重写／关闭／强杀／回滚恢复。不能修复已被旧FULL写坏且没有新pending／退休授权的旧集群；P38实际损坏库继续保留。跨FULL的PITR／物理基备未验收；数据库排他锁范围仍大于PG表级锁，未验收分区FULL冷开或高并发等价。硬链接需要文件系统支持，实际失败会走复制回退；不宣称所有平台／文件系统已验证。原15项安全／TDE及额外用户暂缓范围保持，这六种FULL故障不是已暂缓的通用IO／分配器／保存点矩阵，也未恢复执行图／child-provider或retained planning／portal工作。

正式测试库存2402个文件（1135顶层／1140总C++），2401个旧测试逐字节不变；注册E2E684／独立E2E664不变。250个生产文件只改TableManage.cpp，正常CMake C++17/O2 C1为bf9ad7ae，旧c8ca0215、更早84bbadb4；61核心对象冻结。默认425165af不变。754个Markdown路径、725份保护记录保持，只在已有8文档插入说明。原273项和22 complete／166 partial／70 unverified／15 deferred_by_user不变；289条历史保持，新增partial第290条；IDX-07仍unverified/evidence[]。Goal active，直接master提交，未推送或替换默认程序。下方各批是各自历史程序的记录。 [完整性能与归档](performance-report-2026-10-30.md#vacuum-full-relation-snapshot-20261107)。
<!-- P40 vacuum-full-relation-snapshot end -->

<!-- P39 vacuum-full-wal-generation begin -->
<a id="vacuum-full-wal-generation-current"></a>
## 2026-11-07：FULL新代次、恢复与完整性能验证

新增vacuum_full_cold_recovery_test使用同一源码对照：旧84bb两种情形均在首次冷开出现重复可见主键（父actual1，4个子阶段已执行／2失败／2后续NOTRUN）；C1为6个子阶段全部符合实际终态，正常关闭或主动SIGKILL -9后连续两次新进程冷开全值／NULL／主键通过。原堆352256字节（43页）压到32768字节（4页），之后真实UPDATE到1MiB TEXT、INSERT、DELETE并提交。两份WAL共1194条独立CRC32C核对，旧代次578个页镜像最大块42、新代次7个镜像，退休完成及提交顺序正确，0个检查点，不能归因于checkpoint绕过。

新增vacuum_full_generation_recovery_test进一步覆盖有效空09 schema在首次堆WAL前加载、三次FULL／中途重命名、空表两次FULL及后续插入。C1实际10个子阶段均0，完整值／NULL与两次冷开通过；旧版在3次新代次检查失败（父actual1，4阶段执行／3失败／6冷开NOTRUN），不把未运行阶段计为冷开复现。09 fixture保留format2堆格式，不代表所有历史堆版本已迁移。独立解码验证legacy的0x23提交边界、重复重写退休、最终身份2／4／3及不可回退计数器，0个检查点。

12项恢复相关原生测试在旧84bb与C1均本批实际0，覆盖身份codec／保护、退休完成、出生与崩溃、redo、基础WAL／全页／截断、UNLOGGED强杀／恢复和TRUNCATE恢复。C1另10项长字段／VACUUM／投影／视图相关测试实际0，既有vacuum_full_test的4组热态、分区、备份失败与中途失败恢复也实际0。既有toast_test仍在第604行 `!corruptScan.open()` 断言中止（actual -6），后续NOTRUN；保留P38同位失败，未修改预期，full suite=false。该计数为有限相关原生测试，未宣称全套通过或原生分区FULL冷开已验收。

原先因冷开退出-6中断的同一旧库22条SQL计划，现在C1完整执行22条／2130行／9813单元／738NULL／12,433,869请求回复字节，两个正常Main实际0，944个闭库文件核对。初开16条完整请求和回复与P38旧84bb原观察逐字节一致，新增完成原先未运行的6条冷开。原始旧库种子334个名字／模式／字节不变；P38原失败、被旧观察器遮蔽的旧终态及缺失阶段保持原记录。

六种FULL专属有限故障控制全部经独立完整值／NULL／全部T7／错误／tag／Ready核对：schema发布前强杀、发布后提交前强杀、COMMIT后清理前强杀、发布报错后回滚schema期间再次强杀、提交后清理失败、回滚后清理失败。共120条完整SQL、4条确实发出后被中断的FULL、10400行／48966单元／3696NULL／64,393,755字节，4个预期SIGKILL -9和8个正常Main0，6285个实际映像文件；一个预期XX000保留。6087条WAL／47,972,984字节独立CRC与prev链核对，并核实各自COMMIT／ABORT／未提交及不可复用代次。两种清理失败后继续UPDATE，再显式CHECKPOINT，检查点确实晚于该DML提交；冷开值未被旧备份覆盖。只在初开Main加载版本化syscall钩子，冷开使用未插桩的同一正常C1。

首轮测试钩子漏拦截libstdc++实际使用的libc remove：after_commit未按预期断开，采集器actual1且未启动冷开；两种cleanup_fail虽各完成24条SQL，独立故障核对均actual1。所有首轮源码、日志、计划、完整wire、实际终态及NOTRUN保留。V2只增加remove拦截，三个原计划字节不变，正常生产ELF不变，再实际完成相应故障控制；没有改成忽略故障的预期，也没有重采或筛选任何正常性能样本。

原62、72、28条SQL及一条实际wide OID控制在本批C1重新执行，共163请求、458行／1800单元／231NULL／33,383,364字节，6个正常Main0和1个预期SIGKILL -9。所有完整请求与回复均与P38实际84bb逐字节一致，独立第二解码器核对全部值、NULL、T7、错误、NOTICE、tag和Ready；复用的是已认证归档中的真实PG18.6参照，没有本批新跑PG的声明。原62的9项差异、72的7项差异及28的3项SQL子分区差异继续保留，strict PG=false。

全部正常计时共1545项／129个实际正常Main0／3003完整SQL／467094行／1693173单元／120549NULL／911,125,308请求回复字节／19656闭库文件。窗口无本任务编译、压缩、插桩、剖析或其他SQL；自然缓存、不绑定CPU，不声称宿主机全空闲。墙钟／客户端CPU覆盖socket至ReadyI；服务器CPU为整个Main并含后台线程。

本批验收只覆盖未来普通关系FULL的有限重写／关闭／强杀／回滚恢复情形。不能修复已被旧FULL写坏且没有新pending／退休授权的旧集群；P38实际失败库仍保留。跨FULL的PITR／物理基备重建未验收，不能仅凭新生命周期记录重建重写后的完整堆。内部事务暂时使用数据库排他锁，再取表锁和cache锁，并非PG表级并发等价；热态分区既有回归通过，不宣称分区冷开闭环。原始15项安全／TDE暂缓与额外用户暂缓记录保持；这六项不是已暂缓的通用IO／分配器／保存点故障矩阵，也未修改执行图／child-provider、retained planning／portal或CPU-sort interrupt。

正式库存2401个测试文件（1134顶层／1139总C++），2399个旧测试逐字节不变，注册E2E684／独立E2E664不变。源码250个文件，正常CMake C++17/O2候选C1为c8ca0215，旧84bbadb4、更早56aad0c0；默认425165af不变。754个Markdown路径和725份保护记录保持，只在8份已有文档插入本批说明。原273项和22 complete／166 partial／70 unverified／15 deferred_by_user不变，既有288条历史保持、新增partial第289条；IDX-07仍unverified/evidence[]。Goal active，直接master分批提交，未推送或替换默认程序。下方各批均为对应历史程序的记录。 [完整性能与归档](performance-report-2026-10-30.md#vacuum-full-wal-generation-20261107)。
<!-- P39 vacuum-full-wal-generation end -->

<!-- P38 scalar-text-logical-limit begin -->
<a id="scalar-text-logical-limit-current"></a>
## 2026-11-07：TEXT全值、普通恢复与1500项完整成本

新增large_text_toast_test在同一冻结源码上形成33检查：旧版21失败／actual1，当前0失败／actual0。全字节覆盖65535、65536、70003、UTF-8 90012字节／30006字符、可压缩131076字节、随机1MiB、NULL、空串、文本NULL及类似marker的普通串；同时验证第二长字段100003字节、旧式和optional更新的完整返回值、事务回滚／提交、普通VACUUM、热态VACUUM FULL、TRUNCATE后重插1MiB，以及有界VARCHAR的字符数和尾空格约束。

10项相关原生测试在旧版与当前均actual0：vacuum_toast_test、parallel_vacuum_test、vacuum_page_lock_test、vacuum_full_test、bytea_large_toast_test、ordinary_project_toast_demand_test、sorted_projection_null_bitmap_cost_test、view_column_dependency_test、vacuum_toast_physical_reference_test、alter_column_toast_generation_test。旧版前9项使用既有相同源码／对象／ELF的真实终态，第10项重新执行；当前10项均本批执行。既有toast_test两版仍在第604行 `!corruptScan.open()` 断言中止（actual -6），后续检查NOTRUN，未改预期，full suite=false。

新增72条SQL分别在真实PG18.6、旧版56aad0c0和当前程序执行：216条核心请求、507行、1935单元、80NULL、73,199,459请求／回复字节，7个Main正常0、2个主动SIGKILL终态-9。独立解码器核对完整值、NULL、全部T7、tag、Ready、错误及NOTICE，当前目标TEXT用例通过；严格PG比较仍有7项差异：4项系统目录列来源OID元数据，以及大声明／无界／多字节大VARCHAR的3项差异。初轮强杀前已删除1MiB行，其最大存活TEXT为200006字节；该组1MiB重写后的冷开来自正常关闭，不把它计作1MiB崩溃恢复证据。

实际旧版30412452创建、正常闭库并冷开验证的after-DELETE种子，334个名字／模式／字节在本批前后完全一致，固定／单长／多长字段人口241／241／113。另做不含FULL的18条普通维护控制，每角色18条，共36请求／3070行／14670单元／1110NULL／16,337,172字节，4个Main正常0；当前在旧模式中写入70003字节及1MiB TEXT后，普通VACUUM和新Main冷开全值／T7通过，未改动固定／单长字段完整wire。再以相同原始种子做独立18条崩溃控制，共36请求及相同全值工作量，2个主动SIGKILL -9、2个新Main正常0，确认提交后完整1MiB TEXT存活并读回。两项控制各有1600个闭库／崩溃映像文件核对；它们不替代下面失败的原始FULL测试。

新发现的普通VACUUM FULL／WAL恢复缺陷仍OPEN，属于下一项必须处理的非暂缓工作。原始旧模式22条矩阵，两版各完成初开16条并正常退出0；当前16条全值正确，但之后冷启动失败，两版各6条冷开SQL未运行。旧版首次观察器的cleanup断言遮蔽了已退出子进程的wait状态，因此其冷开actual exit未记录；修正观察器后只继续原先未启动的当前角色，实际冷开退出-6，未重跑或补写旧版状态。提前运行的独立审计也因缺少完整receipt实际退出1，失败保留。独立CRC32C核对定位LSN6784528的有效8192字节页镜像：cv_sig、物理身份3、旧块22；FULL后同身份的堆仅18页，重放拒绝跨越4页的缺口。CRC为0xd116b066，名义行宽0是合法format2而非本次原因。本批未修改恢复代码；需要正确的重写物理代次／恢复边界，不能靠忽略错误或无界补页关闭。实际失败快照和正常旧模式混合长值种子均保留在本地。

原62条SQL重新用旧版和当前执行，共124请求、4个Main正常0，完整请求与回复逐字节一致，真实PG18.6原始oracle和既有9项差异保持。P37原28条维护SQL计划字节不变，另在真实PG／旧版／最终C2重新执行，共84核心请求、144行、553单元、129NULL、2,873,498字节、6个Main正常0；额外4条实际OID／PG版本控制单列。原TEXT70003插入、立即读回、冷开读回3个确切缺口现与PG完整值和T7相符，其余25条旧／新完整回复字节不变。28条中24项值／状态／tag匹配、1项按各库真实正OID核对，仍有SQL子分区创建0A000、子表目录身份及冷开身份缺行3项差异，strict PG=false。

正式1500计时／114正常Main，全部162项比较、完整raw20/raw3、首轮、长尾及负向保留。源码、进程终态、原始wire和闭库文件可按SHA重建，C2与实际功能测试C1的61对象／完整ELF逐字节相同。

正式库存2399个测试文件（1132顶层／1137总C++），注册E2E684／独立E2E664不变。754个Markdown路径、725份保护记录保持，8份既有文档只插入本批说明。原273项、22 complete／166 partial／70 unverified／15 deferred_by_user不变，既有287条历史保持并新增partial第288条，IDX-07仍unverified/evidence[]。 [完整性能与压缩证据](performance-report-2026-10-30.md#scalar-text-logical-limit-20261107)。
<!-- P38 scalar-text-logical-limit end -->

<!-- P37 vacuum-physical-references begin -->
<a id="vacuum-physical-references-current"></a>
## 2026-11-07：物理TOAST引用、完整协议与1500项实际成本

新增 vacuum_toast_physical_reference_test 在同一冻结源码上执行全部16种情形：旧版160检查／48失败／actual1，C1为160／0／actual0。覆盖普通表与原生范围分区存储、vacuumToast及外层vacuum、真实同库／跨库同名表调用者、跨位图字节的NULL、空串、文本NULL、UTF-8、类似marker的普通字符串、TEXT11003／40003及BYTEA70003／11009。独立完整值物理解码核对16种情形中的80个存活对象全部原始字节，以及每个真实孤儿的回收、调用者绑定恢复和第二次维护幂等。

独立oracle使用实际DataFileHeader及校验、PageAllocator、PageWrapper和既有完整值解码器；不复用新引用收集算法，也不使用前置失败的逻辑分区读取来判定通过。物理长字段NULL/空串/二进制/全长值及真实调用者绑定均核对。

8项相关原生测试在旧版和C1均actual0：vacuum_toast_test、parallel_vacuum_test、vacuum_page_lock_test、vacuum_full_test、bytea_large_toast_test、ordinary_project_toast_demand_test、sorted_projection_null_bitmap_cost_test、view_column_dependency_test。其中旧版后3项复用相同源码／冻结对象／ELF的既有正常终态，其余均本批实际执行。既有toast_test在旧版与C1仍于第604行 `!corruptScan.open()` 断言中止（actual -6），保留失败，后续断言未运行；未修改测试预期，full suite=false。

原62条SQL在旧版30412452和C1全部执行，4个初开／冷开Main正常0，完整请求／回复逐字节相同，沿用并核对真实PostgreSQL18.6原始oracle，既有9条协议差异仍在。另以真实PG18.6、旧版和C1各执行28条新维护SQL及冷开读取，共84请求／142行／549单元／129NULL／2733516字节，6个Main正常0；第二套解码器核对全值、NULL、完整T7、tag、Ready和原始错误／NOTICE。28条中旧／新完整wire一致；21项值／状态／tag与PG相符，1项用各库真实正OID分别核对，另6项PG差异保留，strict PG=false。

本批实际PG缺口包括：TEXT70003字符INSERT在产品返回22023而PG成功，立即读取与冷开读取缺该行；SQL `CREATE TABLE ... PARTITION OF` 返回0A000，子表目录身份及冷开身份读取缺行。原生分区物理存储控制不代表这项SQL功能已实现，也不代表逻辑分区扫描已通过。

正式1500计时／114正常Main／2733完整回复，另1536诊断查询／3正常Main；不剔除首轮、尾部、CPU回退或既有失败，所有冻结源码、原始wire、三时钟、完整闭库文件和首错可从压缩证据重建。

全部试验失败原样保留：v1因不存在的makeByteaColumn编译actual1；v2/v3用70003字符TEXT建原生数据时拒绝，目标情形NOTRUN；v4较大原生列声明在首行遇IO_ERROR、回滚未完成，退出actual -6／double free，原因OPEN，未调查或修改用户暂缓模块；v5真实SQL声明下原生写TEXT70003仍拒绝。v6的前8个“绑定”控制实际未绑定，不能作为NULL安全证据，后续SQL子分区DDL为0A000并停止。v7改用真实正RID后普通表复现失败，但逻辑分区读取的初始全值前置检查失败，余8种情形NOTRUN；未将其改称通过。v8调用私有路径／行头接口而编译actual1。最终v9改用受控默认路径与既有完整物理解码器作为独立oracle，才完整执行16种情形并形成160／48旧版红测；全部初稿源码、编译／运行终态和首错均留存。

正式库存2398个测试文件（1131顶层／1136总C++），注册E2E684／独立E2E664不变。754个Markdown路径和725份保护记录保持，8份既有文档仅插入本批说明。原273项及22 complete／166 partial／70 unverified／15 deferred_by_user不变，历史增至287，IDX-07仍unverified/evidence[]。 [完整正负结果与归档](performance-report-2026-10-30.md#vacuum-physical-references-20261107)。
<!-- P37 vacuum-physical-references end -->

<!-- P36 physical-row-null-reuse begin -->
<a id="physical-row-null-reuse-current"></a>
## 2026-11-06：物理位图复用、完整协议及真实成本

新增 sorted_projection_null_bitmap_cost_test 在相同冻结测试源码上，旧版 1156 检查／8 失败／exit1，C1 1156／0／exit0；每 64 行排序后投影的文件校验从 353 降到 0，两个回退控制仍为 233／353。覆盖内存与真实落盘排序、升降序、NULL/空串/文本 NULL、UTF-8、二进制、长字段、过滤来源、schema 不匹配、子类虚调用、LIMIT 与虚拟列错误。

7项旧／新均exit0的既有程序为 ordinary_project_toast_demand_test、sort_null_binding_lifetime_test、typed_sort_operator_test、bounded_sort_runs_test、sort_final_merge_demand_test、view_column_dependency_test、scalar_result_null_bitmap_test。两代均使用各自冻结249源码、61core及stub。

另外 7 项相关原生回归在旧版和 C1 均实际 exit0。既有 toast_test 在两版均于第 604 行 `!corruptScan.open()` 断言中止（actual -6）；保留为失败，后续断言未运行，未修改测试预期或调查／修复该故障模块。首个原生控制器因此停止，后续仅执行原计划尚未启动的 11 个运行，没有重跑已完成项，也没有宣称全套通过。

原 62 条 SQL 在旧版与 C1 全部重新运行，初开／冷开 4 个 Main 正常0，完整请求／回复逐字节一致；全部 T7、值／NULL、SQLSTATE、诊断、tag 与 Ready 保留。既有 9 条 PostgreSQL 差异仍在，strict PG=false；沿用并核对真实 PostgreSQL18.6 的固定原始 oracle，没有运行 PG 速度对比。

新正式性能270+720操作共72个Main正常0，1926完整回复逐值核对；定位阶段另720操作／18Main正常0，采样另1536查询／3Main正常0，全部原始错误、尾部、时钟括号与文件核对保留。

正式库存 2397 个测试文件（1130 顶层／1135 总 C++），注册 E2E 684／独立 E2E 664 不变；754 个 Markdown 路径与 725 份保护记录保持。原 273 项及 22 complete／166 partial／70 unverified／15 deferred_by_user 不变，历史增至 286，IDX-07 仍 unverified/evidence[]。 [正负结果与归档](performance-report-2026-10-30.md#physical-row-null-reuse-20261106)。
<!-- P36 physical-row-null-reuse end -->

<!-- P35 view-column-dependencies begin -->
<a id="view-column-dependencies-current"></a>
## 2026-11-06：视图依赖、旧库与事务回归

最终 C4 正常 CMake C++17/O2（程序 SHA 前缀 77418e9c）通过新增原生 242 项检查；同一最终测试旧版实际 120 项失败，中间 C3 实际 2 项失败。7 个既有相关原生测试在旧版及最终 C4 均实际 exit0。28 条真实依赖删除返回 2BP01；29 条普通事务／保存点对照的值、tag、SQLSTATE 和 Ready 与 PostgreSQL18.6 一致，保留先前写入、失败 CASCADE、恢复与冷重启均有记录。

新增 `view_column_dependency_test.cpp` 覆盖真实列／别名／表达式／谓词／排序／分组／窗口／CTE／派生／相关子查询／USING／NATURAL／自连接／CASE/ARRAY/ROW/FILTER/UNION、星号后加列、兼容替换、编译目录列身份、保留注释、列重映射、递归 CASCADE、多子命令失败恢复、真实旧式关系依赖及纯元数据校验。旧版最终 242/120失败/exit1，C3 242/2失败/exit1，C4 242/0/exit0；初版六次 abort 未改为通过。

7 项既有程序为 view_test、view_replacement_contract_test、serial_owned_sequence_test、ddl_rollback_restart_test、temp_alter_column_catalog_test、with_dml_native_command_view_test、catalog_snapshot_test。旧版记录实际复用并核对，最终 C4 全部重新运行 actual0，61 core 与各代冻结源码／stub 对应。

新 201 条协议矩阵仍有 3 条别名运行查询差异，真实旧库升级 42 条仍有 5 条嵌套／字面量运行差异；整表／视图删除 13 条的值与状态匹配 PG，编译目录视图 12 条仍有 1 条别名执行 0A000。全部请求、完整 T7、NULL、诊断和 Ready 原样保留，完整 PG 协议等价仍为 false。原 62 条 SQL 完整回复在旧／C4 间逐字节相同，41 条目录查询及 7 条控制回复相同，原有 9／24 条 PG 差异保持。另保留完整 pg_attribute 89 条调研。

普通事务补充 29 条在 PG／旧版／C4 全部运行，初始及冷开 Main 均实际正常0；C4 的所有值/tag/SQLSTATE/Ready 匹配 PG。前期 C1 首查询超时的 42 条 NOTRUN 保持失败；两个单独 source-first 诊断、旧版完整 42、后续候选完整 42 均有原始记录，未归因为已修复。

正常 270+27 操作的 72 个 Main 正常0，1197 完整回复独立核对。正式库存为 2396 个测试文件（1129 顶层／1134 总 C++），注册 E2E 驱动 684／其中独立 E2E 664 不变；本批没有声称全套通过。原 273 项、22 complete／166 partial／70 unverified／15 deferred_by_user 保持，历史记录增至 285，IDX-07 仍 unverified/evidence[]。754 个 Markdown 路径、725 份保护记录及旧 35 个压缩包保持。 [全部原始值、负向与压缩证据](performance-report-2026-10-30.md#view-column-dependencies-20261106)。
<!-- P35 view-column-dependencies end -->

<!-- P34 projected-toast-demand begin -->
<a id="projected-toast-demand-current"></a>
## 2026-11-06：按需求读取 TOAST 验证

新增原生回归 14 组、83 项检查全部通过；同一测试在旧版实际失败 6 项。另 5 个既有相关原生测试在旧版及最终 C2 均通过，包含多 LOB 正常冷开/恢复生命周期、缺文件错误、BYTEA 和目录声明 550 项检查。原有 62 条完整协议回复、额外 41 条目录查询及 7 条控制回复，在旧版/C1/C2 间逐字节一致；原 9 项与新 24 条 PostgreSQL 查询差异继续保留。

新测试 `ordinary_project_toast_demand_test.cpp` 在旧版为 14 组/83 检查/6 失败/实际 exit1，C1 与最终 C2 均 83/0/exit0。检查真实正常值、NULL/空串/字面 marker、10KiB BYTEA、缺失未选中 LOB 的正向读取、选中 LOB 行输出前失败、Filter/错误 schema 回退、恢复原 LOB 文件和虚拟依赖；没有放宽旧断言或将 NOTRUN 算通过。5 项既有测试为 ordinary_project_bitmap、multilob_storage_lifecycle、shared_toast_runtime_missing_file、pg_class_declaration、bytea_large_toast。

最终 C2 的 6 个原生程序均实际 exit0，完整匹配 61 core/本代 stub/冻结源码。原协议仍 strict CLI exit1；新 41 查询 PG 参考 41/41，产品 17/41、24 条差异不变。最终 C2 新运行 4 个初始/冷 Main 正常 exit0，旧版原始完整记录复用并独立重新解码。

C1/C2 各 270 个正常计时/54 Main、1098 完整响应/96624 行/386388 单元/27864 NULL 独立校验；CPU 诊断另有 2100 回复和 4 个正常 Main。正式库存 2395 文件、1128 顶层/1133 总 C++；注册 684/664 standalone E2E 保持，本批不代表整套通过。[全部原始值与边界](performance-report-2026-10-30.md#projected-toast-demand-20261106)。
<!-- P34 projected-toast-demand end -->

<!-- P33 pg-class-declaration begin -->
<a id="pg-class-declaration-current"></a>
## 2026-11-06 pg_class declaration validation

Final C2 normal CMake C++17/O2 builds successfully. `pg_class_declaration_test.cpp` passes all six groups/550 checks with the exact61-core objects and own test stub. It covers real34 fields/type closure, ordinary DDL, cold CSV/read-only absence, startup idempotence, qualified user type shadows, unavailable selected inputs (NULL included), and finite class/type/attribute/dependency bootstrap conflicts before mutation. It compares complete declared class/type/attribute values, rather than relying on abbreviated `toString()` summaries. The old generation executes the same final test with48 checks,31 failures and one unavailable-capability NOTRUN; it is not counted as a pass.

`catalog_snapshot_test`, `declared_type_physical_catalog_test` and `pg_stats_histogram_extent_test` each pass on old and C2. Earlier C1/native fixtures and both launcher failures remain in the archive. Initial quoted raw-native DDL fixtures retain four failed checks each; the normalMain41-query run confirms the corresponding quoted CREATE/ALTER/index operations succeed. C1's wrong12-byte type and TEXT25 NULL output are rejected, not accepted fixes.

The original58 targets plus four cold controls run unfiltered on before/C1/C2: all strict drivers exit1 with the same9 PG difference IDs. Before/C2 has60 full replies byteexact; only two self-identity replies gain the actual row. C1/C2 all62 replies are exact. Every frame/NULL/T7/error/tag/Ready is independently decoded from the saved bytes.

The additional frozen41-query matrix runs on real PostgreSQL18.6 and each product generation. PG passes41/41; before and C2 each match17/41, while24 queries differ (258 before/246 C2 failed field/shape checks, not246 different queries). Another seven controls per product preserve `pg_attribute`42P01 and type input limitations. Five returned type rows match PG on nine fields; independently decoded catalog files match all34 attribute rows on six fields. Input/result codec completeness is not asserted.

The actual old-image upgrade and cold restart run20 requests with fresh-C2 byteexact replies and normal actualwait0 for both servers. Old class/type/attribute rows and the original old image stay unchanged. Full ordinary performance validation runs270 timed requests/1098 replies, independently checks96624 rows/386388 cells/27864 NULLs, and closes54 Main processes normally. All raw values, tails and regressions are retained in the [report](performance-report-2026-10-30.md#pg-class-declaration-20261106).

Inventory:2394 test files,1127 top-level/1132 total C++ files;684 registered drivers/664 standalone E2E drivers unchanged. The full inventory is not represented as an all-tests pass.
<!-- P33 pg-class-declaration end -->

<!-- CURRENT_LEGACY_LOG_APPEND_20261106 -->

## 普通日志追加与实际查询成本（2026-11-06，P32-C1）

正式只新增1个CPP回归，最终源码 `d2669466…`。完整61对象/core加本代stub的实际配对中，before为6组普通行为通过、7组新机制能力缺失NOTRUN；C1共13组、105项检查通过，测试作用域内观察38次writev封装层命中（含控制返回，非物理写盘次数），两代最终测试编译stderr均0字节。覆盖NUL/UTF8/空串/换行/长值、追加、轮转、权限、symlink、实际打开错误和有限EINTR/短写/零进展/EIO控制；128条并发短记录比较完整无序集合，不宣称任意大记录跨短写原子性。两项既有TRUNCATE测试两代均正常0，完整stdout/stderr逐字节一致，实际分别记录5次/3次TRUNCATE日志。

原58个严格协议目标及4个额外冷锚控制全部保留，两代产品各完成62请求、目标各53/58，原9项普通差异未关闭，strict CLI仍为1；全部62请求/响应原始字节一致。每代实际207行/848单元/156个NULL，4个初始/冷Main均取得真实子进程wait0、PID/组消失。复用此前真实PG18.6参考，没有重跑PG，没有把目录来源元数据或选中错误的TEZ/EZ差异改成PASS或暂缓。

本批固定成本另有270计时/54正常Main，不能与native检查数相加冒充全仓测试。正式测试2393，源248/头177；仅新增1CPP，不宣称全套重跑。[本批完整报告](performance-report-2026-10-30.md#legacy-log-append-20261106)与[可逆原始证据](benchmarks/legacy-log-append-20261106.json.gz)。以下为各代历史。

<!-- CURRENT_FIXED_ID_PROFILE_20261106 -->

## 主键查询路径的独立诊断（2026-11-06，P31）

复用P30真实三表闭库状态，P29-C2→P30-C2→同一P29-C2三个实际正常进程，各512次原样主键查询。外层及3个Main实际wait0、PID/组退出，零重试。独立重解1,575个完整响应：1,536个目标的394,752个ID、18次前后完整三表模型、1,560次物理SELECT的全部7项描述字段/标签/ReadyI均相符；合计400,179行观测、412,533单元、1,230个NULL。6次EXPLAIN旁观均为TableScan→Project；6次pprof渲染和独立原始PC解码全部真实退出0。

这是CPU8/libprofiler诊断，不是新增正常性能矩阵或功能验收。原P30正常负例、58个严格协议目标和9项差异保持，回退原因仍OPEN。pg_class完整34列声明及来源补全只有依赖审计和未运行方案，未应用生产补丁，不计功能通过。正式测试仍2392、源248/头177、原功能273项状态与history281不变；没有新正式测试或重跑既有native/PG参考。完整定义见[本批报告](performance-report-2026-10-30.md#fixed-id-query-profile-20261106)与[压缩原始证据](benchmarks/fixed-id-query-profile-20261106.json.gz)。

<!-- CURRENT_ORDINARY_PROJECT_BITMAP_20261106 -->

## 当前行NULL位图复用与按需求捕获（2026-11-06，P30-C2）

普通物理表的直接投影现在按选中列需求，复用当前行已有的完整NULL位图；局部作用域在正常或异常退出时恢复，只选非空物理列时不再复制。既有值提取、虚拟列求值顺序、TOAST处理、文件身份校验函数和暂停范围保持。

正常CMake C++17/O2的最终C2，在固定/单BYTEA/多LOB三类直接完整查询中，墙钟中位数对前后两次旧版降低约9%–32%，服务器CPU降低约11%–35%；这是本机各三样本的有限结果。主键投影负控仍有回退，固定表服务器CPU为+26.19%/+25.84%，单BYTEA主键查询墙钟为+8.03%/+3.65%。跳过多余复制没有消除这些回退，其根因未证，不能据此归因频率、负载或噪声，也不能称全库提速。

本批仅优化真实物理 TableScan→Project 的当前行 NULL 读取：选中列需要 NULL 信息时，借用该行已拥有的完整位图，并在局部作用域退出或异常时恢复；C2 在既未选中可空物理列、也未选中虚拟列时不复制位图。新增的普通 native 回归在 before 执行13组并明确保留6组能力缺失 NOTRUN，C1、最终C2各19组全部真实通过。NULL、空串、文本 NULL、TOAST及标记字面量、相反位图、虚拟列需求、嵌套和异常恢复均保留完整值与真实RID观察。外部 current-file lookup 的有限机制计数不等于 SQL 耗时或整体提速。

真实 PostgreSQL 18.6 参考完成全部58个固定目标及7个控制，观测205行、846个单元、156个NULL；before、C1、C2分别完成全部58个目标及4个冷锚控制，各为53/58，观测204行、845个单元、156个NULL。三代产品全部62组查询的请求、响应及差异保持原始字节一致。既有9项普通差异未关闭：4个初始目录源锚和4个冷目录锚涉及 pg_class 自身行及目录RowDescription源OID/列号，另一个是选中虚拟列除零时PG发送TEZ而产品发送EZ。它们不是PASS，也不是用户暂缓；没有删例或放宽期望。PG启用fsync，所有初始及冷上下文都取得实际子进程wait0，PID和进程组消失。

六个既有native测试在before、C1、C2三代共18次全部真实通过，stdout/stderr逐字节一致；C1与C2新增native的完整stdout也一致。正式只新增1个CPP测试，完整58条portable协议草稿仍以known_gap形式保留，未运行、未注册，不能计为通过。本批不改参数帧、provider生命周期、retained规划或暂停的输出format2边界，不宣称目录/协议错误帧已与PG等价，也不据这些功能回归单独宣称全查询性能提升、默认程序晋升或服务重启。

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

同一机制源码在旧版和C2各实际52219检查/20探测/4096真实行通过；每端六个真实分配、read和目标堆阳性控制命中，20个完整RID集合都精确且目标堆fetch为0。独立归约16614项通过，只核既有输出，没有重跑机制程序。正式reader夹具保留八组110探测：旧版2217检查中4项失败均属零字节文件的两API合同，C2为2215检查/0失败，110次目标堆fetch均0，另有10次真实堆fetch阳性。检查数随实际分支有所不同，未改同源SQL或预期。

新82命令/93动作协议旧版12项失败仅在零字节文件的等值、范围、LIKE结果与路径；C2全部通过。完整原始帧、三列七字段描述符、实际关系OID、NULL/空串/UTF8/DEL、命令tag和Ready均保留。坏CRC/部分头各自从同一健康代损坏并恢复，不是失败后重试；合法非空旧文本与24字节V3零记录是独立正例。原120命令C2通过，PG75复用对照保留73/75原始帧一致及两处DELETE严格差异。

C1生命周期原source_unchanged=false回执原样保留：独立核对正式源、冻结源、计划和基线SHA完全相同；旧运行器source变量被循环中的数据文件遮蔽。C2改用captured_source。该离线核验没有重跑原生测试或改写旧回执。首次归约脚本的诊断参数名冲突也单独保存，修正只影响离线脚本。

这些是有限专项与相关入口，不是全部测试文件重跑，不是PG全功能验收。最终注册数量、完整相关名单、所有首次失败和成本分布见[本轮报告](performance-report-2026-10-30.md#gist-probe-reader-20261106)；下方历史完整保留。

生产解析器7个原文字节片段及4个完整依赖（含真实Numeric实现）单独使用ASan/UBSan编译并实际运行一次：12,355检查/8组/2021帧/4042次解析全部通过，含217个非零截断、1744个单bit变异、CRC合法结构异常、1MiB两侧、数值及两个visitor形状；开启泄漏检测，stderr为空。它是解析器组件检查，完整SQL/堆行为由上述真实引擎夹具分别验证。

6个旧协议入口全部正常0；生命周期all2879检查及独立冷启183检查通过，各阶段统计文件在冷启前逐字捕获。

当前为2386个测试文件：原2383份逐字保持，新增两份CPP和一份协议测试；顶层CPP1121（含链接桩）、全部CPP1126/子目录5，登记682个driver/662个独立E2E。源248/头177，754份Markdown路径和725份保护记录保持；26份旧gzip逐字保持，新增第27份。原273项及22complete/166partial/70unverified/15用户暂缓状态、277条历史保持，仅增加第278条partial。

<!-- CURRENT_GIST_EMPTY_BOUNDS_20261106 -->

## GiST 空字符串、边界与 LIKE 检索（2026-11-06，P26-C4）

最终C4普通CMake C++17/O2构建退出0。格式组件O2与ASan/UBSan（含泄漏检测）各8434检查；现有API生命周期2879检查及独立进程冷启183检查通过；typed接口806检查/9组/68探测全部目标堆fetch为0，并有4次真实堆fetch正控制；文件归属冲突四个语义探针通过。17个有限native入口（其中2个新、15个旧）和6个旧协议入口全部正常0；迁移后的all-AM native实际12168检查/17场景，恢复实际3579检查/18workers（5次预期SIGKILL、13次正常0，36份完整输出及真实marker/event保留）。新协议120条命令、15个索引图像记录及六阶段真实heap RID/NULL见证通过，含正常双进程冷启。

真实PG18.6安装并加载btree_gist后重放75条共享SQL；最终C4逐项比较3231行/19382单元（661个NULL）、248列描述符的1736个元数据字段。75/75语义及描述符一致，73/75完整帧一致；两次DELETE本项目额外发送零字段T，PG只发CZ，严格全协议通过仍为false。两端关系OID各由真实目录锚定；PG正常退出、PID已回收，未比较PG访问方法格式、路径或速度。

首次结果全部保留：旧版现有API夹具183检查中21失败；文件名冲突v1先因设置顺序失败，v2真实执行后旧版1失败、C1四失败，C2及最终C4修正通过。C2首次协议的117个RID见证解析错误与1个真实全宽索引浪费分开记录；C3旧LIKE严格性能路径失败由C4独立LIKE角色修复。PG首次启动角色设置错误时0条共享SQL；修正initdb角色后才执行首次75条SQL。三个生命周期冷启前.runtime_stats只有原始长度/SHA清单、未保当时字节，明确标为metadata-only；所有成本459图像、实际捕获流与其余可用阶段字节完整归档，不凭冷启后统计伪造早期图像。

当前清单为2383个测试文件：原2378中2374份逐字保持，4份只迁移当前GiST格式断言并加强V2兼容/空串真实RID检查，另增4个CPP及1个协议测试。顶层CPP1119（含链接桩）、全部CPP1124/其中子目录5；登记681个driver/661个独立E2E。源248/头177，754份Markdown、725份保护记录保持；25份旧gzip逐字保持，新增第26份。原273项及22complete/166partial/70unverified/15用户暂缓状态、原276条历史逐字保持，仅prepend第277条partial。

这些是有限专项检查，不是2383文件全量重跑或PG全功能验收。未运行R151原341/103矩阵，未扩展已有用户暂缓范围。完整源码、计划、首轮失败与性能证据见[本轮报告](performance-report-2026-10-30.md#gist-empty-bounds-20261106)。以下 P25 及更早段落均保留当时的实现、计数和未完事项；本轮状态以本节为准。


<!-- CURRENT_REINDEX_ACCESS_METHODS_20261106 -->

## 普通访问方法 REINDEX 与空文本生命周期（2026-11-06）

本批补入普通 `REINDEX TABLE` 的 B-tree/Hash/Bloom/FTI/GIN/GiST/SPGiST/BRIN 重建、物理 Bloom NULL/空文本维护与 UPPER/LOWER 表达式索引 SQL 桥接。有效旧头的 BRIN `pages_per_range` 与 Bloom `k` 保留；`m` 随真实新键集按既有 loader 默认128 bitsPerEntry重算。旧头损坏/缺失时回退宽64/k7。默认程序仍为 `425165…`，没有服务重启或默认升级。

当前统计：2378 测试文件（原2375字节不变加2 CPP及1协议），顶层CPP1115（含链接桩）、总CPP1120/其中子目录5；登记680个driver/660个独立E2E。源248/头177，754 Markdown、725保护记录；24份旧gzip逐字保持，归档总计25份。原273项22/166/70/15及275条历史保持，仅新增partial第276条。

C2 主 native 实际 12096 检查/17 场景通过，恢复 native 实际 3183 检查、8 控制/18 workers （5 次预期 SIGKILL、13 次正常退出）；24 条 PG18.6 共享命令比较 1343 行/8058 单元及全部字段声明。31 个既有 native 与 5 个相关协议及 auto3 通过；另一个相关协议仍失败，旧版配对尚未到相同阶段。

GiST 空文本编码、native-only 丢失定义、虚拟 Bloom NEW NULL 和上述失败均保留普通未完；不关闭原273项。

本批本地专项协议实际69条命令/40个文件图像通过；PG共享24条、7次完整人口结果共1,343行/8,058单元（441个NULL），42个列描述符逐项检查7个字段。这些是6个数据列的描述元数据，并非7个数据列；两端tableOID各自真实锚定。PG保留4个实际BRIN metapage，损坏/丢失时本地宽64与PG intact宽1明确分列；事务内1个ReadyT图像没有CHECKPOINT或物理页读取。

恢复控制覆盖真实V2持久标记、首次辅助文件旧代备份/新代发布、全部rename及目录fsync后的进程中断，并保留V1兼容控制、私有文件fsync/发布rename的定点EIO与正常控制。5次预期SIGKILL不是普通退出；18个worker的36份完整stdout/stderr均保留。该控制不覆盖发布标记前崩溃的临时文件回收，也不等价完整PG事务WAL/并发恢复；原有混合reindex_atomicity回归仅原样观察一次，不扩展安全/TDE范围。

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

本批新增三个永久入口：analyzed_scalar_statistics_test（真实DDL/heap/ANALYZE，641检查和独立cold2）与analyzed_scalar_collector_test（22个计算/边界控制）。最终16相关入口均0；计算组件启用leaks的ASan/UBSan通过，非整个Capture/StorageEngine/数据库的SAN。Main4条SQL成功，pg_stats2条0A000仍失败；PG189中保留5个差异。旧2342测试与725保护记录保持，当前2345/顶层1094/去桩1093/子目录5。

原始输入、正常和失败构建/执行、PG两种locale及完整9进程成本见[报告](performance-report-2026-10-30.md)和[归档](benchmarks/analyzed-scalar-collector-20261105.json.gz)。

最终C4还修复默认自动分析的可见性：旧C3在批量INSERT达到阈值时生成空列快照；现在普通事务成功COMMIT、清理原命令视图与锁后，按已有改动表集合触发分析。失败/回滚不提前发布；只有分析成功才扣除已统计修改数，保留并发新增计数。维护错误不把已持久提交伪装成失败。25条普通协议SQL验证INSERT55、显式事务提交110、UPDATE110、DELETE50、回滚不改快照、ALTER SYSTEM+reload关闭自动分析后显式分析105，并核对5次EXPLAIN基数；服务器正常0。原恢复入口1574检查及7个worker全部正常0。共享IO/分配失败、两阶段提交的自动统计调度及显式事务内ANALYZE的全部回滚语义仍未完整验收。

按用户清理要求删除93209个旧编译文件，实际空闲增加约658GiB；分区85%→72%、空闲约1.5TiB，项目约111→28GiB。源码、原2342测试、全部已跟踪文件、当前C4/default425及PG18参考工具已核对保持。后续以现有CMake缓存和当前core复用构建，证据归档后清理可重建产物。删除在9样本全部结束后进行；只读du盘点与最终对照的回测段重叠，全部结果保留，不称为无并发负载测量。

第三入口为tests/analyzed_scalar_auto_protocol_e2e_test.py，25条SQL原始wire和5份真实快照均留存。两个配置控制草稿误用SET/SET GLOBAL，被当前SQL模式拒绝，错误原样保留；最终沿用既有ALTER SYSTEM及pg_reload_conf，未改配置策略。


<!-- CURRENT_ANALYZED_STATS_SNAPSHOT_20261105 -->

## 完整统计视图的快照基础（2026-11-05，P10）

P10新增3个生产头文件及 `analyzed_statistics_snapshot_test.cpp`，源234/头164，原2341测试文件保持，新增后2342；顶层1092 CPP含test_stubs（排除后1091入口），另5个子目录CPP。正常CMake没有重编译，62旧对象与后端 `30c57ca8…` 保持；正式原生测试采用冻结的新头文件、经哈希核对的61个旧core对象和实际新编译stub794，既有161头/231源码字节不变。

同一组件测试在未优化O2、最终O2、最终ASan/UBSan和正式core入口分别通过：每次父进程568次检查，3个fresh子进程各1次检查。包含独立Python220B固定字节夹具、逐字节损坏/全截断前缀、修复校验后的非法字段、拥有数组/记录/域身份、冷读、实际OFD阻塞的并发写入、关系整组替换和代别耗尽；这些是组件检查，不是568个SQL场景。SAN覆盖组件与StatsPersistence，启用泄漏检测，不是全库SAN。

真实PG18.6参考核对17字段并采集8列的全部7类槽及实际数组元素OID；仅声明/类型契约对应，不宣称产品统计数值已与PG逐项一致。当前普通Main的两条全17列查询均0A000、server正常0；记录为2严格失败。9次组件成本测试全部正常0且完整payload相同。[完整报告](performance-report-2026-10-30.md#完整统计视图的快照基础2026-11-05p10)及[归档](benchmarks/analyzed-statistics-snapshot-20261105.json.gz)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧节保留各批次当时结果。


<!-- CURRENT_STORAGE_LIFETIME_20261105 -->

## 存储退出期生命周期（2026-11-05，P9-C2）

最终P9-C2正常程序 `30c57ca8…`，源231/头161；C1、C2各按正常CMake重编TableManage一个单元，另61对象依赖及fresh62血缘核对，fresh stub794。新永久 `relation_list_lifetime_test.cpp` 的9控制在main和更早owner析构各执行一次，最终18次检查通过；旧O2在第11次检查失败，旧ASan报use-after-free，新ASan/UBSan通过。原562项名称/列表/WAL组件草稿在旧/新O2与ASan/UBSan四次均通过，仅作兼容性控制，不验收R218功能族。

最终23个相关入口、新生命周期入口及原七阶段恢复均通过；完整七阶段Memcheck父进程+7worker均0错误，未启用泄漏检查。P9-C1的advance134和Memcheck冷进程维护表错误完整保留。正常Main固定22对象与P8-C3/P9-C1相同，含轮询计数为C1 219/224、最终C2 212/217，各5项元数据失败，两个进程上下文各正常0。

原2340测试文件逐字保持，新增1文件后2341；顶层1091 CPP含test_stubs，排除后1090入口，另5个子目录CPP。完整恢复9次/108检查通过，无重采；详见[报告](performance-report-2026-10-30.md#存储退出期生命周期2026-11-05p9-c2)及[归档](benchmarks/relation-list-engine-lifetime-20261105.json.gz)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user，Goal active。下方旧节保留当时版本的结果。


<!-- CURRENT_RESTORE_TRANSACTION_HORIZON_20261105 -->

## 物理恢复事务号水位（2026-11-05，P8-C3）

本批最终P8-C3 `d778afbd…`，源231/头161；C++17、O2、ICU、zlib及TLS stub保持。正常CMake在C1按真实依赖重编16个单元，C2重编WAL一个、C3重编TableManage一个，追溯原fresh62构建；测试桩794重新编译并冻结。相关21个旧入口通过；独立文件读取19控制、事务分配器8个顶层及3个fresh-peer控制、WAL32控制通过。并发测试的3种语义由5个实际工作进程执行，全部正常退出。

同一七阶段原生控制：P7-C1在较早目标上数据失败；P8-C1七阶段全部通过；P8-C2/C3均在advance阶段出现wait状态134，后四阶段NOTRUN。已有实际写入循环，精确异常位置/原因未知，必须保留严格FAIL。独立低目标数据探针及正确目标计时通过不替代此入口。

正常Main原始固定22操作在C1/C2/C3的完整固定响应逐字节一致，首次探针为4/4；各版均有5项探针描述符/缺目录身份失败，其他值、命令、NULL和冷启动检查通过。含实际轮询的检查计数分别为C1 249/254、C2 234/239、C3 209/214；这三个检查总数不同不代表改变固定SQL。旧版首次1/4及冷启动−6完整保留，未作原因推断或重试。

新增3个原生入口和1个同源夹具头，测试文件2336→2340，原2336字节保持；顶层1090个CPP含test_stubs，排除该桩后1089个入口，另5个子目录CPP。注册时缺夹具头、缺wrap注解的首轮构建失败保留；补齐同源夹具头和首行wrap注解后，并发入口93335实际编译运行exit0；最终夹具头及七阶段入口仅清理原型注释，控制体和断言保持。新入口不能概括为全部通过。两轮公开恢复各12次、各144检查全部通过，所有长尾及完整失败均见[完整报告](performance-report-2026-10-30.md#物理恢复事务号水位2026-11-05p8-c3)和[机器归档](benchmarks/restore-transaction-horizon-20261105.json.gz)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧节保留各批次当时结果，不能覆盖本节的失败与范围。


<!-- CURRENT_BACKUP_ANALYZE_20261104 -->

## 备份摘要与 virtual 列统计（2026-11-04，P7-C1）

本批 P7-C1 `724c1147…`，0.2.0 / C++17 / GCC13.3 / O2 / ICU74.2 / zlib1.3 / TLS stub。ANALYZE永久20控制由旧14通过/6失败变为20/0；摘要永久864/0，独立固定预期全部保留；相关12个原有入口全部通过。原2334测试源字节保持，现2336；顶层1087 CPP（排除桩1086），另5个子目录CPP，未全仓重跑。

PG18.6原34参考全部保留，两处最初帧顺序预测错误亦保留；当前正常Main同34为17匹配/17差异，零服务故障、正常退出。差异为11个qualified pg_stats入口、REINDEX INDEX与ANALYZE列列表各1、错误帧2、DELETE空描述帧2。实际停止后的.stats证实普通/STORED统计存在、virtual统计缺席；不代替缺失的SQL视图。

两代创建备份清单与所有载荷摘要完全一致。首轮旧→旧恢复探针行数失败保留；独立既有探针目标的四组交叉恢复均通过完整1000+4行/真实RID/NULL。完整API9与正常SQL DDL72均成功，wall p50为2快/2慢，ALTER与TRUNCATE慢于两次旧版。预检和所有原始失败见[完整报告](performance-report-2026-10-30.md)及[机器归档](benchmarks/backup-analyze-20261104.json.gz)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方按批次保留当时记录，当前状态以本节为准。


<!-- CURRENT_GENERATED_DEMAND_20261104 -->

## 虚拟生成列读值需求与完整 SQL 实测（2026-11-04）

本批源码 P6-C8，对应正常程序 SHA256 `45264111…`，保持 0.2.0 / C++17 / GCC 13.3 / O2 / ICU 74.2 / zlib 1.3 / TLS stub。根目录默认程序仍为 `425165af…`，现有服务未重启。

新永久原生入口[读取需求36控制](../tests/generated_column_demand_test.cpp)与[真实spill身份10控制](../tests/generated_context_spill_validation_test.cpp)通过；实际运行C6的61个核心对象/159头文件与当前C8一致。相关25原入口在C5为24/1，旧C2同一quoted extract普通控制也失败，本批未修，未新增deferred；不称当前全仓重跑。

C8固定258为238通过/20差异（13/3/2/2），较C2新增82整项通过、零整项回退，另有6项描述符组件损失。原767观察仍129差异，完整记录对旧版/C6一致、九enum原始记录一致。所有原始SQL、预期、失败与raw保留。

当前Main15个实际上下文正常退出；72个正常基准请求、7200行/25200单元零失败。原2332测试源字节不变，现2334；顶层1085 CPP（排除桩1084），另5子目录CPP。首批编译/包装器/接收器缺口与拒绝C7见[详细报告](performance-report-2026-10-30.md)及[机器归档](benchmarks/generated-column-demand-20261104.json.gz)。

原273项仍为22 complete /166 partial /70 unverified /15 deferred_by_user；Goal active。下方旧批次保留当时事实，当前状态以本节为准。


## ANALYZE 与 REINDEX 成本优化（2026-11-04）

本批源码 P5-C2（a3baaca6…）优化同次 ANALYZE 的类型/locale 比较和真实行 NULL 位图，以及 REINDEX 逻辑 map 保留。C++17、原维护频率/持久化和用户暂缓边界保持。ANALYZE p50 369–403→74 ms，范围 UPDATE 600–650→389 ms；完整 35 操作为4快/28慢/3居间，36组查询为4快/6慢/26居间，整体性能提升尚未验收，默认仍为425，现有服务未重启。

原相关22入口全过；新比较8规模/240完整字段、bitmap caller新旧各12/0、REINDEX拥有值/RID8/0，私有观察器8/3344与完整PUBLIC60阶段分开。新增3个永久入口，原2329测试字节不变，现2332文件/顶层1083 CPP（排除桩后1082顶层入口，另有5子目录CPP）；未宣称全套重跑。55资源、630动作、1638查询均完整收集；首次预检/编译/观察失败保留。

详见[完整性能报告](performance-report-2026-10-30.md)和[本批机器归档](benchmarks/analyze-reindex-main-costs-20261104.json.gz)。下方旧批次保留其当时结论，当前以本节与报告首节为准。

## C4功能与完整成本记录（2026-11-04）

当前源码C4（9d85c688…）补入RETURNING纯声明与真实来源元数据、虚拟列拥有值及部分MERGE来源输出。新增123原生控制122通过/1个严格元数据差异，新132协议对照全过，原56项为55通过/1差异；273项状态未变，Goal继续active。

实际35操作为7快/19慢/9居间，范围UPDATE及REINDEX仍慢；36组完整查询25快/2慢/9居间。未通过本机默认程序的整体性能提升验收，默认仍为425（425165af…），现有服务未重启。独立API512行的分配/CPU收益及空表/单行负值分开报告，不能概括为所有数据库操作已提速。

新增5入口：[声明46](../tests/returning_declaration_api_test.cpp)45/1，[虚拟列28](../tests/virtual_generated_owning_test.cpp)、[语法14](../tests/local_expression_syntax_test.cpp)、[REINDEX16](../tests/reindex_single_pass_test.cpp)、[MERGE19](../tests/merge_returning_source_owning_test.cpp)全部通过。quoted stored extract严格integer预期保留。原2324测试字节未改，现2329文件、1080个cpp文件（排除链接桩test_stubs.cpp后为1079原生入口），未宣称全部重跑。C4选取57原生56/1、31协议24/7，10533共同观察无旧正确变错；严格56旧48/8→55/1，新132旧9/123→132/0，新33生成语义仍27/6。原579为451匹配/128差异，全部raw/canonical/原正确组件连续；暂停enum九项保持，原341/103未执行。

详见[本批报告](performance-report-2026-10-30.md#returning-声明真实行值与操作热路径2026-11-04)及[机器归档](benchmarks/returning-declarations-hotpaths-20261104.json.gz)。以下为历史记录。

## 2026-11-03 内置函数注册与复合 RETURNING

最终功能程序C3 `425165af…` 与初期C2宽回归按不同代别记录，不能将所有通过项拼成C3全量验收：

| 实际程序与入口 | 完整终态 | 范围说明 |
| --- | --- | --- |
| C2宽原生147 | 145通过/2既有失败 | 原math、routine失败保留 |
| C2宽协议95 | 86通过/8完整语义失败/1启动失败 | 启动超时103、0 SQL；原记录保留 |
| C8宽协议95参考 | 85通过/10未通过 | 其中2入口未完整观察，不能称10个已完成语义矩阵 |
| C3原生选取56+新增RETURNING1 | 57入口、56通过/1既有math失败 | 新注册14控制与新RETURNING20控制通过 |
| C3协议选取31+CASE准备1 | 32入口、24通过/8既有失败 | CASE准备额外入口保留3项旧差异 |
| 固定301类型协议 | 188匹配/113差异 | 仅第254项INSERT binary RETURNING新增通过，其他300项完整实际响应保持 |
| 固定83/111/68/16 | 278项实际响应与C8相同 | 既有配置、REGTYPE等差异没有改预期 |
| 新PG参考RETURNING56 | C8为36通过/20差异，C3为48/8 | DELETE42内容和事务后状态正确，仍缺描述符 |

新增成本夹具分别记录默认分配器与累计new观察，不把单次局部CPU当作完整SQL分布：

| 局部夹具 | C8→C3实际观察 | 解释范围 |
| --- | --- | --- |
| extra helper，512行×3，捕获关闭 | 1,218,134→213,590次请求；88,677,248→14,491,520B | 累计C++new请求字节，不是live/峰值/RSS |
| 同夹具，捕获开启 | 1,450,070→445,526次请求；126,917,504→52,731,776B | 拥有值捕获的原功能校验保留 |
| 默认分配器extra helper，捕获关闭 | 单次进程CPU466.525→56.841ms | 独立辅助循环，不是整SQL或PG速度 |
| 公开游标512行 | 分配不变；单次冷CPU20.750→35.862ms | 慢值保留，不以helper收益替代 |
| 独立构造器 | 首次多1次请求/96B；预热后64次构造及25,216项检查中29,760→8,704次请求 | 共享表冷成本与常驻成本仍存在 |

原2322测试文件保持，新两个原生文件共34控制接入后为2324文件、1074个原生入口；本批头文件157个，ExprEvaluator实际sizeof保持248B；文件数、入口数与控制数不混算。新RETURNING20同源C8为17失败、C3全部通过。首轮原生20/2源于测试在另一序列会话读取currval，源码/失败保留，仅将原setval/currval观察置于同一真实Session；SQL与20项预期不变。C2公共回调重入诊断的2处回退由C3修复，原3项完整实际观察恢复到C8。

新56项剩余22/23/24/40/41/42只缺OLD/NEW别名描述符，47为虚拟数组载荷，53为历史数组NULL位图；均为普通未完项，未新增暂缓。完整查询S3/C3/S3的1638请求（1296计时、324预热、18 SET/SHOW（9+9））全终态，36组p50均低于两次S3，每预算12/0/0；不是与PG新速度比较。35类操作C8/C3/C8为630样本、每标签210项完整校验、3进程正常结束，18快/2慢/15居间；update_range和reindex为慢项，基线漂移全部保留。

全部证据、旧启动失败及性能结果见[本批报告](performance-report-2026-10-30.md#内置函数注册与复合-returning2026-11-03)；独立机器归档见[全部原始统计](benchmarks/function-registry-returning-20261103.json.gz)。以下C8及更早记录保留原始代别、失败和时钟数据。

## 2026-11-03 原始类型行物化分配优化

C8 `91d339db…` 仅重编实际修改的ExecCPP，沿用有效的新ABI对象谱系。选取的26原生入口全部通过；31协议入口仍为24通过/7既有失败入口。固定301观察仍有114差异，83/111各两项配置差异，REGTYPE68仍为57通过/11普通差异，参数16全部通过；这些入口的完整实际响应与C7相同，没有以修改预期刷绿。2322个既有测试文件均未改，不能将选取集称为完整106/95重跑。

实际512行公开游标的累计分配比C7少1536次请求/393216B，比S3少1024次请求、冷次少188256B/重启少188416B。完整查询唯一窗口的36组为5快/18慢/13居间，基线漂移和全部慢项保留；分配减少不是RSS或普遍速度结论。完整收据及原始样本见[本批报告](performance-report-2026-10-30.md#原始类型行物化分配优化2026-11-03)和[机器归档](benchmarks/owned-datum-primitive-20261103.json.gz)。下列C7及更早记录按其当时输入保留，原273与既有暂缓不变。

## 2026-11-03 拥有值复合输出与引用类型存储

本节对应基线 `6c27d232` 与最终功能程序 C7 `f135335e…` 的 226 源码输入。先以完整 fresh 构建建立新 ABI，再按真实依赖重编；C7 两个实际 CPP 重编、60 个对象经谱系核对复用，不能称每代均重编全部 62 TU。原始失败、首轮构建/配置问题与后续正确谱系分别保留。

| 批次/入口 | 实际结果 | 解释 |
| --- | --- | --- |
| C5 完整原生 106 | 105 通过/1 既有失败 | 例程数组签名入口的原退出失败保留 |
| C5 完整协议 95 | 86 通过/9 既有失败入口 | 5011 明细比较无新增错误结果，220 完整相等；不是全部通过 |
| C7 最终选取原生 26 | 全部通过 | 包含三个新增入口 38 控制；不是全库全量 |
| C7 最终选取协议 31 | 24 通过/7 既有失败入口 | bit、name、Bind、CTE、nested、interval、143 的完整失败继续保留 |
| 固定 PG18.6 301 观察 | C7 114 差异 | 比 AA52 的 201 差异改善 87，原正例未改成错误预期 |
| 数组位图/CAST 83 | 81 通过/2 差异 | 原 SHOW/UTC 配置预期不放宽 |
| 多态数组 111 | 109 通过/2 差异 | 原 SHOW/UTC 配置预期不放宽 |
| REGTYPE 真实物理 68 | 57 通过/11 差异 | 同数据目录两次进程 epoch，索引/COPY/冷读控制保留 |
| REGTYPE 正常参数 16 | 全部通过 | 文本/二进制、NULL、空值、边界及原错误检查 |

三个新增入口为 `owned_compound_spill_codec_test.cpp`（16）、`owned_compound_physical_spill_test.cpp`（12）、`numeric_special_binary_output_test.cpp`（10），共 38 控制。原 2319 测试逐文件保持，新增后为 2322 文件；文件、入口和观察不是同一种计数，不相加称已覆盖功能数。

C4→C6 的 REGTYPE68 从 29 差异降至 13，C7 再修正 CAST 输入数组声明与实际 PreparedReturningExecution 的行物化，成为 11；C6/C7 完整实际响应仅第 10/42 项改变，其余 66 项逐帧一致。C7 原参数16仍全过；自有两个同目录物理进程和参数进程均正常退出。SHOW、链式 CAST、域重命名/删除/重建连锁及 USING 的 11 差异全部按真实普通依赖列在[本批功能与成本报告](performance-report-2026-10-30.md#拥有值的复合类型输出与引用类型存储2026-11-03)，不笼统称暂停项。

原 273 完整对象/状态及 251 条历史原文须保持；本批新增近期记录只为 partial。资源55与1638请求执行完整通过值校验；四个公开成本进程完整3例/0失败。36组速度为4快/23慢/9居间，默认分配器和计数器分开。原 35 类操作的历史停机失败不重写，也不以本轮正确退出控制代替该验收。

## 2026-11-03 窗口暂存候选与类型协议调研

基线 `3d576b8b` 的S3与scratch1/scratch2严格隔离。两候选均完成既有71原生入口+永久10控制入口（72个不同入口）全部通过，额外cold6每代6/0；没有新增永久测试或修改既有2319文件。
两候选各自完整既有61协议入口为56通过/5保留失败，加220入口为62个不同入口57/5。五失败完整日志或JSON逐字保持S3；原四1641及143/8保持，不调查暂停模块。scratch2首轮220完整值/日志0失败但未持久化外层退出码；同源独立终态复核220/0、exit0且完整响应一致，首轮未覆盖。
固定301新调研观察的独立PG18.6参考完整通过预期阶段/字节/来源锚校验；三版实际响应逐项相同，各有201处PG差异，候选driver退出1但自有服务器正常退出0，不计PG兼容验收通过。误连17.2的版本guard失败和本任务UUID库清理结果保留。
每候选独立55资源请求/40000行118000单元、5正常context及预期只读kill/reopen通过；作用为普通最终Sort与这些实际读取控制，不证明窗口预算/写恢复/R15已修。每候选另一次完整1638请求/1620000行4725000单元、9正常context通过，全部1296计时保留。两版均因未获稳定跨预算收益而拒绝，未安装。
类型输出提案仅有ExprEvaluator/TypedSortSpill/PostgresProtocol统一树syntax-only通过；首轮混头63重定义错误、metadata静态阻断与全部依赖仍保留，未称全生产编译/运行通过。安装程序及生产源码恢复/保持S3，Goal active；数据与复现输入见[本批报告](performance-report-2026-10-30.md#窗口暂存分配与类型输出调研2026-11-03)。

## 2026-11-03 窗口结果共享与排序投影

S3 `aa52b5fe…` 使用冻结216源/62生产对象，147头、Main、网络协议、实际frame/context/provider及原共享比较/R177/R185保持；只在ExecCPP增加私有状态/接线，并扩展私有窗口宿主。
原71个原生入口全部通过，加新10控制入口，共72个不同入口通过。新10直接读取512行物理表，经真实绑定/公开cursor验证重复纯LAG/SUM、完整嵌套record/OID数组、同图重启/提前close、独立命名窗口、LIMIT0及22012。
独立审查后加强错误前缀为完整integer行[[1,0,0],[2,-1,-1]]、3列真实OID23及两次重启一致；冻结a585bd7a源在C1/S1/S2/S3均10/0。先前较弱源和所有日志保留。
新固定220观察包括53条窗口查询及DDL/序列seed/post，独立PG18.6为220/0，原C1为220/27，S1/S2/S3均220/0；三版全部case/fullactual/raw/normalized/match一致。
原155观察完整保留，归一化仅由真实id LIMIT0锚定该物理表OID，不放宽类型/格式/消息次序/返回行/错误/计数。稳定/不可变/易变函数、命名声明、wrapper/CASE、hidden排序及双保留别名反例均严格保留。
S3旧61协议首次56pass/5fail，合并新220后62不同入口57pass/5fail；190项本轮首个完整入口通过。Bind463/40、CTE747/29、nested325/42全文log与S2字节一致；interval106/10及原143/8完整JSON字节一致，原四1641观察不变。
190的全部server observation/rawFrames/compatibility oracle及状态与S2成功独立观察相同；随机自有schema UUID导致请求SQL/frontend packet不同，没有归一化任何服务响应，也不声称整log byteequal。
S2旧61首轮55pass/6fail的连接103、planned190/recorded0/attempted0日志保留；终态后同源同S2独立一次完整190/0（53项预期SQL错误、cleanup/序列基线健康），按实际完成观察合并新220为62入口57/5；不改写S2首轮56/6。
原143恰42/43/85/86/128/129六项改善14→8，余137 fullactual/normalized/match不变。剩余catalog2/record-json错误需求3/compound format1 binary3仍开放，不笼统归入暂缓。

同源512行warm实际cursor的new请求：单LAG226489→226489、双LAG419723→232335、四LAG806565→244394、双SUM44542→43514；接收行和restart/close在计量内，初次建图在外。
单份请求字节只差-3B，是微小对象布局记账，不宣称内存收益。S2单份多1请求/8B的原始结果保留。S3仅对完整stage两套策略均无共享/无外来别名且原producer顺序成立时使用原类型化循环，隐藏重排或共享需求仍走完整通用路径。
请求字节、S2完整36项慢值、另行68/48进程诊断、S3资源和全量时钟分别见[本批报告](performance-report-2026-10-30.md#窗口结果共享与排序投影2026-11-03)，不混用累计请求/时钟/live/truepeak/RSS。
原2316测试文件均未改，新3资产按标准发现/单注册行接入；原273完整对象、249历史、725保护记录保留，新增近期记录为partial。文档后最终检查另按真实收据记录，整体Goal active。

S3实际Main资源55请求/40000行118000单元全部通过；完整1638请求/1296计时、三预算和9正常context成功。重复两/四份LAG(length)降低37.94%–40.83%/63.65%–66.34%，但原common8仍16/24组p50高于两次C1；全部36组11快/19慢/6夹两旧，不能宣称全面提速。完整原始12样本、min/max、漂移与S2历史均保留。

## 2026-11-03 窗口整数键缓存与实际执行成本

C1 `56d06bca…` 使用冻结216源输入、62生产对象；公共147头、Main、ExecCPP/共享Set/R185/R177/真实frame-context-provider均与b04基线字节一致。
原70原生全过，加新实际游标36控制入口全过，合计71个不同原生入口。相同新36在C9有3个成本断言失败，33个语义控制均通过；不是三个新SQL bug。
成本控制的warm实际cursor累计C++new 450603→73176，请求bytes19260574→10314925；原Input外层容量在warm已分配，不能当cold/activepeak/live/RSS下降。
新143在独立PG18.6全过、原C9和C1均14处差异且fullactual逐项相同；首次fixture错误要求用户OID>16383导致多3项失败，已仅改为实际pg_type正OID，原源/143-17/PG原预期保留。
14分类为catalog来源2、secondary record-json的rank错误前缀3、重复volatile window调用共享6、compound binary结果3；没有改期望、删SQL或将它们泛称暂停项。
原60协议仍56/4，Bind463/40、CTE747/29、nested325/42整log及interval106/10完整观察与C9一致；加新143入口后61unique/56pass5fail。没有把观察数当独立缺陷数。

公共API诊断分clocks/allocations/evidence独立进程；before与C1各48进程、144次1000行结果全部0，各144000行/396000单元。
16组owning行/descriptor/typeOID/来源/公开context及图rows/loops完全相同，仅排除跨进程指针数值和运行时间；同一图的重启身份由实际public cursor核对。
CPU p50降低41.85%–61.90%、C++分配请求53.89%–63.96%，未将allocation版时间混入clocks，也未将inclusive节点耗时相减后命名为exclusive comparator CPU。
初次建图每case仅一次、仍约11.6–38.2ms；解析/规划/建图与执行分别报告。C++请求字节不是malloc全量、live、peak或RSS。

实际Main普通finalSort匿名FD/EOF、owned强杀和同目录重开等43请求通过，原1072fixture不变，全部自有进程退出。该资源范围不包括Window partition/stage/cache的预算治理。
本批不重跑C9已有35操作shutdown失败，不把新的只读资源正常退出当作修复该停机问题。
额外allocator18进程/162采样/12006行24012单元全0；整数首/末行uordblks +68160B，文本+22544B，单行+48B，close保留容量/RSS不一致原样报告，不声称truepeak降低。
整SQL864计时+216预热+18SET-SHOW完整1098请求全值通过；4/16MB全部16项p50较两旧低22.3%–39.1%，64kB保留3个较两旧慢项及全部漂移，无重跑。
源码范围pre-doc29项、原273完整对象/raw数组与248历史、2313既有测试、725专属记录和754 Markdown路径均保持；最终版本及完整24项SQL样本见[本批报告](performance-report-2026-10-30.md#窗口整数键缓存与实际执行成本2026-11-03)。整体Goal仍active。

## 2026-11-03 类型化窗口宿主与有序输出复用

最终 C9 `c696d3b2…` 使用同代 61 core、Main 及已重编译且与当前公共头匹配的 stub；主回归 68 原生入口全部通过，另两个独立原生入口也通过，合计 70 个不同入口。
新 kernel 223、宿主 37、异常清理 3、interval 除法 39、计数绑定 30、有序输出执行 16，共 348 个控制全部通过。
覆盖真实行源/过滤、derived/CTE/相关作用域、真实 SourceContexts JOIN、三种参数 origin 同图重启、完整 typed owning 值/描述符、NULL/枚举/OID、提前关闭/EOF、运行错误前缀与 WITH TIES。
通用 Core JOIN lowering 未实现；上述 JOIN 结果不能推广为所有 Core 计划支持。

新永久协议四入口分别为窗口宿主 226、输入键 241、interval 除法 106、有序输出需求 201；完整 SQL/PG18.6 预期/错误阶段和原始响应均保留。
窗口宿主 226、有序输出 201 在最终 C9 全过；键 241 与 interval 106另用同一最终C9完整运行，按实际结果与主58合并为60个不同协议入口。
interval 106 的 10 个差异仅为常量错误的 Bind/Execute 阶段；值、类型、SQLSTATE 和 20 个 AVG 控制均符合严格预期。未改预期、未过滤失败、未恢复 R303 等暂缓调查。
最终同代60个不同协议入口为56通过/4失败；其中主协议回归58入口为55通过/3失败，另键241通过、interval106/10失败。新宿主与有序输出已包含，重复运行的同一 201 不重复累计。
既有失败是 Bind/portal 完整 463/40（日志字节与旧版相同）、CTE/window-owner 747/29 和 nested-window-projection 325/42；后两旧版分别 747/453、325/77。剩余差异仍开放，不能泛称全部属于参数阶段暂缓。
前批 binding58 的实际窗口子查询宿主缺口在当前入口已通过。前批完整910原始观察与其中独立SELECT的9处差异仍保留，本批没有把该历史完整矩阵描述为重新全量验收。

最终运行资源三阶段全过：共同查询旧/新/旧 24 次完整 1,000 行核对；不兼容最终排序在 64kB 实采最多2个匿名排序 FD，16MB 未采到，均验证1,000行/3,000单元且 EOF 清理；
自有进程匿名文件打开时 SIGKILL，原目录重开核对基表1,000行/4,000单元及再次1,000行排序。共43个工作负载请求，28个完成全值读取、1个强杀读取、14个SET/SHOW。
1072个源夹具文件不变。另35类操作每类一次全部数据校验成功，但server context退出时抛std::system_error: Invalid argument，返回-6，driver exit1且complete=false；该入口不能记为通过。原始失败保留，R15既有暂缓保持，未调查/修改停机链或重复挑选通过样本。它不是并发/任意写入崩溃或真实TLS验收。

C1..C9 的不可变二进制、源/对象输入、全部失败及修复前后观察保留；初次 native JOIN 夹具/错误阶段、序列脚本和 metadata 检查器问题单独标记，没有算作产品修复。
实际修复包括窗口子查询宿主、窗口键求值顺序/复用、真实 WindowClause 身份、继承/放置规则、FILTER 与参数需求、ntile 首行NULL、interval 分数级联、异常清理及最终输出排序需求。
C7相对旧58的14个新增回退曾被总失败数减少掩盖；逐观察对比后全部修复，原失败和C7已完成的64kB性能窗口完整保留。
新增原生16控制首轮15/1来自两个新增控制未显式启用低层Core常量规划，真实C8 Main同源PG14/0；只对新增15/16使用真实公开规划owner后16/0，原14逐字保留，没有改变通用Core默认规划策略。
C9仅在私有窗口read减少一次整行上下文复制、删除重复分区/排序键datum；三种origin/真实owner/已有输出快照不变，边界检查时点保留。
原2302个测试、原始273项完整对象/原子条款和725个保护文件字节不变；受保护共享排序助手、原聚合执行与真实参数帧/provider/portal边界审计通过。
方法、研究来源、所有慢值、开放项和本批证据路径见[窗口宿主报告](performance-report-2026-10-30.md#类型化窗口宿主与有序输出复用2026-11-03)。整体Goal保持active。

## 2026-11-02 全局计数表达式与动态排序上限

最终相同61 core的48原生入口全过；新执行33、绑定34、语法840控制全过，同最终源旧ABI分别33/31/572处不符。
新协议75与900在PG18.6/最终58c94e05…全过，旧版61/604处不符；binding58的PG全过、旧53、新1处差异仍是实际窗口子查询宿主缺口。
最终36协议按各入口完整结果34/2，既有Bind完整463/40日志与前版逐字节相同；首候选原33为31/2，普通SELECT超时失败保留，最终同源原超时入口通过，不改超时/连接/重试策略。
原完整910观察最终前900全过，4个独立SELECT的10观察中9处差异未删；没有把接受子集充作全部SELECT计数支持。

旧860中仅2个PG合法括号参数FETCH断言迁移为严格AST/源位置验证，其余858字节/顺序保持；原860/2日志保留。
旧VALUES63中1个K1强制落盘前提修正为动态NULL，保留真实落盘/22012/终止；新增K1控制由PG核定精确源/键计数25/24，最终64/0、旧64仅新成本契约1失败。
新执行最初32中2个同类资源前提错误与所有原源/日志保留，最终33严格验证有限无I/O→NULL落盘→有限不新增I/O、真实异常身份、原图/参数来源、取消/超时/EIO与有限FD。

12项实际Main资源全过，4161行/12483单元及精确描述符全值正确；最多2个排序FD、EOF清理，1072夹具文件未变。
35类操作每类一次是有限正确性检查；匿名动态计数排序文件内强杀、重开1000行/4000单元及再次2000行排序通过，不是任意写入恢复或真实TLS验收。
1620整SQL样本/405预热/36SET-SHOW与独立432执行样本/108预热分层保留，后者10800行/32400单元全部owning及同图校验通过，仍无普遍加速结论。
同代构建、全部失败、完整性能样本及开放项见[全局计数报告](performance-report-2026-10-30.md#全局计数表达式与动态排序上限2026-11-02)，证据目录 `build/pg-feature-global-count-xqbytfo5/`；原273/725与所有用户暂缓不变，整体Goal active。


## 2026-11-02 集合整数键与比较参数优化

原44原生全过，加19真实算子metadata/restart/spool/成本控制，最终45全过；旧19仅6项成本预算失败，功能metadata/resource全过。
新66在PG18.6/前版/新版同源码全过，33协议32/1；原112/143/33/59/52及既有463/40观察不变。
真实integer spool独立arity与公共源宽度错误的XX000/FD0/terminal next均核验，不称私有header破坏测试。
uppercase OID前提共47失败（2个DDL错误、45个级联）及harness错误保留；仅fixture物理BIGINT+真实o::oid，预期不变，未修该DDL。
所有累计分配（非RSS/CPU）、输入顺序计数、1980延迟及慢值、Main资源/35类有限操作/强杀重开见[集合键缓存报告](performance-report-2026-10-30.md#集合整数键缓存与比较参数复制2026-11-02)；原273/725及所有暂停边界不变。

## 2026-11-02 集合排序预算与 VALUES 查询子句

最终相同 61 core 的 44 个原生入口全部通过。新 SetSort 算子 26、语法 860、VALUES 执行 63 共 949 控制全过；同最终源码旧 ABI 基线分别 20/694/63 失败，均为观察数，不是独立 bug 数。
集合 112、VALUES 143、枚举 33、显式类型计数 59 四份固定协议矩阵在严格 PG18.6/最终程序全过，旧版分别 8/127/20/51 处不符；新增类型修饰/语法阶段 52 严格观察全过。
覆盖真共同类型/绑定范围、参数三 origin 同图重启、相关与 shadow 子查询、volatile 求值次数/顺序、NULL/负数/溢出、隐藏键同行、完整递归 metadata、实际匿名文件与有限 FD、提前关闭、取消和超时。

完整 31 协议首轮为 29/2；暂缓 Bind/portal 的 463 项完整日志及其中 40 处差异与前版逐字节相同。
另一失败在 start_ours 的 sock.connect、尚未发送 SQL；同一测试源码和程序独立重跑通过。最终逐入口复核为 30/1，首轮整批结果与原失败日志保留，未改连接策略、超时或重试逻辑。

新增52永久协议入口独立验证通过；全部32个已运行入口按各自最终结果复核为31/1。
首轮 C3 原生两失败源为夹具 collation C/c 及 grammar child/span 预期；C4 的 :: AST 形状与参数槽位四项预期也仅按真实绑定修正。
原失败、源、PG 预期和同源码旧版均留存，不把它们算作生产修复。真实 TIES cancel/timeout 清理、CASE Parse 异常逃逸、缺操作数、类型修饰截断已修复。
该前批全局集合表达式计数曾静默被忽略，随后部分已解析形状明确报 0A000，当时仍列未实现；本批普通 UNION ALL 接入另见最新记录。原始 52 探针的 C4 实际只完成 49 项即 SIGABRT，剩余三项未观察；最终相同严格预期完整 52/0。

实际 Main 全值 4,107 行/12,321 单元的七项资源核验全过；首轮采样曾把 mkstemp→unlink 的瞬时命名 FD 误判，后按同 FD/路径转为 deleted 或确实关闭且路径消失验证，原观察保留、生产文件 helper 未改。
35 类操作有限检查、匿名文件打开时 SIGKILL/重开 1,000 行/4,000 单元及再次集合排序通过，不是任意写入崩溃或真实 TLS 验收。
同代构建/哈希、所有失败与性能数据见[集合排序与 VALUES 报告](performance-report-2026-10-30.md#集合排序预算与-values-查询子句2026-11-02)，证据目录 `build/pg-feature-set-sort-wi9y4qzg/`；原始总账与专属暂缓未变。

## 2026-11-02 普通类型化排序预算与比较缓存

最终 39 个原生入口全部通过；新增 codec 51、通用有界段 17、真实算子 17，共 85 控制。
覆盖完整 owning ExprValue/四类 RowContext 容器、递归 record/OID/NULL、实际低预算文件、TopN/超大行、有限 FD、
多轮归并、短 I/O/EINTR/ENOSPC/EMFILE/损坏输入、主异常清理和 volatile 需求。
真实 SourceContexts 的 JOIN 绑定测试不等于实现新的 JOIN 算法；公共 buildPreparedQueryPlan 的 CTE/derived VALUES 与实际堆另独立执行。

所选完整 26 协议入口为 25 通过/1 失败；新增固定 108 在严格 PG 18.6/最终程序全过，旧版仅 NaN WITH TIES 两处失败。
原 80 控制顺序和预期不变，追加 28 个无 ARRAY/无 LIMIT 的普通入口控制；Simple 与正常 text/binary 不限额 Execute 均保留。
旧 Bind/portal 完整 463/40 观察逐字节相同，不排除失败、不恢复 R197/R303/R304/R305；旧会话 470 等完整回归通过。

首批 38 原生有一入口失败：新普通回调使用 std::sort，使原 80 中 16 项 stable_sort 分配观察未发生；
恢复原稳定批次回调后最终 80 全过，共享模板和全部原断言字节不变。没有恢复 R185 的旧专项调查。
原 TopN 八项的 SQL/排序/NULL/行数/需求断言保留；旧四项要求保留全输入的资源契约被新的严格预算要求替代，
64 KiB 下全部八项要求输入结束活跃分配小于 1 MiB，旧版四项失败、新版全过。原源码/初次四失败与相同默认预算观测均保留。
codec 初次编译缺少实际中断声明、初版 NAME 声明/SHOW 标签/重复二进制 statement 名、
算子夹具的树观察/序列目录/Offset open 时点错误及全部候选失败也保留，不把夹具失败写成已修生产能力。

实际 Main 同一 9 项旧版 6 项资源失败、最终全过：低预算 6 项有匿名文件，高预算 3 项无文件，完整值正确、最多 2 FD 且全部释放。
35 类操作有限检查和类型化匿名文件内 SIGKILL 后重开 1,000 行/4,000 单元格通过；不是全库恢复或真实 TLS 验收。
构建、全部原始记录、1,584 个完整延迟样本及限制见[类型化排序报告](performance-report-2026-10-30.md#普通类型化排序的预算与比较缓存2026-11-02)。
证据目录为 `build/pg-feature-typed-sort-zm9x0zst/`；原始 273 项与专属暂缓保持原文。

## 2026-11-02 会话 work_mem 与实际排序预算

36 个原生入口全部通过；新增解析/状态 136、真实 Sort/Config 27 控制，共 163，覆盖会话/启动默认、事务/保存点及预算打开时快照。
现有搜索路径/时区语法、扫描/MVCC、TOAST/索引、恢复和排序错误控制重跑通过。
实际 Main 所选完整 25 协议为 24 通过/1 失败；新增完整 470 在严格 PG 18.6/最终程序全过，旧版完整 404 处不符。
保留原 391/448 所有断言与步骤，追加缓存、语法和警告/错误顺序，验证 Simple 与正常格式不限额 Execute、OID/text/binary、原始帧和 Ready。

首候选完整 448 有 8 处不符；b190 虽通过 448，新增 22 个错误边界仍有 4 处不符，随后移动警告至值校验前再完成 470。
第一版 PG 检查器的 143 个 bytes/str 字段名差异也保留原源码/日志；没有将错误前缀/帧断言改为多个可接受值。
最终唯一失败仍为暂缓 Bind/portal 的完整 463/40，与前版全部观察逐字节相同；b190 的完整 25 另有部分 COPY 帧关闭失败，
最终入口通过不改写原失败，也不恢复或关闭 R197/R303/R304/R305。

实际 Main 的 31 个会话预算/FD/重载控制全过：低预算落盘、高预算不建文件、两个连接互不改值，最终核对 4,000 单元格。
35 类操作有限 smoke、匿名文件内 SIGKILL/重开及再次排序通过；不作全库、完整例程 GUC 或真实 TLS 验收。
CMake 21 次成功编译/16 个不同 CPP、链接 62 对象；最终 Main-only 变化后核对相同 61 core，复用不链接 Main 的 36 原生，实际 Main 另完整重跑。
504 个独立 SQL 样本与 600 次 CPU 诊断均保留，未证明全面提速或无回退，见[会话工作内存报告](performance-report-2026-10-30.md#会话工作内存与实际排序预算2026-11-02)。
证据目录 `build/pg-feature-workmem-_a2m0fg2/` 的 `native-final.json`、`protocol-final.json`、470 旧/PG/各候选完整日志与 `final-provenance.json` 记录来源。

## 2026-11-02 最终归并按需输出与排序绑定恢复

最终 31 个原生入口全部通过；新增 8 个归并控制和 32 个真实堆 NULL 绑定生命周期控制，旧版分别 6/26 不符，新版全过。
现有 I/O 16 个控制也通过；输出 EIO 从旧 0 行改为精确 1 行后报 58030，原失败日志保留，未放宽为任意前缀。
错误后立即清理，重复 next 无额外 I/O；失败 open 的诊断、主异常身份、嵌套/析构和后来绑定均检查。

实际 Main 所选完整 22 协议为 21 通过/1 失败；同一排序完整 47 条在严格 PG 18.6/新版全过。
既有暂缓 Bind/portal 的完整 463 中 40 处差异与前版逐字节相同；完整 timeout 入口通过，不据此恢复或关闭 R197。
实际匿名文件内 SIGKILL、重开 1,000 行/4,000 单元格、再次排序及 35 类操作有限 smoke 通过。

本批只改 ExecutionPlan.cpp，CMake 两次成功重编该单元并链接 62 对象；原生链接 61 core，未称 fresh 全部编译。
新测试的首版旧程序未捕获预期 EIO 而中止、原输出 EIO 断言失败均保存，之后完整重跑所有控制，没有删除失败观察。
宽行原生 15 样本约快 1.78–1.86 倍；504 个 SQL 样本未证明稳定提速，960 混合诊断另存，详见[最终归并报告](performance-report-2026-10-30.md#最终归并按需输出与排序绑定恢复2026-11-02)。
原始 273 项、原子条款、专属问题/暂缓记录及共享排序中断 helper 未改；这不是整库或完整 PG 验收。
证据目录 `build/pg-feature-query-oxy0qhou/` 的 `native-final.json`、`protocol-final.json`、新旧完整日志与 `final-provenance.json` 记录实际来源。

## 2026-11-02 普通物理排序预算与外部归并

该历史批次的 61 个 core 对象通过 29 个原生入口：前批 24、三个新排序入口及 redo_crash_recovery/recovery_integrity。
新入口完整 39 控制（17 内存、16 I/O/清理、6 真堆/TOAST），冻结旧版分别 13/15/4 不符，新版均全过；不是 32 个独立缺陷。
逐输入调用边界及 EOF 的分配观察分开记录，完整消费所有结果；另验单个 256 KiB 超大行。
真实 syscall 包装只用于测试程序，验证短传输、EINTR、ENOSPC、EMFILE、EOF/EIO、截断失败、取消及原异常/清理次数；没有生产故障注入 hook。
新测试的链接注解由整批、单项及 sanitizer 构建脚本共同读取，现有构建路由检查通过；本批没有运行整库 sanitizer。

随后只改 Main 接入和函数拆分，全部 61 个 core 哈希仍相同，因此复用这些明确不链接 Main 的原生结果；
最终 O2 实际 Main 另完整跑 22 个协议入口，结果 **21 通过、1 失败**。
新增完整 **47 条**在严格 PG 18.6/最终程序全过，冻结旧版 1 条 ORDER BY 1 整数按文本排序失败。
覆盖实例配置 64 KiB、NULL 顺序/空串/文本 NULL、浮点特殊值、日期/时间、BIGINT、参数/二进制结果、投影重排、别名/序号、AND 及未扩大的其他分流。
NUMERIC >2^53 控制继续走原比较所有者，没有因接入物理 Sort 变成 double 比较。

未通过的是既有暂缓的 Bind/portal 完整 463：旧/最终 40 处差异、全部观察逐字节相同。
前一 `1865fddb` 候选的完整 22 为 20/2，另一项为 statement_timeout 的部分 COPY 帧关闭；
隔离旧/新/旧也均未通过，失败点不同，不能认定同一根因。最终该完整入口通过，不擦除前述失败、不声明 R197 修复。
专属 R197 记录中的既有 COPY 响应/中断暂缓补入[中央索引](deferred-scope.md)，没有继续相关实现；NetworkServer 和共享 CPU 排序中断 helper 保持原字节。

初始 37 条的 `pg_settings.setting::integer` 在 PG 全过、本项目一处 0A000；该目录表达式缺口保持未修复。
配置核验改成原生 text 列后保留全部其他 36 控制，并扩展为 47；最初失败和 PG 对照没有删除。
首个仅算子候选虽然通过原生/协议控制，却在真实 SQL 文件观察中失败；补 SQL 接入后才捕获实际匿名文件。
O0 Main 的快速功能构建只作预验，最终声明使用另行 O2 构建的真实 Main。

最终 Main 的实际匿名文件内 SIGKILL、无命名残留、重开 1,000 行/4,000 单元格及再次全值排序通过。
35 类操作也完成预热和一轮现有检查，只作有限 smoke；后续独立最终延迟 504 样本、前期 756 样本及 900 查询 CPU 诊断分开保存。
收益与较慢控制完整列于[报告](performance-report-2026-10-30.md#普通物理排序的内存预算与外部归并2026-11-02)；不据此通过全仓库、真实 TLS、全故障恢复或完整 PG 行为验收。
证据目录 `build/pg-feature-sort-1be1avdu/` 的 `native-final.json`、`protocol-final.json`、旧/PG/新日志、
`timeout-isolated.json`、`main-sort-crash-outlined.json`、`operations-outlined/operations.json` 和 `final-provenance.json` 保存范围与来源。

## 2026-11-02 普通 LIMIT 需求与运行错误前缀

最终 24 个原生入口通过。新增结果前缀原生完整 16 项验证 SQL 错误、hasError、open/close 错误、非 SQL 异常身份、
close 恰好一次、NULL/空串/文本 NULL/UTF-8，以及保留前缀与默认 checked 清空之间的独立契约。
原 23 项扫描、MVCC/快照、TOAST、索引、聚合、排序取消等入口全部重跑通过。

完整所选 21 个协议入口 **20 通过、1 失败**。新增 `physical_limit_demand_protocol_e2e_test` 完整 62 条在严格 PostgreSQL 18.6/候选全过，冻结旧版有 9 条失败。
覆盖 Simple/text Bind/binary Bind 与结果、LIMIT 0/超量/OFFSET、未请求行中的错误、序列及存储写函数调用次数、DISTINCT/聚合、
FETCH、负数/负零计数、单列星号/NULL，以及 OID/typmod/属性来源。另检查错误前 5 行的原始帧与二进制值，随后 22012 且没有 CommandComplete。
首份 54 条夹具误用“错误前无行”的通用 helper，实际 PG 在第 6 行失败前发送 5 行；保留该失败，改成明确校验 PG 原始行前缀及帧顺序后扩展为 62 条，没有放宽断言。
仅执行分流的中间候选仍有错误前缀和负数 LIMIT 2 条失败，后续才补生产修复。

未通过项为既有 `bind_portal_lifecycle_protocol_e2e_test`：完整 463 中旧/新各 40 处不符，全部观察逐字节一致；不算 40 个独立 bug。
专属 [R304](issue-review-r304-bind-portal-lifecycle.md)/[R305](issue-review-r305-result-format-demand.md)已记录用户暂缓，此次只保留未过滤回归，不修改 Bind/格式策略或恢复调查。
额外有限行数 portal 观察共 6 条，PG 全过，旧/新各有相同 2 条差异；没有接受该观察为修复，不以本轮不限额 Execute 的前缀通过关闭共享 portal 能力。
R303/R304/R305 既有范围补入[中央索引](deferred-scope.md)，专属暂缓记录与原始 273 项状态保留。

35 类操作完成预热及一轮现有检查；该 smoke 与相邻编译/回归重叠，不用于性能结论。
另有隔离的六类 216 个旧/新/旧延迟样本，见[性能报告](performance-report-2026-10-30.md#普通-limit-按需执行与错误前缀2026-11-02)。
本批不是全仓库、恢复/故障注入、真实 TLS 或完整行为兼容验收；默认持久化和 15 秒协议期限未放宽。
证据目录 `build/pg-feature-limit-20261102-tafn38be/` 的 `native-final.json`、`protocol-final.json` 包含实际退出码，
`portal-existing-failure-comparison.json`、`protocol-before-reference-62.json` 和 `final-provenance.json` 保留未通过范围、参考版本及当前源/对象/程序核对。

## 2026-11-02 按页物理扫描与固定快照

最终候选通过 23 个原生入口、17 个协议入口，既有 35 类操作也完成预热及一轮现有 oracle 检查。
新增物理扫描原生 6 项：冻结旧版有 1 项 open 内存失败，其余值/NULL/重开控制通过；新版完整 6 项通过。
新增跨后端快照 8 项：autocommit、READ COMMITTED、REPEATABLE READ、SERIALIZABLE 四种场景中，
游标打开后另一后端提交 UPDATE/DELETE/INSERT，原扫描仍得到 192 行旧值及旧 TOAST，随后新扫描得到新值。
统计专项验证提前停止、EOF、重复 close、析构和未打开子算子的 NULL 绑定；增加真实 allocator 关闭故障后验证 58030、页/表锁释放与重开。

新增写函数排序协议完整 13 条，在 PostgreSQL 18.6、冻结旧版和最终候选均全过；
覆盖同一语句中的后续页修改、显式事务及回滚。前批完整 48 条 Top-N、80 项原生排序取消、190 条聚合排序和 69 条参数索引均再次通过，
相邻原生还包括并行执行、MVCC、索引快照/损坏、丢失堆文件、TOAST 失败、VACUUM/快照导入导出；协议包括事务特征、完整值和 NULL 谓词。

公共头变化触发 CMake 13 个生产单元重编，另明确重编两次 ExecutionPlan 以核验资源释放及统计锁边界，当前 62 个对象链接；不是重新 fresh 编译全部 62。
PG 参考首次在监听后、就绪前收到 57P03，未执行任何专项 SQL；保留日志，并用 pg_isready 在原 15 秒启动期限内判断就绪后才完成参考检查。
初次四类 144 样本、扩展六类 216 样本及最后统计锁边界修正后的六类测量分开保存，不合并择优；实际数值、内存口径及剩余普通 LIMIT 分流见[性能报告](performance-report-2026-10-30.md#按页物理扫描2026-11-02)。
证据目录 `build/pg-feature-scan-20261102-i7t0b3bo/` 中，最终原生汇总为 `native-release.json`（前阶段及统计夹具增补原始运行另存），
协议为 `protocol-release.json`，生产输入/实际对象/当前测试源与二进制核对为 `final-provenance.json`。
这不是完整 PG 兼容、全仓库、真实 TLS、长期并发或崩溃恢复验收；分区/RLS 和其他用户暂缓范围未改，273 项原状态不变。

## 2026-11-02 有界排序与类型化执行

- 最终候选通过 11 个原生入口和 11 个真实协议入口；新增 `prepared_topn_memory_test` 完整 8 项，
  `prepared_topn_protocol_e2e_test` 完整 48 条在本项目及 PostgreSQL 18.6 均全过。冻结旧程序跑同一份 48 条有 11 条失败，原始日志保留。
- 协议覆盖 Simple、text Bind、binary 整数 Bind 和 binary 结果、多键/NULL/OFFSET、0/超量 LIMIT、DISTINCT/WITH TIES、
  排序键与投影的序列调用次数、丢弃候选中的除零错误、OID、超出 2^53 的 numeric/bigint、整数最小/最大值、NaN/Infinity 与虚拟生成列的有效值。
- 10 个相邻协议入口包括完整 190 条普通聚合排序检查、69 条参数索引检查、取消、子查询上下文、存储函数与 FETCH 边界；
  10 个其他原生入口包括完整 80 项排序中断及扩展后的 NULL 位图逐列/按行等价与嵌套绑定恢复检查。
- 既有操作驱动的 35 类全部完成一轮检查（另有预热）；该轮与原生编译有重叠，只作为有限正确性回归，不用于提速结论。
  独立旧/新/旧延迟测量含 144 个已校验样本，详见[性能记录](performance-report-2026-10-30.md#有界排序-top-n2026-11-02)。
- 额外三方探针确认既有缺口：虚拟生成列 `b AS (1/a)` 在 `a=0` 且读取 b 时，旧版和新版均返回 NULL，PG 18.6 报 22012；
  只读 a 的控制三方均成功。本批没有关闭生成列表达式错误传播，也不把上述有效值检查视为全部虚拟列兼容。

本批仅重编译有变化的生产单元并链接当前 CMake 对象，不是重新 fresh 编译全部 62 单元。
NULL 位图测试初次编译缺少 DbError 头文件、早期候选的延迟回退、浮点排序失败及 10,000 行夹具加载超时均留有原始记录。
未修改原始 273 项状态、用户暂缓项、R244 Bind 规划、默认持久化或协议期限；未执行完整仓库/TLS/并发/恢复矩阵。
证据目录为 `build/pg-feature-topn-20261102-bc582b_s/`，最终入口退出码见 `native-keycache.json`、`protocol-keycache.json`，
测试源、生产源、对象及程序哈希见 `final-provenance.json`。

## 2026-11-02 PostgreSQL 差距评估与操作对照

复核 273 项总账及功能族分布，没有改变 complete/partial/unverified/deferred 状态。
使用 `scripts/benchmark_postgresql.py` 在两个私有的本机进程执行既有 35 类操作：双方各预热 35 类，
再各完成 210 个计时样本，既有驱动的协议/结果检查均通过，范围及只读事务/序列的有限 oracle 见[性能报告](performance-report-2026-10-30.md#postgresql-186-同机对照2026-11-02)。
参考进程严格核验为 PostgreSQL 18.6；已有默认 `pgref` 实际为 17.2，没有用于本次对照或被修改。
这不是新一轮全仓库回归、完整 PostgreSQL 差分、真实 TLS、长期/并发或生产验收；[差距评估](postgresql-comparison.md)说明尚缺的能力和证据。

## 2026-10-30 数据库性能批次

2026-10-30 数据库性能批次：最终候选通过 **123 项原生入口和 45 项真实协议入口**，WAL 追加故障矩阵完整 246 项检查全 0。
35 类操作的新版本完整完成 315 个样本；前后对照采用双方均完成的前两轮各 210 个样本，旧版第三轮 CHECKPOINT 错误保留。
测量方法、完整耗时表、故障回归、早期失败、构建/源码哈希和复现命令见[性能测试报告](performance-report-2026-10-30.md)。
默认持久化设置与协议 15 秒期限未放宽；这是本轮指定范围，不是全仓库兼容性或历史 R164 根因关闭。下文历史批次继续按原范围解释。

## 2026-11-01 文档与系统一致性核查

版本一致性、兼容性契约、数据目录端到端、`version()` 协议及原生配置测试共 5 个指定入口通过。
另在私有服务执行 23 条协议观察，包括 README 原文 9 条 SQL、保存点回滚结果、配置默认值/重载/重启和模式门控，全部通过。
实际缓冲池参数来源和后台 checkpoint 间隔只经源码追踪确认；首次 CMake 配置因未安装工具退出 127，该失败记录保留。
同日已补装 CMake 4.4.4，按 README 完成配置和完整编译，新编译 62 个单元；增量构建没有重复编译。
对新产物 `build/cmake/dbms_main` 运行数据目录、`version()` 协议两个入口及上述 23 条文档观察，全部通过。
根目录原程序未替换；本次构建采用 TLS stub，没有重跑真实 TLS、完整 CTest、性能或兼容性矩阵。
修订项目、原始证据与其他覆盖限制见[文档核查记录](documentation-inventory.md#文档与系统一致性核查2026-11-01)。

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

新增[R128 TIME二进制参数](issue-review-r128-time-binary-parameter.md)158协议控制全过，实际PG158
全过、旧完整132失败；初版旧fixture112项后缺RowDescription中断保留、强化失败
收集后按同一158期望重跑。NetworkServer实际normal1/repeat0均0，最终24native+
28协议+3核对后新链接当前57库并实际执行的真实Main=55不同完整入口全0。
原122核心匹配（原始2条PG内部来源字段不同保留）、原97精确匹配；本项接受
898native+530Python+3Main=1431，工作区901+533+3=1437含原六候选，554AST/13Bash
另检。无新完整注册/TLS/整库sanitizer证明，不冒重编Main或旧heap升级。

新增[R127 直接TIME过高精度](issue-review-r127-time-excess-precision.md)252原生/276协议全过，
PG276全过、固定旧原生225/协议246失败；2/0及复查1/0增量normal/repeat均0。
最终24native+27协议+3核对后重新链接当前57库并执行的真实Main=54不同完整入口
全0，不冒重编Main。原122核心行为全匹配，原始两条PG内部诊断来源不同不伪造；
原97仍精确匹配。本项接受898native+529Python+3Main=1430，工作区901+532+3=1436
含原六候选；553AST/13Bash另检。以下轮次保留各自历史输入，无新完整注册/TLS/
整库sanitizer证明。

新增[R126 TIME关键字精度语法](issue-review-r126-time-precision-grammar.md)严格549原生/326协议，
实际PG326全过，固定旧原生217/协议296失败。首版协议只完成19项后重置的实际1
保留并修正；末版23native+26协议+3核对后重连的真实Main=52不同完整入口全0。
原122现120匹配/2条TIME(7)通知及typmod差异，原97全匹配。本项接受897native+
528Python+3Main=1428，工作区900+531+3=1434含原六候选；552AST/13Bash另检，
无新完整注册/TLS/整库sanitizer证明，Main对象复用不冒称新TU编译。

新增[R125 合法TIME显式精度](issue-review-r125-time-precision-value.md)339原生/830协议，
PG830全过、旧830协议/317原生失败；最终22native+25协议+3真实Main=50完整入口
均0。未改Main TU核对后重连当前57库并执行，不冒重新编译。原97全匹配、122
仍12边界差异；本项接受896native+527Python+3Main=1426，工作区899+530+3=1432
含原候选，无新完整注册/TLS/整库sanitizer证明。

新增[R123 时间列precision](issue-review-r123-temporal-column-precision.md)782项原生/104项协议，
实际PG104全过、旧218原生/94协议失败；首版原生全过、协议六个R124路由失败保留。
最终匹配产物的有限回归见记录；不把metadata检查当微秒heap、CAST值舍入或新全量证明。

新增[R124 TIMETZ后缀表投影](issue-review-r124-timetz-postfix-projection.md)96项协议控制，
实际PG96全过、旧24失败，原schema回归暴露的六个42703断言保留。
本项重编与匹配产物的有限终态见记录，不冒新完整注册、真实TLS或整库sanitizer证明。

新增[R121 默认TIME文本输入](issue-review-r121-time-input-normalization.md)90native/177协议，
实际PG177全过、旧130协议/65native失败。最终18native+22协议+3真实Main=43入口
均0，Main是核对未改单元后重连当前57库并实际执行，未重编TU。原32对照全匹配，
97仍21显式精度差异。本项接受894native+3Main+524Python=1421，工作区897+3+527
=1427含原候选，无新完整注册回归、真实TLS或整库sanitizer证明。

新增[R119 时间戳binary](issue-review-r119-timestamp-binary-result.md)235低层/232协议，
实际PG232全过、旧144协议/192低层失败；保留首版完整36终态1及8个上游输入失败，
R120独立修复后共享41完整入口全0，原64条观察当前全匹配。本项接受组合
893native+3Main+523Python=1419，工作区896+3+526=1425含保留候选；无新完整注册
回归、真实TLS或整库sanitizer证明，不捏造R120之后又一轮编译/41重跑。

新增[R120 日期逆换算](issue-review-r120-date-cycle-inverse.md)独立枚举3652059天
及152协议控制，实际PG152全过、旧48失败；匹配58单元/三真实Main及有限终态见记录。
本项独立提交后892native+3Main+522Python=1417，工作区896+3+526=1425含尚未提交
R119及原候选；无新完整注册、真实TLS或整库sanitizer证明，组件sanitizer不冒充整库。

新增[R117 TIME binary](issue-review-r117-time-binary-result.md)121低层/144协议
控制，实际PG144全过、旧114协议/58低层失败。最终有限匹配终态见记录；本项
提交后891native+3Main+521Python=1415，工作区894+3+524=1421含保留候选，无新
完整注册、真实TLS或整库sanitizer证明，R118真实Main结果不冒充本项重跑。

新增[R118 时态物化布局](issue-review-r118-temporal-view-materialization.md)186条
协议控制，实际PG全过、固定旧60失败、首版仍36失败均保留。最终匹配回归/真实
Main终态见记录。本项提交后的接受组合为890native+3Main+520Python=1413，
工作区893+3+523=1419含保留候选；未获新完整注册回归、真实TLS或整库sanitizer证明。

新增[R116 视图完成tag](issue-review-r116-view-command-tags.md)54条完整协议
控制，实际180006全过、旧14tag失败。有限匹配回归与真实终态见记录；接受
890native+3Main+519Python=1412，工作区893+3+522=1418含保留候选，无新完整注册、
真实TLS或整库sanitizer证明。原273统计不提升。

新增[R113 INTERVAL binary结果](issue-review-r113-interval-binary-result.md)117低层
及171协议控制，实际PG171全过、依赖修复后旧149失败/原生78失败。最终有限回归
与失败原文见记录；接受890native+3Main+518Python=1411，工作区893+3+521=1417
含未接受R98/R109候选，无新完整注册运行，不证明真实TLS或整库sanitizer。

新增[R114 逻辑长度](issue-review-r114-physical-attribute-length.md)102原生及28协议
控制，PG28全过、旧22失败；候选曾剩5个既有CHAR失败，R115另项修复。最终有限
匹配回归见记录。本项接受889native+3Main+517Python=1409，工作区892+3+521=1416，
含保留候选和未接受R113；尚无新完整注册回归，原273统计不提升。

新增[R115 标量别名投影](issue-review-r115-scalar-alias-projection-metadata.md)228条
完整协议控制：实际180006全过、旧13失败。有限匹配产物回归及逐版失败见记录；
本项接受组合888native+3Main+516Python=1407，R114另项。工作区892+3+521=1416
含未接受候选及R113，未跑新完整注册集合，不证明真实TLS或整库sanitizer。

新增[R110 数组二进制结果](issue-review-r110-array-binary-result.md)110条
协议和182条低层控制，实际180006参考110零失败、旧75失败。有限回归和逐版
失败见记录；本项提交后接受组合888native+3Main+515Python=1406，含保留候选
的工作区891+3+518=1412，均无新完整注册回归。未证明真实TLS或整库sanitizer。

新增[R112 具名数组输入](issue-review-r112-unconstrained-named-array-input.md)
57条协议控制，实际参考180006零失败、固定旧程序44失败；逐版失败、正常/repeat
与有限门禁见记录。只新增一个协议注册入口，本项提交后的注册组合为887native+3Main+
514Python=1404，无新完整注册回归；R109/R110 候选另计，不复用旧全量证明。

当前运行结果、失败明细和复现方法见[代码审查索引](review-index.md)。验证入口为 `bash scripts/build_tests.sh`，只运行其已注册集合；未注册测试需单独运行或补入入口。

后续[R109](issue-review-r109-array-positions.md)已按用户要求跳过，候选源码与
测试保留未提交。其388/248和66有限完整入口全0是候选历史证据，不计已修复
或接受manifest；旧248/160、388/290、242/154与228参考日志均保留。

后续[R108数组元素相等](issue-review-r108-array-position-typed-equality.md)新增
354原生与258协议控制，最终参考258零失败、固定旧版本158失败；旧原生320/183
和早期239/153保留，逐版失败与原生夹具校正细节见记录。第五版
32native/29协议/3真实Main共64完整入口及28补充JSONB均0，核心/最终
fence0。两个新增注册入口对应接受manifest887native+3Main+513Python=1403，
含未接受R98工作区889+3+515=1407；本项有限验收不是新完整注册回归。

后续[R107长度函数签名](issue-review-r107-string-length-signatures.md)增加607
原生及654协议控制；最终参考654零失败、旧程序392失败，旧原生607/440失败。
首两版实际失败和逐版artifact核对分别保留；实际候选与邻接验收以本项记录
为准。新增两个注册入口，本项提交后的组合886native+3真实Main+512Python=1401；
工作区含R98时为888+3+514=1405。两套新组合均无全量终态，R106草稿未注册。

后续[R104 网络值转换](issue-review-r104-network-value-conversions.md)实际PG/
Root649、原738及完整SDK、21native/17协议/3真实Main通过；五CPP实际4/3/2
增量normal/repeat及核心/最终独立核对均0，按单项本地提交。旧649/419、SDK134、
首版582/3与649/58、第二版649/1保留；无新1399/1403全量，原273不变。

2026-10-16后续[R105 限定NAME→INET/CIDR](issue-review-r105-network-name-input.md)
PG/Root738、完整SDK及21native/16协议/3真实Main全过，自身真实全58/
normal/repeat、核心与最终独立核对均0，按单项本地提交。旧738/34、SDK134
保留；无新1398/1402全量；R104后续独立修复，原273未关闭。

2026-10-16后续[R103 NAME→MAC](issue-review-r103-macaddr-name-input.md)实际参考
最终PG/Root366、完整SDK及21native/16协议/3真实Main通过，真实全58/normal/
repeat和独立终态fence全0，单项本地提交。旧366/30、SDK134及首版358/6与
366/12失败保留；75纯组件sanitizer不冒整库证明，无新1398/1402全量。

2026-10-16后续[R102 INET/CIDR输入](issue-review-r102-network-input-notation.md)
最终旧188组件72失败、672协议389失败；当前188/纯组件ASan/UBSan、PG/Root672、
2558对照及21native/16协议/3真实Main通过；最终真实全58/normal/repeat、匹配
核心及最终fence全0，单项本地提交。首版672/23保留；无新1398/1402全量，
R103/R104/R105后续各独立修复，不把本项写成所有网络类型问题均已完成。

2026-10-16后续[R101 MAC输入](issue-review-r101-macaddr-input-and-widths.md)核心75、
PG/Root328及236额外codec差分通过，纯头ASan/UBSan75通过；二十完整native和
十五协议及三个真实Main全0。头依赖修正后全58真实重编0，所有对象/程序字节
相同，新匹配核心2/328及最终fence全0；单项本地提交，不冒新1396或工作区1400
全量。旧75/38、328/186及首版314/5失败保留，不挪用旧1391全量结果。

2026-10-16后续[R100几何输入](issue-review-r100-geometric-coordinate-delimiters.md)
最终232组件、PG/Root230、十个完整native及七个完整协议通过，纯组件ASan/UBSan
通过；旧232/104、230/135、首轮准备超时与旧夹具134均保留。只证明本项范围，
不把下列1391历史全量用于本项后的1394或含未提交R98的1398组合。

2026-10-15最新[冻结5091b39c完整回归](issue-full-registered-5091b39c.md)实际退出0：
881 native、3真实Main及507 Python共1391入口各一次，1390实际通过、1 TLS环境
skip、0失败。源码/生产产物及全部58 receipt、57 test库字节独立终态核对0。
不证明真实TLS、原273全部完成或旧间歇性失败根因已修复；下方保留旧输入结果。

2026-10-13 审查输入为 `a502ffa3`。本轮完整注册回归已终态：普通native 783通过/2失败，真实Main入口3通过，Python驱动436返回成功/8失败，共1232项、10项失败；Python成功数包含一个无OpenSSL的TLS显式跳过。驱动实际exit1，源码及冻结生产二进制终态校验通过，注册集合无遗漏/重复。完整失败清单、实际SQL差分、正常API错值和修复建议见[审查报告](code-review-2026-10-13.md)。前次同源码专项60/60 native、45/46 wire中的ALTER ARRAY超时仍保留，不能用本次单次通过消除；下方历史全绿记录不代表当前HEAD。

## 历史记录：2026-08-14

以下仅保存当时 137 个 C++ 测试和 2 个 E2E 的 PASS=139、FAIL=0 记录，不代表当前 HEAD、当前测试总数或所有功能已验证。
当前功能状态见[原总账](postgresql-18-gap-audit.md)，原始旧依据为[命令手册](commandsList.md)和[历史 TODO](all-gaps-todo.md)。

本轮完成性能与并发硬化轮次的全面复验（13 个提交后）：

- **全量回归**：`./scripts/build_tests.sh` 完整重跑，137 个 C++ 测试 + 协议 E2E + 窗口函数 E2E（13/13）全部通过，0 失败。
- **并发与事务专项独立复跑**：`concurrency_test`（含 INSERT/UPDATE/DELETE 回滚恢复二级/复合/Hash/TOAST、checkpoint 持久化、MVCC 快照隔离、性能子项）、`wal_truncate_test`、`redo_crash_recovery_test`、`prepared_transaction_test` 在独立临时目录中单独复跑，全部通过。
- **线程安全压测（新增，本轮引入）**：针对 `BufferPool` 新的两阶段加载/孤儿帧失效语义，构造多线程压力（8 读者 + 改写者 + 失效器 + 4 帧小池强制驱逐/重载），分别在 TSAN 与 ASAN+UBSan 下运行：**0 数据竞争报告、0 内存错误、0 内容损坏**。修复前同一压测报告约 1 万次内容损坏（`invalidatePage` 在并发读者下重置被 pin 帧导致帧复用污染）。
- **确定性失效语义回归（新增）**：pin → invalidate → unpin → 重取必须返回新鲜正确数据；invalidateAll 后重取同样正确；同线程持 pin 期间重取同页返回不可用而非死锁。
- **性能基准复测**（与优化前同机对比）：事务内带 PK 插入 200 行 86–114ms（1760–2370 行/秒，优化前 11.7 行/秒），commit 12–15ms（优化前 267ms）；并发套件 500 行插入 + COUNT/SUM/MAX/GROUP BY 聚合 1.4ms、插入 265–305ms（优化前约 106s）；索引等值查找 14 次查找 1–2ms。
- **回归发现的缺陷均在当步修复**：removeMulti 二分改造后等值键 RID 扫描、schema 缓存引入期捕获的 RLS/fillfactor/catalog 快照隔离、hashidx 失效误指 secidx 等，均由现有测试套件（gin_brin_index_test、policy_test、fillfactor_test、catalog_snapshot_test 等）拦截后修复，最终保持全绿。

本轮新增 Volcano 执行失败契约回归：合成 `open()` 失败必须由
`executePlanChecked()` 返回 `ok=false` 和错误信息，不能与合法空结果混淆；所有测试调用
方均使用 checked contract，旧空结果兼容入口已删除。

本轮新增运行时统计持久化回归：数据库/表统计可从当前格式快照重载，损坏 magic/版本或
尾随数据会 fail-closed；关系删除标记不会在下一次持久化时恢复旧表统计。

本轮新增协议配置边界回归：不同 backend 的 `statement_timeout` 会话值互不污染，非管理员
不能执行 `SET GLOBAL`，进程级参数不能通过普通 `SET` 修改；planner 参数变化后旧 `EXPLAIN`
缓存不会被复用。

本轮新增配置文件回归：非法键和值、非有限浮点、越界整数和损坏配置加载均拒绝；候选配置
失败不会污染当前对象，成功保存使用临时文件、`fsync` 和原子替换。

本轮新增 typed DDL 类型边界回归：未知列类型和非法类型修饰符在物理变更前拒绝，不再
静默降级为 `varchar`；合法 `SERIAL`、`NCHAR`、`BINARY` 映射及自动序列仍保持可用。

本轮新增 DDL 路由收敛回归：`CREATE/DROP DOMAIN`、`CREATE/DROP SEQUENCE`、
`CREATE/DROP SCHEMA`、角色/用户和数据库均通过 typed bridge 执行，确认标准入口不再依赖
`main.cpp` 旧字符串分支。
同时验证 `ALTER SEQUENCE ... RESTART ... INCREMENT BY ...` 和 `RENAME TO` 由 typed bridge
执行；重命名保持 catalog OID、更新 `nextval` 默认表达式，重启后新名称可用且冲突不会破坏原对象。
存储完整性回归还验证了损坏 heap page、损坏/截断边界被拒绝，以及 page compaction 不覆盖 special space。

本轮新增序列持久化回归：`sequence_full_test` 验证失败的 `ALTER SEQUENCE` 不会遗留启动恢复快照，
重启后 `nextval` 保持已提交状态；序列文件拒绝尾随字段、非法 cycle/范围/cache，写入使用原子发布，
`INT64` 正负边界、`currval`、`setval` 和非循环耗尽行为均通过。

本轮新增 DDL 会话生命周期回归：typed `DdlExecutor` 执行结束后恢复原线程局部会话指针，避免局部
`Session` 销毁后后续嵌入式存储调用解引用悬空指针；完整序列专项测试通过。

本轮新增数据库生命周期持久化回归：新建数据库的 `tlist.lst`、字符集和 public schema 标记必须完整，
checkpoint 文件必须是精确的当前三字段格式且拒绝尾随数据；完整回归继续验证物理备份标记和恢复路径。

本轮新增数据库 DDL 错误传播回归：构造数据库目录创建失败时，`CREATE DATABASE` 返回错误并输出
SQLSTATE，不会伪报成功；`DROP DATABASE` 也只接受 `StorageEngine::OK` 为成功结果。

本轮新增 DDL 路径与重复对象回归：重复 `CREATE SCHEMA` 必须失败，`IF NOT EXISTS` 才跳过；损坏
domain sidecar、非法数据库路径、序列/视图/关系名称路径组件均 fail-closed，schema marker 和
domain 创建使用原子发布。

本轮新增辅助对象 sidecar 回归：view、function、TVF、procedure 的元数据父路径被普通文件占用时
返回 `IO_ERROR`，正常定义使用原子发布，重复定义返回 already-exists 且不会覆盖旧内容。

本轮补强 `DROP TABLE ... CASCADE` 回归：构造 catalog 依赖表及其索引，验证 CASCADE 同时清理
依赖表的物理关系和索引 sidecar，再删除依赖 catalog 行；RESTRICT 预检仍不产生任何修改。

本轮新增索引 sidecar 故障回归：`.secidx` 或 `.idxnames` 被目录占用时，`CREATE INDEX` 返回错误，
不会留下物理索引或名称映射；索引元数据改写采用原子发布，复合索引删除保留剩余元数据选项。

本轮修复 `planner_runtime_stats_test` 的后台 writer 清理竞态：测试 teardown 通过 `dropDatabase()` 释放
数据库缓存与 WAL 所有权后再清理目录；专项测试和完整回归均通过，最终基线保持 `PASS=139 FAIL=0`。

---

## 测试环境

- OS: Linux 6.8.0-117-generic
- Compiler: g++ -std=c++17 -O2 -pthread
- 标准编译命令：`./scripts/build.sh`；规范测试入口：`./scripts/build_tests.sh`；`./scripts/run_all_tests_fast.sh` 是只改变输出形式的安静外壳
- `build_tests.sh` 会在测试前检查 `dbms_main` 的源码、头文件、manifest 和构建配置指纹；二进制缺失或过期时自动原子重建，因此完整回归不依赖调用者预先执行 `build.sh`。
- 生产二进制构建失败时，`build_tests.sh` 会立即 fail-closed，拒绝运行任何可能过期的 E2E 二进制。
- 每个 C++ 测试在独立临时工作目录中运行，回收目录后才进入下一项；顺序回归不再共享 `.txnid`、WAL、catalog 或后台 worker 的相对路径状态。
- 本轮构建质量检查：主程序在 `-Wall -Wextra` 下无编译警告；修复 join cost 参数错误及 parser/测试中的未使用代码。
- schema 格式回归：当前格式 `0x44420009`，长表名、列名和约束名可完整写入/读取；旧格式仍按 fail-closed 策略拒绝。
- 窗口函数端到端测试：由统一回归入口执行 `tests/window_e2e_test.py`（13 用例，包含 Volcano `WindowAgg` 排名/偏移、窗口聚合、`ROWS/RANGE/GROUPS` frame/exclusion、OFFSET 和命名窗口路径，使用隔离临时目录和临时管理员账号）
- Volcano 窗口单元测试：`tests/volcano_select_phase51_test.cpp` 覆盖多窗口独立排序、排名并列、分区边界、`lag`、默认 frame、`EXCLUDE CURRENT ROW`、count/first/last/ntile/percent_rank/cume_dist 及文本/JSON EXPLAIN。
- Volcano 分组单元测试：同一测试覆盖 `GroupAggregateOp` 的常见聚合、`HAVING`、`ROLLUP`/多个 grouping set、省略分组列的 `NULL` 输出及文本/JSON EXPLAIN；主 SQL 手工回归同时验证普通 GROUP BY 与 ROLLUP。
- Semi/Anti/Existence/Quantified/Scalar 回归：`tests/volcano_select_phase51_test.cpp` 验证未关联 `IN`/`NOT IN`、inner 过滤、重复键、NULL 语义、`QuantifiedSubqueryFilterOp` 的 `ANY`/`ALL` 空集与 NULL 语义、`ExistenceFilterOp` 的 `EXISTS`/`NOT EXISTS` 及 `ScalarSubqueryProjectOp` 的 0/1 行与 cardinality error；协议 E2E 验证真实 PostgreSQL 查询路径及 NULL 结果。
- 普通聚合结构化回归：`tests/volcano_select_phase51_test.cpp` 验证带过滤条件的隐式空 grouping set；协议 E2E 验证主 SQL `COUNT(*)`/`SUM()` 已走统一聚合计划路径。
- TOAST 回归：`tests/toast_test.cpp` 验证大值插入/更新/删除、zlib 压缩后的物理体积和解压读取。
- 事务回滚回归：`tests/concurrency_test.cpp` 验证 INSERT、UPDATE、DELETE 的普通 `ROLLBACK` 与 `ROLLBACK TO SAVEPOINT` 一致恢复主键、单列二级、复合、Hash 索引和 TOAST；DELETE savepoint 回滚后在同一事务提交仍可读，UPDATE/DELETE 场景在新 `StorageEngine` 重启后再次校验堆、Hash 与大字段内容。
- 锁管理器并发回归：`tests/lock_manager_concurrency_test.cpp` 使用真实双线程验证等待图死锁只释放一个受害者、表锁重入与共享锁升级、row/page token 归属、等待期间资源生命周期和批量清理；`tests/cross_backend_lock_test.cpp` 验证独立 `StorageEngine` backend 共享同一锁注册表、隔离不同数据库的同名表、backend-local 超时策略，以及独立 fork 进程对同一数据库的表/row/page 锁阻塞、释放后获取和 gap 锁阻塞；重复运行 10 次通过。
- 保存点锁资源回归：`tests/lock_manager_concurrency_test.cpp` 验证保存点后新增的表/row/page/gap token 会在检查点回滚时释放，并验证保存点前共享表锁在后续升级后恢复为共享模式。
- 复制管理器并发回归：`tests/replication_concurrency_test.cpp` 验证复制槽激活/停用、值快照读取、standby/conninfo/sync 状态并发写入及 slot 生命周期；复制槽定义校验和 active slot 禁止删除也有覆盖。
- 锁失败传播回归：`tests/lock_failure_propagation_test.cpp` 持有真实表级排他锁并在另一线程执行建索引，验证调用方收到 `LOCK_CONFLICT`、等待图清理且不会继续发布索引。
- DDL 辅助对象回滚回归：`ddl_transaction_skeleton_test` 验证失败时撤销 view、materialized view、UDF/TVF、procedure、trigger、RLS policy 和 collation；collation 最后一个条目删除后物理文件也会清理。
- DDL 外层事务回归：`ddl_transaction_skeleton_test` 验证多个 CREATE 跨语句由外层 `ROLLBACK` 逆序撤销，并验证 `ROLLBACK TO SAVEPOINT` 只撤销保存点后的 CREATE；含该内存 undo 队列的事务会拒绝 `PREPARE TRANSACTION`。
- 2PC 跨 backend/跨进程重启回归：`prepared_transaction_test` 由独立子进程创建 prepared 事务，验证 PREPARE 前刷出 heap/index 缓存、prepared 文件 durable 原子发布、表/row/page/gap 锁在另一进程完成前持续阻塞、重启后锁 ownership 重建、`COMMIT PREPARED` 后数据可见、`ROLLBACK PREPARED` 后数据不可见、顺序/索引条件查询均不可见，以及事务内拒绝完成 prepared transaction。
- DDL DROP/REPLACE 回归：`ddl_transaction_skeleton_test` 验证 DROP TABLE 与 CREATE OR REPLACE VIEW 的变更前快照恢复，外层回滚先恢复对象再撤销同一事务中的新增行；快照已被 DDL 修改后另一条快照型 DDL 及 SAVEPOINT 会 fail-closed。
- CMake 与脚本构建共同读取 `cmake/dbms_sources.txt`，并在配置阶段校验源文件存在性、重复项和 `src/main.cpp` 入口，避免生产源文件列表漂移。
- CMake 现在提供 `check`/CTest 标准入口；它们调用唯一的 `scripts/build_tests.sh` 完整测试编排器，不再复制 standalone test 的链接、stub 选择或 E2E 调度逻辑。
- `snapshot_export_import_test` 现在覆盖 v2 快照数据库绑定、版本/长度/尾随字节/XID 校验、重复导入、跨数据库导入，以及首次读写后拒绝替换快照；`DELETE`/`TRUNCATE` 也会阻止事务中途导入快照。
- `scripts/build.sh`、`build_tests.sh`、`build_one_test.sh` 和 `run_all_tests_fast.sh` 共同读取 `scripts/build_common.sh`；本轮验证了配置指纹失效与增量单测入口。
- `./scripts/build_tests.sh` 是唯一的完整测试编排实现，会先通过 `build_common.sh` 的共享逻辑检查并构建 `dbms_main`，再缓存生产对象、逐个测试独立链接运行，并自动执行两个 E2E；因此不依赖调用者预先运行 `build.sh`。`run_all_tests_fast.sh` 仅捕获其成功输出并显示 PASS 计数，失败时原样打印完整编译/链接/运行日志。
- 文档一致性检查确认根目录旧版 `MANUAL.md` 已移除，`docs/MANUAL.md` 是唯一受 README 与测试/部署文档引用的完整手册。
- PostgreSQL 协议 E2E 覆盖普通顶层多行 DML 和写 CTE 的 statement atomicity：批量 INSERT 或 CTE INSERT 后续行冲突时，前序行不会残留；成功的 `WITH ... RETURNING` 结果也能通过协议返回。
- PostgreSQL 协议 E2E 覆盖 typed `CREATE TEMP/TEMPORARY TABLE`：临时表在所属会话内持续可用、遮蔽同名永久表，不同 backend 可创建同名临时表，其他会话不可见，断开连接后对象被清理。
- PostgreSQL 协议 E2E 额外覆盖 `ON COMMIT DELETE ROWS` 与 `ON COMMIT DROP`：提交后分别保留空表和删除临时表；parser 单测覆盖 `PRESERVE/DELETE/DROP` AST 状态。
- 新增临时表重启生命周期回归：`tests/temp_restart_cleanup_test.cpp` 模拟临时 relation 已落盘后重启，确认 `tlist.lst`、schema/data/index/TOAST 等残留全部清理；完整测试入口通过。
- 同一回归还覆盖 `CREATE TABLE` 在损坏 heap 初始化失败时的清理：schema、heap 和 `tlist.lst` 均不残留；唯一/索引初始化失败路径同时释放元数据锁。
- `tests/ddl_transaction_skeleton_test.cpp` 新增后处理失败回归：强制约束 metadata 写入失败，验证 DDL executor 的物理表创建会通过事务记录完整回滚。
- 同一测试覆盖 CatalogManager 只读 drop plan 与延后应用边界；`DROP TABLE` 的物理 relation 仍独立于 catalog plan，executor 只在物理删除成功后应用计划。
- 同一测试新增 `DROP SCHEMA` 的 namespace drop plan 延后应用回归，确认 catalog 计划与物理 schema 删除相互独立。
- 新增数据库生命周期回归：`DROP DATABASE` 释放数据库级缓存后，同名重建不会继承旧 CLOG/WAL/page/index 状态。
- 新增 typed `ALTER TABLE` 路由回归：验证 AST bridge 的 ADD COLUMN、DEFAULT、NOT NULL、RENAME COLUMN/TABLE，以及 `RENAME TO` 不再误判为列重命名。
- 新增 ALTER statement atomicity 回归：`ddl_transaction_skeleton_test` 验证多子命令中前一动作已落盘、后一动作失败时，整句 schema 会从 DDL 快照恢复，且事务快照不会残留。
- 新增外置 tablespace 事务快照回归：表移动到自定义 tablespace 后，在事务内移回默认目录并回滚，快照恢复 schema 与外置 heap 数据；同一场景覆盖全新引擎读取。
- 新增 DDL 提交失败回归：延迟 CHECK 使 `StorageEngine::commitTransaction()` 失败时，`DdlTransaction` 返回失败、清理事务状态，并恢复已执行的物理 `ALTER TABLE` 快照，调用方不会输出成功结果。
- DDL 隐式提交错误传播回归：`deferrable_test` 在延迟 CHECK 失败的显式事务中执行 `CREATE DATABASE`，确认隐式提交失败后 DDL 不继续执行、事务清理且目标数据库不存在。
- DDL 隐式提交入口审计：规范执行器和 legacy DDL 分发共用失败即停止的提交守卫，避免兼容路径吞掉 `commitTransaction()` 错误。
- 新增事务资源生命周期回归：普通 COMMIT/ROLLBACK 不会遗留整库快照；DDL 快照在 COMMIT/ROLLBACK 后清理；前一事务的 deferred CHECK 导致隐式提交失败时，后续 `BEGIN` 返回错误且不会开启新事务。
- 新增 DDL 快照并发回归：文件级 DDL 快照使用事务 ID 独立命名，并持有数据库级排他锁；另一 backend 在快照事务结束前阻塞，避免物理恢复覆盖并发提交。
- 新增 DDL 快照重启回归：模拟未完成的 `.txn_backup.<xid>`，新 `StorageEngine` 按无 COMMIT 证据恢复 DDL 前的 schema 并清理快照目录。
- WAL 提交回归继续覆盖 COMMIT 记录与刷盘路径；`WALManager::XLogFlush()` 现在返回 segment 同步结果，提交在 WAL 不可用/刷盘失败时不会先发布 CLOG 可见性。
- CLOG 持久化回归继续验证段重载；段写入现在完整写入临时文件、`fsync` 后原子替换并同步 `pg_xact` 目录，写失败不会提前清除 dirty 状态。
- CLOG 提交故障回归：删除事务的 `pg_xact` 目录后，提交返回 `IO_ERROR`，行级修改被撤销并追加 ABORT WAL；新 `StorageEngine` 重启恢复也不会重新出现该行。
- CLOG 跨 backend 回归验证两个独立 `CommitLog` 实例对同一段的按位合并，以及已打开读缓存发现外部段替换后刷新，不会丢失或永久读取旧事务状态。
- CLOG 截断回归验证旧段正常回收，并在文件锁导致持久化失败时保留最后一个 durable segment image。
- WAL 截断回归验证未归档段不会删除、已归档且早于保留 LSN 的段会回收、最早保留段可被恢复扫描发现，并且后续 checkpoint 不会为已删除段重建 archive marker。
- WAL 归档故障回归验证归档目录不可用时源段和 `.ready` 均保留，修复目录后可重试完成原子归档与 `.done` 发布。
- Catalog 持久化回归验证临时文件替换/目录同步失败会返回错误，并保留原有 catalog 目标文件，不会把部分写入报告为成功。
- WAL 崩溃恢复回归覆盖首个合法 LSN 0、未初始化页 LSN、已提交插入/删除重做和未提交事务回滚；`redo_crash_recovery_test` 同时校验 heap 与主键 B+Tree，并覆盖同一事务连续插入两行后 before-image 逆序 undo 不会留下中间状态；多实例 WAL writer 的尾部刷新/锁也由 catalog snapshot 场景覆盖。
- 新增 `recovery_integrity_test`：构造 CRC 正确但 payload 损坏或路径越界的索引 WAL 记录，验证恢复拒绝非法镜像、不会写出数据库目录外文件，并以异常中止启动；heap/index 应用失败均不再静默忽略。
- `recovery_integrity_test` 同时覆盖损坏 WAL 长度/`xl_prev` 链、非法 heap page image 和非连续 page ID 输入；WAL 截断回归使用完整有效记录构造 segment，并验证记录所在完整 segment 的刷盘边界。
- Checkpoint 回归现在断言 checkpoint 真实成功；页刷盘、checkpoint WAL、checkpoint 文件 fsync 和 archive status 任一失败都会返回失败，不再伪报 `CHECKPOINT completed`。
- Checkpoint 活动事务回归验证：活动事务期间不会推进数据库恢复起点；事务结束后 checkpoint 才刷已加载 heap/index 缓存并成功持久化 checkpoint WAL/LSN。
- SSI 回归：`phase5_remaining_test` 验证非空索引谓词按 heap page 登记 SIREAD，单列主键/二级 B+Tree 比较谓词登记逻辑索引范围并与写键进行重叠检测，非相交页事务可同时提交；跨页读写危险结构仍有一个事务返回 `SERIALIZATION_FAILURE`，空谓词关系级保护继续生效。复合/表达式/部分索引、其他访问方法的物理 predicate lock 和完整 SSI 规则仍未完成。
- BufferPool 回归覆盖一帧缓存的脏页淘汰/重载与 pinned 页保护，确认缓存不足时返回失败而不是强制淘汰活动页。
- GIN/BRIN/B+Tree 回归继续通过，并覆盖 root split/search/range、长键固定宽度规范化、重开索引后的查找和精确删除；B+Tree 的 header/node I/O 失败现在 fail-closed，避免底层页读取失败升级为空指针崩溃。
- B+Tree 多值索引回归补充 `(key, RID)` 精确删除：删除 6000 条跨叶重复键中的单个 RID 后，其余相邻 RID 仍可检索，重复删除会正确失败；生产 DML/事务回滚路径直接使用 `StorageEngine` 的 B+Tree 实现。

- 索引架构清理回归：删除未接入生产执行链的 `IIndexAM`、B+Tree/Hash 适配器及独立 GIN/BRIN 类；保留并增强测试以验证 `StorageEngine` canonical GIN/BRIN 创建、查询和损坏 sidecar fail-closed。
- CREATE INDEX 事务回归：B-tree、Hash、GIN、BRIN 在显式外层事务回滚后均清理物理文件、名称映射和 catalog；重复名称拒绝，`IF NOT EXISTS` 安全跳过。
- 事务索引 WAL 回归通过：B+Tree/Hash `flush()` 和事务边界统一缓存刷盘路径已接入；脏索引刷盘会记录完整文件 before/after image，提交前 WAL 先刷盘；`wal_basic_test` 检查索引 WAL 记录，`redo_crash_recovery_test` 验证未提交插入回滚、已提交插入恢复和已提交删除恢复。
- Hash/GIN/BRIN 持久化回归覆盖 `HashIndex` 二进制格式重载、StorageEngine GIN/BRIN sidecar 原子发布和损坏文件拒绝；Hash dirty 状态在关闭失败时保留。
- StorageEngine GIN/BRIN 回归通过真实 DDL 构建后查询索引内容，验证 `forEachRow()` 页面失败契约、GIN 原子发布和 BRIN 二进制范围文件路径。
- 索引/执行器错误传播回归通过：B-tree/复合/全文/GiST/SP-GiST/Hash/GIN/BRIN 构建与 `REINDEX` 不再吞掉 heap 扫描失败，Volcano 顺序扫描和索引扫描遇到页面读取失败时 fail-closed；`volcano_select_phase51_test` 通过。
- 执行器架构清理回归：删除未使用的 `IExpr`/`PlanNode`/`IExecutionPlanner` 接口后，Volcano 算子仍统一实现 `IOperator`；`IndexScanOp` 改用带 page shared lock 和 MVCC 检查的直接 RID 回表，`volcano_select_phase51_test` 的索引、bitmap 和组合算子路径全部通过。
- RLS 访问路径回归：带 `owner` 索引的启用 RLS 表，Volcano 计划强制选择策略感知 `TableScanOp`，只返回当前用户可见行；索引绕过测试通过。
- 访问方法安全回归：移除不具备 MVCC/NULL 位图证明的伪 `IndexOnlyScanOp`；Bitmap AND/OR 改用受保护 RID 回表并保持原谓词 recheck，Volcano 专项和全量回归通过。
- 统计/查询/DML 错误传播回归通过：`ANALYZE` 失败不发布半成品统计文件，过滤器、聚合、JOIN、FK/EXCLUDE 检查、ON CONFLICT 目标扫描、表重写和 TOAST/page 写入路径不再把页面 I/O 失败当成空结果或成功；Volcano 并行 page-range scan 会向算子返回失败。
- 新增 schema 格式完整性回归：截断 schema 与错误 magic 均 fail-closed，不会返回可写的部分 schema。
- Docker 镜像构建：`docker build -t dbms-cpp:verification .` ✅（OpenSSL 真实 TLS）；Compose 配置检查 ✅。
- CMake：当前验证环境未安装 `cmake`，配置/编译未执行。
- 网络启动安全边界：无 TLS 证书/私钥时默认拒绝启动；明文仅由显式 `--insecure` 开关启用。
- 网络服务生命周期回归：`tests/network_server_lifecycle_test.cpp` 验证端口占用时 `startServer()` 返回失败、显式 shutdown 停止监听并等待客户端 worker，活动连接槽位最终归零。
- 网络连接容量回归：accept 路径原子预留 `maxConnections` 槽位，认证失败和 TLS 握手失败均释放槽位。
- 网络输出并发回归：`tests/network_security_test.cpp` 并发创建 4 个线程局部输出捕获，每个线程写入 32 行并验证没有其他会话内容串入。
- 角色 DDL 回归：`CREATE/ALTER/DROP ROLE`、`CREATE/ALTER/DROP USER` 统一走 Catalog/AST 路径，混合大小写密码通过真实验证。
- PostgreSQL 协议 E2E：`python3 tests/postgres_protocol_test.py` 验证 SSLRequest/plaintext 协商、StartupMessage、真实 SCRAM-SHA-256 握手、simple query 和 Parse/Bind/Execute/Sync，并覆盖 NUMERIC 精确存储、1700 binary 参数/结果与 PostgreSQL INSERT command tag。
- 协议扩展查询回归：验证 `ParameterDescription`、文本和 binary `int4` 参数 `$1`、binary `int4` 结果、statement `Describe`、portal/statement `Close` 的真实 Parse/Bind/Execute 生命周期，以及 OID/长度/属性号/表 OID RowDescription 元数据。
- 协议日期时间/UUID binary 回归：验证 `date`、`time`、秒精度 `timestamp`/`timestamptz` 和 `uuid` 的 binary 参数、binary 结果、OID/格式元数据及单列空格值不被拆分。
- 协议 portal 分页回归：验证 `Execute maxRows` 分批返回、未耗尽时发送 `PortalSuspended`、耗尽时发送 `CommandComplete`，以及耗尽 portal 再执行不重复返回数据。
- 协议 ACL 回归：alice 将 `SELECT` 授予 `analyst`，bob 通过角色继承读取表数据；bob 未获得 `INSERT` 时真实协议返回权限错误。
- 角色继承边界回归：`NOINHERIT` 用户保留原始成员关系但不获得父角色 ACL/RLS 权限，切换为 `INHERIT` 后两条权限路径同时生效；`pg_hba.conf` 角色匹配仍使用原始成员关系。
- 对象授权回归：非所有者即使带有管理员标志也不能执行 `ALTER TABLE ... OWNER TO`；所有者仅可转移给能够 `SET ROLE` 到的目标角色；`SET ROLE` 后超级用户绕过和表 ACL/RLS 有效角色均正确收敛。
- owner ACL 回归：无显式 `.permissions` 记录时，表 owner 仍拥有表级/列级权限、`GRANT OPTION` 和 `all` 权限视图。
- 表级 GRANT OPTION 回归：验证表级 grant marker 仍提供基础列权限，独立撤销只移除 grant option，存在下游授权时 RESTRICT 拒绝、CASCADE 递归撤销并清理 grant chain。
- 角色 ADMIN OPTION 回归：验证角色关系的 ADMIN OPTION 授予、重复授权升级、只撤销 ADMIN OPTION，以及撤销后成员关系仍保留。
- 默认权限回归：验证 typed `ALTER DEFAULT PRIVILEGES` 的表级多权限 GRANT、幂等写入、REVOKE 只影响后续新表，以及不支持对象类型/grant option 时 fail-closed。
- TRUNCATE 回归：验证 typed 多表 AST、`RESTRICT` 在 FK 依赖下不产生部分修改、递归 `CASCADE`、多表截断、`ONLY` 字段及 `RESTART IDENTITY`。
- ALTER TABLE typed 回归：`tests/alter_table_only_test.cpp` 验证 RLS enable/disable/force/no-force、ATTACH/DETACH PARTITION 的 AST 规格传递，以及 trigger enable/disable 的持久化状态。
- EXCLUDE typed 回归：`tests/exclude_test.cpp` 通过 DDL bridge 验证 `ALTER TABLE ... ADD/DROP CONSTRAINT ... EXCLUDE` 的约束名保留、冲突拒绝、删除后放行和持久化清理；CREATE TABLE 的等值与范围排斥回归继续覆盖 INSERT/UPDATE。
- ALTER TABLE typed 清理回归：`tests/ddl_bridge_routing_test.cpp` 验证基础动作在删除 `main.cpp` 重复分支后仍由 typed bridge 执行；`tests/ddl_ast_bridge_test.cpp` 进一步验证 CLUSTER、REPLICA IDENTITY、VALIDATE/ALTER CONSTRAINT 的 catalog、索引归属和持久化结果。
- 协议错误恢复回归：验证扩展查询错误后的 Bind/Execute 被忽略至 Sync、Sync 后连接可继续查询，以及事务外错误返回 `ReadyForQuery('I')`。
- 协议失败事务回归：验证显式事务中的语句错误返回 `ReadyForQuery('E')`/`25P02`、普通命令被拒绝、失败状态下 `COMMIT` 不提交先前 INSERT 而执行完整回滚，以及 `ROLLBACK TO SAVEPOINT` 可恢复外层事务。
- 协议两阶段命令边界回归：验证失败事务中的 `COMMIT PREPARED`/`ROLLBACK PREPARED` 不会被改写为本地恢复命令，未知 prepared transaction 返回错误。
- 协议 backend 隔离回归：双连接验证事务 ID/快照不串线，未提交行不可见，ROLLBACK 后状态清理，连接断开回滚未完成事务，COMMIT 后新事务可见；另覆盖 `START TRANSACTION` 选项与 `SAVEPOINT`/`ROLLBACK TO SAVEPOINT` 路由。
- SQL 统计模块回归：`tests/sql_stats_test.cpp` 验证字符串/数字常量归一化、引号标识符区分、调用次数与耗时聚合、数据库过滤、`pg_stat_statements.max` 确定性淘汰、当前格式快照重载、尾随数据损坏拒绝及 reset。
- 运行时统计模块回归：`tests/runtime_stats_test.cpp` 验证并发 SQL 计数、失败/提交/回滚计数、表扫描和实际 DML 行数统计及 reset。
- 协议运行时统计回归：`postgres_protocol_test.py` 通过真实 PostgreSQL wire 查询验证 `pg_stat_database`/`pg_stat_tables` 返回数据库查询、表 DML 及 Volcano 索引扫描/取行计数，而非固定零值。
- Planner 统计反馈回归：`tests/planner_runtime_stats_test.cpp` 验证完整扫描 live-row 估计参与 Hash/Nested Loop Join 选型和 EXPLAIN，未取得精确证据时回退物理行数，重建同名表后旧估计不会泄漏。
- DML AST 回归：`parser_phase1_test.cpp`、`dml_semantics_test.cpp` 与 `dml_returning_test.cpp` 验证 `INSERT` 多行 AST、混合 `DEFAULT` 位置、`DEFAULT VALUES` 区分、简单单表 INSERT SELECT（含 WHERE/列表达式）、无 target 或显式匹配主键/唯一约束 target 的 `ON CONFLICT DO NOTHING`、单列及复合主键/唯一约束冲突的常量或只引用 `excluded` 的 evaluator 标量 `DO UPDATE`、目标行/`excluded` 受限 `WHERE`、复合 UNIQUE 中 NULL 不互相冲突、不同唯一约束冲突不会被错误忽略、以当前目标行列值为输入的受限标量表达式 UPDATE、单源表 `UPDATE ... FROM`、单源表 `DELETE ... USING`、INNER/CROSS JOIN 的 UPDATE FROM/DELETE USING、窄版 MERGE、INSERT/UPDATE/DELETE 的受限标量表达式 RETURNING 以及 DML 尾随垃圾 fail-closed；协议 E2E 验证普通单表 INSERT 的多行/表达式/DEFAULT、显式 NULL 不被默认值覆盖、字符串大小写保留、常量及行级表达式 UPDATE、简单谓词 DELETE、INSERT/UPDATE/DELETE 的列投影和受限标量表达式 `RETURNING`、INSERT SELECT/冲突处理返回结果和 `INSERT 0 N` command tag。

---

## 测试汇总

| 功能域 | 测试项数 | 通过 | 失败 | 备注 |
|--------|----------|------|------|------|
| 认证与连接 | 2 | 2 | 0 | — |
| DDL - 数据库 | 3 | 3 | 0 | — |
| DDL - 表 | 14 | 14 | 0 | 含 CTAS / RENAME / POINT / INET/CIDR 类型 |
| DDL - 索引 | 7 | 7 | 0 | B+Tree/Hash/FullText/GIN/GiST/BRIN/SP-GiST 基础路径通过；专用访问方法仍是简化实现，不宣称 PostgreSQL 完整 opclass 兼容；B+Tree 分裂/重复键/范围回归见 `gin_brin_index_test.cpp` |
| DDL - 视图 | 5 | 5 | 0 | 含 ALTER VIEW RENAME TO / SET SCHEMA |
| DDL - 触发器 | 2 | 2 | 0 | — |
| DDL - 用户/角色 | 4 | 4 | 0 | — |
| DML - INSERT | 5 | 5 | 0 | 含 AST bridge 的多行/DEFAULT/显式 NULL 回归 |
| DML - UPDATE/DELETE | 2 | 2 | 0 | — |
| DQL - SELECT | 5 | 5 | 0 | — |
| DQL - JOIN | 3 | 3 | 0 | — |
| DQL - UNION | 1 | 1 | 0 | — |
| TCL - 事务 | 3 | 3 | 0 | — |
| DCL - 权限 | 4 | 4 | 0 | 含 SHOW USERS/SHOW ROLES |
| 工具命令 | 8 | 8 | 0 | — |
| 分区管理 | 3 | 3 | 0 | Range/List/Hash + ATTACH/DETACH |
| 高级特性 | 2 | 2 | 0 | NOTIFY/LISTEN, RLS |
| **合计** | **73** | **73** | **0** | 新增跨 backend 2PC prepared 事务回归 |

---

## 1. 认证与连接

### 1.1 登录（admin）

**输入**
```
admin admin
```

**预期输出**
```
successfully login
```

**实际结果** ✅ `successfully login`

---

### 1.2 退出

**输入**
```
exit
```

**预期输出**
```
Quitting...
```

**实际结果** ✅ `Quitting...`

---

## 2. DDL - 数据库管理

### 2.1 CREATE DATABASE

**输入**
```sql
CREATE DATABASE testdb;
```

**实际结果** ✅ `Create Database succeeded (charset=utf8)`

---

### 2.2 USE DATABASE

**输入**
```sql
USE DATABASE testdb;
```

**实际结果** ✅ `set Database to testdb`

---

### 2.3 DROP DATABASE

**输入**
```sql
DROP DATABASE testdb;
```

**实际结果** ✅ `Database dropped`

---

## 3. DDL - 表管理

### 3.1 CREATE TABLE（圆括号 + 冒号格式）

**输入**
```sql
CREATE TABLE t1 (id:int:0:1, name:varchar(50):0);
```

**实际结果** ✅ `Table create succeeded`
> 注：此前 `(col:type:flags)` 格式因 tokenize 不分割冒号导致零列解析，已修复。

---

### 3.2 CREATE TABLE（花括号格式）

**输入**
```sql
CREATE TABLE t2 {id:int pk, age:int:0};
```

**实际结果** ✅ `Table create succeeded`

---

### 3.3 INSERT + SELECT 验证

**输入**
```sql
INSERT INTO t1 (id, name) VALUES (1, 'alice');
INSERT INTO t1 (id, name) VALUES (2, 'bob');
SELECT * FROM t1;
```

**实际结果** ✅
```
1 row(s) inserted
1 row(s) inserted
id name
1 alice
2 bob
```

---

### 3.4 ALTER TABLE ADD COLUMN

**输入**
```sql
ALTER TABLE t1 ADD COLUMN email varchar(100);
```

**实际结果** ✅ `Column added`

---

### 3.5 ALTER TABLE DROP COLUMN

**输入**
```sql
ALTER TABLE t1 DROP COLUMN email;
```

**实际结果** ✅ `Column dropped`

---

### 3.6 DROP TABLE

**输入**
```sql
DROP TABLE t2;
```

**实际结果** ✅ `Table dropped`

---

### 3.7 ALTER TABLE RENAME COLUMN

**输入**
```sql
ALTER TABLE t1 RENAME COLUMN name TO username;
```

**实际结果** ✅ `Column renamed`

---

### 3.8 ALTER TABLE RENAME TO

**输入**
```sql
ALTER TABLE t1 RENAME TO t1_new;
```

**实际结果** ✅ `Table renamed`

---

### 3.9 CREATE TABLE AS SELECT (CTAS)

**输入**
```sql
CREATE TABLE t3 AS SELECT * FROM t1;
CREATE TABLE t4 AS SELECT id, name FROM t1 WHERE id = 1;
```

**实际结果** ✅ `Table created with N rows`，数据正确复制

---

### 3.10 CREATE TABLE with POINT type

**输入**
```sql
CREATE TABLE geo_points (id:int:0:1, loc:point:0);
```

**实际结果** ✅ `Table create succeeded`

---

### 3.11 INSERT POINT values

**输入**
```sql
INSERT INTO geo_points (id, loc) VALUES (1, '0.0,0.0');
INSERT INTO geo_points (id, loc) VALUES (2, '10.0,10.0');
INSERT INTO geo_points (id, loc) VALUES (3, '20.0,5.0');
INSERT INTO geo_points (id, loc) VALUES (4, '-5.0,15.0');
```

**实际结果** ✅ 全部 `1 row(s) inserted`

---

### 3.12 SELECT with spatial operators

**输入**
```sql
SELECT * FROM geo_points WHERE loc << '15.0,15.0';   -- left of
SELECT * FROM geo_points WHERE loc >> '15.0,15.0';   -- right of
SELECT * FROM geo_points WHERE loc <^ '15.0,15.0';   -- below
SELECT * FROM geo_points WHERE loc >^ '15.0,15.0';   -- above
SELECT * FROM geo_points WHERE loc <@ '10.0,10.0,5.0'; -- within circle
```

**实际结果** ✅
- `<<` 返回 x < 15 的点（id 1, 2, 4）
- `>>` 返回 x > 15 的点（id 3）
- `<^` 返回 y < 15 的点（id 1, 2, 3）
- `>^` 返回 y > 15 的点（无）
- `<@` 返回圆内点（id 2，中心 (10,10) 半径 5）

---

### 3.13 CREATE TABLE with INET / CIDR type

**输入**
```sql
CREATE TABLE networks (id INT, name VARCHAR(50), ip INET, net CIDR);
```

**实际结果** ✅ `Table create succeeded`

---

### 3.14 INSERT and query INET/CIDR with operators

**输入**
```sql
INSERT INTO networks (id, name, ip, net) VALUES (1, 'host1', '192.168.1.1', '192.168.1.0/24');
INSERT INTO networks (id, name, ip, net) VALUES (2, 'host2', '192.168.2.1', '192.168.2.0/24');
INSERT INTO networks (id, name, ip, net) VALUES (3, 'host3', '10.0.0.1', '10.0.0.0/16');
-- subnet-of: MATCH rows 1,2
SELECT * FROM networks WHERE net << '192.168.0.0/16';
-- contains: MATCH row 1
SELECT * FROM networks WHERE net >> '192.168.1.1';
-- overlap: MATCH rows 1,2
SELECT * FROM networks WHERE net && '192.168.0.0/16';
-- equality
SELECT * FROM networks WHERE ip = '192.168.1.1';
```

**实际结果** ✅ 所有操作符返回正确结果：`<<` 子网判断、`>>` 包含判断、`&&` 重叠判断、`=` 精确匹配

---

## 4. DDL - 索引管理

### 4.1 CREATE INDEX (B+Tree)

**输入**
```sql
CREATE INDEX idx_name ON t1(name);
```

**实际结果** ✅ `Index created`

边界回归同时验证了 250 条跨叶唯一键、6000 条跨叶及内部节点重复键的精确查找，以及范围扫描不重复返回。

---

### 4.2 CREATE INDEX USING HASH

**输入**
```sql
CREATE INDEX idx_hash ON t1 USING HASH (id);
```

**实际结果** ✅ `Index created`

---

### 4.3 CREATE FULLTEXT INDEX

**输入**
```sql
CREATE FULLTEXT INDEX idx_ft ON t1(name);
```

**实际结果** ✅ `Fulltext index created`

该命令是项目扩展语法，不属于 PostgreSQL `CREATE INDEX ... USING ...` 标准路径。

---

### 4.4 CREATE GIN INDEX

**输入**
```sql
CREATE INDEX idx_gin ON t1 USING GIN (name);
```

**实际结果** ✅ 创建真实 GIN 文件索引并登记 typed catalog 元数据。

---

### 4.5 CREATE GiST INDEX

**输入**
```sql
CREATE INDEX idx_gist ON t1 USING GiST (id);
```

**实际结果** ✅ 通过 typed DDL 创建 GiST 风格物理索引；当前实现仍是简化 range/spatial 结构，不宣称 PostgreSQL 通用 GiST opclass 兼容。

---

### 4.6 CREATE BRIN INDEX

**输入**
```sql
CREATE INDEX idx_brin ON t1 USING BRIN (id);
```

**实际结果** ✅ `Index created`

---

### 4.7 CREATE SPGIST INDEX

**输入**
```sql
CREATE TABLE locations (id INT PRIMARY KEY, pos CHAR(20));
INSERT INTO locations VALUES (1, '0.0,0.0');
INSERT INTO locations VALUES (2, '10.0,10.0');
INSERT INTO locations VALUES (3, '20.0,20.0');
INSERT INTO locations VALUES (4, '-5.0,5.0');
CREATE INDEX idx_spg ON locations USING SPGIST (pos);
SELECT * FROM locations;
```

**实际结果** ✅ 四叉树空间索引创建成功，查询返回 4 行坐标数据

---

### 4.8 DROP INDEX

**输入**
```sql
DROP INDEX idx_name;
DROP INDEX idx_hash;
DROP INDEX idx_name, idx_hash CASCADE;
DROP INDEX IF EXISTS missing_index;
DROP INDEX idx_name ON t1; -- migration-only legacy suffix
```

**实际结果** ✅ 标准 name-only、多名称、`IF EXISTS`、`CASCADE/RESTRICT` 以及 B-tree/Hash/GIN/GiST/BRIN/SP-GiST 物理索引均通过；物理索引、`pg_class` 和依赖元数据同步清理。

---

## 5. DDL - 视图

### 5.1 CREATE VIEW

**输入**
```sql
CREATE VIEW v1 AS SELECT id, name FROM t1 WHERE id > 0;
```

**实际结果** ✅ `View created`

---

### 5.2 SELECT FROM VIEW

**输入**
```sql
SELECT * FROM v1;
```

**实际结果** ✅ 返回视图数据

---

### 5.3 DROP VIEW

**输入**
```sql
DROP VIEW v1;
```

**实际结果** ✅ `View dropped`

---

### 5.4 ALTER VIEW RENAME TO

**输入**
```sql
CREATE VIEW v1 AS SELECT id, name FROM t1 WHERE id > 0;
ALTER VIEW v1 RENAME TO v2;
SELECT * FROM v2;
```

**实际结果** ✅ `View renamed`，v2 可正常查询并返回数据

---

### 5.5 ALTER VIEW SET SCHEMA

**输入**
```sql
ALTER VIEW v2 SET SCHEMA other_db;
USE DATABASE other_db;
SELECT * FROM v2;
```

**实际结果** ✅ `View schema changed`
> 注：视图定义中的基表名未带数据库限定，若目标数据库中不存在同名基表，查询会报错。此为已知限制。

---

## 6. DDL - 触发器

### 6.1 CREATE TRIGGER

**输入**
```sql
CREATE TRIGGER trg1 AFTER INSERT ON t1 PRINT inserted;
```

**实际结果** ✅ `Trigger created`

---

### 6.2 DROP TRIGGER

**输入**
```sql
DROP TRIGGER trg1;
```

**实际结果** ✅ `Trigger dropped`

---

## 7. DML

### 7.1 INSERT INTO（显式列名）

**输入**
```sql
INSERT INTO t1 (id, name) VALUES (3, 'charlie');
```

**实际结果** ✅ `1 row(s) inserted`

---

### 7.2 INSERT INTO（省略列名）

**输入**
```sql
INSERT INTO t1 VALUES (4, 'dave', 28);
```

**实际结果** ✅ `1 row(s) inserted`

---

### 7.3 UPDATE

**输入**
```sql
UPDATE t1 SET name = 'alice2' WHERE id = 1;
```

**实际结果** ✅ `1 row(s) updated`

---

### 7.4 DELETE

**输入**
```sql
DELETE FROM t1 WHERE id = 3;
```

**实际结果** ✅ `1 row(s) deleted`

---

## 8. DQL

### 8.1 基本 SELECT

**输入**
```sql
SELECT * FROM t1;
SELECT id, name FROM t1 WHERE id = 1;
SELECT COUNT(*) FROM t1;
```

**实际结果** ✅ 均返回正确结果

---

### 8.2 JOIN

**输入**
```sql
CREATE TABLE t2 {fid:int pk, info:varchar(20)};
INSERT INTO t2 (fid, info) VALUES (1, 'info1');
SELECT * FROM t1 JOIN t2 ON t1.id = t2.fid;
```

**实际结果** ✅ 返回 JOIN 结果

---

### 8.3 UNION

**输入**
```sql
SELECT id FROM t1 UNION SELECT fid FROM t2;
```

**实际结果** ✅ 返回 UNION 结果

`tests/set_operation_volcano_test.cpp` 进一步验证了 Volcano `SetOperationOp` 的
UNION/INTERSECT/EXCEPT 及 ALL 多重集结果，并覆盖投影后的 DISTINCT 去重；协议 E2E
另验证了聚合 legacy producer 经 `MaterializedRowsOp` 进入同一集合节点。

---

### 8.4 GROUP BY + HAVING

**输入**
```sql
SELECT name, COUNT(*) FROM t1 GROUP BY name HAVING COUNT(*) > 0;
```

**实际结果** ✅ 返回分组结果

---

## 9. TCL - 事务控制

### 9.1 BEGIN / COMMIT

**输入**
```sql
BEGIN;
INSERT INTO t1 (id, name) VALUES (4, 'dave');
COMMIT;
```

**实际结果** ✅ `Transaction committed`

---

### 9.2 BEGIN / ROLLBACK

**输入**
```sql
BEGIN;
INSERT INTO t1 (id, name) VALUES (5, 'eve');
ROLLBACK;
SELECT * FROM t1 WHERE id = 5;
```

**实际结果** ✅ `Transaction rolled back`，后续 SELECT 无 id=5 行

---

### 9.3 SAVEPOINT

**输入**
```sql
BEGIN;
INSERT INTO t1 (id, name) VALUES (6, 'frank');
SAVEPOINT sp1;
INSERT INTO t1 (id, name) VALUES (7, 'grace');
ROLLBACK TO SAVEPOINT sp1;
COMMIT;
```

**实际结果** ✅ id=6 保留，id=7 回滚

协议回归还验证 `BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY`、`START TRANSACTION READ COMMITTED READ WRITE`，以及无固定字符串偏移依赖的保存点创建和 `ROLLBACK TO SAVEPOINT`。

---

## 10. DCL - 权限控制

### 10.1 CREATE USER + GRANT

**输入**
```sql
CREATE USER testuser WITH PASSWORD 'testpass' NOSUPERUSER;
GRANT SELECT ON t1 TO testuser;
```

**实际结果** ✅ `User created`, `GRANT succeeded`

---

### 10.2 REVOKE

**输入**
```sql
REVOKE SELECT ON t1 FROM testuser;
```

**实际结果** ✅ `REVOKE succeeded`

---

### 10.3 SHOW USERS

**输入**
```sql
SHOW USERS;
```

**实际结果** ✅ 列出所有用户及其权限级别（admin 1，普通用户 0 等）

---

### 10.4 SHOW ROLES

**输入**
```sql
CREATE ROLE auditor;
SHOW ROLES;
GRANT auditor TO testuser;
SHOW ROLES;
```

**实际结果** ✅ 列出所有角色名称，仅显示角色不显示已授予用户

---

## 11. 工具与诊断

### 11.1 SHOW TABLES

**输入**
```sql
SHOW TABLES;
```

**实际结果** ✅ 列出当前数据库所有表

---

### 11.2 SHOW COLUMNS

**输入**
```sql
SHOW COLUMNS FROM t1;
```

**实际结果** ✅ 显示列定义

---

### 11.3 SHOW INDEXES

**输入**
```sql
SHOW INDEXES FROM t1;
```

**实际结果** ✅ 显示索引列表

---

### 11.4 DESC

**输入**
```sql
DESC t1;
```

**实际结果** ✅ 显示表结构

---

### 11.5 EXPLAIN

**输入**
```sql
EXPLAIN SELECT * FROM t1 WHERE id = 1;
```

**实际结果** ✅ 返回执行计划

---

### 11.6 ANALYZE TABLE

**输入**
```sql
ANALYZE TABLE t1;
```

**实际结果** ✅ `ANALYZE completed`

---

### 11.7 VACUUM

**输入**
```sql
VACUUM t1;
```

**实际结果** ✅ `VACUUM completed, 0 pages freed`

---

### 11.8 CHECKPOINT

**输入**
```sql
CHECKPOINT;
```

**实际结果** ✅ `CHECKPOINT completed`（checkpoint WAL、LSN 文件和关系页刷盘均成功）

---

## 12. 分区管理

### 12.1 CREATE TABLE ... PARTITION BY RANGE

**输入**
```sql
CREATE TABLE sales (id:int:0:1, amount:int:0) PARTITION BY RANGE(id) (
    PARTITION p_low VALUES LESS THAN (100),
    PARTITION p_high VALUES LESS THAN (200)
);
```

**实际结果** ✅ `Table create succeeded`

---

### 12.2 ATTACH PARTITION

**输入**
```sql
CREATE TABLE p_mid {id:int pk, amount:int:0};
ALTER TABLE sales ATTACH PARTITION p_mid FOR VALUES FROM (100) TO (150);
```

**实际结果** ✅ `Partition attached`

---

### 12.3 DETACH PARTITION

**输入**
```sql
ALTER TABLE sales DETACH PARTITION p_mid;
```

**实际结果** ✅ `Partition detached`

---

## 13. 高级特性

### 13.1 NOTIFY / LISTEN

**输入**
```sql
LISTEN mychannel;
NOTIFY mychannel, 'hello';
```

**实际结果** ✅ `LISTEN mychannel`, `NOTIFY mychannel`

---

### 13.2 行级安全 (RLS)

**输入**
```sql
CREATE TABLE users (id:int:0:1, name:varchar(50):0, role:varchar(20):0);
INSERT INTO users (id, name, role) VALUES (1, 'alice', 'admin');
INSERT INTO users (id, name, role) VALUES (2, 'bob', 'user');
CREATE POLICY p1 ON users FOR SELECT USING (role = 'admin');
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
```

**实际结果** ✅ `Policy created`, `RLS enabled`

运行时回归还验证了：`USING (owner = current_user)` 只返回当前用户行；显式 `TO public` 按 PUBLIC 生效；多个默认 permissive 策略按 OR 组合，`AS RESTRICTIVE` 按 AND 组合；`FOR ALL` 省略 `WITH CHECK` 时继承 `USING`，阻止越权写入；`NOINHERIT` 用户不匹配父角色策略；表 owner、`pg_authid.rolsuper`/`rolbypassrls` 绕过普通 RLS，`FORCE ROW LEVEL SECURITY` 重新执行策略；owner 同步持久化到正式 schema 和 `pg_class.relowner`，`ALTER TABLE ... OWNER TO` 会立即改变 RLS 行为。普通查询、删除以及 `UPDATE ... FROM` 的来源扫描均不再暴露其他用户行；启用 RLS 但没有适用策略时默认返回零行。策略解析/求值失败会安全拒绝该扫描。

---

## 14. 已知问题与限制

### 14.0 TABLESPACE 物理路由

`CREATE TABLE ... TABLESPACE`、`ALTER TABLE ... SET TABLESPACE` 已验证真实关系文件迁移：
heap、FSM/VM、索引、分区和 TOAST 文件位于 `<LOCATION>/<DATABASE>/`，目标目录不存在或
表空间 marker 缺失时拒绝操作，不会回退到默认目录。测试还使用全新 `StorageEngine`
重启读取迁移后的行，覆盖了此前“重启打开空文件”的数据损失风险。

剩余限制：表空间权限/owner、`ALTER TABLESPACE` 完整语义和 PostgreSQL OID/符号链接布局。

### 14.1 ALTER TABLE SET SCHEMA

**当前行为**：命令已由 AST bridge 转发到现有存储实现；当前实现的目标参数仍按数据库级迁移处理，不等价于 PostgreSQL 的 namespace-only 语义。

**级别**：部分实现（语义与 catalog namespace 迁移仍待补齐）

---

## 15. 结论

本次测试覆盖的历史手册场景、137 个独立 C++ 回归测试、PostgreSQL 协议 E2E 和窗口函数 E2E 均通过；分组 Volcano 单元与主 SQL 手工回归也通过。系统在以下方面表现稳定：

- ✅ 基本 CRUD（CREATE/INSERT/SELECT/UPDATE/DELETE/DROP）
- ✅ **POINT 数据类型**与空间运算符（`<<` / `>>` / `<^` / `>^` / `<@`）
- ✅ **INET/CIDR 网络类型**与网络运算符（`<<` / `>>` / `&&`）
- ✅ 索引系统基础路径（B+Tree/Hash/FullText/GIN/BRIN/SP-GiST）；GiST 仍列为缺失
- ✅ 视图与触发器（含 ALTER VIEW RENAME TO / SET SCHEMA，以及简单单表视图多行 `INSTEAD OF` INSERT/UPDATE/DELETE 协议回归）
- ✅ 事务控制（BEGIN/COMMIT/ROLLBACK/SAVEPOINT）
- ✅ 权限管理（GRANT/REVOKE）
- ✅ 分区管理（Range/List/Hash + ATTACH/DETACH）
- ✅ 查询能力（JOIN/UNION/INTERSECT/EXCEPT 含 ALL 语义、基础 GROUP BY/窗口函数/CTE/LATERAL、未关联单列 IN/NOT IN、未关联单表 EXISTS/NOT EXISTS、单列未关联 ANY/ALL、单个未关联标量目标）
- ✅ Volcano 并行 heap scan（page-range workers、确定性 Gather、串行结果等价性）
- ✅ 工具命令（SHOW/EXPLAIN/ANALYZE/VACUUM/CHECKPOINT）
- ✅ INSERT 省略列名 / ALTER RENAME / CREATE TABLE AS SELECT

**未发现阻塞性 bug**。

---

*报告生成时间：2026-08-14*
*测试执行人：自动化测试脚本 + 人工验证*
- Parser 边界回归：`parser_phase1_test` 新增非法 `LIMIT/OFFSET/FETCH`、负数、缺失计数以及省略 FETCH count 的测试；非法计数 fail-closed，省略 count 默认 1。
- Parser 数值选项回归：同一测试新增非法函数 `COST/ROWS`、角色 `CONNECTION LIMIT` 和列统计目标测试，确认解析失败不会产生默认 AST。
- Sequence DDL 边界回归：`sequence_full_test` 新增非法 `CREATE SEQUENCE START`、`ALTER SEQUENCE INCREMENT/CACHE` 测试，确认不会创建或改变序列，合法序列、缓存、重命名、依赖和重启行为保持通过。
- DDL 清理回归：`ddl_bridge_routing_test` 验证 `ALTER ROLE/ALTER USER` 继续由 typed bridge 执行；删除 main.cpp 中不可达的重复角色执行器后路径保持一致。
